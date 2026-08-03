"""Restart-durable file adapters for the ADR-002 actuation boundary.

Purpose:
    Persist immutable actuation audit records and the automatic-action disable
    switch without adding Milvus, detector, or policy logic.
Inputs:
    Frozen ``ActuationAuditRecord`` values, externally supplied audit identity,
    and an injectable RFC3339-UTC clock.
Outputs:
    An append-only, process-locked JSONL audit and an atomic controller state.
Dependencies:
    Python's POSIX file-locking and filesystem primitives only; never PyMilvus.
Failure modes:
    Duplicate audit IDs and malformed audit logs raise. Missing controller state
    is enabled; unreadable or malformed controller state is disabled fail-closed.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, fields
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, TypeAlias

from .actuation import (
    ActuationAuditRecord,
    ActuationContext,
    RollbackVerification,
    ShadowResult,
)
from .config import Metric
from .drift import EvidenceProvenance, evidence_provenance_valid
from .policy import CanaryObservation, QualificationResult, SafetyGateResult

AUDIT_SCHEMA_VERSION = 2
CONTROLLER_SCHEMA_VERSION = 1
REENABLE_CONFIRMATION_TOKEN = "I_CONFIRM_RE_ENABLE_AUTOMATIC_ACTIONS"

Clock: TypeAlias = Callable[[], str]

_AUDIT_ENVELOPE_FIELDS = frozenset({"schema_version", "record"})
_AUDIT_RECORD_FIELDS = frozenset(field.name for field in fields(ActuationAuditRecord))
_CONTEXT_FIELDS = frozenset(field.name for field in fields(ActuationContext))
_QUALIFICATION_FIELDS = frozenset(field.name for field in fields(QualificationResult))
_SAFETY_GATE_FIELDS = frozenset(field.name for field in fields(SafetyGateResult))
_SHADOW_FIELDS = frozenset(field.name for field in fields(ShadowResult))
_CANARY_FIELDS = frozenset(field.name for field in fields(CanaryObservation))
_ROLLBACK_FIELDS = frozenset(field.name for field in fields(RollbackVerification))
_EVIDENCE_PROVENANCE_FIELDS = frozenset(
    field.name for field in fields(EvidenceProvenance)
)
_CONTROLLER_FIELDS = frozenset(
    {
        "schema_version",
        "state",
        "audit_id",
        "reason",
        "changed_at_utc",
        "confirmed_by",
    }
)
_RFC3339_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")


class AuditLogCorruptedError(RuntimeError):
    """Raised when any audit line cannot be trusted as schema-version-1 data."""


class DuplicateAuditIdError(ValueError):
    """Raised when append would reuse an immutable audit identity."""


class _DuplicateJsonField(ValueError):
    """Internal marker for duplicate JSON object keys."""


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _valid_rfc3339_utc(value: object) -> bool:
    if not isinstance(value, str) or _RFC3339_UTC.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    offset = parsed.utcoffset()
    return offset is not None and offset.total_seconds() == 0


def _current_rfc3339_utc() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _object_without_duplicate_keys(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _exact_mapping(value: object, expected_fields: frozenset[str]) -> bool:
    return isinstance(value, Mapping) and frozenset(value) == expected_fields


def _valid_optional_mapping(
    value: object,
    expected_fields: frozenset[str],
) -> bool:
    return value is None or _exact_mapping(value, expected_fields)


def _validate_evidence_provenance(value: object) -> None:
    if value is None:
        return
    if not _exact_mapping(value, _EVIDENCE_PROVENANCE_FIELDS):
        raise AuditLogCorruptedError(
            "audit evidence provenance fields do not match schema"
        )
    assert isinstance(value, Mapping)
    try:
        provenance = EvidenceProvenance(
            schema_version=value["schema_version"],
            metric=Metric(value["metric"]),
            threshold_stratum=value["threshold_stratum"],
            reference_window_id=value["reference_window_id"],
            current_window_id=value["current_window_id"],
            reference_manifest_sha256=value["reference_manifest_sha256"],
            current_manifest_sha256=value["current_manifest_sha256"],
            configuration_identity=value["configuration_identity"],
            data_identity=value["data_identity"],
            flat_binding_id=value["flat_binding_id"],
            hnsw_binding_id=value["hnsw_binding_id"],
            reference_audit_ids=tuple(value["reference_audit_ids"]),
            reference_audit_rank_digests=tuple(value["reference_audit_rank_digests"]),
            current_audit_ids=tuple(value["current_audit_ids"]),
            current_audit_rank_digests=tuple(value["current_audit_rank_digests"]),
            sha256=value["sha256"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AuditLogCorruptedError("audit evidence provenance is malformed") from exc
    if not evidence_provenance_valid(provenance):
        raise AuditLogCorruptedError("audit evidence provenance is invalid")


def _validate_audit_payload(payload: object) -> dict[str, Any]:
    if not _exact_mapping(payload, _AUDIT_ENVELOPE_FIELDS):
        raise AuditLogCorruptedError("audit envelope fields do not match schema")
    assert isinstance(payload, Mapping)
    if (
        type(payload["schema_version"]) is not int
        or payload["schema_version"] != AUDIT_SCHEMA_VERSION
    ):
        raise AuditLogCorruptedError("unsupported audit schema version")
    record = payload["record"]
    if not _exact_mapping(record, _AUDIT_RECORD_FIELDS):
        raise AuditLogCorruptedError("audit record fields do not match schema")
    assert isinstance(record, Mapping)
    if not _nonempty(record["audit_id"]):
        raise AuditLogCorruptedError("audit record has an empty audit_id")
    if not _exact_mapping(record["context"], _CONTEXT_FIELDS):
        raise AuditLogCorruptedError("audit context fields do not match schema")
    context = record["context"]
    assert isinstance(context, Mapping)
    if not _exact_mapping(context["last_known_good"], _QUALIFICATION_FIELDS):
        raise AuditLogCorruptedError("audit last-known-good fields do not match schema")
    gates = record["safety_gate_results"]
    if not isinstance(gates, list) or any(
        not _exact_mapping(gate, _SAFETY_GATE_FIELDS) for gate in gates
    ):
        raise AuditLogCorruptedError("audit safety-gate fields do not match schema")
    optional_fields = (
        (record["shadow_result"], _SHADOW_FIELDS, "shadow result"),
        (record["canary_observation"], _CANARY_FIELDS, "canary observation"),
        (
            record["rollback_verification"],
            _ROLLBACK_FIELDS,
            "rollback verification",
        ),
    )
    for value, expected_fields, label in optional_fields:
        if not _valid_optional_mapping(value, expected_fields):
            raise AuditLogCorruptedError(f"audit {label} fields do not match schema")
    _validate_evidence_provenance(record["evidence_provenance"])
    return dict(record)


def _decode_audit_line(line: str, *, line_number: int) -> dict[str, Any]:
    try:
        payload = json.loads(line, object_pairs_hook=_object_without_duplicate_keys)
    except (json.JSONDecodeError, _DuplicateJsonField, TypeError, ValueError) as exc:
        raise AuditLogCorruptedError(
            f"malformed audit JSON at line {line_number}"
        ) from exc
    return _validate_audit_payload(payload)


def _scan_audit_handle(handle: Any) -> set[str]:
    handle.seek(0)
    try:
        payload = handle.read()
    except (OSError, UnicodeError) as exc:
        raise AuditLogCorruptedError("audit log is unreadable") from exc
    if payload and not payload.endswith("\n"):
        raise AuditLogCorruptedError("audit log has an incomplete final line")
    audit_ids: set[str] = set()
    for line_number, line in enumerate(payload.splitlines(), start=1):
        record = _decode_audit_line(line, line_number=line_number)
        audit_id = record["audit_id"]
        if audit_id in audit_ids:
            raise AuditLogCorruptedError(
                f"audit log contains duplicate audit_id {audit_id!r}"
            )
        audit_ids.add(audit_id)
    return audit_ids


class JsonlAuditSink:
    """Process-safe, append-only schema-version-2 JSONL audit sink."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)

    def contains(self, audit_id: str) -> bool:
        """Return whether ``audit_id`` exists, raising on any corrupt line."""

        if not _nonempty(audit_id):
            raise ValueError("audit_id must be non-empty")
        try:
            handle = self.path.open("r", encoding="utf-8", newline="")
        except FileNotFoundError:
            return False
        except (OSError, UnicodeError) as exc:
            raise AuditLogCorruptedError("audit log is unreadable") from exc
        with handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
            try:
                return audit_id in _scan_audit_handle(handle)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def append(self, record: ActuationAuditRecord) -> None:
        """Lock, duplicate-check, append once, flush, and fsync one record."""

        if not isinstance(record, ActuationAuditRecord):
            raise TypeError("record must be an ActuationAuditRecord")
        if not _nonempty(record.audit_id):
            raise ValueError("record.audit_id must be non-empty")
        if not self.path.parent.is_dir():
            raise FileNotFoundError(
                f"parent directory does not exist: {self.path.parent}"
            )
        envelope = {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "record": asdict(record),
        }
        serialized = json.dumps(
            envelope,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )

        descriptor = os.open(
            self.path,
            os.O_RDWR | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        with os.fdopen(descriptor, "a+", encoding="utf-8", newline="") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                audit_ids = _scan_audit_handle(handle)
                if record.audit_id in audit_ids:
                    raise DuplicateAuditIdError(
                        f"duplicate audit_id: {record.audit_id}"
                    )
                handle.seek(0, os.SEEK_END)
                handle.write(serialized + "\n")
                handle.flush()
                os.fsync(handle.fileno())
                _fsync_parent(self.path)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _fsync_parent(path: Path) -> None:
    directory_descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    parent = path.parent
    if not parent.is_dir():
        raise FileNotFoundError(f"parent directory does not exist: {parent}")
    serialized = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=parent,
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            descriptor = -1
            handle.write(serialized + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_parent(path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _decode_controller_state(path: Path) -> dict[str, Any] | None:
    try:
        payload = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    try:
        decoded = json.loads(payload, object_pairs_hook=_object_without_duplicate_keys)
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        _DuplicateJsonField,
        TypeError,
        ValueError,
    ) as exc:
        raise ValueError("controller state is malformed") from exc
    if not _exact_mapping(decoded, _CONTROLLER_FIELDS):
        raise ValueError("controller state fields do not match schema")
    assert isinstance(decoded, Mapping)
    if (
        type(decoded["schema_version"]) is not int
        or decoded["schema_version"] != CONTROLLER_SCHEMA_VERSION
    ):
        raise ValueError("unsupported controller schema version")
    if decoded["state"] not in {"DISABLED", "ENABLED"}:
        raise ValueError("controller state value is invalid")
    if not _nonempty(decoded["reason"]):
        raise ValueError("controller reason is empty")
    if not _valid_rfc3339_utc(decoded["changed_at_utc"]):
        raise ValueError("controller timestamp is invalid")
    if decoded["state"] == "DISABLED":
        if not _nonempty(decoded["audit_id"]) or decoded["confirmed_by"] is not None:
            raise ValueError("disabled controller identity is invalid")
    elif not _nonempty(decoded["confirmed_by"]):
        raise ValueError("enabled controller confirmation identity is invalid")
    if decoded["audit_id"] is not None and not _nonempty(decoded["audit_id"]):
        raise ValueError("controller audit identity is invalid")
    return dict(decoded)


class FileAutomaticActionController:
    """Atomic, restart-durable automatic-action state with human re-enable."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        clock: Clock = _current_rfc3339_utc,
    ) -> None:
        if not callable(clock):
            raise TypeError("clock must be callable")
        self.path = Path(path)
        self._clock = clock

    def _timestamp(self) -> str:
        timestamp = self._clock()
        if not _valid_rfc3339_utc(timestamp):
            raise ValueError("clock must return an RFC3339 UTC timestamp ending Z")
        return timestamp

    def disable_automatic_actions(self, *, audit_id: str, reason: str) -> None:
        """Atomically persist disabled state for the triggering audit record."""

        if not _nonempty(audit_id):
            raise ValueError("audit_id must be non-empty")
        if not _nonempty(reason):
            raise ValueError("reason must be non-empty")
        _atomic_write_json(
            self.path,
            {
                "schema_version": CONTROLLER_SCHEMA_VERSION,
                "state": "DISABLED",
                "audit_id": audit_id,
                "reason": reason,
                "changed_at_utc": self._timestamp(),
                "confirmed_by": None,
            },
        )

    def is_disabled(self) -> bool:
        """Return false only for missing or strictly valid enabled state."""

        try:
            state = _decode_controller_state(self.path)
        except (OSError, ValueError):
            return True
        return state is not None and state["state"] == "DISABLED"

    def re_enable(
        self,
        *,
        confirmation: str,
        confirmed_by: str,
        reason: str,
    ) -> None:
        """Explicitly re-enable after exact human confirmation."""

        if confirmation != REENABLE_CONFIRMATION_TOKEN:
            raise ValueError("exact human confirmation token is required")
        if not _nonempty(confirmed_by):
            raise ValueError("confirmed_by must be non-empty")
        if not _nonempty(reason):
            raise ValueError("reason must be non-empty")
        try:
            prior = _decode_controller_state(self.path)
        except (OSError, ValueError):
            prior = None
        _atomic_write_json(
            self.path,
            {
                "schema_version": CONTROLLER_SCHEMA_VERSION,
                "state": "ENABLED",
                "audit_id": None if prior is None else prior["audit_id"],
                "reason": reason,
                "changed_at_utc": self._timestamp(),
                "confirmed_by": confirmed_by,
            },
        )


__all__ = [
    "AUDIT_SCHEMA_VERSION",
    "CONTROLLER_SCHEMA_VERSION",
    "REENABLE_CONFIRMATION_TOKEN",
    "AuditLogCorruptedError",
    "DuplicateAuditIdError",
    "FileAutomaticActionController",
    "JsonlAuditSink",
]
