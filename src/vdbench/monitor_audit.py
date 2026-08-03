"""Restart-durable append-only audit sink for ADR-005's workload monitor.

Purpose:
    Provide the concrete single-host implementation of ``MonitorAuditSink``
    needed by live DRY_RUN composition roots.
Inputs:
    Immutable ``MonitorAuditRecord`` instances supplied by ``WorkloadMonitor``.
Outputs:
    Strict schema-versioned JSONL records, written under an advisory process
    lock with ``O_APPEND`` and ``fsync`` durability.
Failure modes:
    A malformed, unsafe, or duplicate record fails closed.  The sink neither
    repairs records nor permits a corrupt history to be silently ignored.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import stat
from typing import Any

from .config import Metric
from .shadow_event_types import MonitorStreamKey
from .workload_monitor import MonitorAuditRecord, MonitorAuditSink, MonitorRecordStatus


__all__ = [
    "DuplicateMonitorAuditRecordError",
    "FileMonitorAuditSink",
    "MonitorAuditLogCorruptedError",
]


_SCHEMA_VERSION = "workload-monitor-audit-v1"
_ROOT_FIELDS = frozenset({"schema_version", "record"})
_RECORD_FIELDS = frozenset(
    {
        "record_id",
        "stream_key",
        "window_id",
        "window_sequence",
        "event_ids",
        "event_trace_sha256",
        "status",
        "reason_codes",
        "manifest_sha256",
        "detector_state",
        "detector_classification",
        "policy_action",
        "policy_reason",
        "policy_audit_id",
    }
)
_KEY_FIELDS = frozenset(
    {
        "stream_id",
        "metric",
        "threshold_stratum",
        "configuration_identity",
        "data_identity",
        "flat_binding_id",
        "hnsw_binding_id",
    }
)
_SHA256_HEX = frozenset("0123456789abcdef")


class MonitorAuditLogCorruptedError(RuntimeError):
    """Raised when the persistent audit history cannot be trusted."""


class DuplicateMonitorAuditRecordError(ValueError):
    """Raised when an immutable monitor record ID would be reused."""


class _DuplicateJsonField(ValueError):
    """Internal marker for a JSON object containing duplicate keys."""


def _reject_duplicate_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _nonempty(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise MonitorAuditLogCorruptedError(f"invalid {field}")
    return value


def _optional_text(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    return _nonempty(value, field=field)


def _sha256(value: object, *, field: str) -> str:
    result = _nonempty(value, field=field)
    if len(result) != 64 or any(character not in _SHA256_HEX for character in result):
        raise MonitorAuditLogCorruptedError(f"invalid {field}")
    return result


def _window_id(value: object) -> int | str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise MonitorAuditLogCorruptedError("invalid window_id")
    if isinstance(value, int):
        return value
    return _nonempty(value, field="window_id")


def _optional_nonnegative_integer(value: object, *, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MonitorAuditLogCorruptedError(f"invalid {field}")
    return value


def _string_tuple(value: object, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise MonitorAuditLogCorruptedError(f"invalid {field}")
    return tuple(_nonempty(item, field=f"{field}[]") for item in value)


def _stream_key_document(value: MonitorStreamKey) -> dict[str, object]:
    return {
        "stream_id": value.stream_id,
        "metric": value.metric.value,
        "threshold_stratum": value.threshold_stratum,
        "configuration_identity": value.configuration_identity,
        "data_identity": value.data_identity,
        "flat_binding_id": value.flat_binding_id,
        "hnsw_binding_id": value.hnsw_binding_id,
    }


def _stream_key_from_document(value: object) -> MonitorStreamKey:
    if not isinstance(value, Mapping) or frozenset(value) != _KEY_FIELDS:
        raise MonitorAuditLogCorruptedError("invalid stream_key schema")
    try:
        return MonitorStreamKey(
            stream_id=_nonempty(value["stream_id"], field="stream_id"),
            metric=Metric(_nonempty(value["metric"], field="metric")),
            threshold_stratum=_nonempty(
                value["threshold_stratum"], field="threshold_stratum"
            ),
            configuration_identity=_nonempty(
                value["configuration_identity"], field="configuration_identity"
            ),
            data_identity=_nonempty(value["data_identity"], field="data_identity"),
            flat_binding_id=_nonempty(
                value["flat_binding_id"], field="flat_binding_id"
            ),
            hnsw_binding_id=_nonempty(
                value["hnsw_binding_id"], field="hnsw_binding_id"
            ),
        )
    except (TypeError, ValueError) as exc:
        raise MonitorAuditLogCorruptedError("invalid stream_key") from exc


def _record_document(record: MonitorAuditRecord) -> dict[str, object]:
    if not isinstance(record, MonitorAuditRecord):
        raise TypeError("record must be a MonitorAuditRecord")
    _nonempty(record.record_id, field="record_id")
    return {
        "record_id": record.record_id,
        "stream_key": _stream_key_document(record.stream_key),
        "window_id": record.window_id,
        "window_sequence": record.window_sequence,
        "event_ids": list(record.event_ids),
        "event_trace_sha256": list(record.event_trace_sha256),
        "status": record.status.value,
        "reason_codes": list(record.reason_codes),
        "manifest_sha256": record.manifest_sha256,
        "detector_state": record.detector_state,
        "detector_classification": record.detector_classification,
        "policy_action": record.policy_action,
        "policy_reason": record.policy_reason,
        "policy_audit_id": record.policy_audit_id,
    }


def _record_from_document(value: object) -> MonitorAuditRecord:
    if not isinstance(value, Mapping) or frozenset(value) != _RECORD_FIELDS:
        raise MonitorAuditLogCorruptedError("invalid record schema")
    event_ids = _string_tuple(value["event_ids"], field="event_ids")
    trace_hashes = _string_tuple(value["event_trace_sha256"], field="event_trace_sha256")
    if len(event_ids) != len(trace_hashes):
        raise MonitorAuditLogCorruptedError("event/hash cardinality mismatch")
    for index, digest in enumerate(trace_hashes):
        _sha256(digest, field=f"event_trace_sha256[{index}]")
    manifest = value["manifest_sha256"]
    if manifest is not None:
        _sha256(manifest, field="manifest_sha256")
    try:
        status = MonitorRecordStatus(_nonempty(value["status"], field="status"))
    except ValueError as exc:
        raise MonitorAuditLogCorruptedError("invalid status") from exc
    return MonitorAuditRecord(
        record_id=_nonempty(value["record_id"], field="record_id"),
        stream_key=_stream_key_from_document(value["stream_key"]),
        window_id=_window_id(value["window_id"]),
        window_sequence=_optional_nonnegative_integer(
            value["window_sequence"], field="window_sequence"
        ),
        event_ids=event_ids,
        event_trace_sha256=trace_hashes,
        status=status,
        reason_codes=_string_tuple(value["reason_codes"], field="reason_codes"),
        manifest_sha256=manifest,
        detector_state=_optional_text(value["detector_state"], field="detector_state"),
        detector_classification=_optional_text(
            value["detector_classification"], field="detector_classification"
        ),
        policy_action=_optional_text(value["policy_action"], field="policy_action"),
        policy_reason=_optional_text(value["policy_reason"], field="policy_reason"),
        policy_audit_id=_optional_text(value["policy_audit_id"], field="policy_audit_id"),
    )


def _decode_records(payload: bytes) -> tuple[MonitorAuditRecord, ...]:
    if not payload:
        return ()
    if not payload.endswith(b"\n"):
        raise MonitorAuditLogCorruptedError("audit log has an incomplete final line")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MonitorAuditLogCorruptedError("audit log is not UTF-8") from exc
    records: list[MonitorAuditRecord] = []
    seen_ids: set[str] = set()
    for number, line in enumerate(text.splitlines(), start=1):
        try:
            document = json.loads(line, object_pairs_hook=_reject_duplicate_fields)
        except (json.JSONDecodeError, _DuplicateJsonField) as exc:
            raise MonitorAuditLogCorruptedError(
                f"audit log line {number} is malformed"
            ) from exc
        if not isinstance(document, Mapping) or frozenset(document) != _ROOT_FIELDS:
            raise MonitorAuditLogCorruptedError(f"audit log line {number} has invalid schema")
        if document["schema_version"] != _SCHEMA_VERSION:
            raise MonitorAuditLogCorruptedError(f"audit log line {number} has unknown schema")
        record = _record_from_document(document["record"])
        if record.record_id in seen_ids:
            raise MonitorAuditLogCorruptedError("audit log contains duplicate record_id")
        seen_ids.add(record.record_id)
        records.append(record)
    return tuple(records)


def _safe_parent(path: Path) -> None:
    try:
        metadata = os.lstat(path.parent)
    except OSError as exc:
        raise MonitorAuditLogCorruptedError("audit parent is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & 0o022
    ):
        raise MonitorAuditLogCorruptedError("audit parent is unsafe")


def _checked_open(path: Path, flags: int, mode: int = 0o600) -> int:
    _safe_parent(path)
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        metadata = None
    except OSError as exc:
        raise MonitorAuditLogCorruptedError("audit log is unavailable") from exc
    if metadata is not None and (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & 0o077
    ):
        raise MonitorAuditLogCorruptedError("audit log is unsafe")
    try:
        descriptor = os.open(path, flags | os.O_NOFOLLOW, mode)
    except OSError as exc:
        raise MonitorAuditLogCorruptedError("audit log open failed") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_mode & 0o077
        ):
            raise MonitorAuditLogCorruptedError("audit log is unsafe")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_all(descriptor: int) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    blocks: list[bytes] = []
    while True:
        block = os.read(descriptor, 1024 * 1024)
        if not block:
            break
        blocks.append(block)
    return b"".join(blocks)


def _fsync_parent(path: Path) -> None:
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class FileMonitorAuditSink(MonitorAuditSink):
    """Process-safe strict JSONL implementation of ``MonitorAuditSink``."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)

    def contains(self, record_id: str) -> bool:
        """Return membership while rejecting a malformed prior audit history."""

        _nonempty(record_id, field="record_id")
        try:
            descriptor = _checked_open(self.path, os.O_RDONLY)
        except MonitorAuditLogCorruptedError as exc:
            if not self.path.exists() and "open failed" in str(exc):
                return False
            raise
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH)
            return any(record.record_id == record_id for record in _decode_records(_read_all(descriptor)))
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def read_records(self) -> tuple[MonitorAuditRecord, ...]:
        """Load all records strictly; intended for evidence reporting only."""

        try:
            descriptor = _checked_open(self.path, os.O_RDONLY)
        except MonitorAuditLogCorruptedError as exc:
            if not self.path.exists() and "open failed" in str(exc):
                return ()
            raise
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH)
            return _decode_records(_read_all(descriptor))
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def append(self, record: MonitorAuditRecord) -> None:
        """Append exactly one immutable record after a locked duplicate check."""

        record_document = _record_document(record)
        # Validate the outgoing value before it becomes durable.  Otherwise a
        # permissive caller could create an audit line that permanently blocks
        # all later reads instead of receiving an immediate fail-closed error.
        _record_from_document(record_document)
        document = {"schema_version": _SCHEMA_VERSION, "record": record_document}
        try:
            payload = (
                json.dumps(
                    document,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise MonitorAuditLogCorruptedError("audit record is not serializable") from exc
        descriptor = _checked_open(self.path, os.O_RDWR | os.O_APPEND | os.O_CREAT)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            records = _decode_records(_read_all(descriptor))
            if any(existing.record_id == record.record_id for existing in records):
                raise DuplicateMonitorAuditRecordError(record.record_id)
            written = os.write(descriptor, payload)
            if written != len(payload):
                raise OSError("partial audit append")
            os.fsync(descriptor)
            _fsync_parent(self.path)
        except OSError as exc:
            raise MonitorAuditLogCorruptedError("audit append failed") from exc
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
