"""Atomic LKG-only recovery marker for EXP-009 Stage 2.

This module persists activation intent, never a candidate route.  It has no
policy, approval, routing-plan, network, or Milvus dependency.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import stat
import tempfile
import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from .config import HNSW_EF_SWEEP, THRESHOLD_LABELS, Metric

__all__ = [
    "FileCanaryRouteStateStore",
    "RouteState",
    "RouteStateBinding",
    "RouteStateRecord",
    "RouteStateRecovery",
    "RouteStateStoreError",
]


_SCHEMA_VERSION = "canary-route-state-v1"
_FIELDS = frozenset(
    {
        "schema_version", "state", "metric", "threshold_stratum",
        "last_known_good_ef", "configuration_identity", "data_identity",
        "flat_binding_id", "hnsw_binding_id", "grant_id", "plan_sha256",
        "changed_at_utc", "reason_code",
    }
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_TIMESTAMP_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_REASON_RE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


class RouteStateStoreError(RuntimeError):
    """A fail-closed route-state persistence error."""


class RouteState(StrEnum):
    """Only activation intent and LKG-only states can be persisted."""

    LKG_ONLY = "LKG_ONLY"
    ACTIVATING = "ACTIVATING"


@dataclass(frozen=True, slots=True)
class RouteStateBinding:
    """Non-sensitive current LKG identity required to trust a marker."""

    metric: Metric
    threshold_stratum: str
    last_known_good_ef: int
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str


@dataclass(frozen=True, slots=True)
class RouteStateRecord:
    """Strict persisted marker; it intentionally cannot reconstruct a plan."""

    state: RouteState
    binding: RouteStateBinding
    grant_id: str | None
    plan_sha256: str | None
    changed_at_utc: str
    reason_code: str


@dataclass(frozen=True, slots=True)
class RouteStateRecovery:
    """Startup result that is always non-routing and fail-closed."""

    record: RouteStateRecord
    recovered: bool
    persisted: bool
    reason_code: str


class _DuplicateJsonField(ValueError):
    pass


def _canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} is not text")  # domain error type carries the governed reason code  # noqa: TRY004
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or normalized != value
        or value.strip() != value
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError(f"{field} is not canonical")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} is not a SHA-256 digest")
    return value


def _timestamp(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _TIMESTAMP_RE.fullmatch(value) is None:
        raise ValueError(f"{field} is not RFC3339 UTC")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} is not a valid UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} is not UTC")
    return value


def _reason(value: object) -> str:
    if not isinstance(value, str) or _REASON_RE.fullmatch(value) is None:
        raise ValueError("reason_code is invalid")
    return value


def _validate_binding(binding: object) -> RouteStateBinding:
    if not isinstance(binding, RouteStateBinding):
        raise ValueError("binding is invalid")  # domain error type carries the governed reason code  # noqa: TRY004
    if not isinstance(binding.metric, Metric) or binding.threshold_stratum not in THRESHOLD_LABELS:
        raise ValueError("binding metric or stratum is invalid")
    if binding.last_known_good_ef not in tuple(value for value in HNSW_EF_SWEEP if value != 100):
        raise ValueError("binding last-known-good ef is invalid")
    for field in (
        "configuration_identity", "data_identity", "flat_binding_id", "hnsw_binding_id"
    ):
        _canonical_text(getattr(binding, field), field=field)
    return binding


def _lkg_record(
    binding: RouteStateBinding, *, changed_at_utc: str, reason_code: str
) -> RouteStateRecord:
    return RouteStateRecord(
        state=RouteState.LKG_ONLY,
        binding=_validate_binding(binding),
        grant_id=None,
        plan_sha256=None,
        changed_at_utc=_timestamp(changed_at_utc, field="changed_at_utc"),
        reason_code=_reason(reason_code),
    )


def _document(record: RouteStateRecord) -> dict[str, object]:
    binding = _validate_binding(record.binding)
    timestamp = _timestamp(record.changed_at_utc, field="changed_at_utc")
    reason = _reason(record.reason_code)
    if record.state is RouteState.ACTIVATING:
        grant_id = _canonical_text(record.grant_id, field="grant_id")
        plan_sha256 = _sha256(record.plan_sha256, field="plan_sha256")
    elif record.state is RouteState.LKG_ONLY:
        if record.grant_id is not None or record.plan_sha256 is not None:
            raise ValueError("LKG-only marker must not retain activation data")
        grant_id = None
        plan_sha256 = None
    else:
        raise ValueError("route state is invalid")
    return {
        "schema_version": _SCHEMA_VERSION,
        "state": record.state.value,
        "metric": binding.metric.value,
        "threshold_stratum": binding.threshold_stratum,
        "last_known_good_ef": binding.last_known_good_ef,
        "configuration_identity": binding.configuration_identity,
        "data_identity": binding.data_identity,
        "flat_binding_id": binding.flat_binding_id,
        "hnsw_binding_id": binding.hnsw_binding_id,
        "grant_id": grant_id,
        "plan_sha256": plan_sha256,
        "changed_at_utc": timestamp,
        "reason_code": reason,
    }


def _pairs_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(key)
        result[key] = value
    return result


def _record_from_document(document: object) -> RouteStateRecord:
    if not isinstance(document, dict) or frozenset(document) != _FIELDS:
        raise ValueError("route-state document fields are invalid")
    if document["schema_version"] != _SCHEMA_VERSION:
        raise ValueError("route-state schema is unsupported")
    binding = RouteStateBinding(
        metric=Metric(document["metric"]),
        threshold_stratum=document["threshold_stratum"],
        last_known_good_ef=document["last_known_good_ef"],
        configuration_identity=document["configuration_identity"],
        data_identity=document["data_identity"],
        flat_binding_id=document["flat_binding_id"],
        hnsw_binding_id=document["hnsw_binding_id"],
    )
    record = RouteStateRecord(
        state=RouteState(document["state"]),
        binding=_validate_binding(binding),
        grant_id=document["grant_id"],
        plan_sha256=document["plan_sha256"],
        changed_at_utc=document["changed_at_utc"],
        reason_code=document["reason_code"],
    )
    if _document(record) != document:
        raise ValueError("route-state document is noncanonical")
    return record


class FileCanaryRouteStateStore:
    """Single-host atomic marker; all recovery results remain LKG-only."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)

    def begin_activation(
        self, *, binding: RouteStateBinding, grant_id: str, plan_sha256: str, changed_at_utc: str
    ) -> RouteStateRecord:
        record = RouteStateRecord(
            state=RouteState.ACTIVATING, binding=_validate_binding(binding),
            grant_id=_canonical_text(grant_id, field="grant_id"),
            plan_sha256=_sha256(plan_sha256, field="plan_sha256"),
            changed_at_utc=_timestamp(changed_at_utc, field="changed_at_utc"),
            reason_code="ACTIVATION_PENDING",
        )
        with self._locked():
            prior = self._read_unlocked(optional=True)
            if prior is not None and prior.state is RouteState.ACTIVATING:
                raise RouteStateStoreError("ROUTE_STATE_ACTIVATION_ALREADY_PENDING")
            if prior is not None and prior.binding != record.binding:
                raise RouteStateStoreError("ROUTE_STATE_IDENTITY_MISMATCH")
            self._write_unlocked(record)
        return record

    def clear_to_lkg(
        self, *, binding: RouteStateBinding, reason_code: str, changed_at_utc: str
    ) -> RouteStateRecord:
        record = _lkg_record(binding, changed_at_utc=changed_at_utc, reason_code=reason_code)
        with self._locked():
            self._write_unlocked(record)
        return record

    def load(self) -> RouteStateRecord | None:
        with self._locked():
            return self._read_unlocked(optional=True)

    def recover(
        self, *, expected_binding: RouteStateBinding, changed_at_utc: str
    ) -> RouteStateRecovery:
        binding = _validate_binding(expected_binding)
        timestamp = _timestamp(changed_at_utc, field="changed_at_utc")
        try:
            with self._locked():
                try:
                    prior = self._read_unlocked(optional=True)
                    if prior is None:
                        reason, recovered = "RECOVERY_NO_MARKER", True
                    elif prior.binding != binding:
                        reason, recovered = "RECOVERY_IDENTITY_MISMATCH", True
                    elif prior.state is RouteState.ACTIVATING:
                        reason, recovered = "RECOVERY_FAILBACK", True
                    else:
                        return RouteStateRecovery(prior, False, True, "LKG_ONLY")
                except RouteStateStoreError:
                    reason, recovered = "RECOVERY_MARKER_INVALID", True
                result = _lkg_record(binding, changed_at_utc=timestamp, reason_code=reason)
                self._write_unlocked(result)
                return RouteStateRecovery(result, recovered, True, reason)
        except (OSError, RouteStateStoreError):
            result = _lkg_record(
                binding, changed_at_utc=timestamp, reason_code="RECOVERY_STATE_WRITE_FAILED"
            )
            return RouteStateRecovery(result, True, False, "RECOVERY_STATE_WRITE_FAILED")

    @contextmanager
    def _locked(self) -> Iterator[None]:
        parent = self.path.parent
        if not parent.is_dir() or stat.S_IMODE(parent.stat().st_mode) & 0o077:
            raise RouteStateStoreError("ROUTE_STATE_DIRECTORY_NOT_PRIVATE")
        try:
            marker_status = self.path.lstat()
        except FileNotFoundError:
            marker_status = None
        except OSError as exc:
            raise RouteStateStoreError("ROUTE_STATE_PATH_INVALID") from exc
        if marker_status is not None and (
            stat.S_ISLNK(marker_status.st_mode) or not stat.S_ISREG(marker_status.st_mode)
        ):
            raise RouteStateStoreError("ROUTE_STATE_PATH_INVALID")
        lock_path = parent / f".{self.path.name}.lock"
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        with os.fdopen(descriptor, "r+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _read_unlocked(self, *, optional: bool) -> RouteStateRecord | None:
        try:
            payload = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            if optional:
                return None
            raise RouteStateStoreError("ROUTE_STATE_MARKER_MISSING") from None
        except (OSError, UnicodeError) as exc:
            raise RouteStateStoreError("ROUTE_STATE_MARKER_INVALID") from exc
        try:
            document = json.loads(payload, object_pairs_hook=_pairs_without_duplicates)
            record = _record_from_document(document)
            canonical = json.dumps(_document(record), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n"
            if payload != canonical:
                raise ValueError("route-state payload is noncanonical")
            return record
        except (TypeError, ValueError, json.JSONDecodeError, _DuplicateJsonField) as exc:
            raise RouteStateStoreError("ROUTE_STATE_MARKER_INVALID") from exc

    def _write_unlocked(self, record: RouteStateRecord) -> None:
        document = _document(record)
        parent = self.path.parent
        payload = (json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{self.path.name}.", suffix=".tmp", dir=parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                descriptor = -1
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, self.path)
            directory_descriptor = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError as exc:
            raise RouteStateStoreError("ROUTE_STATE_WRITE_FAILED") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
