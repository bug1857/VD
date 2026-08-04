"""Append-only non-sensitive lifecycle audit for EXP-009 Stage 2."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any
import unicodedata


__all__ = [
    "CanaryLifecycleAuditRecord", "JsonlCanaryLifecycleAuditSink",
    "LifecycleAuditCorruptedError", "LifecycleAuditDuplicateError", "lifecycle_event_id",
]

_SCHEMA = "canary-lifecycle-audit-v1"
_DOMAIN = b"vdbench.canary-lifecycle-audit/v1\0"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_UTC = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z")


class LifecycleAuditCorruptedError(RuntimeError):
    """The log cannot be trusted and must fail closed."""


class LifecycleAuditDuplicateError(ValueError):
    """An immutable lifecycle event ID was reused."""


@dataclass(frozen=True, slots=True)
class CanaryLifecycleAuditRecord:
    """Non-sensitive durable approval/route lifecycle evidence."""

    event_id: str
    event_type: str
    grant_id: str
    signed_payload_sha256: str
    policy_audit_id: str
    plan_sha256: str
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str
    recorded_at_utc: str
    reason_code: str


_RECORD_FIELDS = frozenset(field.name for field in fields(CanaryLifecycleAuditRecord))


def _text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} is invalid")
    if not value or value != value.strip() or value != unicodedata.normalize("NFC", value):
        raise ValueError(f"{field} is invalid")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise ValueError(f"{field} is invalid")
    return value


def _sha(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise ValueError(f"{field} is invalid")
    return value


def _code(value: object, field: str) -> str:
    if not isinstance(value, str) or _CODE.fullmatch(value) is None:
        raise ValueError(f"{field} is invalid")
    return value


def _utc(value: object) -> str:
    if not isinstance(value, str) or _UTC.fullmatch(value) is None:
        raise ValueError("recorded_at_utc is invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("recorded_at_utc is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("recorded_at_utc is invalid")
    return value


def lifecycle_event_id(*, grant_id: str, signed_payload_sha256: str, plan_sha256: str, event_type: str) -> str:
    """Derive the sole deterministic identity for a grant/plan lifecycle event."""

    material = "\0".join((_text(grant_id, "grant_id"), _sha(signed_payload_sha256, "signed_payload_sha256"), _sha(plan_sha256, "plan_sha256"), _code(event_type, "event_type"))).encode("utf-8")
    return hashlib.sha256(_DOMAIN + material).hexdigest()


def _validated(record: object) -> CanaryLifecycleAuditRecord:
    if not isinstance(record, CanaryLifecycleAuditRecord):
        raise ValueError("record is invalid")
    event_type = _code(record.event_type, "event_type")
    grant_id = _text(record.grant_id, "grant_id")
    payload = _sha(record.signed_payload_sha256, "signed_payload_sha256")
    plan = _sha(record.plan_sha256, "plan_sha256")
    expected = lifecycle_event_id(grant_id=grant_id, signed_payload_sha256=payload, plan_sha256=plan, event_type=event_type)
    if record.event_id != expected:
        raise ValueError("event_id is invalid")
    for name in ("policy_audit_id", "configuration_identity", "data_identity", "flat_binding_id", "hnsw_binding_id"):
        _text(getattr(record, name), name)
    _utc(record.recorded_at_utc)
    _code(record.reason_code, "reason_code")
    return record


def _decode(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _record_from_line(line: str) -> CanaryLifecycleAuditRecord:
    try:
        envelope = json.loads(line, object_pairs_hook=_decode)
        if not isinstance(envelope, dict) or frozenset(envelope) != {"schema_version", "record"}:
            raise ValueError("envelope invalid")
        if envelope["schema_version"] != _SCHEMA:
            raise ValueError("schema invalid")
        payload = envelope["record"]
        if not isinstance(payload, dict) or frozenset(payload) != _RECORD_FIELDS:
            raise ValueError("record fields invalid")
        record = CanaryLifecycleAuditRecord(**payload)
        _validated(record)
        canonical = _serialize(record).decode("utf-8").rstrip("\n")
        if line != canonical:
            raise ValueError("record noncanonical")
        return record
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LifecycleAuditCorruptedError("LIFECYCLE_AUDIT_CORRUPTED") from exc


def _serialize(record: CanaryLifecycleAuditRecord) -> bytes:
    return (json.dumps({"schema_version": _SCHEMA, "record": asdict(_validated(record))}, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


class JsonlCanaryLifecycleAuditSink:
    """Locked append-only lifecycle audit; malformed history always blocks append."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)

    def records(self) -> tuple[CanaryLifecycleAuditRecord, ...]:
        try:
            with self.path.open("r", encoding="utf-8", newline="") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
                try:
                    return self._scan(handle.read())
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except FileNotFoundError:
            return ()
        except (OSError, UnicodeError) as exc:
            raise LifecycleAuditCorruptedError("LIFECYCLE_AUDIT_CORRUPTED") from exc

    def contains(self, event_id: str) -> bool:
        _sha(event_id, "event_id")
        return any(record.event_id == event_id for record in self.records())

    def append(self, record: CanaryLifecycleAuditRecord) -> None:
        payload = _serialize(record)
        if not self.path.parent.is_dir():
            raise FileNotFoundError(f"parent directory does not exist: {self.path.parent}")
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(descriptor, "a+", encoding="utf-8", newline="") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0)
                if any(item.event_id == record.event_id for item in self._scan(handle.read())):
                    raise LifecycleAuditDuplicateError(f"duplicate lifecycle event: {record.event_id}")
                handle.seek(0, os.SEEK_END)
                handle.write(payload.decode("utf-8"))
                handle.flush(); os.fsync(handle.fileno())
                directory = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _scan(payload: str) -> tuple[CanaryLifecycleAuditRecord, ...]:
        if payload and not payload.endswith("\n"):
            raise LifecycleAuditCorruptedError("LIFECYCLE_AUDIT_CORRUPTED")
        records = tuple(_record_from_line(line) for line in payload.splitlines())
        if len({record.event_id for record in records}) != len(records):
            raise LifecycleAuditCorruptedError("LIFECYCLE_AUDIT_CORRUPTED")
        return records
