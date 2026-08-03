"""ADR-006 single-host durable outbox for immutable shadow-trace events.

Purpose:
    Persist a complete ``ShadowAuditTrace`` envelope before making a compact,
    checksum-bound event available to the DRY_RUN monitor protocol.
Inputs:
    A completed trace and externally assigned stream/window membership.
Outputs:
    At-least-once ``ShadowTraceEvent`` values and immutable on-disk evidence.
Dependencies:
    Existing trace-envelope codec and monitor event value types only.  This
    module does not construct database clients or evaluate detector/policy code.
Failure modes:
    Unsafe storage, bad context/trace data, capacity pressure, corruption, and
    conflicting publication fail closed.  Corrupt pending events are retained in
    a rejected ledger and never delivered.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Protocol
import unicodedata

from .config import Metric
from .shadow_artifacts import ShadowTraceArtifactError, load_persisted_shadow_trace_envelope, persist_shadow_trace_envelope
from .shadow_window import PersistedShadowTraceEnvelope, SHA256_HEX, TRACE_QUERY_COUNT, hash_shadow_audit_trace
from .workload_monitor import MonitorStreamKey, ShadowTraceEvent, ShadowTraceEventSource


EVENT_SCHEMA_VERSION = "live-shadow-event-v1"
EVENT_ID_DOMAIN = "live-shadow-event-v1"
_RFC3339_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")
_EVENT_ID = re.compile(r"[0-9a-f]{64}\Z")
_EVENT_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "stream_key",
        "window_id",
        "window_sequence",
        "trace_sequence_index",
        "trace_id",
        "captured_at_utc",
        "envelope_path",
        "expected_trace_sha256",
    }
)
_STREAM_KEY_FIELDS = frozenset(
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


class ShadowEventSourceError(ValueError):
    """Raised when the producer/outbox cannot safely publish or deliver evidence."""


class PublicationStatus(StrEnum):
    """Explicit outcomes for background publication attempts."""

    PUBLISHED = "PUBLISHED"
    IDEMPOTENT = "IDEMPOTENT"
    DROPPED_BACKPRESSURE = "DROPPED_BACKPRESSURE"


@dataclass(frozen=True, slots=True)
class TracePublicationContext:
    """Externally assigned immutable membership for one 50-query trace."""

    stream_key: MonitorStreamKey
    window_id: int | str
    window_sequence: int
    trace_sequence_index: int
    trace_id: str
    captured_at_utc: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "window_id", _canonical_window_id(self.window_id))
        if isinstance(self.window_sequence, bool) or not isinstance(self.window_sequence, int) or self.window_sequence < 0:
            raise ValueError("window_sequence must be a non-negative integer")
        if (
            isinstance(self.trace_sequence_index, bool)
            or not isinstance(self.trace_sequence_index, int)
            or self.trace_sequence_index not in range(4)
        ):
            raise ValueError("trace_sequence_index must be one of 0, 1, 2, 3")
        object.__setattr__(self, "trace_id", _canonical_nonempty(self.trace_id, name="trace_id"))
        _parse_rfc3339_utc(self.captured_at_utc)


@dataclass(frozen=True, slots=True)
class TracePublicationReceipt:
    """Result of one publish attempt; drops never contain a deliverable event."""

    status: PublicationStatus
    event_id: str
    event: ShadowTraceEvent | None
    reason_code: str | None = None


class ShadowTracePublisher(Protocol):
    """Boundary between an external background trace worker and the outbox."""

    def publish(
        self, *, trace: object, context: TracePublicationContext
    ) -> TracePublicationReceipt: ...


def _canonical_nonempty(value: object, *, name: str) -> str:
    if not isinstance(value, str):
        raise ShadowEventSourceError(f"{name.upper()}_INVALID")
    normalized = unicodedata.normalize("NFC", value)
    if not normalized:
        raise ShadowEventSourceError(f"{name.upper()}_INVALID")
    return normalized


def _canonical_window_id(value: object) -> int | str:
    if isinstance(value, bool):
        raise ShadowEventSourceError("WINDOW_ID_INVALID")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return _canonical_nonempty(value, name="window_id")
    raise ShadowEventSourceError("WINDOW_ID_INVALID")


def _parse_rfc3339_utc(value: object) -> datetime:
    if not isinstance(value, str) or _RFC3339_UTC.fullmatch(value) is None:
        raise ShadowEventSourceError("CAPTURE_TIMESTAMP_INVALID")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ShadowEventSourceError("CAPTURE_TIMESTAMP_INVALID") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ShadowEventSourceError("CAPTURE_TIMESTAMP_INVALID")
    return parsed


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _no_duplicate_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _reject_constant(_: str) -> None:
    raise ValueError("non-finite JSON constant")


def _stream_key_document(value: MonitorStreamKey) -> dict[str, str]:
    return {
        "stream_id": value.stream_id,
        "metric": value.metric.value,
        "threshold_stratum": value.threshold_stratum,
        "configuration_identity": value.configuration_identity,
        "data_identity": value.data_identity,
        "flat_binding_id": value.flat_binding_id,
        "hnsw_binding_id": value.hnsw_binding_id,
    }


def _context_key(context: TracePublicationContext) -> dict[str, object]:
    return {
        "stream_key": _stream_key_document(context.stream_key),
        "window_id": _canonical_window_id(context.window_id),
        "window_sequence": context.window_sequence,
        "trace_sequence_index": context.trace_sequence_index,
        "trace_id": _canonical_nonempty(context.trace_id, name="trace_id"),
    }


def _event_id(context: TracePublicationContext, *, expected_trace_sha256: str) -> str:
    if not isinstance(expected_trace_sha256, str) or SHA256_HEX.fullmatch(expected_trace_sha256) is None:
        raise ShadowEventSourceError("TRACE_CHECKSUM_INVALID")
    return hashlib.sha256(
        _canonical_bytes(
            {
                "domain": EVENT_ID_DOMAIN,
                **_context_key(context),
                "expected_trace_sha256": expected_trace_sha256,
            }
        )
    ).hexdigest()


def _safe_directory(path: Path) -> None:
    if path.is_symlink():
        raise ShadowEventSourceError("OUTBOX_SYMLINK_REJECTED")
    if not path.exists():
        try:
            path.mkdir(mode=0o700, parents=True, exist_ok=False)
        except FileExistsError:
            pass
    try:
        details = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise ShadowEventSourceError("OUTBOX_UNAVAILABLE") from exc
    if not stat.S_ISDIR(details.st_mode):
        raise ShadowEventSourceError("OUTBOX_NOT_DIRECTORY")
    if details.st_uid != os.getuid() or stat.S_IMODE(details.st_mode) & 0o077:
        raise ShadowEventSourceError("OUTBOX_UNSAFE_PERMISSIONS")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _safe_regular_file(path: Path) -> None:
    if path.is_symlink():
        raise ShadowEventSourceError("OUTBOX_SYMLINK_REJECTED")
    try:
        details = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise ShadowEventSourceError("OUTBOX_LOCK_UNAVAILABLE") from exc
    if not stat.S_ISREG(details.st_mode):
        raise ShadowEventSourceError("OUTBOX_LOCK_UNSAFE")
    if details.st_uid != os.getuid() or stat.S_IMODE(details.st_mode) & 0o077:
        raise ShadowEventSourceError("OUTBOX_UNSAFE_PERMISSIONS")


def _write_new(path: Path, payload: bytes) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite immutable event artifact: {path}")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _move_durable(source: Path, target: Path) -> None:
    if target.exists():
        raise FileExistsError(f"refusing to overwrite immutable event artifact: {target}")
    os.replace(source, target)
    for directory in {source.parent, target.parent}:
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class FileShadowTraceEventSource(ShadowTraceEventSource, ShadowTracePublisher):
    """Owner-only single-host durable outbox with at-least-once delivery.

    The caller is a background trace worker. ``publish`` never derives window
    membership or performs a database operation; it only commits already-built
    immutable trace evidence and compact event metadata.
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        max_pending_events: int,
        max_pending_bytes: int,
    ) -> None:
        if isinstance(max_pending_events, bool) or not isinstance(max_pending_events, int) or max_pending_events < 1:
            raise ValueError("max_pending_events must be a positive integer")
        if isinstance(max_pending_bytes, bool) or not isinstance(max_pending_bytes, int) or max_pending_bytes < 1:
            raise ValueError("max_pending_bytes must be a positive integer")
        self.root = Path(root)
        _safe_directory(self.root)
        for directory in ("traces", "pending", "acknowledged", "rejected"):
            _safe_directory(self.root / directory)
        lock = self.root / ".lock"
        if not lock.exists():
            try:
                descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                pass
            else:
                os.close(descriptor)
                _fsync_directory(self.root)
        self.max_pending_events = max_pending_events
        self.max_pending_bytes = max_pending_bytes
        self._ensure_secure_layout()

    @property
    def _traces(self) -> Path:
        return self.root / "traces"

    @property
    def _pending(self) -> Path:
        return self.root / "pending"

    @property
    def _acknowledged(self) -> Path:
        return self.root / "acknowledged"

    @property
    def _rejected(self) -> Path:
        return self.root / "rejected"

    @contextmanager
    def _locked(self) -> Iterator[None]:
        descriptor = os.open(self.root / ".lock", os.O_RDWR)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _ensure_secure_layout(self) -> None:
        _safe_directory(self.root)
        for directory in (self._traces, self._pending, self._acknowledged, self._rejected):
            _safe_directory(directory)
        _safe_regular_file(self.root / ".lock")

    def publish(self, *, trace: object, context: TracePublicationContext) -> TracePublicationReceipt:
        self._ensure_secure_layout()
        expected_trace_sha256 = self._validate_trace(trace, context=context)
        event_id = _event_id(context, expected_trace_sha256=expected_trace_sha256)
        document = self._event_document(
            context=context,
            event_id=event_id,
            expected_trace_sha256=expected_trace_sha256,
        )
        payload = _canonical_bytes(document)
        with self._locked():
            existing = self._find_same_publication(context)
            if existing is not None:
                existing_document, existing_path = existing
                if existing_document["expected_trace_sha256"] != expected_trace_sha256:
                    raise ShadowEventSourceError("PUBLICATION_CONFLICT")
                event = self._event_from_document(existing_document, path=existing_path)
                self._validate_envelope(event, context=context)
                return TracePublicationReceipt(PublicationStatus.IDEMPOTENT, event.event_id, event)

            pending_count = len(tuple(self._pending.glob("*.json")))
            pending_bytes = sum(path.stat().st_size for path in self._pending.glob("*.json"))
            if pending_count >= self.max_pending_events or pending_bytes + len(payload) > self.max_pending_bytes:
                return TracePublicationReceipt(
                    PublicationStatus.DROPPED_BACKPRESSURE,
                    event_id,
                    None,
                    "PENDING_EVENT_CAPACITY_EXCEEDED",
                )

            event = self._event_from_document(document, path=self._pending / f"{event_id}.json")
            envelope_path = event.envelope_path
            envelope = PersistedShadowTraceEnvelope(
                trace_id=context.trace_id,
                captured_at_utc=context.captured_at_utc,
                sequence_index=context.trace_sequence_index,
                declared_observation_count=TRACE_QUERY_COUNT,
                expected_trace_sha256=expected_trace_sha256,
                trace=trace,  # validated through the canonical trace codec above
            )
            if envelope_path.exists():
                self._validate_envelope(event, context=context)
            else:
                persist_shadow_trace_envelope(envelope_path, envelope)
            self._publish_event_document(event_id=event_id, payload=payload)
            return TracePublicationReceipt(PublicationStatus.PUBLISHED, event_id, event)

    def poll(self, *, limit: int) -> tuple[ShadowTraceEvent, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        self._ensure_secure_layout()
        delivered: list[ShadowTraceEvent] = []
        with self._locked():
            for path in self._ordered_pending_paths():
                try:
                    document = self._load_event_document(path)
                    event = self._event_from_document(document, path=path)
                    self._validate_envelope(event, context=None)
                except ShadowEventSourceError as exc:
                    self._quarantine(path, str(exc))
                    continue
                delivered.append(event)
                if len(delivered) == limit:
                    break
        return tuple(delivered)

    def acknowledge(self, event_ids: tuple[str, ...]) -> None:
        if not isinstance(event_ids, tuple) or not event_ids:
            raise ValueError("event_ids must be a non-empty tuple")
        if len(set(event_ids)) != len(event_ids):
            raise ShadowEventSourceError("ACK_DUPLICATE_EVENT_ID")
        for event_id in event_ids:
            if not isinstance(event_id, str) or _EVENT_ID.fullmatch(event_id) is None:
                raise ShadowEventSourceError("ACK_EVENT_ID_INVALID")
        self._ensure_secure_layout()
        with self._locked():
            moves: list[tuple[Path, Path]] = []
            for event_id in event_ids:
                pending = self._pending / f"{event_id}.json"
                acknowledged = self._acknowledged / f"{event_id}.json"
                if acknowledged.exists():
                    event = self._event_from_document(self._load_event_document(acknowledged), path=acknowledged)
                    self._validate_envelope(event, context=None)
                    continue
                if not pending.exists():
                    raise ShadowEventSourceError("ACK_UNKNOWN_EVENT")
                event = self._event_from_document(self._load_event_document(pending), path=pending)
                self._validate_envelope(event, context=None)
                moves.append((pending, acknowledged))
            for pending, acknowledged in moves:
                _move_durable(pending, acknowledged)

    def orphaned_trace_paths(self) -> tuple[Path, ...]:
        """Return persisted traces not referenced by pending/acknowledged evidence."""

        self._ensure_secure_layout()
        with self._locked():
            referenced: set[Path] = set()
            for path in (*self._pending.glob("*.json"), *self._acknowledged.glob("*.json")):
                try:
                    event = self._event_from_document(self._load_event_document(path), path=path)
                except ShadowEventSourceError:
                    continue
                referenced.add(event.envelope_path)
            return tuple(sorted((path for path in self._traces.glob("*.json") if path not in referenced), key=str))

    def rejected_reason_codes(self) -> tuple[str, ...]:
        self._ensure_secure_layout()
        values: list[str] = []
        for path in sorted(self._rejected.glob("*.reason.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                reason = payload["reason_code"]
            except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError):
                values.append("REJECTION_LEDGER_CORRUPTED")
                continue
            if isinstance(reason, str):
                values.append(reason)
        return tuple(values)

    def _validate_trace(self, trace: object, *, context: TracePublicationContext) -> str:
        if getattr(trace, "complete", None) is not True:
            raise ShadowEventSourceError("TRACE_INCOMPLETE")
        try:
            metric = Metric(getattr(trace, "metric"))
        except (TypeError, ValueError) as exc:
            raise ShadowEventSourceError("TRACE_METRIC_INVALID") from exc
        if metric is not context.stream_key.metric:
            raise ShadowEventSourceError("TRACE_METRIC_MISMATCH")
        if getattr(trace, "threshold_stratum", None) != context.stream_key.threshold_stratum:
            raise ShadowEventSourceError("TRACE_THRESHOLD_STRATUM_MISMATCH")
        if getattr(trace, "configuration_identity", None) != context.stream_key.configuration_identity:
            raise ShadowEventSourceError("TRACE_CONFIGURATION_IDENTITY_MISMATCH")
        if getattr(trace, "data_identity", None) != context.stream_key.data_identity:
            raise ShadowEventSourceError("TRACE_DATA_IDENTITY_MISMATCH")
        if getattr(getattr(trace, "flat_identity", None), "expected_binding_id", None) != context.stream_key.flat_binding_id:
            raise ShadowEventSourceError("TRACE_FLAT_BINDING_MISMATCH")
        if getattr(getattr(trace, "hnsw_identity", None), "expected_binding_id", None) != context.stream_key.hnsw_binding_id:
            raise ShadowEventSourceError("TRACE_HNSW_BINDING_MISMATCH")
        if len(getattr(trace, "queries", ())) != TRACE_QUERY_COUNT:
            raise ShadowEventSourceError("TRACE_OBSERVATION_COUNT_INVALID")
        try:
            return hash_shadow_audit_trace(trace)
        except Exception as exc:  # canonical codec is the authority for trace validity
            raise ShadowEventSourceError("TRACE_PAYLOAD_INVALID") from exc

    def _event_document(
        self,
        *,
        context: TracePublicationContext,
        event_id: str,
        expected_trace_sha256: str,
    ) -> dict[str, object]:
        relative_envelope = f"traces/{event_id}.json"
        return {
            "schema_version": EVENT_SCHEMA_VERSION,
            "event_id": event_id,
            "stream_key": _stream_key_document(context.stream_key),
            "window_id": _canonical_window_id(context.window_id),
            "window_sequence": context.window_sequence,
            "trace_sequence_index": context.trace_sequence_index,
            "trace_id": _canonical_nonempty(context.trace_id, name="trace_id"),
            "captured_at_utc": context.captured_at_utc,
            "envelope_path": relative_envelope,
            "expected_trace_sha256": expected_trace_sha256,
        }

    def _publish_event_document(self, *, event_id: str, payload: bytes) -> None:
        _write_new(self._pending / f"{event_id}.json", payload)

    def _find_same_publication(
        self, context: TracePublicationContext
    ) -> tuple[dict[str, object], Path] | None:
        expected_key = _context_key(context)
        expected_slot = _window_slot_key(expected_key)
        for path in (*self._pending.glob("*.json"), *self._acknowledged.glob("*.json")):
            try:
                document = self._load_event_document(path)
            except ShadowEventSourceError as exc:
                if path.parent == self._pending:
                    self._quarantine(path, str(exc))
                    continue
                raise
            existing_key = _document_context_key(document)
            if _window_slot_key(existing_key) == expected_slot:
                if existing_key == expected_key:
                    return document, path
                raise ShadowEventSourceError("WINDOW_SLOT_CONFLICT")
        return None

    def _ordered_pending_paths(self) -> tuple[Path, ...]:
        ordered: list[tuple[str, int, int, str, Path]] = []
        for path in self._pending.glob("*.json"):
            try:
                document = self._load_event_document(path)
                key = _document_context_key(document)
                stream = key["stream_key"]
                assert isinstance(stream, dict)
                ordered.append(
                    (
                        str(stream["stream_id"]),
                        int(key["window_sequence"]),
                        int(key["trace_sequence_index"]),
                        str(document["event_id"]),
                        path,
                    )
                )
            except (ShadowEventSourceError, AssertionError, KeyError, TypeError, ValueError) as exc:
                self._quarantine(path, str(exc) if isinstance(exc, ShadowEventSourceError) else "EVENT_MALFORMED")
        return tuple(item[-1] for item in sorted(ordered))

    def _load_event_document(self, path: Path) -> dict[str, object]:
        if path.is_symlink():
            raise ShadowEventSourceError("EVENT_SYMLINK_REJECTED")
        try:
            value = json.loads(
                path.read_text(encoding="utf-8"),
                object_pairs_hook=_no_duplicate_object,
                parse_constant=_reject_constant,
            )
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ShadowEventSourceError("EVENT_MALFORMED") from exc
        if not isinstance(value, dict) or frozenset(value) != _EVENT_FIELDS:
            raise ShadowEventSourceError("EVENT_SCHEMA_MISMATCH")
        if value.get("schema_version") != EVENT_SCHEMA_VERSION:
            raise ShadowEventSourceError("EVENT_SCHEMA_MISMATCH")
        event_id = value.get("event_id")
        if not isinstance(event_id, str) or _EVENT_ID.fullmatch(event_id) is None or path.name != f"{event_id}.json":
            raise ShadowEventSourceError("EVENT_ID_INVALID")
        _document_context_key(value)
        relative = value.get("envelope_path")
        if not isinstance(relative, str) or Path(relative).is_absolute() or Path(relative).parts != ("traces", f"{event_id}.json"):
            raise ShadowEventSourceError("EVENT_ENVELOPE_PATH_INVALID")
        expected = value.get("expected_trace_sha256")
        if not isinstance(expected, str) or SHA256_HEX.fullmatch(expected) is None:
            raise ShadowEventSourceError("TRACE_CHECKSUM_INVALID")
        expected_id = _event_id(_context_from_document(value), expected_trace_sha256=expected)
        if event_id != expected_id:
            raise ShadowEventSourceError("EVENT_ID_CHECKSUM_MISMATCH")
        return value

    def _event_from_document(self, document: dict[str, object], *, path: Path) -> ShadowTraceEvent:
        context = _context_from_document(document)
        relative = document["envelope_path"]
        assert isinstance(relative, str)
        return ShadowTraceEvent(
            event_id=document["event_id"],  # validated by _load_event_document
            stream_key=context.stream_key,
            window_id=context.window_id,
            window_sequence=context.window_sequence,
            envelope_path=self.root / relative,
            expected_trace_sha256=document["expected_trace_sha256"],  # validated above
        )

    def _validate_envelope(
        self, event: ShadowTraceEvent, *, context: TracePublicationContext | None
    ) -> None:
        if event.envelope_path.is_symlink():
            raise ShadowEventSourceError("ENVELOPE_SYMLINK_REJECTED")
        try:
            envelope = load_persisted_shadow_trace_envelope(event.envelope_path)
        except (OSError, ShadowTraceArtifactError) as exc:
            raise ShadowEventSourceError("ENVELOPE_INVALID") from exc
        if envelope.expected_trace_sha256 != event.expected_trace_sha256:
            raise ShadowEventSourceError("ENVELOPE_CHECKSUM_MISMATCH")
        if context is not None:
            if (
                envelope.trace_id != context.trace_id
                or envelope.captured_at_utc != context.captured_at_utc
                or envelope.sequence_index != context.trace_sequence_index
                or envelope.declared_observation_count != TRACE_QUERY_COUNT
            ):
                raise ShadowEventSourceError("ENVELOPE_CONTEXT_MISMATCH")

    def _quarantine(self, pending_path: Path, reason_code: str) -> None:
        if not pending_path.exists():
            return
        filename = pending_path.name
        target = self._rejected / filename
        if target.exists():
            raise ShadowEventSourceError("REJECTION_CONFLICT")
        _move_durable(pending_path, target)
        _write_new(
            self._rejected / f"{filename[:-5]}.reason.json",
            _canonical_bytes({"schema_version": "shadow-event-rejection-v1", "reason_code": reason_code}),
        )


def _document_context_key(document: dict[str, object]) -> dict[str, object]:
    stream = document.get("stream_key")
    if not isinstance(stream, dict) or frozenset(stream) != _STREAM_KEY_FIELDS:
        raise ShadowEventSourceError("EVENT_STREAM_KEY_INVALID")
    try:
        key = MonitorStreamKey(
            stream_id=_canonical_nonempty(stream["stream_id"], name="stream_id"),
            metric=Metric(_canonical_nonempty(stream["metric"], name="metric")),
            threshold_stratum=_canonical_nonempty(stream["threshold_stratum"], name="threshold_stratum"),
            configuration_identity=_canonical_nonempty(stream["configuration_identity"], name="configuration_identity"),
            data_identity=_canonical_nonempty(stream["data_identity"], name="data_identity"),
            flat_binding_id=_canonical_nonempty(stream["flat_binding_id"], name="flat_binding_id"),
            hnsw_binding_id=_canonical_nonempty(stream["hnsw_binding_id"], name="hnsw_binding_id"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ShadowEventSourceError("EVENT_STREAM_KEY_INVALID") from exc
    window_id = _canonical_window_id(document.get("window_id"))
    window_sequence = document.get("window_sequence")
    trace_sequence_index = document.get("trace_sequence_index")
    if isinstance(window_sequence, bool) or not isinstance(window_sequence, int) or window_sequence < 0:
        raise ShadowEventSourceError("EVENT_WINDOW_SEQUENCE_INVALID")
    if isinstance(trace_sequence_index, bool) or not isinstance(trace_sequence_index, int) or trace_sequence_index not in range(4):
        raise ShadowEventSourceError("EVENT_TRACE_SEQUENCE_INVALID")
    _canonical_nonempty(document.get("trace_id"), name="trace_id")
    _parse_rfc3339_utc(document.get("captured_at_utc"))
    return {
        "stream_key": _stream_key_document(key),
        "window_id": window_id,
        "window_sequence": window_sequence,
        "trace_sequence_index": trace_sequence_index,
        "trace_id": document["trace_id"],
    }


def _window_slot_key(context_key: dict[str, object]) -> tuple[object, ...]:
    stream = context_key["stream_key"]
    assert isinstance(stream, dict)
    return (
        tuple(sorted(stream.items())),
        context_key["window_id"],
        context_key["window_sequence"],
        context_key["trace_sequence_index"],
    )


def _context_from_document(document: dict[str, object]) -> TracePublicationContext:
    key = _document_context_key(document)
    stream = key["stream_key"]
    assert isinstance(stream, dict)
    return TracePublicationContext(
        stream_key=MonitorStreamKey(
            stream_id=stream["stream_id"],
            metric=Metric(stream["metric"]),
            threshold_stratum=stream["threshold_stratum"],
            configuration_identity=stream["configuration_identity"],
            data_identity=stream["data_identity"],
            flat_binding_id=stream["flat_binding_id"],
            hnsw_binding_id=stream["hnsw_binding_id"],
        ),
        window_id=key["window_id"],
        window_sequence=key["window_sequence"],
        trace_sequence_index=key["trace_sequence_index"],
        trace_id=key["trace_id"],
        captured_at_utc=document["captured_at_utc"],
    )


__all__ = [
    "EVENT_ID_DOMAIN",
    "EVENT_SCHEMA_VERSION",
    "FileShadowTraceEventSource",
    "PublicationStatus",
    "ShadowEventSourceError",
    "ShadowTracePublisher",
    "TracePublicationContext",
    "TracePublicationReceipt",
]
