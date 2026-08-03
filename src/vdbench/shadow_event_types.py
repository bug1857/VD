"""Dependency-neutral stream and event value types shared by ADR-005–007.

These types deliberately live below the detector, policy, monitor, outbox, and
host-observation layers.  Moving them here prevents a foreground host hook or
durable outbox from importing detector/policy code merely to name a stream.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
import re
from typing import Protocol
import unicodedata

from .config import Metric


__all__ = [
    "MonitorStreamKey",
    "PublicationStatus",
    "ShadowEventSourceError",
    "ShadowTraceEvent",
    "ShadowTraceEventSource",
    "ShadowTracePublisher",
    "TracePublicationContext",
    "TracePublicationReceipt",
]


_RFC3339_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")


class ShadowEventSourceError(ValueError):
    """Raised when evidence publication context or durable outbox fails closed."""


class PublicationStatus(StrEnum):
    """Explicit outcomes for a background trace publication attempt."""

    PUBLISHED = "PUBLISHED"
    IDEMPOTENT = "IDEMPOTENT"
    DROPPED_BACKPRESSURE = "DROPPED_BACKPRESSURE"


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


@dataclass(frozen=True, slots=True)
class MonitorStreamKey:
    """Stable stream lineage plus the exact identity snapshot it may use."""

    stream_id: str
    metric: Metric
    threshold_stratum: str
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.metric, Metric):
            raise TypeError("metric must be a Metric")
        for field in (
            "stream_id",
            "threshold_stratum",
            "configuration_identity",
            "data_identity",
            "flat_binding_id",
            "hnsw_binding_id",
        ):
            if not isinstance(getattr(self, field), str) or not getattr(self, field):
                raise ValueError(f"{field} must be a non-empty string")


@dataclass(frozen=True, slots=True)
class ShadowTraceEvent:
    """Reference to one already-persisted immutable trace envelope."""

    event_id: str
    stream_key: MonitorStreamKey
    window_id: int | str
    window_sequence: int
    envelope_path: Path
    expected_trace_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("event_id must be a non-empty string")
        if isinstance(self.window_sequence, bool) or not isinstance(self.window_sequence, int):
            raise ValueError("window_sequence must be an integer")
        if self.window_sequence < 0:
            raise ValueError("window_sequence must be non-negative")
        if isinstance(self.window_id, bool) or not isinstance(self.window_id, (int, str)):
            raise ValueError("window_id must be an integer or string")
        if isinstance(self.window_id, str) and not self.window_id:
            raise ValueError("window_id must be non-empty")
        if not isinstance(self.envelope_path, Path):
            raise TypeError("envelope_path must be a Path")
        if not _is_sha256(self.expected_trace_sha256):
            raise ValueError("expected_trace_sha256 must be lowercase SHA-256")


class ShadowTraceEventSource(Protocol):
    """At-least-once event source with explicit idempotent acknowledgement."""

    def poll(self, *, limit: int) -> tuple[ShadowTraceEvent, ...]: ...

    def acknowledge(self, event_ids: tuple[str, ...]) -> None: ...


@dataclass(frozen=True, slots=True)
class TracePublicationContext:
    """Externally assigned immutable membership for one complete trace."""

    stream_key: MonitorStreamKey
    window_id: int | str
    window_sequence: int
    trace_sequence_index: int
    trace_id: str
    captured_at_utc: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "window_id", _canonical_window_id(self.window_id))
        if isinstance(self.window_sequence, bool) or not isinstance(self.window_sequence, int) or self.window_sequence < 0:
            raise ShadowEventSourceError("WINDOW_SEQUENCE_INVALID")
        if isinstance(self.trace_sequence_index, bool) or not isinstance(self.trace_sequence_index, int) or self.trace_sequence_index not in range(4):
            raise ShadowEventSourceError("TRACE_SEQUENCE_INDEX_INVALID")
        object.__setattr__(self, "trace_id", _canonical_nonempty(self.trace_id, name="trace_id"))
        _parse_rfc3339_utc(self.captured_at_utc)


@dataclass(frozen=True, slots=True)
class TracePublicationReceipt:
    """Result of one publish attempt; a drop never contains a deliverable event."""

    status: PublicationStatus
    event_id: str
    event: ShadowTraceEvent | None
    reason_code: str | None = None


class ShadowTracePublisher(Protocol):
    """Boundary implemented by an ADR-006 durable trace publisher."""

    def publish(self, *, trace: object, context: TracePublicationContext) -> TracePublicationReceipt: ...


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
