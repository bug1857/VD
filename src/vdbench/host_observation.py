"""ADR-007 framework-neutral post-response observation and shadow worker.

Purpose:
    Keep an application's served range-query path independent from monitoring,
    then batch compatible completed observations into read-only shadow traces.
Inputs:
    Immutable post-response observations, an injected background trace executor,
    an ADR-006 trace publisher, and a restart-durable metadata-only state store.
Outputs:
    Explicit foreground receipts and worker-cycle outcomes.  The worker publishes
    only validated, complete 50-query traces.
Dependencies:
    The shared stream identity and ADR-006 publisher value objects.  This module
    never imports PyMilvus, the detector, policy, safe-actuation boundary, or
    automatic-action control code.
Complexity:
    ``offer`` is O(1) bounded in-memory queue insertion.  ``run_once`` is O(n)
    for drained observations plus O(50) validation per completed trace.
Failure modes:
    Invalid, stale, incompatible, incomplete, failed, or ambiguously published
    evidence is rejected or blocks its stream.  No failure changes the served
    query outcome or fabricates a trace.
Extension points:
    A production host may call ``ReferenceRangeGateway`` or call ``offer`` in
    its own post-response hook.  A later read-only Milvus executor implements
    ``ShadowAuditExecutor`` without changing this boundary.
"""

from __future__ import annotations

import json
import math
import os
import queue
import re
import stat
import tempfile
import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol

from .config import Metric
from .shadow_event_types import (
    MonitorStreamKey,
    PublicationStatus,
    ShadowTracePublisher,
    TracePublicationContext,
    TracePublicationReceipt,
)

__all__ = [
    "BackgroundShadowWorker",
    "BoundedHostObservationRecorder",
    "CompletedRangeQueryObservation",
    "FileHostWorkerStateStore",
    "GatewayExecutionResult",
    "HostObservationRecorder",
    "HostWorkerState",
    "HostWorkerStateError",
    "HostWorkerStateStore",
    "InMemoryHostWorkerStateStore",
    "ObservationReceipt",
    "ObservationStatus",
    "RangeQueryRequest",
    "RangeServingExecutor",
    "ReferenceRangeGateway",
    "RegisteredTraceParameters",
    "ServedQueryOutcome",
    "ShadowAuditExecutor",
    "StreamWorkerState",
    "WorkerCycleResult",
]


_STATE_SCHEMA_VERSION = "host-worker-state-v1"
_STATE_FIELDS = frozenset({"schema_version", "streams"})
_STREAM_STATE_FIELDS = frozenset(
    {
        "stream_key",
        "next_trace_ordinal",
        "partial_observation_count",
        "inflight_observation_count",
        "restart_loss_count",
        "rejected_observation_count",
        "blocked_reason_code",
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
_RFC3339_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")
_TRACE_SIZE = 50


class HostWorkerStateError(ValueError):
    """Raised when durable worker metadata cannot be safely trusted."""


class ObservationStatus(StrEnum):
    """Explicit, non-sensitive outcome of a post-response recorder offer."""

    ACCEPTED = "ACCEPTED"
    DROPPED_BACKPRESSURE = "DROPPED_BACKPRESSURE"
    REJECTED_INVALID = "REJECTED_INVALID"
    CLOSED = "CLOSED"


@dataclass(frozen=True, slots=True)
class ObservationReceipt:
    """Constant-size result of one recorder offer; it contains no raw payload."""

    status: ObservationStatus
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class ServedQueryOutcome:
    """Minimal immutable summary of the host's already-completed served query."""

    success: bool
    timed_out: bool
    result_count: int
    latency_ms: float
    error_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.success, bool) or not isinstance(self.timed_out, bool):
            raise TypeError("success and timed_out must be bool")
        if isinstance(self.result_count, bool) or not isinstance(self.result_count, int) or self.result_count < 0:
            raise ValueError("result_count must be a non-negative integer")
        if not isinstance(self.latency_ms, (int, float)) or isinstance(self.latency_ms, bool) or not math.isfinite(float(self.latency_ms)) or float(self.latency_ms) < 0.0:
            raise ValueError("latency_ms must be a finite non-negative number")
        object.__setattr__(self, "latency_ms", float(self.latency_ms))
        if self.error_code is not None:
            object.__setattr__(self, "error_code", _canonical_nonempty(self.error_code, name="error_code"))


@dataclass(frozen=True, slots=True)
class CompletedRangeQueryObservation:
    """Immutable post-response input retained only in volatile worker memory."""

    request_id: int | str
    captured_at_utc: str
    stream_key: MonitorStreamKey
    query_vector: tuple[float, ...]
    threshold_radius: float
    range_filter: float
    limit: int
    served_ef: int
    served_outcome: ServedQueryOutcome

    def __post_init__(self) -> None:
        request_id = _canonical_request_id(self.request_id)
        object.__setattr__(self, "request_id", request_id)
        _parse_rfc3339_utc(self.captured_at_utc)
        if not isinstance(self.stream_key, MonitorStreamKey):
            raise TypeError("stream_key must be a MonitorStreamKey")
        vector = tuple(float(value) for value in self.query_vector)
        if not vector or not all(math.isfinite(value) for value in vector):
            raise ValueError("query_vector must be a non-empty finite vector")
        object.__setattr__(self, "query_vector", vector)
        for field in ("threshold_radius", "range_filter"):
            value = getattr(self, field)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(float(value)):
                raise ValueError(f"{field} must be finite")
            object.__setattr__(self, field, float(value))
        if isinstance(self.limit, bool) or not isinstance(self.limit, int) or self.limit <= 0:
            raise ValueError("limit must be a positive integer")
        if isinstance(self.served_ef, bool) or not isinstance(self.served_ef, int) or self.served_ef <= 0:
            raise ValueError("served_ef must be a positive integer")
        if not isinstance(self.served_outcome, ServedQueryOutcome):
            raise TypeError("served_outcome must be a ServedQueryOutcome")


class HostObservationRecorder(Protocol):
    """Foreground-safe protocol implemented with non-blocking bounded enqueue."""

    def offer(self, observation: CompletedRangeQueryObservation) -> ObservationReceipt: ...


class BoundedHostObservationRecorder:
    """Bounded, in-memory, post-response recorder with no I/O dependencies."""

    def __init__(self, *, max_pending_observations: int) -> None:
        if isinstance(max_pending_observations, bool) or not isinstance(max_pending_observations, int) or max_pending_observations <= 0:
            raise ValueError("max_pending_observations must be a positive integer")
        self._queue: queue.Queue[CompletedRangeQueryObservation] = queue.Queue(maxsize=max_pending_observations)
        self._closed = False

    @property
    def pending_count(self) -> int:
        """Approximate in-memory queue depth for host-local telemetry only."""

        return self._queue.qsize()

    def close(self) -> None:
        """Prevent future monitoring offers without affecting served queries."""

        self._closed = True

    def offer(self, observation: CompletedRangeQueryObservation) -> ObservationReceipt:
        """Try one constant-time enqueue; never wait, persist, or call dependencies."""

        if self._closed:
            return ObservationReceipt(ObservationStatus.CLOSED, "RECORDER_CLOSED")
        if not isinstance(observation, CompletedRangeQueryObservation):
            return ObservationReceipt(ObservationStatus.REJECTED_INVALID, "OBSERVATION_INVALID")
        try:
            self._queue.put_nowait(observation)
        except queue.Full:
            return ObservationReceipt(
                ObservationStatus.DROPPED_BACKPRESSURE,
                "PENDING_OBSERVATION_CAPACITY_EXCEEDED",
            )
        return ObservationReceipt(ObservationStatus.ACCEPTED)

    def drain(self, *, limit: int) -> tuple[CompletedRangeQueryObservation, ...]:
        """Worker-only, non-blocking removal of at most ``limit`` observations."""

        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        values: list[CompletedRangeQueryObservation] = []
        for _ in range(limit):
            try:
                values.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return tuple(values)


@dataclass(frozen=True, slots=True)
class RangeQueryRequest:
    """Reference-gateway input before its injected serving executor is called."""

    request_id: int | str
    stream_key: MonitorStreamKey
    query_vector: tuple[float, ...]
    threshold_radius: float
    range_filter: float
    limit: int
    served_ef: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_id", _canonical_request_id(self.request_id))
        if not isinstance(self.stream_key, MonitorStreamKey):
            raise TypeError("stream_key must be a MonitorStreamKey")
        vector = tuple(float(value) for value in self.query_vector)
        if not vector or not all(math.isfinite(value) for value in vector):
            raise ValueError("query_vector must be a non-empty finite vector")
        object.__setattr__(self, "query_vector", vector)
        for field in ("threshold_radius", "range_filter"):
            value = getattr(self, field)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(float(value)):
                raise ValueError(f"{field} must be finite")
            object.__setattr__(self, field, float(value))
        if isinstance(self.limit, bool) or not isinstance(self.limit, int) or self.limit <= 0:
            raise ValueError("limit must be a positive integer")
        if isinstance(self.served_ef, bool) or not isinstance(self.served_ef, int) or self.served_ef <= 0:
            raise ValueError("served_ef must be a positive integer")


class RangeServingExecutor(Protocol):
    """Host-owned served-query path; this boundary never specifies a database."""

    def execute(self, request: RangeQueryRequest) -> ServedQueryOutcome: ...


@dataclass(frozen=True, slots=True)
class GatewayExecutionResult:
    """Served result plus independent best-effort monitoring receipt."""

    served_outcome: ServedQueryOutcome
    observation_receipt: ObservationReceipt


class ReferenceRangeGateway:
    """Reference in-process post-response hook for EXP-008, not a web server."""

    def __init__(
        self,
        *,
        serving_executor: RangeServingExecutor,
        recorder: HostObservationRecorder,
        clock: Callable[[], str],
    ) -> None:
        self._serving_executor = serving_executor
        self._recorder = recorder
        self._clock = clock

    def execute(self, request: RangeQueryRequest) -> GatewayExecutionResult:
        """Serve first, then best-effort enqueue without changing the served result."""

        served_outcome = self._serving_executor.execute(request)
        if not isinstance(served_outcome, ServedQueryOutcome):
            raise TypeError("serving_executor must return ServedQueryOutcome")
        try:
            observation = CompletedRangeQueryObservation(
                request_id=request.request_id,
                captured_at_utc=self._clock(),
                stream_key=request.stream_key,
                query_vector=request.query_vector,
                threshold_radius=request.threshold_radius,
                range_filter=request.range_filter,
                limit=request.limit,
                served_ef=request.served_ef,
                served_outcome=served_outcome,
            )
        except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
            receipt = ObservationReceipt(
                ObservationStatus.REJECTED_INVALID,
                "OBSERVATION_CAPTURE_FAILED",
            )
        else:
            try:
                receipt = self._recorder.offer(observation)
            except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
                receipt = ObservationReceipt(
                    ObservationStatus.REJECTED_INVALID,
                    "RECORDER_FAILED",
                )
        return GatewayExecutionResult(served_outcome=served_outcome, observation_receipt=receipt)


class ShadowAuditExecutor(Protocol):
    """Read-only background executor returning the existing structural trace object."""

    def capture(self, observations: tuple[CompletedRangeQueryObservation, ...]) -> object: ...


@dataclass(frozen=True, slots=True)
class RegisteredTraceParameters:
    """Explicitly injected query-time values registered for captured evidence."""

    allowed_candidate_and_lkg_efs: frozenset[int]
    sentinel_ef: int

    def __post_init__(self) -> None:
        values = frozenset(self.allowed_candidate_and_lkg_efs)
        if not values or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in values):
            raise ValueError("allowed_candidate_and_lkg_efs must contain positive integers")
        if isinstance(self.sentinel_ef, bool) or not isinstance(self.sentinel_ef, int) or self.sentinel_ef <= 0:
            raise ValueError("sentinel_ef must be a positive integer")
        object.__setattr__(self, "allowed_candidate_and_lkg_efs", values)


@dataclass(frozen=True, slots=True)
class StreamWorkerState:
    """Persisted non-sensitive scheduling metadata for one exact stream lineage."""

    stream_key: MonitorStreamKey
    next_trace_ordinal: int = 0
    partial_observation_count: int = 0
    inflight_observation_count: int = 0
    restart_loss_count: int = 0
    rejected_observation_count: int = 0
    blocked_reason_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.stream_key, MonitorStreamKey):
            raise TypeError("stream_key must be a MonitorStreamKey")
        for field in (
            "next_trace_ordinal",
            "partial_observation_count",
            "inflight_observation_count",
            "restart_loss_count",
            "rejected_observation_count",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field} must be a non-negative integer")
        if self.blocked_reason_code is not None:
            object.__setattr__(
                self,
                "blocked_reason_code",
                _canonical_nonempty(self.blocked_reason_code, name="blocked_reason_code"),
            )


@dataclass(frozen=True, slots=True)
class HostWorkerState:
    """Strict metadata-only state; raw observations never enter this object on disk."""

    streams: Mapping[MonitorStreamKey, StreamWorkerState]

    def __post_init__(self) -> None:
        normalized = dict(self.streams)
        for key, value in normalized.items():
            if not isinstance(key, MonitorStreamKey) or not isinstance(value, StreamWorkerState):
                raise TypeError("streams must map MonitorStreamKey to StreamWorkerState")
            if key != value.stream_key:
                raise ValueError("stream state key must match its stream_key")
        object.__setattr__(self, "streams", MappingProxyType(normalized))


class HostWorkerStateStore(Protocol):
    """Atomic metadata-only store; ``recover`` accounts for volatile restart loss."""

    def recover(self) -> HostWorkerState: ...

    def save(self, state: HostWorkerState) -> None: ...


class InMemoryHostWorkerStateStore:
    """Test/store implementation with the same restart-loss semantics as file state."""

    def __init__(self, initial_state: HostWorkerState | None = None) -> None:
        self._state = initial_state or HostWorkerState(streams={})

    def save(self, state: HostWorkerState) -> None:
        self._state = HostWorkerState(streams=state.streams)

    def snapshot(self) -> HostWorkerState:
        return HostWorkerState(streams=self._state.streams)

    def recover(self) -> HostWorkerState:
        recovered = _recover_state(self._state)
        self._state = recovered
        return HostWorkerState(streams=recovered.streams)


class FileHostWorkerStateStore:
    """Strict atomic JSON state with fsync and no raw query/vector persistence."""

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self._directory = Path(directory)
        self._ensure_directory()

    @property
    def _path(self) -> Path:
        return self._directory / "host-worker-state.json"

    def _ensure_directory(self) -> None:
        if self._directory.is_symlink():
            raise HostWorkerStateError("HOST_WORKER_STATE_SYMLINK_REJECTED")
        self._directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        details = self._directory.stat(follow_symlinks=False)
        if not stat.S_ISDIR(details.st_mode) or details.st_uid != os.getuid() or stat.S_IMODE(details.st_mode) & 0o077:
            raise HostWorkerStateError("HOST_WORKER_STATE_UNSAFE_DIRECTORY")

    def save(self, state: HostWorkerState) -> None:
        self._ensure_directory()
        if self._path.is_symlink():
            raise HostWorkerStateError("HOST_WORKER_STATE_SYMLINK_REJECTED")
        payload = _canonical_json(_state_document(state))
        descriptor, name = tempfile.mkstemp(prefix=".host-worker-state.", suffix=".tmp", dir=self._directory)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                descriptor = -1
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._path)
            directory_descriptor = os.open(self._directory, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError as exc:
            raise HostWorkerStateError("HOST_WORKER_STATE_WRITE_FAILED") from exc
        finally:
            if descriptor != -1:
                os.close(descriptor)
            if temporary.exists():
                temporary.unlink()

    def snapshot(self) -> HostWorkerState:
        self._ensure_directory()
        if self._path.is_symlink():
            raise HostWorkerStateError("HOST_WORKER_STATE_SYMLINK_REJECTED")
        if not self._path.exists():
            return HostWorkerState(streams={})
        try:
            details = self._path.stat(follow_symlinks=False)
        except OSError as exc:
            raise HostWorkerStateError("HOST_WORKER_STATE_UNAVAILABLE") from exc
        if not stat.S_ISREG(details.st_mode) or details.st_uid != os.getuid() or stat.S_IMODE(details.st_mode) & 0o077:
            raise HostWorkerStateError("HOST_WORKER_STATE_UNSAFE_FILE")
        try:
            document = json.loads(self._path.read_text(encoding="utf-8"), object_pairs_hook=_no_duplicate_object)
            return _state_from_document(document)
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            if isinstance(exc, HostWorkerStateError):
                raise
            raise HostWorkerStateError("HOST_WORKER_STATE_CORRUPTED") from exc

    def recover(self) -> HostWorkerState:
        state = self.snapshot()
        recovered = _recover_state(state)
        if recovered != state:
            self.save(recovered)
        return recovered


@dataclass(frozen=True, slots=True)
class WorkerCycleResult:
    """Non-sensitive summary of one explicit background worker execution."""

    drained_observation_count: int
    captured_trace_count: int
    published_trace_count: int
    rejected_observation_count: int
    blocked_stream_count: int
    reason_codes: tuple[str, ...] = ()


class BackgroundShadowWorker:
    """Owns volatile batches; captures and publishes only complete compatible traces."""

    def __init__(
        self,
        *,
        recorder: BoundedHostObservationRecorder,
        executor: ShadowAuditExecutor,
        publisher: ShadowTracePublisher,
        state_store: HostWorkerStateStore,
        registered_trace_parameters: RegisteredTraceParameters,
        max_partial_streams: int,
        max_observation_age_seconds: float,
        clock: Callable[[], str],
    ) -> None:
        if not isinstance(recorder, BoundedHostObservationRecorder):
            raise TypeError("recorder must be a BoundedHostObservationRecorder")
        if isinstance(max_partial_streams, bool) or not isinstance(max_partial_streams, int) or max_partial_streams <= 0:
            raise ValueError("max_partial_streams must be a positive integer")
        if not isinstance(max_observation_age_seconds, (int, float)) or isinstance(max_observation_age_seconds, bool) or not math.isfinite(float(max_observation_age_seconds)) or float(max_observation_age_seconds) <= 0.0:
            raise ValueError("max_observation_age_seconds must be finite and positive")
        self._recorder = recorder
        self._executor = executor
        self._publisher = publisher
        self._state_store = state_store
        if not isinstance(registered_trace_parameters, RegisteredTraceParameters):
            raise TypeError("registered_trace_parameters must be RegisteredTraceParameters")
        self._registered_trace_parameters = registered_trace_parameters
        self._max_partial_streams = max_partial_streams
        self._max_observation_age_seconds = float(max_observation_age_seconds)
        self._clock = clock
        self._state = state_store.recover()
        self._partials: dict[MonitorStreamKey, list[CompletedRangeQueryObservation]] = {}
        self._stream_order: list[MonitorStreamKey] = []

    def run_once(self, *, max_observations: int) -> WorkerCycleResult:
        """Drain bounded input, then capture/publish complete groups without retries."""

        if isinstance(max_observations, bool) or not isinstance(max_observations, int) or max_observations <= 0:
            raise ValueError("max_observations must be a positive integer")
        now = _parse_rfc3339_utc(self._clock())
        drained = self._recorder.drain(limit=max_observations)
        reasons: list[str] = []
        rejected = 0
        for observation in drained:
            reason = self._buffer_observation(observation, now=now)
            if reason is not None:
                rejected += 1
                _append_reason(reasons, reason)

        captured = 0
        published = 0
        for stream_key in tuple(self._stream_order):
            while len(self._partials.get(stream_key, ())) >= _TRACE_SIZE:
                state = self._stream_state(stream_key)
                if state.blocked_reason_code is not None:
                    break
                observations = tuple(self._partials[stream_key][:_TRACE_SIZE])
                del self._partials[stream_key][:_TRACE_SIZE]
                self._set_stream_state(
                    replace(
                        state,
                        partial_observation_count=len(self._partials[stream_key]),
                        inflight_observation_count=_TRACE_SIZE,
                    )
                )
                trace, capture_reason = self._capture_and_validate(observations)
                if capture_reason is not None:
                    rejected += _TRACE_SIZE
                    _append_reason(reasons, capture_reason)
                    current = self._stream_state(stream_key)
                    self._set_stream_state(
                        replace(
                            current,
                            inflight_observation_count=0,
                            rejected_observation_count=current.rejected_observation_count + _TRACE_SIZE,
                        )
                    )
                    continue
                assert trace is not None
                captured += 1
                publication_reason, was_published = self._publish_trace(
                    stream_key=stream_key,
                    trace=trace,
                )
                if was_published:
                    published += 1
                if publication_reason is not None:
                    rejected += _TRACE_SIZE
                    _append_reason(reasons, publication_reason)
                if self._stream_state(stream_key).blocked_reason_code is not None:
                    break
            if not self._partials.get(stream_key):
                self._partials.pop(stream_key, None)
                if stream_key in self._stream_order:
                    self._stream_order.remove(stream_key)

        return WorkerCycleResult(
            drained_observation_count=len(drained),
            captured_trace_count=captured,
            published_trace_count=published,
            rejected_observation_count=rejected,
            blocked_stream_count=sum(
                1 for state in self._state.streams.values() if state.blocked_reason_code is not None
            ),
            reason_codes=tuple(reasons),
        )

    def _buffer_observation(
        self,
        observation: CompletedRangeQueryObservation,
        *,
        now: datetime,
    ) -> str | None:
        state = self._stream_state(observation.stream_key)
        if state.blocked_reason_code is not None:
            return "STREAM_BLOCKED"
        age = (now - _parse_rfc3339_utc(observation.captured_at_utc)).total_seconds()
        if age > self._max_observation_age_seconds:
            self._set_stream_state(
                replace(state, rejected_observation_count=state.rejected_observation_count + 1)
            )
            return "OBSERVATION_STALE"
        if not observation.served_outcome.success or observation.served_outcome.timed_out:
            self._set_stream_state(
                replace(state, rejected_observation_count=state.rejected_observation_count + 1)
            )
            return "SERVED_QUERY_UNSUITABLE"
        if observation.stream_key not in self._partials:
            if len(self._partials) >= self._max_partial_streams:
                self._set_stream_state(
                    replace(state, rejected_observation_count=state.rejected_observation_count + 1)
                )
                return "PARTIAL_STREAM_CAPACITY_EXCEEDED"
            self._partials[observation.stream_key] = []
            self._stream_order.append(observation.stream_key)
        self._partials[observation.stream_key].append(observation)
        self._set_stream_state(
            replace(
                state,
                partial_observation_count=len(self._partials[observation.stream_key]),
            )
        )
        return None

    def _capture_and_validate(
        self, observations: tuple[CompletedRangeQueryObservation, ...]
    ) -> tuple[object | None, str | None]:
        try:
            trace = self._executor.capture(observations)
        except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
            return None, "EXECUTOR_CAPTURE_FAILED"
        try:
            reason = _validate_trace(
                trace,
                observations,
                registered_trace_parameters=self._registered_trace_parameters,
            )
        except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
            return None, "TRACE_VALIDATION_FAILED"
        return (trace, None) if reason is None else (None, reason)

    def _publish_trace(self, *, stream_key: MonitorStreamKey, trace: object) -> tuple[str | None, bool]:
        state = self._stream_state(stream_key)
        ordinal = state.next_trace_ordinal
        window_sequence, trace_sequence_index = divmod(ordinal, 4)
        try:
            context = TracePublicationContext(
                stream_key=stream_key,
                window_id=f"{stream_key.stream_id}:window:{window_sequence}",
                window_sequence=window_sequence,
                trace_sequence_index=trace_sequence_index,
                trace_id=f"{stream_key.stream_id}:trace:{ordinal}",
                captured_at_utc=self._clock(),
            )
            receipt = self._publisher.publish(trace=trace, context=context)
        except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
            self._set_stream_state(
                replace(
                    state,
                    inflight_observation_count=0,
                    rejected_observation_count=state.rejected_observation_count + _TRACE_SIZE,
                    blocked_reason_code="PUBLISH_OUTCOME_UNKNOWN",
                )
            )
            return "PUBLISH_OUTCOME_UNKNOWN", False
        if not isinstance(receipt, TracePublicationReceipt):
            self._set_stream_state(
                replace(
                    state,
                    inflight_observation_count=0,
                    rejected_observation_count=state.rejected_observation_count + _TRACE_SIZE,
                    blocked_reason_code="PUBLISH_OUTCOME_UNKNOWN",
                )
            )
            return "PUBLISH_OUTCOME_UNKNOWN", False
        if receipt.status in (PublicationStatus.PUBLISHED, PublicationStatus.IDEMPOTENT):
            self._set_stream_state(
                replace(
                    state,
                    next_trace_ordinal=ordinal + 1,
                    inflight_observation_count=0,
                )
            )
            return None, True
        if receipt.status is PublicationStatus.DROPPED_BACKPRESSURE:
            self._set_stream_state(
                replace(
                    state,
                    inflight_observation_count=0,
                    rejected_observation_count=state.rejected_observation_count + _TRACE_SIZE,
                )
            )
            return receipt.reason_code or "PUBLISH_DROPPED_BACKPRESSURE", False
        self._set_stream_state(
            replace(
                state,
                inflight_observation_count=0,
                rejected_observation_count=state.rejected_observation_count + _TRACE_SIZE,
                blocked_reason_code="PUBLISH_OUTCOME_UNKNOWN",
            )
        )
        return "PUBLISH_OUTCOME_UNKNOWN", False

    def _stream_state(self, stream_key: MonitorStreamKey) -> StreamWorkerState:
        return self._state.streams.get(stream_key, StreamWorkerState(stream_key=stream_key))

    def _set_stream_state(self, updated: StreamWorkerState) -> None:
        streams = dict(self._state.streams)
        streams[updated.stream_key] = updated
        self._state = HostWorkerState(streams=streams)
        self._state_store.save(self._state)


def _validate_trace(
    trace: object,
    observations: tuple[CompletedRangeQueryObservation, ...],
    *,
    registered_trace_parameters: RegisteredTraceParameters,
) -> str | None:
    """Structural validation of the existing trace object without importing its module."""

    stream_key = observations[0].stream_key
    required = (
        "metric",
        "threshold_stratum",
        "configuration_identity",
        "data_identity",
        "flat_identity",
        "hnsw_identity",
        "queries",
        "complete",
        "candidate_ef",
        "last_known_good_ef",
        "sentinel_ef",
    )
    if any(not hasattr(trace, attribute) for attribute in required):
        return "TRACE_INVALID"
    if not bool(trace.complete):
        return "TRACE_INCOMPLETE"
    if trace.metric is not stream_key.metric:
        return "TRACE_METRIC_MISMATCH"
    if trace.threshold_stratum != stream_key.threshold_stratum:
        return "TRACE_THRESHOLD_STRATUM_MISMATCH"
    if trace.configuration_identity != stream_key.configuration_identity:
        return "TRACE_CONFIGURATION_IDENTITY_MISMATCH"
    if trace.data_identity != stream_key.data_identity:
        return "TRACE_DATA_IDENTITY_MISMATCH"
    if (
        trace.candidate_ef not in registered_trace_parameters.allowed_candidate_and_lkg_efs
        or trace.last_known_good_ef not in registered_trace_parameters.allowed_candidate_and_lkg_efs
        or trace.sentinel_ef != registered_trace_parameters.sentinel_ef
    ):
        return "TRACE_QUERY_PARAMETER_UNREGISTERED"
    for identity, expected_binding_id in (
        (trace.flat_identity, stream_key.flat_binding_id),
        (trace.hnsw_identity, stream_key.hnsw_binding_id),
    ):
        if (
            not hasattr(identity, "expected_binding_id")
            or identity.expected_binding_id != expected_binding_id
            or not bool(getattr(identity, "pre_binding_match", False))
            or not bool(getattr(identity, "post_binding_match", False))
            or not bool(getattr(getattr(identity, "pre_capture", None), "success", False))
            or not bool(getattr(getattr(identity, "post_capture", None), "success", False))
        ):
            return "TRACE_IDENTITY_MISMATCH"
    queries = tuple(trace.queries)
    if len(queries) != _TRACE_SIZE:
        return "TRACE_QUERY_COUNT_INVALID"
    if tuple(getattr(query, "query_id", object()) for query in queries) != tuple(
        observation.request_id for observation in observations
    ):
        return "TRACE_QUERY_IDS_MISMATCH"
    for query in queries:
        stages = tuple(getattr(query, "stages", ()))
        stage_names = {getattr(stage, "stage", None) for stage in stages}
        if not {"ORACLE", "FLAT", "SENTINEL_HNSW"}.issubset(stage_names):
            return "TRACE_STAGE_MISSING"
        if any(
            not bool(getattr(stage, "success", False))
            or bool(getattr(stage, "timed_out", False))
            or int(getattr(stage, "threshold_violation_count", 0)) != 0
            for stage in stages
        ):
            return "TRACE_STAGE_FAILED"
        if any(
            getattr(stage, "stage", None) == "FLAT"
            and getattr(stage, "oracle_agreement", None) is not True
            for stage in stages
        ):
            return "TRACE_STAGE_FAILED"
    return None


def _recover_state(state: HostWorkerState) -> HostWorkerState:
    streams: dict[MonitorStreamKey, StreamWorkerState] = {}
    for key, value in state.streams.items():
        lost = value.partial_observation_count + value.inflight_observation_count
        streams[key] = replace(
            value,
            partial_observation_count=0,
            inflight_observation_count=0,
            restart_loss_count=value.restart_loss_count + lost,
        )
    return HostWorkerState(streams=streams)


def _state_document(state: HostWorkerState) -> dict[str, object]:
    return {
        "schema_version": _STATE_SCHEMA_VERSION,
        "streams": [
            {
                "stream_key": _stream_key_document(key),
                "next_trace_ordinal": value.next_trace_ordinal,
                "partial_observation_count": value.partial_observation_count,
                "inflight_observation_count": value.inflight_observation_count,
                "restart_loss_count": value.restart_loss_count,
                "rejected_observation_count": value.rejected_observation_count,
                "blocked_reason_code": value.blocked_reason_code,
            }
            for key, value in sorted(state.streams.items(), key=lambda item: _stream_sort_key(item[0]))
        ],
    }


def _state_from_document(document: object) -> HostWorkerState:
    if not isinstance(document, dict) or set(document) != _STATE_FIELDS or document.get("schema_version") != _STATE_SCHEMA_VERSION:
        raise HostWorkerStateError("HOST_WORKER_STATE_SCHEMA_INVALID")
    values = document.get("streams")
    if not isinstance(values, list):
        raise HostWorkerStateError("HOST_WORKER_STATE_SCHEMA_INVALID")
    streams: dict[MonitorStreamKey, StreamWorkerState] = {}
    for value in values:
        if not isinstance(value, dict) or set(value) != _STREAM_STATE_FIELDS:
            raise HostWorkerStateError("HOST_WORKER_STATE_SCHEMA_INVALID")
        stream_key = _stream_key_from_document(value["stream_key"])
        if stream_key in streams:
            raise HostWorkerStateError("HOST_WORKER_STATE_DUPLICATE_STREAM")
        try:
            streams[stream_key] = StreamWorkerState(
                stream_key=stream_key,
                next_trace_ordinal=value["next_trace_ordinal"],
                partial_observation_count=value["partial_observation_count"],
                inflight_observation_count=value["inflight_observation_count"],
                restart_loss_count=value["restart_loss_count"],
                rejected_observation_count=value["rejected_observation_count"],
                blocked_reason_code=value["blocked_reason_code"],
            )
        except (TypeError, ValueError) as exc:
            raise HostWorkerStateError("HOST_WORKER_STATE_SCHEMA_INVALID") from exc
    return HostWorkerState(streams=streams)


def _stream_key_document(stream_key: MonitorStreamKey) -> dict[str, str]:
    return {
        "stream_id": stream_key.stream_id,
        "metric": stream_key.metric.value,
        "threshold_stratum": stream_key.threshold_stratum,
        "configuration_identity": stream_key.configuration_identity,
        "data_identity": stream_key.data_identity,
        "flat_binding_id": stream_key.flat_binding_id,
        "hnsw_binding_id": stream_key.hnsw_binding_id,
    }


def _stream_key_from_document(value: object) -> MonitorStreamKey:
    if not isinstance(value, dict) or set(value) != _STREAM_KEY_FIELDS:
        raise HostWorkerStateError("HOST_WORKER_STATE_SCHEMA_INVALID")
    try:
        return MonitorStreamKey(
            stream_id=value["stream_id"],
            metric=Metric(value["metric"]),
            threshold_stratum=value["threshold_stratum"],
            configuration_identity=value["configuration_identity"],
            data_identity=value["data_identity"],
            flat_binding_id=value["flat_binding_id"],
            hnsw_binding_id=value["hnsw_binding_id"],
        )
    except (TypeError, ValueError) as exc:
        raise HostWorkerStateError("HOST_WORKER_STATE_SCHEMA_INVALID") from exc


def _stream_sort_key(value: MonitorStreamKey) -> tuple[str, ...]:
    document = _stream_key_document(value)
    return tuple(document[key] for key in sorted(document))


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _no_duplicate_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, member in pairs:
        if key in value:
            raise HostWorkerStateError("HOST_WORKER_STATE_SCHEMA_INVALID")
        value[key] = member
    return value


def _canonical_nonempty(value: object, *, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")  # domain error type carries the governed reason code  # noqa: TRY004
    normalized = unicodedata.normalize("NFC", value)
    if not normalized:
        raise ValueError(f"{name} must be non-empty")
    return normalized


def _canonical_request_id(value: object) -> int | str:
    if isinstance(value, bool):
        raise ValueError("request_id must be an integer or non-empty string")  # domain error type carries the governed reason code  # noqa: TRY004
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return _canonical_nonempty(value, name="request_id")
    raise ValueError("request_id must be an integer or non-empty string")


def _parse_rfc3339_utc(value: object) -> datetime:
    if not isinstance(value, str) or _RFC3339_UTC.fullmatch(value) is None:
        raise ValueError("timestamp must be RFC3339 UTC")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("timestamp must be RFC3339 UTC") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be RFC3339 UTC")
    return parsed


def _append_reason(values: list[str], reason: str) -> None:
    if reason not in values:
        values.append(reason)
