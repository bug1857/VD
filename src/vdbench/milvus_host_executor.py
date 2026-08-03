"""Read-only ADR-007 Milvus shadow executor.

Purpose:
    Convert one compatible 50-observation host batch into the existing immutable
    ``ShadowAuditTrace`` by composing the proven ``shadow_candidate`` path.
Inputs:
    An injected Milvus-actuation adapter, an explicit per-stream shadow plan,
    and completed foreground observations retained only by the worker.
Outputs:
    One captured trace or a non-sensitive ``HostShadowExecutionError``.
Dependencies:
    Existing immutable adapter value contracts and MilvusHarness protocol only.
    This module neither constructs a client nor imports PyMilvus.
Failure modes:
    Observation, plan, health, load, identity, shadow-result, and trace-capture
    mismatches fail closed before a trace can reach the durable publisher.
Extension points:
    A future host creates the injected adapter separately and can retain this
    executor unchanged as long as its workload and identity contracts hold.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
import threading
from types import MappingProxyType
from typing import Protocol

import numpy as np

from .actuation import ActuationContext, ShadowResult
from .config import IndexTrack, Metric, SearchConfiguration
from .drift import AUDIT_QUERY_COUNT, SENTINEL_EF
from .host_observation import CompletedRangeQueryObservation
from .milvus import ClientLike, CollectionIdentity, MilvusHarness
from .milvus_actuation import (
    ActuationWorkload,
    ShadowAuditTrace,
    ShadowAuditTraceSinkLike,
    StackHealth,
    StackHealthProbeLike,
)
from .policy import QualificationResult
from .shadow_event_types import MonitorStreamKey


__all__ = [
    "HostShadowExecutionError",
    "HostShadowPlan",
    "MilvusHostShadowExecutor",
]


_RFC3339_UTC_SUFFIX = "+00:00"


class HostShadowExecutionError(RuntimeError):
    """Non-sensitive fail-closed reason emitted by the background executor."""


@dataclass(frozen=True, slots=True)
class HostShadowPlan:
    """Explicit query-time plan for one exact host observation stream."""

    candidate_ef: int
    last_known_good_ef: int
    required_served_ef: int

    def __post_init__(self) -> None:
        for field in (
            "candidate_ef",
            "last_known_good_ef",
            "required_served_ef",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field} must be a positive integer")
        if self.candidate_ef == self.last_known_good_ef:
            raise ValueError("candidate_ef and last_known_good_ef must differ")


class _MilvusShadowAdapterLike(Protocol):
    """Narrow, read-only subset of the injected Milvus adapter contract."""

    workload: ActuationWorkload
    client: ClientLike
    harness: MilvusHarness
    stack_health_probe: StackHealthProbeLike
    shadow_trace_sink: ShadowAuditTraceSinkLike | None

    def shadow_candidate(
        self,
        *,
        context: ActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult: ...


class _SingleTraceSink:
    """Private in-memory capture sink; never persists or republishes evidence."""

    def __init__(self) -> None:
        self.traces: list[ShadowAuditTrace] = []

    def append(self, trace: ShadowAuditTrace) -> None:
        if not isinstance(trace, ShadowAuditTrace):
            raise TypeError("captured trace has the wrong type")
        self.traces.append(trace)


class MilvusHostShadowExecutor:
    """Validate and capture one read-only 50-query host shadow trace.

    The adapter is injected intentionally.  Constructing it from a URI belongs
    to a host composition root, never to this worker-facing executor.  The lock
    gives this executor exclusive ownership of the adapter's temporary trace
    sink, so concurrent worker cycles cannot cross-contaminate traces.
    """

    def __init__(
        self,
        *,
        adapter: _MilvusShadowAdapterLike,
        plans: Mapping[MonitorStreamKey, HostShadowPlan],
        clock: Callable[[], str],
    ) -> None:
        if not callable(clock):
            raise TypeError("clock must be callable")
        normalized = dict(plans)
        if not normalized:
            raise ValueError("plans must not be empty")
        for key, plan in normalized.items():
            if not isinstance(key, MonitorStreamKey):
                raise TypeError("plans must use MonitorStreamKey keys")
            if not isinstance(plan, HostShadowPlan):
                raise TypeError("plans must use HostShadowPlan values")
        self._adapter = adapter
        self._plans = MappingProxyType(normalized)
        self._clock = clock
        self._capture_lock = threading.Lock()

    def capture(
        self, observations: tuple[CompletedRangeQueryObservation, ...]
    ) -> ShadowAuditTrace:
        """Return one trace after read-only pre/postflight and exact validation."""

        stream_key, plan = self._validate_observations(observations)
        with self._capture_lock:
            if self._adapter.shadow_trace_sink is not None:
                raise HostShadowExecutionError("TRACE_SINK_OWNERSHIP_CONFLICT")
            self._checked_runtime_preflight(stream_key, phase="PRE")
            try:
                context = self._context_for(observations, stream_key)
            except HostShadowExecutionError:
                raise
            except Exception as exc:
                raise HostShadowExecutionError("CAPTURE_CONTEXT_INVALID") from exc
            sink = _SingleTraceSink()
            shadow_result: ShadowResult | None = None
            shadow_failure = False
            try:
                self._adapter.shadow_trace_sink = sink
            except Exception as exc:
                raise HostShadowExecutionError("TRACE_SINK_INSTALL_FAILED") from exc
            postflight_failed = False
            try:
                shadow_result = self._adapter.shadow_candidate(
                    context=context,
                    candidate_ef=plan.candidate_ef,
                    last_known_good_ef=plan.last_known_good_ef,
                )
            except Exception:  # injected read-only adapter boundary
                shadow_failure = True
            finally:
                try:
                    self._checked_runtime_preflight(stream_key, phase="POST")
                except HostShadowExecutionError:
                    shadow_failure = True
                    postflight_failed = True
                finally:
                    try:
                        self._adapter.shadow_trace_sink = None
                    except Exception as exc:
                        raise HostShadowExecutionError(
                            "TRACE_SINK_RESTORE_FAILED"
                        ) from exc
            if postflight_failed:
                raise HostShadowExecutionError("POSTFLIGHT_RUNTIME_INVALID")
            if shadow_failure:
                raise HostShadowExecutionError("SHADOW_CALL_FAILED")
            self._validate_shadow_result(shadow_result)
            trace = self._single_trace(sink)
            self._validate_trace_against_batch(trace, observations, stream_key, plan)
            return trace

    def _checked_runtime_preflight(
        self, stream_key: MonitorStreamKey, *, phase: str
    ) -> None:
        """Normalize every injected runtime-boundary failure to one safe code."""

        try:
            self._runtime_preflight(stream_key)
        except HostShadowExecutionError:
            raise
        except Exception as exc:
            raise HostShadowExecutionError(
                f"{phase}FLIGHT_RUNTIME_UNAVAILABLE"
            ) from exc

    def _validate_observations(
        self, observations: Sequence[CompletedRangeQueryObservation]
    ) -> tuple[MonitorStreamKey, HostShadowPlan]:
        if len(observations) != AUDIT_QUERY_COUNT:
            raise HostShadowExecutionError("OBSERVATION_COUNT_INVALID")
        if any(not isinstance(value, CompletedRangeQueryObservation) for value in observations):
            raise HostShadowExecutionError("OBSERVATION_TYPE_INVALID")
        stream_key = observations[0].stream_key
        if any(value.stream_key != stream_key for value in observations):
            raise HostShadowExecutionError("OBSERVATION_STREAM_MISMATCH")
        try:
            plan = self._plans[stream_key]
        except KeyError as exc:
            raise HostShadowExecutionError("HOST_SHADOW_PLAN_MISSING") from exc
        request_ids = tuple(value.request_id for value in observations)
        if len(set(request_ids)) != AUDIT_QUERY_COUNT:
            raise HostShadowExecutionError("OBSERVATION_QUERY_IDS_DUPLICATE")
        self._validate_against_workload(observations, stream_key, plan)
        return stream_key, plan

    def _validate_against_workload(
        self,
        observations: Sequence[CompletedRangeQueryObservation],
        stream_key: MonitorStreamKey,
        plan: HostShadowPlan,
    ) -> None:
        metric = stream_key.metric
        workload = self._adapter.workload
        if (
            workload.configuration_identity != stream_key.configuration_identity
            or workload.data_identity != stream_key.data_identity
        ):
            raise HostShadowExecutionError("WORKLOAD_IDENTITY_MISMATCH")
        self._require_bindings(stream_key)
        try:
            radius = workload.threshold_radii[(metric, stream_key.threshold_stratum)]
            configuration = SearchConfiguration(
                metric=metric,
                threshold_label=stream_key.threshold_stratum,
                radius=radius,
                index_track=IndexTrack.HNSW,
                ef=plan.required_served_ef,
            )
            configuration.validate()
        except Exception as exc:
            raise HostShadowExecutionError("WORKLOAD_CONFIGURATION_INVALID") from exc
        for observation in observations:
            if (
                not observation.served_outcome.success
                or observation.served_outcome.timed_out
            ):
                raise HostShadowExecutionError("SERVED_OUTCOME_INVALID")
            if observation.served_ef != plan.required_served_ef:
                raise HostShadowExecutionError("OBSERVATION_SERVED_EF_MISMATCH")
            if (
                observation.threshold_radius != configuration.radius
                or observation.range_filter != configuration.range_filter
                or observation.limit != configuration.limit
            ):
                raise HostShadowExecutionError("OBSERVATION_RANGE_CONFIGURATION_MISMATCH")
            try:
                expected = workload.query_vectors[observation.request_id]
            except KeyError as exc:
                raise HostShadowExecutionError("OBSERVATION_QUERY_ID_UNKNOWN") from exc
            actual = np.asarray(observation.query_vector, dtype="<f4")
            if actual.shape != expected.shape or not np.array_equal(actual, expected):
                raise HostShadowExecutionError("OBSERVATION_VECTOR_MISMATCH")

    def _require_bindings(self, stream_key: MonitorStreamKey) -> None:
        bindings = self._adapter.workload.identity_bindings
        try:
            flat = bindings[(stream_key.metric, IndexTrack.FLAT)]
            hnsw = bindings[(stream_key.metric, IndexTrack.HNSW)]
        except KeyError as exc:
            raise HostShadowExecutionError("WORKLOAD_BINDING_MISSING") from exc
        if (
            flat.identity_id != stream_key.flat_binding_id
            or hnsw.identity_id != stream_key.hnsw_binding_id
        ):
            raise HostShadowExecutionError("WORKLOAD_BINDING_MISMATCH")

    def _runtime_preflight(self, stream_key: MonitorStreamKey) -> None:
        try:
            health = self._adapter.stack_health_probe.check()
        except Exception as exc:
            raise HostShadowExecutionError("STACK_HEALTH_UNAVAILABLE") from exc
        if (
            not isinstance(health, StackHealth)
            or health.etcd_healthy is not True
            or health.minio_healthy is not True
        ):
            raise HostShadowExecutionError("STACK_HEALTH_UNHEALTHY")
        self._require_bindings(stream_key)
        for track in IndexTrack:
            self._require_loaded(stream_key, track)
            self._require_identity(stream_key, track)

    def _require_loaded(self, stream_key: MonitorStreamKey, track: IndexTrack) -> None:
        name = self._adapter.workload.collection_names.get((stream_key.metric, track))
        if not isinstance(name, str) or not name:
            raise HostShadowExecutionError("COLLECTION_NAME_MISSING")
        try:
            response = self._adapter.client.get_load_state(collection_name=name)
        except Exception as exc:
            raise HostShadowExecutionError("COLLECTION_LOAD_STATE_UNAVAILABLE") from exc
        state = response.get("state") if isinstance(response, Mapping) else response
        if getattr(state, "name", str(state)) != "Loaded":
            raise HostShadowExecutionError("COLLECTION_NOT_LOADED")

    def _require_identity(self, stream_key: MonitorStreamKey, track: IndexTrack) -> None:
        name = self._adapter.workload.collection_names[(stream_key.metric, track)]
        binding = self._adapter.workload.identity_bindings[(stream_key.metric, track)]
        try:
            actual = self._adapter.harness.index_identity(name, stream_key.metric, track)
        except Exception as exc:
            raise HostShadowExecutionError("COLLECTION_IDENTITY_UNAVAILABLE") from exc
        if not isinstance(actual, CollectionIdentity) or not binding.matches(actual):
            raise HostShadowExecutionError("COLLECTION_IDENTITY_MISMATCH")

    def _context_for(
        self,
        observations: Sequence[CompletedRangeQueryObservation],
        stream_key: MonitorStreamKey,
    ) -> ActuationContext:
        occurred_at_utc = self._clock()
        self._validate_utc_timestamp(occurred_at_utc)
        hnsw_name = self._adapter.workload.collection_names[(stream_key.metric, IndexTrack.HNSW)]
        return ActuationContext(
            metric=stream_key.metric,
            threshold_stratum=stream_key.threshold_stratum,
            collection_name=hnsw_name,
            configuration_identity=stream_key.configuration_identity,
            index_identity=stream_key.hnsw_binding_id,
            flat_index_identity=stream_key.flat_binding_id,
            data_identity=stream_key.data_identity,
            audited_query_ids=tuple(value.request_id for value in observations),
            last_known_good=QualificationResult(
                qualified=False,
                ef=None,
                reasons=("HOST_SHADOW_READ_ONLY",),
            ),
            occurred_at_utc=occurred_at_utc,
        )

    @staticmethod
    def _validate_utc_timestamp(value: object) -> None:
        if not isinstance(value, str) or not value.endswith("Z"):
            raise HostShadowExecutionError("CAPTURE_TIMESTAMP_INVALID")
        try:
            parsed = datetime.fromisoformat(value[:-1] + _RFC3339_UTC_SUFFIX)
        except ValueError as exc:
            raise HostShadowExecutionError("CAPTURE_TIMESTAMP_INVALID") from exc
        if parsed.utcoffset() != timedelta(0):
            raise HostShadowExecutionError("CAPTURE_TIMESTAMP_INVALID")

    @staticmethod
    def _validate_shadow_result(result: ShadowResult | None) -> None:
        if (
            not isinstance(result, ShadowResult)
            or result.success is not True
            or result.audited_query_count != AUDIT_QUERY_COUNT
            or result.failed_query_count != 0
            or result.timeout_query_count != 0
            or result.threshold_violation_count != 0
            or result.candidate_flat_oracle_agreement is not True
            or result.last_known_good_flat_oracle_agreement is not True
        ):
            raise HostShadowExecutionError("SHADOW_RESULT_INVALID")

    @staticmethod
    def _single_trace(sink: _SingleTraceSink) -> ShadowAuditTrace:
        if len(sink.traces) != 1:
            raise HostShadowExecutionError("TRACE_CAPTURE_COUNT_INVALID")
        return sink.traces[0]

    @staticmethod
    def _validate_trace_against_batch(
        trace: ShadowAuditTrace,
        observations: Sequence[CompletedRangeQueryObservation],
        stream_key: MonitorStreamKey,
        plan: HostShadowPlan,
    ) -> None:
        try:
            if (
                trace.metric is not stream_key.metric
                or trace.threshold_stratum != stream_key.threshold_stratum
                or trace.configuration_identity != stream_key.configuration_identity
                or trace.data_identity != stream_key.data_identity
                or trace.candidate_ef != plan.candidate_ef
                or trace.last_known_good_ef != plan.last_known_good_ef
                or trace.sentinel_ef != SENTINEL_EF
            ):
                raise HostShadowExecutionError("TRACE_METADATA_MISMATCH")
            for identity, expected_binding in (
                (trace.flat_identity, stream_key.flat_binding_id),
                (trace.hnsw_identity, stream_key.hnsw_binding_id),
            ):
                if identity.expected_binding_id != expected_binding:
                    raise HostShadowExecutionError("TRACE_BINDING_MISMATCH")
            if tuple(value.query_id for value in trace.queries) != tuple(
                value.request_id for value in observations
            ):
                raise HostShadowExecutionError("TRACE_QUERY_IDS_MISMATCH")
        except HostShadowExecutionError:
            raise
        except Exception as exc:
            raise HostShadowExecutionError("TRACE_STRUCTURE_INVALID") from exc
