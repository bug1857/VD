"""Read-only Milvus range-query adapter for ADR-007's reference gateway.

Purpose:
    Execute the foreground HNSW threshold/range query for an already-validated
    host request and return only its compact ``ServedQueryOutcome``.
Inputs:
    An injected Milvus-client-like object, immutable stream-specific serving
    plans, an injected health probe, and a monotonic clock.
Outputs:
    A read-only admission-preflight result and one minimal served-query outcome.
Dependencies:
    The existing synchronous Milvus harness and host-observation value objects;
    this module never imports PyMilvus, detector, policy, or actuation code.
Complexity:
    ``preflight`` is O(number of configured streams); ``execute`` performs one
    HNSW search plus O(vector dimension) request validation.
Failure modes:
    Health, load, identity, plan, request, and search failures are represented
    with non-sensitive reason codes.  No failure invokes a mutation, retry, or
    background-monitoring operation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import math
from time import perf_counter_ns
from threading import Event
from types import MappingProxyType
from typing import Protocol

import numpy as np

from .config import RESULT_LIMIT, IndexTrack, Metric, SearchConfiguration
from .host_observation import RangeQueryRequest, ServedQueryOutcome
from .milvus import ClientLike, CollectionIdentity, MilvusHarness
from .shadow_event_types import MonitorStreamKey


__all__ = [
    "HostServingPlan",
    "MilvusRangeServingExecutor",
    "ServingPreflightResult",
]


class CollectionIdentityBindingLike(Protocol):
    """Structural identity binding implemented by the existing Milvus adapter."""

    identity_id: str

    def matches(self, actual: CollectionIdentity) -> bool: ...


class StackHealthProbeLike(Protocol):
    """Structural health probe kept outside the foreground search path."""

    def check(self) -> object: ...


@dataclass(frozen=True, slots=True)
class HostServingPlan:
    """Immutable HNSW serving contract bound to one exact monitor stream."""

    flat_collection_name: str
    hnsw_collection_name: str
    flat_binding: CollectionIdentityBindingLike
    hnsw_binding: CollectionIdentityBindingLike
    threshold_radius: float
    dimensions: int
    allowed_served_efs: frozenset[int]

    def __post_init__(self) -> None:
        for field in ("flat_collection_name", "hnsw_collection_name"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{field} must be non-empty")
        if self.flat_collection_name == self.hnsw_collection_name:
            raise ValueError("FLAT and HNSW collection names must differ")
        for field in ("flat_binding", "hnsw_binding"):
            binding = getattr(self, field)
            if (
                not isinstance(getattr(binding, "identity_id", None), str)
                or not binding.identity_id
                or not callable(getattr(binding, "matches", None))
            ):
                raise TypeError(f"{field} must satisfy CollectionIdentityBindingLike")
        if (
            not isinstance(self.threshold_radius, (int, float))
            or isinstance(self.threshold_radius, bool)
            or not math.isfinite(float(self.threshold_radius))
        ):
            raise ValueError("threshold_radius must be finite")
        if isinstance(self.dimensions, bool) or not isinstance(self.dimensions, int) or self.dimensions <= 0:
            raise ValueError("dimensions must be a positive integer")
        values = frozenset(self.allowed_served_efs)
        if not values or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in values
        ):
            raise ValueError("allowed_served_efs must contain positive integers")
        object.__setattr__(self, "threshold_radius", float(self.threshold_radius))
        object.__setattr__(self, "allowed_served_efs", values)


@dataclass(frozen=True, slots=True)
class ServingPreflightResult:
    """Inspectable read-only admission result; incomplete means do not serve."""

    complete: bool
    checked_stream_count: int
    reason_codes: tuple[str, ...] = ()


class MilvusRangeServingExecutor:
    """One-search foreground executor plus explicitly separate admission check."""

    def __init__(
        self,
        *,
        client: ClientLike,
        plans: Mapping[MonitorStreamKey, HostServingPlan],
        stack_health_probe: StackHealthProbeLike,
        clock_ns: Callable[[], int] = perf_counter_ns,
    ) -> None:
        if not callable(clock_ns):
            raise TypeError("clock_ns must be callable")
        normalized = dict(plans)
        if not normalized:
            raise ValueError("plans must not be empty")
        for stream_key, plan in normalized.items():
            if not isinstance(stream_key, MonitorStreamKey):
                raise TypeError("plans must use MonitorStreamKey keys")
            if not isinstance(plan, HostServingPlan):
                raise TypeError("plans must use HostServingPlan values")
            self._validate_plan_for_stream(stream_key, plan)
        self._client = client
        self._plans = MappingProxyType(normalized)
        self._stack_health_probe = stack_health_probe
        self._clock_ns = clock_ns
        self._admitted = Event()
        self._harnesses = {
            dimensions: MilvusHarness(client, dimensions=dimensions)
            for dimensions in {plan.dimensions for plan in normalized.values()}
        }

    def preflight(self) -> ServingPreflightResult:
        """Perform only explicit read-only admission checks, outside ``execute``."""

        reasons: list[str] = []
        try:
            health = self._stack_health_probe.check()
        except Exception:
            return self._set_admission(
                ServingPreflightResult(False, 0, ("STACK_HEALTH_UNAVAILABLE",))
            )
        if (
            getattr(health, "etcd_healthy", None) is not True
            or getattr(health, "minio_healthy", None) is not True
        ):
            return self._set_admission(
                ServingPreflightResult(False, 0, ("STACK_HEALTH_UNHEALTHY",))
            )
        checked = 0
        for stream_key, plan in self._plans.items():
            stream_complete = True
            for track, name, binding in (
                (IndexTrack.FLAT, plan.flat_collection_name, plan.flat_binding),
                (IndexTrack.HNSW, plan.hnsw_collection_name, plan.hnsw_binding),
            ):
                try:
                    state = self._client.get_load_state(collection_name=name)
                except Exception:
                    reasons.append(f"COLLECTION_LOAD_STATE_UNAVAILABLE:{track.value}")
                    stream_complete = False
                    continue
                value = state.get("state") if isinstance(state, Mapping) else state
                if getattr(value, "name", str(value)) != "Loaded":
                    reasons.append(f"COLLECTION_NOT_LOADED:{track.value}")
                    stream_complete = False
                    continue
                try:
                    identity = self._harnesses[plan.dimensions].index_identity(
                        name, stream_key.metric, track
                    )
                except Exception:
                    reasons.append(f"COLLECTION_IDENTITY_UNAVAILABLE:{track.value}")
                    stream_complete = False
                    continue
                try:
                    binding_matches = binding.matches(identity)
                except Exception:
                    reasons.append(f"COLLECTION_BINDING_UNAVAILABLE:{track.value}")
                    stream_complete = False
                    continue
                if not binding_matches:
                    reasons.append(f"COLLECTION_IDENTITY_MISMATCH:{track.value}")
                    stream_complete = False
                    continue
            if stream_complete:
                checked += 1
        return self._set_admission(
            ServingPreflightResult(
                complete=not reasons,
                checked_stream_count=checked,
                reason_codes=tuple(dict.fromkeys(reasons)),
            )
        )

    def execute(self, request: RangeQueryRequest) -> ServedQueryOutcome:
        """Run exactly one HNSW search; never call health/load/identity APIs."""

        start = self._clock_ns()
        if not self._admitted.is_set():
            return self._failure(start, "SERVING_PREFLIGHT_REQUIRED")
        try:
            plan = self._request_plan(request)
            with np.errstate(over="ignore", invalid="ignore"):
                query = np.asarray(request.query_vector, dtype="<f4")
            if query.shape != (plan.dimensions,):
                return self._failure(start, "QUERY_DIMENSION_MISMATCH")
            if not np.all(np.isfinite(query)):
                return self._failure(start, "QUERY_VECTOR_OUT_OF_RANGE")
            configuration = SearchConfiguration(
                metric=request.stream_key.metric,
                threshold_label=request.stream_key.threshold_stratum,
                radius=plan.threshold_radius,
                index_track=IndexTrack.HNSW,
                ef=request.served_ef,
                limit=request.limit,
            )
            configuration.validate()
        except _RequestRejected as exc:
            return self._failure(start, str(exc))
        except Exception:
            return self._failure(start, "SERVING_REQUEST_INVALID")

        try:
            hits = self._harnesses[plan.dimensions].search(
                name=plan.hnsw_collection_name,
                query=query,
                configuration=configuration,
            )
        except TimeoutError:
            return self._failure(start, "MILVUS_SEARCH_TIMEOUT", timed_out=True)
        except Exception:
            return self._failure(start, "MILVUS_SEARCH_FAILED")
        return ServedQueryOutcome(
            success=True,
            timed_out=False,
            result_count=len(hits),
            latency_ms=self._elapsed_ms(start),
        )

    @staticmethod
    def _validate_plan_for_stream(
        stream_key: MonitorStreamKey, plan: HostServingPlan
    ) -> None:
        if (
            plan.flat_binding.identity_id != stream_key.flat_binding_id
            or plan.hnsw_binding.identity_id != stream_key.hnsw_binding_id
        ):
            raise ValueError("serving plan binding IDs must match stream key")
        for ef in plan.allowed_served_efs:
            configuration = SearchConfiguration(
                metric=stream_key.metric,
                threshold_label=stream_key.threshold_stratum,
                radius=plan.threshold_radius,
                index_track=IndexTrack.HNSW,
                ef=ef,
            )
            configuration.validate()

    def _request_plan(self, request: RangeQueryRequest) -> HostServingPlan:
        if not isinstance(request, RangeQueryRequest):
            raise _RequestRejected("SERVING_REQUEST_TYPE_INVALID")
        try:
            plan = self._plans[request.stream_key]
        except KeyError as exc:
            raise _RequestRejected("SERVING_PLAN_MISSING") from exc
        if request.served_ef not in plan.allowed_served_efs:
            raise _RequestRejected("SERVED_EF_UNREGISTERED")
        expected_range_filter = 0.0 if request.stream_key.metric is Metric.L2 else 1.0
        if (
            request.threshold_radius != plan.threshold_radius
            or request.range_filter != expected_range_filter
            or request.limit != RESULT_LIMIT
        ):
            raise _RequestRejected("SERVING_RANGE_CONFIGURATION_MISMATCH")
        return plan

    def _set_admission(self, result: ServingPreflightResult) -> ServingPreflightResult:
        """Make a failed fresh preflight revoke foreground search admission."""

        if result.complete:
            self._admitted.set()
        else:
            self._admitted.clear()
        return result

    def _failure(
        self, start: int, code: str, *, timed_out: bool = False
    ) -> ServedQueryOutcome:
        return ServedQueryOutcome(
            success=False,
            timed_out=timed_out,
            result_count=0,
            latency_ms=self._elapsed_ms(start),
            error_code=code,
        )

    def _elapsed_ms(self, start: int) -> float:
        end = self._clock_ns()
        if (
            isinstance(start, bool)
            or not isinstance(start, int)
            or isinstance(end, bool)
            or not isinstance(end, int)
            or end < start
        ):
            return 0.0
        return float(end - start) / 1_000_000.0


class _RequestRejected(ValueError):
    """Internal carrier for the small, non-sensitive foreground error vocabulary."""
