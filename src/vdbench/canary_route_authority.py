"""In-memory, one-shot foreground route authority for EXP-009 Stage 2.

The authority is deliberately narrow: it atomically publishes one immutable
plan after a matching ``ACTIVATING`` marker, claims each plan occurrence at
most once, and clears every claim with the plan. It performs no persistence,
approval verification, policy evaluation, audit write, network, or Milvus I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import threading
from typing import Protocol

from .canary_routing import CanaryRouteKind, RouteResolution
from .canary_route_state import RouteState, RouteStateRecord
from .config import Metric


__all__ = [
    "CanaryRouteAuthority",
    "RouteAuthoritySnapshot",
    "RouteAuthorityState",
    "RouteClaim",
]


class _PlanLike(Protocol):
    plan_sha256: str
    metric: Metric
    threshold_stratum: str
    last_known_good_ef: int
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str

    def resolve(self, occurrence_id: object) -> RouteResolution: ...


class RouteAuthorityState(StrEnum):
    """The sole active route is either absent or one immutable plan."""

    LKG_ONLY = "LKG_ONLY"
    ACTIVE = "ACTIVE"


@dataclass(frozen=True, slots=True)
class RouteAuthoritySnapshot:
    """Non-sensitive inspection state; it never contains a route plan."""

    state: RouteAuthorityState
    grant_id: str | None
    plan_sha256: str | None
    claimed_occurrence_count: int
    reason_code: str


@dataclass(frozen=True, slots=True)
class RouteClaim:
    """One foreground claim result; a refusal supplies no query or ef."""

    accepted: bool
    occurrence_id: str | None
    dataset_query_id: int | None
    ef: int | None
    kind: CanaryRouteKind | None
    reason_code: str | None = None


class CanaryRouteAuthority:
    """Lock-protected reference authority with no fallback candidate path."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._plan: _PlanLike | None = None
        self._grant_id: str | None = None
        self._claimed_occurrence_ids: set[str] = set()
        self._reason_code = "ROUTE_INACTIVE"

    def activate(
        self,
        *,
        plan: _PlanLike,
        activation_marker: RouteStateRecord,
    ) -> RouteAuthoritySnapshot:
        """Publish one plan only when the already-written marker binds it exactly."""

        self._validate_activation(plan, activation_marker)
        with self._lock:
            if self._plan is not None:
                raise ValueError("ROUTE_ALREADY_ACTIVE")
            self._plan = plan
            self._grant_id = activation_marker.grant_id
            self._claimed_occurrence_ids = set()
            self._reason_code = "ROUTE_ACTIVE"
            return self._snapshot_unlocked()

    def resolve_and_claim(self, occurrence_id: object) -> RouteClaim:
        """Atomically reject inactivity/duplicates before any caller can dispatch."""

        with self._lock:
            plan = self._plan
            if plan is None:
                return RouteClaim(False, None, None, None, None, "ROUTE_INACTIVE")
            resolution = plan.resolve(occurrence_id)
            if not isinstance(resolution, RouteResolution) or not resolution.accepted:
                return RouteClaim(
                    False,
                    None if not isinstance(resolution, RouteResolution) else resolution.occurrence_id,
                    None,
                    None,
                    None,
                    "ROUTE_PLAN_RESOLUTION_INVALID"
                    if not isinstance(resolution, RouteResolution)
                    else resolution.reason_code or "ROUTE_PLAN_REFUSED",
                )
            identifier = resolution.occurrence_id
            if identifier is None or identifier in self._claimed_occurrence_ids:
                return RouteClaim(False, identifier, None, None, None, "OCCURRENCE_ALREADY_CLAIMED")
            if resolution.dataset_query_id is None or resolution.ef is None or resolution.kind is None:
                return RouteClaim(False, identifier, None, None, None, "ROUTE_PLAN_RESOLUTION_INVALID")
            self._claimed_occurrence_ids.add(identifier)
            return RouteClaim(
                True,
                identifier,
                resolution.dataset_query_id,
                resolution.ef,
                resolution.kind,
            )

    def clear(self, *, reason_code: str) -> RouteAuthoritySnapshot:
        """Atomically drop the complete plan and all one-shot claims."""

        if not isinstance(reason_code, str) or not reason_code:
            raise ValueError("reason_code must be non-empty")
        with self._lock:
            self._plan = None
            self._grant_id = None
            self._claimed_occurrence_ids = set()
            self._reason_code = reason_code
            return self._snapshot_unlocked()

    def snapshot(self) -> RouteAuthoritySnapshot:
        """Return a constant-size non-sensitive snapshot under the same lock."""

        with self._lock:
            return self._snapshot_unlocked()

    def _snapshot_unlocked(self) -> RouteAuthoritySnapshot:
        if self._plan is None:
            return RouteAuthoritySnapshot(
                RouteAuthorityState.LKG_ONLY, None, None, 0, self._reason_code
            )
        return RouteAuthoritySnapshot(
            RouteAuthorityState.ACTIVE,
            self._grant_id,
            self._plan.plan_sha256,
            len(self._claimed_occurrence_ids),
            self._reason_code,
        )

    @staticmethod
    def _validate_activation(plan: _PlanLike, marker: RouteStateRecord) -> None:
        try:
            marker_binding = marker.binding
            matches = (
                marker.state is RouteState.ACTIVATING
                and isinstance(marker.grant_id, str)
                and bool(marker.grant_id)
                and marker.plan_sha256 == plan.plan_sha256
                and marker_binding.metric is plan.metric
                and marker_binding.threshold_stratum == plan.threshold_stratum
                and marker_binding.last_known_good_ef == plan.last_known_good_ef
                and marker_binding.configuration_identity == plan.configuration_identity
                and marker_binding.data_identity == plan.data_identity
                and marker_binding.flat_binding_id == plan.flat_binding_id
                and marker_binding.hnsw_binding_id == plan.hnsw_binding_id
                and callable(plan.resolve)
            )
        except (AttributeError, TypeError):
            matches = False
        if not matches:
            raise ValueError("ACTIVATION_MARKER_MISMATCH")
