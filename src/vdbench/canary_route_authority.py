"""In-memory, one-shot foreground route authority for EXP-009 Stage 2.

The authority is deliberately narrow: it atomically publishes one immutable
plan after a matching ``ACTIVATING`` marker, binds it to the already-verified
approval expiry, claims each plan occurrence at most once, and clears every
claim with the plan. It performs no persistence, approval verification, policy
evaluation, audit write, network, or Milvus I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import re
import threading
from typing import Callable, Protocol

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
    """Lock-protected, expiry-bound authority with no fallback candidate path."""

    def __init__(self, *, clock: Callable[[], datetime] | None = None) -> None:
        """Create an empty authority using an injected UTC clock for expiry checks.

        The clock is used only for an in-memory timestamp comparison on the
        foreground path.  An invalid or unavailable clock clears the active
        plan and refuses the lookup rather than risking post-expiry exposure.
        """

        self._lock = threading.Lock()
        self._clock = _utc_now if clock is None else clock
        self._plan: _PlanLike | None = None
        self._grant_id: str | None = None
        self._expires_at_utc: datetime | None = None
        self._claimed_occurrence_ids: set[str] = set()
        self._reason_code = "ROUTE_INACTIVE"

    def activate(
        self,
        *,
        plan: _PlanLike,
        activation_marker: RouteStateRecord,
        expires_at_utc: str,
    ) -> RouteAuthoritySnapshot:
        """Publish one unexpired plan only when the marker binds it exactly."""

        self._validate_activation(plan, activation_marker)
        expires_at = _parse_utc(expires_at_utc)
        with self._lock:
            clock_failure = self._enforce_expiry_unlocked()
            if clock_failure is not None:
                raise ValueError(clock_failure)
            now = self._read_clock_unlocked()
            if now is None:
                self._clear_unlocked("ROUTE_CLOCK_UNAVAILABLE")
                raise ValueError("ROUTE_CLOCK_UNAVAILABLE")
            if now >= expires_at:
                raise ValueError("ROUTE_APPROVAL_EXPIRED")
            if self._plan is not None:
                raise ValueError("ROUTE_ALREADY_ACTIVE")
            self._plan = plan
            self._grant_id = activation_marker.grant_id
            self._expires_at_utc = expires_at
            self._claimed_occurrence_ids = set()
            self._reason_code = "ROUTE_ACTIVE"
            return self._snapshot_unlocked()

    def resolve_and_claim(self, occurrence_id: object) -> RouteClaim:
        """Atomically reject inactivity/duplicates before any caller can dispatch."""

        with self._lock:
            expiry_failure = self._enforce_expiry_unlocked()
            if expiry_failure is not None:
                return RouteClaim(False, None, None, None, None, expiry_failure)
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
            self._clear_unlocked(reason_code)
            return self._snapshot_unlocked()

    def snapshot(self) -> RouteAuthoritySnapshot:
        """Return a constant-size non-sensitive snapshot under the same lock."""

        with self._lock:
            self._enforce_expiry_unlocked()
            return self._snapshot_unlocked()

    def _enforce_expiry_unlocked(self) -> str | None:
        """Fail closed before a foreground caller can observe an active route."""

        if self._plan is None:
            return None
        now = self._read_clock_unlocked()
        if now is None:
            self._clear_unlocked("ROUTE_CLOCK_UNAVAILABLE")
            return "ROUTE_CLOCK_UNAVAILABLE"
        if self._expires_at_utc is None or now >= self._expires_at_utc:
            self._clear_unlocked("ROUTE_APPROVAL_EXPIRED")
            return "ROUTE_APPROVAL_EXPIRED"
        return None

    def _read_clock_unlocked(self) -> datetime | None:
        try:
            value = self._clock()
        except Exception:
            return None
        if not isinstance(value, datetime) or value.tzinfo is None:
            return None
        try:
            if value.utcoffset() != timezone.utc.utcoffset(value):
                return None
        except (TypeError, ValueError):
            return None
        return value

    def _clear_unlocked(self, reason_code: str) -> None:
        self._plan = None
        self._grant_id = None
        self._expires_at_utc = None
        self._claimed_occurrence_ids = set()
        self._reason_code = reason_code

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


_RFC3339_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: object) -> datetime:
    if not isinstance(value, str) or _RFC3339_UTC_RE.fullmatch(value) is None:
        raise ValueError("ROUTE_APPROVAL_EXPIRY_INVALID")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("ROUTE_APPROVAL_EXPIRY_INVALID") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("ROUTE_APPROVAL_EXPIRY_INVALID")
    return parsed
