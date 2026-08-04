"""Offline, fail-closed EXP-009 approval-to-route-authority composition.

Purpose:
    Compose an already verified human approval, an immutable 60-of-600 route
    plan, durable grant/audit state, an activation marker, and an injected
    in-memory route authority in one ordered, compensating transaction.
Inputs:
    An externally signed grant, its independent verification context, a real
    ``CanaryRoutePlan``, matching LKG identity, and externally supplied UTC
    timestamps.  Every dependency is injected.
Outputs:
    Immutable activation-attempt evidence.  A successful result only publishes
    the plan to the injected authority; it never resolves an occurrence,
    dispatches a query, creates a client, or contacts Milvus.
Dependencies:
    Local Stage-2 boundaries only.  No network, PyMilvus, policy evaluation,
    request-serving, or cryptographic private-key dependency is present here.
Failure modes:
    Verification/binding/reservation refusals stop before durable route state.
    Audit, marker, or publication errors leave the authority cleared, attempt a
    durable LKG-only marker, and consume the one-time grant where possible.

This is the sole intended composition point for ADR-008 Stage 2.  Stage 3 owns
runtime rollback and restoration auditing; a separate, human-gated stage owns
any connection between this offline authority and a serving path.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Protocol

from .canary_approval import (
    ApprovalVerificationContext,
    ApprovalVerificationResult,
    CanaryApprovalGrant,
    CanaryApprovalTrustStore,
    verify_canary_approval_grant,
)
from .canary_grant_store import GrantUseResult
from .canary_lifecycle_audit import CanaryLifecycleAuditRecord, lifecycle_event_id
from .canary_route_authority import RouteAuthoritySnapshot
from .canary_route_state import RouteStateBinding, RouteStateRecord
from .canary_routing import CanaryRoutePlan


__all__ = [
    "ActivationAttempt",
    "ActivationTimestamps",
    "CanaryActivationCoordinator",
]


_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)


class _GrantUseStoreLike(Protocol):
    def reserve(
        self, *, grant_id: str, signed_payload_sha256: str, reserved_at_utc: str
    ) -> GrantUseResult: ...

    def record_terminal(
        self,
        *,
        grant_id: str,
        signed_payload_sha256: str,
        reason_code: str,
        occurred_at_utc: str,
    ) -> GrantUseResult: ...


class _LifecycleAuditSinkLike(Protocol):
    def append(self, record: CanaryLifecycleAuditRecord) -> None: ...


class _RouteStateStoreLike(Protocol):
    def begin_activation(
        self,
        *,
        binding: RouteStateBinding,
        grant_id: str,
        plan_sha256: str,
        changed_at_utc: str,
    ) -> RouteStateRecord: ...

    def clear_to_lkg(
        self,
        *,
        binding: RouteStateBinding,
        reason_code: str,
        changed_at_utc: str,
    ) -> RouteStateRecord: ...


class _RouteAuthorityLike(Protocol):
    def activate(
        self, *, plan: CanaryRoutePlan, activation_marker: RouteStateRecord
    ) -> RouteAuthoritySnapshot: ...

    def clear(self, *, reason_code: str) -> RouteAuthoritySnapshot: ...


@dataclass(frozen=True, slots=True)
class ActivationTimestamps:
    """Externally supplied audit/ledger instants for deterministic evidence."""

    reserved_at_utc: str
    authorized_at_utc: str
    marker_at_utc: str
    failure_at_utc: str


@dataclass(frozen=True, slots=True)
class ActivationAttempt:
    """Non-sensitive immutable evidence from one coordinator attempt."""

    activated: bool
    reason_code: str
    grant_id: str | None
    plan_sha256: str | None
    authorization_event_id: str | None


class CanaryActivationCoordinator:
    """Compose only the six ADR-008 activation prerequisites in fixed order."""

    def __init__(
        self,
        *,
        verifier: Callable[..., ApprovalVerificationResult] = verify_canary_approval_grant,
        grant_store: _GrantUseStoreLike,
        lifecycle_audit_sink: _LifecycleAuditSinkLike,
        route_state_store: _RouteStateStoreLike,
        route_authority: _RouteAuthorityLike,
    ) -> None:
        self._verifier = verifier
        self._grant_store = grant_store
        self._lifecycle_audit_sink = lifecycle_audit_sink
        self._route_state_store = route_state_store
        self._route_authority = route_authority

    def activate(
        self,
        *,
        grant: CanaryApprovalGrant | None,
        trust_store: CanaryApprovalTrustStore,
        approval_context: ApprovalVerificationContext,
        plan: CanaryRoutePlan,
        binding: RouteStateBinding,
        timestamps: ActivationTimestamps,
    ) -> ActivationAttempt:
        """Verify, reserve, audit, mark, then publish once; compensate on error.

        This method deliberately does not expose the authority's foreground
        claim operation, and it performs zero route resolutions itself.
        """

        if not _timestamps_valid(timestamps) or not isinstance(plan, CanaryRoutePlan):
            return _attempt(False, "ACTIVATION_INPUT_INVALID")
        try:
            verification = self._verifier(
                grant,
                trust_store=trust_store,
                context=approval_context,
            )
        except Exception:
            return _attempt(False, "APPROVAL_VERIFIER_UNAVAILABLE")
        if not isinstance(verification, ApprovalVerificationResult):
            return _attempt(False, "APPROVAL_VERIFIER_INVALID")
        if not verification.approved:
            return _attempt(False, verification.reason_code or "APPROVAL_REFUSED")
        verified_grant = verification.grant
        signed_payload_sha256 = verification.grant_sha256
        if (
            verified_grant is None
            or not isinstance(signed_payload_sha256, str)
            or _SHA256_RE.fullmatch(signed_payload_sha256) is None
        ):
            return _attempt(False, "APPROVAL_VERIFIER_INVALID")
        if not _grant_plan_binding_matches(verified_grant, plan, binding):
            return _attempt(
                False,
                "GRANT_PLAN_BINDING_MISMATCH",
                grant_id=verified_grant.grant_id,
                plan_sha256=plan.plan_sha256,
            )

        try:
            reservation = self._grant_store.reserve(
                grant_id=verified_grant.grant_id,
                signed_payload_sha256=signed_payload_sha256,
                reserved_at_utc=timestamps.reserved_at_utc,
            )
        except Exception:
            return _attempt(
                False,
                "GRANT_RESERVATION_UNAVAILABLE",
                grant_id=verified_grant.grant_id,
                plan_sha256=plan.plan_sha256,
            )
        if not isinstance(reservation, GrantUseResult) or not reservation.accepted:
            return _attempt(
                False,
                reservation.reason_code
                if isinstance(reservation, GrantUseResult) and reservation.reason_code
                else "GRANT_RESERVATION_REFUSED",
                grant_id=verified_grant.grant_id,
                plan_sha256=plan.plan_sha256,
            )

        authorization_event = _authorization_record(
            grant=verified_grant,
            signed_payload_sha256=signed_payload_sha256,
            plan=plan,
            recorded_at_utc=timestamps.authorized_at_utc,
        )
        try:
            self._lifecycle_audit_sink.append(authorization_event)
        except Exception:
            self._record_terminal_best_effort(
                grant=verified_grant,
                signed_payload_sha256=signed_payload_sha256,
                reason_code="REFUSED_AUDIT_WRITE_FAILED",
                occurred_at_utc=timestamps.failure_at_utc,
            )
            return _attempt(
                False,
                "REFUSED_AUDIT_WRITE_FAILED",
                grant_id=verified_grant.grant_id,
                plan_sha256=plan.plan_sha256,
            )

        try:
            marker = self._route_state_store.begin_activation(
                binding=binding,
                grant_id=verified_grant.grant_id,
                plan_sha256=plan.plan_sha256,
                changed_at_utc=timestamps.marker_at_utc,
            )
        except Exception:
            self._compensate_to_lkg(
                binding=binding,
                clear_reason="ACTIVATION_MARKER_WRITE_FAILED",
                changed_at_utc=timestamps.failure_at_utc,
            )
            self._record_terminal_best_effort(
                grant=verified_grant,
                signed_payload_sha256=signed_payload_sha256,
                reason_code="REFUSED_ROUTE_STATE_WRITE_FAILED",
                occurred_at_utc=timestamps.failure_at_utc,
            )
            return _attempt(
                False,
                "REFUSED_ROUTE_STATE_WRITE_FAILED",
                grant_id=verified_grant.grant_id,
                plan_sha256=plan.plan_sha256,
                authorization_event_id=authorization_event.event_id,
            )

        try:
            self._route_authority.activate(plan=plan, activation_marker=marker)
        except Exception:
            self._compensate_to_lkg(
                binding=binding,
                clear_reason="ACTIVATION_AUTHORITY_REFUSED",
                changed_at_utc=timestamps.failure_at_utc,
            )
            self._record_terminal_best_effort(
                grant=verified_grant,
                signed_payload_sha256=signed_payload_sha256,
                reason_code="REFUSED_ROUTE_AUTHORITY_FAILED",
                occurred_at_utc=timestamps.failure_at_utc,
            )
            return _attempt(
                False,
                "REFUSED_ROUTE_AUTHORITY_FAILED",
                grant_id=verified_grant.grant_id,
                plan_sha256=plan.plan_sha256,
                authorization_event_id=authorization_event.event_id,
            )
        return _attempt(
            True,
            "ACTIVATION_PUBLISHED",
            grant_id=verified_grant.grant_id,
            plan_sha256=plan.plan_sha256,
            authorization_event_id=authorization_event.event_id,
        )

    def _compensate_to_lkg(
        self,
        *,
        binding: RouteStateBinding,
        clear_reason: str,
        changed_at_utc: str,
    ) -> None:
        """Attempt all compensations; neither failure can revive a candidate route."""

        try:
            self._route_authority.clear(reason_code=clear_reason)
        except Exception:
            pass
        try:
            self._route_state_store.clear_to_lkg(
                binding=binding,
                reason_code=clear_reason,
                changed_at_utc=changed_at_utc,
            )
        except Exception:
            pass

    def _record_terminal_best_effort(
        self,
        *,
        grant: CanaryApprovalGrant,
        signed_payload_sha256: str,
        reason_code: str,
        occurred_at_utc: str,
    ) -> None:
        try:
            self._grant_store.record_terminal(
                grant_id=grant.grant_id,
                signed_payload_sha256=signed_payload_sha256,
                reason_code=reason_code,
                occurred_at_utc=occurred_at_utc,
            )
        except Exception:
            pass


def _attempt(
    activated: bool,
    reason_code: str,
    *,
    grant_id: str | None = None,
    plan_sha256: str | None = None,
    authorization_event_id: str | None = None,
) -> ActivationAttempt:
    return ActivationAttempt(
        activated=activated,
        reason_code=reason_code,
        grant_id=grant_id,
        plan_sha256=plan_sha256,
        authorization_event_id=authorization_event_id,
    )


def _timestamps_valid(timestamps: object) -> bool:
    if not isinstance(timestamps, ActivationTimestamps):
        return False
    parsed_timestamps: list[datetime] = []
    for value in (
        timestamps.reserved_at_utc,
        timestamps.authorized_at_utc,
        timestamps.marker_at_utc,
        timestamps.failure_at_utc,
    ):
        if not isinstance(value, str) or _RFC3339_UTC_RE.fullmatch(value) is None:
            return False
        try:
            parsed = datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError:
            return False
        if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
            return False
        parsed_timestamps.append(parsed)
    return parsed_timestamps == sorted(parsed_timestamps)


def _grant_plan_binding_matches(
    grant: CanaryApprovalGrant,
    plan: CanaryRoutePlan,
    binding: RouteStateBinding,
) -> bool:
    try:
        return (
            isinstance(binding, RouteStateBinding)
            and grant.metric is plan.metric is binding.metric
            and grant.threshold_stratum == plan.threshold_stratum == binding.threshold_stratum
            and grant.current_ef == plan.last_known_good_ef
            and grant.candidate_ef == plan.candidate_ef
            and grant.last_known_good_ef == plan.last_known_good_ef == binding.last_known_good_ef
            and grant.configuration_identity == plan.configuration_identity == binding.configuration_identity
            and grant.data_identity == plan.data_identity == binding.data_identity
            and grant.flat_binding_id == plan.flat_binding_id == binding.flat_binding_id
            and grant.hnsw_binding_id == plan.hnsw_binding_id == binding.hnsw_binding_id
            and grant.eligible_workload_sha256 == plan.eligible_workload_sha256
            and grant.candidate_selection_sha256 == plan.candidate_selection_sha256
        )
    except (AttributeError, TypeError):
        return False


def _authorization_record(
    *,
    grant: CanaryApprovalGrant,
    signed_payload_sha256: str,
    plan: CanaryRoutePlan,
    recorded_at_utc: str,
) -> CanaryLifecycleAuditRecord:
    event_type = "ACTIVATION_AUTHORIZED"
    return CanaryLifecycleAuditRecord(
        event_id=lifecycle_event_id(
            grant_id=grant.grant_id,
            signed_payload_sha256=signed_payload_sha256,
            plan_sha256=plan.plan_sha256,
            event_type=event_type,
        ),
        event_type=event_type,
        grant_id=grant.grant_id,
        signed_payload_sha256=signed_payload_sha256,
        policy_audit_id=grant.policy_audit_id,
        plan_sha256=plan.plan_sha256,
        configuration_identity=plan.configuration_identity,
        data_identity=plan.data_identity,
        flat_binding_id=plan.flat_binding_id,
        hnsw_binding_id=plan.hnsw_binding_id,
        recorded_at_utc=recorded_at_utc,
        reason_code="ACTIVATION_PENDING",
    )
