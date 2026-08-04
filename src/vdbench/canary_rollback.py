"""Offline rollback-containment coordinator for EXP-009 Stage 3.

The coordinator owns no query routing plan and performs no Milvus operation.
It clears the existing in-memory authority before durable bookkeeping or a
restoration audit, then leaves only last-known-good routing available.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import re
import threading
from typing import Protocol

from .canary_grant_store import GrantUseRecord, GrantUseResult, GrantUseStatus
from .canary_lifecycle_audit import CanaryLifecycleAuditRecord, lifecycle_event_id
from .canary_route_authority import RouteAuthoritySnapshot, RouteAuthorityState
from .canary_route_state import RouteState, RouteStateBinding, RouteStateRecord
from .config import Metric, THRESHOLD_LABELS
from .policy import PolicyAction, PolicyDecision


__all__ = [
    "CanaryRollbackCoordinator",
    "RestorationAuditResult",
    "RollbackContext",
    "RollbackRequest",
    "RollbackResult",
    "RollbackTrigger",
]


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_UTC = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_RESTORATION_AUDIT_QUERY_COUNT = 50


class RollbackTrigger(StrEnum):
    """Sources that can require immediate candidate-route containment."""

    POLICY_ROLLBACK = "POLICY_ROLLBACK"
    ROUTE_STATE_CORRUPTION = "ROUTE_STATE_CORRUPTION"
    IDENTITY_CHANGE = "IDENTITY_CHANGE"
    APPROVAL_EXPIRY = "APPROVAL_EXPIRY"


@dataclass(frozen=True, slots=True)
class RollbackContext:
    """Immutable non-sensitive binding for one active canary rollback."""

    grant_id: str
    signed_payload_sha256: str
    policy_audit_id: str
    plan_sha256: str
    binding: RouteStateBinding
    occurred_at_utc: str


@dataclass(frozen=True, slots=True)
class RollbackRequest:
    """One policy or external safety trigger bound to an active route."""

    trigger: RollbackTrigger
    context: RollbackContext
    policy_decision: PolicyDecision | None


@dataclass(frozen=True, slots=True)
class RestorationAuditResult:
    """Injected post-failback health/identity/FLAT-oracle audit evidence."""

    health_passed: bool
    identity_matches: bool
    flat_oracle_audit_passed: bool
    audited_query_count: int
    reason_code: str


@dataclass(frozen=True, slots=True)
class RollbackResult:
    """A completed result never implies that a candidate route remains usable."""

    contained: bool
    restoration_verified: bool
    reason_code: str
    trigger_event_id: str | None
    restoration_event_id: str | None
    automatic_actions_disabled: bool


class _AuthorityLike(Protocol):
    def snapshot(self) -> RouteAuthoritySnapshot: ...

    def clear(self, *, reason_code: str) -> RouteAuthoritySnapshot: ...


class _StateStoreLike(Protocol):
    def load(self) -> RouteStateRecord | None: ...

    def clear_to_lkg(
        self, *, binding: RouteStateBinding, reason_code: str, changed_at_utc: str
    ) -> RouteStateRecord: ...


class _GrantStoreLike(Protocol):
    def load(self, grant_id: str) -> GrantUseRecord | None: ...

    def record_terminal(
        self,
        *,
        grant_id: str,
        signed_payload_sha256: str,
        reason_code: str,
        occurred_at_utc: str,
    ) -> GrantUseResult: ...


class _LifecycleAuditSinkLike(Protocol):
    def contains(self, event_id: str) -> bool: ...

    def append(self, record: CanaryLifecycleAuditRecord) -> None: ...


class _AutomaticActionControllerLike(Protocol):
    def disable_automatic_actions(self, *, audit_id: str, reason: str) -> None: ...


class _RestorationAuditPort(Protocol):
    def verify_restoration(
        self, *, context: RollbackContext
    ) -> RestorationAuditResult: ...


class _ExpiryReconcilerLike(Protocol):
    def reconcile(
        self,
        *,
        binding: RouteStateBinding,
        grant_id: str,
        signed_payload_sha256: str,
        policy_audit_id: str,
        plan_sha256: str,
        occurred_at_utc: str,
    ) -> object: ...


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip()


def _valid_context(value: object) -> bool:
    if not isinstance(value, RollbackContext):
        return False
    binding = value.binding
    if not isinstance(binding, RouteStateBinding):
        return False
    try:
        parsed = datetime.fromisoformat(value.occurred_at_utc[:-1] + "+00:00")
    except (TypeError, ValueError):
        return False
    return bool(
        _nonempty(value.grant_id)
        and _SHA256.fullmatch(value.signed_payload_sha256) is not None
        and _nonempty(value.policy_audit_id)
        and _SHA256.fullmatch(value.plan_sha256) is not None
        and _UTC.fullmatch(value.occurred_at_utc) is not None
        and parsed.tzinfo is not None
        and parsed.utcoffset() == timezone.utc.utcoffset(parsed)
        and isinstance(binding.metric, Metric)
        and binding.threshold_stratum in THRESHOLD_LABELS
        and binding.last_known_good_ef in {200, 400, 800, 1600}
        and all(
            _nonempty(item)
            for item in (
                binding.configuration_identity,
                binding.data_identity,
                binding.flat_binding_id,
                binding.hnsw_binding_id,
            )
        )
    )


def _policy_request_valid(request: RollbackRequest) -> bool:
    decision = request.policy_decision
    return bool(
        isinstance(decision, PolicyDecision)
        and decision.action is PolicyAction.ROLLBACK
        and isinstance(decision.reason, str)
        and _CODE.fullmatch(decision.reason) is not None
        and decision.audit_id == request.context.policy_audit_id
    )


def _marker_matches(record: object, context: RollbackContext) -> bool:
    return bool(
        isinstance(record, RouteStateRecord)
        and record.state is RouteState.ACTIVATING
        and record.binding == context.binding
        and record.grant_id == context.grant_id
        and record.plan_sha256 == context.plan_sha256
    )


def _snapshot_matches(snapshot: object, context: RollbackContext) -> bool:
    return bool(
        isinstance(snapshot, RouteAuthoritySnapshot)
        and snapshot.state is RouteAuthorityState.ACTIVE
        and snapshot.grant_id == context.grant_id
        and snapshot.plan_sha256 == context.plan_sha256
    )


def _restoration_valid(result: object) -> bool:
    return bool(
        isinstance(result, RestorationAuditResult)
        and result.health_passed is True
        and result.identity_matches is True
        and result.flat_oracle_audit_passed is True
        and result.audited_query_count == _RESTORATION_AUDIT_QUERY_COUNT
        and _CODE.fullmatch(result.reason_code) is not None
    )


class CanaryRollbackCoordinator:
    """Fail closed from a Stage-3 trigger to durable LKG-only containment."""

    def __init__(
        self,
        *,
        route_authority: _AuthorityLike,
        route_state_store: _StateStoreLike,
        grant_store: _GrantStoreLike,
        lifecycle_audit_sink: _LifecycleAuditSinkLike,
        automatic_action_controller: _AutomaticActionControllerLike,
        restoration_auditor: _RestorationAuditPort,
        expiry_reconciler: _ExpiryReconcilerLike,
    ) -> None:
        self._route_authority = route_authority
        self._route_state_store = route_state_store
        self._grant_store = grant_store
        self._lifecycle_audit_sink = lifecycle_audit_sink
        self._automatic_action_controller = automatic_action_controller
        self._restoration_auditor = restoration_auditor
        self._expiry_reconciler = expiry_reconciler
        self._lock = threading.Lock()

    def rollback(self, request: RollbackRequest) -> RollbackResult:
        """Contain one trigger; authority clearing always precedes restoration."""

        with self._lock:
            if (
                not isinstance(request, RollbackRequest)
                or not isinstance(request.trigger, RollbackTrigger)
                or not _valid_context(request.context)
            ):
                return self._clear_untrusted_request()
            if request.trigger is RollbackTrigger.APPROVAL_EXPIRY:
                return self._rollback_expiry(request)
            if request.trigger is RollbackTrigger.POLICY_ROLLBACK and not _policy_request_valid(request):
                return self._clear_invalid_context(request.context)
            if request.trigger is not RollbackTrigger.POLICY_ROLLBACK and request.policy_decision is not None:
                return self._clear_invalid_context(request.context)
            return self._rollback_active(request)

    def _rollback_active(self, request: RollbackRequest) -> RollbackResult:
        context = request.context
        try:
            snapshot = self._route_authority.snapshot()
        except Exception:
            return self._clear_authority_unavailable(context)
        try:
            cleared = self._route_authority.clear(
                reason_code=self._clear_reason(request)
            )
        except Exception:
            return self._authority_clear_failed(context)
        if not isinstance(cleared, RouteAuthoritySnapshot) or cleared.state is not RouteAuthorityState.LKG_ONLY:
            return self._authority_clear_failed(context)
        if not _snapshot_matches(snapshot, context):
            return self._contain_invalid_context(context)
        try:
            marker = self._route_state_store.load()
            ledger = self._grant_store.load(context.grant_id)
        except Exception:
            return self._contain_invalid_context(context)
        if not _marker_matches(marker, context):
            return self._contain_invalid_context(context)
        if not isinstance(ledger, GrantUseRecord) or ledger.signed_payload_sha256 != context.signed_payload_sha256:
            return self._contain_invalid_context(context)
        if ledger.status is GrantUseStatus.TERMINAL:
            disabled = self._disable_best_effort(context, "ROLLBACK_ALREADY_TERMINAL")
            return RollbackResult(True, False, "ROLLBACK_ALREADY_TERMINAL", None, None, disabled)
        if ledger.status is not GrantUseStatus.RESERVED:
            return self._contain_invalid_context(context)
        return self._persist_and_restore(request)

    def _rollback_expiry(self, request: RollbackRequest) -> RollbackResult:
        context = request.context
        try:
            snapshot = self._route_authority.snapshot()
        except Exception:
            return self._clear_authority_unavailable(context)
        if not (
            isinstance(snapshot, RouteAuthoritySnapshot)
            and snapshot.state is RouteAuthorityState.LKG_ONLY
            and snapshot.reason_code in {"ROUTE_APPROVAL_EXPIRED", "ROUTE_CLOCK_UNAVAILABLE"}
        ):
            return self._clear_invalid_context(context)
        try:
            expiry = self._expiry_reconciler.reconcile(
                binding=context.binding,
                grant_id=context.grant_id,
                signed_payload_sha256=context.signed_payload_sha256,
                policy_audit_id=context.policy_audit_id,
                plan_sha256=context.plan_sha256,
                occurred_at_utc=context.occurred_at_utc,
            )
        except Exception:
            return self._expiry_reconciliation_failed(context)
        if getattr(expiry, "reconciled", None) is not True:
            return self._expiry_reconciliation_failed(context)
        return self._disable_and_restore(context, trigger_event_id=None)

    def _persist_and_restore(self, request: RollbackRequest) -> RollbackResult:
        context = request.context
        reason = self._clear_reason(request)
        try:
            marker = self._route_state_store.clear_to_lkg(
                binding=context.binding,
                reason_code=reason,
                changed_at_utc=context.occurred_at_utc,
            )
        except Exception:
            disabled = self._disable_best_effort(context, "ROLLBACK_MARKER_WRITE_FAILED")
            self._record_event(
                context=context,
                event_type="ROLLBACK_TRIGGERED",
                reason_code="ROLLBACK_MARKER_WRITE_FAILED",
            )
            self._terminal_best_effort(context, "ROLLBACK_MARKER_WRITE_FAILED")
            return RollbackResult(True, False, "ROLLBACK_MARKER_WRITE_FAILED", None, None, disabled)
        if not isinstance(marker, RouteStateRecord) or marker.state is not RouteState.LKG_ONLY or marker.binding != context.binding:
            disabled = self._disable_best_effort(context, "ROLLBACK_MARKER_WRITE_FAILED")
            self._record_event(
                context=context,
                event_type="ROLLBACK_TRIGGERED",
                reason_code="ROLLBACK_MARKER_WRITE_FAILED",
            )
            self._terminal_best_effort(context, "ROLLBACK_MARKER_WRITE_FAILED")
            return RollbackResult(True, False, "ROLLBACK_MARKER_WRITE_FAILED", None, None, disabled)
        try:
            self._automatic_action_controller.disable_automatic_actions(
                audit_id=context.policy_audit_id,
                reason=self._policy_reason(request),
            )
        except Exception:
            self._record_event(
                context=context,
                event_type="ROLLBACK_TRIGGERED",
                reason_code="ROLLBACK_CONTROLLER_DISABLE_FAILED",
            )
            self._terminal_best_effort(context, "ROLLBACK_CONTROLLER_DISABLE_FAILED")
            return RollbackResult(True, False, "ROLLBACK_CONTROLLER_DISABLE_FAILED", None, None, False)
        trigger_event = self._record_event(
            context=context,
            event_type="ROLLBACK_TRIGGERED",
            reason_code=self._policy_reason(request),
        )
        if trigger_event is None:
            self._terminal_best_effort(context, "ROLLBACK_AUDIT_WRITE_FAILED")
            return RollbackResult(True, False, "ROLLBACK_AUDIT_WRITE_FAILED", None, None, True)
        terminal = self._terminal(context, reason)
        if not terminal:
            return RollbackResult(True, False, "ROLLBACK_TERMINAL_WRITE_FAILED", trigger_event, None, True)
        return self._restore(context, trigger_event)

    def _disable_and_restore(
        self, context: RollbackContext, *, trigger_event_id: str | None
    ) -> RollbackResult:
        try:
            self._automatic_action_controller.disable_automatic_actions(
                audit_id=context.policy_audit_id,
                reason="APPROVAL_EXPIRY",
            )
        except Exception:
            return RollbackResult(True, False, "ROLLBACK_CONTROLLER_DISABLE_FAILED", trigger_event_id, None, False)
        return self._restore(context, trigger_event_id)

    def _restore(self, context: RollbackContext, trigger_event_id: str | None) -> RollbackResult:
        try:
            evidence = self._restoration_auditor.verify_restoration(context=context)
        except Exception:
            evidence = None
        verified = _restoration_valid(evidence)
        event_type = "ROLLBACK_RESTORATION_VERIFIED" if verified else "ROLLBACK_RESTORATION_UNVERIFIED"
        reason = evidence.reason_code if isinstance(evidence, RestorationAuditResult) and _CODE.fullmatch(evidence.reason_code) else event_type
        event_id = self._record_event(context=context, event_type=event_type, reason_code=reason)
        if event_id is None:
            return RollbackResult(True, False, "ROLLBACK_RESTORATION_AUDIT_WRITE_FAILED", trigger_event_id, None, True)
        return RollbackResult(True, verified, event_type, trigger_event_id, event_id, True)

    def _clear_invalid_context(self, context: RollbackContext) -> RollbackResult:
        try:
            cleared = self._route_authority.clear(reason_code="ROLLBACK_CONTEXT_INVALID")
        except Exception:
            return self._authority_clear_failed(context)
        return self._contain_invalid_context(context, cleared=cleared)

    def _contain_invalid_context(
        self, context: RollbackContext, *, cleared: RouteAuthoritySnapshot | None = None
    ) -> RollbackResult:
        self._clear_marker_best_effort(context, "ROLLBACK_CONTEXT_INVALID")
        disabled = self._disable_best_effort(context, "ROLLBACK_CONTEXT_INVALID")
        self._record_event(
            context=context,
            event_type="ROLLBACK_TRIGGERED",
            reason_code="ROLLBACK_CONTEXT_INVALID",
        )
        self._terminal_best_effort(context, "ROLLBACK_CONTEXT_INVALID")
        return RollbackResult(
            cleared is None or (
                isinstance(cleared, RouteAuthoritySnapshot)
                and cleared.state is RouteAuthorityState.LKG_ONLY
            ),
            False,
            "ROLLBACK_CONTEXT_INVALID",
            None,
            None,
            disabled,
        )

    def _clear_untrusted_request(self) -> RollbackResult:
        """Drop a possibly active candidate route without trusting request fields."""

        try:
            cleared = self._route_authority.clear(reason_code="ROLLBACK_CONTEXT_INVALID")
        except Exception:
            return RollbackResult(False, False, "ROLLBACK_AUTHORITY_CLEAR_FAILED", None, None, False)
        return RollbackResult(
            isinstance(cleared, RouteAuthoritySnapshot)
            and cleared.state is RouteAuthorityState.LKG_ONLY,
            False,
            "ROLLBACK_CONTEXT_INVALID",
            None,
            None,
            False,
        )

    def _clear_authority_unavailable(self, context: RollbackContext) -> RollbackResult:
        try:
            cleared = self._route_authority.clear(reason_code="ROLLBACK_AUTHORITY_UNAVAILABLE")
        except Exception:
            return self._authority_clear_failed(context)
        self._clear_marker_best_effort(context, "ROLLBACK_AUTHORITY_UNAVAILABLE")
        disabled = self._disable_best_effort(context, "ROLLBACK_AUTHORITY_UNAVAILABLE")
        self._record_event(
            context=context,
            event_type="ROLLBACK_TRIGGERED",
            reason_code="ROLLBACK_AUTHORITY_UNAVAILABLE",
        )
        self._terminal_best_effort(context, "ROLLBACK_AUTHORITY_UNAVAILABLE")
        return RollbackResult(
            isinstance(cleared, RouteAuthoritySnapshot)
            and cleared.state is RouteAuthorityState.LKG_ONLY,
            False,
            "ROLLBACK_AUTHORITY_UNAVAILABLE",
            None,
            None,
            disabled,
        )

    def _authority_clear_failed(self, context: RollbackContext) -> RollbackResult:
        """Freeze future automation when the sole route authority is unknowable.

        No restoration audit can be meaningful while the authority may still
        retain a candidate plan.  The result therefore remains explicitly
        uncontained, but all durable safety controls are attempted using the
        already validated context.
        """

        reason_code = "ROLLBACK_AUTHORITY_CLEAR_FAILED"
        self._clear_marker_best_effort(context, reason_code)
        disabled = self._disable_best_effort(context, reason_code)
        trigger_event_id = self._record_event(
            context=context,
            event_type="ROLLBACK_TRIGGERED",
            reason_code=reason_code,
        )
        self._terminal_best_effort(context, reason_code)
        return RollbackResult(
            False,
            False,
            reason_code,
            trigger_event_id,
            None,
            disabled,
        )

    def _expiry_reconciliation_failed(self, context: RollbackContext) -> RollbackResult:
        disabled = self._disable_best_effort(
            context, "ROLLBACK_EXPIRY_RECONCILIATION_FAILED"
        )
        self._record_event(
            context=context,
            event_type="ROLLBACK_TRIGGERED",
            reason_code="ROLLBACK_EXPIRY_RECONCILIATION_FAILED",
        )
        self._terminal_best_effort(context, "ROLLBACK_EXPIRY_RECONCILIATION_FAILED")
        return RollbackResult(
            True,
            False,
            "ROLLBACK_EXPIRY_RECONCILIATION_FAILED",
            None,
            None,
            disabled,
        )

    @staticmethod
    def _clear_reason(request: RollbackRequest) -> str:
        return "ROLLBACK_" + (
            request.policy_decision.reason
            if request.trigger is RollbackTrigger.POLICY_ROLLBACK and request.policy_decision is not None
            else request.trigger.value
        )

    @staticmethod
    def _policy_reason(request: RollbackRequest) -> str:
        return (
            request.policy_decision.reason
            if request.trigger is RollbackTrigger.POLICY_ROLLBACK and request.policy_decision is not None
            else request.trigger.value
        )

    def _clear_marker_best_effort(self, context: RollbackContext, reason_code: str) -> None:
        try:
            self._route_state_store.clear_to_lkg(
                binding=context.binding,
                reason_code=reason_code,
                changed_at_utc=context.occurred_at_utc,
            )
        except Exception:
            pass

    def _disable_best_effort(self, context: RollbackContext, reason: str) -> bool:
        try:
            self._automatic_action_controller.disable_automatic_actions(
                audit_id=context.policy_audit_id, reason=reason
            )
        except Exception:
            return False
        return True

    def _terminal_best_effort(self, context: RollbackContext, reason_code: str) -> None:
        try:
            self._terminal(context, reason_code)
        except Exception:
            pass

    def _terminal(self, context: RollbackContext, reason_code: str) -> bool:
        try:
            result = self._grant_store.record_terminal(
                grant_id=context.grant_id,
                signed_payload_sha256=context.signed_payload_sha256,
                reason_code=reason_code,
                occurred_at_utc=context.occurred_at_utc,
            )
        except Exception:
            return False
        return bool(isinstance(result, GrantUseResult) and result.accepted is True)

    def _record_event(
        self, *, context: RollbackContext, event_type: str, reason_code: str
    ) -> str | None:
        event_id = lifecycle_event_id(
            grant_id=context.grant_id,
            signed_payload_sha256=context.signed_payload_sha256,
            plan_sha256=context.plan_sha256,
            event_type=event_type,
        )
        record = CanaryLifecycleAuditRecord(
            event_id=event_id,
            event_type=event_type,
            grant_id=context.grant_id,
            signed_payload_sha256=context.signed_payload_sha256,
            policy_audit_id=context.policy_audit_id,
            plan_sha256=context.plan_sha256,
            configuration_identity=context.binding.configuration_identity,
            data_identity=context.binding.data_identity,
            flat_binding_id=context.binding.flat_binding_id,
            hnsw_binding_id=context.binding.hnsw_binding_id,
            recorded_at_utc=context.occurred_at_utc,
            reason_code=reason_code,
        )
        try:
            if not self._lifecycle_audit_sink.contains(event_id):
                self._lifecycle_audit_sink.append(record)
        except Exception:
            return None
        return event_id
