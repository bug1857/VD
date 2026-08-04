"""Off-path, durable LKG reconciliation after an EXP-009 lease expiry.

This component never serves a query or installs a candidate route.  It observes
only an authority already made inactive by its foreground expiry lease, then
durably records the failback in the marker, lifecycle audit, and grant ledger.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .canary_grant_store import GrantUseRecord, GrantUseResult, GrantUseStatus
from .canary_lifecycle_audit import CanaryLifecycleAuditRecord, lifecycle_event_id
from .canary_route_authority import RouteAuthoritySnapshot, RouteAuthorityState
from .canary_route_state import RouteStateBinding, RouteStateRecord


__all__ = ["CanaryExpiryReconciler", "ExpiryReconciliation"]


class _AuthorityLike(Protocol):
    def snapshot(self) -> RouteAuthoritySnapshot: ...


class _StateStoreLike(Protocol):
    def clear_to_lkg(
        self, *, binding: RouteStateBinding, reason_code: str, changed_at_utc: str
    ) -> RouteStateRecord: ...


class _AuditSinkLike(Protocol):
    def contains(self, event_id: str) -> bool: ...

    def append(self, record: CanaryLifecycleAuditRecord) -> None: ...


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


@dataclass(frozen=True, slots=True)
class ExpiryReconciliation:
    """Immutable result; false never implies a candidate route is available."""

    reconciled: bool
    reason_code: str
    lifecycle_event_id: str | None


class CanaryExpiryReconciler:
    """Persist a previously observed authority lease failback exactly once."""

    def __init__(
        self,
        *,
        route_authority: _AuthorityLike,
        route_state_store: _StateStoreLike,
        lifecycle_audit_sink: _AuditSinkLike,
        grant_store: _GrantStoreLike,
    ) -> None:
        self._route_authority = route_authority
        self._route_state_store = route_state_store
        self._lifecycle_audit_sink = lifecycle_audit_sink
        self._grant_store = grant_store

    def reconcile(
        self,
        *,
        binding: RouteStateBinding,
        grant_id: str,
        signed_payload_sha256: str,
        policy_audit_id: str,
        plan_sha256: str,
        occurred_at_utc: str,
    ) -> ExpiryReconciliation:
        """Durably reconcile only an observed expired authority, never an active one."""

        try:
            snapshot = self._route_authority.snapshot()
        except Exception:
            return ExpiryReconciliation(False, "ROUTE_AUTHORITY_UNAVAILABLE", None)
        if (
            not isinstance(snapshot, RouteAuthoritySnapshot)
            or snapshot.state is not RouteAuthorityState.LKG_ONLY
            or snapshot.reason_code not in {"ROUTE_APPROVAL_EXPIRED", "ROUTE_CLOCK_UNAVAILABLE"}
        ):
            return ExpiryReconciliation(False, "EXPIRY_NOT_OBSERVED", None)
        try:
            prior = self._grant_store.load(grant_id)
        except Exception:
            return ExpiryReconciliation(False, "GRANT_LEDGER_UNAVAILABLE", None)
        if not isinstance(prior, GrantUseRecord):
            return ExpiryReconciliation(False, "GRANT_RESERVATION_MISSING", None)
        if prior.status is GrantUseStatus.TERMINAL:
            return ExpiryReconciliation(False, "EXPIRY_FAILBACK_ALREADY_TERMINAL", None)
        if prior.status is not GrantUseStatus.RESERVED or prior.signed_payload_sha256 != signed_payload_sha256:
            return ExpiryReconciliation(False, "GRANT_RESERVATION_MISMATCH", None)

        event_type = (
            "APPROVAL_EXPIRED_FAILBACK"
            if snapshot.reason_code == "ROUTE_APPROVAL_EXPIRED"
            else "ROUTE_CLOCK_UNAVAILABLE_FAILBACK"
        )
        try:
            self._route_state_store.clear_to_lkg(
                binding=binding,
                reason_code=event_type,
                changed_at_utc=occurred_at_utc,
            )
        except Exception:
            return ExpiryReconciliation(False, "DURABLE_FAILBACK_WRITE_FAILED", None)

        record = _audit_record(
            event_type=event_type,
            grant_id=grant_id,
            signed_payload_sha256=signed_payload_sha256,
            policy_audit_id=policy_audit_id,
            plan_sha256=plan_sha256,
            binding=binding,
            occurred_at_utc=occurred_at_utc,
        )
        try:
            if not self._lifecycle_audit_sink.contains(record.event_id):
                self._lifecycle_audit_sink.append(record)
        except Exception:
            self._terminal_best_effort(
                grant_id=grant_id,
                signed_payload_sha256=signed_payload_sha256,
                reason_code="REFUSED_EXPIRY_AUDIT_WRITE_FAILED",
                occurred_at_utc=occurred_at_utc,
            )
            return ExpiryReconciliation(False, "REFUSED_EXPIRY_AUDIT_WRITE_FAILED", None)
        try:
            terminal = self._grant_store.record_terminal(
                grant_id=grant_id,
                signed_payload_sha256=signed_payload_sha256,
                reason_code=event_type,
                occurred_at_utc=occurred_at_utc,
            )
        except Exception:
            return ExpiryReconciliation(False, "GRANT_TERMINAL_WRITE_FAILED", record.event_id)
        if not isinstance(terminal, GrantUseResult) or not terminal.accepted:
            return ExpiryReconciliation(False, "GRANT_TERMINAL_WRITE_FAILED", record.event_id)
        return ExpiryReconciliation(True, event_type, record.event_id)

    def _terminal_best_effort(
        self,
        *,
        grant_id: str,
        signed_payload_sha256: str,
        reason_code: str,
        occurred_at_utc: str,
    ) -> None:
        try:
            self._grant_store.record_terminal(
                grant_id=grant_id,
                signed_payload_sha256=signed_payload_sha256,
                reason_code=reason_code,
                occurred_at_utc=occurred_at_utc,
            )
        except Exception:
            pass


def _audit_record(
    *,
    event_type: str,
    grant_id: str,
    signed_payload_sha256: str,
    policy_audit_id: str,
    plan_sha256: str,
    binding: RouteStateBinding,
    occurred_at_utc: str,
) -> CanaryLifecycleAuditRecord:
    return CanaryLifecycleAuditRecord(
        event_id=lifecycle_event_id(
            grant_id=grant_id,
            signed_payload_sha256=signed_payload_sha256,
            plan_sha256=plan_sha256,
            event_type=event_type,
        ),
        event_type=event_type,
        grant_id=grant_id,
        signed_payload_sha256=signed_payload_sha256,
        policy_audit_id=policy_audit_id,
        plan_sha256=plan_sha256,
        configuration_identity=binding.configuration_identity,
        data_identity=binding.data_identity,
        flat_binding_id=binding.flat_binding_id,
        hnsw_binding_id=binding.hnsw_binding_id,
        recorded_at_utc=occurred_at_utc,
        reason_code=event_type,
    )
