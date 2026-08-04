"""Offline tests for restart-safe durable reconciliation after lease expiry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from vdbench.canary_expiry_reconciliation import CanaryExpiryReconciler
from vdbench.canary_grant_store import CanaryGrantUseStore, GrantUseRecord, GrantUseResult, GrantUseStatus
from vdbench.canary_lifecycle_audit import CanaryLifecycleAuditRecord, JsonlCanaryLifecycleAuditSink
from vdbench.canary_route_authority import CanaryRouteAuthority, RouteAuthoritySnapshot, RouteAuthorityState
from vdbench.canary_route_state import FileCanaryRouteStateStore, RouteState, RouteStateBinding, RouteStateRecord
from vdbench.canary_routing import CanaryRouteKind, RouteResolution
from vdbench.config import Metric


def _sha(char: str) -> str:
    return char * 64


def _binding() -> RouteStateBinding:
    return RouteStateBinding(
        metric=Metric.L2, threshold_stratum="target-075", last_known_good_ef=400,
        configuration_identity="config-v1", data_identity="data-v1",
        flat_binding_id="flat-v1", hnsw_binding_id="hnsw-v1",
    )


class FakeAuthority:
    def __init__(self, reason: str) -> None:
        self.reason = reason
        self.snapshot_calls = 0

    def snapshot(self) -> RouteAuthoritySnapshot:
        self.snapshot_calls += 1
        return RouteAuthoritySnapshot(RouteAuthorityState.LKG_ONLY, None, None, 0, self.reason)


@dataclass(frozen=True, slots=True)
class FakePlan:
    plan_sha256: str = _sha("d")
    metric: Metric = Metric.L2
    threshold_stratum: str = "target-075"
    last_known_good_ef: int = 400
    configuration_identity: str = "config-v1"
    data_identity: str = "data-v1"
    flat_binding_id: str = "flat-v1"
    hnsw_binding_id: str = "hnsw-v1"

    def resolve(self, occurrence_id: object) -> RouteResolution:
        return RouteResolution(
            True, str(occurrence_id), 0, 800, CanaryRouteKind.CANDIDATE
        )


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class FakeRouteStateStore:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls = []

    def clear_to_lkg(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise OSError("state unavailable")
        return RouteStateRecord(
            RouteState.LKG_ONLY, kwargs["binding"], None, None,
            kwargs["changed_at_utc"], kwargs["reason_code"],
        )


class FakeAuditSink:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.records: list[CanaryLifecycleAuditRecord] = []

    def contains(self, event_id: str) -> bool:
        return any(record.event_id == event_id for record in self.records)

    def append(self, record: CanaryLifecycleAuditRecord) -> None:
        if self.fail:
            raise OSError("audit unavailable")
        if self.contains(record.event_id):
            raise ValueError("duplicate")
        self.records.append(record)


class FakeGrantStore:
    def __init__(self, *, terminal: bool = False) -> None:
        self.record = GrantUseRecord(
            grant_id="grant-001", signed_payload_sha256=_sha("a"),
            reserved_at_utc="2026-08-04T10:00:00Z",
            status=GrantUseStatus.TERMINAL if terminal else GrantUseStatus.RESERVED,
            terminal_reason_code="APPROVAL_EXPIRED_FAILBACK" if terminal else None,
            terminal_at_utc="2026-08-04T10:01:00Z" if terminal else None,
            terminal_record_id=_sha("b") if terminal else None,
        )
        self.calls = []

    def load(self, grant_id: str):
        return self.record if grant_id == self.record.grant_id else None

    def record_terminal(self, **kwargs):
        self.calls.append(kwargs)
        if self.record.status is GrantUseStatus.TERMINAL:
            return GrantUseResult(False, "GRANT_ALREADY_TERMINAL", self.record)
        self.record = GrantUseRecord(
            grant_id=kwargs["grant_id"], signed_payload_sha256=kwargs["signed_payload_sha256"],
            reserved_at_utc=self.record.reserved_at_utc, status=GrantUseStatus.TERMINAL,
            terminal_reason_code=kwargs["reason_code"], terminal_at_utc=kwargs["occurred_at_utc"],
            terminal_record_id=_sha("c"),
        )
        return GrantUseResult(True, None, self.record)


class CanaryExpiryReconcilerTests(unittest.TestCase):
    def _reconciler(self, *, reason="ROUTE_APPROVAL_EXPIRED", state=None, audit=None, ledger=None):
        return CanaryExpiryReconciler(
            route_authority=FakeAuthority(reason),
            route_state_store=state or FakeRouteStateStore(),
            lifecycle_audit_sink=audit or FakeAuditSink(),
            grant_store=ledger or FakeGrantStore(),
        )

    def _reconcile(self, reconciler):
        return reconciler.reconcile(
            binding=_binding(), grant_id="grant-001", signed_payload_sha256=_sha("a"),
            policy_audit_id="policy-audit-001", plan_sha256=_sha("d"),
            occurred_at_utc="2026-08-04T10:01:00Z",
        )

    def test_expiry_persists_lkg_audits_and_terminates_grant_exactly_once(self) -> None:
        state, audit, ledger = FakeRouteStateStore(), FakeAuditSink(), FakeGrantStore()
        reconciler = CanaryExpiryReconciler(
            route_authority=FakeAuthority("ROUTE_APPROVAL_EXPIRED"), route_state_store=state,
            lifecycle_audit_sink=audit, grant_store=ledger,
        )

        result = self._reconcile(reconciler)
        repeated = self._reconcile(reconciler)

        self.assertTrue(result.reconciled)
        self.assertEqual(result.reason_code, "APPROVAL_EXPIRED_FAILBACK")
        self.assertEqual(state.calls[0]["reason_code"], "APPROVAL_EXPIRED_FAILBACK")
        self.assertEqual(audit.records[0].event_type, "APPROVAL_EXPIRED_FAILBACK")
        self.assertEqual(ledger.calls[0]["reason_code"], "APPROVAL_EXPIRED_FAILBACK")
        self.assertFalse(repeated.reconciled)
        self.assertEqual(repeated.reason_code, "EXPIRY_FAILBACK_ALREADY_TERMINAL")
        self.assertEqual(len(audit.records), 1)
        self.assertEqual(len(ledger.calls), 1)

    def test_unrelated_inactive_reason_does_not_change_durable_state(self) -> None:
        state, audit, ledger = FakeRouteStateStore(), FakeAuditSink(), FakeGrantStore()
        reconciler = CanaryExpiryReconciler(
            route_authority=FakeAuthority("EXPLICIT_REMOVAL"), route_state_store=state,
            lifecycle_audit_sink=audit, grant_store=ledger,
        )

        result = self._reconcile(reconciler)

        self.assertFalse(result.reconciled)
        self.assertEqual(result.reason_code, "EXPIRY_NOT_OBSERVED")
        self.assertEqual(state.calls, [])
        self.assertEqual(audit.records, [])
        self.assertEqual(ledger.calls, [])

    def test_marker_failure_stops_before_audit_or_terminal_write(self) -> None:
        state, audit, ledger = FakeRouteStateStore(fail=True), FakeAuditSink(), FakeGrantStore()
        reconciler = CanaryExpiryReconciler(
            route_authority=FakeAuthority("ROUTE_APPROVAL_EXPIRED"), route_state_store=state,
            lifecycle_audit_sink=audit, grant_store=ledger,
        )

        result = self._reconcile(reconciler)

        self.assertFalse(result.reconciled)
        self.assertEqual(result.reason_code, "DURABLE_FAILBACK_WRITE_FAILED")
        self.assertEqual(audit.records, [])
        self.assertEqual(ledger.calls, [])

    def test_audit_failure_records_specific_terminal_refusal_after_lkg_marker(self) -> None:
        state, audit, ledger = FakeRouteStateStore(), FakeAuditSink(fail=True), FakeGrantStore()
        reconciler = CanaryExpiryReconciler(
            route_authority=FakeAuthority("ROUTE_APPROVAL_EXPIRED"), route_state_store=state,
            lifecycle_audit_sink=audit, grant_store=ledger,
        )

        result = self._reconcile(reconciler)

        self.assertFalse(result.reconciled)
        self.assertEqual(result.reason_code, "REFUSED_EXPIRY_AUDIT_WRITE_FAILED")
        self.assertEqual(len(state.calls), 1)
        self.assertEqual(audit.records, [])
        self.assertEqual(ledger.calls[0]["reason_code"], "REFUSED_EXPIRY_AUDIT_WRITE_FAILED")

    def test_real_components_persist_one_expiry_failback_across_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            clock = MutableClock(datetime(2026, 8, 4, 10, 0, tzinfo=timezone.utc))
            authority = CanaryRouteAuthority(clock=clock)
            state = FileCanaryRouteStateStore(root / "route-state.json")
            ledger = CanaryGrantUseStore(root / "grant-ledger.sqlite")
            audit = JsonlCanaryLifecycleAuditSink(root / "lifecycle.jsonl")
            marker = state.begin_activation(
                binding=_binding(), grant_id="grant-001", plan_sha256=_sha("d"),
                changed_at_utc="2026-08-04T10:00:00Z",
            )
            authority.activate(
                plan=FakePlan(), activation_marker=marker,
                expires_at_utc="2026-08-04T10:01:00Z",
            )
            ledger.reserve(
                grant_id="grant-001", signed_payload_sha256=_sha("a"),
                reserved_at_utc="2026-08-04T10:00:00Z",
            )
            clock.value = datetime(2026, 8, 4, 10, 1, tzinfo=timezone.utc)
            self.assertEqual(authority.snapshot().reason_code, "ROUTE_APPROVAL_EXPIRED")

            result = CanaryExpiryReconciler(
                route_authority=authority, route_state_store=state,
                lifecycle_audit_sink=audit, grant_store=ledger,
            ).reconcile(
                binding=_binding(), grant_id="grant-001", signed_payload_sha256=_sha("a"),
                policy_audit_id="policy-audit-001", plan_sha256=_sha("d"),
                occurred_at_utc="2026-08-04T10:01:00Z",
            )

            restarted_state = FileCanaryRouteStateStore(root / "route-state.json").load()
            restarted_ledger = CanaryGrantUseStore(root / "grant-ledger.sqlite").load("grant-001")
            restarted_audit = JsonlCanaryLifecycleAuditSink(root / "lifecycle.jsonl").records()
            self.assertTrue(result.reconciled)
            self.assertEqual(restarted_state.state, RouteState.LKG_ONLY)
            self.assertEqual(restarted_ledger.status, GrantUseStatus.TERMINAL)
            self.assertEqual(restarted_ledger.terminal_reason_code, "APPROVAL_EXPIRED_FAILBACK")
            self.assertEqual(len(restarted_audit), 1)
            self.assertEqual(restarted_audit[0].event_type, "APPROVAL_EXPIRED_FAILBACK")


if __name__ == "__main__":
    unittest.main()
