"""Offline failure-contract tests for EXP-009 Stage-3 rollback containment."""

from __future__ import annotations

import ast
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from vdbench.actuation_persistence import FileAutomaticActionController
from vdbench.canary_expiry_reconciliation import CanaryExpiryReconciler
from vdbench.canary_grant_store import CanaryGrantUseStore, GrantUseRecord, GrantUseResult, GrantUseStatus
from vdbench.canary_lifecycle_audit import CanaryLifecycleAuditRecord, JsonlCanaryLifecycleAuditSink
from vdbench.canary_route_authority import CanaryRouteAuthority, RouteAuthoritySnapshot, RouteAuthorityState
from vdbench.canary_route_state import FileCanaryRouteStateStore, RouteState, RouteStateBinding, RouteStateRecord
from vdbench.canary_rollback import (
    CanaryRollbackCoordinator,
    RestorationAuditResult,
    RollbackContext,
    RollbackRequest,
    RollbackResult,
    RollbackTrigger,
)
from vdbench.config import Metric
from vdbench.policy import PolicyAction, PolicyDecision, PolicyMode, SafetyGateResult


def _sha(character: str) -> str:
    return character * 64


def _binding() -> RouteStateBinding:
    return RouteStateBinding(
        metric=Metric.L2,
        threshold_stratum="target-075",
        last_known_good_ef=400,
        configuration_identity="stage3-config-v1",
        data_identity="stage3-data-v1",
        flat_binding_id="stage3-flat-v1",
        hnsw_binding_id="stage3-hnsw-v1",
    )


def _context() -> RollbackContext:
    return RollbackContext(
        grant_id="stage3-grant-001",
        signed_payload_sha256=_sha("a"),
        policy_audit_id="stage3-policy-audit-001",
        plan_sha256=_sha("b"),
        binding=_binding(),
        occurred_at_utc="2026-08-04T12:00:00Z",
    )


def _decision(reason: str) -> PolicyDecision:
    return PolicyDecision(
        action=PolicyAction.ROLLBACK,
        current_ef=800,
        candidate_ef=800,
        last_known_good_ef=400,
        expected_mean_recall=None,
        expected_recall_lower_bound_95=None,
        expected_p95_latency_ms=None,
        expected_latency_upper_bound_95_ms=None,
        predicted_recall_improvement=None,
        predicted_latency_reduction_fraction=None,
        reason=reason,
        detector_confidence=None,
        detector_magnitude=None,
        safety_gate_results=(SafetyGateResult("CANARY_FAILURE", False, reason),),
        mode=PolicyMode.CANARY_ENABLED,
        audit_id=_context().policy_audit_id,
        alert_required=True,
    )


class FakeAuthority:
    def __init__(self, *, active: bool = True, reason: str = "ROUTE_ACTIVE") -> None:
        self.snapshot_value = RouteAuthoritySnapshot(
            RouteAuthorityState.ACTIVE if active else RouteAuthorityState.LKG_ONLY,
            _context().grant_id if active else None,
            _context().plan_sha256 if active else None,
            3 if active else 0,
            reason,
        )
        self.calls: list[tuple[str, str | None]] = []

    def snapshot(self) -> RouteAuthoritySnapshot:
        self.calls.append(("snapshot", None))
        return self.snapshot_value

    def clear(self, *, reason_code: str) -> RouteAuthoritySnapshot:
        self.calls.append(("clear", reason_code))
        self.snapshot_value = RouteAuthoritySnapshot(
            RouteAuthorityState.LKG_ONLY, None, None, 0, reason_code
        )
        return self.snapshot_value


class FakeStateStore:
    def __init__(self, *, fail_clear: bool = False, invalid_marker: bool = False) -> None:
        self.fail_clear = fail_clear
        self.record = RouteStateRecord(
            RouteState.LKG_ONLY if invalid_marker else RouteState.ACTIVATING,
            _binding(),
            None if invalid_marker else _context().grant_id,
            None if invalid_marker else _context().plan_sha256,
            "2026-08-04T11:59:00Z",
            "FIXTURE",
        )
        self.calls: list[tuple[str, str | None]] = []

    def load(self) -> RouteStateRecord:
        self.calls.append(("load", None))
        return self.record

    def clear_to_lkg(self, *, binding, reason_code: str, changed_at_utc: str) -> RouteStateRecord:
        self.calls.append(("clear_to_lkg", reason_code))
        if self.fail_clear:
            raise OSError("synthetic marker failure")
        self.record = RouteStateRecord(
            RouteState.LKG_ONLY, binding, None, None, changed_at_utc, reason_code
        )
        return self.record


class FakeGrantStore:
    def __init__(self, *, terminal: bool = False, fail_terminal: bool = False) -> None:
        self.fail_terminal = fail_terminal
        self.record = GrantUseRecord(
            _context().grant_id,
            _context().signed_payload_sha256,
            "2026-08-04T11:59:00Z",
            GrantUseStatus.TERMINAL if terminal else GrantUseStatus.RESERVED,
            "PRIOR" if terminal else None,
            "2026-08-04T11:59:01Z" if terminal else None,
            _sha("c") if terminal else None,
        )
        self.calls: list[str] = []

    def load(self, grant_id: str) -> GrantUseRecord | None:
        self.calls.append("load")
        return self.record if grant_id == self.record.grant_id else None

    def record_terminal(self, *, grant_id: str, signed_payload_sha256: str, reason_code: str, occurred_at_utc: str) -> GrantUseResult:
        self.calls.append("terminal")
        if self.fail_terminal:
            raise OSError("synthetic terminal failure")
        if self.record.status is GrantUseStatus.TERMINAL:
            return GrantUseResult(False, "GRANT_ALREADY_TERMINAL", self.record)
        self.record = GrantUseRecord(
            grant_id,
            signed_payload_sha256,
            self.record.reserved_at_utc,
            GrantUseStatus.TERMINAL,
            reason_code,
            occurred_at_utc,
            _sha("d"),
        )
        return GrantUseResult(True, None, self.record)


class FakeAudit:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.records: list[CanaryLifecycleAuditRecord] = []

    def contains(self, event_id: str) -> bool:
        return any(record.event_id == event_id for record in self.records)

    def append(self, record: CanaryLifecycleAuditRecord) -> None:
        if self.fail:
            raise OSError("synthetic audit failure")
        self.records.append(record)


class FakeController:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[tuple[str, str]] = []

    def disable_automatic_actions(self, *, audit_id: str, reason: str) -> None:
        self.calls.append((audit_id, reason))
        if self.fail:
            raise OSError("synthetic controller failure")


class FakeRestorationAuditor:
    def __init__(self, *, result: RestorationAuditResult | None = None) -> None:
        self.result = result or RestorationAuditResult(True, True, True, 50, "RESTORATION_OK")
        self.calls: list[str] = []

    def verify_restoration(self, *, context: RollbackContext) -> RestorationAuditResult:
        self.calls.append(context.grant_id)
        return self.result


class FakeExpiryReconciler:
    def __init__(self) -> None:
        self.calls: list[RollbackContext] = []

    def reconcile(self, **kwargs):
        self.calls.append(
            RollbackContext(
                grant_id=kwargs["grant_id"],
                signed_payload_sha256=kwargs["signed_payload_sha256"],
                policy_audit_id=kwargs["policy_audit_id"],
                plan_sha256=kwargs["plan_sha256"],
                binding=kwargs["binding"],
                occurred_at_utc=kwargs["occurred_at_utc"],
            )
        )
        return type("ExpiryResult", (), {"reconciled": True})()


@dataclass(frozen=True, slots=True)
class FakePlan:
    plan_sha256: str = _sha("b")
    metric: Metric = Metric.L2
    threshold_stratum: str = "target-075"
    last_known_good_ef: int = 400
    configuration_identity: str = "stage3-config-v1"
    data_identity: str = "stage3-data-v1"
    flat_binding_id: str = "stage3-flat-v1"
    hnsw_binding_id: str = "stage3-hnsw-v1"

    def resolve(self, occurrence_id: object) -> None:
        return None


class CanaryRollbackCoordinatorTests(unittest.TestCase):
    def _coordinator(self, *, authority=None, state=None, ledger=None, audit=None, controller=None, restoration=None, expiry=None):
        selected_authority = authority or FakeAuthority()
        selected_state = state or FakeStateStore()
        selected_ledger = ledger or FakeGrantStore()
        selected_audit = audit or FakeAudit()
        selected_controller = controller or FakeController()
        selected_restoration = restoration or FakeRestorationAuditor()
        selected_expiry = expiry or FakeExpiryReconciler()
        coordinator = CanaryRollbackCoordinator(
            route_authority=selected_authority,
            route_state_store=selected_state,
            grant_store=selected_ledger,
            lifecycle_audit_sink=selected_audit,
            automatic_action_controller=selected_controller,
            restoration_auditor=selected_restoration,
            expiry_reconciler=selected_expiry,
        )
        return coordinator, selected_authority, selected_state, selected_ledger, selected_audit, selected_controller, selected_restoration, selected_expiry

    def _policy_request(self, reason: str) -> RollbackRequest:
        return RollbackRequest(
            trigger=RollbackTrigger.POLICY_ROLLBACK,
            context=_context(),
            policy_decision=_decision(reason),
        )

    def test_hard_recall_and_latency_policy_triggers_clear_then_restore_lkg(self) -> None:
        for reason in ("QUERY_FAILURE", "RECALL_FLOOR_FAILED", "LATENCY_CEILING_FAILED"):
            with self.subTest(reason=reason):
                coordinator, authority, state, ledger, audit, controller, restoration, _ = self._coordinator()
                result = coordinator.rollback(self._policy_request(reason))

                self.assertIsInstance(result, RollbackResult)
                self.assertTrue(result.contained)
                self.assertTrue(result.restoration_verified)
                self.assertEqual(authority.calls[0][0], "snapshot")
                self.assertEqual(authority.calls[1][0], "clear")
                self.assertEqual(state.calls[-1][0], "clear_to_lkg")
                self.assertEqual(ledger.record.status, GrantUseStatus.TERMINAL)
                self.assertEqual([record.event_type for record in audit.records], ["ROLLBACK_TRIGGERED", "ROLLBACK_RESTORATION_VERIFIED"])
                self.assertEqual(controller.calls, [(_context().policy_audit_id, reason)])
                self.assertEqual(restoration.calls, [_context().grant_id])

    def test_restoration_audit_failure_keeps_lkg_and_disables_automatic_actions(self) -> None:
        restoration = FakeRestorationAuditor(
            result=RestorationAuditResult(False, True, False, 50, "FLAT_ORACLE_FAILED")
        )
        coordinator, authority, _, ledger, audit, controller, _, _ = self._coordinator(restoration=restoration)

        result = coordinator.rollback(self._policy_request("QUERY_TIMEOUT"))

        self.assertTrue(result.contained)
        self.assertFalse(result.restoration_verified)
        self.assertEqual(result.reason_code, "ROLLBACK_RESTORATION_UNVERIFIED")
        self.assertEqual(authority.snapshot_value.state, RouteAuthorityState.LKG_ONLY)
        self.assertEqual(ledger.record.status, GrantUseStatus.TERMINAL)
        self.assertEqual(audit.records[-1].event_type, "ROLLBACK_RESTORATION_UNVERIFIED")
        self.assertEqual(len(controller.calls), 1)

    def test_route_state_corruption_and_identity_change_triggers_use_same_lkg_path(self) -> None:
        for trigger in (RollbackTrigger.ROUTE_STATE_CORRUPTION, RollbackTrigger.IDENTITY_CHANGE):
            with self.subTest(trigger=trigger):
                coordinator, authority, _, ledger, audit, controller, restoration, _ = self._coordinator()
                result = coordinator.rollback(RollbackRequest(trigger, _context(), None))

                self.assertTrue(result.contained)
                self.assertTrue(result.restoration_verified)
                self.assertEqual(authority.snapshot_value.state, RouteAuthorityState.LKG_ONLY)
                self.assertEqual(ledger.record.status, GrantUseStatus.TERMINAL)
                self.assertEqual(controller.calls, [(_context().policy_audit_id, trigger.value)])
                self.assertEqual(restoration.calls, [_context().grant_id])
                self.assertEqual(audit.records[0].reason_code, trigger.value)

    def test_authority_clear_precedes_the_restoration_port(self) -> None:
        call_order: list[str] = []

        class OrderedAuthority(FakeAuthority):
            def clear(self, *, reason_code: str) -> RouteAuthoritySnapshot:
                call_order.append("authority_clear")
                return super().clear(reason_code=reason_code)

        class OrderedRestoration(FakeRestorationAuditor):
            def verify_restoration(self, *, context: RollbackContext) -> RestorationAuditResult:
                call_order.append("restoration")
                return super().verify_restoration(context=context)

        coordinator, _, _, _, _, _, _, _ = self._coordinator(
            authority=OrderedAuthority(), restoration=OrderedRestoration()
        )
        result = coordinator.rollback(self._policy_request("QUERY_FAILURE"))

        self.assertTrue(result.restoration_verified)
        self.assertLess(call_order.index("authority_clear"), call_order.index("restoration"))

    def test_authority_clear_failure_disables_automation_and_consumes_grant_without_restoration(self) -> None:
        class FailingClearAuthority(FakeAuthority):
            def clear(self, *, reason_code: str) -> RouteAuthoritySnapshot:
                self.calls.append(("clear", reason_code))
                raise OSError("synthetic authority clear failure")

        coordinator, authority, state, ledger, audit, controller, restoration, _ = (
            self._coordinator(authority=FailingClearAuthority())
        )

        result = coordinator.rollback(self._policy_request("QUERY_FAILURE"))

        self.assertFalse(result.contained)
        self.assertFalse(result.restoration_verified)
        self.assertEqual(result.reason_code, "ROLLBACK_AUTHORITY_CLEAR_FAILED")
        self.assertTrue(result.automatic_actions_disabled)
        self.assertEqual(authority.snapshot_value.state, RouteAuthorityState.ACTIVE)
        self.assertEqual(state.calls[-1], ("clear_to_lkg", "ROLLBACK_AUTHORITY_CLEAR_FAILED"))
        self.assertEqual(ledger.record.status, GrantUseStatus.TERMINAL)
        self.assertEqual(
            audit.records[0].reason_code,
            "ROLLBACK_AUTHORITY_CLEAR_FAILED",
        )
        self.assertEqual(
            controller.calls,
            [(_context().policy_audit_id, "ROLLBACK_AUTHORITY_CLEAR_FAILED")],
        )
        self.assertEqual(restoration.calls, [])

    def test_marker_controller_audit_and_terminal_failures_never_call_restoration_or_leave_authority_active(self) -> None:
        cases = (
            ("marker", {"state": FakeStateStore(fail_clear=True)}, "ROLLBACK_MARKER_WRITE_FAILED"),
            ("controller", {"controller": FakeController(fail=True)}, "ROLLBACK_CONTROLLER_DISABLE_FAILED"),
            ("audit", {"audit": FakeAudit(fail=True)}, "ROLLBACK_AUDIT_WRITE_FAILED"),
            ("terminal", {"ledger": FakeGrantStore(fail_terminal=True)}, "ROLLBACK_TERMINAL_WRITE_FAILED"),
        )
        for name, dependencies, expected in cases:
            with self.subTest(name=name):
                coordinator, authority, _, _, _, _, restoration, _ = self._coordinator(**dependencies)
                result = coordinator.rollback(self._policy_request("QUERY_FAILURE"))

                self.assertFalse(result.restoration_verified)
                self.assertEqual(result.reason_code, expected)
                self.assertEqual(authority.snapshot_value.state, RouteAuthorityState.LKG_ONLY)
                self.assertEqual(restoration.calls, [])
                self.assertEqual(
                    result.automatic_actions_disabled,
                    name != "controller",
                )

    def test_invalid_marker_and_replayed_terminal_grant_clear_authority_without_repeating_work(self) -> None:
        for name, dependencies, expected in (
            ("marker", {"state": FakeStateStore(invalid_marker=True)}, "ROLLBACK_CONTEXT_INVALID"),
            ("terminal", {"ledger": FakeGrantStore(terminal=True)}, "ROLLBACK_ALREADY_TERMINAL"),
        ):
            with self.subTest(name=name):
                coordinator, authority, _, ledger, audit, _, restoration, _ = self._coordinator(**dependencies)
                result = coordinator.rollback(self._policy_request("QUERY_FAILURE"))

                self.assertFalse(result.restoration_verified)
                self.assertEqual(result.reason_code, expected)
                self.assertEqual(authority.snapshot_value.state, RouteAuthorityState.LKG_ONLY)
                self.assertEqual(restoration.calls, [])
                if name == "marker":
                    self.assertEqual(audit.records[0].reason_code, "ROLLBACK_CONTEXT_INVALID")
                    self.assertEqual(ledger.record.status, GrantUseStatus.TERMINAL)
                else:
                    self.assertEqual(audit.records, [])

    def test_malformed_context_or_trigger_clears_an_active_authority_before_refusal(self) -> None:
        for request in (
            RollbackRequest(
                RollbackTrigger.POLICY_ROLLBACK,
                replace(_context(), occurred_at_utc="2026-13-04T12:00:00Z"),
                _decision("QUERY_FAILURE"),
            ),
            RollbackRequest("UNTRUSTED_TRIGGER", _context(), None),  # type: ignore[arg-type]
        ):
            with self.subTest(request=request):
                coordinator, authority, state, ledger, audit, controller, restoration, _ = self._coordinator()
                result = coordinator.rollback(request)

                self.assertTrue(result.contained)
                self.assertEqual(result.reason_code, "ROLLBACK_CONTEXT_INVALID")
                self.assertEqual(authority.snapshot_value.state, RouteAuthorityState.LKG_ONLY)
                self.assertEqual(state.calls, [])
                self.assertEqual(ledger.calls, [])
                self.assertEqual(audit.records, [])
                self.assertEqual(controller.calls, [])
                self.assertEqual(restoration.calls, [])

    def test_malformed_policy_reason_is_contained_as_an_invalid_context(self) -> None:
        coordinator, authority, _, ledger, audit, controller, restoration, _ = (
            self._coordinator()
        )
        malformed = replace(_decision("QUERY_FAILURE"), reason="not a stable code")

        result = coordinator.rollback(
            RollbackRequest(
                RollbackTrigger.POLICY_ROLLBACK,
                _context(),
                malformed,
            )
        )

        self.assertTrue(result.contained)
        self.assertFalse(result.restoration_verified)
        self.assertEqual(result.reason_code, "ROLLBACK_CONTEXT_INVALID")
        self.assertEqual(authority.snapshot_value.state, RouteAuthorityState.LKG_ONLY)
        self.assertEqual(ledger.record.status, GrantUseStatus.TERMINAL)
        self.assertEqual(audit.records[0].reason_code, "ROLLBACK_CONTEXT_INVALID")
        self.assertEqual(
            controller.calls,
            [(_context().policy_audit_id, "ROLLBACK_CONTEXT_INVALID")],
        )
        self.assertEqual(restoration.calls, [])

    def test_expiry_uses_reconciler_then_runs_same_restoration_path(self) -> None:
        authority = FakeAuthority(active=False, reason="ROUTE_APPROVAL_EXPIRED")
        expiry = FakeExpiryReconciler()
        coordinator, _, _, _, audit, controller, restoration, reconciler = self._coordinator(authority=authority, expiry=expiry)
        request = RollbackRequest(RollbackTrigger.APPROVAL_EXPIRY, _context(), None)

        result = coordinator.rollback(request)

        self.assertTrue(result.contained)
        self.assertTrue(result.restoration_verified)
        self.assertEqual(reconciler.calls, [_context()])
        self.assertEqual([record.event_type for record in audit.records], ["ROLLBACK_RESTORATION_VERIFIED"])
        self.assertEqual(len(controller.calls), 1)
        self.assertEqual(restoration.calls, [_context().grant_id])

    def test_real_stores_survive_restart_with_only_lkg_and_terminal_grant(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = FileCanaryRouteStateStore(root / "route-state.json")
            ledger = CanaryGrantUseStore(root / "grant-ledger.sqlite")
            audit = JsonlCanaryLifecycleAuditSink(root / "lifecycle.jsonl")
            controller = FileAutomaticActionController(
                root / "automatic-actions.json",
                clock=lambda: _context().occurred_at_utc,
            )
            authority = CanaryRouteAuthority()
            marker = state.begin_activation(
                binding=_binding(),
                grant_id=_context().grant_id,
                plan_sha256=_context().plan_sha256,
                changed_at_utc="2026-08-04T11:59:00Z",
            )
            authority.activate(
                plan=FakePlan(),
                activation_marker=marker,
                expires_at_utc="2026-08-04T12:30:00Z",
            )
            ledger.reserve(
                grant_id=_context().grant_id,
                signed_payload_sha256=_context().signed_payload_sha256,
                reserved_at_utc="2026-08-04T11:59:00Z",
            )
            result = CanaryRollbackCoordinator(
                route_authority=authority,
                route_state_store=state,
                grant_store=ledger,
                lifecycle_audit_sink=audit,
                automatic_action_controller=controller,
                restoration_auditor=FakeRestorationAuditor(),
                expiry_reconciler=FakeExpiryReconciler(),
            ).rollback(self._policy_request("QUERY_FAILURE"))

            restarted_marker = FileCanaryRouteStateStore(root / "route-state.json").load()
            restarted_ledger = CanaryGrantUseStore(root / "grant-ledger.sqlite").load(
                _context().grant_id
            )
            restarted_audit = JsonlCanaryLifecycleAuditSink(root / "lifecycle.jsonl").records()
            restarted_controller = FileAutomaticActionController(root / "automatic-actions.json")
            self.assertTrue(result.contained)
            self.assertTrue(result.restoration_verified)
            self.assertEqual(restarted_marker.state, RouteState.LKG_ONLY)
            self.assertEqual(restarted_ledger.status, GrantUseStatus.TERMINAL)
            self.assertTrue(restarted_controller.is_disabled())
            self.assertEqual(
                [record.event_type for record in restarted_audit],
                ["ROLLBACK_TRIGGERED", "ROLLBACK_RESTORATION_VERIFIED"],
            )

    def test_real_expiry_reconciliation_persists_failback_before_restoration_audit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            clock_value = datetime(2026, 8, 4, 12, 29, tzinfo=timezone.utc)

            def clock() -> datetime:
                return clock_value

            state = FileCanaryRouteStateStore(root / "route-state.json")
            ledger = CanaryGrantUseStore(root / "grant-ledger.sqlite")
            audit = JsonlCanaryLifecycleAuditSink(root / "lifecycle.jsonl")
            authority = CanaryRouteAuthority(clock=clock)
            controller = FileAutomaticActionController(
                root / "automatic-actions.json",
                clock=lambda: _context().occurred_at_utc,
            )
            marker = state.begin_activation(
                binding=_binding(),
                grant_id=_context().grant_id,
                plan_sha256=_context().plan_sha256,
                changed_at_utc="2026-08-04T11:59:00Z",
            )
            ledger.reserve(
                grant_id=_context().grant_id,
                signed_payload_sha256=_context().signed_payload_sha256,
                reserved_at_utc="2026-08-04T11:59:00Z",
            )
            authority.activate(
                plan=FakePlan(),
                activation_marker=marker,
                expires_at_utc="2026-08-04T12:30:00Z",
            )
            clock_value = datetime(2026, 8, 4, 12, 30, tzinfo=timezone.utc)
            self.assertEqual(
                authority.snapshot().reason_code,
                "ROUTE_APPROVAL_EXPIRED",
            )
            reconciler = CanaryExpiryReconciler(
                route_authority=authority,
                route_state_store=state,
                lifecycle_audit_sink=audit,
                grant_store=ledger,
            )

            result = CanaryRollbackCoordinator(
                route_authority=authority,
                route_state_store=state,
                grant_store=ledger,
                lifecycle_audit_sink=audit,
                automatic_action_controller=controller,
                restoration_auditor=FakeRestorationAuditor(),
                expiry_reconciler=reconciler,
            ).rollback(
                RollbackRequest(RollbackTrigger.APPROVAL_EXPIRY, _context(), None)
            )

            self.assertTrue(result.contained)
            self.assertTrue(result.restoration_verified)
            self.assertEqual(
                FileCanaryRouteStateStore(root / "route-state.json").load().reason_code,
                "APPROVAL_EXPIRED_FAILBACK",
            )
            self.assertEqual(
                CanaryGrantUseStore(root / "grant-ledger.sqlite")
                .load(_context().grant_id)
                .status,
                GrantUseStatus.TERMINAL,
            )
            self.assertTrue(
                FileAutomaticActionController(root / "automatic-actions.json").is_disabled()
            )
            self.assertEqual(
                [record.event_type for record in JsonlCanaryLifecycleAuditSink(
                    root / "lifecycle.jsonl"
                ).records()],
                ["APPROVAL_EXPIRED_FAILBACK", "ROLLBACK_RESTORATION_VERIFIED"],
            )

    def test_module_has_no_milvus_or_actuation_execution_import(self) -> None:
        source = Path("src/vdbench/canary_rollback.py")
        tree = ast.parse(source.read_text(encoding="utf-8"))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        self.assertFalse(
            {
                "pymilvus",
                "vdbench.milvus",
                "vdbench.milvus_actuation",
                "vdbench.actuation",
                "vdbench.execute_live",
            }
            & imports
        )


if __name__ == "__main__":
    unittest.main()
