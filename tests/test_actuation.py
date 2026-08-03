import ast
from dataclasses import replace
from pathlib import Path
import unittest

from vdbench.actuation import (
    ActuationContext,
    ActuationOutcome,
    RollbackVerification,
    SafeActuationBoundary,
    ShadowResult,
)
from vdbench.config import Metric
from vdbench.drift import build_evidence_provenance
from vdbench.policy import (
    CanaryObservation,
    PolicyAction,
    PolicyDecision,
    PolicyMode,
    QualificationResult,
    SafetyGateResult,
)

AUDIT_ID = "actuation-audit-001"
CONFIGURATION_ID = "config-v1"
INDEX_ID = "hnsw-m16-efc200-v1"
FLAT_INDEX_ID = "flat-v1"
DATA_ID = "dataset-v1"
THRESHOLD_STRATUM = "target-025"
MODULE_PATH = Path(__file__).parents[1] / "src" / "vdbench" / "actuation.py"


def last_known_good() -> QualificationResult:
    return QualificationResult(
        qualified=True,
        ef=400,
        reasons=(),
        metric=Metric.L2,
        threshold_stratum=THRESHOLD_STRATUM,
        configuration_identity=CONFIGURATION_ID,
        index_identity=INDEX_ID,
        data_identity=DATA_ID,
        qualifying_window_ids=("qualification-10", "qualification-11"),
    )


def context() -> ActuationContext:
    return ActuationContext(
        metric=Metric.L2,
        threshold_stratum=THRESHOLD_STRATUM,
        collection_name="vd_l2_hnsw",
        configuration_identity=CONFIGURATION_ID,
        index_identity=INDEX_ID,
        flat_index_identity=FLAT_INDEX_ID,
        data_identity=DATA_ID,
        audited_query_ids=tuple(range(50)),
        last_known_good=last_known_good(),
        occurred_at_utc="2026-08-03T16:00:00Z",
    )


def gate(*, passed: bool = True) -> SafetyGateResult:
    return SafetyGateResult(
        name="PRE_ACTION_READY",
        passed=passed,
        detail="fixture gate",
    )


def decision(
    action: PolicyAction,
    *,
    audit_id: str = AUDIT_ID,
    gates: tuple[SafetyGateResult, ...] | None = None,
) -> PolicyDecision:
    if gates is None:
        gates = (gate(),)
    return PolicyDecision(
        action=action,
        current_ef=400,
        candidate_ef=800 if action is not PolicyAction.NO_CHANGE else None,
        last_known_good_ef=400,
        expected_mean_recall=0.97,
        expected_recall_lower_bound_95=0.96,
        expected_p95_latency_ms=4.8,
        expected_latency_upper_bound_95_ms=5.0,
        predicted_recall_improvement=0.02,
        predicted_latency_reduction_fraction=None,
        reason="fixture policy decision",
        detector_confidence=0.995,
        detector_magnitude=1.25,
        safety_gate_results=gates,
        mode=PolicyMode.CANARY_ENABLED,
        audit_id=audit_id,
        alert_required=action is PolicyAction.ROLLBACK,
        evidence_provenance=build_evidence_provenance(
            metric=Metric.L2,
            threshold_stratum=THRESHOLD_STRATUM,
            reference_window_id="reference-window",
            current_window_id="current-window",
            reference_manifest_sha256="a" * 64,
            current_manifest_sha256="b" * 64,
            configuration_identity=CONFIGURATION_ID,
            data_identity=DATA_ID,
            flat_binding_id=FLAT_INDEX_ID,
            hnsw_binding_id=INDEX_ID,
            reference_audit_ids=tuple(range(50)),
            reference_audit_rank_digests=tuple(f"{value:064x}" for value in range(50)),
            current_audit_ids=tuple(range(50)),
            current_audit_rank_digests=tuple(f"{value + 50:064x}" for value in range(50)),
        ),
    )


def canary_observation() -> CanaryObservation:
    return CanaryObservation(
        metric=Metric.L2,
        threshold_stratum=THRESHOLD_STRATUM,
        candidate_ef=800,
        last_known_good_ef=400,
        completed_query_count=50,
        candidate_mean_recall=0.97,
        candidate_recall_lower_bound_95=0.96,
        last_known_good_mean_recall=0.96,
        candidate_p95_latency_ms=4.8,
        candidate_latency_upper_bound_95_ms=5.0,
        last_known_good_p95_latency_ms=4.0,
        configuration_identity=CONFIGURATION_ID,
        index_identity=INDEX_ID,
        data_identity=DATA_ID,
    )


def successful_shadow() -> ShadowResult:
    return ShadowResult(
        success=True,
        audited_query_count=50,
        failed_query_count=0,
        timeout_query_count=0,
        threshold_violation_count=0,
        candidate_flat_oracle_agreement=True,
        last_known_good_flat_oracle_agreement=True,
        detail="shadow complete",
    )


def successful_verification() -> RollbackVerification:
    return RollbackVerification(
        success=True,
        restored_ef=400,
        health_passed=True,
        audit_passed=True,
        configuration_identity=CONFIGURATION_ID,
        index_identity=INDEX_ID,
        data_identity=DATA_ID,
        detail="restoration verified",
    )


class FakeActuationClient:
    def __init__(
        self,
        *,
        shadow: ShadowResult | None = None,
        observation: CanaryObservation | None = None,
        verification: RollbackVerification | None = None,
    ) -> None:
        self.shadow = shadow or successful_shadow()
        self.observation = observation or canary_observation()
        self.verification = verification or successful_verification()
        self.calls: list[tuple[str, object]] = []

    def shadow_candidate(
        self,
        *,
        context: ActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult:
        self.calls.append(
            (
                "shadow_candidate",
                (context.collection_name, candidate_ef, last_known_good_ef),
            )
        )
        return self.shadow

    def start_canary(
        self,
        *,
        context: ActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
        traffic_fraction: float,
    ) -> CanaryObservation:
        self.calls.append(
            (
                "start_canary",
                (
                    context.collection_name,
                    candidate_ef,
                    last_known_good_ef,
                    traffic_fraction,
                ),
            )
        )
        return self.observation

    def stop_candidate(self) -> None:
        self.calls.append(("stop_candidate", None))

    def restore_last_known_good(self, ef: int) -> None:
        self.calls.append(("restore_last_known_good", ef))

    def verify_restoration(
        self,
        *,
        context: ActuationContext,
        expected_ef: int,
    ) -> RollbackVerification:
        self.calls.append(
            ("verify_restoration", (context.collection_name, expected_ef))
        )
        return self.verification


class FakeAuditSink:
    def __init__(self, existing_ids: tuple[str, ...] = ()) -> None:
        self.records = {}
        self.existing_ids = set(existing_ids)
        self.contains_calls: list[str] = []
        self.append_calls = []

    def contains(self, audit_id: str) -> bool:
        self.contains_calls.append(audit_id)
        return audit_id in self.existing_ids or audit_id in self.records

    def append(self, record) -> None:
        if record.audit_id in self.existing_ids or record.audit_id in self.records:
            raise ValueError("duplicate audit ID")
        self.append_calls.append(record.audit_id)
        self.records[record.audit_id] = record


class FakeController:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def disable_automatic_actions(self, *, audit_id: str, reason: str) -> None:
        self.calls.append((audit_id, reason))


def harness(
    *,
    client: FakeActuationClient | None = None,
    sink: FakeAuditSink | None = None,
    controller: FakeController | None = None,
):
    selected_client = client or FakeActuationClient()
    selected_sink = sink or FakeAuditSink()
    selected_controller = controller or FakeController()
    return (
        SafeActuationBoundary(
            selected_client,
            selected_sink,
            selected_controller,
        ),
        selected_client,
        selected_sink,
        selected_controller,
    )


class SafeActuationBoundaryTests(unittest.TestCase):
    def test_start_canary_without_provenance_is_blocked_before_client_calls(self) -> None:
        client = FakeActuationClient()
        sink = FakeAuditSink()
        controller = FakeController()
        result = SafeActuationBoundary(client, sink, controller).execute(
            replace(decision(PolicyAction.START_CANARY), evidence_provenance=None),
            context(),
        )

        self.assertEqual(result.outcome, ActuationOutcome.BLOCKED)
        self.assertEqual(result.reason, "EVIDENCE_PROVENANCE_MISSING")
        self.assertEqual(client.calls, [])
        self.assertEqual(len(sink.append_calls), 1)

    def test_start_canary_with_context_binding_mismatch_is_blocked(self) -> None:
        client = FakeActuationClient()
        sink = FakeAuditSink()
        controller = FakeController()
        result = SafeActuationBoundary(client, sink, controller).execute(
            decision(PolicyAction.START_CANARY),
            replace(context(), flat_index_identity="unexpected-flat-binding"),
        )

        self.assertEqual(result.outcome, ActuationOutcome.BLOCKED)
        self.assertEqual(result.reason, "EVIDENCE_PROVENANCE_CONTEXT_MISMATCH")
        self.assertEqual(client.calls, [])

    def test_successful_canary_start_shadows_then_exposes_ten_percent(self) -> None:
        boundary, client, sink, controller = harness()

        result = boundary.execute(
            decision(PolicyAction.START_CANARY),
            context(),
            traffic_fraction=0.10,
        )

        self.assertEqual(result.outcome, ActuationOutcome.SUCCEEDED)
        self.assertTrue(result.executed)
        self.assertTrue(result.success)
        self.assertEqual(result.reason, "CANARY_STARTED")
        self.assertEqual(
            [name for name, _ in client.calls],
            ["shadow_candidate", "start_canary"],
        )
        self.assertEqual(client.calls[1][1][-1], 0.10)
        self.assertIs(result.canary_observation, client.observation)
        self.assertIs(result.audit_record.shadow_result, client.shadow)
        self.assertIs(result.audit_record.canary_observation, client.observation)
        self.assertEqual(sink.append_calls, [AUDIT_ID])
        self.assertEqual(controller.calls, [])

    def test_successful_rollback_uses_failed_gates_as_trigger_not_blocker(self) -> None:
        boundary, client, sink, controller = harness()
        rollback = decision(
            PolicyAction.ROLLBACK,
            gates=(gate(passed=False),),
        )

        result = boundary.execute(rollback, context())

        self.assertEqual(result.outcome, ActuationOutcome.SUCCEEDED)
        self.assertTrue(result.success)
        self.assertEqual(result.reason, "ROLLBACK_VERIFIED")
        self.assertEqual(
            client.calls,
            [
                ("stop_candidate", None),
                ("restore_last_known_good", 400),
                ("verify_restoration", ("vd_l2_hnsw", 400)),
            ],
        )
        self.assertFalse(result.audit_record.safety_gate_results[0].passed)
        self.assertIs(
            result.audit_record.rollback_verification,
            client.verification,
        )
        self.assertEqual(sink.append_calls, [AUDIT_ID])
        self.assertEqual(controller.calls, [])

    def test_rollback_verification_failure_disables_automatic_actions(self) -> None:
        failed_verification = replace(
            successful_verification(),
            success=False,
            audit_passed=False,
            detail="restored audit disagreed",
        )
        selected_client = FakeActuationClient(verification=failed_verification)
        boundary, client, sink, controller = harness(client=selected_client)

        result = boundary.execute(
            decision(PolicyAction.ROLLBACK, gates=(gate(passed=False),)),
            context(),
        )

        self.assertEqual(result.outcome, ActuationOutcome.FAILED)
        self.assertFalse(result.success)
        self.assertEqual(result.reason, "ROLLBACK_VERIFICATION_FAILED")
        self.assertTrue(result.automatic_actions_disabled)
        self.assertEqual(
            controller.calls,
            [(AUDIT_ID, "ROLLBACK_VERIFICATION_FAILED")],
        )
        self.assertEqual(
            [name for name, _ in client.calls],
            ["stop_candidate", "restore_last_known_good", "verify_restoration"],
        )
        self.assertEqual(sink.append_calls, [AUDIT_ID])
        self.assertTrue(sink.records[AUDIT_ID].automatic_actions_disabled)

    def test_no_change_and_recommendation_are_recorded_zero_client_call_noops(
        self,
    ) -> None:
        for index, action in enumerate(
            (PolicyAction.NO_CHANGE, PolicyAction.RECOMMEND_EF),
            start=1,
        ):
            with self.subTest(action=action):
                boundary, client, sink, controller = harness()
                audit_id = f"noop-audit-{index}"

                result = boundary.execute(
                    decision(action, audit_id=audit_id),
                    context(),
                )

                self.assertEqual(result.outcome, ActuationOutcome.NO_OP)
                self.assertFalse(result.executed)
                self.assertTrue(result.success)
                self.assertEqual(client.calls, [])
                self.assertEqual(sink.append_calls, [audit_id])
                self.assertEqual(controller.calls, [])

    def test_missing_audit_id_blocks_with_zero_client_and_sink_calls(self) -> None:
        boundary, client, sink, controller = harness()

        result = boundary.execute(
            decision(PolicyAction.START_CANARY, audit_id=""),
            context(),
        )

        self.assertEqual(result.outcome, ActuationOutcome.BLOCKED)
        self.assertEqual(result.reason, "AUDIT_ID_MISSING")
        self.assertEqual(client.calls, [])
        self.assertEqual(sink.contains_calls, [])
        self.assertEqual(sink.append_calls, [])
        self.assertEqual(controller.calls, [])

    def test_duplicate_audit_id_is_rejected_before_client_action(self) -> None:
        selected_sink = FakeAuditSink(existing_ids=(AUDIT_ID,))
        boundary, client, sink, controller = harness(sink=selected_sink)

        result = boundary.execute(
            decision(PolicyAction.START_CANARY),
            context(),
        )

        self.assertEqual(result.outcome, ActuationOutcome.BLOCKED)
        self.assertEqual(result.reason, "AUDIT_ID_DUPLICATE")
        self.assertEqual(client.calls, [])
        self.assertEqual(sink.contains_calls, [AUDIT_ID])
        self.assertEqual(sink.append_calls, [])
        self.assertEqual(controller.calls, [])

    def test_traffic_fraction_above_ten_percent_is_rejected(self) -> None:
        boundary, client, sink, controller = harness()

        result = boundary.execute(
            decision(PolicyAction.START_CANARY),
            context(),
            traffic_fraction=0.100001,
        )

        self.assertEqual(result.outcome, ActuationOutcome.BLOCKED)
        self.assertEqual(result.reason, "CANARY_TRAFFIC_FRACTION_INVALID")
        self.assertEqual(client.calls, [])
        self.assertEqual(sink.append_calls, [AUDIT_ID])
        self.assertEqual(controller.calls, [])

    def test_start_canary_with_failed_gate_is_blocked_but_rollback_is_not(self) -> None:
        boundary, client, sink, controller = harness()

        result = boundary.execute(
            decision(
                PolicyAction.START_CANARY,
                gates=(gate(passed=False),),
            ),
            context(),
        )

        self.assertEqual(result.outcome, ActuationOutcome.BLOCKED)
        self.assertEqual(result.reason, "SAFETY_GATE_FAILED:PRE_ACTION_READY")
        self.assertEqual(client.calls, [])
        self.assertEqual(sink.append_calls, [AUDIT_ID])
        self.assertEqual(controller.calls, [])

    def test_module_has_no_pymilvus_or_execute_live_import(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
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
        self.assertFalse(any(name.startswith("pymilvus") for name in imports))
        self.assertNotIn("execute_live", MODULE_PATH.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
