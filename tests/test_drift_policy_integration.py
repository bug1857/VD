import ast
import unittest
from dataclasses import dataclass
from pathlib import Path

from tests.test_policy import phase3_pair
from vdbench.drift import (
    DetectorState,
    DriftClassification,
    Signal,
    SignalEvidence,
    build_evidence_provenance,
    evaluate_drift_decision,
    finalize_window_evidence,
)
from vdbench.policy import (
    PolicyAction,
    PolicyMode,
    PreActionSafety,
    QualificationWindow,
    ResponseEstimate,
    evaluate_tuning_policy,
)

EFFECT_FLOORS = {
    Signal.QUERY_VECTOR: 0.01,
    Signal.THRESHOLD: 0.20,
    Signal.CARDINALITY: 0.20,
    Signal.RECALL: 0.02,
}
BREACH_EFFECTS = {
    Signal.QUERY_VECTOR: 0.02,
    Signal.THRESHOLD: 0.25,
    Signal.CARDINALITY: 0.25,
    Signal.RECALL: 0.03,
}
SAMPLE_COUNTS = {
    Signal.QUERY_VECTOR: 200,
    Signal.THRESHOLD: 200,
    Signal.CARDINALITY: 50,
    Signal.RECALL: 50,
}
CONFIGURATION_ID = "config-v1"
INDEX_ID = "hnsw-m16-efc200-v1"
FLAT_INDEX_ID = "flat-index-v1"
DATA_ID = "dataset-v1"
THRESHOLD_STRATUM = "target-025"
AUDIT_ID = "integration-audit-001"


@dataclass(frozen=True, slots=True)
class SyntheticScenario:
    name: str
    previous_breaches: tuple[Signal, ...]
    current_breaches: tuple[Signal, ...]
    expected_state: DetectorState
    expected_classification: DriftClassification


SCENARIOS = (
    SyntheticScenario(
        name="stationary",
        previous_breaches=(),
        current_breaches=(),
        expected_state=DetectorState.NO_DRIFT,
        expected_classification=DriftClassification.NONE,
    ),
    SyntheticScenario(
        name="abrupt-input-drift",
        previous_breaches=(Signal.QUERY_VECTOR,),
        current_breaches=(Signal.QUERY_VECTOR,),
        expected_state=DetectorState.DRIFT,
        expected_classification=DriftClassification.INPUT_DRIFT,
    ),
    SyntheticScenario(
        name="quality-only-drift",
        previous_breaches=(Signal.RECALL,),
        current_breaches=(Signal.RECALL,),
        expected_state=DetectorState.DRIFT,
        expected_classification=DriftClassification.QUALITY_DRIFT,
    ),
    SyntheticScenario(
        name="mixed-input-quality-drift",
        previous_breaches=(Signal.QUERY_VECTOR, Signal.RECALL),
        current_breaches=(Signal.QUERY_VECTOR, Signal.RECALL),
        expected_state=DetectorState.DRIFT,
        expected_classification=DriftClassification.INPUT_AND_QUALITY_DRIFT,
    ),
)


def synthetic_signal(signal: Signal, *, breach: bool) -> SignalEvidence:
    count = SAMPLE_COUNTS[signal]
    effect = BREACH_EFFECTS[signal] if breach else 0.0
    return SignalEvidence(
        signal=signal,
        complete=True,
        reference_count=count,
        current_count=count,
        statistic=effect,
        effect=effect,
        effect_floor=EFFECT_FLOORS[signal],
        raw_p_value=0.001 if breach else 1.0,
    )


def synthetic_window(window_id: str, breaches: tuple[Signal, ...]):
    return finalize_window_evidence(
        metric="L2",
        window_id=window_id,
        signals=tuple(
            synthetic_signal(signal, breach=signal in breaches) for signal in Signal
        ),
        provenance=build_evidence_provenance(
            metric="L2",
            threshold_stratum=THRESHOLD_STRATUM,
            reference_window_id="synthetic-reference",
            current_window_id=window_id,
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


def detector_decision(scenario: SyntheticScenario):
    return evaluate_drift_decision(
        synthetic_window(f"{scenario.name}-previous", scenario.previous_breaches),
        synthetic_window(f"{scenario.name}-current", scenario.current_breaches),
    )


def response_estimate(
    ef: int,
    *,
    mean_recall: float,
    recall_lcb: float,
    p95_latency: float,
    latency_ucb: float,
) -> ResponseEstimate:
    return ResponseEstimate(
        metric="L2",
        threshold_stratum=THRESHOLD_STRATUM,
        ef=ef,
        mean_recall=mean_recall,
        recall_lower_bound_95=recall_lcb,
        p95_latency_ms=p95_latency,
        latency_upper_bound_95_ms=latency_ucb,
        validated_model=True,
        provenance="synthetic-validated-response-model-v1",
    )


def input_drift_response_estimates() -> dict[int, ResponseEstimate]:
    return {
        200: response_estimate(
            200,
            mean_recall=0.965,
            recall_lcb=0.962,
            p95_latency=3.6,
            latency_ucb=3.8,
        ),
        400: response_estimate(
            400,
            mean_recall=0.970,
            recall_lcb=0.965,
            p95_latency=4.0,
            latency_ucb=4.2,
        ),
        800: response_estimate(
            800,
            mean_recall=0.980,
            recall_lcb=0.975,
            p95_latency=5.0,
            latency_ucb=5.3,
        ),
        1600: response_estimate(
            1600,
            mean_recall=0.990,
            recall_lcb=0.985,
            p95_latency=7.0,
            latency_ucb=7.4,
        ),
    }


def quality_drift_response_estimates() -> dict[int, ResponseEstimate]:
    return {
        200: response_estimate(
            200,
            mean_recall=0.930,
            recall_lcb=0.920,
            p95_latency=3.2,
            latency_ucb=3.5,
        ),
        400: response_estimate(
            400,
            mean_recall=0.940,
            recall_lcb=0.930,
            p95_latency=4.0,
            latency_ucb=4.2,
        ),
        800: response_estimate(
            800,
            mean_recall=0.960,
            recall_lcb=0.955,
            p95_latency=4.6,
            latency_ucb=4.8,
        ),
        1600: response_estimate(
            1600,
            mean_recall=0.980,
            recall_lcb=0.970,
            p95_latency=7.0,
            latency_ucb=7.5,
        ),
    }


def pre_action_safety() -> PreActionSafety:
    return PreActionSafety(
        metric="L2",
        threshold_stratum=THRESHOLD_STRATUM,
        configuration_identity=CONFIGURATION_ID,
        index_identity=INDEX_ID,
        data_identity=DATA_ID,
        response_model_provenance="synthetic-validated-response-model-v1",
        flat_index_identity=FLAT_INDEX_ID,
    )


def qualification_window(sequence_number: int) -> QualificationWindow:
    return QualificationWindow(
        window_id=f"qualification-{sequence_number}",
        sequence_number=sequence_number,
        metric="L2",
        threshold_stratum=THRESHOLD_STRATUM,
        ef=400,
        mean_recall=0.970,
        recall_lower_bound_95=0.960,
        p95_latency_ms=4.0,
        latency_upper_bound_95_ms=4.5,
        configuration_identity=CONFIGURATION_ID,
        index_identity=INDEX_ID,
        data_identity=DATA_ID,
    )


def policy_decision(
    drift_decision,
    *,
    mode: PolicyMode,
    response_estimates: dict[int, ResponseEstimate],
):
    canary_enabled = mode is PolicyMode.CANARY_ENABLED
    return evaluate_tuning_policy(
        drift_decision,
        current_ef=400,
        response_estimates=response_estimates,
        pre_action=pre_action_safety(),
        canary_observation=None,
        qualification_windows=(
            None
            if canary_enabled
            else (qualification_window(20), qualification_window(21))
        ),
        mode=mode,
        threshold_stratum=THRESHOLD_STRATUM,
        audit_id=AUDIT_ID,
        lkg_authority=(phase3_pair() if canary_enabled else None),
    )


class DriftPolicyIntegrationTests(unittest.TestCase):
    def test_synthetic_scenarios_produce_real_expected_drift_decisions(self) -> None:
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario.name):
                decision = detector_decision(scenario)
                self.assertEqual(decision.state, scenario.expected_state)
                self.assertEqual(
                    decision.classification, scenario.expected_classification
                )

    def test_stationary_scenario_flows_to_no_change(self) -> None:
        scenario = next(item for item in SCENARIOS if item.name == "stationary")
        drift = detector_decision(scenario)
        policy = policy_decision(
            drift,
            mode=PolicyMode.CANARY_ENABLED,
            response_estimates=input_drift_response_estimates(),
        )
        self.assertEqual(drift.state, DetectorState.NO_DRIFT)
        self.assertEqual(policy.action, PolicyAction.NO_CHANGE)
        self.assertEqual(policy.reason, "DETECTOR_NO_DRIFT")

    def test_abrupt_input_drift_respects_policy_mode(self) -> None:
        scenario = next(item for item in SCENARIOS if item.name == "abrupt-input-drift")
        drift = detector_decision(scenario)
        dry_run = policy_decision(
            drift,
            mode=PolicyMode.DRY_RUN,
            response_estimates=input_drift_response_estimates(),
        )
        canary_enabled = policy_decision(
            drift,
            mode=PolicyMode.CANARY_ENABLED,
            response_estimates=input_drift_response_estimates(),
        )
        self.assertEqual(drift.classification, DriftClassification.INPUT_DRIFT)
        self.assertEqual(dry_run.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(canary_enabled.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(
            canary_enabled.reason, "RESPONSE_PROFILE_AUTHORITY_UNAVAILABLE"
        )
        self.assertEqual(dry_run.candidate_ef, 200)
        self.assertEqual(canary_enabled.candidate_ef, 200)

    def test_quality_only_drift_recommends_only_next_higher_ef(self) -> None:
        scenario = next(item for item in SCENARIOS if item.name == "quality-only-drift")
        drift = detector_decision(scenario)
        policy = policy_decision(
            drift,
            mode=PolicyMode.DRY_RUN,
            response_estimates=quality_drift_response_estimates(),
        )
        self.assertEqual(drift.classification, DriftClassification.QUALITY_DRIFT)
        self.assertEqual(policy.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(policy.current_ef, 400)
        self.assertEqual(policy.candidate_ef, 800)
        self.assertGreater(policy.candidate_ef, policy.current_ef)

    def test_mixed_drift_uses_quality_dominant_upward_rule(self) -> None:
        scenario = next(
            item for item in SCENARIOS if item.name == "mixed-input-quality-drift"
        )
        drift = detector_decision(scenario)
        policy = policy_decision(
            drift,
            mode=PolicyMode.CANARY_ENABLED,
            response_estimates=quality_drift_response_estimates(),
        )
        self.assertEqual(
            drift.classification, DriftClassification.INPUT_AND_QUALITY_DRIFT
        )
        self.assertEqual(policy.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(policy.reason, "RESPONSE_PROFILE_AUTHORITY_UNAVAILABLE")
        self.assertEqual(policy.candidate_ef, 800)
        self.assertGreater(policy.candidate_ef, policy.current_ef)

    def test_recovery_scenario_eventually_returns_policy_to_no_change(self) -> None:
        drift_previous = synthetic_window(
            "recovery-drift-previous", (Signal.QUERY_VECTOR,)
        )
        drift_current = synthetic_window(
            "recovery-drift-current", (Signal.QUERY_VECTOR,)
        )
        recovered = synthetic_window("recovery-baseline", ())

        active_drift = evaluate_drift_decision(drift_previous, drift_current)
        active_policy = policy_decision(
            active_drift,
            mode=PolicyMode.DRY_RUN,
            response_estimates=input_drift_response_estimates(),
        )
        recovered_drift = evaluate_drift_decision(drift_current, recovered)
        recovered_policy = policy_decision(
            recovered_drift,
            mode=PolicyMode.CANARY_ENABLED,
            response_estimates=input_drift_response_estimates(),
        )

        self.assertEqual(active_drift.state, DetectorState.DRIFT)
        self.assertEqual(active_policy.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(recovered_drift.state, DetectorState.NO_DRIFT)
        self.assertEqual(recovered_drift.classification, DriftClassification.NONE)
        self.assertEqual(recovered_policy.action, PolicyAction.NO_CHANGE)
        self.assertEqual(recovered_policy.reason, "DETECTOR_NO_DRIFT")

    def test_integration_suite_has_no_pymilvus_import(self) -> None:
        tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
        imported_modules: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported_modules.append(node.module or "")
        self.assertFalse(
            any(
                name == "pymilvus" or name == "vdbench.milvus"
                for name in imported_modules
            )
        )


if __name__ == "__main__":
    unittest.main()
