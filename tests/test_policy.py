from dataclasses import replace
from pathlib import Path
import unittest

from vdbench.drift import (
    DetectorState,
    DriftClassification,
    DriftDecision,
    build_evidence_provenance,
)
from vdbench.policy import (
    CanaryObservation,
    PolicyAction,
    PolicyMode,
    PreActionSafety,
    QualificationWindow,
    ResponseEstimate,
    evaluate_tuning_policy,
    qualify_last_known_good,
)

CONFIG_ID = "config-v1"
INDEX_ID = "hnsw-m16-efc200-v1"
FLAT_INDEX_ID = "flat-v1"
DATA_ID = "dataset-v1"
AUDIT_ID = "audit-policy-001"


def provenance(
    *,
    current_window_id: str = "current-window",
    threshold: str = "target-025",
    configuration_identity: str = CONFIG_ID,
):
    return build_evidence_provenance(
        metric="L2",
        threshold_stratum=threshold,
        reference_window_id="reference-window",
        current_window_id=current_window_id,
        reference_manifest_sha256="a" * 64,
        current_manifest_sha256="b" * 64,
        configuration_identity=configuration_identity,
        data_identity=DATA_ID,
        flat_binding_id=FLAT_INDEX_ID,
        hnsw_binding_id=INDEX_ID,
        reference_audit_ids=tuple(range(50)),
        reference_audit_rank_digests=tuple(f"{value:064x}" for value in range(50)),
        current_audit_ids=tuple(range(50)),
        current_audit_rank_digests=tuple(f"{value + 50:064x}" for value in range(50)),
    )


def detector(
    state: DetectorState = DetectorState.DRIFT,
    classification: DriftClassification = DriftClassification.QUALITY_DRIFT,
    threshold: str = "target-025",
) -> DriftDecision:
    return DriftDecision(
        state=state,
        classification=classification,
        significance_evidence_score=(
            0.995 if state is DetectorState.DRIFT else None
        ),
        drift_magnitude=1.25 if state is DetectorState.DRIFT else None,
        evidence_provenance=(
            provenance(threshold=threshold) if state is DetectorState.DRIFT else None
        ),
    )


def estimate(
    ef: int,
    *,
    threshold: str = "target-025",
    mean_recall: float,
    recall_lcb: float,
    p95: float,
    latency_ucb: float,
    validated_model: bool = True,
) -> ResponseEstimate:
    return ResponseEstimate(
        metric="L2",
        threshold_stratum=threshold,
        ef=ef,
        mean_recall=mean_recall,
        recall_lower_bound_95=recall_lcb,
        p95_latency_ms=p95,
        latency_upper_bound_95_ms=latency_ucb,
        validated_model=validated_model,
        provenance="response-model-v1",
    )


def quality_estimates(*, threshold: str = "target-025") -> dict[int, ResponseEstimate]:
    return {
        200: estimate(
            200,
            threshold=threshold,
            mean_recall=0.93,
            recall_lcb=0.92,
            p95=3.2,
            latency_ucb=3.5,
        ),
        400: estimate(
            400,
            threshold=threshold,
            mean_recall=0.94,
            recall_lcb=0.93,
            p95=4.0,
            latency_ucb=4.2,
        ),
        800: estimate(
            800,
            threshold=threshold,
            mean_recall=0.96,
            recall_lcb=0.955,
            p95=4.6 if threshold != "target-075" else 5.4,
            latency_ucb=4.8 if threshold != "target-075" else 5.6,
        ),
        1600: estimate(
            1600,
            threshold=threshold,
            mean_recall=0.98,
            recall_lcb=0.97,
            p95=7.0,
            latency_ucb=7.5,
        ),
    }


def input_latency_estimates() -> dict[int, ResponseEstimate]:
    return {
        200: estimate(
            200,
            mean_recall=0.965,
            recall_lcb=0.962,
            p95=3.6,
            latency_ucb=3.8,
        ),
        400: estimate(
            400,
            mean_recall=0.97,
            recall_lcb=0.965,
            p95=4.0,
            latency_ucb=4.2,
        ),
        800: estimate(
            800,
            mean_recall=0.98,
            recall_lcb=0.975,
            p95=5.0,
            latency_ucb=5.3,
        ),
        1600: estimate(
            1600,
            mean_recall=0.99,
            recall_lcb=0.985,
            p95=7.0,
            latency_ucb=7.4,
        ),
    }


def pre_action(
    *, threshold: str = "target-025", exception_authorized: bool = False
) -> PreActionSafety:
    return PreActionSafety(
        metric="L2",
        threshold_stratum=threshold,
        configuration_identity=CONFIG_ID,
        index_identity=INDEX_ID,
        flat_index_identity=FLAT_INDEX_ID,
        data_identity=DATA_ID,
        response_model_provenance="response-model-v1",
        exception_authorized=exception_authorized,
    )


def qualification_window(
    sequence: int,
    *,
    ef: int = 400,
    threshold: str = "target-025",
    **changes,
) -> QualificationWindow:
    values = {
        "window_id": f"window-{sequence}",
        "sequence_number": sequence,
        "metric": "L2",
        "threshold_stratum": threshold,
        "ef": ef,
        "mean_recall": 0.97,
        "recall_lower_bound_95": 0.96,
        "p95_latency_ms": 4.0,
        "latency_upper_bound_95_ms": 4.5,
        "configuration_identity": CONFIG_ID,
        "index_identity": INDEX_ID,
        "data_identity": DATA_ID,
    }
    values.update(changes)
    return QualificationWindow(**values)


def qualification_pair(
    *, ef: int = 400, threshold: str = "target-025"
) -> tuple[QualificationWindow, QualificationWindow]:
    return (
        qualification_window(10, ef=ef, threshold=threshold),
        qualification_window(11, ef=ef, threshold=threshold),
    )


def canary(
    *,
    threshold: str = "target-025",
    candidate_recall: float = 0.97,
    candidate_recall_lcb: float = 0.96,
    last_known_good_recall: float = 0.96,
    candidate_p95: float = 4.5,
    candidate_latency_ucb: float = 4.8,
    last_known_good_p95: float = 4.0,
    **changes,
) -> CanaryObservation:
    values = {
        "metric": "L2",
        "threshold_stratum": threshold,
        "candidate_ef": 800,
        "last_known_good_ef": 400,
        "completed_query_count": 50,
        "candidate_mean_recall": candidate_recall,
        "candidate_recall_lower_bound_95": candidate_recall_lcb,
        "last_known_good_mean_recall": last_known_good_recall,
        "candidate_p95_latency_ms": candidate_p95,
        "candidate_latency_upper_bound_95_ms": candidate_latency_ucb,
        "last_known_good_p95_latency_ms": last_known_good_p95,
        "configuration_identity": CONFIG_ID,
        "index_identity": INDEX_ID,
        "data_identity": DATA_ID,
    }
    values.update(changes)
    return CanaryObservation(**values)


def decide(
    *,
    drift: DriftDecision | None = None,
    current_ef: int = 400,
    estimates: dict[int, ResponseEstimate] | None = None,
    safety: PreActionSafety | None = None,
    observation: CanaryObservation | None = None,
    windows: tuple[QualificationWindow, QualificationWindow] | None = None,
    mode: PolicyMode = PolicyMode.CANARY_ENABLED,
    threshold: str = "target-025",
    audit_id: str = AUDIT_ID,
):
    return evaluate_tuning_policy(
        drift or detector(threshold=threshold),
        current_ef=current_ef,
        response_estimates=estimates or quality_estimates(threshold=threshold),
        pre_action=safety or pre_action(threshold=threshold),
        canary_observation=observation,
        qualification_windows=windows or qualification_pair(threshold=threshold),
        mode=mode,
        threshold_stratum=threshold,
        audit_id=audit_id,
    )


class DetectorAndDirectionTests(unittest.TestCase):
    def test_drift_below_old_confidence_floor_is_not_blocked(self) -> None:
        result = decide(
            drift=DriftDecision(
                state=DetectorState.DRIFT,
                classification=DriftClassification.QUALITY_DRIFT,
                significance_evidence_score=0.98,
                drift_magnitude=1.25,
                evidence_provenance=provenance(),
            )
        )

        self.assertEqual(result.action, PolicyAction.START_CANARY)
        self.assertEqual(result.detector_confidence, 0.98)
        self.assertNotIn(
            "DETECTOR_CONFIDENCE",
            {gate.name for gate in result.safety_gate_results},
        )

    def test_drift_without_valid_provenance_cannot_recommend_or_start_canary(self) -> None:
        result = decide(drift=replace(detector(), evidence_provenance=None))

        self.assertEqual(result.action, PolicyAction.NO_CHANGE)
        self.assertEqual(result.reason, "EVIDENCE_PROVENANCE_MISSING")
        self.assertTrue(result.alert_required)

    def test_drift_with_pre_action_identity_mismatch_cannot_recommend(self) -> None:
        result = decide(
            drift=replace(
                detector(),
                evidence_provenance=provenance(
                    configuration_identity="unexpected-configuration"
                ),
            )
        )

        self.assertEqual(result.action, PolicyAction.NO_CHANGE)
        self.assertEqual(result.reason, "EVIDENCE_PROVENANCE_MISMATCH")
        self.assertFalse(result.safety_gate_results[0].passed)

    def test_policy_output_carries_the_detector_provenance(self) -> None:
        evidence = provenance()
        result = decide(drift=replace(detector(), evidence_provenance=evidence))

        self.assertEqual(result.evidence_provenance, evidence)

    def test_no_drift_and_insufficient_evidence_emit_no_change(self) -> None:
        cases = (
            (DetectorState.NO_DRIFT, "DETECTOR_NO_DRIFT"),
            (
                DetectorState.INSUFFICIENT_EVIDENCE,
                "DETECTOR_INSUFFICIENT_EVIDENCE",
            ),
        )
        for state, reason in cases:
            with self.subTest(state=state):
                result = decide(
                    drift=detector(state=state, classification=DriftClassification.NONE)
                )
                self.assertEqual(result.action, PolicyAction.NO_CHANGE)
                self.assertEqual(result.reason, reason)

    def test_input_drift_at_or_above_floor_selects_next_lower_ef(self) -> None:
        result = decide(
            drift=detector(classification=DriftClassification.INPUT_DRIFT),
            estimates=input_latency_estimates(),
        )
        self.assertEqual(result.action, PolicyAction.START_CANARY)
        self.assertEqual(result.candidate_ef, 200)
        self.assertAlmostEqual(result.predicted_latency_reduction_fraction, 0.10)
        self.assertAlmostEqual(result.predicted_recall_improvement, -0.005)

    def test_input_drift_below_floor_selects_next_higher_ef(self) -> None:
        result = decide(drift=detector(classification=DriftClassification.INPUT_DRIFT))
        self.assertEqual(result.action, PolicyAction.START_CANARY)
        self.assertEqual(result.candidate_ef, 800)
        self.assertAlmostEqual(result.predicted_recall_improvement, 0.02)

    def test_quality_and_mixed_drift_select_only_next_higher_ef(self) -> None:
        for classification in (
            DriftClassification.QUALITY_DRIFT,
            DriftClassification.INPUT_AND_QUALITY_DRIFT,
        ):
            with self.subTest(classification=classification):
                result = decide(drift=detector(classification=classification))
                self.assertEqual(result.action, PolicyAction.START_CANARY)
                self.assertEqual(result.candidate_ef, 800)

    def test_quality_drift_at_ef_1600_emits_no_change_and_alert(self) -> None:
        estimates = quality_estimates()
        estimates[1600] = replace(
            estimates[1600], mean_recall=0.94, recall_lower_bound_95=0.93
        )
        result = decide(
            current_ef=1600,
            estimates=estimates,
            windows=qualification_pair(ef=1600),
        )
        self.assertEqual(result.action, PolicyAction.NO_CHANGE)
        self.assertEqual(result.reason, "QUALITY_SLO_UNSATISFIED_AT_MAX_EF")
        self.assertTrue(result.alert_required)

    def test_dry_run_never_starts_canary(self) -> None:
        result = decide(mode=PolicyMode.DRY_RUN)
        self.assertEqual(result.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(result.reason, "DRY_RUN_RECOMMENDATION")
        self.assertEqual(result.candidate_ef, 800)


class LastKnownGoodTests(unittest.TestCase):
    def test_exactly_two_consecutive_matching_windows_qualify(self) -> None:
        result = qualify_last_known_good(qualification_pair(), audit_id=AUDIT_ID)
        self.assertTrue(result.qualified)
        self.assertEqual(result.ef, 400)
        self.assertEqual(result.configuration_identity, CONFIG_ID)

    def test_ef_100_can_never_qualify(self) -> None:
        result = qualify_last_known_good(qualification_pair(ef=100), audit_id=AUDIT_ID)
        self.assertFalse(result.qualified)
        self.assertIsNone(result.ef)
        self.assertIn("QUALIFICATION_EF_INELIGIBLE", result.reasons)

    def test_nonconsecutive_or_identity_mismatched_windows_fail(self) -> None:
        first = qualification_window(10)
        second = qualification_window(12, data_identity="different-dataset")
        result = qualify_last_known_good((first, second), audit_id=AUDIT_ID)
        self.assertFalse(result.qualified)
        self.assertIn("QUALIFICATION_WINDOWS_NOT_CONSECUTIVE", result.reasons)
        self.assertIn("QUALIFICATION_DATA_IDENTITY_MISMATCH", result.reasons)


class PreActionAndExceptionTests(unittest.TestCase):
    def test_output_contains_bounds_gate_results_and_passed_audit_id(self) -> None:
        result = decide()
        self.assertEqual(result.action, PolicyAction.START_CANARY)
        self.assertEqual(result.current_ef, 400)
        self.assertEqual(result.candidate_ef, 800)
        self.assertEqual(result.last_known_good_ef, 400)
        self.assertEqual(result.expected_mean_recall, 0.96)
        self.assertEqual(result.expected_recall_lower_bound_95, 0.955)
        self.assertEqual(result.expected_p95_latency_ms, 4.6)
        self.assertEqual(result.expected_latency_upper_bound_95_ms, 4.8)
        self.assertEqual(result.audit_id, AUDIT_ID)
        self.assertTrue(all(gate.passed for gate in result.safety_gate_results))

    def test_l2_target_075_exception_allows_400_to_800_at_1_40x(self) -> None:
        result = decide(
            estimates=quality_estimates(threshold="target-075"),
            safety=pre_action(threshold="target-075", exception_authorized=True),
            windows=qualification_pair(threshold="target-075"),
            threshold="target-075",
        )
        self.assertEqual(result.action, PolicyAction.START_CANARY)
        relative = next(
            gate
            for gate in result.safety_gate_results
            if gate.name == "RELATIVE_LATENCY_CEILING"
        )
        self.assertTrue(relative.passed)
        self.assertIn("1.50x", relative.detail)

    def test_exception_identity_or_authorization_mismatch_uses_1_25x(self) -> None:
        result = decide(
            estimates=quality_estimates(threshold="target-075"),
            safety=pre_action(threshold="target-075", exception_authorized=False),
            windows=qualification_pair(threshold="target-075"),
            threshold="target-075",
        )
        self.assertEqual(result.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(result.reason, "SAFETY_GATE_FAILED:RELATIVE_LATENCY_CEILING")
        relative = next(
            gate
            for gate in result.safety_gate_results
            if gate.name == "RELATIVE_LATENCY_CEILING"
        )
        self.assertIn("1.25x", relative.detail)

    def test_truthy_nonboolean_exception_authorization_is_rejected(self) -> None:
        result = decide(
            estimates=quality_estimates(threshold="target-075"),
            safety=replace(pre_action(threshold="target-075"), exception_authorized=1),
            windows=qualification_pair(threshold="target-075"),
            threshold="target-075",
        )
        self.assertEqual(result.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(result.reason, "SAFETY_GATE_FAILED:RELATIVE_LATENCY_CEILING")

    def test_exception_requires_qualified_last_known_good_ef_400(self) -> None:
        estimates = quality_estimates(threshold="target-075")
        estimates[200] = replace(
            estimates[200],
            mean_recall=0.94,
            recall_lower_bound_95=0.93,
            p95_latency_ms=4.0,
            latency_upper_bound_95_ms=4.2,
        )
        result = decide(
            estimates=estimates,
            safety=pre_action(threshold="target-075", exception_authorized=True),
            windows=qualification_pair(ef=200, threshold="target-075"),
            threshold="target-075",
        )
        self.assertEqual(result.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(result.reason, "SAFETY_GATE_FAILED:RELATIVE_LATENCY_CEILING")

    def test_unvalidated_input_drift_model_stays_recommendation_only(self) -> None:
        estimates = input_latency_estimates()
        estimates = {
            ef: replace(item, validated_model=False) for ef, item in estimates.items()
        }
        result = decide(
            drift=detector(classification=DriftClassification.INPUT_DRIFT),
            estimates=estimates,
        )
        self.assertEqual(result.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(result.reason, "SAFETY_GATE_FAILED:RESPONSE_MODEL_VALIDATED")

    def test_truthy_nonboolean_and_noninteger_zero_fail_pre_action(self) -> None:
        cases = (
            (
                replace(pre_action(), milvus_healthy=1),
                "SAFETY_GATE_FAILED:SERVICES_HEALTHY",
            ),
            (
                replace(pre_action(), current_failed_query_count=0.0),
                "SAFETY_GATE_FAILED:CURRENT_QUERY_HEALTH",
            ),
        )
        for safety, reason in cases:
            with self.subTest(reason=reason):
                result = decide(safety=safety)
                self.assertEqual(result.action, PolicyAction.RECOMMEND_EF)
                self.assertEqual(result.reason, reason)


class AuditIdentityTests(unittest.TestCase):
    def test_missing_audit_id_without_active_canary_is_no_change(self) -> None:
        result = decide(audit_id="")
        self.assertEqual(result.action, PolicyAction.NO_CHANGE)
        self.assertEqual(result.reason, "AUDIT_ID_MISSING")
        self.assertEqual(result.audit_id, "")

    def test_missing_audit_id_with_active_canary_is_rollback(self) -> None:
        result = decide(observation=canary(), audit_id="")
        self.assertEqual(result.action, PolicyAction.ROLLBACK)
        self.assertEqual(result.reason, "AUDIT_ID_MISSING")
        self.assertEqual(result.audit_id, "")

    def test_missing_active_canary_audit_record_is_rollback(self) -> None:
        result = decide(observation=canary(audit_record_present=False))
        self.assertEqual(result.action, PolicyAction.ROLLBACK)
        self.assertEqual(result.reason, "AUDIT_ID_MISSING")


class CanaryRollbackTests(unittest.TestCase):
    def test_each_immediate_hard_failure_category_rolls_back(self) -> None:
        cases = (
            ({"failed_query_count": 1}, "QUERY_FAILURE"),
            ({"timeout_query_count": 1}, "QUERY_TIMEOUT"),
            ({"threshold_violation_count": 1}, "THRESHOLD_VIOLATION"),
            ({"flat_oracle_agreement": False}, "FLAT_ORACLE_DISAGREEMENT"),
            ({"milvus_healthy": False}, "REQUIRED_SERVICE_UNHEALTHY"),
            ({"collection_loaded": False}, "COLLECTION_UNLOADED"),
            (
                {"configuration_valid": False},
                "CONFIGURATION_VALIDATION_FAILURE",
            ),
            ({"index_identity_unchanged": False}, "INDEX_IDENTITY_CHANGED"),
            ({"actuation_exception": True}, "ACTUATION_EXCEPTION"),
            (
                {"data_identity": "unexpected-data"},
                "CONFIGURATION_VALIDATION_FAILURE",
            ),
        )
        for changes, reason in cases:
            with self.subTest(reason=reason):
                result = decide(observation=canary(**changes))
                self.assertEqual(result.action, PolicyAction.ROLLBACK)
                self.assertEqual(result.reason, reason)

    def test_canary_current_ef_or_direction_mismatch_rolls_back(self) -> None:
        downward_quality_canary = canary(candidate_ef=200)
        cases = (
            (
                decide(observation=downward_quality_canary),
                "quality drift cannot move downward",
            ),
            (
                decide(current_ef=800, observation=canary()),
                "current ef must equal canary last-known-good",
            ),
        )
        for result, description in cases:
            with self.subTest(description=description):
                self.assertEqual(result.action, PolicyAction.ROLLBACK)
                self.assertEqual(result.reason, "CONFIGURATION_VALIDATION_FAILURE")

    def test_active_canary_must_match_explicit_threshold_input(self) -> None:
        result = decide(
            estimates=quality_estimates(),
            safety=pre_action(),
            observation=canary(),
            windows=qualification_pair(),
            threshold="target-075",
        )
        self.assertEqual(result.action, PolicyAction.ROLLBACK)
        self.assertEqual(result.reason, "CONFIGURATION_VALIDATION_FAILURE")

    def test_rollback_is_allowed_in_dry_run_mode(self) -> None:
        result = decide(
            observation=canary(failed_query_count=1), mode=PolicyMode.DRY_RUN
        )
        self.assertEqual(result.action, PolicyAction.ROLLBACK)
        self.assertEqual(result.mode, PolicyMode.DRY_RUN)

    def test_nonfinite_in_progress_canary_evidence_rolls_back_immediately(self) -> None:
        result = decide(
            observation=canary(
                completed_query_count=10,
                candidate_recall=float("nan"),
            )
        )
        self.assertEqual(result.action, PolicyAction.ROLLBACK)
        self.assertEqual(result.reason, "CONFIGURATION_VALIDATION_FAILURE")

    def test_recall_floor_and_paired_degradation_failures_roll_back(self) -> None:
        cases = (
            (
                canary(
                    candidate_recall=0.96,
                    candidate_recall_lcb=0.949,
                ),
                "RECALL_FLOOR_FAILURE",
            ),
            (
                canary(
                    candidate_recall=0.975,
                    candidate_recall_lcb=0.969,
                    last_known_good_recall=0.98,
                ),
                "PAIRED_RECALL_DEGRADATION_FAILURE",
            ),
        )
        for observation, reason in cases:
            with self.subTest(reason=reason):
                result = decide(observation=observation)
                self.assertEqual(result.action, PolicyAction.ROLLBACK)
                self.assertEqual(result.reason, reason)

    def test_absolute_and_relative_latency_failures_roll_back(self) -> None:
        cases = (
            (
                canary(
                    candidate_p95=9.8,
                    candidate_latency_ucb=10.1,
                    last_known_good_p95=9.0,
                ),
                "ABSOLUTE_LATENCY_FAILURE",
            ),
            (
                canary(candidate_p95=5.0, candidate_latency_ucb=5.1),
                "RELATIVE_LATENCY_FAILURE",
            ),
        )
        for observation, reason in cases:
            with self.subTest(reason=reason):
                result = decide(observation=observation)
                self.assertEqual(result.action, PolicyAction.ROLLBACK)
                self.assertEqual(result.reason, reason)

    def test_completed_passing_canary_emits_no_change(self) -> None:
        result = decide(observation=canary())
        self.assertEqual(result.action, PolicyAction.NO_CHANGE)
        self.assertEqual(result.reason, "CANARY_PASSED")


class CanaryExceptionTests(unittest.TestCase):
    def test_exact_exception_canary_passes_at_1_40x_with_0_006_recall_gain(
        self,
    ) -> None:
        observation = canary(
            threshold="target-075",
            candidate_recall=0.96,
            candidate_recall_lcb=0.956,
            last_known_good_recall=0.95,
            candidate_p95=5.4,
            candidate_latency_ucb=5.6,
            last_known_good_p95=4.0,
        )
        result = decide(
            estimates=quality_estimates(threshold="target-075"),
            safety=pre_action(threshold="target-075", exception_authorized=True),
            observation=observation,
            windows=qualification_pair(threshold="target-075"),
            threshold="target-075",
        )
        self.assertEqual(result.action, PolicyAction.NO_CHANGE)
        self.assertEqual(result.reason, "CANARY_PASSED")

    def test_exception_rolls_back_when_conservative_recall_gain_is_0_004(self) -> None:
        observation = canary(
            threshold="target-075",
            candidate_recall=0.96,
            candidate_recall_lcb=0.954,
            last_known_good_recall=0.95,
            candidate_p95=5.4,
            candidate_latency_ucb=5.6,
            last_known_good_p95=4.0,
        )
        result = decide(
            estimates=quality_estimates(threshold="target-075"),
            safety=pre_action(threshold="target-075", exception_authorized=True),
            observation=observation,
            windows=qualification_pair(threshold="target-075"),
            threshold="target-075",
        )
        self.assertEqual(result.action, PolicyAction.ROLLBACK)
        self.assertEqual(result.reason, "EXCEPTION_RECALL_IMPROVEMENT_FAILURE")

    def test_nonmatching_stratum_uses_standard_ceiling_and_rolls_back(self) -> None:
        result = decide(
            observation=canary(
                candidate_p95=5.4,
                candidate_latency_ucb=5.6,
                last_known_good_p95=4.0,
            ),
            safety=pre_action(exception_authorized=True),
        )
        self.assertEqual(result.action, PolicyAction.ROLLBACK)
        self.assertEqual(result.reason, "RELATIVE_LATENCY_FAILURE")


class OfflineBoundaryTests(unittest.TestCase):
    def test_policy_module_has_no_pymilvus_or_backend_adapter_import(self) -> None:
        source = (
            Path(__file__).resolve().parents[1] / "src" / "vdbench" / "policy.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("pymilvus", source.lower())
        self.assertNotIn("from .milvus", source)
        self.assertNotIn("import vdbench.milvus", source)


if __name__ == "__main__":
    unittest.main()
