import math
import unittest
from dataclasses import fields

import numpy as np

from vdbench.drift import (
    DetectorState,
    DriftClassification,
    DriftDecision,
    RecallAuditSample,
    Signal,
    SignalEvidence,
    canonical_serialize_tuple,
    derive_permutation_seed,
    deterministic_permutation_p_value,
    evaluate_drift_decision,
    finalize_window_evidence,
    holm_step_down,
    ks_signal_test,
    mmd_squared,
    query_vector_signal_test,
    recall_signal_test,
    select_audit_sample,
    two_sample_ks_statistic,
    _prepare_mmd,
)


EFFECT_FLOORS = {
    Signal.QUERY_VECTOR: 0.01,
    Signal.THRESHOLD: 0.20,
    Signal.CARDINALITY: 0.20,
    Signal.RECALL: 0.02,
}
EXPECTED_COUNTS = {
    Signal.QUERY_VECTOR: 200,
    Signal.THRESHOLD: 200,
    Signal.CARDINALITY: 50,
    Signal.RECALL: 50,
}


def signal_evidence(
    signal: Signal, *, p_value: float = 1.0, effect: float = 0.0
) -> SignalEvidence:
    count = EXPECTED_COUNTS[signal]
    return SignalEvidence(
        signal=signal,
        complete=True,
        reference_count=count,
        current_count=count,
        statistic=effect,
        effect=effect,
        effect_floor=EFFECT_FLOORS[signal],
        raw_p_value=p_value,
    )


def complete_window(
    window_id: str, breaches: tuple[Signal, ...] = ()
):
    signals = [
        signal_evidence(
            signal,
            p_value=0.001 if signal in breaches else 1.0,
            effect=(EFFECT_FLOORS[signal] + 0.10) if signal in breaches else 0.0,
        )
        for signal in Signal
    ]
    return finalize_window_evidence(
        metric="L2", window_id=window_id, signals=signals
    )


def recall_sample(
    window_id: str,
    values: np.ndarray,
    *,
    observed_count: int = 50,
) -> RecallAuditSample:
    expected = tuple(range(50))
    observed = tuple(range(observed_count))
    return RecallAuditSample(
        window_id=window_id,
        metric="L2",
        expected_audit_ids=expected,
        observed_audit_ids=observed,
        values=values,
        flat_oracle_agreement=np.ones(values.shape, dtype=bool),
        collection_data_identity="dataset-v1",
        index_build_identity="hnsw-m16-efc200",
    )


class SerializationAndPermutationTests(unittest.TestCase):
    def test_canonical_serialization_and_sha256_seed_match_fixed_vector(self) -> None:
        serialized = canonical_serialize_tuple((7, "L2", "window-1", "THRESHOLD"))
        self.assertEqual(
            serialized.hex(),
            "0000000400000000000000013700000000000000024c32"
            "000000000000000877696e646f772d310000000000000009"
            "5448524553484f4c44",
        )
        seed = derive_permutation_seed(7, "L2", "window-1", "THRESHOLD")
        self.assertEqual(seed.seed_u64, 9_284_828_618_250_786_414)
        self.assertEqual(
            seed.sha256,
            "80da566f6738166ecabb345e95cf388eb5887f018c8a5da692009dd88d791632",
        )

    def test_permutation_p_value_is_exactly_one_for_constant_statistic(self) -> None:
        result = deterministic_permutation_p_value(
            observed_statistic=0.0,
            total_count=4,
            reference_count=2,
            detector_seed=3,
            metric="L2",
            window_id="constant",
            signal="THRESHOLD",
            batch_statistic=lambda membership: np.zeros(
                membership.shape[0], dtype=np.float64
            ),
        )
        self.assertEqual(result.exceedance_count, 9_999)
        self.assertEqual(result.permutation_count, 9_999)
        self.assertEqual(result.p_value, 1.0)


class StatisticTests(unittest.TestCase):
    def test_l2_pooled_preprocessing_is_symmetric(self) -> None:
        reference = [[0.0], [2.0]]
        current = [[4.0], [6.0]]

        forward = _prepare_mmd(reference, current, metric="L2")
        reversed_groups = _prepare_mmd(current, reference, metric="L2")

        self.assertAlmostEqual(forward.sigma, 3.0 / math.sqrt(5.0), places=15)
        self.assertAlmostEqual(reversed_groups.sigma, forward.sigma, places=15)
        self.assertAlmostEqual(
            reversed_groups.statistic, forward.statistic, places=15
        )

    def test_l2_reference_only_zero_variance_dimension_is_not_excluded(self) -> None:
        prepared = _prepare_mmd(
            [[0.0, 5.0], [2.0, 5.0]],
            [[4.0, 4.0], [6.0, 6.0]],
            metric="L2",
        )

        self.assertEqual(prepared.excluded_dimension_count, 0)
        self.assertEqual(prepared.excluded_dimension_indices, ())

    def test_l2_pooled_zero_variance_dimension_is_excluded_and_recorded(
        self,
    ) -> None:
        reference = [[0.0, 5.0], [2.0, 5.0]]
        current = [[4.0, 5.0], [6.0, 5.0]]

        prepared = _prepare_mmd(reference, current, metric="L2")
        evidence = query_vector_signal_test(
            reference,
            current,
            metric="L2",
            detector_seed=7,
            window_id="pooled-zero-variance",
        )

        self.assertEqual(prepared.excluded_dimension_count, 1)
        self.assertEqual(prepared.excluded_dimension_indices, (1,))
        self.assertTrue(evidence.complete)
        self.assertEqual(evidence.excluded_dimension_count, 1)
        self.assertEqual(evidence.excluded_dimension_indices, (1,))

    def test_zero_median_from_off_diagonal_duplicates_is_insufficient(self) -> None:
        evidence = query_vector_signal_test(
            [[0.0], [0.0], [0.0]],
            [[0.0], [0.0], [1.0]],
            metric="L2",
            detector_seed=7,
            window_id="zero-pooled-median",
        )

        self.assertFalse(evidence.complete)
        self.assertIsNone(evidence.statistic)
        self.assertIn("sigma must be positive", evidence.reason)

    def test_mmd_squared_matches_hand_computed_two_by_two_kernel(self) -> None:
        result = mmd_squared([[0.0], [1.0]], [[2.0], [3.0]], metric="L2")
        expected = 2.0 * math.exp(-2.0 / 9.0) - 0.5 * (
            2.0 * math.exp(-8.0 / 9.0)
            + math.exp(-2.0)
            + math.exp(-2.0 / 9.0)
        )
        self.assertAlmostEqual(result.sigma, 3.0 / math.sqrt(5.0), places=15)
        self.assertAlmostEqual(result.statistic, expected, places=15)

    def test_mmd_permutation_path_matches_fixed_small_contract_vector(self) -> None:
        evidence = query_vector_signal_test(
            [[0.0], [1.0]],
            [[2.0], [3.0]],
            metric="L2",
            detector_seed=7,
            window_id="mmd-small",
        )
        self.assertTrue(evidence.complete)
        self.assertAlmostEqual(evidence.statistic, 0.7223261722497185, places=15)
        self.assertEqual(evidence.raw_p_value, 0.3323)
        self.assertEqual(evidence.seed.seed_u64, 10_010_057_822_588_224_695)

    def test_l2_pooled_variable_dimension_contributes_to_mmd(self) -> None:
        one_dimension = mmd_squared(
            [[0.0], [1.0]], [[2.0], [3.0]], metric="L2"
        )
        with_constant_reference_dimension = mmd_squared(
            [[0.0, 5.0], [1.0, 5.0]],
            [[2.0, 999.0], [3.0, -999.0]],
            metric="L2",
        )
        self.assertNotEqual(
            with_constant_reference_dimension.sigma, one_dimension.sigma
        )
        self.assertNotAlmostEqual(
            with_constant_reference_dimension.statistic,
            one_dimension.statistic,
            places=15,
        )

    def test_cosine_inputs_are_normalized_before_mmd(self) -> None:
        base = np.array([[1.0, 0.0], [0.0, 1.0]])
        scaled = np.array([[10.0, 0.0], [0.0, 3.0]])
        result = mmd_squared(base, scaled, metric="COSINE")
        self.assertLessEqual(result.statistic, 0.0)
        self.assertAlmostEqual(result.sigma, math.sqrt(2.0), places=15)

    def test_cosine_sigma_uses_pooled_normalized_vectors(self) -> None:
        result = mmd_squared(
            [[1.0, 0.0], [2.0, 0.0]],
            [[0.0, 1.0], [0.0, -1.0]],
            metric="COSINE",
        )

        self.assertAlmostEqual(result.sigma, math.sqrt(2.0), places=15)

    def test_two_sample_ks_matches_hand_computed_empirical_cdfs(self) -> None:
        self.assertEqual(two_sample_ks_statistic([0.0, 1.0], [0.0, 2.0]), 0.5)
        self.assertEqual(two_sample_ks_statistic([0.0, 1.0], [0.0, 1.0]), 0.0)

    def test_identical_threshold_distributions_have_p_one_and_no_effect(self) -> None:
        values = np.repeat(np.arange(4, dtype=np.float64), 50)
        evidence = ks_signal_test(
            values,
            values.copy(),
            signal="THRESHOLD",
            metric="L2",
            detector_seed=11,
            window_id="identical",
        )
        self.assertTrue(evidence.complete)
        self.assertEqual(evidence.statistic, 0.0)
        self.assertEqual(evidence.effect, 0.0)
        self.assertEqual(evidence.raw_p_value, 1.0)

    def test_exact_cardinality_signal_rejects_fractional_values(self) -> None:
        evidence = ks_signal_test(
            np.zeros(50, dtype=np.float64),
            np.full(50, 1.5, dtype=np.float64),
            signal="CARDINALITY",
            metric="L2",
            detector_seed=11,
            window_id="fractional-cardinality",
        )
        self.assertFalse(evidence.complete)
        self.assertIsNone(evidence.raw_p_value)
        self.assertIn("non-negative integers", evidence.reason)

    def test_holm_adjustment_matches_hand_computed_step_down_values(self) -> None:
        adjusted = holm_step_down(
            {
                Signal.QUERY_VECTOR: 0.001,
                Signal.THRESHOLD: 0.01,
                Signal.CARDINALITY: 0.03,
                Signal.RECALL: 0.20,
            }
        )
        self.assertEqual(adjusted[Signal.QUERY_VECTOR], 0.004)
        self.assertEqual(adjusted[Signal.THRESHOLD], 0.03)
        self.assertEqual(adjusted[Signal.CARDINALITY], 0.06)
        self.assertEqual(adjusted[Signal.RECALL], 0.20)


class AuditSelectionTests(unittest.TestCase):
    def test_blake2b_selection_matches_fixed_contract_vector(self) -> None:
        first = select_audit_sample(
            tuple(range(200)),
            detector_seed=20260801,
            metric="L2",
            window_id="window-001",
        )
        second = select_audit_sample(
            tuple(reversed(range(200))),
            detector_seed=20260801,
            metric="L2",
            window_id="window-001",
        )
        self.assertTrue(first.complete)
        self.assertEqual(first, second)
        self.assertEqual(len(first.query_ids), 50)
        self.assertEqual(first.query_ids[:5], (106, 166, 53, 153, 60))
        self.assertEqual(
            first.digest_hex[:2],
            (
                "003b628b7a1509ddca76392322a9d6e907e395c2ecf7b03e8bf41f03330cc610",
                "026af5f48465901e644ac0b51acceae6223cb6e8e35d2959ac3ce47c19738e1e",
            ),
        )

    def test_incomplete_or_duplicate_query_ids_fail_closed(self) -> None:
        missing = select_audit_sample(
            tuple(range(199)),
            detector_seed=1,
            metric="L2",
            window_id="missing",
        )
        duplicate = select_audit_sample(
            tuple(range(199)) + (198,),
            detector_seed=1,
            metric="L2",
            window_id="duplicate",
        )
        self.assertFalse(missing.complete)
        self.assertFalse(duplicate.complete)
        self.assertIn("exactly 200", missing.reason)
        self.assertIn("unique", duplicate.reason)


class RecallSignalTests(unittest.TestCase):
    def test_identical_complete_recall_samples_have_p_one(self) -> None:
        values = np.full(50, 0.95, dtype=np.float64)
        evidence = recall_signal_test(
            recall_sample("reference", values),
            recall_sample("current", values.copy()),
            detector_seed=19,
        )
        self.assertTrue(evidence.complete)
        self.assertAlmostEqual(evidence.statistic, 0.0, places=15)
        self.assertEqual(evidence.raw_p_value, 1.0)

    def test_recall_input_with_49_values_is_insufficient(self) -> None:
        reference = recall_sample("reference", np.ones(50, dtype=np.float64))
        current = recall_sample(
            "current", np.ones(49, dtype=np.float64), observed_count=49
        )
        evidence = recall_signal_test(reference, current, detector_seed=19)
        self.assertFalse(evidence.complete)
        self.assertIsNone(evidence.raw_p_value)
        self.assertIn("exactly 50", evidence.reason)

    def test_recall_degradation_has_hand_computable_effect_and_minimum_p(self) -> None:
        reference = recall_sample("reference", np.ones(50, dtype=np.float64))
        current = recall_sample("current", np.zeros(50, dtype=np.float64))
        evidence = recall_signal_test(reference, current, detector_seed=23)
        self.assertTrue(evidence.complete)
        self.assertEqual(evidence.statistic, 1.0)
        self.assertEqual(evidence.effect, 1.0)
        self.assertEqual(evidence.raw_p_value, 0.0001)


class DegenerateEvidenceTests(unittest.TestCase):
    def test_zero_norm_cosine_vector_is_insufficient(self) -> None:
        reference = np.tile(np.array([[1.0, 0.0], [0.0, 1.0]]), (100, 1))
        current = reference.copy()
        current[0] = 0.0
        evidence = query_vector_signal_test(
            reference,
            current,
            metric="COSINE",
            detector_seed=5,
            window_id="zero-norm",
        )
        self.assertFalse(evidence.complete)
        self.assertIsNone(evidence.raw_p_value)
        self.assertIn("non-zero norm", evidence.reason)

    def test_zero_or_undefined_sigma_is_insufficient(self) -> None:
        values = np.ones((200, 3), dtype=np.float64)
        evidence = query_vector_signal_test(
            values,
            values.copy(),
            metric="L2",
            detector_seed=5,
            window_id="undefined-sigma",
        )
        self.assertFalse(evidence.complete)
        self.assertIsNone(evidence.statistic)
        self.assertIn("sigma is undefined", evidence.reason)
        self.assertEqual(evidence.excluded_dimension_count, 3)
        self.assertEqual(evidence.excluded_dimension_indices, (0, 1, 2))


class DecisionTests(unittest.TestCase):
    def test_drift_decision_exposes_significance_evidence_score_only(self) -> None:
        decision = DriftDecision(
            state=DetectorState.DRIFT,
            classification=DriftClassification.INPUT_DRIFT,
            significance_evidence_score=0.98,
            drift_magnitude=1.25,
        )

        field_names = {field.name for field in fields(DriftDecision)}
        self.assertEqual(decision.significance_evidence_score, 0.98)
        self.assertIn("significance_evidence_score", field_names)
        self.assertNotIn("decision_confidence", field_names)
        self.assertFalse(hasattr(decision, "decision_confidence"))

    def test_identical_no_breach_windows_emit_no_drift(self) -> None:
        decision = evaluate_drift_decision(
            complete_window("previous"), complete_window("current")
        )
        self.assertEqual(decision.state, DetectorState.NO_DRIFT)
        self.assertEqual(decision.classification, DriftClassification.NONE)

    def test_single_breached_window_is_insufficient_not_drift(self) -> None:
        decision = evaluate_drift_decision(
            complete_window("previous"),
            complete_window("current", (Signal.THRESHOLD,)),
        )
        self.assertEqual(decision.state, DetectorState.INSUFFICIENT_EVIDENCE)
        self.assertEqual(decision.classification, DriftClassification.NONE)
        self.assertEqual(decision.reason_codes, ("PENDING_CONFIRMATION",))

    def test_two_consecutive_input_and_quality_breaches_are_classified(self) -> None:
        breaches = (Signal.THRESHOLD, Signal.RECALL)
        decision = evaluate_drift_decision(
            complete_window("previous", breaches),
            complete_window("current", breaches),
        )
        self.assertEqual(decision.state, DetectorState.DRIFT)
        self.assertEqual(
            decision.classification,
            DriftClassification.INPUT_AND_QUALITY_DRIFT,
        )
        self.assertEqual(
            decision.triggering_signals, (Signal.THRESHOLD, Signal.RECALL)
        )
        self.assertGreaterEqual(decision.significance_evidence_score, 0.99)
        self.assertGreaterEqual(decision.drift_magnitude, 1.0)

    def test_single_class_consecutive_breaches_preserve_attribution(self) -> None:
        cases = (
            (Signal.THRESHOLD, DriftClassification.INPUT_DRIFT),
            (Signal.RECALL, DriftClassification.QUALITY_DRIFT),
        )
        for signal, classification in cases:
            with self.subTest(signal=signal):
                decision = evaluate_drift_decision(
                    complete_window(f"previous-{signal.value}", (signal,)),
                    complete_window(f"current-{signal.value}", (signal,)),
                )
                self.assertEqual(decision.state, DetectorState.DRIFT)
                self.assertEqual(decision.classification, classification)

    def test_missing_or_incomplete_window_never_becomes_no_drift(self) -> None:
        missing_previous = evaluate_drift_decision(
            None, complete_window("current")
        )
        incomplete = finalize_window_evidence(
            metric="L2",
            window_id="incomplete",
            signals=[signal_evidence(signal) for signal in Signal],
            eligible_query_count=199,
        )
        incomplete_current = evaluate_drift_decision(
            complete_window("previous"), incomplete
        )
        self.assertEqual(
            missing_previous.state, DetectorState.INSUFFICIENT_EVIDENCE
        )
        self.assertEqual(
            incomplete_current.state, DetectorState.INSUFFICIENT_EVIDENCE
        )
        self.assertNotEqual(missing_previous.state, DetectorState.NO_DRIFT)
        self.assertNotEqual(incomplete_current.state, DetectorState.NO_DRIFT)


if __name__ == "__main__":
    unittest.main()
