"""Hand-checkable tests for EXP-009's offline statistical contract."""

from __future__ import annotations

import unittest
from math import comb, log, sqrt

from vdbench.canary_statistics import (
    EXP009_CANDIDATE_COUNT,
    EXP009_RECALL_AUDIT_COUNT,
    EXP009_ROUTING_POPULATION_COUNT,
    FinitePopulationP95Bound,
    RecallLowerBound,
    exp009_latency_bound_contract,
    one_sided_hoeffding_recall_lower_bound,
)


class CanaryStatisticsTests(unittest.TestCase):
    def test_finite_population_p95_coverage_matches_hypergeometric_formula(self) -> None:
        result = exp009_latency_bound_contract()

        self.assertIsInstance(result, FinitePopulationP95Bound)
        self.assertEqual(result.population_size, 600)
        self.assertEqual(result.sample_size, 60)
        self.assertEqual(result.percentile_rank, 570)
        self.assertEqual(result.conservative_tail_count, 30)
        self.assertAlmostEqual(
            result.coverage_probability,
            1 - comb(570, 60) / comb(600, 60),
            places=15,
        )
        self.assertGreater(result.coverage_probability, 0.95)

    def test_exp009_constants_are_exactly_the_preregistered_contract(self) -> None:
        self.assertEqual(EXP009_ROUTING_POPULATION_COUNT, 600)
        self.assertEqual(EXP009_CANDIDATE_COUNT, 60)
        self.assertEqual(EXP009_RECALL_AUDIT_COUNT, 1200)

    def test_hoeffding_bound_for_all_perfect_recalls_is_hand_computable(self) -> None:
        result = one_sided_hoeffding_recall_lower_bound([1.0] * 1200)

        self.assertIsInstance(result, RecallLowerBound)
        expected_margin = sqrt(log(20) / (2 * 1200))
        self.assertAlmostEqual(result.observed_mean, 1.0, places=15)
        self.assertAlmostEqual(result.margin, expected_margin, places=15)
        self.assertAlmostEqual(result.lower_bound, 1.0 - expected_margin, places=15)
        self.assertEqual(result.confidence_level, 0.95)

    def test_hoeffding_bound_is_clamped_to_the_unit_interval(self) -> None:
        result = one_sided_hoeffding_recall_lower_bound([0.0] * 1200)

        self.assertEqual(result.lower_bound, 0.0)

    def test_recall_bound_rejects_non_contract_sample_size_and_invalid_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly 1200"):
            one_sided_hoeffding_recall_lower_bound([1.0] * 1199)
        with self.assertRaisesRegex(ValueError, r"within \[0, 1\]"):
            one_sided_hoeffding_recall_lower_bound([1.1] * 1200)
        with self.assertRaisesRegex(ValueError, r"within \[0, 1\]"):
            one_sided_hoeffding_recall_lower_bound([float("nan")] * 1200)


if __name__ == "__main__":
    unittest.main()
