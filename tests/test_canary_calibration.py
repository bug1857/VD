"""TDD coverage for EXP-009's pre-registered offline diagnostics."""

from __future__ import annotations

import unittest

from vdbench.canary_calibration import (
    FINITE_POPULATION_CALIBRATION_REPLAYS,
    FINITE_POPULATION_CALIBRATION_SEED,
    RECALL_CALIBRATION_MEANS,
    RECALL_CALIBRATION_REPLAYS,
    RECALL_CALIBRATION_SEED,
    FinitePopulationCalibration,
    RecallCalibration,
    run_exp009_calibration,
    simulate_finite_population_diagnostic,
    simulate_recall_diagnostic,
)
from vdbench.canary_statistics import exp009_latency_bound_contract


class CanaryCalibrationTests(unittest.TestCase):
    def test_constants_match_the_preregistered_diagnostic_contract(self) -> None:
        self.assertEqual(FINITE_POPULATION_CALIBRATION_SEED, 20260810)
        self.assertEqual(FINITE_POPULATION_CALIBRATION_REPLAYS, 100_000)
        self.assertEqual(RECALL_CALIBRATION_SEED, 20260811)
        self.assertEqual(RECALL_CALIBRATION_REPLAYS, 10_000)
        self.assertEqual(RECALL_CALIBRATION_MEANS, (0.50, 0.95, 0.99))

    def test_finite_population_diagnostic_is_deterministic_and_matches_exact_target(self) -> None:
        first = simulate_finite_population_diagnostic(seed=101, replay_count=37)
        second = simulate_finite_population_diagnostic(seed=101, replay_count=37)

        self.assertIsInstance(first, FinitePopulationCalibration)
        self.assertEqual(first, second)
        self.assertEqual(first.replay_count, 37)
        self.assertGreaterEqual(first.tail_hit_count, 0)
        self.assertLessEqual(first.tail_hit_count, 37)
        self.assertAlmostEqual(
            first.analytic_coverage,
            exp009_latency_bound_contract().coverage_probability,
            places=15,
        )
        self.assertAlmostEqual(
            first.empirical_coverage,
            first.tail_hit_count / 37,
            places=15,
        )

    def test_recall_diagnostic_uses_fixed_1200_value_hoeffding_contract(self) -> None:
        first = simulate_recall_diagnostic(
            seed=202,
            replay_count=41,
            true_mean=0.50,
        )
        second = simulate_recall_diagnostic(
            seed=202,
            replay_count=41,
            true_mean=0.50,
        )

        self.assertIsInstance(first, RecallCalibration)
        self.assertEqual(first, second)
        self.assertEqual(first.observations_per_replay, 1_200)
        self.assertEqual(first.replay_count, 41)
        self.assertEqual(first.true_mean, 0.50)
        self.assertGreaterEqual(first.noncoverage_count, 0)
        self.assertLessEqual(first.noncoverage_count, 41)
        self.assertAlmostEqual(
            first.empirical_noncoverage,
            first.noncoverage_count / 41,
            places=15,
        )
        self.assertGreater(first.hoeffding_margin, 0.0)

    def test_full_contract_groups_each_preregistered_diagnostic_without_rechoosing_estimator(self) -> None:
        result = run_exp009_calibration(
            finite_population_replay_count=19,
            recall_replay_count=23,
        )

        self.assertEqual(result.finite_population.seed, FINITE_POPULATION_CALIBRATION_SEED)
        self.assertEqual(result.finite_population.replay_count, 19)
        self.assertEqual(
            tuple(item.true_mean for item in result.recall),
            RECALL_CALIBRATION_MEANS,
        )
        self.assertTrue(all(item.replay_count == 23 for item in result.recall))

    def test_invalid_replay_or_mean_inputs_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "replay_count"):
            simulate_finite_population_diagnostic(seed=1, replay_count=0)
        with self.assertRaisesRegex(ValueError, "seed"):
            simulate_recall_diagnostic(seed=True, replay_count=1, true_mean=0.5)
        with self.assertRaisesRegex(ValueError, "true_mean"):
            simulate_recall_diagnostic(seed=1, replay_count=1, true_mean=1.1)


if __name__ == "__main__":
    unittest.main()
