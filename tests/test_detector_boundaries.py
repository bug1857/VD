"""FINDING-009: exact decision boundaries of the governed detector.

No scientific threshold is changed here. These tests pin the existing
constants and the exact comparison direction at each gate, so a future edit
that turns `<=` into `<` -- or that nudges an effect floor -- fails loudly
instead of silently redefining what DRIFT means.
"""

from __future__ import annotations

import math
import unittest

import numpy as np

from vdbench import drift
from vdbench.config import Metric
from vdbench.drift import (
    AUDIT_QUERY_COUNT,
    ELIGIBLE_QUERY_COUNT,
    FAMILY_WISE_ALPHA,
    PERMUTATION_COUNT,
    PERMUTATION_DENOMINATOR,
    SENTINEL_EF,
    IncompleteEvidenceError,
    Signal,
    SignalEvidence,
    derive_permutation_seed,
    deterministic_permutation_p_value,
    holm_step_down,
)

_FLOORS = drift._EFFECT_FLOORS


def _evidence(
    signal: Signal,
    *,
    effect: float,
    adjusted: float,
    complete: bool = True,
) -> SignalEvidence:
    return SignalEvidence(
        signal=signal,
        complete=complete,
        reference_count=ELIGIBLE_QUERY_COUNT,
        current_count=ELIGIBLE_QUERY_COUNT,
        statistic=1.0,
        effect=effect,
        effect_floor=_FLOORS[signal],
        raw_p_value=adjusted,
        adjusted_p_value=adjusted,
    )


class GovernedConstantsTests(unittest.TestCase):
    """These values are the detector contract; the identity digest binds them."""

    def test_constants_are_exactly_as_governed(self) -> None:
        self.assertEqual(FAMILY_WISE_ALPHA, 0.01)
        self.assertEqual(PERMUTATION_COUNT, 9_999)
        self.assertEqual(PERMUTATION_DENOMINATOR, 10_000)
        self.assertEqual(ELIGIBLE_QUERY_COUNT, 200)
        self.assertEqual(AUDIT_QUERY_COUNT, 50)
        self.assertEqual(SENTINEL_EF, 100)

    def test_effect_floors_are_exactly_as_governed(self) -> None:
        self.assertEqual(
            _FLOORS,
            {
                Signal.QUERY_VECTOR: 0.01,
                Signal.THRESHOLD: 0.20,
                Signal.CARDINALITY: 0.20,
                Signal.RECALL: 0.02,
            },
        )


class AlphaBoundaryTests(unittest.TestCase):
    """`adjusted_p_value <= FAMILY_WISE_ALPHA` -- inclusive at the boundary."""

    def test_p_exactly_at_alpha_breaches(self) -> None:
        item = _evidence(
            Signal.RECALL, effect=1.0, adjusted=FAMILY_WISE_ALPHA
        )
        self.assertTrue(item.breach)

    def test_p_one_ulp_below_alpha_breaches(self) -> None:
        item = _evidence(
            Signal.RECALL,
            effect=1.0,
            adjusted=math.nextafter(FAMILY_WISE_ALPHA, 0.0),
        )
        self.assertTrue(item.breach)

    def test_p_one_ulp_above_alpha_does_not_breach(self) -> None:
        item = _evidence(
            Signal.RECALL,
            effect=1.0,
            adjusted=math.nextafter(FAMILY_WISE_ALPHA, 1.0),
        )
        self.assertFalse(item.breach)

    def test_missing_adjusted_p_value_never_breaches(self) -> None:
        item = SignalEvidence(
            signal=Signal.RECALL,
            complete=True,
            reference_count=ELIGIBLE_QUERY_COUNT,
            current_count=ELIGIBLE_QUERY_COUNT,
            statistic=1.0,
            effect=1.0,
            effect_floor=_FLOORS[Signal.RECALL],
            raw_p_value=0.0,
            adjusted_p_value=None,
        )
        self.assertFalse(item.breach)

    def test_incomplete_evidence_never_breaches(self) -> None:
        item = _evidence(
            Signal.RECALL, effect=1.0, adjusted=0.0, complete=False
        )
        self.assertFalse(item.breach)


class EffectFloorBoundaryTests(unittest.TestCase):
    """`effect >= effect_floor` -- inclusive at the floor, for every signal."""

    def test_effect_exactly_at_the_floor_breaches(self) -> None:
        for signal, floor in _FLOORS.items():
            with self.subTest(signal=signal):
                item = _evidence(signal, effect=floor, adjusted=0.0)
                self.assertTrue(item.breach)

    def test_effect_one_ulp_below_the_floor_does_not_breach(self) -> None:
        for signal, floor in _FLOORS.items():
            with self.subTest(signal=signal):
                item = _evidence(
                    signal, effect=math.nextafter(floor, 0.0), adjusted=0.0
                )
                self.assertFalse(item.breach)

    def test_effect_one_ulp_above_the_floor_breaches(self) -> None:
        for signal, floor in _FLOORS.items():
            with self.subTest(signal=signal):
                item = _evidence(
                    signal, effect=math.nextafter(floor, 1.0), adjusted=0.0
                )
                self.assertTrue(item.breach)

    def test_gate_ratio_is_exactly_one_at_the_floor(self) -> None:
        for signal, floor in _FLOORS.items():
            with self.subTest(signal=signal):
                self.assertEqual(
                    _evidence(signal, effect=floor, adjusted=0.0).gate_ratio, 1.0
                )

    def test_both_gates_are_required(self) -> None:
        floor = _FLOORS[Signal.THRESHOLD]
        significant_only = _evidence(
            Signal.THRESHOLD, effect=math.nextafter(floor, 0.0), adjusted=0.0
        )
        large_only = _evidence(
            Signal.THRESHOLD, effect=floor, adjusted=FAMILY_WISE_ALPHA * 2
        )
        self.assertFalse(significant_only.breach)
        self.assertFalse(large_only.breach)


class HolmBoundaryTests(unittest.TestCase):
    def test_holm_multipliers_are_step_down_over_four_signals(self) -> None:
        adjusted = holm_step_down(
            {
                Signal.QUERY_VECTOR: 0.001,
                Signal.THRESHOLD: 0.002,
                Signal.CARDINALITY: 0.003,
                Signal.RECALL: 0.004,
            }
        )
        self.assertAlmostEqual(adjusted[Signal.QUERY_VECTOR], 0.004)
        self.assertAlmostEqual(adjusted[Signal.THRESHOLD], 0.006)
        self.assertAlmostEqual(adjusted[Signal.CARDINALITY], 0.006)
        self.assertAlmostEqual(adjusted[Signal.RECALL], 0.006)

    def test_holm_output_is_monotone_non_decreasing_in_rank(self) -> None:
        adjusted = holm_step_down(
            {
                Signal.QUERY_VECTOR: 0.01,
                Signal.THRESHOLD: 0.2,
                Signal.CARDINALITY: 0.5,
                Signal.RECALL: 0.9,
            }
        )
        ordered = [adjusted[item] for item in sorted(adjusted, key=lambda s: s.value)]
        self.assertEqual(
            sorted(adjusted.values()), sorted(adjusted.values())
        )
        self.assertTrue(all(0.0 <= value <= 1.0 for value in ordered))

    def test_holm_saturates_at_one(self) -> None:
        adjusted = holm_step_down({signal: 0.9 for signal in _FLOORS})
        self.assertTrue(all(value == 1.0 for value in adjusted.values()))

    def test_smallest_raw_p_exactly_at_alpha_over_four_adjusts_to_alpha(self) -> None:
        """The exact four-signal Holm rejection boundary."""

        smallest = FAMILY_WISE_ALPHA / 4.0
        adjusted = holm_step_down(
            {
                Signal.QUERY_VECTOR: smallest,
                Signal.THRESHOLD: 1.0,
                Signal.CARDINALITY: 1.0,
                Signal.RECALL: 1.0,
            }
        )
        self.assertAlmostEqual(adjusted[Signal.QUERY_VECTOR], FAMILY_WISE_ALPHA)
        self.assertLessEqual(adjusted[Signal.QUERY_VECTOR], FAMILY_WISE_ALPHA)

    def test_duplicate_signal_keys_are_refused(self) -> None:
        class _DuplicateMapping(dict):
            """`Signal` is a StrEnum, so a dict literal would silently merge."""

            def items(self):
                return ((Signal.RECALL, 0.1), ("RECALL", 0.2))

        with self.assertRaises(ValueError):
            holm_step_down(_DuplicateMapping())

    def test_out_of_range_p_values_are_refused(self) -> None:
        for value in (-0.0001, 1.0001, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                holm_step_down({Signal.RECALL: value})

    def test_alpha_must_be_strictly_inside_the_unit_interval(self) -> None:
        for alpha in (0.0, 1.0, -0.1, 1.1):
            with self.subTest(alpha=alpha), self.assertRaises(ValueError):
                holm_step_down({Signal.RECALL: 0.5}, alpha=alpha)


class PermutationBoundaryTests(unittest.TestCase):
    def _p_value(self, *, observed: float, statistic_value: float) -> float:
        def batch(membership):
            return np.full(membership.shape[0], statistic_value)

        return deterministic_permutation_p_value(
            observed_statistic=observed,
            total_count=8,
            reference_count=4,
            detector_seed=20260813,
            metric=Metric.L2,
            window_id=1,
            signal=Signal.RECALL,
            batch_statistic=batch,
        ).p_value

    def test_minimum_attainable_p_value_is_one_over_the_denominator(self) -> None:
        value = self._p_value(observed=10.0, statistic_value=0.0)
        self.assertEqual(value, 1.0 / PERMUTATION_DENOMINATOR)

    def test_maximum_p_value_is_one(self) -> None:
        value = self._p_value(observed=0.0, statistic_value=10.0)
        self.assertEqual(value, 1.0)

    def test_exceedance_is_inclusive_at_equality(self) -> None:
        """`statistics >= observed`: an exactly equal permutation counts."""

        equal = self._p_value(observed=1.0, statistic_value=1.0)
        self.assertEqual(equal, 1.0)
        just_above = self._p_value(
            observed=math.nextafter(1.0, 2.0), statistic_value=1.0
        )
        self.assertEqual(just_above, 1.0 / PERMUTATION_DENOMINATOR)

    def test_non_finite_observed_statistic_fails_closed(self) -> None:
        for observed in (float("nan"), float("inf")):
            with self.subTest(observed=observed), self.assertRaises(
                IncompleteEvidenceError
            ):
                self._p_value(observed=observed, statistic_value=0.0)

    def test_reference_count_must_be_strictly_inside_total(self) -> None:
        def batch(membership):
            return np.zeros(membership.shape[0])

        for reference, total in ((0, 8), (8, 8), (9, 8)):
            with self.subTest(reference=reference, total=total), self.assertRaises(
                ValueError
            ):
                deterministic_permutation_p_value(
                    observed_statistic=1.0,
                    total_count=total,
                    reference_count=reference,
                    detector_seed=20260813,
                    metric=Metric.L2,
                    window_id=1,
                    signal=Signal.RECALL,
                    batch_statistic=batch,
                )


class DeterminismTests(unittest.TestCase):
    """Restart/reconstruction determinism: same operands, same evidence."""

    def _run(self, *, seed: int, window_id: object, signal: Signal) -> tuple:
        calls: list[int] = []

        def batch(membership):
            calls.append(int(membership.sum()))
            return membership.sum(axis=1).astype(float)

        evidence = deterministic_permutation_p_value(
            observed_statistic=4.0,
            total_count=8,
            reference_count=4,
            detector_seed=seed,
            metric=Metric.L2,
            window_id=window_id,
            signal=signal,
            batch_statistic=batch,
        )
        return evidence.p_value, evidence.exceedance_count, tuple(calls)

    def test_same_operands_reproduce_identical_evidence(self) -> None:
        first = self._run(seed=20260813, window_id=1, signal=Signal.RECALL)
        second = self._run(seed=20260813, window_id=1, signal=Signal.RECALL)
        self.assertEqual(first, second)

    def test_a_different_seed_changes_the_permutation_stream(self) -> None:
        base = derive_permutation_seed(20260813, Metric.L2, 1, Signal.RECALL)
        other = derive_permutation_seed(20260812, Metric.L2, 1, Signal.RECALL)
        self.assertNotEqual(base.seed_u64, other.seed_u64)

    def test_seed_material_separates_window_metric_and_signal(self) -> None:
        base = derive_permutation_seed(20260813, Metric.L2, 1, Signal.RECALL)
        variants = (
            derive_permutation_seed(20260813, Metric.L2, 2, Signal.RECALL),
            derive_permutation_seed(20260813, Metric.L2, 1, Signal.THRESHOLD),
            derive_permutation_seed(20260813, Metric.COSINE, 1, Signal.RECALL),
        )
        for variant in variants:
            self.assertNotEqual(base.seed_u64, variant.seed_u64)

    def test_seed_derivation_is_pure(self) -> None:
        first = derive_permutation_seed(20260813, Metric.L2, 7, Signal.CARDINALITY)
        second = derive_permutation_seed(20260813, Metric.L2, 7, Signal.CARDINALITY)
        self.assertEqual(first.seed_u64, second.seed_u64)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
