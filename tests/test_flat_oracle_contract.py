"""FINDING-002: the governed FLAT/oracle comparator behaves as intended.

Closed as intended behaviour, not as a defect, so these are adversarial tests
that pin the contract rather than a fix. Rule 7 (execution order variance) was
admitted later by governed amendment; the three cases that previously pinned
its absence now pin its exact scope, and each keeps a neighbouring case that
must still fail. Each of the six governed rules gets a
case that *should* agree and a neighbouring case that must not, and the last
group proves `NUMERIC_TOLERANCE` reaches the threshold check only -- never a
FLAT-score-versus-oracle-score magnitude comparison.
"""

from __future__ import annotations

import itertools
import math
import struct
import unittest

from vdbench import flat_oracle_agreement
from vdbench.config import NUMERIC_TOLERANCE, Metric
from vdbench.flat_oracle_agreement import (
    FlatOracleAgreementKind,
    FlatOracleAgreementResult,
    compare_flat_oracle_hits,
)
from vdbench.milvus import SearchHit
from vdbench.oracle import OracleHit, OracleResult

_RADIUS = 10.0
_RANGE_FILTER = 0.0
_LIMIT = 100


def _binary32(value: float) -> float:
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _compare(
    flat,
    oracle,
    *,
    limit: int = _LIMIT,
    radius: float = _RADIUS,
    dimensions: int | None = None,
):
    return compare_flat_oracle_hits(
        flat_hits=tuple(flat),
        oracle_result=oracle,
        metric=Metric.L2,
        radius=radius,
        range_filter=_RANGE_FILTER,
        limit=limit,
        dimensions=dimensions,
    )


def _oracle(pairs, *, full_count: int | None = None, limit: int = _LIMIT):
    hits = tuple(OracleHit(identifier, score) for identifier, score in pairs)
    total = len(hits) if full_count is None else full_count
    return OracleResult(hits=hits, full_count=total, capped=total > limit)


class ExactOrderedTests(unittest.TestCase):
    def test_identical_ordered_membership_agrees_exactly(self) -> None:
        pairs = [(1, 1.0), (2, 2.0), (3, 3.0)]
        result = _compare([SearchHit(i, s) for i, s in pairs], _oracle(pairs))
        self.assertIs(result.kind, FlatOracleAgreementKind.EXACT_ORDERED)
        self.assertTrue(result.agrees)
        self.assertEqual(result.reason_codes, ())


class Rule1And6CappedMembershipTests(unittest.TestCase):
    """No capped-membership substitution, even for an exactly tied id."""

    def test_substituting_an_equally_tied_unreturned_id_is_a_mismatch(self) -> None:
        tied = _binary32(2.0)
        # The oracle capped at 2 and selected ids 1 and 2; id 3 has the exact
        # same binary32 score but was not selected.
        oracle = _oracle([(1, 1.0), (2, tied)], full_count=3, limit=2)
        flat = [SearchHit(1, 1.0), SearchHit(3, tied)]
        result = _compare(flat, oracle, limit=2)
        self.assertIs(result.kind, FlatOracleAgreementKind.MEMBERSHIP_MISMATCH)
        self.assertFalse(result.agrees)
        self.assertEqual(
            result.reason_codes, ("FLAT_ORACLE_MEMBERSHIP_MISMATCH",)
        )

    def test_missing_member_is_a_mismatch(self) -> None:
        pairs = [(1, 1.0), (2, 2.0), (3, 3.0)]
        flat = [SearchHit(1, 1.0), SearchHit(2, 2.0)]
        result = _compare(flat, _oracle(pairs))
        self.assertIs(result.kind, FlatOracleAgreementKind.MEMBERSHIP_MISMATCH)

    def test_duplicate_flat_id_is_invalid_evidence(self) -> None:
        pairs = [(1, 1.0), (2, 2.0)]
        flat = [SearchHit(1, 1.0), SearchHit(1, 2.0)]
        result = _compare(flat, _oracle(pairs))
        self.assertIs(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)
        self.assertEqual(result.reason_codes, ("FLAT_ORACLE_ID_DUPLICATE",))


class Rule2ThresholdValidityTests(unittest.TestCase):
    def test_flat_score_beyond_radius_is_a_threshold_violation(self) -> None:
        pairs = [(1, 1.0), (2, 2.0)]
        flat = [SearchHit(1, 1.0), SearchHit(2, _RADIUS + 1.0)]
        result = _compare(flat, _oracle(pairs))
        self.assertIs(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)
        self.assertEqual(result.reason_codes, ("FLAT_THRESHOLD_VIOLATION",))

    def test_score_exactly_on_the_radius_is_admitted_by_the_tolerance(self) -> None:
        """The tolerance exists for exactly this last-bit case, nothing more."""

        pairs = [(1, 1.0), (2, _RADIUS)]
        flat = [SearchHit(1, 1.0), SearchHit(2, _RADIUS)]
        result = _compare(flat, _oracle(pairs))
        self.assertIs(result.kind, FlatOracleAgreementKind.EXACT_ORDERED)

    def test_score_beyond_the_tolerance_band_is_still_rejected(self) -> None:
        beyond = _RADIUS + NUMERIC_TOLERANCE * 10.0
        pairs = [(1, 1.0), (2, beyond)]
        flat = [SearchHit(1, 1.0), SearchHit(2, beyond)]
        result = _compare(flat, _oracle(pairs))
        self.assertIs(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)


class Rule3RawFlatOrderingTests(unittest.TestCase):
    def test_correct_ids_with_misordered_flat_scores_is_invalid(self) -> None:
        """Order is judged on the scores Milvus actually returned."""

        pairs = [(1, 1.0), (2, 2.0), (3, 3.0)]
        flat = [SearchHit(1, 3.0), SearchHit(2, 2.0), SearchHit(3, 1.0)]
        result = _compare(flat, _oracle(pairs))
        self.assertIs(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)
        self.assertEqual(result.reason_codes, ("FLAT_SCORE_ORDER_INVALID",))


class Rule4And5TieGroupTests(unittest.TestCase):
    def test_permutation_inside_one_binary32_tie_group_is_equivalent(self) -> None:
        tied = _binary32(2.0)
        pairs = [(1, 1.0), (2, tied), (3, tied)]
        flat = [SearchHit(1, 1.0), SearchHit(3, tied), SearchHit(2, tied)]
        result = _compare(flat, _oracle(pairs))
        self.assertIs(
            result.kind, FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT
        )
        self.assertTrue(result.agrees)

    def test_two_binary64_scores_collapsing_to_one_binary32_value_tie(self) -> None:
        """The precise situation the contract exists for."""

        first = 2.0
        second = math.nextafter(2.0, 3.0)
        self.assertNotEqual(first, second)
        self.assertEqual(_binary32(first), _binary32(second))
        pairs = [(1, 1.0), (2, first), (3, second)]
        flat = [SearchHit(1, 1.0), SearchHit(3, _binary32(second)),
                SearchHit(2, _binary32(first))]
        result = _compare(flat, _oracle(pairs))
        self.assertIs(
            result.kind, FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT
        )

    def test_permutation_across_two_groups_is_a_non_tie_order_mismatch(self) -> None:
        pairs = [(1, 1.0), (2, 2.0), (3, 3.0)]
        # Ids swapped across distinct score groups; FLAT scores still ordered.
        flat = [SearchHit(1, 1.0), SearchHit(3, 2.0), SearchHit(2, 3.0)]
        result = _compare(flat, _oracle(pairs))
        self.assertIs(
            result.kind, FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH
        )
        self.assertFalse(result.agrees)
        self.assertEqual(
            result.reason_codes, ("FLAT_ORACLE_NON_TIE_ORDER_MISMATCH",)
        )

    def test_adjacent_binary32_scores_are_distinct_groups(self) -> None:
        """One ULP apart in binary32 is NOT a tie, and may not be permuted."""

        low = _binary32(2.0)
        # The next representable *binary32* value, not the next binary64 one:
        # `math.nextafter` on a double would round straight back to `low`.
        high = struct.unpack(
            "<f", struct.pack("<I", struct.unpack("<I", struct.pack("<f", low))[0] + 1)
        )[0]
        self.assertNotEqual(low, high)
        pairs = [(1, low), (2, high)]
        flat = [SearchHit(2, high), SearchHit(1, low)]
        result = _compare(flat, _oracle(pairs))
        self.assertIsNot(
            result.kind, FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT
        )


class L2ExecutionTieTests(unittest.TestCase):
    """ADR-015 amendment: formula-bound, exact returned-score L2 ties."""

    def test_frozen_source_475_divergence_is_execution_tie_equivalent(self) -> None:
        oracle_pairs = (
            (4352, 181.933313757053),
            (8999, 182.48259229506598),
            (9017, 182.7277454875737),
            (8745, 182.7277686415395),
            (5249, 183.02588001577578),
            (8643, 183.16591634483262),
        )
        flat_pairs = (
            (4352, 181.9333038330078),
            (8999, 182.4825897216797),
            (8745, 182.72775268554688),
            (9017, 182.72775268554688),
            (5249, 183.02587890625),
            (8643, 183.16592407226562),
        )
        self.assertNotEqual(
            struct.pack("<f", oracle_pairs[2][1]),
            struct.pack("<f", oracle_pairs[3][1]),
        )
        self.assertEqual(
            struct.pack("<f", flat_pairs[2][1]),
            struct.pack("<f", flat_pairs[3][1]),
        )
        self.assertEqual(
            struct.unpack("<I", struct.pack("<f", oracle_pairs[2][1]))[0],
            0x4336BA4E,
        )
        self.assertEqual(
            struct.unpack("<I", struct.pack("<f", oracle_pairs[3][1]))[0],
            0x4336BA4F,
        )
        result = _compare(
            tuple(SearchHit(*pair) for pair in flat_pairs),
            _oracle(oracle_pairs),
            radius=191.85897352125554,
            dimensions=128,
        )
        self.assertIs(
            result.kind, FlatOracleAgreementKind.EXECUTION_TIE_EQUIVALENT
        )
        self.assertTrue(result.agrees)

    def test_cross_oracle_group_returned_tie_inside_interval_passes(self) -> None:
        first = 182.7277454875737
        second = 182.7277686415395
        returned = _binary32(first)
        result = _compare(
            (SearchHit(2, returned), SearchHit(1, returned)),
            _oracle(((1, first), (2, second))),
            radius=200.0,
            dimensions=128,
        )
        self.assertIs(
            result.kind, FlatOracleAgreementKind.EXECUTION_TIE_EQUIVALENT
        )

    def test_cross_oracle_group_returned_tie_outside_interval_fails(self) -> None:
        result = _compare(
            (SearchHit(2, 1.0), SearchHit(1, 1.0)),
            _oracle(((1, 1.0), (2, 1.1))),
            dimensions=128,
        )
        self.assertIs(
            result.kind, FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH
        )

    def test_one_and_multiple_ulp_legal_ties_do_not_use_a_free_epsilon(self) -> None:
        base_bits = struct.unpack("<I", struct.pack("<f", 1.0))[0]
        for ulps in (1, 4):
            with self.subTest(ulps=ulps):
                separated = struct.unpack(
                    "<f", struct.pack("<I", base_bits + ulps)
                )[0]
                result = _compare(
                    (SearchHit(2, 1.0), SearchHit(1, 1.0)),
                    _oracle(((1, 1.0), (2, separated))),
                    dimensions=128,
                )
                self.assertIs(
                    result.kind,
                    FlatOracleAgreementKind.EXECUTION_TIE_EQUIVALENT,
                )

    def test_large_multi_ulp_tie_outside_formula_bound_fails(self) -> None:
        base_bits = struct.unpack("<I", struct.pack("<f", 1.0))[0]
        separated = struct.unpack(
            "<f", struct.pack("<I", base_bits + 512)
        )[0]
        result = _compare(
            (SearchHit(2, 1.0), SearchHit(1, 1.0)),
            _oracle(((1, 1.0), (2, separated))),
            dimensions=128,
        )
        self.assertIs(
            result.kind, FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH
        )

    def test_noncontiguous_ranks_are_no_tie_block_but_may_be_order_variance(self) -> None:
        """Structurally not a tie block; admissible only on the rule-7 numbers.

        These three oracle scores lie within one binary32 execution interval at
        128 dimensions, so binary32 cannot order them at all. The tie-block rule
        still rejects them -- asserted directly, so the structural guarantee
        cannot regress -- and rule 7 admits them on the numbers instead.
        """

        oracle = _oracle(((1, 1.0), (2, 1.000001), (3, 1.000002)))
        flat = (
            SearchHit(3, 1.0),
            SearchHit(1, 1.0),
            SearchHit(2, 1.000002),
        )
        self.assertFalse(
            flat_oracle_agreement._execution_tie_equivalent(
                flat_ids=tuple(hit.id for hit in flat),
                flat_scores=tuple(hit.score for hit in flat),
                oracle_ids=(1, 2, 3),
                oracle_score_by_id={1: 1.0, 2: 1.000001, 3: 1.000002},
                metric=Metric.L2,
                dimensions=128,
            )
        )
        result = _compare(flat, oracle, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.EXECUTION_ORDER_EQUIVALENT
        )

    def test_noncontiguous_ranks_with_separable_scores_still_fail(self) -> None:
        """The same shape, separable scores: rule 7 must not rescue it."""

        oracle = _oracle(((1, 1.0), (2, 2.0), (3, 3.0)))
        flat = (SearchHit(3, 1.0), SearchHit(1, 2.0), SearchHit(2, 3.0))
        result = _compare(flat, oracle, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH
        )

    def test_different_returned_scores_are_execution_order_variance(self) -> None:
        """Distinct adjacent returned scores are rule 7, never a returned tie."""

        first = 182.7277454875737
        second = 182.7277686415395
        flat = (SearchHit(2, _binary32(first)), SearchHit(1, _binary32(second)))
        self.assertFalse(
            flat_oracle_agreement._execution_tie_equivalent(
                flat_ids=(2, 1),
                flat_scores=tuple(hit.score for hit in flat),
                oracle_ids=(1, 2),
                oracle_score_by_id={1: first, 2: second},
                metric=Metric.L2,
                dimensions=128,
            )
        )
        result = _compare(flat, _oracle(((1, first), (2, second))),
                          radius=200.0, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.EXECUTION_ORDER_EQUIVALENT
        )

    def test_binary64_values_casting_to_one_f32_are_order_variance_not_tie(self) -> None:
        """One-ULP-apart returned scores: not a returned tie, but rule 7 holds."""

        first = 182.7277454875737
        second = 182.7277686415395
        returned = _binary32(first)
        flat = (
            SearchHit(2, returned),
            SearchHit(1, math.nextafter(returned, math.inf)),
        )
        result = _compare(flat, _oracle(((1, first), (2, second))),
                          radius=200.0, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.EXECUTION_ORDER_EQUIVALENT
        )

    def test_execution_tie_is_unavailable_outside_governed_dimensions(self) -> None:
        first = 182.7277454875737
        second = 182.7277686415395
        returned = _binary32(first)
        for dimensions in (None, 127, 129):
            with self.subTest(dimensions=dimensions):
                result = _compare(
                    (SearchHit(2, returned), SearchHit(1, returned)),
                    _oracle(((1, first), (2, second))),
                    radius=200.0,
                    dimensions=dimensions,
                )
                self.assertIs(
                    result.kind,
                    FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH,
                )

    def test_cosine_contract_is_not_broadened(self) -> None:
        result = compare_flat_oracle_hits(
            flat_hits=(SearchHit(2, 0.9), SearchHit(1, 0.9)),
            oracle_result=_oracle(((1, 0.90001), (2, 0.89999))),
            metric=Metric.COSINE,
            radius=0.25,
            range_filter=1.0,
            limit=100,
            dimensions=128,
        )
        self.assertIs(
            result.kind, FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH
        )

    def test_malformed_dimensions_fail_closed(self) -> None:
        oracle = _oracle(((1, 1.0),))
        for dimensions in (True, 128.0, "128", 0, -1):
            with self.subTest(dimensions=dimensions):
                result = compare_flat_oracle_hits(
                    flat_hits=(SearchHit(1, 1.0),),
                    oracle_result=oracle,
                    metric=Metric.L2,
                    radius=_RADIUS,
                    range_filter=_RANGE_FILTER,
                    limit=_LIMIT,
                    dimensions=dimensions,
                )
                self.assertIs(
                    result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE
                )


class ToleranceScopeTests(unittest.TestCase):
    def test_tolerance_is_only_reachable_through_threshold_violations(self) -> None:
        """No score-magnitude comparison may consume the tolerance."""

        import inspect

        source = inspect.getsource(flat_oracle_agreement)
        # Ignore the module docstring, which names the constant while
        # explaining precisely this restriction.
        body = source.split('"""', 2)[-1]
        occurrences = [
            line for line in body.splitlines() if "NUMERIC_TOLERANCE" in line
        ]
        # One import plus exactly the two `threshold_violations` call sites.
        self.assertEqual(len(occurrences), 3, occurrences)
        for line in occurrences:
            if "import" not in line:
                self.assertIn("tolerance=", line)

    def test_flat_scores_need_not_equal_oracle_scores_numerically(self) -> None:
        """Agreement is membership and order, never numeric closeness."""

        # binary64 oracle scores vs the binary32 values Milvus returns: these
        # differ numerically by far more than NUMERIC_TOLERANCE would allow,
        # and the comparator still agrees because order and membership hold.
        oracle_scores = (1.0000001, 2.0000002, 3.0000003)
        pairs = list(enumerate(oracle_scores, start=1))
        flat = [SearchHit(i, _binary32(s)) for i, s in pairs]
        result = _compare(flat, _oracle(pairs))
        self.assertTrue(result.agrees)

    def test_no_direct_score_equality_contract_exists(self) -> None:
        import inspect

        source = inspect.getsource(flat_oracle_agreement)
        # A magnitude test would have to compare a flat score against an
        # oracle score; nothing in the module does.
        self.assertNotIn("abs(", source)
        self.assertNotIn("isclose", source)


class InvalidEvidenceTests(unittest.TestCase):
    def test_capped_flag_must_agree_with_full_count(self) -> None:
        hits = tuple(OracleHit(i, float(i)) for i in range(1, 3))
        oracle = OracleResult(hits=hits, full_count=2, capped=True)
        result = _compare([SearchHit(i, float(i)) for i in range(1, 3)], oracle)
        self.assertIs(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)

    def test_flat_result_longer_than_the_limit_is_invalid(self) -> None:
        pairs = [(i, float(i)) for i in range(1, 4)]
        flat = [SearchHit(i, float(i)) for i in range(1, 4)]
        result = _compare(flat, _oracle(pairs, limit=2), limit=2)
        self.assertIs(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)

    def test_non_finite_score_is_invalid(self) -> None:
        pairs = [(1, 1.0), (2, 2.0)]
        flat = [SearchHit(1, 1.0), SearchHit(2, float("nan"))]
        result = _compare(flat, _oracle(pairs))
        self.assertIs(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)

    def test_wrong_operand_types_are_invalid_rather_than_raising(self) -> None:
        result = compare_flat_oracle_hits(
            flat_hits=[SearchHit(1, 1.0)],
            oracle_result=_oracle([(1, 1.0)]),
            metric=Metric.L2,
            radius=_RADIUS,
            range_filter=_RANGE_FILTER,
            limit=_LIMIT,
        )
        self.assertIs(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)
        self.assertEqual(
            result.reason_codes, ("FLAT_ORACLE_EVIDENCE_INVALID",)
        )



class Rule7ExecutionOrderVarianceTests(unittest.TestCase):
    """Rule 7: adjacent inversions binary32 cannot resolve.

    The two live cases below are the only order-variance events observed across
    6600 audited EXP-012 queries. They are reproduced here from the persisted
    Gate-C evidence as minimal local pairs, so the classifier stays pinned to
    real measured numbers rather than to synthetic ones.
    """

    # exp012-scale2400-v1 q475: Milvus collapsed the pair (equal returned
    # scores) -> rule 5's returned-tie class, unchanged by the amendment.
    _Q475_ORACLE = ((9017, 182.7277454875737), (8745, 182.7277686415395))
    _Q475_RETURNED = 182.72775268554688

    # exp012-scale10000-v1 q3669: Milvus resolved the pair the other way
    # (distinct returned scores, transposed ids) -> rule 7.
    _Q3669_ORACLE = ((3756, 180.42217994069432), (752, 180.4221929662512))
    _Q3669_FLAT = ((752, 180.42218017578125), (3756, 180.4221954345703))

    def test_live_q475_remains_a_returned_tie_not_order_variance(self) -> None:
        flat = tuple(
            SearchHit(identifier, self._Q475_RETURNED)
            for identifier in (8745, 9017)
        )
        result = _compare(flat, _oracle(self._Q475_ORACLE),
                          radius=200.0, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.EXECUTION_TIE_EQUIVALENT
        )

    def test_live_q3669_is_execution_order_variance(self) -> None:
        flat = tuple(
            SearchHit(identifier, _binary32(score))
            for identifier, score in self._Q3669_FLAT
        )
        result = _compare(flat, _oracle(self._Q3669_ORACLE),
                          radius=200.0, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.EXECUTION_ORDER_EQUIVALENT
        )
        self.assertTrue(result.agrees)
        self.assertEqual(result.reason_codes, ())

    def test_q3669_membership_mismatch_still_fails(self) -> None:
        """One ULP apart is irrelevant once membership moves."""

        flat = (
            SearchHit(999_999, _binary32(180.42218017578125)),
            SearchHit(3756, _binary32(180.4221954345703)),
        )
        result = _compare(flat, _oracle(self._Q3669_ORACLE),
                          radius=200.0, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.MEMBERSHIP_MISMATCH
        )

    def test_q3669_cardinality_mismatch_still_fails(self) -> None:
        flat = (SearchHit(752, _binary32(180.42218017578125)),)
        result = _compare(flat, _oracle(self._Q3669_ORACLE),
                          radius=200.0, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.MEMBERSHIP_MISMATCH
        )

    def test_score_outside_execution_interval_fails(self) -> None:
        """Ordering stays valid, so only the interval check can reject this."""

        oracle = _oracle(((1, 1.0), (2, 2.0)))
        flat = (SearchHit(2, 1.0), SearchHit(1, 2.0))
        result = _compare(flat, oracle, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH
        )

    def test_multiple_independent_local_inversions_agree(self) -> None:
        oracle = _oracle(((1, 1.0), (2, 1.000001), (3, 5.0), (4, 5.000001)))
        flat = (
            SearchHit(2, 1.0),
            SearchHit(1, 1.000001),
            SearchHit(4, 5.0),
            SearchHit(3, 5.000001),
        )
        result = _compare(flat, oracle, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.EXECUTION_ORDER_EQUIVALENT
        )

    def test_rule_7_does_not_widen_into_general_order_insensitivity(self) -> None:
        """Separable scores: no permutation of them may ever be admitted."""

        pairs = tuple((index, float(index)) for index in range(1, 9))
        oracle = _oracle(pairs)
        shuffled = (8, 3, 6, 1, 7, 2, 5, 4)
        flat = tuple(
            SearchHit(identifier, float(position + 1))
            for position, identifier in enumerate(shuffled)
        )
        result = _compare(flat, oracle, radius=100.0, dimensions=128)
        self.assertIs(
            result.kind, FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH
        )
        self.assertFalse(result.agrees)

    def test_rule_7_requires_governed_l2_dimensions(self) -> None:
        """Without the governed execution model there is no interval to use."""

        oracle = _oracle(self._Q3669_ORACLE)
        flat = tuple(
            SearchHit(identifier, _binary32(score))
            for identifier, score in self._Q3669_FLAT
        )
        for dimensions in (None, 64):
            with self.subTest(dimensions=dimensions):
                result = _compare(flat, oracle, radius=200.0,
                                  dimensions=dimensions)
                self.assertIs(
                    result.kind,
                    FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH,
                )

    def test_non_finite_and_malformed_evidence_is_invalid_not_variance(self) -> None:
        oracle = _oracle(self._Q3669_ORACLE)
        for score in (math.nan, math.inf, -math.inf):
            with self.subTest(score=score):
                flat = (
                    SearchHit(752, score),
                    SearchHit(3756, _binary32(180.4221954345703)),
                )
                result = _compare(flat, oracle, radius=200.0, dimensions=128)
                self.assertIs(
                    result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE
                )
                self.assertFalse(result.agrees)

    def test_duplicate_ids_are_invalid_not_variance(self) -> None:
        flat = (
            SearchHit(752, _binary32(180.42218017578125)),
            SearchHit(752, _binary32(180.4221954345703)),
        )
        result = _compare(flat, _oracle(self._Q3669_ORACLE),
                          radius=200.0, dimensions=128)
        self.assertIs(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)

    def test_classification_is_deterministic_and_stably_named(self) -> None:
        """Replay safety: the kind is reconstructable and its wire name frozen.

        The stage record persists only a boolean, so the classification is
        recovered by re-running this pure comparator over the durably bound
        FLAT and oracle evidence. That is only sound if repeated evaluation is
        identical and the name never drifts.
        """

        flat = tuple(
            SearchHit(identifier, _binary32(score))
            for identifier, score in self._Q3669_FLAT
        )
        oracle = _oracle(self._Q3669_ORACLE)
        first = _compare(flat, oracle, radius=200.0, dimensions=128)
        second = _compare(flat, oracle, radius=200.0, dimensions=128)
        self.assertEqual(first, second)
        self.assertEqual(
            FlatOracleAgreementKind.EXECUTION_ORDER_EQUIVALENT.value,
            "EXECUTION_ORDER_EQUIVALENT",
        )

    def test_every_agreement_kind_is_explicitly_enumerated(self) -> None:
        """A new kind must never default into agreement unnoticed."""

        agreeing = {
            kind
            for kind in FlatOracleAgreementKind
            if FlatOracleAgreementResult(kind=kind, reason_codes=()).agrees
        }
        self.assertEqual(
            agreeing,
            {
                FlatOracleAgreementKind.EXACT_ORDERED,
                FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT,
                FlatOracleAgreementKind.EXECUTION_TIE_EQUIVALENT,
                FlatOracleAgreementKind.EXECUTION_ORDER_EQUIVALENT,
            },
        )

if __name__ == "__main__":  # pragma: no cover
    unittest.main()


class Rule7ForcedPrecedenceTests(unittest.TestCase):
    """Rule 7 states a partial order, so pin exactly what it forces.

    The governed interval decides precedence: id i must precede j exactly when
    U_i < L_j. These tests hunt for an accepted ordering that breaks such a
    relation -- including cases where every *adjacent* pair overlaps but a
    non-adjacent pair is forced, which is where a pairwise-only reading of the
    rule would be unsound if the relation were not transitively closed.
    """

    _DIMENSIONS = 128

    @staticmethod
    def _intervals(scores):
        return {
            identifier: flat_oracle_agreement._l2_binary32_execution_interval(
                score, dimensions=128
            )
            for identifier, score in scores.items()
        }

    @classmethod
    def _forced_pairs(cls, scores):
        intervals = cls._intervals(scores)
        return {
            (first, second)
            for first in scores
            for second in scores
            if first != second and intervals[first][1] < intervals[second][0]
        }

    # All three overlap pairwise AND transitively: order is unconstrained.
    _OPEN = {1: 1.0, 2: 1.000005, 3: 1.00001}
    # Adjacent pairs overlap, but 1 before 3 is forced: the transitivity trap.
    _TRAP = {1: 1.0, 2: 1.00001, 3: 1.00002}
    # Every pair forced: only the exact order may be admitted.
    _SEPARATED = {1: 1.0, 2: 2.0, 3: 3.0}

    def _compare_permutation(self, scores, permutation):
        ordered = sorted(scores.items(), key=lambda item: item[1])
        oracle = _oracle(tuple(ordered))
        ladder = [_binary32(score) for _identifier, score in ordered]
        flat = tuple(
            SearchHit(identifier, ladder[index])
            for index, identifier in enumerate(permutation)
        )
        return _compare(flat, oracle, radius=100.0, dimensions=self._DIMENSIONS)

    def test_no_accepted_permutation_violates_a_forced_relation(self) -> None:
        """The central property: acceptance never breaks forced precedence."""

        for name, scores in (
            ("open", self._OPEN),
            ("trap", self._TRAP),
            ("separated", self._SEPARATED),
        ):
            forced = self._forced_pairs(scores)
            for permutation in itertools.permutations(sorted(scores)):
                result = self._compare_permutation(scores, permutation)
                if not result.agrees:
                    continue
                position = {
                    identifier: index for index, identifier in enumerate(permutation)
                }
                for first, second in forced:
                    with self.subTest(case=name, order=permutation,
                                      forced=(first, second)):
                        self.assertLess(position[first], position[second])

    def test_transitively_forced_pair_rejects_a_locally_plausible_order(self) -> None:
        """Every adjacent pair overlaps, yet 3 before 1 must still be refused."""

        forced = self._forced_pairs(self._TRAP)
        self.assertIn((1, 3), forced)
        self.assertNotIn((1, 2), forced)
        self.assertNotIn((2, 3), forced)
        self.assertFalse(self._compare_permutation(self._TRAP, (3, 2, 1)).agrees)

    def test_fully_overlapping_candidates_admit_reordering(self) -> None:
        self.assertEqual(self._forced_pairs(self._OPEN), set())
        result = self._compare_permutation(self._OPEN, (2, 1, 3))
        self.assertIs(
            result.kind, FlatOracleAgreementKind.EXECUTION_ORDER_EQUIVALENT
        )

    def test_separated_candidates_admit_only_the_exact_order(self) -> None:
        for permutation in itertools.permutations((1, 2, 3)):
            with self.subTest(order=permutation):
                result = self._compare_permutation(self._SEPARATED, permutation)
                if permutation == (1, 2, 3):
                    self.assertIs(
                        result.kind, FlatOracleAgreementKind.EXACT_ORDERED
                    )
                else:
                    self.assertFalse(result.agrees)

    def test_rule_7_never_claims_joint_reduction_attainability(self) -> None:
        """A returned score no legal execution could produce is still refused.

        Rule 7 weakens *ordering*, never value plausibility: each returned
        score must still lie inside the interval of the id placed there.
        """

        oracle = _oracle(((1, 1.0), (2, 1.000005)))
        outside = _binary32(1.0) * (1.0 + 1e-3)
        flat = (SearchHit(2, _binary32(1.0)), SearchHit(1, outside))
        result = _compare(flat, oracle, radius=100.0, dimensions=128)
        self.assertFalse(result.agrees)
