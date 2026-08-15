"""FINDING-002: the governed FLAT/oracle comparator behaves as intended.

Closed as intended behaviour, not as a defect, so these are adversarial tests
that pin the contract rather than a fix. Each of the six governed rules gets a
case that *should* agree and a neighbouring case that must not, and the last
group proves `NUMERIC_TOLERANCE` reaches the threshold check only -- never a
FLAT-score-versus-oracle-score magnitude comparison.
"""

from __future__ import annotations

import math
import struct
import unittest

from vdbench.config import Metric, NUMERIC_TOLERANCE
from vdbench.flat_oracle_agreement import (
    FlatOracleAgreementKind,
    compare_flat_oracle_hits,
)
from vdbench.milvus import SearchHit
from vdbench.oracle import OracleHit, OracleResult
import vdbench.flat_oracle_agreement as flat_oracle_agreement


_RADIUS = 10.0
_RANGE_FILTER = 0.0
_LIMIT = 100


def _binary32(value: float) -> float:
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _compare(flat, oracle, *, limit: int = _LIMIT, radius: float = _RADIUS):
    return compare_flat_oracle_hits(
        flat_hits=tuple(flat),
        oracle_result=oracle,
        metric=Metric.L2,
        radius=radius,
        range_filter=_RANGE_FILTER,
        limit=limit,
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


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
