"""Canonical ordered FLAT/oracle agreement for governed range-query evidence.

The independent oracle accumulates scores in binary64 while the governed
vectors and Milvus search path use binary32.  Distinct oracle scores can
therefore collapse to the same representable binary32 value, and legal
binary32 L2 reduction orders can also collapse scores whose final oracle casts
remain distinct.  This module keeps exact capped membership mandatory and
permits only the explicitly governed precision classes below.  It never applies
a free tolerance and never replaces ordered agreement with global set equality.

The governed contract, in full (FINDING-002 -- intended behaviour, not a bug):

1. Exact capped membership. `set(flat_ids) == set(oracle_ids)` and the two
   lengths are equal.  At a capped limit there is deliberately NO substitution
   of an unreturned tied id: a different-but-equally-tied member is a
   `MEMBERSHIP_MISMATCH`, never an agreement.
2. Threshold validity.  Both the FLAT scores and the oracle scores must
   satisfy the metric's range contract.
3. Raw returned FLAT metric ordering.  The scores Milvus actually returned
   must themselves be ordered for the metric; a correctly-ordered id list
   carrying mis-ordered scores is `FLAT_SCORE_ORDER_INVALID`.
4. Distinguishable oracle score-group order.  Oracle members are bucketed by
   the exact IEEE-754 binary32 bit pattern of their score, and those buckets
   must appear in metric order.
5. Permutation is legal inside one exact binary32 oracle-score tie group.  For
   governed 128-dimensional L2 only, a second, separately classified rule also
   permits a contiguous oracle-rank permutation inside one exact returned-FLAT
   binary32 tie block when every returned score lies inside the analytical
   IEEE-754 execution interval for its independent oracle score.  The interval
   covers binary32 subtraction and any legal binary32 product/FMA reduction
   order; it is formula-derived, not fitted to observed evidence.
6. No capped-membership substitution (the emphatic restatement of rule 1,
   because it is the rule most often mistaken for over-strictness).
7. Execution order variance (ADR-016 item 10, amended).  Rule 5's second class
   only covers a run Milvus returned as one binary32 tie -- it *collapsed* a
   distinction the oracle drew.  The same physical event also appears with the
   pair *resolved the other way*: adjacent, distinct returned binary32 scores
   carrying transposed ids.  Rule 7 admits that case as a numerical
   partial-order contract.  The analytical interval decides which precedence
   relations are *forced*: id `i` must precede `j` exactly when `U_i < L_j`.
   A returned order is admissible when every returned score lies inside the
   execution interval of the id placed there, and no pair is inverted against
   a forced relation.  Rule 7 does NOT assert that one particular reduction
   schedule generated the returned list -- independently derived per-candidate
   intervals cannot support that claim -- only that the governed numerical
   model cannot establish a stricter total order.  Membership and cardinality
   stay exact, nothing is fitted to observed evidence, and order variance is
   reported as its own kind so it stays separately countable as scientific
   evidence rather than being absorbed into EXACT_ORDERED.

`NUMERIC_TOLERANCE` (1e-6) is THRESHOLD-ONLY.  It is passed to
`oracle.threshold_violations` so a score sitting exactly on the radius is not
rejected for a last-bit representation difference.  It is never applied to a
FLAT-score-versus-oracle-score comparison, and this module deliberately
contains no direct score-magnitude equality test: agreement is decided by
membership and order, not by numeric closeness.  Introducing such a test would
silently redefine what a Gate-C PASS means.
"""

from __future__ import annotations

import itertools
import math
import struct
from dataclasses import dataclass
from enum import StrEnum
from fractions import Fraction

from .config import EXP001_DATASET_SPEC, NUMERIC_TOLERANCE, Metric
from .milvus import SearchHit
from .oracle import OracleHit, OracleResult, threshold_violations, validate_range

__all__ = [
    "FlatOracleAgreementKind",
    "FlatOracleAgreementResult",
    "compare_flat_oracle_hits",
]


class FlatOracleAgreementKind(StrEnum):
    """Exhaustive outcome of one canonical FLAT/oracle comparison."""

    EXACT_ORDERED = "EXACT_ORDERED"
    PRECISION_TIE_EQUIVALENT = "PRECISION_TIE_EQUIVALENT"
    EXECUTION_TIE_EQUIVALENT = "EXECUTION_TIE_EQUIVALENT"
    EXECUTION_ORDER_EQUIVALENT = "EXECUTION_ORDER_EQUIVALENT"
    MEMBERSHIP_MISMATCH = "MEMBERSHIP_MISMATCH"
    NON_TIE_ORDER_MISMATCH = "NON_TIE_ORDER_MISMATCH"
    INVALID_EVIDENCE = "INVALID_EVIDENCE"


@dataclass(frozen=True, slots=True)
class FlatOracleAgreementResult:
    """Structured comparison result; only the three explicit agreement kinds agree."""

    kind: FlatOracleAgreementKind
    reason_codes: tuple[str, ...]

    @property
    def agrees(self) -> bool:
        return self.kind in {
            FlatOracleAgreementKind.EXACT_ORDERED,
            FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT,
            FlatOracleAgreementKind.EXECUTION_TIE_EQUIVALENT,
            FlatOracleAgreementKind.EXECUTION_ORDER_EQUIVALENT,
        }


def _result(
    kind: FlatOracleAgreementKind, *reason_codes: str
) -> FlatOracleAgreementResult:
    return FlatOracleAgreementResult(kind=kind, reason_codes=tuple(reason_codes))


def _binary32(value: float) -> tuple[bytes, float]:
    """Return the exact little-endian IEEE-754 binary32 representation."""

    try:
        packed = struct.pack("<f", value)
    except (OverflowError, struct.error) as exc:
        raise ValueError("score is not representable as finite binary32") from exc
    converted = struct.unpack("<f", packed)[0]
    if not math.isfinite(converted):
        raise ValueError("score is not representable as finite binary32")
    # Canonicalize signed zero: threshold ordering treats -0.0 and +0.0 as
    # equal, so they belong to the same score-equivalence group.
    canonical = 0.0 if converted == 0.0 else converted
    return struct.pack("<f", canonical), canonical


def _is_metric_ordered(values: tuple[float, ...], metric: Metric) -> bool:
    pairs = itertools.pairwise(values)
    if metric is Metric.L2:
        return all(left <= right for left, right in pairs)
    return all(left >= right for left, right in pairs)


def _valid_id(value: object) -> bool:
    return type(value) is int


def _valid_score(value: object) -> bool:
    return type(value) is float and math.isfinite(value)


# IEEE-754 binary32 round-to-nearest unit roundoff and minimum normal.  The
# latter deliberately covers both gradual-underflow rounding and a
# flush-to-zero implementation, without requiring an unverified runtime FPCR
# assumption.  These are format constants, not empirical tolerances.
_BINARY32_UNIT_ROUNDOFF = Fraction(1, 2**24)
_BINARY32_UNDERFLOW_ABSOLUTE_BOUND = Fraction(1, 2**126)
_BINARY64_UNIT_ROUNDOFF = Fraction(1, 2**53)
_GOVERNED_L2_DIMENSIONS = EXP001_DATASET_SPEC.dimensions


def _l2_binary32_execution_interval(
    oracle_score: float, *, dimensions: int
) -> tuple[float, float]:
    """Bound legal binary32 squared-L2 reductions around a binary64 oracle.

    For ``n`` finite binary32 input components, a rounded subtraction perturbs
    each exact difference by at most binary32 unit roundoff (a subnormal
    cancellation is exact under gradual underflow).  Squaring contributes that
    factor twice.  A product plus an arbitrary reduction tree contributes at
    most ``n`` further rounded binary32 operations along any term's path; an
    FMA path has no more error than this deliberately conservative model.

    The independent oracle also performs finite binary64 subtraction, square,
    and reduction operations.  ``gamma_(n+2)`` encloses its score around the
    exact real sum.  The returned interval then composes that oracle interval
    with ``(1 +/- u32) ** (n+2)`` and a geometric minimum-normal term for the
    binary32 product/reduction operations.  All arithmetic is expanded by one
    binary64 ``nextafter`` at each edge so host evaluation cannot narrow the
    mathematical interval.  The absolute underflow term uses the minimum
    normal, conservatively covering gradual underflow and flush-to-zero.
    """

    if dimensions <= 0 or oracle_score < 0.0 or not math.isfinite(oracle_score):
        raise ValueError("invalid L2 execution-model input")
    oracle_operations = dimensions + 2
    gamma64_numerator = oracle_operations * _BINARY64_UNIT_ROUNDOFF
    if gamma64_numerator >= 1:
        raise ValueError("unsupported L2 execution-model dimensions")
    gamma64 = gamma64_numerator / (1 - gamma64_numerator)
    exact_score = Fraction.from_float(oracle_score)
    exact_lower = exact_score / (1 + gamma64)
    exact_upper = exact_score / (1 - gamma64)

    binary32_operations = dimensions + 2
    lower_factor = (1 - _BINARY32_UNIT_ROUNDOFF) ** binary32_operations
    upper_factor = (1 + _BINARY32_UNIT_ROUNDOFF) ** binary32_operations
    # Each product/reduction step may lose at most one minimum-normal unit even
    # under flush-to-zero; later operations amplify it by at most (1+u32).
    additive = _BINARY32_UNDERFLOW_ABSOLUTE_BOUND * sum(
        (1 + _BINARY32_UNIT_ROUNDOFF) ** index
        for index in range(binary32_operations)
    )
    lower_fraction = max(Fraction(0), exact_lower * lower_factor - additive)
    upper_fraction = exact_upper * upper_factor + additive
    lower = float(lower_fraction)
    upper = float(upper_fraction)
    if Fraction.from_float(lower) > lower_fraction:
        lower = math.nextafter(lower, -math.inf)
    if Fraction.from_float(upper) < upper_fraction:
        upper = math.nextafter(upper, math.inf)
    if not math.isfinite(upper):
        raise ValueError("unsupported L2 execution-model range")
    return (
        max(0.0, math.nextafter(lower, -math.inf)),
        math.nextafter(upper, math.inf),
    )


def _execution_tie_equivalent(
    *,
    flat_ids: tuple[int, ...],
    flat_scores: tuple[float, ...],
    oracle_ids: tuple[int, ...],
    oracle_score_by_id: dict[int, float],
    metric: Metric,
    dimensions: int | None,
) -> bool:
    """Return whether every changed position is one proved L2 execution tie."""

    if metric is not Metric.L2 or dimensions != _GOVERNED_L2_DIMENSIONS:
        return False
    oracle_rank = {identifier: rank for rank, identifier in enumerate(oracle_ids)}
    changed = False
    position = 0
    while position < len(flat_ids):
        returned_group, returned_value = _binary32(flat_scores[position])
        stop = position + 1
        while stop < len(flat_ids):
            candidate_group, _ = _binary32(flat_scores[stop])
            if candidate_group != returned_group:
                break
            stop += 1

        block_ids = flat_ids[position:stop]
        expected_ranks = tuple(range(position, stop))
        actual_ranks = tuple(sorted(oracle_rank[identifier] for identifier in block_ids))
        if actual_ranks != expected_ranks:
            return False
        if block_ids != oracle_ids[position:stop]:
            changed = True
            if any(
                flat_scores[index] != returned_value
                for index in range(position, stop)
            ):
                return False
            for identifier in block_ids:
                try:
                    lower, upper = _l2_binary32_execution_interval(
                        oracle_score_by_id[identifier], dimensions=dimensions
                    )
                except ValueError:
                    return False
                if not lower <= returned_value <= upper:
                    return False
        position = stop
    return changed


def _execution_order_equivalent(
    *,
    flat_ids: tuple[int, ...],
    flat_scores: tuple[float, ...],
    oracle_ids: tuple[int, ...],
    oracle_score_by_id: dict[int, float],
    metric: Metric,
    dimensions: int | None,
) -> bool:
    """Return whether the returned order violates no *forced* precedence.

    This is a numerical partial-order contract, and deliberately not a claim
    that some single reduction schedule generated the whole returned list.
    Per-candidate interval containment would not support that stronger claim:
    each candidate's interval is derived independently, so containment for
    every position does not establish that one legal execution order attains
    all of those values jointly.

    What the analytical interval does establish is which precedence relations
    are forced.  For candidates ``i`` and ``j`` with governed execution
    intervals ``[L_i, U_i]`` and ``[L_j, U_j]``, ``i`` must precede ``j``
    exactly when ``U_i < L_j``: no legal binary32 execution can score ``j``
    below ``i``.  When the intervals overlap, execution precision does not
    determine their relative order and either is admissible.  The oracle's own
    binary64 order never violates a forced relation, because each oracle score
    lies inside its own interval, so this is a strict weakening of total
    ordering and never of membership.

    Two conditions are required, and both are checked over the whole returned
    list rather than over a locality heuristic:

    1. every returned score lies inside the conservative execution interval
       for the id placed at that position.  Containment means the analytical
       forward-error bound does not exclude the value; it does not prove that
       one particular reduction schedule attains it;
    2. no pair is inverted against a forced precedence relation.

    Checking (2) pairwise is complete rather than merely pairwise-sound: the
    constraint set *is* the set of forced pairs, and the relation is already
    transitively closed by construction -- if ``U_i < L_j`` and ``U_j < L_k``
    then ``U_i < L_j <= U_j < L_k``, so ``i`` before ``k`` is forced and is
    itself one of the pairs examined.  A chain of overlapping intervals
    therefore cannot smuggle in a violation that no single pair exhibits.

    Condition (2) is in fact unreachable as a rejection while condition (1)
    and the module's raw returned-score ordering check both hold, and that is
    the point: for ``p < q`` those two give ``L_p <= s_p <= s_q <= U_q``, hence
    ``L_p <= U_q``, which is exactly the negation of a forced inversion.  The
    partial order is therefore *proved* satisfied rather than merely tested.
    The explicit loop is retained so the contract is stated where it is
    enforced, and so the guarantee survives any future relaxation of the
    ordering check rather than silently depending on it.

    Membership and cardinality are already exact by the time this runs, and
    nothing here is fitted to observed evidence.
    """

    if metric is not Metric.L2 or dimensions != _GOVERNED_L2_DIMENSIONS:
        return False
    if flat_ids == oracle_ids:
        return False
    intervals: dict[int, tuple[float, float]] = {}
    for identifier in oracle_ids:
        try:
            intervals[identifier] = _l2_binary32_execution_interval(
                oracle_score_by_id[identifier], dimensions=dimensions
            )
        except ValueError:
            return False

    for position, identifier in enumerate(flat_ids):
        lower, upper = intervals[identifier]
        if not lower <= flat_scores[position] <= upper:
            return False

    for earlier in range(len(flat_ids)):
        earlier_lower = intervals[flat_ids[earlier]][0]
        for later in range(earlier + 1, len(flat_ids)):
            # The id placed later would have to outrank the id placed earlier
            # under every legal execution; returning it second is impossible.
            if intervals[flat_ids[later]][1] < earlier_lower:
                return False
    return True


def compare_flat_oracle_hits(
    *,
    flat_hits: object,
    oracle_result: object,
    metric: object,
    radius: object,
    range_filter: object,
    limit: object,
    dimensions: object = None,
) -> FlatOracleAgreementResult:
    """Compare one capped FLAT result with its independent exact oracle.

    The oracle's ordered IDs and membership remain authoritative.  At a capped
    limit there is deliberately no substitution of an unreturned tied ID:
    membership must still equal the exact oracle-selected capped membership.
    """

    if (
        type(flat_hits) is not tuple
        or type(oracle_result) is not OracleResult
        or type(metric) is not Metric
        or type(radius) is not float
        or type(range_filter) is not float
        or type(limit) is not int
        or limit <= 0
        or (
            dimensions is not None
            and (type(dimensions) is not int or dimensions <= 0)
        )
    ):
        return _result(
            FlatOracleAgreementKind.INVALID_EVIDENCE,
            "FLAT_ORACLE_EVIDENCE_INVALID",
        )
    try:
        validate_range(metric, radius, range_filter)
    except (TypeError, ValueError):
        return _result(
            FlatOracleAgreementKind.INVALID_EVIDENCE,
            "FLAT_ORACLE_CONFIGURATION_INVALID",
        )

    oracle_hits = oracle_result.hits
    if (
        type(oracle_hits) is not tuple
        or type(oracle_result.full_count) is not int
        or oracle_result.full_count < 0
        or type(oracle_result.capped) is not bool
        or oracle_result.capped != (oracle_result.full_count > limit)
        or len(oracle_hits) != min(oracle_result.full_count, limit)
        or len(flat_hits) > limit
    ):
        return _result(
            FlatOracleAgreementKind.INVALID_EVIDENCE,
            "FLAT_ORACLE_EVIDENCE_INVALID",
        )

    oracle_ids: list[int] = []
    oracle_group_by_id: dict[int, bytes] = {}
    oracle_score_by_id: dict[int, float] = {}
    oracle_group_values: list[float] = []
    oracle_scores: list[float] = []
    for hit in oracle_hits:
        if (
            type(hit) is not OracleHit
            or not _valid_id(hit.id)
            or not _valid_score(hit.score)
        ):
            return _result(
                FlatOracleAgreementKind.INVALID_EVIDENCE,
                "FLAT_ORACLE_EVIDENCE_INVALID",
            )
        try:
            group, group_value = _binary32(hit.score)
        except ValueError:
            return _result(
                FlatOracleAgreementKind.INVALID_EVIDENCE,
                "FLAT_ORACLE_SCORE_INVALID",
            )
        if hit.id in oracle_group_by_id:
            return _result(
                FlatOracleAgreementKind.INVALID_EVIDENCE,
                "FLAT_ORACLE_ID_DUPLICATE",
            )
        oracle_ids.append(hit.id)
        oracle_group_by_id[hit.id] = group
        oracle_score_by_id[hit.id] = hit.score
        oracle_group_values.append(group_value)
        oracle_scores.append(hit.score)

    try:
        oracle_violations = threshold_violations(
            oracle_scores,
            metric,
            radius=radius,
            range_filter=range_filter,
            tolerance=NUMERIC_TOLERANCE,
        )
    except (TypeError, ValueError):
        return _result(
            FlatOracleAgreementKind.INVALID_EVIDENCE,
            "FLAT_ORACLE_CONFIGURATION_INVALID",
        )
    if oracle_violations or not _is_metric_ordered(
        tuple(oracle_group_values), metric
    ):
        return _result(
            FlatOracleAgreementKind.INVALID_EVIDENCE,
            "FLAT_ORACLE_EVIDENCE_INVALID",
        )

    flat_ids: list[int] = []
    flat_scores: list[float] = []
    for hit in flat_hits:
        if (
            type(hit) is not SearchHit
            or not _valid_id(hit.id)
            or not _valid_score(hit.score)
        ):
            return _result(
                FlatOracleAgreementKind.INVALID_EVIDENCE,
                "FLAT_ORACLE_EVIDENCE_INVALID",
            )
        flat_ids.append(hit.id)
        flat_scores.append(hit.score)
        try:
            _binary32(hit.score)
        except ValueError:
            return _result(
                FlatOracleAgreementKind.INVALID_EVIDENCE,
                "FLAT_ORACLE_SCORE_INVALID",
            )
    if len(set(flat_ids)) != len(flat_ids):
        return _result(
            FlatOracleAgreementKind.INVALID_EVIDENCE,
            "FLAT_ORACLE_ID_DUPLICATE",
        )
    try:
        violations = threshold_violations(
            flat_scores,
            metric,
            radius=radius,
            range_filter=range_filter,
            tolerance=NUMERIC_TOLERANCE,
        )
    except (TypeError, ValueError):
        return _result(
            FlatOracleAgreementKind.INVALID_EVIDENCE,
            "FLAT_ORACLE_CONFIGURATION_INVALID",
        )
    if violations:
        return _result(
            FlatOracleAgreementKind.INVALID_EVIDENCE,
            "FLAT_THRESHOLD_VIOLATION",
        )
    if not _is_metric_ordered(tuple(flat_scores), metric):
        return _result(
            FlatOracleAgreementKind.INVALID_EVIDENCE,
            "FLAT_SCORE_ORDER_INVALID",
        )
    if len(flat_ids) != len(oracle_ids) or set(flat_ids) != set(oracle_ids):
        return _result(
            FlatOracleAgreementKind.MEMBERSHIP_MISMATCH,
            "FLAT_ORACLE_MEMBERSHIP_MISMATCH",
        )
    if tuple(flat_ids) == tuple(oracle_ids):
        return _result(FlatOracleAgreementKind.EXACT_ORDERED)

    oracle_groups = tuple(oracle_group_by_id[item] for item in oracle_ids)
    flat_groups = tuple(oracle_group_by_id[item] for item in flat_ids)
    if flat_groups == oracle_groups:
        return _result(FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT)
    if _execution_tie_equivalent(
        flat_ids=tuple(flat_ids),
        flat_scores=tuple(flat_scores),
        oracle_ids=tuple(oracle_ids),
        oracle_score_by_id=oracle_score_by_id,
        metric=metric,
        dimensions=dimensions,
    ):
        return _result(FlatOracleAgreementKind.EXECUTION_TIE_EQUIVALENT)
    if _execution_order_equivalent(
        flat_ids=tuple(flat_ids),
        flat_scores=tuple(flat_scores),
        oracle_ids=tuple(oracle_ids),
        oracle_score_by_id=oracle_score_by_id,
        metric=metric,
        dimensions=dimensions,
    ):
        return _result(FlatOracleAgreementKind.EXECUTION_ORDER_EQUIVALENT)
    return _result(
        FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH,
        "FLAT_ORACLE_NON_TIE_ORDER_MISMATCH",
    )
