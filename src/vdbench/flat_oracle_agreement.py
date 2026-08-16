"""Canonical ordered FLAT/oracle agreement for governed range-query evidence.

The independent oracle accumulates scores in binary64 while the governed
vectors and Milvus search path use binary32.  Distinct oracle scores can
therefore collapse to the same representable binary32 value.  This module
keeps exact capped membership mandatory and permits a permutation only inside
one such deterministic binary32 oracle-score group.  It never applies a free
tolerance and never replaces ordered agreement with global set equality.

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
5. Permutation is legal ONLY inside one exact binary32 oracle-score tie group.
   Reordering across two groups is `NON_TIE_ORDER_MISMATCH`.
6. No capped-membership substitution (the emphatic restatement of rule 1,
   because it is the rule most often mistaken for over-strictness).

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

from .config import NUMERIC_TOLERANCE, Metric
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
    MEMBERSHIP_MISMATCH = "MEMBERSHIP_MISMATCH"
    NON_TIE_ORDER_MISMATCH = "NON_TIE_ORDER_MISMATCH"
    INVALID_EVIDENCE = "INVALID_EVIDENCE"


@dataclass(frozen=True, slots=True)
class FlatOracleAgreementResult:
    """Structured comparison result; only the first two kinds agree."""

    kind: FlatOracleAgreementKind
    reason_codes: tuple[str, ...]

    @property
    def agrees(self) -> bool:
        return self.kind in {
            FlatOracleAgreementKind.EXACT_ORDERED,
            FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT,
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


def compare_flat_oracle_hits(
    *,
    flat_hits: object,
    oracle_result: object,
    metric: object,
    radius: object,
    range_filter: object,
    limit: object,
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
    return _result(
        FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH,
        "FLAT_ORACLE_NON_TIE_ORDER_MISMATCH",
    )
