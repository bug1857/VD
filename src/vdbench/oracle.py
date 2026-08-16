"""Independent exact-distance oracle for EXP-001.

Purpose:
    Compute exact L2 and cosine reference results independently of Milvus.
Inputs:
    Stored little-endian float32 vectors and integer IDs.
Outputs:
    Float64-accumulated scores, threshold-valid ordered hits, and capped cardinality.
Complexity:
    O(ND) time and O(N) temporary memory per query for N vectors of dimension D.
Failure modes:
    Shape mismatch, non-finite data, zero-norm cosine vectors, or invalid thresholds.
Configuration:
    L2 is Milvus squared Euclidean distance; COSINE is clamped only after division.
Extension points:
    New metrics require a new experiment contract and explicit threshold semantics.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

from .config import RESULT_LIMIT, ContractViolation, Metric

FloatArray = npt.NDArray[np.floating]
IntArray = npt.NDArray[np.integer]


@dataclass(frozen=True, slots=True)
class OracleHit:
    """One exact oracle result."""

    id: int
    score: float


@dataclass(frozen=True, slots=True)
class OracleResult:
    """Ordered, capped oracle results plus the uncapped threshold cardinality."""

    hits: tuple[OracleHit, ...]
    full_count: int
    capped: bool

    @property
    def ids(self) -> tuple[int, ...]:
        """Return ordered IDs only."""

        return tuple(hit.id for hit in self.hits)


def _validated_inputs(
    base_vectors: FloatArray,
    query: FloatArray,
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    base = np.asarray(base_vectors)
    query_array = np.asarray(query)
    if base.ndim != 2:
        raise ContractViolation("base_vectors must be a two-dimensional array")
    if query_array.ndim != 1:
        raise ContractViolation("query must be a one-dimensional array")
    if base.shape[1] != query_array.shape[0]:
        raise ContractViolation("base/query dimensions do not match")
    if base.shape[0] == 0:
        raise ContractViolation("base_vectors must not be empty")
    if not np.all(np.isfinite(base)) or not np.all(np.isfinite(query_array)):
        raise ContractViolation("oracle inputs must be finite")
    return base.astype(np.float64, copy=False), query_array.astype(np.float64, copy=False)


def exact_scores(
    base_vectors: FloatArray,
    query: FloatArray,
    metric: Metric,
) -> npt.NDArray[np.float64]:
    """Compute exact scores with float64 accumulation.

    Milvus L2 intentionally returns the squared Euclidean value (the value before
    square root), so this oracle does the same. Cosine values are clamped only after
    the float64 dot-product/norm division.
    """

    base, query_array = _validated_inputs(base_vectors, query)
    if metric is Metric.L2:
        difference = base - query_array
        return np.einsum("ij,ij->i", difference, difference, dtype=np.float64)
    if metric is not Metric.COSINE:
        raise ContractViolation(f"unsupported metric: {metric}")

    query_norm = float(np.linalg.norm(query_array))
    base_norms = np.linalg.norm(base, axis=1)
    if query_norm == 0.0 or np.any(base_norms == 0.0):
        raise ContractViolation("cosine oracle does not accept zero-norm vectors")
    similarities = (base @ query_array) / (base_norms * query_norm)
    return np.clip(similarities, -1.0, 1.0)


def validate_range(metric: Metric, radius: float, range_filter: float) -> None:
    """Validate metric-specific EXP-001 range bounds."""

    if not np.isfinite(radius) or not np.isfinite(range_filter):
        raise ContractViolation("range bounds must be finite")
    if metric is Metric.L2:
        if range_filter != 0.0 or not range_filter < radius:
            raise ContractViolation("L2 requires range_filter=0.0 < radius")
        return
    if metric is Metric.COSINE:
        if range_filter != 1.0 or not -1.0 <= radius < range_filter:
            raise ContractViolation("COSINE requires -1.0 <= radius < range_filter=1.0")
        return
    raise ContractViolation(f"unsupported metric: {metric}")


def threshold_mask(
    scores: npt.NDArray[np.float64],
    metric: Metric,
    radius: float,
    range_filter: float,
) -> npt.NDArray[np.bool_]:
    """Return the strict EXP-001 threshold mask without numeric tolerance."""

    validate_range(metric, radius, range_filter)
    if metric is Metric.L2:
        return (range_filter <= scores) & (scores < radius)
    return (radius < scores) & (scores <= range_filter)


def exact_range_search(
    base_vectors: FloatArray,
    ids: IntArray,
    query: FloatArray,
    metric: Metric,
    *,
    radius: float,
    range_filter: float,
    limit: int = RESULT_LIMIT,
) -> OracleResult:
    """Return deterministic exact threshold results ordered by score then ID.

    L2 sorts ascending and COSINE sorts descending. Integer ID is the explicit
    duplicate-score tie breaker used by the boundary fixture and artifact oracle.
    """

    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ContractViolation("limit must be a positive integer")
    id_array = np.asarray(ids)
    if id_array.ndim != 1 or id_array.shape[0] != np.asarray(base_vectors).shape[0]:
        raise ContractViolation("ids must be one-dimensional and align with base_vectors")
    if len(np.unique(id_array)) != len(id_array):
        raise ContractViolation("ids must be unique")

    scores = exact_scores(base_vectors, query, metric)
    eligible = np.flatnonzero(threshold_mask(scores, metric, radius, range_filter))
    if metric is Metric.L2:
        order = np.lexsort((id_array[eligible], scores[eligible]))
    else:
        order = np.lexsort((id_array[eligible], -scores[eligible]))
    ordered = eligible[order]
    full_count = int(ordered.size)
    selected = ordered[:limit]
    hits = tuple(
        OracleHit(id=int(id_array[position]), score=float(scores[position]))
        for position in selected
    )
    return OracleResult(hits=hits, full_count=full_count, capped=full_count > limit)


def capped_threshold_recall(
    approximate_ids: Iterable[int],
    reference_ids: Iterable[int],
) -> float:
    """Compute EXP-001 capped recall@threshold."""

    approximate = tuple(int(value) for value in approximate_ids)
    reference = tuple(int(value) for value in reference_ids)
    if len(set(approximate)) != len(approximate):
        raise ContractViolation("approximate result IDs must be unique")
    if len(set(reference)) != len(reference):
        raise ContractViolation("reference result IDs must be unique")
    if not reference:
        return 1.0 if not approximate else 0.0
    return len(set(approximate).intersection(reference)) / len(reference)


def threshold_violations(
    scores: Iterable[float],
    metric: Metric,
    *,
    radius: float,
    range_filter: float,
    tolerance: float = 0.0,
) -> tuple[float, ...]:
    """Return scores that violate the strict threshold contract."""

    if not np.isfinite(tolerance) or tolerance < 0.0:
        raise ContractViolation("threshold tolerance must be finite and non-negative")
    values = np.asarray(tuple(scores), dtype=np.float64)
    if values.size == 0:
        return ()
    validate_range(metric, radius, range_filter)
    if metric is Metric.L2:
        valid = (range_filter - tolerance <= values) & (values < radius + tolerance)
    else:
        valid = (radius - tolerance < values) & (values <= range_filter + tolerance)
    return tuple(float(value) for value in values[~valid])
