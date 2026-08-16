"""DATASET-001 generation, threshold calibration, and boundary fixtures.

Purpose:
    Generate the immutable synthetic dataset and semantic micro-fixtures.
Inputs:
    ``DatasetSpec`` fixed to Generator(PCG64(20260801)) for production use.
Outputs:
    Little-endian float32 base/calibration/measured arrays and frozen thresholds.
Complexity:
    Generation is O((N+Q)D); calibration is O(MND) for M calibration queries.
Failure modes:
    Invalid spec, non-finite samples, or insufficient vectors for target cardinalities.
Configuration:
    EXP-001 uses 10,000 base, 50 calibration, 200 measured, dimension 128.
Extension points:
    Tests may inject a smaller spec; the CLI refuses a non-EXP-001 spec.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

from .config import (
    EXP001_DATASET_SPEC,
    THRESHOLD_LABELS,
    THRESHOLD_TARGETS,
    ContractViolation,
    DatasetSpec,
    Metric,
)
from .oracle import exact_scores, threshold_mask


@dataclass(frozen=True, slots=True)
class DatasetBundle:
    """Generated DATASET-001 arrays before artifact serialization."""

    ids: npt.NDArray[np.int64]
    base_vectors: npt.NDArray[np.float32]
    calibration_queries: npt.NDArray[np.float32]
    measured_queries: npt.NDArray[np.float32]
    spec: DatasetSpec


@dataclass(frozen=True, slots=True)
class FrozenThreshold:
    """One calibrated threshold and its observed calibration cardinality."""

    label: str
    target_cardinality: int
    radius: float
    median_cardinality: float

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""

        return {
            "label": self.label,
            "target_cardinality": self.target_cardinality,
            "radius": self.radius,
            "median_cardinality": self.median_cardinality,
        }


@dataclass(frozen=True, slots=True)
class BoundaryFixture:
    """Deterministic semantic fixture excluded from benchmark measurements."""

    name: str
    category: str
    metric: Metric
    ids: tuple[int, ...]
    base_vectors: tuple[tuple[float, ...], ...]
    query: tuple[float, ...]
    radius: float
    range_filter: float
    limit: int
    expected_ids: tuple[int, ...]
    expected_full_count: int

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""

        return {
            "name": self.name,
            "category": self.category,
            "metric": self.metric.value,
            "ids": list(self.ids),
            "base_vectors": [list(vector) for vector in self.base_vectors],
            "query": list(self.query),
            "radius": self.radius,
            "range_filter": self.range_filter,
            "limit": self.limit,
            "expected_ids": list(self.expected_ids),
            "expected_full_count": self.expected_full_count,
        }


def _validate_spec(spec: DatasetSpec) -> None:
    integer_values = (
        spec.seed,
        spec.dimensions,
        spec.base_count,
        spec.calibration_query_count,
        spec.measured_query_count,
    )
    if any(isinstance(value, bool) or value <= 0 for value in integer_values):
        raise ContractViolation("dataset dimensions/counts/seed must be positive integers")
    if spec.dtype != "<f4":
        raise ContractViolation("dataset dtype must be little-endian float32 (<f4)")
    if spec.base_count < max(THRESHOLD_TARGETS):
        raise ContractViolation("base_count is smaller than a threshold target")


def generate_dataset(spec: DatasetSpec = EXP001_DATASET_SPEC) -> DatasetBundle:
    """Generate deterministic standard-normal vectors with one PCG64 stream.

    NumPy first produces float64 standard-normal samples; they are then stored as
    little-endian float32. Base vectors are drawn first, followed by all 250 query
    vectors; the first 50 query rows are calibration-only.
    """

    _validate_spec(spec)
    generator = np.random.Generator(np.random.PCG64(spec.seed))
    base = generator.standard_normal((spec.base_count, spec.dimensions)).astype(
        "<f4", copy=False
    )
    queries = generator.standard_normal((spec.query_count, spec.dimensions)).astype(
        "<f4", copy=False
    )
    if not np.all(np.isfinite(base)) or not np.all(np.isfinite(queries)):
        raise ContractViolation("generated dataset contains non-finite values")
    split = spec.calibration_query_count
    return DatasetBundle(
        ids=np.arange(spec.base_count, dtype=np.int64),
        base_vectors=np.ascontiguousarray(base),
        calibration_queries=np.ascontiguousarray(queries[:split]),
        measured_queries=np.ascontiguousarray(queries[split:]),
        spec=spec,
    )


def _threshold_for_target(
    score_rows: npt.NDArray[np.float64],
    metric: Metric,
    target: int,
) -> tuple[float, float]:
    if target <= 0 or target > score_rows.shape[1]:
        raise ContractViolation("target cardinality is outside the base-vector count")
    if metric is Metric.L2:
        ranked = np.sort(score_rows, axis=1)
        boundary = ranked[:, target - 1]
        radius = float(np.nextafter(np.median(boundary), np.inf))
        range_filter = 0.0
    else:
        ranked = np.sort(score_rows, axis=1)[:, ::-1]
        boundary = ranked[:, target - 1]
        radius = float(np.nextafter(np.median(boundary), -np.inf))
        range_filter = 1.0
    cardinalities = np.count_nonzero(
        threshold_mask(score_rows, metric, radius, range_filter), axis=1
    )
    return radius, float(np.median(cardinalities))


def calibrate_thresholds(
    base_vectors: npt.NDArray[np.float32],
    calibration_queries: npt.NDArray[np.float32],
) -> dict[Metric, tuple[FrozenThreshold, ...]]:
    """Freeze three thresholds per metric using calibration queries only.

    For each target k, the threshold is the outward ``nextafter`` of the median
    per-query k-th score. This preserves the contract's strict inequality while
    targeting a median full-oracle cardinality near k.
    """

    if calibration_queries.ndim != 2:
        raise ContractViolation("calibration_queries must be two-dimensional")
    if calibration_queries.shape[0] == 0:
        raise ContractViolation("calibration_queries must not be empty")
    calibrated: dict[Metric, tuple[FrozenThreshold, ...]] = {}
    for metric in (Metric.L2, Metric.COSINE):
        rows = np.stack(
            [exact_scores(base_vectors, query, metric) for query in calibration_queries]
        )
        values: list[FrozenThreshold] = []
        for label, target in zip(THRESHOLD_LABELS, THRESHOLD_TARGETS, strict=True):
            radius, median_cardinality = _threshold_for_target(rows, metric, target)
            values.append(
                FrozenThreshold(
                    label=label,
                    target_cardinality=target,
                    radius=radius,
                    median_cardinality=median_cardinality,
                )
            )
        calibrated[metric] = tuple(values)
    return calibrated


def threshold_radii(
    thresholds: Mapping[Metric, tuple[FrozenThreshold, ...]],
) -> dict[Metric, tuple[float, ...]]:
    """Extract ordered radii for search-configuration construction."""

    return {
        metric: tuple(value.radius for value in thresholds[metric])
        for metric in (Metric.L2, Metric.COSINE)
    }


def boundary_fixtures() -> tuple[BoundaryFixture, ...]:
    """Return the immutable semantic micro-dataset required by EXP-001."""

    cap_vectors = tuple((float(index + 1), 0.0) for index in range(105))
    return (
        BoundaryFixture(
            name="l2-threshold-equality",
            category="threshold-equality",
            metric=Metric.L2,
            ids=(7,),
            base_vectors=((1.0, 0.0),),
            query=(0.0, 0.0),
            radius=1.0,
            range_filter=0.0,
            limit=100,
            expected_ids=(),
            expected_full_count=0,
        ),
        BoundaryFixture(
            name="cosine-threshold-equality",
            category="threshold-equality",
            metric=Metric.COSINE,
            ids=(8,),
            base_vectors=((0.0, 1.0),),
            query=(1.0, 0.0),
            radius=0.0,
            range_filter=1.0,
            limit=100,
            expected_ids=(),
            expected_full_count=0,
        ),
        BoundaryFixture(
            name="empty-result",
            category="empty-result",
            metric=Metric.L2,
            ids=(0, 1),
            base_vectors=((1.0, 0.0), (2.0, 0.0)),
            query=(0.0, 0.0),
            radius=0.5,
            range_filter=0.0,
            limit=100,
            expected_ids=(),
            expected_full_count=0,
        ),
        BoundaryFixture(
            name="all-match",
            category="all-match",
            metric=Metric.COSINE,
            ids=(2, 1, 3),
            base_vectors=((1.0, 0.0), (1.0, 1.0), (2.0, 1.0)),
            query=(1.0, 0.0),
            radius=0.0,
            range_filter=1.0,
            limit=100,
            expected_ids=(2, 3, 1),
            expected_full_count=3,
        ),
        BoundaryFixture(
            name="duplicate-distance",
            category="duplicate-distance",
            metric=Metric.L2,
            ids=(3, 1, 2),
            base_vectors=((1.0, 0.0), (-1.0, 0.0), (0.0, 1.0)),
            query=(0.0, 0.0),
            radius=2.0,
            range_filter=0.0,
            limit=100,
            expected_ids=(1, 2, 3),
            expected_full_count=3,
        ),
        BoundaryFixture(
            name="result-cap",
            category="result-cap",
            metric=Metric.L2,
            ids=tuple(range(105)),
            base_vectors=cap_vectors,
            query=(0.0, 0.0),
            radius=20_000.0,
            range_filter=0.0,
            limit=100,
            expected_ids=tuple(range(100)),
            expected_full_count=105,
        ),
    )
