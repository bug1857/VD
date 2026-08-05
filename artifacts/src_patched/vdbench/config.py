"""Immutable EXP-001 contract values and validation.

Purpose:
    Keep every dataset, index, query, and environment value in one typed module.
Inputs:
    Frozen thresholds produced from DATASET-001 calibration queries.
Outputs:
    Validated search configurations and deterministic seed streams.
Dependencies:
    NumPy only, for SeedSequence-based deterministic seed derivation.
Failure modes:
    Any out-of-contract value raises ``ContractViolation`` before a database call.
Extension points:
    A future experiment must define a new spec instead of mutating these constants.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Mapping, Sequence

import numpy as np


class ContractViolation(ValueError):
    """Raised when an operation would violate the immutable EXP-001 contract."""


class Metric(StrEnum):
    """Distance/similarity metrics authorized by EXP-001."""

    L2 = "L2"
    COSINE = "COSINE"


class IndexTrack(StrEnum):
    """Index tracks authorized by EXP-001."""

    FLAT = "FLAT"
    HNSW = "HNSW"


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """Immutable DATASET-001 shape and generator contract."""

    dataset_id: str
    version: str
    seed: int
    dimensions: int
    base_count: int
    calibration_query_count: int
    measured_query_count: int
    dtype: str
    distribution: str
    generator: str

    @property
    def query_count(self) -> int:
        """Return the total number of calibration and measured queries."""

        return self.calibration_query_count + self.measured_query_count

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""

        return asdict(self)


@dataclass(frozen=True, slots=True)
class EnvironmentPins:
    """ENV-001 values copied from the verified provisioning evidence."""

    uri: str
    health_uri: str
    milvus_server: str
    pymilvus: str
    docker_desktop: str
    docker_engine: str
    docker_compose: str
    milvus_image: str
    milvus_platform_manifest: str
    etcd_image: str
    etcd_platform_manifest: str
    minio_image: str
    minio_platform_manifest: str
    compose_vendor_sha256: str
    compose_override_sha256: str
    compose_effective_sha256: str
    resource_controls: str

    def as_dict(self) -> dict[str, str]:
        """Return a JSON-serializable representation."""

        return asdict(self)


@dataclass(frozen=True, slots=True)
class SearchConfiguration:
    """One immutable metric/threshold/index/search configuration."""

    metric: Metric
    threshold_label: str
    radius: float
    index_track: IndexTrack
    ef: int | None = None
    limit: int = 100
    consistency_level: str = "Strong"

    @property
    def range_filter(self) -> float:
        """Return the contract-fixed outer range bound for the metric."""

        return 0.0 if self.metric is Metric.L2 else 1.0

    @property
    def key(self) -> str:
        """Return a stable identifier suitable for manifests and JSONL records."""

        ef_value = "none" if self.ef is None else str(self.ef)
        return (
            f"{self.metric.value}:{self.threshold_label}:"
            f"{self.index_track.value}:ef={ef_value}"
        )

    def validate(self) -> None:
        """Reject any value outside ARCHITECTURE.md's EXP-001 registry."""

        if isinstance(self.limit, bool) or self.limit != RESULT_LIMIT:
            raise ContractViolation(f"limit must equal {RESULT_LIMIT}")
        if self.consistency_level != CONSISTENCY_LEVEL:
            raise ContractViolation("consistency_level must equal Strong")
        if not np.isfinite(self.radius):
            raise ContractViolation("radius must be finite")
        if self.metric is Metric.L2:
            if not 0.0 < self.radius:
                raise ContractViolation("L2 radius must be greater than 0.0")
        elif not -1.0 <= self.radius < 1.0:
            raise ContractViolation("COSINE radius must be in [-1.0, 1.0)")

        if self.index_track is IndexTrack.FLAT:
            if self.ef is not None:
                raise ContractViolation("FLAT must not receive ef")
            return

        if isinstance(self.ef, bool) or not isinstance(self.ef, int):
            raise ContractViolation("HNSW ef must be an integer")
        if self.ef not in HNSW_EF_SWEEP:
            raise ContractViolation(f"HNSW ef must be one of {HNSW_EF_SWEEP}")
        if self.ef < self.limit:
            raise ContractViolation("HNSW ef must be at least limit")

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable validated representation."""

        self.validate()
        return {
            "key": self.key,
            "metric": self.metric.value,
            "threshold_label": self.threshold_label,
            "radius": self.radius,
            "range_filter": self.range_filter,
            "index_track": self.index_track.value,
            "ef": self.ef,
            "limit": self.limit,
            "consistency_level": self.consistency_level,
        }


EXP001_DATASET_SPEC = DatasetSpec(
    dataset_id="DATASET-001",
    version="DATASET-001-v1",
    seed=20260801,
    dimensions=128,
    base_count=10_000,
    calibration_query_count=50,
    measured_query_count=200,
    dtype="<f4",
    distribution="independent standard normal",
    generator="numpy.random.Generator(numpy.random.PCG64(seed))",
)

ENV001_PINS = EnvironmentPins(
    uri="http://localhost:19530",
    health_uri="http://localhost:9091/healthz",
    milvus_server="3.0.0",
    pymilvus="3.0.1",
    docker_desktop="4.84.0",
    docker_engine="29.6.2",
    docker_compose="v5.3.1",
    milvus_image=(
        "milvusdb/milvus:v3.0.0@"
        "sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862"
    ),
    milvus_platform_manifest=(
        "linux/arm64@sha256:bfab7739a0479cd81ffdf5e473f88c5b143678c2520a06a19f86f35ecd586cad"
    ),
    etcd_image=(
        "quay.io/coreos/etcd:v3.5.25@"
        "sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366"
    ),
    etcd_platform_manifest=(
        "linux/arm64@sha256:8da34a9df5dc1bd879bea716a301113c4e49b6bbdbe5778214707c6043ccf65d"
    ),
    minio_image=(
        "minio/minio:RELEASE.2024-05-28T17-19-04Z@"
        "sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08"
    ),
    minio_platform_manifest=(
        "linux/arm64@sha256:fa7be14ee3f914469274c5dfc05949e0092500a71de4681f1f1b6b39275a13b1"
    ),
    compose_vendor_sha256=(
        "4518b95ddd719542558f48d84e9a53a5910099888b8ef985ab122524db7d97d1"
    ),
    compose_override_sha256=(
        "bd97b91052ac642593c0af33aa7e90519e472a168d4ada48ba71f0846a4ee8c6"
    ),
    compose_effective_sha256=(
        "76310aee683a1dab714679f0f9202bc193ad87019e2e8bbf3c25fb46454ea217"
    ),
    resource_controls=(
        "Docker VM 6 vCPU/6 GiB RAM/2 GiB swap; Milvus 4 CPU/4 GiB; "
        "etcd 1 CPU/512 MiB; MinIO 1 CPU/1 GiB; no cpuset"
    ),
)

METRICS = (Metric.L2, Metric.COSINE)
INDEX_TRACKS = (IndexTrack.FLAT, IndexTrack.HNSW)
THRESHOLD_TARGETS = (5, 25, 75)
THRESHOLD_LABELS = ("target-005", "target-025", "target-075")
RESULT_LIMIT = 100
CONSISTENCY_LEVEL = "Strong"
HNSW_M = 16
HNSW_EF_CONSTRUCTION = 200
HNSW_EF_SWEEP = (100, 200, 400, 800, 1600)
MEASURED_REPETITIONS = 5
INSERT_BATCH_SIZE = 1_000
INDEX_NAME = "vector_index"
PRIMARY_FIELD = "id"
VECTOR_FIELD = "vector"
NUMERIC_TOLERANCE = 1e-6


def build_search_configurations(
    thresholds: Mapping[Metric | str, Sequence[float]],
) -> tuple[SearchConfiguration, ...]:
    """Build all 36 contract configurations from three frozen thresholds per metric."""

    configurations: list[SearchConfiguration] = []
    for metric in METRICS:
        values = thresholds.get(metric, thresholds.get(metric.value))
        if values is None or len(values) != len(THRESHOLD_LABELS):
            raise ContractViolation(
                f"{metric.value} must provide exactly three frozen thresholds"
            )
        for label, radius in zip(THRESHOLD_LABELS, values, strict=True):
            flat = SearchConfiguration(
                metric=metric,
                threshold_label=label,
                radius=float(radius),
                index_track=IndexTrack.FLAT,
            )
            flat.validate()
            configurations.append(flat)
            for ef in HNSW_EF_SWEEP:
                hnsw = SearchConfiguration(
                    metric=metric,
                    threshold_label=label,
                    radius=float(radius),
                    index_track=IndexTrack.HNSW,
                    ef=ef,
                )
                hnsw.validate()
                configurations.append(hnsw)
    return tuple(configurations)


def derive_seed(stream_id: int, *, primary_seed: int = 20260801) -> int:
    """Derive a deterministic uint64 seed using NumPy SeedSequence.

    The derivation method and every resulting value are written to the run manifest.
    ``stream_id`` is explicit so adding a new stream cannot silently shift existing seeds.
    """

    if isinstance(stream_id, bool) or not isinstance(stream_id, int) or stream_id < 0:
        raise ContractViolation("stream_id must be a non-negative integer")
    sequence = np.random.SeedSequence([primary_seed, stream_id])
    return int(sequence.generate_state(1, dtype=np.uint64)[0])
