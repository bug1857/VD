"""Deterministic EXP-001 warm-up, validation, and measurement protocol."""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter_ns
from typing import Callable, Mapping, Protocol, Sequence

import numpy as np
import numpy.typing as npt

from .config import (
    MEASURED_REPETITIONS,
    NUMERIC_TOLERANCE,
    ContractViolation,
    IndexTrack,
    Metric,
    SearchConfiguration,
    derive_seed,
)
from .milvus import CollectionIdentity, SearchHit
from .oracle import OracleResult, capped_threshold_recall, threshold_violations


class SearchBackend(Protocol):
    def search(
        self,
        *,
        name: str,
        query: npt.NDArray[np.float32],
        configuration: SearchConfiguration,
    ) -> tuple[SearchHit, ...]: ...

    def index_identity(
        self, name: str, metric: Metric, track: IndexTrack
    ) -> CollectionIdentity: ...


@dataclass(frozen=True, slots=True)
class ScheduledConfiguration:
    configuration_key: str
    query_order: tuple[int, ...]
    query_seed: int

    def as_dict(self) -> dict[str, object]:
        return {
            "configuration_key": self.configuration_key,
            "query_order": list(self.query_order),
            "query_seed": self.query_seed,
        }


@dataclass(frozen=True, slots=True)
class RepetitionSchedule:
    repetition: int
    configuration_seed: int
    configurations: tuple[ScheduledConfiguration, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "repetition": self.repetition,
            "configuration_seed": self.configuration_seed,
            "configurations": [value.as_dict() for value in self.configurations],
        }


@dataclass(frozen=True, slots=True)
class ExperimentSchedule:
    warmup_configuration_seed: int
    warmup: tuple[ScheduledConfiguration, ...]
    repetitions: tuple[RepetitionSchedule, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "warmup_configuration_seed": self.warmup_configuration_seed,
            "warmup": [value.as_dict() for value in self.warmup],
            "repetitions": [value.as_dict() for value in self.repetitions],
        }


def _permutation(length: int, stream_id: int) -> tuple[int, ...]:
    generator = np.random.Generator(np.random.PCG64(derive_seed(stream_id)))
    return tuple(int(value) for value in generator.permutation(length))


def build_schedule(
    configurations: Sequence[SearchConfiguration],
    *,
    calibration_query_count: int = 50,
    measured_query_count: int = 200,
    repetitions: int = MEASURED_REPETITIONS,
) -> ExperimentSchedule:
    """Create all deterministic configuration/query orders and expose their seeds."""

    if len({value.key for value in configurations}) != len(configurations):
        raise ContractViolation("configuration keys must be unique")
    if len(configurations) != 36:
        raise ContractViolation("EXP-001 schedule requires exactly 36 configurations")
    if (
        calibration_query_count != 50
        or measured_query_count != 200
        or repetitions != 5
    ):
        raise ContractViolation("schedule must use 50 warm-up, 200 measured, 5 repetitions")

    warm_config_stream = 1000
    warm_order = _permutation(len(configurations), warm_config_stream)
    warm_query_stream = 2000
    warm_query_order = _permutation(calibration_query_count, warm_query_stream)
    warm_query_seed = derive_seed(warm_query_stream)
    warmup: list[ScheduledConfiguration] = []
    for config_index in warm_order:
        warmup.append(
            ScheduledConfiguration(
                configuration_key=configurations[config_index].key,
                query_order=warm_query_order,
                query_seed=warm_query_seed,
            )
        )

    repetition_values: list[RepetitionSchedule] = []
    for repetition in range(repetitions):
        config_stream = 3000 + repetition
        order = _permutation(len(configurations), config_stream)
        query_stream = 10_000 + repetition
        query_order = _permutation(measured_query_count, query_stream)
        query_seed = derive_seed(query_stream)
        scheduled: list[ScheduledConfiguration] = []
        for config_index in order:
            scheduled.append(
                ScheduledConfiguration(
                    configuration_key=configurations[config_index].key,
                    query_order=query_order,
                    query_seed=query_seed,
                )
            )
        repetition_values.append(
            RepetitionSchedule(
                repetition=repetition,
                configuration_seed=derive_seed(config_stream),
                configurations=tuple(scheduled),
            )
        )
    return ExperimentSchedule(
        warmup_configuration_seed=derive_seed(warm_config_stream),
        warmup=tuple(warmup),
        repetitions=tuple(repetition_values),
    )


def _assert_flat_match(
    actual: Sequence[SearchHit],
    expected: OracleResult,
    configuration: SearchConfiguration,
) -> None:
    actual_ids = tuple(hit.id for hit in actual)
    if actual_ids != expected.ids:
        raise ContractViolation(
            f"FLAT/oracle ordered ID disagreement: actual={actual_ids}, expected={expected.ids}"
        )
    violations = threshold_violations(
        (hit.score for hit in actual),
        configuration.metric,
        radius=configuration.radius,
        range_filter=configuration.range_filter,
        tolerance=NUMERIC_TOLERANCE,
    )
    if violations:
        raise ContractViolation(f"FLAT returned threshold-invalid scores: {violations}")


def validate_flat_semantics(
    *,
    backend: SearchBackend,
    configurations: Sequence[SearchConfiguration],
    collection_names: Mapping[tuple[Metric, IndexTrack], str],
    measured_queries: npt.NDArray[np.float32],
    references: Mapping[tuple[str, int], OracleResult],
) -> None:
    """Run every untimed FLAT/oracle check before any HNSW timing."""

    for configuration in configurations:
        if configuration.index_track is not IndexTrack.FLAT:
            continue
        name = collection_names[(configuration.metric, IndexTrack.FLAT)]
        for query_index, query in enumerate(measured_queries):
            actual = backend.search(name=name, query=query, configuration=configuration)
            _assert_flat_match(
                actual, references[(configuration.key, query_index)], configuration
            )


def _identity_payload(identity: CollectionIdentity) -> dict[str, object]:
    return {
        "collection_name": identity.collection_name,
        "metric": identity.metric,
        "index_track": identity.index_track,
        "description": identity.description,
    }


def run_protocol(
    *,
    backend: SearchBackend,
    configurations: Sequence[SearchConfiguration],
    schedule: ExperimentSchedule,
    collection_names: Mapping[tuple[Metric, IndexTrack], str],
    calibration_queries: npt.NDArray[np.float32],
    measured_queries: npt.NDArray[np.float32],
    references: Mapping[tuple[str, int], OracleResult],
    sink: Callable[[Mapping[str, object]], None],
    clock_ns: Callable[[], int] = perf_counter_ns,
) -> tuple[dict[str, object], ...]:
    """Execute the single-client protocol with exact timing/write boundaries.

    Oracle references must already exist. The timer starts immediately before
    ``backend.search`` and stops only after it returns a materialized tuple. Recall,
    diagnostics, and artifact writes all occur after the ending timestamp.
    """

    by_key = {value.key: value for value in configurations}
    validate_flat_semantics(
        backend=backend,
        configurations=configurations,
        collection_names=collection_names,
        measured_queries=measured_queries,
        references=references,
    )

    before: dict[Metric, CollectionIdentity] = {}
    for metric in Metric:
        name = collection_names[(metric, IndexTrack.HNSW)]
        before[metric] = backend.index_identity(name, metric, IndexTrack.HNSW)

    for scheduled in schedule.warmup:
        configuration = by_key[scheduled.configuration_key]
        name = collection_names[(configuration.metric, configuration.index_track)]
        for query_index in scheduled.query_order:
            backend.search(
                name=name,
                query=calibration_queries[query_index],
                configuration=configuration,
            )

    records: list[dict[str, object]] = []
    for repetition in schedule.repetitions:
        for scheduled in repetition.configurations:
            configuration = by_key[scheduled.configuration_key]
            name = collection_names[(configuration.metric, configuration.index_track)]
            segment_before = None
            if configuration.index_track is IndexTrack.HNSW:
                segment_before = backend.index_identity(
                    name, configuration.metric, IndexTrack.HNSW
                )
                if segment_before != before[configuration.metric]:
                    raise ContractViolation(
                        f"HNSW index identity changed before {configuration.key}"
                    )
            for sequence, query_index in enumerate(scheduled.query_order):
                started_ns = clock_ns()
                try:
                    hits = tuple(
                        backend.search(
                            name=name,
                            query=measured_queries[query_index],
                            configuration=configuration,
                        )
                    )
                    ended_ns = clock_ns()
                except Exception as error:
                    ended_ns = clock_ns()
                    failure = {
                        "status": "failed",
                        "repetition": repetition.repetition,
                        "configuration_key": configuration.key,
                        "query_index": query_index,
                        "query_sequence": sequence,
                        "latency_ns": ended_ns - started_ns,
                        "error_type": type(error).__name__,
                        "error": str(error),
                    }
                    sink(failure)
                    records.append(failure)
                    continue

                reference = references[(configuration.key, query_index)]
                violations = threshold_violations(
                    (hit.score for hit in hits),
                    configuration.metric,
                    radius=configuration.radius,
                    range_filter=configuration.range_filter,
                    tolerance=NUMERIC_TOLERANCE,
                )
                record = {
                    "status": "success",
                    "repetition": repetition.repetition,
                    "configuration_key": configuration.key,
                    "query_index": query_index,
                    "query_sequence": sequence,
                    "latency_ns": ended_ns - started_ns,
                    "result_ids": [hit.id for hit in hits],
                    "result_scores": [hit.score for hit in hits],
                    "result_cardinality": len(hits),
                    "oracle_full_cardinality": reference.full_count,
                    "oracle_capped": reference.capped,
                    "recall_at_threshold": capped_threshold_recall(
                        (hit.id for hit in hits), reference.ids
                    ),
                    "threshold_violations": list(violations),
                    "threshold_tolerance": NUMERIC_TOLERANCE,
                }
                sink(record)
                records.append(record)

            if configuration.index_track is IndexTrack.HNSW:
                segment_after = backend.index_identity(
                    name, configuration.metric, IndexTrack.HNSW
                )
                if segment_after != segment_before:
                    raise ContractViolation(
                        f"HNSW index identity changed during {configuration.key}"
                    )
                sink(
                    {
                        "status": "index_identity_unchanged",
                        "scope": "measured_configuration_segment",
                        "repetition": repetition.repetition,
                        "configuration_key": configuration.key,
                        "before": _identity_payload(segment_before),
                        "after": _identity_payload(segment_after),
                    }
                )

    after: dict[Metric, CollectionIdentity] = {}
    for metric in Metric:
        name = collection_names[(metric, IndexTrack.HNSW)]
        after[metric] = backend.index_identity(name, metric, IndexTrack.HNSW)
        if before[metric] != after[metric]:
            raise ContractViolation(f"HNSW index identity changed for {metric.value}")
        sink(
            {
                "status": "index_identity_unchanged",
                "metric": metric.value,
                "before": _identity_payload(before[metric]),
                "after": _identity_payload(after[metric]),
            }
        )
    return tuple(records)


def configuration_manifest(
    configurations: Sequence[SearchConfiguration], schedule: ExperimentSchedule
) -> dict[str, object]:
    """Return the exact immutable configuration and randomized schedule payload."""

    return {
        "configurations": [value.as_dict() for value in configurations],
        "schedule": schedule.as_dict(),
        "timing_boundary": (
            "start immediately before synchronous client search; stop after complete "
            "response materialization; exclude oracle, diagnostics, and artifact writes"
        ),
        "concurrency": 1,
        "client": "one synchronous client",
        "measured_repetitions": 5,
    }


def deliberate_unreachable_probe(
    search_call: Callable[[], object],
    sink: Callable[[Mapping[str, object]], None],
) -> None:
    """Require a stopped/unreachable endpoint to fail without a success record."""

    try:
        search_call()
    except Exception as error:
        sink(
            {
                "status": "expected_failure",
                "probe": "unreachable_milvus",
                "error_type": type(error).__name__,
                "error": str(error),
            }
        )
        return
    raise ContractViolation("unreachable-Milvus probe unexpectedly succeeded")
