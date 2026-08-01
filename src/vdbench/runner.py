"""EXP-002 orchestration; importing this module never contacts Milvus."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from .artifacts import (
    JsonlSink,
    build_run_manifest,
    sha256_file,
    verify_dataset_artifacts,
    write_immutable_json,
)
from .config import (
    EXP001_DATASET_SPEC,
    ENV001_PINS,
    IndexTrack,
    Metric,
    SearchConfiguration,
    build_search_configurations,
)
from .dataset import DatasetBundle, boundary_fixtures
from .milvus import MilvusHarness, collection_name
from .metrics import summarize_records
from .oracle import OracleResult, exact_range_search
from .protocol import build_schedule, configuration_manifest, run_protocol


def load_dataset(dataset_dir: Path) -> tuple[DatasetBundle, dict[Metric, tuple[float, ...]], dict[str, Any]]:
    """Verify and load immutable DATASET-001 artifacts."""

    manifest = verify_dataset_artifacts(dataset_dir)
    if manifest["dataset"] != EXP001_DATASET_SPEC.as_dict():
        raise ValueError("dataset manifest does not match DATASET-001-v1")
    bundle = DatasetBundle(
        ids=np.load(dataset_dir / "base_ids.npy", allow_pickle=False),
        base_vectors=np.load(dataset_dir / "base_vectors.npy", allow_pickle=False),
        calibration_queries=np.load(dataset_dir / "calibration_queries.npy", allow_pickle=False),
        measured_queries=np.load(dataset_dir / "measured_queries.npy", allow_pickle=False),
        spec=EXP001_DATASET_SPEC,
    )
    import json

    payload = json.loads((dataset_dir / "thresholds.json").read_text(encoding="utf-8"))
    thresholds = {
        metric: tuple(float(item["radius"]) for item in payload[metric.value])
        for metric in Metric
    }
    return bundle, thresholds, manifest


def oracle_references(bundle: DatasetBundle, configurations: tuple) -> dict[tuple[str, int], OracleResult]:
    """Precompute exact references outside all search timing boundaries."""

    references: dict[tuple[str, int], OracleResult] = {}
    cache: dict[tuple[Metric, float, int], OracleResult] = {}
    for configuration in configurations:
        for query_index, query in enumerate(bundle.measured_queries):
            cache_key = (configuration.metric, configuration.radius, query_index)
            if cache_key not in cache:
                cache[cache_key] = exact_range_search(
                    bundle.base_vectors,
                    bundle.ids,
                    query,
                    configuration.metric,
                    radius=configuration.radius,
                    range_filter=configuration.range_filter,
                    limit=configuration.limit,
                )
            references[(configuration.key, query_index)] = cache[cache_key]
    return references


def validate_boundary_fixtures_live(
    *, backend: MilvusHarness, collection_prefix: str
) -> tuple[dict[str, object], ...]:
    """Validate every semantic fixture against a dedicated untimed FLAT collection."""

    results: list[dict[str, object]] = []
    for fixture in boundary_fixtures():
        base = np.asarray(fixture.base_vectors, dtype="<f4")
        query = np.asarray(fixture.query, dtype="<f4")
        spec = replace(
            EXP001_DATASET_SPEC,
            dataset_id=f"DATASET-001-{fixture.name}",
            version="boundary-fixture-v1",
            dimensions=base.shape[1],
            base_count=base.shape[0],
            calibration_query_count=1,
            measured_query_count=1,
        )
        bundle = DatasetBundle(
            ids=np.asarray(fixture.ids, dtype=np.int64),
            base_vectors=base,
            calibration_queries=query[None, :],
            measured_queries=query[None, :],
            spec=spec,
        )
        name = collection_name(
            f"{collection_prefix}_{fixture.name}", fixture.metric, IndexTrack.FLAT
        )
        backend.create_and_load_collection(
            name=name,
            metric=fixture.metric,
            track=IndexTrack.FLAT,
            dataset=bundle,
        )
        # Boundary thresholds are semantic fixtures, not calibrated benchmark values.
        configuration = SearchConfiguration(
            metric=fixture.metric,
            threshold_label=fixture.name,
            radius=fixture.radius,
            index_track=IndexTrack.FLAT,
            limit=fixture.limit,
        )
        actual = backend.search(name=name, query=query, configuration=configuration)
        actual_ids = tuple(hit.id for hit in actual)
        if actual_ids != fixture.expected_ids:
            raise ValueError(
                f"boundary fixture {fixture.name} failed: "
                f"actual={actual_ids}, expected={fixture.expected_ids}"
            )
        results.append(
            {
                "fixture": fixture.name,
                "metric": fixture.metric.value,
                "actual_ids": list(actual_ids),
                "expected_ids": list(fixture.expected_ids),
                "status": "matched",
            }
        )
    return tuple(results)


def execute_live(
    *,
    repository: Path,
    dataset_dir: Path,
    run_dir: Path,
    collection_prefix: str,
) -> tuple[dict[str, object], ...]:
    """Execute EXP-002 against ENV-001 when explicitly invoked by a human.

    The current implementation task does not call this function.
    """

    from pymilvus import MilvusClient

    bundle, threshold_values, dataset_manifest = load_dataset(dataset_dir)
    configurations = build_search_configurations(threshold_values)
    schedule = build_schedule(configurations)
    references = oracle_references(bundle, configurations)
    names = {
        (metric, track): collection_name(collection_prefix, metric, track)
        for metric in Metric
        for track in IndexTrack
    }

    if run_dir.exists():
        raise ValueError(f"refusing to overwrite run directory: {run_dir}")
    run_dir.mkdir(parents=True)
    manifest = build_run_manifest(
        repository=repository,
        dataset_manifest=dataset_manifest,
        dataset_manifest_sha256=sha256_file(dataset_dir / "generation_manifest.json"),
        schedule=configuration_manifest(configurations, schedule),
        collection_prefix=collection_prefix,
        timestamp=datetime.now(timezone.utc),
    )
    write_immutable_json(run_dir / "run_manifest.json", manifest)
    sink = JsonlSink(run_dir / "raw_queries.jsonl")

    client = MilvusClient(uri=ENV001_PINS.uri)
    client.list_collections()
    backend = MilvusHarness(client, dimensions=bundle.spec.dimensions)
    # Boundary fixtures are two-dimensional, so they use a separate adapter while
    # sharing the same synchronous client and remaining outside all timed searches.
    boundary_backend = MilvusHarness(client, dimensions=2)
    boundary_results = validate_boundary_fixtures_live(
        backend=boundary_backend, collection_prefix=collection_prefix
    )
    write_immutable_json(run_dir / "boundary_results.json", boundary_results)
    for metric in Metric:
        for track in IndexTrack:
            backend.create_and_load_collection(
                name=names[(metric, track)],
                metric=metric,
                track=track,
                dataset=bundle,
            )
    records = run_protocol(
        backend=backend,
        configurations=configurations,
        schedule=schedule,
        collection_names=names,
        calibration_queries=bundle.calibration_queries,
        measured_queries=bundle.measured_queries,
        references=references,
        sink=sink,
    )
    write_immutable_json(run_dir / "summary.json", summarize_records(records))
    return records
