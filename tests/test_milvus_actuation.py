from __future__ import annotations

import ast
import unittest
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import numpy as np

from vdbench.actuation import ActuationContext
from vdbench.config import IndexTrack, Metric
from vdbench.milvus import CollectionIdentity
from vdbench.milvus_actuation import (
    ActuationWorkload,
    CanaryBounds,
    CanaryPairedMeasurements,
    CollectionIdentityBinding,
    MilvusActuationClient,
    ShadowAuditTrace,
    StackHealth,
)
from vdbench.oracle import exact_range_search
from vdbench.policy import QualificationResult

REPOSITORY = Path(__file__).parents[1]
MODULE_PATH = REPOSITORY / "src" / "vdbench" / "milvus_actuation.py"
CONFIGURATION_ID = "config-v1"
DATA_ID = "data-v1"
INDEX_ID = "hnsw-identity-v1"
THRESHOLD_STRATUM = "target-025"
FLAT_NAME = "actuation_l2_flat"
HNSW_NAME = "actuation_l2_hnsw"
ROUTING_SEED = 20260804


def index_description(track: IndexTrack, *, build_id: int | None = None):
    value = {
        "index_name": "vector_index",
        "index_type": track.value,
        "metric_type": Metric.L2.value,
        "state": "Finished",
        "build_id": 11 if track is IndexTrack.FLAT else 22,
    }
    if track is IndexTrack.HNSW:
        value.update({"M": "16", "efConstruction": "200"})
    if build_id is not None:
        value["build_id"] = build_id
    return value


class FakePyMilvusClient:
    def __init__(
        self,
        *,
        base_ids: np.ndarray,
        base_vectors: np.ndarray,
        query_vectors: dict[int, np.ndarray],
    ) -> None:
        self.base_ids = base_ids
        self.base_vectors = base_vectors
        self.query_ids_by_bytes = {
            np.asarray(vector, dtype="<f4").tobytes(): query_id
            for query_id, vector in query_vectors.items()
        }
        self.search_calls: list[dict[str, object]] = []
        self.other_calls: list[tuple[str, dict[str, object]]] = []
        self.failures: set[tuple[str, int | None, int]] = set()
        self.flat_build_id = 11
        self.hnsw_build_id = 22
        self.loaded = True
        self.identity_capture_failures: set[str] = set()

    def search(self, **kwargs):
        vector = np.asarray(kwargs["data"][0], dtype="<f4")
        query_id = self.query_ids_by_bytes[vector.tobytes()]
        parameters = kwargs["search_params"]["params"]
        ef = parameters.get("ef")
        collection = kwargs["collection_name"]
        track = "flat" if collection == FLAT_NAME else "hnsw"
        self.search_calls.append(
            {
                "query_id": query_id,
                "collection": collection,
                "track": track,
                "ef": ef,
            }
        )
        if (track, ef, query_id) in self.failures:
            raise TimeoutError("injected fake timeout")
        metric = Metric(kwargs["search_params"]["metric_type"])
        reference = exact_range_search(
            self.base_vectors,
            self.base_ids,
            vector,
            metric,
            radius=float(parameters["radius"]),
            range_filter=float(parameters["range_filter"]),
            limit=int(kwargs["limit"]),
        )
        return [[{"id": hit.id, "distance": hit.score} for hit in reference.hits]]

    def get_load_state(self, **kwargs):
        self.other_calls.append(("get_load_state", kwargs))
        return {"state": "Loaded" if self.loaded else "NotLoaded"}

    def describe_index(self, **kwargs):
        self.other_calls.append(("describe_index", kwargs))
        if kwargs["collection_name"] in self.identity_capture_failures:
            raise RuntimeError("injected identity capture failure")
        track = (
            IndexTrack.FLAT
            if kwargs["collection_name"] == FLAT_NAME
            else IndexTrack.HNSW
        )
        build_id = (
            self.flat_build_id if track is IndexTrack.FLAT else self.hnsw_build_id
        )
        return index_description(track, build_id=build_id)


class FakeBoundEstimator:
    def __init__(self) -> None:
        self.calls: list[CanaryPairedMeasurements] = []

    def estimate(self, measurements: CanaryPairedMeasurements) -> CanaryBounds:
        self.calls.append(measurements)
        return CanaryBounds(
            recall_lower_bound_95=0.97,
            latency_upper_bound_95_ms=1.25,
            confidence_level=0.95,
            provenance="fake-bound-estimator-v1",
        )


class FakeStackHealthProbe:
    def __init__(self) -> None:
        self.calls = 0
        self.result = StackHealth(
            etcd_healthy=True,
            minio_healthy=True,
            detail="fake stack healthy",
        )

    def check(self) -> StackHealth:
        self.calls += 1
        return self.result


class FakeShadowTraceSink:
    def __init__(self) -> None:
        self.records: list[ShadowAuditTrace] = []

    def append(self, trace: ShadowAuditTrace) -> None:
        self.records.append(trace)


class FailingShadowTraceSink:
    def __init__(self) -> None:
        self.calls = 0

    def append(self, trace: ShadowAuditTrace) -> None:
        self.calls += 1
        raise RuntimeError("injected trace sink failure")


class StepClock:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self) -> int:
        self.value += 1_000_000
        return self.value


def fixture_components(
    *,
    include_canary_batch: bool = True,
    shadow_trace_sink=None,
):
    base_ids = np.arange(4, dtype=np.int64)
    base_vectors = np.asarray(
        [[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [3.0, 0.0]],
        dtype="<f4",
    )
    query_vectors = {
        query_id: np.asarray(
            [0.01 + query_id / 1_000.0, 0.25],
            dtype="<f4",
        )
        for query_id in range(500)
    }
    flat_identity = CollectionIdentity(
        FLAT_NAME,
        Metric.L2.value,
        IndexTrack.FLAT.value,
        index_description(IndexTrack.FLAT),
    )
    hnsw_identity = CollectionIdentity(
        HNSW_NAME,
        Metric.L2.value,
        IndexTrack.HNSW.value,
        index_description(IndexTrack.HNSW),
    )
    workload_kwargs = {}
    if include_canary_batch:
        workload_kwargs["canary_query_ids"] = tuple(range(500))
    workload = ActuationWorkload(
        query_vectors=query_vectors,
        base_ids=base_ids,
        base_vectors=base_vectors,
        threshold_radii={(Metric.L2, THRESHOLD_STRATUM): 100.0},
        collection_names={
            (Metric.L2, IndexTrack.FLAT): FLAT_NAME,
            (Metric.L2, IndexTrack.HNSW): HNSW_NAME,
        },
        identity_bindings={
            (Metric.L2, IndexTrack.FLAT): CollectionIdentityBinding(
                "flat-identity-v1", flat_identity
            ),
            (Metric.L2, IndexTrack.HNSW): CollectionIdentityBinding(
                INDEX_ID, hnsw_identity
            ),
        },
        configuration_identity=CONFIGURATION_ID,
        data_identity=DATA_ID,
        **workload_kwargs,
    )
    client = FakePyMilvusClient(
        base_ids=base_ids,
        base_vectors=base_vectors,
        query_vectors=query_vectors,
    )
    estimator = FakeBoundEstimator()
    health = FakeStackHealthProbe()
    adapter = MilvusActuationClient(
        client,
        workload=workload,
        routing_seed=ROUTING_SEED,
        bound_estimator=estimator,
        stack_health_probe=health,
        initial_ef=400,
        clock_ns=StepClock(),
        shadow_trace_sink=shadow_trace_sink,
    )
    return workload, client, estimator, health, adapter


def context() -> ActuationContext:
    qualification = QualificationResult(
        qualified=True,
        ef=400,
        reasons=(),
        metric=Metric.L2,
        threshold_stratum=THRESHOLD_STRATUM,
        configuration_identity=CONFIGURATION_ID,
        index_identity=INDEX_ID,
        data_identity=DATA_ID,
        qualifying_window_ids=("window-10", "window-11"),
    )
    return ActuationContext(
        metric=Metric.L2,
        threshold_stratum=THRESHOLD_STRATUM,
        collection_name=HNSW_NAME,
        configuration_identity=CONFIGURATION_ID,
        index_identity=INDEX_ID,
        flat_index_identity="flat-identity-v1",
        data_identity=DATA_ID,
        audited_query_ids=tuple(range(50)),
        last_known_good=qualification,
        occurred_at_utc="2026-08-04T10:00:00Z",
    )


class MilvusActuationAdapterTests(unittest.TestCase):
    def test_shadow_does_not_require_an_unrelated_canary_batch(self) -> None:
        workload, client, _, _, adapter = fixture_components(
            include_canary_batch=False
        )

        self.assertEqual(workload.canary_query_ids, ())
        result = adapter.shadow_candidate(
            context=context(),
            candidate_ef=800,
            last_known_good_ef=400,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.audited_query_count, 50)
        self.assertEqual(len(client.search_calls), 150)

    def test_legacy_canary_serving_api_is_removed_with_zero_side_effects(self) -> None:
        _, client, estimator, health, adapter = fixture_components()
        default_before = adapter.default_ef
        candidate_before = adapter.candidate_ef

        with self.assertRaises(AttributeError):
            getattr(adapter, "start_canary")

        self.assertFalse(hasattr(adapter, "start_canary"))
        self.assertEqual(adapter.default_ef, default_before)
        self.assertEqual(adapter.candidate_ef, candidate_before)
        self.assertEqual(client.search_calls, [])
        self.assertEqual(client.other_calls, [])
        self.assertEqual(estimator.calls, [])
        self.assertEqual(health.calls, 0)

        source = MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        function_names = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertNotIn("start_canary", function_names)
        self.assertNotIn("select_canary_routes", function_names)

    def test_shadow_runs_flat_candidate_and_lkg_for_all_50_audit_queries(self) -> None:
        _, client, estimator, health, adapter = fixture_components()

        result = adapter.shadow_candidate(
            context=context(),
            candidate_ef=800,
            last_known_good_ef=400,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.audited_query_count, 50)
        self.assertEqual(result.failed_query_count, 0)
        self.assertEqual(result.timeout_query_count, 0)
        self.assertEqual(result.threshold_violation_count, 0)
        self.assertTrue(result.candidate_flat_oracle_agreement)
        self.assertTrue(result.last_known_good_flat_oracle_agreement)
        self.assertEqual(len(client.search_calls), 150)
        self.assertEqual(
            sum(call["collection"] == FLAT_NAME for call in client.search_calls),
            50,
        )
        self.assertEqual(
            sum(call["ef"] == 800 for call in client.search_calls),
            50,
        )
        self.assertEqual(
            sum(call["ef"] == 400 for call in client.search_calls),
            50,
        )
        self.assertEqual(estimator.calls, [])
        self.assertEqual(health.calls, 0)

    def test_shadow_populates_real_failure_and_timeout_counts(self) -> None:
        _, client, _, _, adapter = fixture_components()
        client.failures.add(("hnsw", 800, 0))

        result = adapter.shadow_candidate(
            context=context(),
            candidate_ef=800,
            last_known_good_ef=400,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.failed_query_count, 1)
        self.assertEqual(result.timeout_query_count, 1)

    def test_shadow_trace_captures_200_calls_recall_cardinality_and_identities(
        self,
    ) -> None:
        sink = FakeShadowTraceSink()
        workload, client, estimator, health, adapter = fixture_components(
            shadow_trace_sink=sink
        )

        result = adapter.shadow_candidate(
            context=context(),
            candidate_ef=800,
            last_known_good_ef=400,
        )

        self.assertTrue(result.success)
        self.assertEqual(len(client.search_calls), 200)
        self.assertEqual(
            sum(call["collection"] == FLAT_NAME for call in client.search_calls),
            50,
        )
        self.assertEqual(sum(call["ef"] == 100 for call in client.search_calls), 50)
        self.assertEqual(sum(call["ef"] == 400 for call in client.search_calls), 50)
        self.assertEqual(sum(call["ef"] == 800 for call in client.search_calls), 50)
        self.assertEqual(estimator.calls, [])
        self.assertEqual(health.calls, 0)
        self.assertEqual(len(sink.records), 1)

        trace = sink.records[0]
        self.assertTrue(trace.complete)
        self.assertEqual(trace.reason_codes, ())
        self.assertEqual(trace.metric, Metric.L2)
        self.assertEqual(trace.threshold_stratum, THRESHOLD_STRATUM)
        self.assertEqual(trace.candidate_ef, 800)
        self.assertEqual(trace.last_known_good_ef, 400)
        self.assertEqual(trace.sentinel_ef, 100)
        self.assertEqual(trace.configuration_identity, CONFIGURATION_ID)
        self.assertEqual(trace.data_identity, DATA_ID)
        self.assertEqual(len(trace.queries), 50)
        self.assertTrue(trace.flat_identity.pre_binding_match)
        self.assertTrue(trace.flat_identity.post_binding_match)
        self.assertTrue(trace.hnsw_identity.pre_binding_match)
        self.assertTrue(trace.hnsw_identity.post_binding_match)
        self.assertEqual(
            trace.flat_identity.pre_snapshot,
            trace.flat_identity.post_snapshot,
        )
        self.assertEqual(
            trace.hnsw_identity.pre_snapshot,
            trace.hnsw_identity.post_snapshot,
        )
        with self.assertRaises(TypeError):
            trace.hnsw_identity.pre_snapshot.description[  # type: ignore[index,union-attr]
                "build_id"
            ] = 999

        first = trace.queries[0]
        expected_oracle = exact_range_search(
            workload.base_vectors,
            workload.base_ids,
            workload.query_vectors[0],
            Metric.L2,
            radius=100.0,
            range_filter=0.0,
            limit=100,
        )
        self.assertEqual(
            first.query_vector,
            tuple(float(value) for value in workload.query_vectors[0]),
        )
        self.assertEqual(first.threshold_radius, 100.0)
        self.assertEqual(first.range_filter, 0.0)
        self.assertEqual(first.limit, 100)
        self.assertEqual(first.oracle_result, expected_oracle)
        self.assertEqual(first.exact_cardinality, 4)
        self.assertEqual(
            tuple(hit.id for hit in first.flat_hits or ()),
            expected_oracle.ids,
        )
        self.assertEqual(
            tuple(hit.id for hit in first.sentinel_hits or ()),
            expected_oracle.ids,
        )
        self.assertEqual(first.sentinel_recall, 1.0)
        self.assertEqual(
            tuple(stage.stage for stage in first.stages),
            (
                "ORACLE",
                "FLAT",
                "CANDIDATE_HNSW",
                "LAST_KNOWN_GOOD_HNSW",
                "SENTINEL_HNSW",
            ),
        )
        self.assertTrue(all(stage.success for stage in first.stages))
        with self.assertRaises(FrozenInstanceError):
            trace.complete = False  # type: ignore[misc]
        self.assertIsInstance(first.query_vector, tuple)

    def test_sentinel_failure_only_marks_trace_incomplete(self) -> None:
        sink = FakeShadowTraceSink()
        _, client, _, _, adapter = fixture_components(shadow_trace_sink=sink)
        client.failures.add(("hnsw", 100, 0))

        result = adapter.shadow_candidate(
            context=context(),
            candidate_ef=800,
            last_known_good_ef=400,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.failed_query_count, 0)
        self.assertEqual(result.timeout_query_count, 0)
        trace = sink.records[0]
        self.assertFalse(trace.complete)
        failed_query = trace.queries[0]
        self.assertIsNone(failed_query.sentinel_hits)
        self.assertIsNone(failed_query.sentinel_recall)
        sentinel_stage = next(
            stage for stage in failed_query.stages if stage.stage == "SENTINEL_HNSW"
        )
        self.assertFalse(sentinel_stage.success)
        self.assertTrue(sentinel_stage.timed_out)
        self.assertEqual(sentinel_stage.error_type, "TimeoutError")
        self.assertIn("STAGE_FAILED:0:SENTINEL_HNSW", trace.reason_codes)
        self.assertIn("TIMEOUT:0:SENTINEL_HNSW", trace.reason_codes)

    def test_live_identity_mismatch_only_marks_trace_incomplete(self) -> None:
        sink = FakeShadowTraceSink()
        _, client, _, _, adapter = fixture_components(shadow_trace_sink=sink)
        client.hnsw_build_id = 999

        result = adapter.shadow_candidate(
            context=context(),
            candidate_ef=800,
            last_known_good_ef=400,
        )

        self.assertTrue(result.success)
        trace = sink.records[0]
        self.assertFalse(trace.complete)
        self.assertFalse(trace.hnsw_identity.pre_binding_match)
        self.assertFalse(trace.hnsw_identity.post_binding_match)
        self.assertEqual(
            trace.hnsw_identity.pre_snapshot.description[  # type: ignore[index,union-attr]
                "build_id"
            ],
            999,
        )
        self.assertIn("STAGE_FAILED:PRE_HNSW_IDENTITY", trace.reason_codes)
        self.assertIn("STAGE_FAILED:POST_HNSW_IDENTITY", trace.reason_codes)

    def test_identity_capture_failure_only_marks_trace_incomplete(self) -> None:
        sink = FakeShadowTraceSink()
        _, client, _, _, adapter = fixture_components(shadow_trace_sink=sink)
        client.identity_capture_failures.add(HNSW_NAME)

        result = adapter.shadow_candidate(
            context=context(),
            candidate_ef=800,
            last_known_good_ef=400,
        )

        self.assertTrue(result.success)
        trace = sink.records[0]
        self.assertFalse(trace.complete)
        self.assertIsNone(trace.hnsw_identity.pre_snapshot)
        self.assertIsNone(trace.hnsw_identity.post_snapshot)
        self.assertEqual(trace.hnsw_identity.pre_capture.error_type, "RuntimeError")
        self.assertEqual(trace.hnsw_identity.post_capture.error_type, "RuntimeError")
        self.assertIn("STAGE_FAILED:PRE_HNSW_IDENTITY", trace.reason_codes)
        self.assertIn("STAGE_FAILED:POST_HNSW_IDENTITY", trace.reason_codes)

    def test_trace_sink_failure_raises_after_read_only_collection(self) -> None:
        sink = FailingShadowTraceSink()
        _, client, _, _, adapter = fixture_components(shadow_trace_sink=sink)

        with self.assertRaisesRegex(RuntimeError, "injected trace sink failure"):
            adapter.shadow_candidate(
                context=context(),
                candidate_ef=800,
                last_known_good_ef=400,
            )

        self.assertEqual(sink.calls, 1)
        self.assertEqual(len(client.search_calls), 200)

    def test_stop_and_restore_are_adapter_state_only_with_zero_client_calls(
        self,
    ) -> None:
        _, client, _, _, adapter = fixture_components()
        adapter._candidate_ef = 800  # simulate containment of pre-migration state

        adapter.stop_candidate()
        adapter.restore_last_known_good(400)

        self.assertIsNone(adapter.candidate_ef)
        self.assertEqual(adapter.default_ef, 400)
        self.assertEqual(client.search_calls, [])
        self.assertEqual(client.other_calls, [])

    def test_verify_restoration_reruns_audit_and_checks_health_identity(self) -> None:
        _, client, _, health, adapter = fixture_components()
        adapter.stop_candidate()
        adapter.restore_last_known_good(400)

        verification = adapter.verify_restoration(
            context=context(),
            expected_ef=400,
        )

        self.assertTrue(verification.success)
        self.assertEqual(verification.restored_ef, 400)
        self.assertTrue(verification.health_passed)
        self.assertTrue(verification.audit_passed)
        self.assertEqual(verification.index_identity, INDEX_ID)
        self.assertEqual(len(client.search_calls), 100)
        self.assertEqual(
            sum(call["collection"] == FLAT_NAME for call in client.search_calls),
            50,
        )
        self.assertEqual(
            sum(call["ef"] == 400 for call in client.search_calls),
            50,
        )
        self.assertEqual(health.calls, 1)

    def test_identity_binding_mismatch_fails_restoration_verification(self) -> None:
        _, client, _, _, adapter = fixture_components()
        client.hnsw_build_id = 999
        adapter.stop_candidate()
        adapter.restore_last_known_good(400)

        verification = adapter.verify_restoration(
            context=context(),
            expected_ef=400,
        )

        self.assertFalse(verification.success)
        self.assertTrue(verification.health_passed)
        self.assertTrue(verification.audit_passed)
        self.assertIn("collection identities changed", verification.detail)

    def test_declared_flat_identity_mismatch_fails_restoration_verification(
        self,
    ) -> None:
        _, client, _, _, adapter = fixture_components()
        adapter.stop_candidate()
        adapter.restore_last_known_good(400)

        verification = adapter.verify_restoration(
            context=replace(context(), flat_index_identity="wrong-flat-identity"),
            expected_ef=400,
        )

        self.assertFalse(verification.success)
        self.assertTrue(verification.health_passed)
        self.assertTrue(verification.audit_passed)
        self.assertIn("configuration identity mismatch", verification.detail)

    def test_pymilvus_import_is_lazy_and_execute_live_is_never_referenced(self) -> None:
        source = MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }
        pymilvus_imports = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == "pymilvus"
        ]
        self.assertEqual(len(pymilvus_imports), 1)
        node = pymilvus_imports[0]
        ancestors = []
        while node in parents:
            node = parents[node]
            ancestors.append(node)
        self.assertTrue(
            any(
                isinstance(ancestor, (ast.FunctionDef, ast.AsyncFunctionDef))
                and ancestor.name == "from_uri"
                for ancestor in ancestors
            )
        )
        self.assertNotIn("execute_live", source)


if __name__ == "__main__":
    unittest.main()
