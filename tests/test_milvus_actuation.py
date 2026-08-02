from __future__ import annotations

import ast
import unittest
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
    StackHealth,
    select_canary_routes,
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


class StepClock:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self) -> int:
        self.value += 1_000_000
        return self.value


def fixture_components():
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
    workload = ActuationWorkload(
        query_vectors=query_vectors,
        canary_query_ids=tuple(range(500)),
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
        data_identity=DATA_ID,
        audited_query_ids=tuple(range(50)),
        last_known_good=qualification,
        occurred_at_utc="2026-08-04T10:00:00Z",
    )


class MilvusActuationAdapterTests(unittest.TestCase):
    def test_500_query_routing_selects_exactly_50_deterministically(self) -> None:
        first = select_canary_routes(
            tuple(range(500)),
            routing_seed=ROUTING_SEED,
            metric=Metric.L2,
            threshold_stratum=THRESHOLD_STRATUM,
            traffic_fraction=0.10,
        )
        second = select_canary_routes(
            tuple(range(500)),
            routing_seed=ROUTING_SEED,
            metric=Metric.L2,
            threshold_stratum=THRESHOLD_STRATUM,
            traffic_fraction=0.10,
        )

        self.assertEqual(first, second)
        self.assertEqual(
            first.candidate_query_ids,
            (
                213,
                313,
                322,
                392,
                310,
                433,
                215,
                14,
                427,
                3,
                85,
                73,
                149,
                190,
                288,
                87,
                370,
                472,
                167,
                35,
                220,
                131,
                166,
                158,
                96,
                372,
                431,
                211,
                117,
                182,
                145,
                325,
                50,
                238,
                399,
                383,
                297,
                56,
                108,
                307,
                95,
                321,
                112,
                48,
                330,
                227,
                94,
                200,
                4,
                187,
            ),
        )
        self.assertEqual(len(first.candidate_query_ids), 50)
        self.assertEqual(len(first.last_known_good_query_ids), 450)
        self.assertEqual(
            set(first.candidate_query_ids) | set(first.last_known_good_query_ids),
            set(range(500)),
        )
        self.assertFalse(
            set(first.candidate_query_ids) & set(first.last_known_good_query_ids)
        )
        self.assertEqual(len(set(first.candidate_digest_hex)), 50)

    def test_routing_rejects_non_ten_percent_or_non_500_batch(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly 0.10"):
            select_canary_routes(
                tuple(range(500)),
                routing_seed=ROUTING_SEED,
                metric=Metric.L2,
                threshold_stratum=THRESHOLD_STRATUM,
                traffic_fraction=0.05,
            )
        with self.assertRaisesRegex(ValueError, "exactly 500"):
            select_canary_routes(
                tuple(range(499)),
                routing_seed=ROUTING_SEED,
                metric=Metric.L2,
                threshold_stratum=THRESHOLD_STRATUM,
                traffic_fraction=0.10,
            )

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

    def test_canary_routes_50_and_pairs_same_queries_at_lkg_ef(self) -> None:
        workload, client, estimator, health, adapter = fixture_components()
        expected_routes = select_canary_routes(
            workload.canary_query_ids,
            routing_seed=ROUTING_SEED,
            metric=Metric.L2,
            threshold_stratum=THRESHOLD_STRATUM,
            traffic_fraction=0.10,
        )

        observation = adapter.start_canary(
            context=context(),
            candidate_ef=800,
            last_known_good_ef=400,
            traffic_fraction=0.10,
        )

        candidate_calls = [
            call
            for call in client.search_calls
            if call["collection"] == HNSW_NAME and call["ef"] == 800
        ]
        last_known_good_calls = [
            call
            for call in client.search_calls
            if call["collection"] == HNSW_NAME and call["ef"] == 400
        ]
        flat_calls = [
            call for call in client.search_calls if call["collection"] == FLAT_NAME
        ]
        self.assertEqual(len(candidate_calls), 50)
        self.assertEqual(len(last_known_good_calls), 500)
        self.assertEqual(len(flat_calls), 50)
        self.assertEqual(len(client.search_calls), 600)
        self.assertEqual(
            {call["query_id"] for call in candidate_calls},
            set(expected_routes.candidate_query_ids),
        )
        for query_id in expected_routes.candidate_query_ids:
            paired_efs = {
                call["ef"]
                for call in client.search_calls
                if call["collection"] == HNSW_NAME and call["query_id"] == query_id
            }
            self.assertEqual(paired_efs, {400, 800})

        self.assertEqual(observation.completed_query_count, 50)
        self.assertEqual(observation.candidate_mean_recall, 1.0)
        self.assertEqual(observation.last_known_good_mean_recall, 1.0)
        self.assertEqual(observation.candidate_p95_latency_ms, 1.0)
        self.assertEqual(observation.last_known_good_p95_latency_ms, 1.0)
        self.assertEqual(observation.candidate_recall_lower_bound_95, 0.97)
        self.assertEqual(observation.candidate_latency_upper_bound_95_ms, 1.25)
        self.assertTrue(observation.flat_oracle_agreement)
        self.assertEqual(observation.failed_query_count, 0)
        self.assertEqual(len(estimator.calls), 1)
        measurements = estimator.calls[0]
        self.assertEqual(
            set(measurements.query_ids), set(expected_routes.candidate_query_ids)
        )
        self.assertEqual(len(measurements.candidate_recalls), 50)
        self.assertEqual(len(measurements.last_known_good_recalls), 50)
        self.assertEqual(measurements.candidate_recalls, (1.0,) * 50)
        self.assertEqual(measurements.last_known_good_recalls, (1.0,) * 50)
        self.assertEqual(health.calls, 1)
        self.assertEqual(adapter.candidate_ef, 800)
        self.assertEqual(adapter.default_ef, 400)

    def test_stop_and_restore_are_adapter_state_only_with_zero_client_calls(
        self,
    ) -> None:
        _, client, _, _, adapter = fixture_components()
        adapter.start_canary(
            context=context(),
            candidate_ef=800,
            last_known_good_ef=400,
            traffic_fraction=0.10,
        )
        client.search_calls.clear()
        client.other_calls.clear()

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
