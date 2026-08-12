"""Offline (fake-client) coverage for the real EXP-011 Milvus adapters.

Every test here uses a fake client. None of these tests contact Milvus.
"""

from __future__ import annotations

import ast
from pathlib import Path
import unittest

import numpy as np

from vdbench.config import HNSW_EF_CONSTRUCTION, HNSW_M, IndexTrack, Metric, SearchConfiguration
from vdbench.response_profile_evidence import (
    ResponseProfileRoleKind,
    build_artifact_source_namespace,
    build_canonical_query_identity,
    build_query_vector_identity,
    build_response_profile_query_payload,
    build_response_profile_role,
    build_response_profile_role_member,
)
from vdbench.response_profile_milvus_adapter import (
    ResponseProfileMilvusQueryExecutor,
    ResponseProfileMilvusRuntimeProbe,
)
from vdbench.response_profile_producer import ResponseProfileExecutionQuery

MODULE_PATH = Path(__file__).parents[1] / "src" / "vdbench" / "response_profile_milvus_adapter.py"

DIMENSIONS = 4
COLLECTION = "response-profile-hnsw-v1"


def _flat_configuration() -> SearchConfiguration:
    return SearchConfiguration(
        metric=Metric.L2, threshold_label="target-075", radius=0.75,
        index_track=IndexTrack.FLAT, ef=None,
    )


def _hnsw_configuration(ef: int = 400) -> SearchConfiguration:
    return SearchConfiguration(
        metric=Metric.L2, threshold_label="target-075", radius=0.75,
        index_track=IndexTrack.HNSW, ef=ef,
    )


def _query(index: int = 0, *, configuration: SearchConfiguration | None = None) -> ResponseProfileExecutionQuery:
    namespace = build_artifact_source_namespace(
        dataset_id="DATASET-EXP011-ADAPTER", dataset_version="v1",
        generation_manifest_sha256="a" * 64,
    )
    vector = build_query_vector_identity(np.asarray([1.0, 2.0, 3.0, 4.0], dtype="<f4"))
    member = build_response_profile_role_member(
        source_namespace=namespace,
        query_identity=build_canonical_query_identity(index),
        vector_identity=vector,
        query_payload_identity=build_response_profile_query_payload(
            vector_identity=vector, search_configuration=_flat_configuration()
        ),
    )
    return ResponseProfileExecutionQuery(
        member=member,
        vector_bytes=vector.canonical_vector_bytes,
        dimensions=DIMENSIONS,
        search_configuration=configuration or _hnsw_configuration(),
        measured=True,
    )


class _FakeMilvusClient:
    """Records every call it receives; never performs a mutation."""

    def __init__(
        self,
        *,
        search_response: object = None,
        load_state: object = "Loaded",
        describe_index: object | None = None,
        raise_on: str | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self._search_response = search_response if search_response is not None else [
            [{"id": 1, "distance": 0.5}, {"id": 2, "distance": 0.75}]
        ]
        self._load_state = load_state
        self._describe_index = describe_index or {
            "index_type": "HNSW", "metric_type": "L2", "state": "Finished",
            "params": {"M": HNSW_M, "efConstruction": HNSW_EF_CONSTRUCTION},
        }
        self._raise_on = raise_on

    def search(self, **kwargs: object) -> object:
        self.calls.append(("search", kwargs))
        if self._raise_on == "search":
            raise RuntimeError("simulated Milvus search failure")
        return self._search_response

    def get_load_state(self, **kwargs: object) -> object:
        self.calls.append(("get_load_state", kwargs))
        if self._raise_on == "get_load_state":
            raise RuntimeError("simulated Milvus load-state failure")
        return {"state": self._load_state}

    def describe_index(self, **kwargs: object) -> object:
        self.calls.append(("describe_index", kwargs))
        if self._raise_on == "describe_index":
            raise RuntimeError("simulated Milvus describe_index failure")
        return self._describe_index


class _FakeStackHealthProbe:
    def __init__(self, *, healthy: bool = True, raises: bool = False) -> None:
        self.calls = 0
        self._healthy = healthy
        self._raises = raises

    def check(self) -> object:
        self.calls += 1
        if self._raises:
            raise RuntimeError("simulated stack health probe failure")

        class _Health:
            etcd_healthy = self._healthy
            minio_healthy = self._healthy

        return _Health()


class ResponseProfileMilvusQueryExecutorTests(unittest.TestCase):
    def _executor(self, client: _FakeMilvusClient) -> ResponseProfileMilvusQueryExecutor:
        return ResponseProfileMilvusQueryExecutor(
            client, collection_name=COLLECTION, dimensions=DIMENSIONS
        )

    def test_search_request_translation_is_exact(self) -> None:
        client = _FakeMilvusClient()
        executor = self._executor(client)
        query = _query(configuration=_hnsw_configuration(ef=800))
        executor.execute(query)

        self.assertEqual(len(client.calls), 1)
        name, kwargs = client.calls[0]
        self.assertEqual(name, "search")
        self.assertEqual(kwargs["collection_name"], COLLECTION)
        self.assertEqual(kwargs["search_params"]["metric_type"], "L2")
        self.assertEqual(kwargs["search_params"]["params"]["radius"], 0.75)
        self.assertEqual(kwargs["search_params"]["params"]["ef"], 800)
        self.assertEqual(kwargs["data"], [[1.0, 2.0, 3.0, 4.0]])

    def test_result_ids_and_distances_are_canonicalized(self) -> None:
        client = _FakeMilvusClient(
            search_response=[[{"id": 7, "distance": 1.25}, {"id": 3, "distance": 0.0}]]
        )
        result = self._executor(client).execute(_query())
        self.assertEqual(result.candidate_ids, (7, 3))
        self.assertEqual(result.candidate_distances, (1.25, 0.0))
        self.assertTrue(all(type(item) is float for item in result.candidate_distances))
        self.assertTrue(all(type(item) is int for item in result.candidate_ids))

    def test_threshold_and_search_configuration_preserved_exactly(self) -> None:
        client = _FakeMilvusClient()
        configuration = _hnsw_configuration(ef=200)
        self._executor(client).execute(_query(configuration=configuration))
        _, kwargs = client.calls[0]
        self.assertEqual(kwargs["search_params"]["params"]["radius"], configuration.radius)
        self.assertEqual(kwargs["search_params"]["params"]["range_filter"], configuration.range_filter)
        self.assertEqual(kwargs["limit"], configuration.limit)
        self.assertEqual(kwargs["consistency_level"], configuration.consistency_level)

    def test_malformed_result_fails_closed(self) -> None:
        client = _FakeMilvusClient(search_response=[[{"id": 1, "distance": 1.0}], [{"id": 2, "distance": 1.0}]])
        with self.assertRaises(Exception):
            self._executor(client).execute(_query())

    def test_duplicate_ids_fail_closed(self) -> None:
        client = _FakeMilvusClient(search_response=[[{"id": 1, "distance": 1.0}, {"id": 1, "distance": 2.0}]])
        with self.assertRaises(Exception):
            self._executor(client).execute(_query())

    def test_client_exception_propagates_as_a_typed_failure_not_a_fake_success(self) -> None:
        client = _FakeMilvusClient(raise_on="search")
        with self.assertRaises(Exception):
            self._executor(client).execute(_query())

    def test_dimension_mismatch_refuses_before_any_client_call(self) -> None:
        client = _FakeMilvusClient()
        executor = ResponseProfileMilvusQueryExecutor(
            client, collection_name=COLLECTION, dimensions=8
        )
        with self.assertRaises(ValueError):
            executor.execute(_query())
        self.assertEqual(client.calls, [])

    def test_only_search_is_ever_called_by_the_executor(self) -> None:
        client = _FakeMilvusClient()
        self._executor(client).execute(_query())
        self.assertEqual({name for name, _ in client.calls}, {"search"})


class ResponseProfileMilvusRuntimeProbeTests(unittest.TestCase):
    def _probe(
        self, client: _FakeMilvusClient, *, health: _FakeStackHealthProbe | None = None
    ) -> ResponseProfileMilvusRuntimeProbe:
        return ResponseProfileMilvusRuntimeProbe(
            client,
            collection_name=COLLECTION,
            dimensions=DIMENSIONS,
            metric=Metric.L2,
            stack_health_probe=health or _FakeStackHealthProbe(),
        )

    def test_all_healthy_reports_fully_ready(self) -> None:
        client = _FakeMilvusClient()
        readiness = self._probe(client).collect()
        self.assertTrue(readiness.collection_loaded)
        self.assertTrue(readiness.milvus_healthy)
        self.assertTrue(readiness.etcd_healthy)
        self.assertTrue(readiness.minio_healthy)

    def test_not_loaded_collection_fails_closed(self) -> None:
        client = _FakeMilvusClient(load_state="NotLoad")
        readiness = self._probe(client).collect()
        self.assertFalse(readiness.collection_loaded)

    def test_get_load_state_exception_fails_closed(self) -> None:
        client = _FakeMilvusClient(raise_on="get_load_state")
        readiness = self._probe(client).collect()
        self.assertFalse(readiness.collection_loaded)
        self.assertFalse(readiness.milvus_healthy)

    def test_describe_index_exception_fails_closed_even_when_loaded(self) -> None:
        client = _FakeMilvusClient(raise_on="describe_index")
        readiness = self._probe(client).collect()
        self.assertFalse(readiness.collection_loaded)
        self.assertTrue(readiness.milvus_healthy)

    def test_index_identity_mismatch_fails_closed(self) -> None:
        client = _FakeMilvusClient(
            describe_index={
                "index_type": "HNSW", "metric_type": "COSINE", "state": "Finished",
                "params": {"M": HNSW_M, "efConstruction": HNSW_EF_CONSTRUCTION},
            }
        )
        readiness = self._probe(client).collect()
        self.assertFalse(readiness.collection_loaded)

    def test_stack_health_probe_exception_fails_closed(self) -> None:
        client = _FakeMilvusClient()
        readiness = self._probe(client, health=_FakeStackHealthProbe(raises=True)).collect()
        self.assertFalse(readiness.etcd_healthy)
        self.assertFalse(readiness.minio_healthy)

    def test_unhealthy_stack_fails_closed(self) -> None:
        client = _FakeMilvusClient()
        readiness = self._probe(client, health=_FakeStackHealthProbe(healthy=False)).collect()
        self.assertFalse(readiness.etcd_healthy)
        self.assertFalse(readiness.minio_healthy)

    def test_collect_never_calls_search(self) -> None:
        client = _FakeMilvusClient()
        self._probe(client).collect()
        self.assertNotIn("search", {name for name, _ in client.calls})


class ResponseProfileMilvusAdapterAdversarialTests(unittest.TestCase):
    def test_no_mutation_capable_client_method_is_ever_referenced(self) -> None:
        source = MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(MODULE_PATH))
        attribute_names = {
            node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
        }
        forbidden = {
            "insert", "upsert", "delete", "create_collection", "drop_collection",
            "create_index", "drop_index", "load_collection", "release_collection",
            "alter_alias", "create_alias", "drop_alias", "flush",
        }
        self.assertFalse(attribute_names & forbidden, attribute_names & forbidden)

    def test_module_has_no_candidate_policy_grant_or_route_dependency(self) -> None:
        source = MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(MODULE_PATH))
        imported = {
            node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        } | {
            alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
        }
        forbidden_suffixes = (
            "policy", "canary_admission", "canary_approval", "canary_activation",
            "canary_route_authority", "canary_route_state", "canary_live_runner",
            "canary_grant_store",
        )
        offending = {
            item for item in imported
            if any(item == suffix or item.endswith(f".{suffix}") for suffix in forbidden_suffixes)
        }
        self.assertFalse(offending, offending)


if __name__ == "__main__":
    unittest.main()
