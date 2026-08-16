"""TDD coverage for the production Milvus adapter's exact request/response contract.

Uses only an injected mock PyMilvus-compatible client -- never a live
Milvus connection. Proves the adapter issues exactly the request
ARCHITECTURE.md's EXP-001 contract requires and decodes IDs/distances from
that one response only.
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

import numpy as np

from vdbench.config import CONSISTENCY_LEVEL, RESULT_LIMIT, ContractViolation, Metric
from vdbench.lkg_milvus_adapter import LkgMilvusAdapter, LkgSearchCall

REPOSITORY = Path(__file__).parents[1]
ADAPTER_MODULE_PATH = REPOSITORY / "src" / "vdbench" / "lkg_milvus_adapter.py"
HNSW_NAME = "lkg_l2_hnsw"


class RecordingMilvusClient:
    """A mock PyMilvus-compatible client that records every search() call
    and returns a caller-controlled response shape."""

    def __init__(self) -> None:
        self.search_calls: list[dict[str, object]] = []
        self.response: list[list[dict[str, object]]] = [[{"id": 1, "distance": 0.1}]]
        self.raise_exception: Exception | None = None

    def search(self, **kwargs):
        self.search_calls.append(kwargs)
        if self.raise_exception is not None:
            raise self.raise_exception
        return self.response


class FakeClock:
    def __init__(self, *values: int) -> None:
        self._values = iter(values)

    def __call__(self) -> int:
        return next(self._values)


class LkgMilvusAdapterRequestContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = RecordingMilvusClient()
        self.adapter = LkgMilvusAdapter(
            self.client, dimensions=4, hnsw_collection_name=HNSW_NAME, clock_ns=FakeClock(1_000, 2_000)
        )
        self.query_vector = np.array([0.1, 0.2, 0.3, 0.4], dtype="<f4")

    def _search(self, **overrides):
        kwargs = {
            "query_id": 1,
            "query_vector": self.query_vector,
            "metric": Metric.L2,
            "threshold_stratum": "target-075",
            "ef": 400,
            "radius": 5.0,
        }
        kwargs.update(overrides)
        return self.adapter.search(**kwargs)

    def test_exactly_one_search_call_is_made(self) -> None:
        self._search()
        self.assertEqual(len(self.client.search_calls), 1)

    def test_collection_name_is_correct(self) -> None:
        self._search()
        self.assertEqual(self.client.search_calls[0]["collection_name"], HNSW_NAME)

    def test_exactly_one_query_vector_is_sent(self) -> None:
        self._search()
        data = self.client.search_calls[0]["data"]
        self.assertEqual(len(data), 1)
        np.testing.assert_allclose(data[0], self.query_vector.tolist())

    def test_metric_type_is_correct(self) -> None:
        self._search(metric=Metric.COSINE, radius=0.5)
        params = self.client.search_calls[0]["search_params"]
        self.assertEqual(params["metric_type"], "COSINE")

    def test_radius_is_correct(self) -> None:
        self._search(radius=7.25)
        params = self.client.search_calls[0]["search_params"]["params"]
        self.assertEqual(params["radius"], 7.25)

    def test_range_filter_is_derived_correctly_for_l2(self) -> None:
        self._search(metric=Metric.L2, radius=7.25)
        params = self.client.search_calls[0]["search_params"]["params"]
        self.assertEqual(params["range_filter"], 0.0)

    def test_range_filter_is_derived_correctly_for_cosine(self) -> None:
        self._search(metric=Metric.COSINE, radius=0.5)
        params = self.client.search_calls[0]["search_params"]["params"]
        self.assertEqual(params["range_filter"], 1.0)

    def test_ef_is_correct(self) -> None:
        self._search(ef=800)
        params = self.client.search_calls[0]["search_params"]["params"]
        self.assertEqual(params["ef"], 800)

    def test_limit_is_exactly_100(self) -> None:
        self._search()
        self.assertEqual(self.client.search_calls[0]["limit"], RESULT_LIMIT)
        self.assertEqual(self.client.search_calls[0]["limit"], 100)

    def test_consistency_level_is_correct(self) -> None:
        self._search()
        self.assertEqual(self.client.search_calls[0]["consistency_level"], CONSISTENCY_LEVEL)

    def test_output_fields_request_ids_and_distances_only(self) -> None:
        """anns_field is set and output_fields is empty -- IDs/distances
        come back structurally from the search hit shape itself, not a
        scalar output field, matching MilvusHarness's own decode contract."""

        self._search()
        call = self.client.search_calls[0]
        self.assertIn("anns_field", call)
        self.assertEqual(call["output_fields"], [])

    def test_different_calls_use_different_query_vectors(self) -> None:
        other_vector = np.array([0.9, 0.8, 0.7, 0.6], dtype="<f4")
        self.client.search_calls.clear()
        self.adapter.search(
            query_id=2,
            query_vector=other_vector,
            metric=Metric.L2,
            threshold_stratum="target-075",
            ef=400,
            radius=5.0,
        )
        sent = self.client.search_calls[0]["data"][0]
        np.testing.assert_allclose(sent, other_vector.tolist())


class LkgMilvusAdapterTimingTests(unittest.TestCase):
    def test_monotonic_start_end_wrap_exactly_the_search_call(self) -> None:
        client = RecordingMilvusClient()
        calls_at_search_time: list[int] = []

        class TimestampingClient(RecordingMilvusClient):
            def search(inner_self, **kwargs):
                calls_at_search_time.append(len(inner_self.search_calls))
                return super().search(**kwargs)

        client = TimestampingClient()
        adapter = LkgMilvusAdapter(
            client, dimensions=4, hnsw_collection_name=HNSW_NAME, clock_ns=FakeClock(5_000, 9_000)
        )
        call = adapter.search(
            query_id=1,
            query_vector=np.array([0.1, 0.2, 0.3, 0.4], dtype="<f4"),
            metric=Metric.L2,
            threshold_stratum="target-075",
            ef=400,
            radius=5.0,
        )
        self.assertEqual(call.start_ns, 5_000)
        self.assertEqual(call.end_ns, 9_000)
        self.assertEqual(call.latency_ms, 4_000 / 1_000_000.0)
        # exactly one search() call happened between the two clock reads
        self.assertEqual(len(client.search_calls), 1)

    def test_nonmonotonic_clock_raises(self) -> None:
        client = RecordingMilvusClient()
        adapter = LkgMilvusAdapter(
            client, dimensions=4, hnsw_collection_name=HNSW_NAME, clock_ns=FakeClock(9_000, 5_000)
        )
        with self.assertRaises(ContractViolation):
            adapter.search(
                query_id=1,
                query_vector=np.array([0.1, 0.2, 0.3, 0.4], dtype="<f4"),
                metric=Metric.L2,
                threshold_stratum="target-075",
                ef=400,
                radius=5.0,
            )


class LkgMilvusAdapterResponseDecodingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = RecordingMilvusClient()
        self.adapter = LkgMilvusAdapter(
            self.client, dimensions=4, hnsw_collection_name=HNSW_NAME, clock_ns=FakeClock(1_000, 2_000)
        )
        self.query_vector = np.array([0.1, 0.2, 0.3, 0.4], dtype="<f4")

    def _search(self) -> LkgSearchCall:
        return self.adapter.search(
            query_id=1,
            query_vector=self.query_vector,
            metric=Metric.L2,
            threshold_stratum="target-075",
            ef=400,
            radius=5.0,
        )

    def test_ids_and_distances_decoded_from_the_response_only(self) -> None:
        self.client.response = [[{"id": 7, "distance": 0.5}, {"id": 3, "distance": 1.5}]]
        call = self._search()
        self.assertTrue(call.succeeded)
        self.assertEqual(call.hit_ids, (7, 3))
        self.assertEqual([hit.score for hit in call.hits], [0.5, 1.5])

    def test_duplicate_entity_ids_fail_closed(self) -> None:
        self.client.response = [[{"id": 7, "distance": 0.5}, {"id": 7, "distance": 0.6}]]
        call = self._search()
        self.assertFalse(call.succeeded)
        self.assertIsInstance(call.exception, ContractViolation)

    def test_malformed_batch_shape_fails_closed(self) -> None:
        self.client.response = []  # zero query-groups: invalid batch shape
        call = self._search()
        self.assertFalse(call.succeeded)
        self.assertIsInstance(call.exception, ContractViolation)

    def test_multi_group_batch_shape_fails_closed(self) -> None:
        self.client.response = [[{"id": 1, "distance": 0.1}], [{"id": 2, "distance": 0.2}]]
        call = self._search()
        self.assertFalse(call.succeeded)
        self.assertIsInstance(call.exception, ContractViolation)

    def test_nan_distance_fails_closed(self) -> None:
        self.client.response = [[{"id": 1, "distance": float("nan")}]]
        call = self._search()
        self.assertFalse(call.succeeded)
        self.assertIsInstance(call.exception, ContractViolation)

    def test_infinite_distance_fails_closed(self) -> None:
        self.client.response = [[{"id": 1, "distance": float("inf")}]]
        call = self._search()
        self.assertFalse(call.succeeded)
        self.assertIsInstance(call.exception, ContractViolation)

    def test_result_count_above_limit_fails_closed(self) -> None:
        self.client.response = [
            [{"id": index, "distance": float(index)} for index in range(RESULT_LIMIT + 1)]
        ]
        call = self._search()
        self.assertFalse(call.succeeded)
        self.assertIsInstance(call.exception, ContractViolation)

    def test_result_count_exactly_at_limit_succeeds(self) -> None:
        self.client.response = [
            [{"id": index, "distance": float(index)} for index in range(RESULT_LIMIT)]
        ]
        call = self._search()
        self.assertTrue(call.succeeded)
        self.assertEqual(len(call.hits), RESULT_LIMIT)

    def test_client_exception_is_captured_not_raised(self) -> None:
        self.client.raise_exception = ConnectionError("injected network failure")
        call = self._search()
        self.assertFalse(call.succeeded)
        self.assertIsInstance(call.exception, ConnectionError)

    def test_client_timeout_is_captured_not_raised(self) -> None:
        self.client.raise_exception = TimeoutError("injected timeout")
        call = self._search()
        self.assertFalse(call.succeeded)
        self.assertTrue(call.timed_out)


class LkgMilvusAdapterModuleStructureTests(unittest.TestCase):
    def test_pymilvus_import_is_lazy_and_confined_to_from_uri(self) -> None:
        source = ADAPTER_MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        parents = {
            child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)
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

    def test_module_never_imports_the_canary_execution_flow(self) -> None:
        """AST-based, not a substring search: this module's own docstring
        legitimately names milvus_actuation.py in prose (explaining what it
        does NOT depend on), so a naive substring check would false-positive
        on itself."""

        tree = ast.parse(ADAPTER_MODULE_PATH.read_text(encoding="utf-8"))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            f"vdbench.{node.module}" if node.level and node.module else (node.module or "")
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        forbidden = {
            "vdbench.milvus_actuation",
            "vdbench.canary_recall_audit_ledger",
            "vdbench.shadow_artifacts",
        }
        self.assertFalse(imported & forbidden, imported & forbidden)


if __name__ == "__main__":
    unittest.main()
