"""TDD coverage for the dedicated DATASET-003 LKG-qualification runner.

Also proves this runner's execution path is structurally independent of the
DATASET-002 canary/shadow-audit machinery: it never imports
``milvus_actuation``, the Stage-4 recall-audit ledger, or the persisted
shadow-trace schema.
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

import numpy as np

from vdbench.config import ContractViolation, Metric
from vdbench.lkg_milvus_adapter import LkgMilvusAdapter
from vdbench.lkg_qualification_evidence import LkgAttemptStatus, LkgQueryAttempt
from vdbench.lkg_qualification_runner import LkgQualificationRunner
from vdbench.oracle import exact_range_search

REPOSITORY = Path(__file__).parents[1]
RUNNER_MODULE_PATH = REPOSITORY / "src" / "vdbench" / "lkg_qualification_runner.py"
LEDGER_MODULE_PATH = REPOSITORY / "src" / "vdbench" / "lkg_qualification_ledger.py"
PRODUCER_MODULE_PATH = REPOSITORY / "src" / "vdbench" / "lkg_qualification_producer.py"
LOADER_MODULE_PATH = REPOSITORY / "src" / "vdbench" / "lkg_dataset003_loader.py"
RUN_BINDING_MODULE_PATH = REPOSITORY / "src" / "vdbench" / "lkg_run_binding.py"
EVIDENCE_MODULE_PATH = REPOSITORY / "src" / "vdbench" / "lkg_qualification_evidence.py"
ADAPTER_MODULE_PATH = REPOSITORY / "src" / "vdbench" / "lkg_milvus_adapter.py"
HNSW_NAME = "lkg_l2_hnsw"
RUN_BINDING_SHA256 = "d" * 64

_FORBIDDEN_MODULES = frozenset(
    {
        "vdbench.milvus_actuation",
        "vdbench.canary_recall_audit_ledger",
        "vdbench.canary_recall_audit_producer",
        "vdbench.canary_stage4_evidence_binding",
        "vdbench.shadow_artifacts",
    }
)


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports.update(
        f"vdbench.{node.module}" if node.level and node.module else (node.module or "")
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    )
    return imports


class ModuleIndependenceTests(unittest.TestCase):
    def test_runner_never_imports_the_canary_execution_flow_or_its_schemas(self) -> None:
        imported = _imported_modules(RUNNER_MODULE_PATH)
        self.assertFalse(imported & _FORBIDDEN_MODULES, imported & _FORBIDDEN_MODULES)

    def test_ledger_never_imports_the_canary_execution_flow_or_its_schemas(self) -> None:
        imported = _imported_modules(LEDGER_MODULE_PATH)
        self.assertFalse(imported & _FORBIDDEN_MODULES, imported & _FORBIDDEN_MODULES)

    def test_producer_never_imports_the_canary_execution_flow_or_its_schemas(self) -> None:
        imported = _imported_modules(PRODUCER_MODULE_PATH)
        self.assertFalse(imported & _FORBIDDEN_MODULES, imported & _FORBIDDEN_MODULES)

    def test_loader_never_imports_the_canary_execution_flow_or_its_schemas(self) -> None:
        imported = _imported_modules(LOADER_MODULE_PATH)
        self.assertFalse(imported & _FORBIDDEN_MODULES, imported & _FORBIDDEN_MODULES)

    def test_run_binding_never_imports_the_canary_execution_flow_or_its_schemas(self) -> None:
        imported = _imported_modules(RUN_BINDING_MODULE_PATH)
        self.assertFalse(imported & _FORBIDDEN_MODULES, imported & _FORBIDDEN_MODULES)

    def test_evidence_never_imports_the_canary_execution_flow_or_its_schemas(self) -> None:
        imported = _imported_modules(EVIDENCE_MODULE_PATH)
        self.assertFalse(imported & _FORBIDDEN_MODULES, imported & _FORBIDDEN_MODULES)

    def test_adapter_never_imports_the_canary_execution_flow_or_its_schemas(self) -> None:
        imported = _imported_modules(ADAPTER_MODULE_PATH)
        self.assertFalse(imported & _FORBIDDEN_MODULES, imported & _FORBIDDEN_MODULES)

    def test_runner_never_imports_milvus_actuation_module_directly(self) -> None:
        """The runner is now a thin orchestrator around lkg_milvus_adapter --
        it must not import milvus.py's ClientLike/MilvusHarness directly
        either; all PyMilvus-facing construction lives in the adapter."""

        imported = _imported_modules(RUNNER_MODULE_PATH)
        self.assertNotIn("vdbench.milvus", imported)

    def test_no_lkg_module_imports_pymilvus_except_inside_adapter_from_uri(self) -> None:
        for path in (
            LEDGER_MODULE_PATH,
            PRODUCER_MODULE_PATH,
            LOADER_MODULE_PATH,
            RUN_BINDING_MODULE_PATH,
            EVIDENCE_MODULE_PATH,
            RUNNER_MODULE_PATH,
        ):
            with self.subTest(path=path.name):
                self.assertNotIn("pymilvus", path.read_text(encoding="utf-8"))


class FakeLkgMilvusClient:
    """Deterministic in-memory search: same oracle math the runner itself uses."""

    def __init__(self, *, base_ids: np.ndarray, base_vectors: np.ndarray) -> None:
        self.base_ids = base_ids
        self.base_vectors = base_vectors
        self.search_calls: list[dict[str, object]] = []
        self.injected_exception: Exception | None = None
        self.drop_first_hit = False

    def search(self, **kwargs):
        if self.injected_exception is not None:
            raise self.injected_exception
        vector = np.asarray(kwargs["data"][0], dtype="<f4")
        parameters = kwargs["search_params"]["params"]
        metric = Metric(kwargs["search_params"]["metric_type"])
        self.search_calls.append(kwargs)
        reference = exact_range_search(
            self.base_vectors,
            self.base_ids,
            vector,
            metric,
            radius=float(parameters["radius"]),
            range_filter=float(parameters["range_filter"]),
            limit=int(kwargs["limit"]),
        )
        hits = reference.hits
        if self.drop_first_hit and hits:
            hits = hits[1:]
        return [[{"id": hit.id, "distance": hit.score} for hit in hits]]


def _small_dataset() -> tuple[np.ndarray, np.ndarray]:
    base_ids = np.arange(1, 9, dtype=np.int64)
    rng = np.random.default_rng(20260806)
    base_vectors = rng.standard_normal((8, 4)).astype("<f4")
    return base_ids, base_vectors


class LkgQualificationRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base_ids, self.base_vectors = _small_dataset()
        self.client = FakeLkgMilvusClient(base_ids=self.base_ids, base_vectors=self.base_vectors)
        self.adapter = LkgMilvusAdapter(
            self.client, dimensions=4, hnsw_collection_name=HNSW_NAME
        )
        self.runner = LkgQualificationRunner(
            self.adapter, base_vectors=self.base_vectors, base_ids=self.base_ids
        )
        self.query_vector = self.base_vectors[0] * 0.999

    def _attempt(self, **overrides):
        kwargs = dict(
            query_id=1,
            query_vector=self.query_vector,
            metric=Metric.L2,
            threshold_stratum="target-075",
            ef=400,
            radius=5.0,
            attempt_sequence=0,
            attempt_number=1,
            run_binding_sha256=RUN_BINDING_SHA256,
        )
        kwargs.update(overrides)
        return self.runner.attempt_query(**kwargs)

    def test_constructor_rejects_non_adapter(self) -> None:
        with self.assertRaises(TypeError):
            LkgQualificationRunner(object(), base_vectors=self.base_vectors, base_ids=self.base_ids)

    def test_successful_attempt_carries_a_full_observation(self) -> None:
        attempt = self._attempt()
        self.assertIsInstance(attempt, LkgQueryAttempt)
        self.assertEqual(attempt.status, LkgAttemptStatus.SUCCESS)
        self.assertIsNone(attempt.error_code)
        self.assertIsNotNone(attempt.observation)
        self.assertEqual(attempt.observation.recall, 1.0)
        self.assertEqual(attempt.query_id, 1)
        self.assertEqual(attempt.attempt_sequence, 0)
        self.assertEqual(attempt.attempt_number, 1)
        self.assertEqual(attempt.run_binding_sha256, RUN_BINDING_SHA256)

    def test_one_client_search_call_per_attempt(self) -> None:
        self._attempt()
        self.assertEqual(len(self.client.search_calls), 1)

    def test_client_exception_becomes_a_typed_client_error_attempt(self) -> None:
        self.client.injected_exception = ConnectionError("injected")
        attempt = self._attempt()
        self.assertEqual(attempt.status, LkgAttemptStatus.CLIENT_ERROR)
        self.assertIsNone(attempt.observation)
        self.assertTrue(attempt.error_code.startswith("CLIENT_ERROR:"))

    def test_timeout_becomes_a_typed_timeout_attempt(self) -> None:
        self.client.injected_exception = TimeoutError("injected")
        attempt = self._attempt()
        self.assertEqual(attempt.status, LkgAttemptStatus.TIMEOUT)
        self.assertEqual(attempt.error_code, "TIMEOUT")

    def test_malformed_response_becomes_a_typed_malformed_response_attempt(self) -> None:
        self.client.injected_exception = ContractViolation("Milvus returned duplicate IDs")
        attempt = self._attempt()
        self.assertEqual(attempt.status, LkgAttemptStatus.MALFORMED_RESPONSE)
        self.assertTrue(attempt.error_code.startswith("MALFORMED_RESPONSE:"))

    def test_no_search_call_is_ever_raised_out_of_attempt_query(self) -> None:
        """attempt_query always returns a typed attempt; a search failure
        never propagates as a raw exception to the caller."""

        for exc in (ConnectionError("x"), TimeoutError("x"), RuntimeError("x")):
            with self.subTest(exc=type(exc).__name__):
                self.client.injected_exception = exc
                attempt = self._attempt()
                self.assertNotEqual(attempt.status, LkgAttemptStatus.SUCCESS)

    def test_oracle_failure_becomes_a_typed_oracle_error_attempt(self) -> None:
        """Simulates an oracle computation failure by passing a base_ids
        array that is inconsistent with base_vectors -- exact_range_search
        itself raises for this, which attempt_query must catch and
        classify, never let propagate and never silently treat as success."""

        broken_runner = LkgQualificationRunner(
            self.adapter,
            base_vectors=self.base_vectors,
            base_ids=self.base_ids[:-1],  # length mismatch vs base_vectors
        )
        attempt = broken_runner.attempt_query(
            query_id=1,
            query_vector=self.query_vector,
            metric=Metric.L2,
            threshold_stratum="target-075",
            ef=400,
            radius=5.0,
            attempt_sequence=0,
            attempt_number=1,
            run_binding_sha256=RUN_BINDING_SHA256,
        )
        self.assertEqual(attempt.status, LkgAttemptStatus.ORACLE_ERROR)
        self.assertIsNone(attempt.observation)
        self.assertTrue(attempt.error_code.startswith("ORACLE_ERROR:"))

    def test_oracle_failure_still_issues_only_one_search_call(self) -> None:
        broken_runner = LkgQualificationRunner(
            self.adapter, base_vectors=self.base_vectors, base_ids=self.base_ids[:-1]
        )
        broken_runner.attempt_query(
            query_id=1,
            query_vector=self.query_vector,
            metric=Metric.L2,
            threshold_stratum="target-075",
            ef=400,
            radius=5.0,
            attempt_sequence=0,
            attempt_number=1,
            run_binding_sha256=RUN_BINDING_SHA256,
        )
        self.assertEqual(len(self.client.search_calls), 1)

    def test_missing_hit_reduces_recall_below_one(self) -> None:
        self.client.drop_first_hit = True
        attempt = self._attempt()
        self.assertEqual(attempt.status, LkgAttemptStatus.SUCCESS)
        self.assertLess(attempt.observation.recall, 1.0)

    def test_sentinel_ef_100_search_succeeds_but_attempt_is_rejected(self) -> None:
        """ef=100 is a valid Milvus HNSW config but never LKG-eligible (ADR-002)."""

        with self.assertRaises(ContractViolation):
            self._attempt(ef=100)

    def test_retry_attempt_number_is_carried_through(self) -> None:
        attempt = self._attempt(attempt_number=3)
        self.assertEqual(attempt.attempt_number, 3)

    def test_same_call_provenance_recall_and_latency_share_one_search(self) -> None:
        """Structural proof: recall/latency both derive from exactly the
        one search call this attempt issued -- not independently supplied."""

        attempt = self._attempt()
        self.assertEqual(len(self.client.search_calls), 1)
        self.assertGreaterEqual(attempt.observation.end_ns, attempt.observation.start_ns)
        self.assertEqual(
            attempt.observation.latency_ms,
            max(
                0.0,
                float(attempt.observation.end_ns - attempt.observation.start_ns) / 1_000_000.0,
            ),
        )


if __name__ == "__main__":
    unittest.main()
