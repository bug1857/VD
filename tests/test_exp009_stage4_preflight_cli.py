"""Fake-factory tests for the minimal live invocation wrapper only."""

from __future__ import annotations

import ast
from pathlib import Path
import tempfile
import unittest

from vdbench.config import ENV001_PINS
from vdbench.exp009_stage4_preflight import PreflightEvidenceTarget, target_from_artifacts
from vdbench.milvus_actuation import StackHealth
from experiments.exp009_stage4_preflight import PreflightInvocationError, run_preflight


class _HealthyProbe:
    def check(self) -> StackHealth:
        return StackHealth(True, True, "fake healthy")


class _Client:
    def __init__(self, target: PreflightEvidenceTarget) -> None:
        self.identities = {
            target.baseline.flat_binding.expected.collection_name: target.baseline.flat_binding.expected,
            target.baseline.hnsw_binding.expected.collection_name: target.baseline.hnsw_binding.expected,
        }

    def get_load_state(self, *, collection_name: str) -> dict[str, str]:
        if collection_name not in self.identities:
            raise ValueError("unknown collection")
        return {"state": "Loaded"}

    def describe_index(self, *, collection_name: str, index_name: str) -> object:
        if index_name != "vector_index":
            raise ValueError("wrong index")
        return self.identities[collection_name].description


class Exp009Stage4PreflightCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.target = target_from_artifacts(
            baseline_path=Path("artifacts/exp-005/baselines/l2-target-075-ef800-lkg400.json"),
            dataset_dir=Path("artifacts/exp-001/dataset"),
        )

    def test_success_uses_injected_factories_and_returns_complete_evidence(self) -> None:
        client_uris: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            result = run_preflight(
                output_dir=output,
                target=self.target,
                repository=Path.cwd(),
                client_factory=lambda uri: client_uris.append(uri) or _Client(self.target),
                health_probe_factory=lambda: _HealthyProbe(),
                utc_now=lambda: "2026-08-04T16:00:00Z",
            )

        self.assertEqual(client_uris, [ENV001_PINS.uri])
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["call_counts"], {"get_load_state": 4, "describe_index": 8})

    def test_non_pinned_uri_fails_before_client_construction(self) -> None:
        calls: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(PreflightInvocationError, "URI_NOT_PINNED"):
                run_preflight(
                    output_dir=Path(directory) / "evidence",
                    target=self.target,
                    repository=Path.cwd(),
                    uri="http://example.invalid:19530",
                    client_factory=lambda uri: calls.append(uri) or _Client(self.target),
                    health_probe_factory=lambda: _HealthyProbe(),
                    utc_now=lambda: "2026-08-04T16:00:00Z",
                )
        self.assertEqual(calls, [])

    def test_incomplete_capture_raises_only_after_persisting_evidence(self) -> None:
        class _Unhealthy:
            def check(self) -> StackHealth:
                return StackHealth(False, True, "fake unhealthy")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            with self.assertRaisesRegex(PreflightInvocationError, "PREFLIGHT_INCOMPLETE"):
                run_preflight(
                    output_dir=output,
                    target=self.target,
                    repository=Path.cwd(),
                    client_factory=lambda _uri: _Client(self.target),
                    health_probe_factory=lambda: _Unhealthy(),
                    utc_now=lambda: "2026-08-04T16:00:00Z",
                )
            self.assertTrue((output / "preflight_result.json").is_file())

    def test_existing_output_refuses_before_client_factory(self) -> None:
        calls: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            output.mkdir()
            with self.assertRaisesRegex(PreflightInvocationError, "OUTPUT_PATH_EXISTS"):
                run_preflight(
                    output_dir=output,
                    target=self.target,
                    repository=Path.cwd(),
                    client_factory=lambda uri: calls.append(uri) or _Client(self.target),
                    health_probe_factory=lambda: _HealthyProbe(),
                    utc_now=lambda: "2026-08-04T16:00:00Z",
                )
        self.assertEqual(calls, [])

    def test_wrapper_has_only_a_lazy_pymilvus_import_and_no_search_execution(self) -> None:
        path = Path("experiments/exp009_stage4_preflight.py")
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        self.assertFalse(
            {
                "vdbench.policy",
                "vdbench.actuation",
                "vdbench.canary_approval",
                "vdbench.canary_route_authority",
                "vdbench.canary_rollback",
            }
            & imports
        )
        pymilvus_imports = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == "pymilvus"
        ]
        self.assertEqual(len(pymilvus_imports), 1)
        self.assertNotIn(
            "search",
            {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)},
        )


if __name__ == "__main__":
    unittest.main()
