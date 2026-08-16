"""Fail-closed tests for the read-only EXP-009 Stage-4 preflight core."""

from __future__ import annotations

import ast
import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from vdbench.artifacts import canonical_json_bytes, sha256_file
from vdbench.config import IndexTrack, Metric
from vdbench.exp005_acquisition import IdentityBaseline
from vdbench.exp009_stage4_preflight import (
    PreflightEvidenceTarget,
    ReadOnlyPreflightClient,
    capture_read_only_preflight,
    verify_preflight_evidence,
)
from vdbench.milvus import CollectionIdentity
from vdbench.milvus_actuation import CollectionIdentityBinding, StackHealth


def _identity(track: IndexTrack) -> CollectionIdentity:
    description: dict[str, object] = {
        "field_name": "vector",
        "index_name": "vector_index",
        "index_type": track.value,
        "indexed_rows": 10_000,
        "metric_type": "L2",
        "pending_index_rows": 0,
        "state": "Finished",
        "total_rows": 10_000,
    }
    if track is IndexTrack.HNSW:
        description.update({"M": "16", "efConstruction": "200"})
    return CollectionIdentity(
        f"exp001_20260801T161924Z_l2_{track.value.lower()}",
        "L2",
        track.value,
        description,
    )


class _HealthyProbe:
    def check(self) -> StackHealth:
        return StackHealth(True, True, "fake healthy")


class _UnhealthyProbe:
    def check(self) -> StackHealth:
        return StackHealth(False, True, "fake unhealthy")


class _Client:
    def __init__(self, *, flat: CollectionIdentity, hnsw: CollectionIdentity) -> None:
        self.identities = {flat.collection_name: flat, hnsw.collection_name: hnsw}

    def get_load_state(self, *, collection_name: str) -> dict[str, str]:
        if collection_name not in self.identities:
            raise ValueError("unknown collection")
        return {"state": "Loaded"}

    def describe_index(self, *, collection_name: str, index_name: str) -> object:
        if index_name != "vector_index":
            raise ValueError("wrong index")
        return self.identities[collection_name].description


class Exp009Stage4PreflightTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        flat, hnsw = _identity(IndexTrack.FLAT), _identity(IndexTrack.HNSW)
        cls.target = PreflightEvidenceTarget(
            baseline=IdentityBaseline(
                metric=Metric.L2,
                threshold_stratum="target-075",
                candidate_ef=800,
                last_known_good_ef=400,
                configuration_identity="exp005-config-v1:sha256:" + "a" * 64,
                data_identity="DATASET-001-v1:sha256:" + "b" * 64,
                flat_binding=CollectionIdentityBinding("flat-binding", flat),
                hnsw_binding=CollectionIdentityBinding("hnsw-binding", hnsw),
                sha256="c" * 64,
            ),
            dataset_manifest_sha256="d" * 64,
            dimensions=128,
            threshold_radius=0.75,
        )
        cls.client = _Client(flat=flat, hnsw=hnsw)

    def _capture(self, *, target: PreflightEvidenceTarget | None = None, health: object | None = None):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name) / "evidence"
        result = capture_read_only_preflight(
            target=self.target if target is None else target,
            output_dir=root,
            client=self.client,
            stack_health_probe=_HealthyProbe() if health is None else health,
            repository=Path.cwd(),
            utc_now=lambda: "2026-08-04T15:30:00Z",
        )
        return temporary, root, result

    def test_success_records_exact_read_only_transcript_and_verifies(self) -> None:
        temporary, root, result = self._capture()
        with temporary:
            verified = verify_preflight_evidence(root, target=self.target)
            self.assertTrue(result.complete)
            self.assertEqual(result.reason_codes, ())
            self.assertEqual(result.call_counts, {"get_load_state": 4, "describe_index": 8})
            self.assertEqual(verified["status"], "COMPLETE")
            self.assertEqual(verified["call_counts"], result.call_counts)

    def test_baseline_mismatch_stops_before_runtime_preflight_and_fails_closed(self) -> None:
        bad_flat = replace(
            self.target.baseline.flat_binding,
            expected=replace(
                self.target.baseline.flat_binding.expected,
                description={
                    **self.target.baseline.flat_binding.expected.description,
                    "state": "Unexpected",
                },
            ),
        )
        target = replace(self.target, baseline=replace(self.target.baseline, flat_binding=bad_flat))
        temporary, root, result = self._capture(target=target)
        with temporary:
            self.assertFalse(result.complete)
            self.assertIn("PREFLIGHT_IDENTITY_BASELINE_MISMATCH", result.reason_codes)
            self.assertEqual(result.call_counts, {"get_load_state": 0, "describe_index": 2})
            self.assertEqual(
                verify_preflight_evidence(root, target=target, require_complete=False)["status"],
                "INCOMPLETE",
            )

    def test_unhealthy_stack_fails_closed_without_slot_probe(self) -> None:
        temporary, root, result = self._capture(health=_UnhealthyProbe())
        with temporary:
            self.assertFalse(result.complete)
            self.assertIn("PREFLIGHT_RUNTIME_INCOMPLETE", result.reason_codes)
            self.assertEqual(result.call_counts, {"get_load_state": 0, "describe_index": 2})
            self.assertEqual(
                verify_preflight_evidence(root, target=self.target, require_complete=False)["status"],
                "INCOMPLETE",
            )

    def test_client_facade_exposes_no_search_or_mutation_method(self) -> None:
        facade = ReadOnlyPreflightClient(
            self.client,
            allowed_collections=frozenset(self.client.identities),
        )
        with self.assertRaises(AttributeError):
            facade.search  # type: ignore[attr-defined]  # the expression under test must be evaluated for its side effect  # the expression must be evaluated for its side effect  # noqa: B018
        with self.assertRaises(AttributeError):
            facade.create_collection  # type: ignore[attr-defined]  # the expression under test must be evaluated for its side effect  # the expression must be evaluated for its side effect  # noqa: B018

    def test_existing_output_is_refused_before_client_use(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "evidence"
            root.mkdir()
            with self.assertRaisesRegex(ValueError, "OUTPUT_PATH_EXISTS"):
                capture_read_only_preflight(
                    target=self.target,
                    output_dir=root,
                    client=self.client,
                    stack_health_probe=_HealthyProbe(),
                    repository=Path.cwd(),
                    utc_now=lambda: "2026-08-04T15:30:00Z",
                )

    def test_verifier_rejects_schema_tampering_even_after_internal_rehash(self) -> None:
        temporary, root, _result = self._capture()
        with temporary:
            result_path = root / "preflight_result.json"
            manifest_path = root / "manifest.json"
            receipt_path = root / "execution_receipt.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))
            result["unexpected"] = True
            result.pop("self_sha256")
            result["self_sha256"] = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
            result_path.write_bytes(canonical_json_bytes(result))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["result_file_sha256"] = sha256_file(result_path)
            manifest["result_self_sha256"] = result["self_sha256"]
            manifest.pop("self_sha256")
            manifest["self_sha256"] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
            manifest_path.write_bytes(canonical_json_bytes(manifest))
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt["result_self_sha256"] = result["self_sha256"]
            receipt["manifest_self_sha256"] = manifest["self_sha256"]
            receipt.pop("self_sha256")
            receipt["self_sha256"] = hashlib.sha256(canonical_json_bytes(receipt)).hexdigest()
            receipt_path.write_bytes(canonical_json_bytes(receipt))
            with self.assertRaisesRegex(ValueError, "PREFLIGHT_EVIDENCE_LINKAGE_INVALID"):
                verify_preflight_evidence(root, target=self.target)

    def test_verifier_rejects_a_complete_bundle_for_the_wrong_frozen_target(self) -> None:
        temporary, root, _result = self._capture()
        with temporary:
            wrong_target = replace(self.target, dataset_manifest_sha256="e" * 64)
            with self.assertRaisesRegex(ValueError, "PREFLIGHT_EVIDENCE_LINKAGE_INVALID"):
                verify_preflight_evidence(root, target=wrong_target)

    def test_core_imports_no_pymilvus_policy_authority_or_search_execution(self) -> None:
        path = Path("src/vdbench/exp009_stage4_preflight.py")
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
        forbidden = {
            "pymilvus",
            "vdbench.policy",
            "vdbench.actuation",
            "vdbench.canary_approval",
            "vdbench.canary_route_authority",
            "vdbench.canary_rollback",
        }
        self.assertFalse(forbidden & imports)
        self.assertNotIn(
            "execute",
            {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)},
        )


if __name__ == "__main__":
    unittest.main()
