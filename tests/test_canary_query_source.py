"""Tests for the verified, non-routing DATASET-002 canary query source."""

from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from vdbench.artifacts import sha256_file, write_dataset_artifacts
from vdbench.canary_query_source import CanaryQuerySourceError, Dataset002CanaryQuerySource
from vdbench.canary_workload import (
    WorkloadIdentityBinding,
    build_eligible_workload_manifest,
)
from vdbench.config import EXP001_DATASET_SPEC, Metric
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import Dataset002Spec, generate_dataset002, write_dataset002_artifacts


class Dataset002CanaryQuerySourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        root = Path(cls._temporary.name)
        cls.dataset001 = root / "dataset001"
        cls.dataset002 = root / "dataset002"
        source = generate_dataset(
            replace(
                EXP001_DATASET_SPEC,
                version="dataset001-query-source-fixture-v1",
                dimensions=4,
                base_count=100,
                calibration_query_count=5,
                measured_query_count=7,
            )
        )
        write_dataset_artifacts(
            cls.dataset001,
            source,
            calibrate_thresholds(source.base_vectors, source.calibration_queries),
            boundary_fixtures(),
        )
        write_dataset002_artifacts(
            cls.dataset002,
            generate_dataset002(
                Dataset002Spec(
                    dataset_id="DATASET-002",
                    version="dataset002-query-source-fixture-v1",
                    seed=20260815,
                    dimensions=4,
                    routing_query_count=600,
                    recall_audit_query_count=1200,
                    dtype="<f4",
                    distribution="independent standard normal",
                    generator="numpy.random.Generator(numpy.random.PCG64(seed))",
                )
            ),
            dataset001_dir=cls.dataset001,
        )
        identity = WorkloadIdentityBinding(
            configuration_identity="exp009-query-source-config-v1",
            data_identity=(
                "dataset001-query-source-fixture-v1:sha256:"
                + sha256_file(cls.dataset001 / "generation_manifest.json")
            ),
            flat_binding_id="flat-query-source-binding-v1",
            hnsw_binding_id="hnsw-query-source-binding-v1",
        )
        cls.manifest = build_eligible_workload_manifest(
            dataset002_dir=cls.dataset002,
            dataset001_dir=cls.dataset001,
            metric=Metric.L2,
            threshold_stratum="target-075",
            candidate_ef=800,
            last_known_good_ef=400,
            identity=identity,
            created_at_utc="2026-08-04T12:10:00Z",
        )
    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def _source(self) -> Dataset002CanaryQuerySource:
        return Dataset002CanaryQuerySource.from_verified_artifacts(
            dataset002_dir=self.dataset002,
            dataset001_dir=self.dataset001,
            manifest=self.manifest,
        )

    def test_verified_source_returns_only_manifest_bound_routing_and_control_vectors(self) -> None:
        source = self._source()

        occurrence = self.manifest.occurrences[0]
        routing = source.routing_vector(
            occurrence_id=occurrence.occurrence_id,
            dataset_query_id=occurrence.dataset_query_id,
            vector_sha256=occurrence.vector_sha256,
        )
        control = source.control_vector(control=self.manifest.schedule_stability.controls[0])
        audit = source.recall_audit_vector(query_id=650)

        self.assertEqual(len(routing), 4)
        self.assertEqual(len(control), 4)
        self.assertEqual(len(audit), 4)
        self.assertNotEqual(routing, control)
        self.assertEqual(source.routing_count, 600)
        self.assertEqual(source.control_count, 50)
        self.assertEqual(source.recall_audit_count, 1200)

    def test_substituted_occurrence_or_control_binding_fails_closed(self) -> None:
        source = self._source()
        occurrence = self.manifest.occurrences[0]
        bad_control = replace(
            self.manifest.schedule_stability.controls[0], vector_sha256="e" * 64
        )

        with self.assertRaisesRegex(CanaryQuerySourceError, "ROUTING_OCCURRENCE_MISMATCH"):
            source.routing_vector(
                occurrence_id=occurrence.occurrence_id,
                dataset_query_id=occurrence.dataset_query_id,
                vector_sha256="f" * 64,
            )
        with self.assertRaisesRegex(CanaryQuerySourceError, "CONTROL_BINDING_MISMATCH"):
            source.control_vector(control=bad_control)
        with self.assertRaisesRegex(CanaryQuerySourceError, "RECALL_AUDIT_QUERY_ID_UNKNOWN"):
            source.recall_audit_vector(query_id=42)

    def test_dataset_checksum_or_array_tamper_refuses_construction(self) -> None:
        array_path = self.dataset002 / "routing_queries.npy"
        original = array_path.read_bytes()
        payload = bytearray(original)
        payload[-1] ^= 0x01
        array_path.write_bytes(bytes(payload))
        try:
            with self.assertRaisesRegex(CanaryQuerySourceError, "DATASET002_VERIFICATION_FAILED"):
                self._source()
        finally:
            array_path.write_bytes(original)

    def test_source_has_no_milvus_routing_or_execution_import(self) -> None:
        source_path = Path(__file__).parents[1] / "src" / "vdbench" / "canary_query_source.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        imported = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            node.module.split(".")[-1]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        )
        self.assertFalse(
            {
                "milvus",
                "milvus_serving",
                "milvus_host_executor",
                "canary_activation",
                "canary_rollback",
                "canary_route_authority",
                "canary_routing",
                "actuation",
            }
            & imported
        )


if __name__ == "__main__":
    unittest.main()
