"""Offline integration tests for the EXP-009 Stage-1 evidence runner."""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from vdbench.artifacts import canonical_json_bytes, sha256_file, write_dataset_artifacts
from vdbench.config import EXP001_DATASET_SPEC, IndexTrack, Metric
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import Dataset002Spec, generate_dataset002, write_dataset002_artifacts
from vdbench.exp005_acquisition import capture_identity_baseline, persist_identity_baseline
from vdbench.exp009_stage1 import (
    Exp009Stage1Error,
    _load_canonical_json,
    assert_clean_committed_source,
    run_stage1,
)
from vdbench.milvus import CollectionIdentity


class _BaselineClient:
    def describe_index(self, collection_name: str, index_name: str) -> dict[str, str | int]:
        del index_name
        metric = "L2" if "l2" in collection_name else "COSINE"
        track = "HNSW" if "hnsw" in collection_name else "FLAT"
        result: dict[str, str | int] = {
            "field_name": "vector",
            "index_name": "vector_index",
            "index_type": track,
            "metric_type": metric,
            "state": "Finished",
            "indexed_rows": 100,
            "pending_index_rows": 0,
            "total_rows": 100,
        }
        if track == "HNSW":
            result.update({"M": "16", "efConstruction": "200"})
        return result


class Exp009Stage1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        root = Path(cls._temporary.name)
        cls.dataset001 = root / "dataset001"
        cls.dataset002 = root / "dataset002"
        cls.baseline_path = root / "baseline.json"
        source = generate_dataset(
            replace(
                EXP001_DATASET_SPEC,
                version="dataset001-exp009-runner-fixture-v1",
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
                    version="dataset002-exp009-runner-fixture-v1",
                    seed=20260809,
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
        data_identity = (
            "dataset001-exp009-runner-fixture-v1:sha256:"
            + sha256_file(cls.dataset001 / "generation_manifest.json")
        )
        baseline = capture_identity_baseline(
            client=_BaselineClient(),
            metric=Metric.L2,
            threshold_stratum="target-075",
            candidate_ef=800,
            last_known_good_ef=400,
            flat_collection_name="fixture_l2_flat",
            hnsw_collection_name="fixture_l2_hnsw",
            configuration_identity="exp009-config-v1:sha256:" + "a" * 64,
            data_identity=data_identity,
        )
        persist_identity_baseline(cls.baseline_path, baseline)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def test_stage1_writes_a_complete_independently_verifiable_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "stage1"
            with patch(
                "vdbench.exp009_stage1.assert_clean_committed_source",
                return_value="a" * 40,
            ):
                result = run_stage1(
                    repository=Path.cwd(),
                    dataset001_dir=self.dataset001,
                    dataset002_dir=self.dataset002,
                    baseline_path=self.baseline_path,
                    output_dir=target,
                    clock=lambda: "2026-08-04T12:00:00Z",
                )

            self.assertEqual(result.output_dir, target.resolve())
            self.assertTrue(result.manifest_path.is_file())
            self.assertTrue(result.completion_path.is_file())
            expected = {
                "eligible_workload.json",
                "candidate_selection.json",
                "calibration.json",
                "run_manifest.json",
                "completion.json",
            }
            self.assertEqual({path.name for path in target.iterdir()}, expected)
            completion = json.loads(result.completion_path.read_text(encoding="utf-8"))
            self.assertEqual(completion["status"], "COMPLETE")
            self.assertTrue(all(completion["verification"].values()))
            self.assertEqual(completion["candidate_count"], 60)
            self.assertEqual(completion["eligible_occurrence_count"], 600)
            self.assertEqual(
                completion["run_manifest_sha256"], sha256_file(result.manifest_path)
            )

    def test_runner_rejects_a_reviewed_baseline_for_any_other_transition(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            invalid = json.loads(self.baseline_path.read_text(encoding="utf-8"))
            invalid["baseline"]["candidate_ef"] = 1600
            payload = invalid["baseline"]
            invalid["expected_baseline_sha256"] = __import__("hashlib").sha256(
                canonical_json_bytes(payload)
            ).hexdigest()
            baseline_path = root / "invalid-baseline.json"
            baseline_path.write_bytes(canonical_json_bytes(invalid))
            with patch(
                "vdbench.exp009_stage1.assert_clean_committed_source",
                return_value="a" * 40,
            ):
                with self.assertRaisesRegex(Exp009Stage1Error, "TRANSITION"):
                    run_stage1(
                        repository=Path.cwd(),
                        dataset001_dir=self.dataset001,
                        dataset002_dir=self.dataset002,
                        baseline_path=baseline_path,
                        output_dir=root / "stage1",
                        clock=lambda: "2026-08-04T12:00:00Z",
                    )

    def test_runner_refuses_to_overwrite_an_existing_output_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "stage1"
            target.mkdir()
            with patch(
                "vdbench.exp009_stage1.assert_clean_committed_source",
                return_value="a" * 40,
            ):
                with self.assertRaises(FileExistsError):
                    run_stage1(
                        repository=Path.cwd(),
                        dataset001_dir=self.dataset001,
                        dataset002_dir=self.dataset002,
                        baseline_path=self.baseline_path,
                        output_dir=target,
                        clock=lambda: "2026-08-04T12:00:00Z",
                    )

    def test_clean_source_guard_rejects_modified_or_untracked_relevant_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repository = Path(temporary)
            (repository / "src" / "vdbench").mkdir(parents=True)
            tracked = repository / "src" / "vdbench" / "module.py"
            tracked.write_text("VALUE = 1\n", encoding="utf-8")
            for command in (
                ("git", "init", "-q"),
                ("git", "config", "user.email", "test@example.invalid"),
                ("git", "config", "user.name", "Test"),
                ("git", "add", "."),
                ("git", "commit", "-qm", "fixture"),
            ):
                subprocess.run(command, cwd=repository, check=True)
            assert_clean_committed_source(repository, required_paths=(tracked.relative_to(repository),))

            tracked.write_text("VALUE = 2\n", encoding="utf-8")
            with self.assertRaisesRegex(Exp009Stage1Error, "SOURCE_TRACKED_DIRTY"):
                assert_clean_committed_source(repository, required_paths=(tracked.relative_to(repository),))
            tracked.write_text("VALUE = 1\n", encoding="utf-8")
            untracked = repository / "src" / "vdbench" / "untracked.py"
            untracked.write_text("VALUE = 3\n", encoding="utf-8")
            with self.assertRaisesRegex(Exp009Stage1Error, "SOURCE_PATH_UNTRACKED"):
                assert_clean_committed_source(
                    repository,
                    required_paths=(tracked.relative_to(repository), untracked.relative_to(repository)),
                )

    def test_completion_parser_fails_closed_on_ambiguous_or_nonfinite_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "calibration.json"
            path.write_bytes(b'{"x":1,"x":1}\n')
            with self.assertRaisesRegex(Exp009Stage1Error, "CALIBRATION_INVALID"):
                _load_canonical_json(path, error_code="CALIBRATION_INVALID")
            path.write_bytes(b'{"x":NaN}\n')
            with self.assertRaisesRegex(Exp009Stage1Error, "CALIBRATION_INVALID"):
                _load_canonical_json(path, error_code="CALIBRATION_INVALID")


if __name__ == "__main__":
    unittest.main()
