"""TDD coverage for immutable, role-disjoint DATASET-002 artifacts."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

from vdbench.artifacts import write_dataset_artifacts
from vdbench.config import ContractViolation, EXP001_DATASET_SPEC, Metric
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import (
    DATASET002_SPEC,
    Dataset002Spec,
    generate_dataset002,
    load_recall_audit_oracle_ids,
    verify_dataset002_artifacts,
    write_dataset002_artifacts,
)


def _small_dataset001(path: Path) -> None:
    spec = replace(
        EXP001_DATASET_SPEC,
        version="dataset001-fixture-v1",
        dimensions=4,
        base_count=100,
        calibration_query_count=5,
        measured_query_count=7,
    )
    bundle = generate_dataset(spec)
    write_dataset_artifacts(
        path,
        bundle,
        calibrate_thresholds(bundle.base_vectors, bundle.calibration_queries),
        boundary_fixtures(),
    )


class Dataset002Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = Dataset002Spec(
            dataset_id="DATASET-002",
            version="dataset002-fixture-v1",
            seed=20260809,
            dimensions=4,
            routing_query_count=6,
            recall_audit_query_count=12,
            dtype="<f4",
            distribution="independent standard normal",
            generator="numpy.random.Generator(numpy.random.PCG64(seed))",
        )

    def test_generator_is_deterministic_role_disjoint_and_little_endian(self) -> None:
        first = generate_dataset002(self.spec)
        second = generate_dataset002(self.spec)

        np.testing.assert_array_equal(first.routing_queries, second.routing_queries)
        np.testing.assert_array_equal(
            first.recall_audit_queries, second.recall_audit_queries
        )
        self.assertEqual(first.routing_queries.dtype.str, "<f4")
        self.assertEqual(first.recall_audit_queries.dtype.str, "<f4")
        self.assertFalse(set(first.routing_ids).intersection(first.recall_audit_ids))
        self.assertEqual(first.routing_queries.shape, (6, 4))
        self.assertEqual(first.recall_audit_queries.shape, (12, 4))

    def test_artifacts_round_trip_and_oracle_records_are_verified(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            output = root / "dataset002"
            _small_dataset001(dataset001)

            manifest = write_dataset002_artifacts(
                output,
                generate_dataset002(self.spec),
                dataset001_dir=dataset001,
            )
            verified = verify_dataset002_artifacts(output, dataset001_dir=dataset001)

            self.assertEqual(verified, manifest)
            self.assertEqual(
                set(manifest["artifacts"]),
                {
                    "routing_ids.npy",
                    "routing_queries.npy",
                    "recall_audit_ids.npy",
                    "recall_audit_queries.npy",
                    "inherited_dataset001.json",
                    "oracle_records.jsonl",
                },
            )
            records = (output / "oracle_records.jsonl").read_text(
                encoding="utf-8"
            ).splitlines()
            self.assertEqual(len(records), (6 + 12) * 6)
            first_record = json.loads(records[0])
            self.assertEqual(first_record["query_id"], 0)
            self.assertEqual(first_record["role"], "routing")
            self.assertIn("hits", first_record)

    def test_verifier_fails_closed_on_hidden_role_overlap_even_with_updated_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            output = root / "dataset002"
            _small_dataset001(dataset001)
            write_dataset002_artifacts(
                output,
                generate_dataset002(self.spec),
                dataset001_dir=dataset001,
            )

            audit_ids_path = output / "recall_audit_ids.npy"
            routing_ids = np.load(output / "routing_ids.npy", allow_pickle=False)
            audit_ids = np.load(audit_ids_path, allow_pickle=False)
            audit_ids[0] = routing_ids[0]
            with audit_ids_path.open("wb") as handle:
                np.save(handle, audit_ids, allow_pickle=False)

            manifest_path = output / "dataset002_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            payload = audit_ids_path.read_bytes()
            manifest["artifacts"][audit_ids_path.name] = {
                "file": audit_ids_path.name,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
            manifest_path.write_text(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            sums_path = output / "SHA256SUMS"
            entries = []
            for filename in (
                "routing_ids.npy",
                "routing_queries.npy",
                "recall_audit_ids.npy",
                "recall_audit_queries.npy",
                "inherited_dataset001.json",
                "oracle_records.jsonl",
                "dataset002_manifest.json",
            ):
                entries.append(
                    f"{hashlib.sha256((output / filename).read_bytes()).hexdigest()}  {filename}"
                )
            sums_path.write_text("\n".join(entries) + "\n", encoding="utf-8")

            with self.assertRaisesRegex(ContractViolation, "role overlap"):
                verify_dataset002_artifacts(output, dataset001_dir=dataset001)

    def test_verifier_rejects_hidden_generation_metadata_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            output = root / "dataset002"
            _small_dataset001(dataset001)
            write_dataset002_artifacts(
                output,
                generate_dataset002(self.spec),
                dataset001_dir=dataset001,
            )

            manifest_path = output / "dataset002_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["generation"]["license"] = "tampered"
            manifest_path.write_text(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            sums_path = output / "SHA256SUMS"
            entries = []
            for filename in (
                "routing_ids.npy",
                "routing_queries.npy",
                "recall_audit_ids.npy",
                "recall_audit_queries.npy",
                "inherited_dataset001.json",
                "oracle_records.jsonl",
                "dataset002_manifest.json",
            ):
                entries.append(
                    f"{hashlib.sha256((output / filename).read_bytes()).hexdigest()}  {filename}"
                )
            sums_path.write_text("\n".join(entries) + "\n", encoding="utf-8")

            with self.assertRaisesRegex(ContractViolation, "generation contract"):
                verify_dataset002_artifacts(output, dataset001_dir=dataset001)

    def test_writer_refuses_an_unverified_or_incompatible_dataset001_parent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            _small_dataset001(dataset001)
            with (dataset001 / "base_vectors.npy").open("ab") as handle:
                handle.write(b"tamper")

            with self.assertRaisesRegex(ContractViolation, "size mismatch"):
                write_dataset002_artifacts(
                    root / "dataset002",
                    generate_dataset002(self.spec),
                    dataset001_dir=dataset001,
                )

    def test_recall_audit_oracle_loader_filters_role_metric_and_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            output = root / "dataset002"
            _small_dataset001(dataset001)
            write_dataset002_artifacts(
                output,
                generate_dataset002(self.spec),
                dataset001_dir=dataset001,
            )

            all_records = [
                json.loads(line)
                for line in (output / "oracle_records.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            expected = {
                record["query_id"]: tuple(hit["id"] for hit in record["hits"])
                for record in all_records
                if record["role"] == "recall_audit"
                and record["metric"] == Metric.L2.value
                and record["threshold_label"] == "target-025"
            }
            self.assertEqual(len(expected), self.spec.recall_audit_query_count)

            loaded = load_recall_audit_oracle_ids(
                output, metric=Metric.L2, threshold_label="target-025"
            )

            self.assertEqual(loaded, expected)
            # Routing-role and other metric/threshold records must never leak in.
            self.assertFalse(
                set(loaded).intersection(
                    record["query_id"] for record in all_records if record["role"] == "routing"
                )
            )

    def test_recall_audit_oracle_loader_rejects_unregistered_threshold_label(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            output = root / "dataset002"
            _small_dataset001(dataset001)
            write_dataset002_artifacts(
                output,
                generate_dataset002(self.spec),
                dataset001_dir=dataset001,
            )

            with self.assertRaises(ContractViolation):
                load_recall_audit_oracle_ids(output, metric=Metric.L2, threshold_label="not-a-label")


if __name__ == "__main__":
    unittest.main()
