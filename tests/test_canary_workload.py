"""TDD coverage for EXP-009's immutable workload and CSPRNG selection boundary."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from vdbench.artifacts import sha256_file, write_dataset_artifacts
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    ELIGIBLE_WORKLOAD_SCHEMA_VERSION,
    CandidateSelectionRecord,
    WorkloadIdentityBinding,
    build_eligible_workload_manifest,
    create_candidate_selection_record,
    load_candidate_selection_record,
    load_eligible_workload_manifest,
    persist_candidate_selection_record,
    persist_eligible_workload_manifest,
    verify_candidate_selection_record,
    verify_eligible_workload_manifest,
)
from vdbench.config import EXP001_DATASET_SPEC, ContractViolation, Metric
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import Dataset002Spec, generate_dataset002, write_dataset002_artifacts


class _FixedCSPRNG:
    """A deterministic test double for the real SystemRandom boundary."""

    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], int]] = []

    def sample(self, population: list[str], count: int) -> list[str]:
        self.calls.append((tuple(population), count))
        return list(reversed(population[-count:]))


class CanaryWorkloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        root = Path(cls._temporary.name)
        cls.dataset001 = root / "dataset001"
        cls.dataset002 = root / "dataset002"
        source = generate_dataset(
            replace(
                EXP001_DATASET_SPEC,
                version="dataset001-canary-workload-fixture-v1",
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
        spec = Dataset002Spec(
            dataset_id="DATASET-002",
            version="dataset002-canary-workload-fixture-v1",
            seed=20260809,
            dimensions=4,
            routing_query_count=600,
            recall_audit_query_count=1200,
            dtype="<f4",
            distribution="independent standard normal",
            generator="numpy.random.Generator(numpy.random.PCG64(seed))",
        )
        write_dataset002_artifacts(
            cls.dataset002,
            generate_dataset002(spec),
            dataset001_dir=cls.dataset001,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def setUp(self) -> None:
        self.identity = WorkloadIdentityBinding(
            configuration_identity="exp009-config-v1:sha256:" + "a" * 64,
            data_identity=(
                "dataset001-canary-workload-fixture-v1:sha256:"
                + sha256_file(self.dataset001 / "generation_manifest.json")
            ),
            flat_binding_id="exp005-index-binding-v1:" + "b" * 64,
            hnsw_binding_id="exp005-index-binding-v1:" + "c" * 64,
        )

    def _manifest(self):
        return build_eligible_workload_manifest(
            dataset002_dir=self.dataset002,
            dataset001_dir=self.dataset001,
            metric=Metric.L2,
            threshold_stratum="target-075",
            candidate_ef=800,
            last_known_good_ef=400,
            identity=self.identity,
            created_at_utc="2026-08-04T12:00:00Z",
        )

    def test_valid_manifest_is_canonical_600_unique_and_independently_verifiable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "eligible.json"
            manifest = self._manifest()
            persist_eligible_workload_manifest(path, manifest)

            loaded = load_eligible_workload_manifest(path)
            verified = verify_eligible_workload_manifest(
                path,
                dataset002_dir=self.dataset002,
                dataset001_dir=self.dataset001,
            )

            self.assertEqual(loaded, manifest)
            self.assertEqual(verified, manifest)
            self.assertEqual(loaded.schema_version, ELIGIBLE_WORKLOAD_SCHEMA_VERSION)
            self.assertEqual(len(loaded.occurrences), 600)
            self.assertEqual(
                [entry.sequence_index for entry in loaded.occurrences], list(range(600))
            )
            self.assertEqual(len({entry.occurrence_id for entry in loaded.occurrences}), 600)
            self.assertEqual(len({entry.vector_sha256 for entry in loaded.occurrences}), 600)
            self.assertEqual(loaded.vector_mapping, "one_to_one_unique_dataset002_routing_vectors")
            self.assertEqual(loaded.radius, loaded.occurrences[0].threshold_radius)
            self.assertEqual(loaded.limit, 100)

    def test_manifest_freezes_the_stage4_schedule_stability_control_contract(self) -> None:
        """Stage 1 must bind the future no-interference diagnostic before selection."""

        manifest = self._manifest()
        schedule = manifest.schedule_stability

        self.assertEqual(schedule.control_role, "recall_audit")
        self.assertEqual(schedule.control_ef, 400)
        self.assertEqual(schedule.control_query_ids, tuple(range(600, 650)))
        self.assertEqual(len(schedule.control_query_ids), 50)
        self.assertEqual(len(set(schedule.control_vector_sha256)), 50)
        self.assertEqual(schedule.pre_sweep_count, 3)
        self.assertEqual(schedule.routing_block_size, 100)
        self.assertEqual(schedule.interleaved_sweep_count, 6)
        self.assertEqual(schedule.post_sweep_count, 3)
        self.assertEqual(schedule.execution_mode, "synchronous_serial_manifest_order")
        self.assertEqual(schedule.absolute_p95_latency_ms_ceiling, 10.0)
        self.assertEqual(schedule.p95_relative_ceiling, 1.5)
        self.assertEqual(schedule.median_relative_ceiling, 1.25)
        self.assertTrue(schedule.require_all_success)
        self.assertTrue(schedule.require_identity_and_health_per_sweep)
        self.assertEqual(
            schedule.control_vector_sha256,
            tuple(
                hashlib.sha256(
                    np.ascontiguousarray(vector, dtype="<f4").tobytes(order="C")
                ).hexdigest()
                for vector in np.load(
                    self.dataset002 / "recall_audit_queries.npy", allow_pickle=False
                )[:50]
            ),
        )

    def test_manifest_rejects_tampered_or_overlapping_schedule_controls(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "eligible.json"
            overlap_path = Path(temporary) / "overlap.json"
            persist_eligible_workload_manifest(path, self._manifest())
            document = json.loads(path.read_text(encoding="utf-8"))
            document["schedule_stability"]["controls"][0]["query_id"] = 0
            path.write_text(
                json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ContractViolation, "schedule stability"):
                load_eligible_workload_manifest(path)

            persist_eligible_workload_manifest(overlap_path, self._manifest())
            document = json.loads(overlap_path.read_text(encoding="utf-8"))
            document["schedule_stability"]["controls"][0]["vector_sha256"] = document[
                "occurrences"
            ][0]["vector_sha256"]
            overlap_path.write_text(
                json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ContractViolation, "disjoint from routing"):
                load_eligible_workload_manifest(overlap_path)

    def test_selection_is_csprng_draw_after_persisted_manifest_and_binds_exact_file_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = root / "eligible.json"
            selection_path = root / "selection.json"
            persist_eligible_workload_manifest(manifest_path, self._manifest())
            fake = _FixedCSPRNG()

            with patch("vdbench.canary_workload.secrets.SystemRandom", return_value=fake):
                selection = create_candidate_selection_record(
                    manifest_path,
                    selected_at_utc="2026-08-04T12:01:00Z",
                )
            persist_candidate_selection_record(selection_path, selection, manifest_path)
            loaded = load_candidate_selection_record(selection_path)
            verified = verify_candidate_selection_record(selection_path, manifest_path)

            self.assertEqual(fake.calls[0][1], 60)
            self.assertEqual(selection, loaded)
            self.assertEqual(selection, verified)
            self.assertIsInstance(selection, CandidateSelectionRecord)
            self.assertEqual(selection.schema_version, CANDIDATE_SELECTION_SCHEMA_VERSION)
            self.assertEqual(selection.eligible_manifest_sha256, sha256_file(manifest_path))
            self.assertEqual(len(selection.candidate_occurrence_ids), 60)
            self.assertEqual(len(set(selection.candidate_occurrence_ids)), 60)
            eligible_ids = [entry.occurrence_id for entry in self._manifest().occurrences]
            expected_ids = tuple(eligible_ids[-60:])
            self.assertEqual(selection.candidate_occurrence_ids, expected_ids)
            self.assertNotIn("entropy", selection.to_document()["random_source"])

    def test_selection_refuses_unpersisted_manifest_and_non_increasing_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            missing_path = root / "not-persisted.json"
            with self.assertRaises(FileNotFoundError):
                create_candidate_selection_record(
                    missing_path,
                    selected_at_utc="2026-08-04T12:01:00Z",
                )

            manifest_path = root / "eligible.json"
            persist_eligible_workload_manifest(manifest_path, self._manifest())
            with self.assertRaisesRegex(ContractViolation, "strictly after"):
                create_candidate_selection_record(
                    manifest_path,
                    selected_at_utc="2026-08-04T12:00:00Z",
                )

    def test_selection_fails_closed_on_manifest_substitution_or_tampered_record(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = root / "eligible.json"
            other_manifest_path = root / "other-eligible.json"
            selection_path = root / "selection.json"
            persist_eligible_workload_manifest(manifest_path, self._manifest())
            alternate = build_eligible_workload_manifest(
                dataset002_dir=self.dataset002,
                dataset001_dir=self.dataset001,
                metric=Metric.COSINE,
                threshold_stratum="target-025",
                candidate_ef=400,
                last_known_good_ef=200,
                identity=self.identity,
                created_at_utc="2026-08-04T12:00:01Z",
            )
            persist_eligible_workload_manifest(other_manifest_path, alternate)
            selection = create_candidate_selection_record(
                manifest_path,
                selected_at_utc="2026-08-04T12:01:00Z",
            )
            persist_candidate_selection_record(selection_path, selection, manifest_path)

            with self.assertRaisesRegex(ContractViolation, "manifest digest"):
                verify_candidate_selection_record(selection_path, other_manifest_path)

            document = json.loads(selection_path.read_text(encoding="utf-8"))
            document["candidate_occurrence_ids"][0] = "exp009-routing-999999"
            selection_path.write_text(
                json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ContractViolation, "selection record"):
                verify_candidate_selection_record(selection_path, manifest_path)

    def test_manifest_rejects_nonadjacent_ef_wrong_data_identity_and_existing_output(self) -> None:
        with self.assertRaisesRegex(ContractViolation, "adjacent"):
            build_eligible_workload_manifest(
                dataset002_dir=self.dataset002,
                dataset001_dir=self.dataset001,
                metric=Metric.L2,
                threshold_stratum="target-075",
                candidate_ef=1600,
                last_known_good_ef=400,
                identity=self.identity,
                created_at_utc="2026-08-04T12:00:00Z",
            )
        with self.assertRaisesRegex(ContractViolation, "DATASET-001"):
            build_eligible_workload_manifest(
                dataset002_dir=self.dataset002,
                dataset001_dir=self.dataset001,
                metric=Metric.L2,
                threshold_stratum="target-075",
                candidate_ef=800,
                last_known_good_ef=400,
                identity=replace(self.identity, data_identity="wrong-data-identity"),
                created_at_utc="2026-08-04T12:00:00Z",
            )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "eligible.json"
            persist_eligible_workload_manifest(path, self._manifest())
            with self.assertRaises(FileExistsError):
                persist_eligible_workload_manifest(path, self._manifest())

    def test_verifier_rejects_noncanonical_or_dataset_tampered_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = root / "eligible.json"
            persist_eligible_workload_manifest(manifest_path, self._manifest())
            manifest_path.write_bytes(manifest_path.read_bytes() + b"\n")
            with self.assertRaisesRegex(ContractViolation, "canonical JSON"):
                load_eligible_workload_manifest(manifest_path)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            copied_dataset001 = root / "dataset001"
            copied_dataset002 = root / "dataset002"
            shutil.copytree(self.dataset001, copied_dataset001)
            shutil.copytree(self.dataset002, copied_dataset002)
            copied_identity = replace(
                self.identity,
                data_identity=(
                    "dataset001-canary-workload-fixture-v1:sha256:"
                    + sha256_file(copied_dataset001 / "generation_manifest.json")
                ),
            )
            manifest = build_eligible_workload_manifest(
                dataset002_dir=copied_dataset002,
                dataset001_dir=copied_dataset001,
                metric=Metric.L2,
                threshold_stratum="target-075",
                candidate_ef=800,
                last_known_good_ef=400,
                identity=copied_identity,
                created_at_utc="2026-08-04T12:00:00Z",
            )
            manifest_path = root / "eligible.json"
            persist_eligible_workload_manifest(manifest_path, manifest)
            with (copied_dataset002 / "routing_queries.npy").open("ab") as handle:
                handle.write(b"tamper")
            with self.assertRaisesRegex(ContractViolation, "checksum"):
                verify_eligible_workload_manifest(
                    manifest_path,
                    dataset002_dir=copied_dataset002,
                    dataset001_dir=copied_dataset001,
                )

    def test_selection_rejects_unknown_fields_and_noncanonical_serialization(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = root / "eligible.json"
            selection_path = root / "selection.json"
            persist_eligible_workload_manifest(manifest_path, self._manifest())
            selection = create_candidate_selection_record(
                manifest_path,
                selected_at_utc="2026-08-04T12:01:00Z",
            )
            persist_candidate_selection_record(selection_path, selection, manifest_path)

            document = json.loads(selection_path.read_text(encoding="utf-8"))
            document["raw_entropy"] = "prohibited"
            selection_path.write_text(
                json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ContractViolation, "candidate selection record"):
                load_candidate_selection_record(selection_path)

            selection_path.write_bytes(
                json.dumps(selection.to_document(), sort_keys=True, separators=(",", ":"))
                .encode("utf-8")
                + b"\n\n"
            )
            with self.assertRaisesRegex(ContractViolation, "canonical JSON"):
                load_candidate_selection_record(selection_path)


if __name__ == "__main__":
    unittest.main()
