"""TDD coverage for EXP-009's restart-durable Stage-4 execution ledger."""

from __future__ import annotations

import ast
from dataclasses import replace
import hashlib
from pathlib import Path
import sqlite3
import tempfile
import unittest

from vdbench.artifacts import canonical_json_bytes, sha256_file, write_dataset_artifacts
from vdbench.canary_execution_ledger import (
    Stage4ExecutionLedger,
    Stage4LedgerError,
    Stage4LedgerStatus,
    Stage4SlotObservation,
)
from vdbench.canary_routing import build_canary_route_plan
from vdbench.canary_schedule import build_stage4_execution_schedule
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    CandidateSelectionRecord,
    WorkloadIdentityBinding,
    build_eligible_workload_manifest,
)
from vdbench.config import EXP001_DATASET_SPEC, Metric
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import Dataset002Spec, generate_dataset002, write_dataset002_artifacts


class CanaryExecutionLedgerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        root = Path(cls._temporary.name)
        dataset001 = root / "dataset001"
        dataset002 = root / "dataset002"
        source = generate_dataset(
            replace(
                EXP001_DATASET_SPEC,
                version="dataset001-ledger-fixture-v1",
                dimensions=4,
                base_count=100,
                calibration_query_count=5,
                measured_query_count=7,
            )
        )
        write_dataset_artifacts(
            dataset001,
            source,
            calibrate_thresholds(source.base_vectors, source.calibration_queries),
            boundary_fixtures(),
        )
        write_dataset002_artifacts(
            dataset002,
            generate_dataset002(
                Dataset002Spec(
                    dataset_id="DATASET-002",
                    version="dataset002-ledger-fixture-v1",
                    seed=20260816,
                    dimensions=4,
                    routing_query_count=600,
                    recall_audit_query_count=1200,
                    dtype="<f4",
                    distribution="independent standard normal",
                    generator="numpy.random.Generator(numpy.random.PCG64(seed))",
                )
            ),
            dataset001_dir=dataset001,
        )
        identity = WorkloadIdentityBinding(
            configuration_identity="exp009-ledger-config-v1",
            data_identity=(
                "dataset001-ledger-fixture-v1:sha256:"
                + sha256_file(dataset001 / "generation_manifest.json")
            ),
            flat_binding_id="flat-ledger-binding-v1",
            hnsw_binding_id="hnsw-ledger-binding-v1",
        )
        cls.manifest = build_eligible_workload_manifest(
            dataset002_dir=dataset002,
            dataset001_dir=dataset001,
            metric=Metric.L2,
            threshold_stratum="target-075",
            candidate_ef=800,
            last_known_good_ef=400,
            identity=identity,
            created_at_utc="2026-08-04T14:00:00Z",
        )
        manifest_sha = hashlib.sha256(
            canonical_json_bytes(cls.manifest.to_document())
        ).hexdigest()
        selection = CandidateSelectionRecord(
            schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
            selected_at_utc="2026-08-04T14:01:00Z",
            eligible_manifest_sha256=manifest_sha,
            population_count=600,
            candidate_count=60,
            candidate_fraction=0.10,
            candidate_occurrence_ids=tuple(
                item.occurrence_id
                for item in cls.manifest.occurrences
                if item.sequence_index % 10 == 0
            ),
            random_source="python.secrets.SystemRandom.sample",
            selected_before_candidate_results=True,
        )
        cls.plan = build_canary_route_plan(cls.manifest, selection)
        cls.schedule = build_stage4_execution_schedule(cls.manifest, cls.plan)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def _ledger(self, root: Path, *, schedule=None) -> Stage4ExecutionLedger:
        private = root / "private"
        private.mkdir(mode=0o700)
        return Stage4ExecutionLedger(
            private / "stage4.sqlite3",
            run_id="exp009-ledger-test-001",
            schedule=self.schedule if schedule is None else schedule,
        )

    def _observation(
        self,
        execution_index: int,
        *,
        success: bool = True,
        timed_out: bool = False,
        threshold_semantics_valid: bool = True,
        health_before_ok: bool = True,
        health_after_ok: bool = True,
        identity_before_ok: bool = True,
        identity_after_ok: bool = True,
        started_monotonic_ns: int | None = None,
        finished_monotonic_ns: int | None = None,
        reason_code: str | None = None,
    ) -> Stage4SlotObservation:
        step = self.schedule.steps[execution_index]
        start = execution_index * 10 if started_monotonic_ns is None else started_monotonic_ns
        end = start + 5 if finished_monotonic_ns is None else finished_monotonic_ns
        return Stage4SlotObservation(
            execution_index=execution_index,
            observed_ef=step.expected_ef,
            started_monotonic_ns=start,
            finished_monotonic_ns=end,
            recorded_at_utc=f"2026-08-04T14:02:{execution_index % 60:02d}Z",
            success=success,
            timed_out=timed_out,
            threshold_semantics_valid=threshold_semantics_valid,
            health_before_ok=health_before_ok,
            health_after_ok=health_after_ok,
            identity_before_ok=identity_before_ok,
            identity_after_ok=identity_after_ok,
            result_count=0,
            latency_ms=0.005,
            reason_code=reason_code,
        )

    def test_persists_ordered_tamper_evident_history_across_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = self._ledger(root)
            first = ledger.append(self._observation(0))
            second = ledger.append(self._observation(1))
            duplicate = ledger.append(self._observation(1))
            restarted = Stage4ExecutionLedger(
                root / "private" / "stage4.sqlite3",
                run_id="exp009-ledger-test-001",
                schedule=self.schedule,
            )

            self.assertTrue(first.accepted)
            self.assertTrue(second.accepted)
            self.assertFalse(duplicate.accepted)
            self.assertEqual(duplicate.reason_code, "EXECUTION_INDEX_UNEXPECTED")
            progress = restarted.progress()
            self.assertEqual(progress.status, Stage4LedgerStatus.IN_PROGRESS)
            self.assertEqual(progress.record_count, 2)
            self.assertEqual(len(restarted.records()), 2)
            self.assertEqual(restarted.records()[1].previous_record_sha256, first.record_sha256)

    def test_failed_outcome_is_recorded_then_blocks_all_later_slots(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ledger = self._ledger(Path(temporary))
            self.assertTrue(ledger.append(self._observation(0)).accepted)
            failed = ledger.append(
                self._observation(
                    1,
                    success=False,
                    threshold_semantics_valid=False,
                    reason_code="MILVUS_SEARCH_FAILED",
                )
            )
            after_failure = ledger.append(self._observation(2))

            self.assertTrue(failed.accepted)
            self.assertEqual(ledger.progress().status, Stage4LedgerStatus.FAILED)
            self.assertEqual(ledger.progress().reason_code, "MILVUS_SEARCH_FAILED")
            self.assertFalse(after_failure.accepted)
            self.assertEqual(after_failure.reason_code, "RUN_NOT_ACTIVE")

    def test_every_integrity_failure_is_terminal_once_persisted(self) -> None:
        cases = (
            (
                "query_failure",
                {
                    "success": False,
                    "threshold_semantics_valid": False,
                    "reason_code": "MILVUS_SEARCH_FAILED",
                },
            ),
            (
                "timeout",
                {
                    "success": False,
                    "timed_out": True,
                    "threshold_semantics_valid": False,
                    "reason_code": "MILVUS_SEARCH_TIMEOUT",
                },
            ),
            (
                "threshold",
                {
                    "threshold_semantics_valid": False,
                    "reason_code": "MILVUS_THRESHOLD_SEMANTICS_INVALID",
                },
            ),
            (
                "health_before",
                {
                    "health_before_ok": False,
                    "reason_code": "STACK_HEALTH_UNHEALTHY",
                },
            ),
            (
                "health_after",
                {
                    "health_after_ok": False,
                    "reason_code": "STACK_HEALTH_UNHEALTHY",
                },
            ),
            (
                "identity_before",
                {
                    "identity_before_ok": False,
                    "reason_code": "COLLECTION_IDENTITY_MISMATCH",
                },
            ),
            (
                "identity_after",
                {
                    "identity_after_ok": False,
                    "reason_code": "COLLECTION_IDENTITY_MISMATCH",
                },
            ),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, changes in cases:
                with self.subTest(name=name):
                    case_root = root / name
                    case_root.mkdir()
                    ledger = self._ledger(case_root)
                    result = ledger.append(self._observation(0, **changes))
                    self.assertTrue(result.accepted)
                    self.assertEqual(ledger.progress().status, Stage4LedgerStatus.FAILED)
                    self.assertFalse(ledger.append(self._observation(1)).accepted)

    def test_rejects_overlapping_interval_and_schedule_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = self._ledger(root)
            self.assertTrue(
                ledger.append(
                    self._observation(0, started_monotonic_ns=100, finished_monotonic_ns=110)
                ).accepted
            )
            overlapping = ledger.append(
                self._observation(1, started_monotonic_ns=110, finished_monotonic_ns=120)
            )
            self.assertFalse(overlapping.accepted)
            self.assertEqual(overlapping.reason_code, "MONOTONIC_INTERVAL_VIOLATION")

            alternate_selection = CandidateSelectionRecord(
                schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
                selected_at_utc="2026-08-04T14:01:00Z",
                eligible_manifest_sha256=hashlib.sha256(
                    canonical_json_bytes(self.manifest.to_document())
                ).hexdigest(),
                population_count=600,
                candidate_count=60,
                candidate_fraction=0.10,
                candidate_occurrence_ids=tuple(
                    item.occurrence_id
                    for item in self.manifest.occurrences
                    if item.sequence_index % 10 == 1
                ),
                random_source="python.secrets.SystemRandom.sample",
                selected_before_candidate_results=True,
            )
            alternate_schedule = build_stage4_execution_schedule(
                self.manifest,
                build_canary_route_plan(self.manifest, alternate_selection),
            )
            with self.assertRaisesRegex(Stage4LedgerError, "LEDGER_SCHEDULE_MISMATCH"):
                Stage4ExecutionLedger(
                    root / "private" / "stage4.sqlite3",
                    run_id="exp009-ledger-test-001",
                    schedule=alternate_schedule,
                )

    def test_all_1200_passing_records_are_the_only_complete_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ledger = self._ledger(Path(temporary))
            for execution_index in range(1200):
                result = ledger.append(self._observation(execution_index))
                self.assertTrue(result.accepted)
            progress = ledger.progress()

            self.assertEqual(progress.status, Stage4LedgerStatus.COMPLETE)
            self.assertEqual(progress.record_count, 1200)
            self.assertIsNone(progress.reason_code)

    def test_corrupt_storage_fails_closed_and_module_has_no_execution_imports(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = self._ledger(root)
            self.assertTrue(ledger.append(self._observation(0)).accepted)
            database = root / "private" / "stage4.sqlite3"
            connection = sqlite3.connect(database)
            try:
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute("DELETE FROM execution_records")
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(
                        "UPDATE execution_records SET record_json = record_json"
                    )
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute("DELETE FROM execution_run")
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute("UPDATE execution_run SET run_id = run_id")
            finally:
                connection.close()
            database.write_bytes(b"not a sqlite database")
            with self.assertRaisesRegex(Stage4LedgerError, "LEDGER_STORE_CORRUPTED"):
                Stage4ExecutionLedger(
                    database,
                    run_id="exp009-ledger-test-001",
                    schedule=self.schedule,
                )

        source = Path("src/vdbench/canary_execution_ledger.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        forbidden = {
            "milvus",
            "milvus_serving",
            "canary_activation",
            "canary_route_authority",
            "canary_query_source",
            "host_observation",
        }
        self.assertFalse(any(name.split(".")[-1] in forbidden for name in imports))


if __name__ == "__main__":
    unittest.main()
