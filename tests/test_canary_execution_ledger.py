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
    Stage4LedgerAppendResult,
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

    def _start_and_complete(
        self,
        ledger: Stage4ExecutionLedger,
        execution_index: int,
        **kwargs: object,
    ) -> Stage4LedgerAppendResult:
        """Drive the real two-phase start_slot -> complete_slot lifecycle.

        Mirrors the pre-hardening ``ledger.append(self._observation(...))``
        call shape as closely as possible so most existing test bodies need
        only this one substitution.
        """

        started_monotonic_ns = kwargs.pop("started_monotonic_ns", None)
        if started_monotonic_ns is None:
            started_monotonic_ns = execution_index * 10
        start_result = ledger.start_slot(
            execution_index,
            started_monotonic_ns=started_monotonic_ns,
            recorded_at_utc=f"2026-08-04T14:01:{execution_index % 60:02d}Z",
        )
        if not start_result.accepted:
            raise AssertionError(f"start_slot unexpectedly refused: {start_result.reason_code}")
        observation = self._observation(
            execution_index, started_monotonic_ns=started_monotonic_ns, **kwargs
        )
        return ledger.complete_slot(
            observation, started_record_sha256=start_result.start_sha256
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
            first = self._start_and_complete(ledger, 0)
            second = self._start_and_complete(ledger, 1)
            # A replay of the same already-closed index, even with a stale
            # digest, is refused before it can touch the ledger at all: this
            # exact ledger instance's one-time start session for index 1 was
            # already consumed by `second`.
            duplicate = ledger.complete_slot(
                self._observation(1), started_record_sha256=("0" * 64)
            )
            restarted = Stage4ExecutionLedger(
                root / "private" / "stage4.sqlite3",
                run_id="exp009-ledger-test-001",
                schedule=self.schedule,
            )

            self.assertTrue(first.accepted)
            self.assertTrue(second.accepted)
            self.assertFalse(duplicate.accepted)
            self.assertEqual(duplicate.reason_code, "STARTED_SESSION_MISMATCH")
            progress = restarted.progress()
            self.assertEqual(progress.status, Stage4LedgerStatus.IN_PROGRESS)
            self.assertEqual(progress.record_count, 2)
            self.assertEqual(len(restarted.records()), 2)
            self.assertEqual(restarted.records()[1].previous_record_sha256, first.record_sha256)

    def test_failed_outcome_is_recorded_then_blocks_all_later_slots(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ledger = self._ledger(Path(temporary))
            self.assertTrue(self._start_and_complete(ledger, 0).accepted)
            failed = self._start_and_complete(
                ledger,
                1,
                success=False,
                threshold_semantics_valid=False,
                reason_code="MILVUS_SEARCH_FAILED",
            )
            after_failure = ledger.start_slot(
                2, started_monotonic_ns=30, recorded_at_utc="2026-08-04T14:01:02Z"
            )

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
                    result = self._start_and_complete(ledger, 0, **changes)
                    self.assertTrue(result.accepted)
                    self.assertEqual(ledger.progress().status, Stage4LedgerStatus.FAILED)
                    blocked = ledger.start_slot(
                        1, started_monotonic_ns=30, recorded_at_utc="2026-08-04T14:01:01Z"
                    )
                    self.assertFalse(blocked.accepted)
                    self.assertEqual(blocked.reason_code, "RUN_NOT_ACTIVE")

    def test_rejects_overlapping_interval_and_schedule_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = self._ledger(root)
            self.assertTrue(
                self._start_and_complete(
                    ledger, 0, started_monotonic_ns=100, finished_monotonic_ns=110
                ).accepted
            )
            start1 = ledger.start_slot(
                1, started_monotonic_ns=110, recorded_at_utc="2026-08-04T14:01:01Z"
            )
            self.assertTrue(start1.accepted)
            overlapping = ledger.complete_slot(
                self._observation(1, started_monotonic_ns=110, finished_monotonic_ns=120),
                started_record_sha256=start1.start_sha256,
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
                result = self._start_and_complete(ledger, execution_index)
                self.assertTrue(result.accepted)
            progress = ledger.progress()

            self.assertEqual(progress.status, Stage4LedgerStatus.COMPLETE)
            self.assertEqual(progress.record_count, 1200)
            self.assertIsNone(progress.reason_code)

    def test_corrupt_storage_fails_closed_and_module_has_no_execution_imports(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = self._ledger(root)
            self.assertTrue(self._start_and_complete(ledger, 0).accepted)
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
                    connection.execute("DELETE FROM execution_starts")
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(
                        "UPDATE execution_starts SET start_json = start_json"
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

    def test_orphan_started_marker_is_ambiguous_and_blocks_start_and_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = self._ledger(root)
            self.assertTrue(self._start_and_complete(ledger, 0).accepted)
            # Slot 1 is started but the process "crashes" before completing it
            # -- start_slot succeeds, complete_slot is never called.
            orphan_start = ledger.start_slot(
                1, started_monotonic_ns=10, recorded_at_utc="2026-08-04T14:01:01Z"
            )
            self.assertTrue(orphan_start.accepted)

            progress = ledger.progress()
            self.assertEqual(progress.status, Stage4LedgerStatus.AMBIGUOUS)
            self.assertEqual(progress.reason_code, "ORPHAN_STARTED_NO_TERMINAL")
            self.assertEqual(progress.chain_head_sha256, orphan_start.start_sha256)
            self.assertEqual(progress.record_count, 1)

            # No new slot may be started while the ledger is ambiguous.
            blocked_start = ledger.start_slot(
                2, started_monotonic_ns=20, recorded_at_utc="2026-08-04T14:01:02Z"
            )
            self.assertFalse(blocked_start.accepted)
            self.assertEqual(blocked_start.reason_code, "LEDGER_AMBIGUOUS_ORPHAN_START")

    def test_same_instance_may_complete_the_slot_it_just_started(self) -> None:
        """The normal, non-crash lifecycle: one instance starts then
        completes its own slot without ever being refused as ambiguous."""

        with tempfile.TemporaryDirectory() as temporary:
            ledger = self._ledger(Path(temporary))
            start = ledger.start_slot(
                0, started_monotonic_ns=5, recorded_at_utc="2026-08-04T14:01:00Z"
            )
            self.assertTrue(start.accepted)
            completed = ledger.complete_slot(
                self._observation(0, started_monotonic_ns=5, finished_monotonic_ns=8),
                started_record_sha256=start.start_sha256,
            )
            self.assertTrue(completed.accepted)
            self.assertEqual(ledger.progress().status, Stage4LedgerStatus.IN_PROGRESS)

    def test_fresh_instance_cannot_resolve_an_orphan_even_with_the_correct_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = self._ledger(root)
            self.assertTrue(self._start_and_complete(ledger, 0).accepted)
            orphan_start = ledger.start_slot(
                1, started_monotonic_ns=10, recorded_at_utc="2026-08-04T14:01:01Z"
            )
            self.assertTrue(orphan_start.accepted)
            # Simulate process death: the in-memory session that owns this
            # start is gone, only the durable digest remains on disk.
            del ledger

            reopened = Stage4ExecutionLedger(
                root / "private" / "stage4.sqlite3",
                run_id="exp009-ledger-test-001",
                schedule=self.schedule,
            )
            self.assertEqual(reopened.progress().status, Stage4LedgerStatus.AMBIGUOUS)

            # Attempting to close it out as a success, using the exact
            # correct started_record_sha256, must still be refused: a fresh
            # instance never owns the session, regardless of digest
            # correctness.
            success_attempt = reopened.complete_slot(
                self._observation(1, started_monotonic_ns=10, finished_monotonic_ns=15),
                started_record_sha256=orphan_start.start_sha256,
            )
            self.assertFalse(success_attempt.accepted)
            self.assertEqual(success_attempt.reason_code, "STARTED_SESSION_MISMATCH")

            # Nor may it be silently converted into a FAILED terminal record
            # and "cleanly" closed out that way either.
            failure_attempt = reopened.complete_slot(
                self._observation(
                    1,
                    started_monotonic_ns=10,
                    finished_monotonic_ns=15,
                    success=False,
                    threshold_semantics_valid=False,
                    reason_code="OPERATOR_RECOVERY_MARK_FAILED",
                ),
                started_record_sha256=orphan_start.start_sha256,
            )
            self.assertFalse(failure_attempt.accepted)
            self.assertEqual(failure_attempt.reason_code, "STARTED_SESSION_MISMATCH")

            # The ledger remains exactly as ambiguous as before either attempt.
            self.assertEqual(reopened.progress().status, Stage4LedgerStatus.AMBIGUOUS)
            self.assertEqual(len(reopened.records()), 1)

    def test_restart_reconstructs_identical_start_and_terminal_history(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = self._ledger(root)
            for execution_index in range(3):
                self.assertTrue(self._start_and_complete(ledger, execution_index).accepted)

            reopened = Stage4ExecutionLedger(
                root / "private" / "stage4.sqlite3",
                run_id="exp009-ledger-test-001",
                schedule=self.schedule,
            )
            self.assertEqual(len(reopened.starts()), 3)
            self.assertEqual(len(reopened.records()), 3)
            for index, (start, record) in enumerate(
                zip(reopened.starts(), reopened.records())
            ):
                self.assertEqual(start.execution_index, index)
                self.assertEqual(record.observation.execution_index, index)
                self.assertEqual(record.started_record_sha256, start.start_sha256)

    def test_start_slot_out_of_order_and_non_monotonic_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ledger = self._ledger(Path(temporary))
            out_of_order = ledger.start_slot(
                5, started_monotonic_ns=1, recorded_at_utc="2026-08-04T14:01:00Z"
            )
            self.assertFalse(out_of_order.accepted)
            self.assertEqual(out_of_order.reason_code, "EXECUTION_INDEX_UNEXPECTED")

            self.assertTrue(self._start_and_complete(ledger, 0, started_monotonic_ns=100, finished_monotonic_ns=105).accepted)
            non_monotonic = ledger.start_slot(
                1, started_monotonic_ns=50, recorded_at_utc="2026-08-04T14:01:01Z"
            )
            self.assertFalse(non_monotonic.accepted)
            self.assertEqual(non_monotonic.reason_code, "MONOTONIC_START_VIOLATION")

    def test_complete_slot_without_any_start_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ledger = self._ledger(Path(temporary))
            result = ledger.complete_slot(
                self._observation(0), started_record_sha256=("a" * 64)
            )
            self.assertFalse(result.accepted)
            self.assertEqual(result.reason_code, "STARTED_SESSION_MISMATCH")


if __name__ == "__main__":
    unittest.main()
