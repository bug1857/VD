"""Integration tests for Checkpoint C SQLite evaluation ledger and lifecycle finalization."""

import os
import shutil
import sqlite3
import stat
import tempfile
import threading
import unittest
from dataclasses import replace
from unittest import mock

from vdbench.artifacts import canonical_json_bytes
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.lkg_phase2_readiness_ledger import Phase2ReadinessLedger
from vdbench.lkg_phase2_source_binding import (
    PHASE1_LEDGER_SCHEMA_VERSION,
    SEAL_SCHEMA_VERSION_PIN,
    SOURCE_BINDING_SCHEMA_VERSION,
    ingestion_payload_document,
    ingestion_payload_document_digest,
    lkg_window_readiness_ingestion_from_payload,
    source_binding_payload_document_digest,
)
from vdbench.lkg_qualification_evaluation import (
    EF_ELIGIBILITY_RULE_SCHEMA_VERSION,
    LkgQualificationStatus,
    ef_eligibility_rule_payload_document_digest,
    evaluation_payload_document_digest,
    lkg_ef_eligibility_rule_from_payload,
)
from vdbench.lkg_qualification_evaluation_ledger import (
    LkgQualificationEvaluationError,
    LkgQualificationEvaluationLedger,
)
from vdbench.lkg_qualification_evidence import (
    LkgAttemptStatus,
    LkgQueryAttempt,
    LkgQueryObservation,
)
from vdbench.lkg_qualification_ledger import (
    LkgQualificationLedger,
    seal_lkg_qualification_run,
    verify_seal,
)
from vdbench.lkg_qualification_seal import LkgSealCompletionState
from vdbench.lkg_run_binding import LkgRunBinding, lkg_ordered_query_ids_sha256
from vdbench.lkg_window_readiness import (
    READINESS_SCHEMA_VERSION,
    FakeLkgWindowOperationalReadinessProvider,
    lkg_window_operational_readiness_evidence_from_payload,
    readiness_payload_document,
    readiness_payload_document_digest,
)


def _expected_phase2_source_binding_digest(seal) -> str:
    """Independently recompute the expected digest using ONLY the exported
    pure functions from lkg_phase2_source_binding.py -- proves the ledger's
    derivation has no dependency on Phase2ReadinessLedger internals."""

    payload = {
        "source_binding_schema_version": SOURCE_BINDING_SCHEMA_VERSION,
        "source_run_id": seal.run_id,
        "source_run_binding_sha256": seal.run_binding_sha256,
        "source_phase1_ledger_schema_version": PHASE1_LEDGER_SCHEMA_VERSION,
        "source_seal_schema_version": SEAL_SCHEMA_VERSION_PIN,
        "source_run_seal_digest": seal.canonical_seal_document_digest,
        "source_sealed_chain_head_sha256": seal.final_chain_head_sha256,
        "workload_identity": {
            "dataset_id": seal.workload_identity.dataset_id,
            "dataset_version": seal.workload_identity.dataset_version,
            "manifest_sha256": seal.workload_identity.manifest_sha256,
            "query_role": seal.workload_identity.query_role,
        },
        "qualification_ordered_query_ids_sha256": seal.qualification_ordered_query_ids_sha256,
        "expected_query_count": seal.expected_query_count,
    }
    return source_binding_payload_document_digest(payload)


def _bad_health_builder(**kwargs):
    document = {
        "readiness_schema_version": READINESS_SCHEMA_VERSION,
        "source_run_id": kwargs["source_run_id"],
        "source_run_binding_sha256": kwargs["source_run_binding_sha256"],
        "window_index": kwargs["window_index"],
        "epoch_index": kwargs["epoch_index"],
        "first_attempt_sequence": kwargs["first_attempt_sequence"],
        "last_attempt_sequence": kwargs["last_attempt_sequence"],
        "readiness_check_id": kwargs["readiness_check_id"],
        "provider_run_id": kwargs["provider_run_id"],
        "health_checked": True,
        "health_passed": False,
        "health_evidence_source_identity": "test-health-provider",
        "health_evidence_source_digest": "a" * 64,
        "rollback_tested": True,
        "rollback_ready": True,
        "rollback_evidence_source_identity": "test-rollback-provider",
        "rollback_evidence_source_digest": "b" * 64,
        "checked_at_utc": "2026-01-01T00:00:00.000000Z",
        "check_start_ns": 0,
        "check_end_ns": 1,
        "reason_codes": ["HEALTH_CHECK_FAILED"],
    }
    return lkg_window_operational_readiness_evidence_from_payload(
        document,
        canonical_document_digest=readiness_payload_document_digest(document),
    )


class TestLkgQualificationEvaluationLedger(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.template_dir = tempfile.TemporaryDirectory()
        cls.template_p1_path = os.path.join(cls.template_dir.name, "phase1-template.db")
        cls.search_config = SearchConfiguration(
            metric=Metric.L2,
            threshold_label="target-075",
            radius=1.0,
            index_track=IndexTrack.HNSW,
            ef=200,
            limit=100,
            consistency_level="Strong",
        )
        cls.query_ids = tuple(range(1000, 3400))
        cls.query_ids_sha256 = lkg_ordered_query_ids_sha256(cls.query_ids)
        cls.run_binding = LkgRunBinding(
            run_id="run-001",
            producer_identity="producer-v1",
            search_configuration=cls.search_config,
            collection_name="lkg_collection",
            base_data_identity="base-v1",
            index_identity="index-v1",
            qualification_dataset_id="DATASET-003",
            qualification_dataset_version="DATASET-003-v1",
            qualification_manifest_sha256="a" * 64,
            qualification_query_role="lkg_qualification",
            qualification_query_id_array_sha256="b" * 64,
            qualification_ordered_query_ids_sha256=cls.query_ids_sha256,
            qualification_query_array_sha256="c" * 64,
            qualification_expected_query_count=2400,
            environment_identity="env-v1",
            source_revision="rev-v1",
        )
        template_ledger = LkgQualificationLedger(
            cls.template_p1_path,
            run_binding=cls.run_binding,
            ordered_query_ids=cls.query_ids,
            lock_timeout_seconds=10.0,
        )
        for sequence, query_id in enumerate(cls.query_ids):
            obs = LkgQueryObservation(
                query_id=query_id,
                metric=Metric.L2,
                threshold_stratum="target-075",
                ef=200,
                recall=0.96,
                latency_ms=5.0,
                start_ns=0,
                end_ns=1,
                exact_cardinality=10,
                threshold_violation_count=0,
            )
            att = LkgQueryAttempt(
                query_id=query_id,
                attempt_sequence=sequence,
                attempt_number=1,
                status=LkgAttemptStatus.SUCCESS,
                error_code=None,
                run_binding_sha256=cls.run_binding.sha256,
                observation=obs,
            )
            result = template_ledger.append(att)
            if not result.accepted:
                raise AssertionError(f"template append failed at sequence {sequence}")
        cls.template_seal = seal_lkg_qualification_run(
            template_ledger,
            seal_reason="RUN_COMPLETE",
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
        )

    @classmethod
    def tearDownClass(cls):
        cls.template_dir.cleanup()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.c_db_path = os.path.join(self.temp_dir.name, "c_evaluation.db")
        self.p1_db_path = os.path.join(self.temp_dir.name, "phase1.db")
        self.p2_db_path = os.path.join(self.temp_dir.name, "phase2_readiness.db")
        shutil.copy2(self.template_p1_path, self.p1_db_path)
        self.p1_ledger = LkgQualificationLedger(
            self.p1_db_path,
            run_binding=self.run_binding,
            ordered_query_ids=self.query_ids,
            lock_timeout_seconds=10.0,
        )
        self.p1_seal = verify_seal(self.p1_ledger)
        self.p2_ledger = Phase2ReadinessLedger(
            self.p2_db_path, phase1_ledger=self.p1_ledger, lock_timeout_seconds=10.0
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def _ingest_windows(self, window_indexes=range(12), *, provider=None):
        provider = provider or FakeLkgWindowOperationalReadinessProvider()
        for window_index in window_indexes:
            check_id = f"check-{window_index}"
            provider.capture_or_return(
                readiness_check_id=check_id,
                source_run_id="run-001",
                source_run_binding_sha256=self.run_binding.sha256,
                window_index=window_index,
                epoch_index=window_index // 6,
                first_attempt_sequence=window_index * 200,
                last_attempt_sequence=window_index * 200 + 199,
            )
            self.p2_ledger.ingest_window_readiness(
                provider=provider,
                readiness_check_id=check_id,
                window_index=window_index,
            )

    def _evaluate(self, ledger, **overrides):
        kwargs = {
            "phase1_ledger": self.p1_ledger,
            "phase2_readiness_ledger": self.p2_ledger,
            "evaluator_identity": "evaluator-1",
            "evaluator_source_revision": "rev-1",
            "evaluated_at_utc": "2026-01-01T00:02:00.000000Z",
        }
        kwargs.update(overrides)
        return ledger.evaluate_and_finalize(**kwargs)

    def _new_c_ledger(self):
        return LkgQualificationEvaluationLedger(
            self.c_db_path,
            phase1_ledger_path=self.p1_db_path,
            phase2_readiness_ledger_path=self.p2_db_path,
        )

    def _finalize_passing(self):
        self._ingest_windows()
        ledger = self._new_c_ledger()
        evaluation = self._evaluate(ledger)
        self.assertEqual(evaluation.status, LkgQualificationStatus.PASSING)
        return ledger, evaluation

    @staticmethod
    def _drop_update_trigger(connection):
        connection.execute("DROP TRIGGER trg_no_update_final_eval")

    @staticmethod
    def _restore_update_trigger(connection):
        connection.execute(
            """
            CREATE TRIGGER trg_no_update_final_eval
            BEFORE UPDATE ON lkg_qualification_final_evaluation
            BEGIN
                SELECT RAISE(ABORT, 'LKG qualification final evaluation ledger rows are append-only and immutable');
            END;
            """
        )

    def test_alias_and_symlink_rejection(self):
        # Direct path aliasing rejection
        with self.assertRaises(LkgQualificationEvaluationError):
            LkgQualificationEvaluationLedger(
                self.p1_db_path,
                phase1_ledger_path=self.p1_db_path,
                phase2_readiness_ledger_path=self.p2_db_path,
            )
        with self.assertRaises(LkgQualificationEvaluationError):
            LkgQualificationEvaluationLedger(
                self.p2_db_path,
                phase1_ledger_path=self.p1_db_path,
                phase2_readiness_ledger_path=self.p2_db_path,
            )

        # Symlink path rejection
        symlink_path = os.path.join(self.temp_dir.name, "sym_c.db")
        try:
            os.symlink(self.c_db_path, symlink_path)
            with self.assertRaises(LkgQualificationEvaluationError) as ctx:
                LkgQualificationEvaluationLedger(
                    symlink_path,
                    phase1_ledger_path=self.p1_db_path,
                    phase2_readiness_ledger_path=self.p2_db_path,
                )
            self.assertEqual(ctx.exception.code, "LKG_QUAL_EVAL_PATH_ALIAS_REJECTED")
        except OSError as exc:
            self.skipTest(f"platform cannot create symlinks: {exc}")

        real_parent = os.path.join(self.temp_dir.name, "real-parent")
        linked_parent = os.path.join(self.temp_dir.name, "linked-parent")
        os.mkdir(real_parent)
        os.symlink(real_parent, linked_parent)
        with self.assertRaises(LkgQualificationEvaluationError) as parent_caught:
            LkgQualificationEvaluationLedger(
                os.path.join(linked_parent, "evaluation.db"),
                phase1_ledger_path=self.p1_db_path,
                phase2_readiness_ledger_path=self.p2_db_path,
            )
        self.assertEqual(
            parent_caught.exception.code, "LKG_QUAL_EVAL_PATH_ALIAS_REJECTED"
        )

    def test_existing_nonregular_database_path_is_rejected(self):
        directory_path = os.path.join(self.temp_dir.name, "not-a-database-file")
        os.mkdir(directory_path)
        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            LkgQualificationEvaluationLedger(
                directory_path,
                phase1_ledger_path=self.p1_db_path,
                phase2_readiness_ledger_path=self.p2_db_path,
            )
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_INVALID_PATH")

    def test_wrong_user_version_rejection(self):
        bad_db = os.path.join(self.temp_dir.name, "bad_version.db")
        conn = sqlite3.connect(bad_db)
        conn.execute("PRAGMA user_version = 99")
        conn.close()
        with self.assertRaises(LkgQualificationEvaluationError):
            LkgQualificationEvaluationLedger(
                bad_db,
                phase1_ledger_path=self.p1_db_path,
                phase2_readiness_ledger_path=self.p2_db_path,
            )

    def test_upstream_paths_are_required_before_schema_initialization(self):
        uninitialized = os.path.join(self.temp_dir.name, "must-not-initialize.db")
        with self.assertRaises(TypeError):
            LkgQualificationEvaluationLedger(uninitialized)
        self.assertFalse(os.path.exists(uninitialized))

    def test_evaluation_error_is_runtime_error(self):
        self.assertTrue(issubclass(LkgQualificationEvaluationError, RuntimeError))

    def test_correct_schema_inventory_pragmas_and_private_mode(self):
        ledger = self._new_c_ledger()
        ledger.close()
        connection = sqlite3.connect(self.c_db_path)
        try:
            objects = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type IN ('table', 'trigger') AND name NOT LIKE 'sqlite_%'"
                )
            }
            self.assertEqual(
                objects,
                {
                    "lkg_qualification_final_evaluation",
                    "trg_single_row_final_eval",
                    "trg_no_update_final_eval",
                    "trg_no_delete_final_eval",
                },
            )
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)
            self.assertEqual(
                str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
                "delete",
            )
        finally:
            connection.close()
        self.assertEqual(stat.S_IMODE(os.stat(self.c_db_path).st_mode), 0o600)

    def test_sql_rejects_inconsistent_status_qualified_pairs(self):
        ledger = self._new_c_ledger()
        ledger.close()
        statement = """
            INSERT INTO lkg_qualification_final_evaluation (
                source_run_id, evaluation_schema_version, status, qualified,
                evaluated_ef, evaluation_contract_digest,
                ef_eligibility_rule_digest,
                qualification_semantics_rule_digest,
                source_run_binding_sha256, source_run_seal_digest,
                source_sealed_phase1_chain_head_sha256,
                phase2_source_binding_digest, canonical_evaluation_digest,
                evaluation_document_json, evaluator_identity,
                evaluator_source_revision, evaluated_at_utc
            ) VALUES (?, 1, ?, ?, 200, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?, ?)
        """
        connection = sqlite3.connect(self.c_db_path)
        try:
            for status, qualified in (
                ("PASSING", 0),
                ("FAILING", 1),
                ("INCOMPLETE", 1),
            ):
                values = (
                    f"run-{status.lower()}",
                    status,
                    qualified,
                    *("a" * 64 for _ in range(8)),
                    "evaluator",
                    "revision",
                    "2026-01-01T00:00:00Z",
                )
                with self.subTest(status=status, qualified=qualified):
                    with self.assertRaises(sqlite3.IntegrityError):
                        connection.execute(statement, values)
                    connection.rollback()
        finally:
            connection.close()

    def test_unexpected_schema_object_rejected(self):
        ledger = self._new_c_ledger()
        ledger.close()
        connection = sqlite3.connect(self.c_db_path)
        connection.execute("CREATE TABLE unexpected_table(value INTEGER)")
        connection.commit()
        connection.close()
        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            self._new_c_ledger()
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_INVALID_SCHEMA")

    def test_same_name_trigger_with_wrong_definition_is_rejected(self):
        ledger = self._new_c_ledger()
        ledger.close()
        connection = sqlite3.connect(self.c_db_path)
        connection.execute("DROP TRIGGER trg_no_update_final_eval")
        connection.execute(
            """
            CREATE TRIGGER trg_no_update_final_eval
            BEFORE UPDATE ON lkg_qualification_final_evaluation
            BEGIN
                SELECT 1;
            END;
            """
        )
        connection.commit()
        connection.close()

        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            self._new_c_ledger()
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_INVALID_SCHEMA")

    def test_same_columns_with_weakened_table_constraint_are_rejected(self):
        ledger = self._new_c_ledger()
        ledger.close()
        connection = sqlite3.connect(self.c_db_path)
        table_sql = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='lkg_qualification_final_evaluation'"
        ).fetchone()[0]
        weakened_table_sql = table_sql.replace(
            "CHECK (evaluated_ef > 0)", "CHECK (evaluated_ef >= 0)"
        )
        self.assertNotEqual(weakened_table_sql, table_sql)
        trigger_rows = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
        for trigger_name, _ in trigger_rows:
            connection.execute(f'DROP TRIGGER "{trigger_name}"')
        connection.execute("DROP TABLE lkg_qualification_final_evaluation")
        connection.execute(weakened_table_sql)
        for _, trigger_sql in trigger_rows:
            connection.execute(trigger_sql)
        connection.commit()
        connection.close()

        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            self._new_c_ledger()
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_INVALID_SCHEMA")

    def test_case_changed_check_literals_are_rejected(self):
        ledger = self._new_c_ledger()
        ledger.close()
        connection = sqlite3.connect(self.c_db_path)
        table_sql = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='lkg_qualification_final_evaluation'"
        ).fetchone()[0]
        changed_literal_sql = table_sql.replace("'PASSING'", "'passing'")
        self.assertNotEqual(changed_literal_sql, table_sql)
        trigger_rows = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
        for trigger_name, _ in trigger_rows:
            connection.execute(f'DROP TRIGGER "{trigger_name}"')
        connection.execute("DROP TABLE lkg_qualification_final_evaluation")
        connection.execute(changed_literal_sql)
        for _, trigger_sql in trigger_rows:
            connection.execute(trigger_sql)
        connection.commit()
        connection.close()

        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            self._new_c_ledger()
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_INVALID_SCHEMA")

    def test_hardlink_alias_to_upstream_rejected(self):
        alias = os.path.join(self.temp_dir.name, "phase1-hardlink.db")
        os.link(self.p1_db_path, alias)
        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            LkgQualificationEvaluationLedger(
                alias,
                phase1_ledger_path=self.p1_db_path,
                phase2_readiness_ledger_path=self.p2_db_path,
            )
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_PATH_ALIAS_REJECTED")

    def test_phase2_sql_rejects_duplicate_and_window_index_12(self):
        self._ingest_windows(range(1))
        connection = sqlite3.connect(self.p2_db_path)
        try:
            columns = [
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(window_readiness_ingestion)"
                )
                if row[1] != "insertion_seq"
            ]
            row = list(
                connection.execute(
                    f"SELECT {','.join(columns)} FROM window_readiness_ingestion"
                ).fetchone()
            )
            placeholders = ",".join("?" for _ in columns)
            statement = (
                f"INSERT INTO window_readiness_ingestion ({','.join(columns)}) "
                f"VALUES ({placeholders})"
            )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(statement, row)
            connection.rollback()

            row[columns.index("window_index")] = 12
            row[columns.index("epoch_index")] = 2
            row[columns.index("readiness_check_id")] = "illegal-window-12"
            row[columns.index("canonical_ingestion_digest")] = "1" * 64
            row[columns.index("chain_sha256")] = "2" * 64
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(statement, row)
        finally:
            connection.rollback()
            connection.close()

    def test_transient_incomplete_writes_zero_rows(self):
        provider = FakeLkgWindowOperationalReadinessProvider()
        provider.capture_or_return(
            readiness_check_id="check-0",
            source_run_id="run-001",
            source_run_binding_sha256=self.run_binding.sha256,
            window_index=0,
            epoch_index=0,
            first_attempt_sequence=0,
            last_attempt_sequence=199,
        )
        self.p2_ledger.ingest_window_readiness(
            provider=provider, readiness_check_id="check-0", window_index=0
        )

        with LkgQualificationEvaluationLedger(
            self.c_db_path,
            phase1_ledger_path=self.p1_db_path,
            phase2_readiness_ledger_path=self.p2_db_path,
        ) as c_ledger:
            eval_res = c_ledger.evaluate_and_finalize(
                phase1_ledger=self.p1_ledger,
                phase2_readiness_ledger=self.p2_ledger,
                evaluator_identity="evaluator-1",
                evaluator_source_revision="rev-1",
                evaluated_at_utc="2026-01-01T00:02:00.000000Z",
            )
            self.assertEqual(eval_res.status, LkgQualificationStatus.INCOMPLETE)
            self.assertIn(
                "AWAITING_READINESS_EVIDENCE", eval_res.status_reason_codes
            )
            self.assertNotIn(
                "PHASE1_POSITION_PERMANENTLY_MISSING",
                eval_res.status_reason_codes,
            )
            self.assertIsNone(c_ledger.get_final_evaluation())

    def test_adversarial_missing_position_with_12_readiness_is_terminal_incomplete_and_restart_safe(self):
        p1_path = os.path.join(self.temp_dir.name, "missing-phase1.db")
        p2_path = os.path.join(self.temp_dir.name, "missing-phase2.db")
        c_path = os.path.join(self.temp_dir.name, "missing-c.db")
        p1 = LkgQualificationLedger(
            p1_path,
            run_binding=self.run_binding,
            ordered_query_ids=self.query_ids,
            lock_timeout_seconds=10.0,
        )
        seal_lkg_qualification_run(
            p1,
            seal_reason="RUN_INCOMPLETE",
            expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
        )
        p2 = Phase2ReadinessLedger(p2_path, phase1_ledger=p1, lock_timeout_seconds=10.0)
        provider = FakeLkgWindowOperationalReadinessProvider()
        for window_index in range(12):
            check_id = f"missing-check-{window_index}"
            provider.capture_or_return(
                readiness_check_id=check_id,
                source_run_id="run-001",
                source_run_binding_sha256=self.run_binding.sha256,
                window_index=window_index,
                epoch_index=window_index // 6,
                first_attempt_sequence=window_index * 200,
                last_attempt_sequence=window_index * 200 + 199,
            )
            p2.ingest_window_readiness(
                provider=provider,
                readiness_check_id=check_id,
                window_index=window_index,
            )
        ledger = LkgQualificationEvaluationLedger(
            c_path, phase1_ledger_path=p1_path, phase2_readiness_ledger_path=p2_path
        )
        original = ledger.evaluate_and_finalize(
            phase1_ledger=p1,
            phase2_readiness_ledger=p2,
            evaluator_identity="evaluator-1",
            evaluator_source_revision="rev-1",
            evaluated_at_utc="2026-01-01T00:02:00.000000Z",
        )
        self.assertEqual(original.status, LkgQualificationStatus.INCOMPLETE)
        self.assertIn(
            "PHASE1_POSITION_PERMANENTLY_MISSING",
            original.status_reason_codes,
        )
        self.assertNotIn(
            "AWAITING_READINESS_EVIDENCE", original.status_reason_codes
        )
        self.assertIsNotNone(ledger.get_final_evaluation())
        ledger.close()
        reopened = LkgQualificationEvaluationLedger(
            c_path, phase1_ledger_path=p1_path, phase2_readiness_ledger_path=p2_path
        )
        try:
            replayed = reopened.evaluate_and_finalize(
                phase1_ledger=p1,
                phase2_readiness_ledger=p2,
                evaluator_identity="other",
                evaluator_source_revision="other",
                evaluated_at_utc="2026-01-02T00:00:00.000000Z",
            )
            self.assertEqual(replayed, original)
        finally:
            reopened.close()

    def test_terminal_passing_and_replay(self):
        provider = FakeLkgWindowOperationalReadinessProvider()
        for w in range(12):
            check_id = f"check-{w}"
            provider.capture_or_return(
                readiness_check_id=check_id,
                source_run_id="run-001",
                source_run_binding_sha256=self.run_binding.sha256,
                window_index=w,
                epoch_index=w // 6,
                first_attempt_sequence=w * 200,
                last_attempt_sequence=w * 200 + 199,
            )
            self.p2_ledger.ingest_window_readiness(
                provider=provider, readiness_check_id=check_id, window_index=w
            )

        with LkgQualificationEvaluationLedger(
            self.c_db_path,
            phase1_ledger_path=self.p1_db_path,
            phase2_readiness_ledger_path=self.p2_db_path,
        ) as c_ledger:
            eval1 = c_ledger.evaluate_and_finalize(
                phase1_ledger=self.p1_ledger,
                phase2_readiness_ledger=self.p2_ledger,
                evaluator_identity="evaluator-1",
                evaluator_source_revision="rev-1",
                evaluated_at_utc="2026-01-01T00:02:00.000000Z",
            )
            self.assertEqual(eval1.status, LkgQualificationStatus.PASSING)
            self.assertTrue(eval1.qualified)

            eval2 = c_ledger.evaluate_and_finalize(
                phase1_ledger=self.p1_ledger,
                phase2_readiness_ledger=self.p2_ledger,
                evaluator_identity="evaluator-2",
                evaluator_source_revision="rev-2",
                evaluated_at_utc="2026-01-01T09:00:00.000000Z",
            )
            self.assertEqual(eval2, eval1)
            self.assertEqual(eval2.evaluated_at_utc, "2026-01-01T00:02:00.000000Z")

    def test_terminal_passing_survives_fresh_ledger_restart(self):
        ledger, original = self._finalize_passing()
        ledger.close()
        reopened = self._new_c_ledger()
        try:
            replayed = self._evaluate(
                reopened,
                evaluator_identity="different-evaluator",
                evaluator_source_revision="different-revision",
                evaluated_at_utc="2026-01-02T00:00:00.000000Z",
            )
            self.assertEqual(replayed, original)
        finally:
            reopened.close()

    def test_final_row_update_delete_and_second_insert_are_rejected(self):
        ledger, _ = self._finalize_passing()
        ledger.close()
        connection = sqlite3.connect(self.c_db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE lkg_qualification_final_evaluation SET status='FAILING'"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM lkg_qualification_final_evaluation")
            columns = [
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(lkg_qualification_final_evaluation)"
                )
            ]
            row = list(
                connection.execute(
                    "SELECT * FROM lkg_qualification_final_evaluation"
                ).fetchone()
            )
            row[columns.index("source_run_id")] = "second-run"
            placeholders = ",".join("?" for _ in columns)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    f"INSERT INTO lkg_qualification_final_evaluation "
                    f"({','.join(columns)}) VALUES ({placeholders})",
                    row,
                )
        finally:
            connection.rollback()
            connection.close()

    def test_phase2_source_binding_digest_reproducible_without_private_upstream_access(self):
        """Proves the ledger's phase2_source_binding_digest derivation has
        no dependency on Phase2ReadinessLedger internals -- it must equal a
        digest independently recomputed using only the exported pure
        functions from lkg_phase2_source_binding.py."""

        provider = FakeLkgWindowOperationalReadinessProvider()
        for w in range(12):
            check_id = f"check-{w}"
            provider.capture_or_return(
                readiness_check_id=check_id,
                source_run_id="run-001",
                source_run_binding_sha256=self.run_binding.sha256,
                window_index=w,
                epoch_index=w // 6,
                first_attempt_sequence=w * 200,
                last_attempt_sequence=w * 200 + 199,
            )
            self.p2_ledger.ingest_window_readiness(
                provider=provider, readiness_check_id=check_id, window_index=w
            )

        with LkgQualificationEvaluationLedger(
            self.c_db_path,
            phase1_ledger_path=self.p1_db_path,
            phase2_readiness_ledger_path=self.p2_db_path,
        ) as c_ledger:
            evaluation = c_ledger.evaluate_and_finalize(
                phase1_ledger=self.p1_ledger,
                phase2_readiness_ledger=self.p2_ledger,
                evaluator_identity="evaluator-1",
                evaluator_source_revision="rev-1",
                evaluated_at_utc="2026-01-01T00:02:00.000000Z",
            )

        seal = verify_seal(self.p1_ledger)
        expected_digest = _expected_phase2_source_binding_digest(seal)
        self.assertEqual(evaluation.phase2_source_binding_digest, expected_digest)

    def test_unexpected_exception_propagates_not_masked_as_finalization_failed(self):
        """Proves the finalization exception boundary only translates
        expected failure categories -- an unexpected exception (a
        programming bug, simulated here) must propagate as itself, not be
        silently wrapped as LKG_QUAL_EVAL_FINALIZATION_FAILED."""

        with LkgQualificationEvaluationLedger(
            self.c_db_path,
            phase1_ledger_path=self.p1_db_path,
            phase2_readiness_ledger_path=self.p2_db_path,
        ) as c_ledger:
            with mock.patch.object(
                LkgQualificationLedger,
                "stored_ordered_query_ids",
                side_effect=TypeError("simulated programming bug"),
            ), self.assertRaises(TypeError):
                c_ledger.evaluate_and_finalize(
                    phase1_ledger=self.p1_ledger,
                    phase2_readiness_ledger=self.p2_ledger,
                    evaluator_identity="evaluator-1",
                    evaluator_source_revision="rev-1",
                    evaluated_at_utc="2026-01-01T00:02:00.000000Z",
                )
            # The ledger must remain usable afterward -- the rollback in the
            # exception boundary must still have released the transaction.
            self.assertIsNone(c_ledger.get_final_evaluation())

    def test_begin_immediate_storage_failure_uses_stable_error_boundary(self):
        ledger = self._new_c_ledger()
        real_connection = ledger._conn
        failing_connection = mock.Mock()
        failing_connection.execute.side_effect = sqlite3.OperationalError("busy")
        ledger._conn = failing_connection
        try:
            with self.assertRaises(LkgQualificationEvaluationError) as caught:
                self._evaluate(ledger)
            self.assertEqual(
                caught.exception.code, "LKG_QUAL_EVAL_FINALIZATION_FAILED"
            )
            self.assertIsInstance(
                caught.exception.__cause__, sqlite3.OperationalError
            )
            self.assertEqual(
                failing_connection.execute.call_args_list[0],
                mock.call("BEGIN IMMEDIATE;"),
            )
            self.assertIn(
                mock.call("ROLLBACK;"), failing_connection.execute.call_args_list
            )
        finally:
            ledger._conn = real_connection
            ledger.close()

    def test_early_terminal_failing_and_later_appends(self):
        bad_provider = FakeLkgWindowOperationalReadinessProvider(builder=_bad_health_builder)
        bad_provider.capture_or_return(
            readiness_check_id="check-bad-0",
            source_run_id="run-001",
            source_run_binding_sha256=self.run_binding.sha256,
            window_index=0,
            epoch_index=0,
            first_attempt_sequence=0,
            last_attempt_sequence=199,
        )
        self.p2_ledger.ingest_window_readiness(
            provider=bad_provider, readiness_check_id="check-bad-0", window_index=0
        )

        with LkgQualificationEvaluationLedger(
            self.c_db_path,
            phase1_ledger_path=self.p1_db_path,
            phase2_readiness_ledger_path=self.p2_db_path,
        ) as c_ledger:
            eval1 = c_ledger.evaluate_and_finalize(
                phase1_ledger=self.p1_ledger,
                phase2_readiness_ledger=self.p2_ledger,
                evaluator_identity="evaluator-1",
                evaluator_source_revision="rev-1",
                evaluated_at_utc="2026-01-01T00:02:00.000000Z",
            )
            self.assertEqual(eval1.status, LkgQualificationStatus.FAILING)
            self.assertFalse(eval1.qualified)

            provider = FakeLkgWindowOperationalReadinessProvider()
            provider.capture_or_return(
                readiness_check_id="check-1",
                source_run_id="run-001",
                source_run_binding_sha256=self.run_binding.sha256,
                window_index=1,
                epoch_index=0,
                first_attempt_sequence=200,
                last_attempt_sequence=399,
            )
            self.p2_ledger.ingest_window_readiness(
                provider=provider, readiness_check_id="check-1", window_index=1
            )

            eval2 = c_ledger.evaluate_and_finalize(
                phase1_ledger=self.p1_ledger,
                phase2_readiness_ledger=self.p2_ledger,
                evaluator_identity="evaluator-2",
                evaluator_source_revision="rev-2",
                evaluated_at_utc="2026-01-01T05:00:00.000000Z",
            )
            self.assertEqual(eval2, eval1)

    def test_phase1_tamper_on_terminal_replay_fails_with_cause(self):
        import json

        ledger, _ = self._finalize_passing()
        connection = sqlite3.connect(self.p1_db_path)
        connection.execute("DROP TRIGGER lkg_qualification_attempts_no_update")
        document = json.loads(connection.execute(
            "SELECT document_json FROM lkg_qualification_attempts ORDER BY insertion_seq LIMIT 1"
        ).fetchone()[0])
        document["observation"]["latency_ms"] = 6.0
        connection.execute(
            "UPDATE lkg_qualification_attempts SET document_json=? "
            "WHERE insertion_seq=(SELECT MIN(insertion_seq) FROM lkg_qualification_attempts)",
            (canonical_json_bytes(document).decode("utf-8"),),
        )
        connection.execute(
            """
            CREATE TRIGGER lkg_qualification_attempts_no_update
            BEFORE UPDATE ON lkg_qualification_attempts
            BEGIN SELECT RAISE(ABORT, 'lkg qualification attempts are append-only'); END
            """
        )
        connection.commit()
        connection.close()
        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            self._evaluate(ledger)
        self.assertIsNotNone(caught.exception.__cause__)
        ledger.close()

    def test_phase2_tamper_on_terminal_replay_fails_with_cause(self):
        ledger, _ = self._finalize_passing()
        connection = sqlite3.connect(self.p2_db_path)
        connection.execute("DROP TRIGGER window_readiness_ingestion_no_update")
        document = connection.execute(
            "SELECT ingestion_document_json FROM window_readiness_ingestion "
            "ORDER BY insertion_seq LIMIT 1"
        ).fetchone()[0]
        connection.execute(
            "UPDATE window_readiness_ingestion SET ingestion_document_json=? "
            "WHERE insertion_seq=(SELECT MIN(insertion_seq) FROM window_readiness_ingestion)",
            (" " + document,),
        )
        connection.execute(
            """
            CREATE TRIGGER window_readiness_ingestion_no_update
            BEFORE UPDATE ON window_readiness_ingestion
            BEGIN SELECT RAISE(ABORT, 'window readiness ingestion is append-only'); END
            """
        )
        connection.commit()
        connection.close()
        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            self._evaluate(ledger)
        self.assertIsNotNone(caught.exception.__cause__)
        ledger.close()

    def test_frozen_readiness_digest_mismatch_rejected_on_replay(self):
        bad_provider = FakeLkgWindowOperationalReadinessProvider(builder=_bad_health_builder)
        bad_provider.capture_or_return(
            readiness_check_id="check-bad-0",
            source_run_id="run-001",
            source_run_binding_sha256=self.run_binding.sha256,
            window_index=0,
            epoch_index=0,
            first_attempt_sequence=0,
            last_attempt_sequence=199,
        )
        self.p2_ledger.ingest_window_readiness(
            provider=bad_provider, readiness_check_id="check-bad-0", window_index=0
        )
        ledger = self._new_c_ledger()
        original = self._evaluate(ledger)
        self.assertEqual(original.status, LkgQualificationStatus.FAILING)
        ingestion = self.p2_ledger.all_verified_ingestions()[0]
        payload = ingestion_payload_document(ingestion)
        payload["ingested_at_utc"] = "2026-01-01T00:01:01.000000Z"
        alternate = lkg_window_readiness_ingestion_from_payload(
            payload,
            canonical_ingestion_digest=ingestion_payload_document_digest(payload),
        )
        with mock.patch.object(
            Phase2ReadinessLedger,
            "all_verified_ingestions",
            return_value=(alternate,),
        ), self.assertRaises(LkgQualificationEvaluationError) as caught:
            self._evaluate(ledger)
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_REPLAY_MISMATCH")
        ledger.close()

    def test_search_configuration_and_dataset_identity_mismatch_rejected(self):
        ledger, _ = self._finalize_passing()
        cases = (
            {"search_configuration": replace(self.search_config, ef=400)},
            {"qualification_dataset_version": "DATASET-003-v2"},
        )
        for changes in cases:
            values = {
                name: getattr(self.run_binding, name)
                for name in self.run_binding.__slots__
            }
            values.update(changes)
            alternate = LkgRunBinding(**values)
            with self.subTest(changes=changes):
                with mock.patch.object(
                    LkgQualificationLedger,
                    "stored_run_binding",
                    return_value=alternate,
                ), self.assertRaises(LkgQualificationEvaluationError) as caught:
                    self._evaluate(ledger)
                self.assertEqual(
                    caught.exception.code, "LKG_QUAL_EVAL_SOURCE_IDENTITY_MISMATCH"
                )
        ledger.close()

    def test_ingestion_original_evidence_run_binding_mismatch_rejected(self):
        self._ingest_windows(range(1))
        ingestion = self.p2_ledger.all_verified_ingestions()[0]
        evidence_payload = readiness_payload_document(ingestion.original_evidence)
        evidence_payload["source_run_binding_sha256"] = "f" * 64
        altered_evidence = lkg_window_operational_readiness_evidence_from_payload(
            evidence_payload,
            canonical_document_digest=readiness_payload_document_digest(
                evidence_payload
            ),
        )
        ingestion_payload = ingestion_payload_document(ingestion)
        ingestion_payload["original_evidence"] = readiness_payload_document(
            altered_evidence
        )
        ingestion_payload["original_evidence_digest"] = (
            altered_evidence.canonical_document_digest
        )
        altered_ingestion = lkg_window_readiness_ingestion_from_payload(
            ingestion_payload,
            canonical_ingestion_digest=ingestion_payload_document_digest(
                ingestion_payload
            ),
        )
        ledger = self._new_c_ledger()
        with mock.patch.object(
            Phase2ReadinessLedger,
            "all_verified_ingestions",
            return_value=(altered_ingestion,),
        ), self.assertRaises(LkgQualificationEvaluationError) as caught:
            self._evaluate(ledger)
        self.assertEqual(
            caught.exception.code, "LKG_QUAL_EVAL_SOURCE_IDENTITY_MISMATCH"
        )
        ledger.close()

    def test_replay_with_different_supported_ef_rule_is_rejected(self):
        ledger, _ = self._finalize_passing()

        alternate_ef_payload = {
            "rule_schema_version": EF_ELIGIBILITY_RULE_SCHEMA_VERSION,
            "eligible_ef_values": [200, 400, 800],
        }
        alternate_ef_rule = lkg_ef_eligibility_rule_from_payload(
            alternate_ef_payload,
            canonical_rule_digest=ef_eligibility_rule_payload_document_digest(
                alternate_ef_payload
            ),
        )
        with self.assertRaises(LkgQualificationEvaluationError) as ef_caught:
            self._evaluate(ledger, ef_rule=alternate_ef_rule)
        self.assertEqual(
            ef_caught.exception.code, "LKG_QUAL_EVAL_REPLAY_MISMATCH"
        )

        ledger.close()

    def test_sql_tamper_detection(self):
        provider = FakeLkgWindowOperationalReadinessProvider()
        for w in range(12):
            check_id = f"check-{w}"
            provider.capture_or_return(
                readiness_check_id=check_id,
                source_run_id="run-001",
                source_run_binding_sha256=self.run_binding.sha256,
                window_index=w,
                epoch_index=w // 6,
                first_attempt_sequence=w * 200,
                last_attempt_sequence=w * 200 + 199,
            )
            self.p2_ledger.ingest_window_readiness(
                provider=provider, readiness_check_id=check_id, window_index=w
            )

        c_ledger = LkgQualificationEvaluationLedger(
            self.c_db_path,
            phase1_ledger_path=self.p1_db_path,
            phase2_readiness_ledger_path=self.p2_db_path,
        )
        c_ledger.evaluate_and_finalize(
            phase1_ledger=self.p1_ledger,
            phase2_readiness_ledger=self.p2_ledger,
            evaluator_identity="evaluator-1",
            evaluator_source_revision="rev-1",
            evaluated_at_utc="2026-01-01T00:02:00.000000Z",
        )
        c_ledger.close()

        # Controlled raw SQL tampering test: Explicitly drop trigger on test DB, mutate column, and restore trigger
        conn = sqlite3.connect(self.c_db_path)
        conn.execute("DROP TRIGGER trg_no_update_final_eval;")
        conn.execute(
            "UPDATE lkg_qualification_final_evaluation SET evaluator_identity = 'tampered-hacker';"
        )
        conn.execute(
            """
            CREATE TRIGGER trg_no_update_final_eval
            BEFORE UPDATE ON lkg_qualification_final_evaluation
            BEGIN
                SELECT RAISE(ABORT, 'LKG qualification final evaluation ledger rows are append-only and immutable');
            END;
            """
        )
        conn.commit()
        conn.close()

        c_ledger2 = LkgQualificationEvaluationLedger(
            self.c_db_path,
            phase1_ledger_path=self.p1_db_path,
            phase2_readiness_ledger_path=self.p2_db_path,
        )
        with self.assertRaises(LkgQualificationEvaluationError) as ctx:
            c_ledger2.get_final_evaluation()
        self.assertEqual(ctx.exception.code, "LKG_QUAL_EVAL_FINAL_CORRUPTED")
        c_ledger2.close()

    def test_noncanonical_json_tamper_rejected(self):
        ledger, _ = self._finalize_passing()
        ledger.close()
        connection = sqlite3.connect(self.c_db_path)
        self._drop_update_trigger(connection)
        document = connection.execute(
            "SELECT evaluation_document_json FROM lkg_qualification_final_evaluation"
        ).fetchone()[0]
        connection.execute(
            "UPDATE lkg_qualification_final_evaluation SET evaluation_document_json=?",
            (" " + document,),
        )
        self._restore_update_trigger(connection)
        connection.commit()
        connection.close()
        reopened = self._new_c_ledger()
        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            reopened.get_final_evaluation()
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_FINAL_CORRUPTED")
        reopened.close()

    def test_canonical_json_payload_tamper_rejected(self):
        import json

        ledger, _ = self._finalize_passing()
        ledger.close()
        connection = sqlite3.connect(self.c_db_path)
        self._drop_update_trigger(connection)
        document = json.loads(
            connection.execute(
                "SELECT evaluation_document_json FROM lkg_qualification_final_evaluation"
            ).fetchone()[0]
        )
        document["evaluator_identity"] = "tampered-in-document"
        connection.execute(
            "UPDATE lkg_qualification_final_evaluation SET evaluation_document_json=?",
            (canonical_json_bytes(document).decode("utf-8"),),
        )
        self._restore_update_trigger(connection)
        connection.commit()
        connection.close()
        reopened = self._new_c_ledger()
        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            reopened.get_final_evaluation()
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_FINAL_CORRUPTED")
        reopened.close()

    def test_recomputed_canonical_verdict_tamper_is_rejected_by_source_replay(self):
        import json

        ledger, _ = self._finalize_passing()
        ledger.close()
        connection = sqlite3.connect(self.c_db_path)
        self._drop_update_trigger(connection)
        document = json.loads(
            connection.execute(
                "SELECT evaluation_document_json "
                "FROM lkg_qualification_final_evaluation"
            ).fetchone()[0]
        )
        for epoch_document in document["epoch_evaluations"]:
            epoch_document["observed_mean_capped_recall"] = 0.99
        forged_digest = evaluation_payload_document_digest(document)
        connection.execute(
            "UPDATE lkg_qualification_final_evaluation "
            "SET evaluation_document_json=?, canonical_evaluation_digest=?",
            (canonical_json_bytes(document).decode("utf-8"), forged_digest),
        )
        self._restore_update_trigger(connection)
        connection.commit()
        connection.close()

        reopened = self._new_c_ledger()
        # Local row reconstruction succeeds: the forged canonical document,
        # digest, and denormalized columns are mutually consistent.
        self.assertIsNotNone(reopened.get_final_evaluation())
        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            self._evaluate(reopened)
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_REPLAY_MISMATCH")
        reopened.close()

    def test_final_digest_tamper_rejected(self):
        ledger, _ = self._finalize_passing()
        ledger.close()
        connection = sqlite3.connect(self.c_db_path)
        self._drop_update_trigger(connection)
        connection.execute(
            "UPDATE lkg_qualification_final_evaluation "
            "SET canonical_evaluation_digest=?",
            ("0" * 64,),
        )
        self._restore_update_trigger(connection)
        connection.commit()
        connection.close()
        reopened = self._new_c_ledger()
        with self.assertRaises(LkgQualificationEvaluationError) as caught:
            reopened.get_final_evaluation()
        self.assertEqual(caught.exception.code, "LKG_QUAL_EVAL_FINAL_CORRUPTED")
        reopened.close()

    def test_evaluator_source_revision_and_timestamp_column_tamper_rejected(self):
        for column, value in (
            ("evaluator_source_revision", "tampered-revision"),
            ("evaluated_at_utc", "2026-01-03T00:00:00.000000Z"),
        ):
            with self.subTest(column=column):
                # Each subtest needs a fresh C file because a terminal row is immutable.
                path = os.path.join(self.temp_dir.name, f"tamper-{column}.db")
                if not self.p2_ledger.all_verified_ingestions():
                    self._ingest_windows()
                ledger = LkgQualificationEvaluationLedger(
                    path,
                    phase1_ledger_path=self.p1_db_path,
                    phase2_readiness_ledger_path=self.p2_db_path,
                )
                self._evaluate(ledger)
                ledger.close()
                connection = sqlite3.connect(path)
                self._drop_update_trigger(connection)
                connection.execute(
                    f"UPDATE lkg_qualification_final_evaluation SET {column}=?", (value,)
                )
                self._restore_update_trigger(connection)
                connection.commit()
                connection.close()
                reopened = LkgQualificationEvaluationLedger(
                    path,
                    phase1_ledger_path=self.p1_db_path,
                    phase2_readiness_ledger_path=self.p2_db_path,
                )
                with self.assertRaises(LkgQualificationEvaluationError) as caught:
                    reopened.get_final_evaluation()
                self.assertEqual(
                    caught.exception.code, "LKG_QUAL_EVAL_FINAL_CORRUPTED"
                )
                reopened.close()

    def test_concurrent_terminal_finalization(self):
        provider = FakeLkgWindowOperationalReadinessProvider()
        for w in range(12):
            check_id = f"check-{w}"
            provider.capture_or_return(
                readiness_check_id=check_id,
                source_run_id="run-001",
                source_run_binding_sha256=self.run_binding.sha256,
                window_index=w,
                epoch_index=w // 6,
                first_attempt_sequence=w * 200,
                last_attempt_sequence=w * 200 + 199,
            )
            self.p2_ledger.ingest_window_readiness(
                provider=provider, readiness_check_id=check_id, window_index=w
            )

        barrier = threading.Barrier(2)
        results = [None, None]
        errors = [None, None]

        def worker(thread_idx: int):
            try:
                p1 = LkgQualificationLedger(
                    self.p1_db_path,
                    run_binding=self.run_binding,
                    ordered_query_ids=self.query_ids,
                    lock_timeout_seconds=10.0,
                )
                p2 = Phase2ReadinessLedger(
                    self.p2_db_path,
                    phase1_ledger=p1,
                    lock_timeout_seconds=10.0,
                )
                ledger = LkgQualificationEvaluationLedger(
                    self.c_db_path,
                    phase1_ledger_path=self.p1_db_path,
                    phase2_readiness_ledger_path=self.p2_db_path,
                )
                barrier.wait(timeout=10.0)
                res = ledger.evaluate_and_finalize(
                    phase1_ledger=p1,
                    phase2_readiness_ledger=p2,
                    evaluator_identity=f"evaluator-{thread_idx}",
                    evaluator_source_revision=f"rev-{thread_idx}",
                    evaluated_at_utc="2026-01-01T00:02:00.000000Z",
                )
                results[thread_idx] = res
                ledger.close()
            except Exception as exc:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
                errors[thread_idx] = exc

        t0 = threading.Thread(target=worker, args=(0,))
        t1 = threading.Thread(target=worker, args=(1,))
        t0.start()
        t1.start()
        t0.join()
        t1.join()

        self.assertIsNone(errors[0])
        self.assertIsNone(errors[1])
        self.assertIsNotNone(results[0])
        self.assertIsNotNone(results[1])
        self.assertEqual(results[0], results[1])
        connection = sqlite3.connect(self.c_db_path)
        try:
            final_row_count = connection.execute(
                "SELECT COUNT(*) FROM lkg_qualification_final_evaluation"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(final_row_count, 1)


if __name__ == "__main__":
    unittest.main()
