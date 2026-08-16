"""TDD coverage for Checkpoint A: the Phase-1 evidence-sealing extension.

Two independent test classes:
``LkgQualificationSealTypesTests`` exercises ``lkg_qualification_seal.py``
in isolation (no SQLite, no ledger); ``LkgQualificationSealTests``
exercises ``lkg_qualification_ledger.py``'s ``seal_lkg_qualification_run``/
``verify_seal`` against a real, file-backed ledger. Fixtures are local to
this file, deliberately not imported from
``tests.test_lkg_qualification_ledger``'s underscore-prefixed helpers.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from vdbench import lkg_qualification_ledger
from vdbench.config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from vdbench.lkg_qualification_evidence import (
    LkgAttemptStatus,
    build_lkg_query_attempt,
    build_lkg_query_observation,
)
from vdbench.lkg_qualification_ledger import (
    LkgQualificationLedger,
    LkgQualificationLedgerError,
    seal_lkg_qualification_run,
    verify_seal,
)
from vdbench.lkg_qualification_seal import (
    SEAL_SCHEMA_VERSION,
    LkgPositionClassification,
    LkgPositionStatus,
    LkgRunSeal,
    LkgSealCompletionState,
    LkgSealWorkloadIdentity,
    lkg_run_seal_from_payload,
    seal_payload_document,
    seal_payload_document_digest,
)
from vdbench.lkg_run_binding import ORDERED_QUERY_IDS_DIGEST_DOMAIN, LkgRunBinding

_ORDERED_QUERY_IDS = (10, 20, 30, 40, 50)


def _ordered_query_ids_sha256(ordered_query_ids: tuple[int, ...]) -> str:
    """Independently reproduces lkg_run_binding.lkg_ordered_query_ids_sha256's
    binary contract -- deliberately not imported from the module under
    test, matching tests/test_lkg_qualification_ledger.py's own
    precedent."""

    ids = tuple(ordered_query_ids)
    payload = bytearray(ORDERED_QUERY_IDS_DIGEST_DOMAIN)
    payload += len(ids).to_bytes(8, byteorder="little", signed=True)
    for query_id in ids:
        payload += query_id.to_bytes(8, byteorder="little", signed=True)
    return hashlib.sha256(bytes(payload)).hexdigest()


_ORDERED_QUERY_IDS_SHA256 = _ordered_query_ids_sha256(_ORDERED_QUERY_IDS)


def _configuration(**overrides) -> SearchConfiguration:
    fields = {
        "metric": Metric.L2,
        "threshold_label": "target-075",
        "radius": 5.0,
        "index_track": IndexTrack.HNSW,
        "ef": 400,
    }
    fields.update(overrides)
    return SearchConfiguration(**fields)


def _binding(**overrides) -> LkgRunBinding:
    fields = {
        "run_id": "run-1",
        "producer_identity": "producer-v1",
        "search_configuration": _configuration(),
        "collection_name": "lkg_l2_hnsw",
        "base_data_identity": "data-v1",
        "index_identity": "index-v1",
        "qualification_dataset_id": "DATASET-003",
        "qualification_dataset_version": "DATASET-003-v1",
        "qualification_manifest_sha256": "a" * 64,
        "qualification_query_role": "lkg_qualification",
        "qualification_query_id_array_sha256": "b" * 64,
        "qualification_ordered_query_ids_sha256": _ORDERED_QUERY_IDS_SHA256,
        "qualification_query_array_sha256": "c" * 64,
        "qualification_expected_query_count": len(_ORDERED_QUERY_IDS),
        "environment_identity": "env-v1",
        "source_revision": "deadbeef",
    }
    fields.update(overrides)
    return LkgRunBinding(**fields)


_BINDING = _binding()


def _success_attempt(query_id, attempt_sequence, attempt_number=1, ef=400, binding=_BINDING, start_ns=1_000, end_ns=2_000):
    observation = build_lkg_query_observation(
        query_id=query_id,
        metric=Metric.L2,
        threshold_stratum="target-075",
        ef=ef,
        recall=1.0,
        latency_ms=1.2,
        start_ns=start_ns,
        end_ns=end_ns,
        exact_cardinality=10,
        threshold_violation_count=0,
    )
    return build_lkg_query_attempt(
        query_id=query_id,
        attempt_sequence=attempt_sequence,
        attempt_number=attempt_number,
        status=LkgAttemptStatus.SUCCESS,
        run_binding_sha256=binding.sha256,
        observation=observation,
    )


def _failure_attempt(
    query_id,
    attempt_sequence,
    attempt_number=1,
    status=LkgAttemptStatus.TIMEOUT,
    error_code="TIMEOUT",
    binding=_BINDING,
):
    return build_lkg_query_attempt(
        query_id=query_id,
        attempt_sequence=attempt_sequence,
        attempt_number=attempt_number,
        status=status,
        run_binding_sha256=binding.sha256,
        error_code=error_code,
    )


def _position(
    attempt_sequence: int,
    query_id: int,
    classification: LkgPositionStatus,
    reason_codes: tuple[str, ...],
) -> LkgPositionClassification:
    return LkgPositionClassification(
        attempt_sequence=attempt_sequence,
        query_id=query_id,
        classification=classification,
        reason_codes=reason_codes,
    )


def _all_clean_positions(ordered_query_ids: tuple[int, ...] = _ORDERED_QUERY_IDS):
    return tuple(
        _position(i, qid, LkgPositionStatus.CLEAN_SUCCESS, ())
        for i, qid in enumerate(ordered_query_ids)
    )


def _seal(**overrides) -> LkgRunSeal:
    positions = overrides.pop("position_classifications", _all_clean_positions())
    fields = {
        "seal_schema_version": SEAL_SCHEMA_VERSION,
        "run_id": "run-1",
        "run_binding_sha256": "a" * 64,
        "phase1_ledger_schema_version": 5,
        "workload_identity": LkgSealWorkloadIdentity(
            dataset_id="DATASET-003",
            dataset_version="DATASET-003-v1",
            manifest_sha256="b" * 64,
            query_role="lkg_qualification",
        ),
        "expected_query_count": len(_ORDERED_QUERY_IDS),
        "qualification_ordered_query_ids_sha256": _ORDERED_QUERY_IDS_SHA256,
        "final_chain_head_sha256": "c" * 64,
        "position_classifications": positions,
        "successful_position_count": len(_ORDERED_QUERY_IDS),
        "failed_position_count": 0,
        "malformed_position_count": 0,
        "missing_position_count": 0,
        "successful_attempt_count": len(_ORDERED_QUERY_IDS),
        "failed_attempt_count": 0,
        "total_durable_attempt_count": len(_ORDERED_QUERY_IDS),
        "completion_state": LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
        "expected_completion_state": LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
        "seal_reason": "ALL_5_POSITIONS_ATTEMPTED",
        "sealed_at_utc": "2026-08-07T00:00:00Z",
        "canonical_seal_document_digest": "d" * 64,
    }
    fields.update(overrides)
    return LkgRunSeal(**fields)


# ---------------------------------------------------------------------------
# lkg_qualification_seal.py -- pure, no SQLite, no ledger
# ---------------------------------------------------------------------------


class LkgQualificationSealTypesTests(unittest.TestCase):
    def test_payload_excludes_digest_field(self) -> None:
        payload = seal_payload_document(_seal())
        self.assertNotIn("canonical_seal_document_digest", payload)

    def test_payload_round_trip_reconstructs_equal_seal(self) -> None:
        seal = _seal()
        payload = seal_payload_document(seal)
        digest = seal_payload_document_digest(payload)
        reconstructed = lkg_run_seal_from_payload(payload, canonical_seal_document_digest=digest)
        self.assertEqual(
            reconstructed,
            _seal(canonical_seal_document_digest=digest),
        )

    def test_digest_is_stable_for_identical_payload(self) -> None:
        payload = seal_payload_document(_seal())
        self.assertEqual(seal_payload_document_digest(payload), seal_payload_document_digest(payload))

    def test_digest_changes_when_a_scalar_field_changes(self) -> None:
        base_digest = seal_payload_document_digest(seal_payload_document(_seal()))
        changed_digest = seal_payload_document_digest(
            seal_payload_document(_seal(seal_reason="A_DIFFERENT_REASON_CODE"))
        )
        self.assertNotEqual(base_digest, changed_digest)

    def test_digest_changes_when_position_classifications_changes(self) -> None:
        base_digest = seal_payload_document_digest(seal_payload_document(_seal()))
        reordered_ids = (20, 10, 30, 40, 50)
        changed = tuple(
            _position(i, qid, LkgPositionStatus.CLEAN_SUCCESS, ())
            for i, qid in enumerate(reordered_ids)
        )
        changed_digest = seal_payload_document_digest(
            seal_payload_document(
                _seal(
                    position_classifications=changed,
                    qualification_ordered_query_ids_sha256=_ordered_query_ids_sha256(reordered_ids),
                )
            )
        )
        self.assertNotEqual(base_digest, changed_digest)

    def test_workload_identity_rejects_missing_field(self) -> None:
        payload = seal_payload_document(_seal())
        del payload["workload_identity"]["query_role"]
        with self.assertRaises(ContractViolation):
            lkg_run_seal_from_payload(payload, canonical_seal_document_digest="e" * 64)

    def test_workload_identity_rejects_unknown_field(self) -> None:
        payload = seal_payload_document(_seal())
        payload["workload_identity"]["bogus"] = "x"
        with self.assertRaises(ContractViolation):
            lkg_run_seal_from_payload(payload, canonical_seal_document_digest="e" * 64)

    def test_workload_identity_rejects_noncanonical_field(self) -> None:
        with self.assertRaises(ContractViolation):
            LkgSealWorkloadIdentity(
                dataset_id=" leading-space",
                dataset_version="v1",
                manifest_sha256="a" * 64,
                query_role="lkg_qualification",
            )

    def test_position_classification_rejects_arbitrary_reason_codes(self) -> None:
        with self.assertRaises(ContractViolation):
            LkgPositionClassification(
                attempt_sequence=0,
                query_id=10,
                classification=LkgPositionStatus.CLEAN_SUCCESS,
                reason_codes=("PLAUSIBLE_BUT_UNAUTHORIZED_REASON_CODE",),
            )

    def test_position_classification_allows_only_closed_reason_code_combinations(self) -> None:
        LkgPositionClassification(
            attempt_sequence=0, query_id=10, classification=LkgPositionStatus.CLEAN_SUCCESS, reason_codes=()
        )
        LkgPositionClassification(
            attempt_sequence=0, query_id=10, classification=LkgPositionStatus.MISSING, reason_codes=()
        )
        LkgPositionClassification(
            attempt_sequence=0,
            query_id=10,
            classification=LkgPositionStatus.FAILED,
            reason_codes=("DURABLE_FAILURE_PRESENT",),
        )
        LkgPositionClassification(
            attempt_sequence=0,
            query_id=10,
            classification=LkgPositionStatus.MALFORMED,
            reason_codes=("MULTIPLE_SUCCESSFUL_ATTEMPTS",),
        )
        LkgPositionClassification(
            attempt_sequence=0,
            query_id=10,
            classification=LkgPositionStatus.MALFORMED,
            reason_codes=("MULTIPLE_SUCCESSFUL_ATTEMPTS", "DURABLE_FAILURES_ALSO_PRESENT"),
        )
        with self.assertRaises(ContractViolation):
            LkgPositionClassification(
                attempt_sequence=0,
                query_id=10,
                classification=LkgPositionStatus.FAILED,
                reason_codes=(),
            )
        with self.assertRaises(ContractViolation):
            LkgPositionClassification(
                attempt_sequence=0,
                query_id=10,
                classification=LkgPositionStatus.CLEAN_SUCCESS,
                reason_codes=("DURABLE_FAILURE_PRESENT",),
            )

    def test_run_seal_rejects_position_classifications_wrong_length(self) -> None:
        with self.assertRaises(ContractViolation):
            _seal(position_classifications=_all_clean_positions()[:-1])

    def test_run_seal_rejects_noncontiguous_attempt_sequence(self) -> None:
        bad = list(_all_clean_positions())
        bad[2] = _position(99, 30, LkgPositionStatus.CLEAN_SUCCESS, ())
        with self.assertRaises(ContractViolation):
            _seal(position_classifications=tuple(bad))

    def test_run_seal_rejects_query_id_digest_mismatch(self) -> None:
        bad = list(_all_clean_positions())
        bad[0] = _position(0, 999999, LkgPositionStatus.CLEAN_SUCCESS, ())
        with self.assertRaises(ContractViolation):
            _seal(position_classifications=tuple(bad))

    def test_run_seal_rejects_summary_count_mismatch(self) -> None:
        with self.assertRaises(ContractViolation):
            _seal(successful_position_count=4, missing_position_count=1)

    def test_run_seal_rejects_completion_state_not_matching_expected(self) -> None:
        with self.assertRaises(ContractViolation):
            _seal(expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE)

    def test_run_seal_rejects_completion_state_not_matching_derived_counts(self) -> None:
        failing_positions = list(_all_clean_positions())
        failing_positions[0] = _position(0, 10, LkgPositionStatus.FAILED, ("DURABLE_FAILURE_PRESENT",))
        with self.assertRaises(ContractViolation):
            _seal(
                position_classifications=tuple(failing_positions),
                successful_position_count=4,
                failed_position_count=1,
                # completion_state/expected_completion_state left as ALL_POSITIONS_SUCCESSFUL,
                # which disagrees with what failed_position_count=1 implies
            )

    def test_payload_from_document_rejects_unknown_top_level_field(self) -> None:
        payload = seal_payload_document(_seal())
        payload["bogus_top_level_field"] = "x"
        with self.assertRaises(ContractViolation):
            lkg_run_seal_from_payload(payload, canonical_seal_document_digest="e" * 64)

    def test_payload_from_document_rejects_missing_top_level_field(self) -> None:
        payload = seal_payload_document(_seal())
        del payload["seal_reason"]
        with self.assertRaises(ContractViolation):
            lkg_run_seal_from_payload(payload, canonical_seal_document_digest="e" * 64)


# ---------------------------------------------------------------------------
# lkg_qualification_ledger.py's seal_lkg_qualification_run() / verify_seal()
# ---------------------------------------------------------------------------


class LkgQualificationSealTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "lkg.sqlite3"

    def _ledger(
        self, *, binding: LkgRunBinding = _BINDING, ordered_query_ids=_ORDERED_QUERY_IDS, **kwargs
    ) -> LkgQualificationLedger:
        return LkgQualificationLedger(
            self.db_path, run_binding=binding, ordered_query_ids=ordered_query_ids, **kwargs
        )

    def _fully_successful_ledger(self) -> LkgQualificationLedger:
        ledger = self._ledger()
        for index, query_id in enumerate(_ORDERED_QUERY_IDS):
            result = ledger.append(_success_attempt(query_id, index))
            self.assertTrue(result.accepted)
        return ledger

    def _tamper_seal_column(self, sql: str, params: tuple = ()) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_seal_no_update")
            connection.execute(sql, params)
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_seal_no_update
                BEFORE UPDATE ON lkg_qualification_seal
                BEGIN SELECT RAISE(ABORT, 'lkg qualification seal is append-only'); END
                """
            )
            connection.commit()
            connection.close()

    def _tamper_seal_document_json(self, mutator, *, canonical: bool) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_seal_no_update")
            row = connection.execute("SELECT document_json FROM lkg_qualification_seal").fetchone()
            document = json.loads(row[0])
            mutated = mutator(document)
            if canonical:
                from vdbench.artifacts import canonical_json_bytes

                tampered_json = canonical_json_bytes(mutated).decode("utf-8")
            else:
                tampered_json = json.dumps(mutated)
            connection.execute(
                "UPDATE lkg_qualification_seal SET document_json = ?", (tampered_json,)
            )
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_seal_no_update
                BEFORE UPDATE ON lkg_qualification_seal
                BEGIN SELECT RAISE(ABORT, 'lkg qualification seal is append-only'); END
                """
            )
            connection.commit()
            connection.close()

    def _tamper_attempts_row_fk_off(self, sql: str, params: tuple = ()) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_attempts_no_update")
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_attempts_no_insert_after_seal")
            connection.execute(sql, params)
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_attempts_no_update
                BEFORE UPDATE ON lkg_qualification_attempts
                BEGIN SELECT RAISE(ABORT, 'lkg qualification attempts are append-only'); END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_attempts_no_insert_after_seal
                BEFORE INSERT ON lkg_qualification_attempts
                WHEN (SELECT COUNT(*) FROM lkg_qualification_seal) >= 1
                BEGIN SELECT RAISE(ABORT,
                    'lkg qualification attempts are frozen once the run is sealed'); END
                """
            )
            connection.commit()
            connection.close()

    # -- sealing states --------------------------------------------------

    def test_sealing_a_fully_clean_run_succeeds_first_time(self) -> None:
        ledger = self._fully_successful_ledger()
        seal = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        self.assertEqual(seal.completion_state, LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL)
        self.assertEqual(seal.successful_position_count, 5)
        self.assertEqual(seal.failed_position_count, 0)
        self.assertEqual(seal.malformed_position_count, 0)
        self.assertEqual(seal.missing_position_count, 0)
        self.assertEqual(seal.final_chain_head_sha256, ledger.chain_state().chain_head_sha256)
        self.assertTrue(all(e.classification is LkgPositionStatus.CLEAN_SUCCESS for e in seal.position_classifications))
        self.assertTrue(all(e.reason_codes == () for e in seal.position_classifications))

    def test_sealing_a_run_containing_a_durable_failure_succeeds_with_contains_durable_failure_state(self) -> None:
        ledger = self._ledger()
        ledger.append(_failure_attempt(10, 0))
        for index, query_id in list(enumerate(_ORDERED_QUERY_IDS))[1:]:
            ledger.append(_success_attempt(query_id, index))
        seal = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.CONTAINS_DURABLE_FAILURE,
            seal_reason="HALTED_ON_QUERY_FAILURE_AT_POSITION_0",
        )
        self.assertEqual(seal.completion_state, LkgSealCompletionState.CONTAINS_DURABLE_FAILURE)
        failing = [e for e in seal.position_classifications if e.classification is LkgPositionStatus.FAILED]
        self.assertEqual(len(failing), 1)
        self.assertEqual(failing[0].reason_codes, ("DURABLE_FAILURE_PRESENT",))

    def test_sealing_an_incomplete_run_succeeds_with_incomplete_no_failure_state(self) -> None:
        ledger = self._ledger()
        for index, query_id in list(enumerate(_ORDERED_QUERY_IDS))[:3]:
            ledger.append(_success_attempt(query_id, index))
        seal = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
            seal_reason="OPERATOR_ABANDONED_INCOMPLETE_RUN",
        )
        self.assertEqual(seal.completion_state, LkgSealCompletionState.INCOMPLETE_NO_FAILURE)
        missing = [e for e in seal.position_classifications if e.classification is LkgPositionStatus.MISSING]
        self.assertEqual(len(missing), 2)

    # -- verification / idempotency --------------------------------------

    def test_verify_seal_after_sealing_returns_matching_seal(self) -> None:
        ledger = self._fully_successful_ledger()
        sealed = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        self.assertEqual(verify_seal(ledger), sealed)

    def test_completion_state_mismatch_is_refused_before_any_write(self) -> None:
        ledger = self._fully_successful_ledger()
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            seal_lkg_qualification_run(
                ledger,
                expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
                seal_reason="WRONG_EXPECTATION",
            )
        self.assertEqual(str(cm.exception), "LKG_SEAL_COMPLETION_STATE_MISMATCH")
        with self.assertRaises(LkgQualificationLedgerError) as cm2:
            verify_seal(ledger)
        self.assertEqual(str(cm2.exception), "LKG_SEAL_MISSING")

    def test_resealing_unchanged_run_returns_byte_identical_original_seal(self) -> None:
        ledger = self._fully_successful_ledger()
        first = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        second = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        self.assertEqual(first, second)
        self.assertEqual(first.sealed_at_utc, second.sealed_at_utc)
        self.assertEqual(first.canonical_seal_document_digest, second.canonical_seal_document_digest)

    def test_reseal_with_different_expected_completion_state_is_call_argument_mismatch(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            seal_lkg_qualification_run(
                ledger,
                expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
                seal_reason="ALL_5_POSITIONS_ATTEMPTED",
            )
        self.assertEqual(str(cm.exception), "LKG_SEAL_CALL_ARGUMENTS_MISMATCH")

    def test_reseal_with_different_seal_reason_is_call_argument_mismatch(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            seal_lkg_qualification_run(
                ledger,
                expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
                seal_reason="A_DIFFERENT_REASON_CODE",
            )
        self.assertEqual(str(cm.exception), "LKG_SEAL_CALL_ARGUMENTS_MISMATCH")

    def test_records_and_chain_state_remain_readable_after_sealing(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        self.assertEqual(len(ledger.records()), 5)
        self.assertEqual(ledger.chain_state().record_count, 5)

    def test_append_idempotency_and_conflict_behavior_unchanged_on_seal_extended_schema_before_sealing(self) -> None:
        ledger = self._ledger()
        attempt = _success_attempt(10, 0)
        first = ledger.append(attempt)
        self.assertTrue(first.accepted)
        second = ledger.append(attempt)
        self.assertTrue(second.accepted)
        conflicting = _failure_attempt(10, 0)
        third = ledger.append(conflicting)
        self.assertFalse(third.accepted)
        self.assertEqual(third.conflict_reason, "QUERY_ID_CONFLICTING_DUPLICATE")

    # -- schema/version ----------------------------------------------------

    def test_v4_ledger_is_rejected_without_migration(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                CREATE TABLE lkg_qualification_run (
                    run_id TEXT PRIMARY KEY NOT NULL,
                    schema_version INTEGER NOT NULL,
                    binding_document_json TEXT NOT NULL,
                    run_binding_sha256 TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE lkg_qualification_workload_positions (
                    run_id TEXT NOT NULL,
                    attempt_sequence INTEGER NOT NULL,
                    query_id INTEGER NOT NULL,
                    PRIMARY KEY (run_id, attempt_sequence),
                    UNIQUE (run_id, query_id),
                    UNIQUE (run_id, attempt_sequence, query_id),
                    FOREIGN KEY (run_id) REFERENCES lkg_qualification_run(run_id),
                    CHECK (attempt_sequence >= 0)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE lkg_qualification_attempts (
                    insertion_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    query_id INTEGER NOT NULL,
                    attempt_sequence INTEGER NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    document_json TEXT NOT NULL,
                    previous_chain_sha256 TEXT NOT NULL,
                    chain_sha256 TEXT UNIQUE NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES lkg_qualification_run(run_id),
                    FOREIGN KEY (run_id, attempt_sequence, query_id)
                        REFERENCES lkg_qualification_workload_positions(
                            run_id, attempt_sequence, query_id
                        ),
                    UNIQUE (run_id, query_id, attempt_number),
                    UNIQUE (run_id, attempt_sequence, attempt_number),
                    CHECK (attempt_sequence >= 0),
                    CHECK (attempt_number >= 1)
                )
                """
            )
            for name, event, table in [
                ("lkg_qualification_run_no_update", "UPDATE", "lkg_qualification_run"),
                ("lkg_qualification_run_no_delete", "DELETE", "lkg_qualification_run"),
                ("lkg_qualification_workload_positions_no_update", "UPDATE", "lkg_qualification_workload_positions"),
                ("lkg_qualification_workload_positions_no_delete", "DELETE", "lkg_qualification_workload_positions"),
                ("lkg_qualification_attempts_no_update", "UPDATE", "lkg_qualification_attempts"),
                ("lkg_qualification_attempts_no_delete", "DELETE", "lkg_qualification_attempts"),
            ]:
                connection.execute(
                    f"""
                    CREATE TRIGGER {name}
                    BEFORE {event} ON {table}
                    BEGIN SELECT RAISE(ABORT, 'append-only'); END
                    """
                )
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_run_single_row
                BEFORE INSERT ON lkg_qualification_run
                WHEN (SELECT COUNT(*) FROM lkg_qualification_run) >= 1
                BEGIN SELECT RAISE(ABORT, 'single row'); END
                """
            )
            connection.execute("PRAGMA user_version = 4")
            connection.commit()
        finally:
            connection.close()

        with self.assertRaises(LkgQualificationLedgerError) as cm:
            self._ledger()
        self.assertEqual(str(cm.exception), "LKG_LEDGER_SCHEMA_MISMATCH")

    def test_pragma_version_diverging_after_construction_is_rejected_on_seal(self) -> None:
        ledger = self._fully_successful_ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA user_version = 999")
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            seal_lkg_qualification_run(
                ledger,
                expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
                seal_reason="ALL_5_POSITIONS_ATTEMPTED",
            )
        self.assertEqual(str(cm.exception), "LKG_LEDGER_SCHEMA_MISMATCH")

    def test_run_row_schema_version_diverging_from_pragma_is_rejected(self) -> None:
        ledger = self._fully_successful_ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_run_no_update")
            connection.execute("UPDATE lkg_qualification_run SET schema_version = 999")
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_run_no_update
                BEFORE UPDATE ON lkg_qualification_run
                BEGIN SELECT RAISE(ABORT, 'lkg qualification run is append-only'); END
                """
            )
            connection.commit()
            connection.close()
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            seal_lkg_qualification_run(
                ledger,
                expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
                seal_reason="ALL_5_POSITIONS_ATTEMPTED",
            )
        self.assertEqual(str(cm.exception), "LKG_LEDGER_SCHEMA_MISMATCH")

    # -- trigger enforcement, no bypass ------------------------------------

    def test_seal_row_update_is_rejected_by_trigger(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        connection = sqlite3.connect(self.db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE lkg_qualification_seal SET seal_reason = 'X'")
        finally:
            connection.rollback()
            connection.close()

    def test_seal_row_delete_is_rejected_by_trigger(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        connection = sqlite3.connect(self.db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM lkg_qualification_seal")
        finally:
            connection.rollback()
            connection.close()

    def test_second_seal_row_different_run_id_fk_off_is_rejected_by_single_row_trigger(self) -> None:
        """A duplicate primary key alone would not prove the single-row
        trigger fires -- this uses a DIFFERENT run_id (so the PRIMARY KEY
        cannot be what blocks it) with foreign_keys disabled (so the
        FOREIGN KEY to lkg_qualification_run cannot be what blocks it
        either); only the single-row trigger remains standing in the way.
        """

        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_seal(
                        run_id, seal_schema_version, run_binding_sha256,
                        phase1_ledger_schema_version, expected_query_count,
                        qualification_ordered_query_ids_sha256, final_chain_head_sha256,
                        successful_position_count, failed_position_count,
                        malformed_position_count, missing_position_count,
                        successful_attempt_count, failed_attempt_count,
                        total_durable_attempt_count, completion_state,
                        expected_completion_state, seal_reason, sealed_at_utc,
                        document_json, canonical_seal_document_digest
                    ) VALUES (
                        'a-completely-different-run-id', 1, ?, 5, 1, ?, ?,
                        1, 0, 0, 0, 1, 0, 1, 'ALL_POSITIONS_SUCCESSFUL',
                        'ALL_POSITIONS_SUCCESSFUL', 'X', '2026-08-07T00:00:00Z',
                        '{}', ?
                    )
                    """,
                    ("a" * 64, "b" * 64, "c" * 64, "d" * 64),
                )
        finally:
            connection.rollback()
            connection.close()

    def test_append_after_seal_is_rejected_by_trigger(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            ledger.append(_success_attempt(10, 0, attempt_number=2))
        self.assertEqual(str(cm.exception), "LKG_LEDGER_CONSTRAINT_VIOLATION")

    # -- external-writer tamper detection (FK off, triggers dropped) ------

    def test_evidence_changed_since_sealing_via_bypassed_insert_trigger_is_detected(self) -> None:
        ledger = self._ledger()
        for index, query_id in list(enumerate(_ORDERED_QUERY_IDS))[:4]:
            ledger.append(_success_attempt(query_id, index))
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
            seal_reason="OPERATOR_ABANDONED_INCOMPLETE_RUN",
        )
        # Insert the previously-missing 5th attempt via the real ledger API,
        # after bypassing only the insert-after-seal trigger -- a
        # chain-valid append that the seal, not chain verification, must
        # detect as evidence having changed since sealing.
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_attempts_no_insert_after_seal")
            connection.commit()
        finally:
            connection.close()
        ledger.append(_success_attempt(50, 4))
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_attempts_no_insert_after_seal
                BEFORE INSERT ON lkg_qualification_attempts
                WHEN (SELECT COUNT(*) FROM lkg_qualification_seal) >= 1
                BEGIN SELECT RAISE(ABORT,
                    'lkg qualification attempts are frozen once the run is sealed'); END
                """
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaises(LkgQualificationLedgerError) as cm:
            verify_seal(ledger)
        self.assertEqual(str(cm.exception), "LKG_SEAL_SOURCE_LEDGER_CHANGED_SINCE_SEALING")

    def test_chain_invalidating_tamper_is_still_caught_via_verified_chain(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        self._tamper_attempts_row_fk_off(
            "UPDATE lkg_qualification_attempts SET query_id = 999 WHERE insertion_seq = 1"
        )
        with self.assertRaises(LkgQualificationLedgerError):
            verify_seal(ledger)

    def test_tampered_run_binding_after_sealing_is_detected(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_run_no_update")
            connection.execute(
                "UPDATE lkg_qualification_run SET run_binding_sha256 = ?", ("f" * 64,)
            )
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_run_no_update
                BEFORE UPDATE ON lkg_qualification_run
                BEGIN SELECT RAISE(ABORT, 'lkg qualification run is append-only'); END
                """
            )
            connection.commit()
            connection.close()
        with self.assertRaises(LkgQualificationLedgerError):
            verify_seal(ledger)

    def test_tampered_workload_position_after_sealing_is_detected(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "DROP TRIGGER IF EXISTS lkg_qualification_workload_positions_no_update"
            )
            connection.execute(
                "UPDATE lkg_qualification_workload_positions SET query_id = 999 "
                "WHERE attempt_sequence = 0"
            )
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_workload_positions_no_update
                BEFORE UPDATE ON lkg_qualification_workload_positions
                BEGIN SELECT RAISE(ABORT,
                    'lkg qualification workload positions are append-only'); END
                """
            )
            connection.commit()
            connection.close()
        with self.assertRaises(LkgQualificationLedgerError):
            verify_seal(ledger)

    def test_tampered_denormalized_seal_column_with_document_json_unchanged_is_detected(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        self._tamper_seal_column(
            "UPDATE lkg_qualification_seal SET seal_reason = 'TAMPERED_REASON_CODE'"
        )
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            verify_seal(ledger)
        self.assertEqual(str(cm.exception), "LKG_SEAL_COLUMN_MISMATCH")

    def test_tampered_document_json_value_with_denormalized_columns_unchanged_is_detected(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )

        def change_run_id(document: dict) -> dict:
            document = dict(document)
            document["run_id"] = "run-1-but-tampered"
            return document

        self._tamper_seal_document_json(change_run_id, canonical=True)
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            verify_seal(ledger)
        self.assertEqual(str(cm.exception), "LKG_SEAL_CORRUPTED")

    def test_document_json_missing_field_is_corrupted(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )

        def remove_field(document: dict) -> dict:
            document = dict(document)
            del document["seal_reason"]
            return document

        self._tamper_seal_document_json(remove_field, canonical=False)
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            verify_seal(ledger)
        self.assertEqual(str(cm.exception), "LKG_SEAL_CORRUPTED")

    def test_document_json_unknown_field_is_corrupted(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )

        def add_field(document: dict) -> dict:
            document = dict(document)
            document["bogus_extra_field"] = "x"
            return document

        self._tamper_seal_document_json(add_field, canonical=False)
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            verify_seal(ledger)
        self.assertEqual(str(cm.exception), "LKG_SEAL_CORRUPTED")

    def test_document_json_noncanonical_is_corrupted(self) -> None:
        ledger = self._fully_successful_ledger()
        seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )

        def reorder(document: dict) -> dict:
            # Every field present and correct, but serialized in a
            # different (non-canonical) key order and with extra
            # whitespace -- still valid, fully-parseable JSON.
            return dict(reversed(list(document.items())))

        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_seal_no_update")
            row = connection.execute("SELECT document_json FROM lkg_qualification_seal").fetchone()
            reordered = reorder(json.loads(row[0]))
            connection.execute(
                "UPDATE lkg_qualification_seal SET document_json = ?",
                (json.dumps(reordered, indent=2),),
            )
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_seal_no_update
                BEFORE UPDATE ON lkg_qualification_seal
                BEGIN SELECT RAISE(ABORT, 'lkg qualification seal is append-only'); END
                """
            )
            connection.commit()
            connection.close()

        with self.assertRaises(LkgQualificationLedgerError) as cm:
            verify_seal(ledger)
        self.assertEqual(str(cm.exception), "LKG_SEAL_CORRUPTED")

    # -- position classification ------------------------------------------

    def test_two_successful_attempts_at_one_position_classifies_malformed(self) -> None:
        ledger = self._fully_successful_ledger()
        ledger.append(_success_attempt(10, 0, attempt_number=2))
        seal = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.CONTAINS_DURABLE_FAILURE,
            seal_reason="MALFORMED_LINEAGE_AT_POSITION_0",
        )
        self.assertEqual(seal.position_classifications[0].classification, LkgPositionStatus.MALFORMED)
        self.assertEqual(seal.position_classifications[0].reason_codes, ("MULTIPLE_SUCCESSFUL_ATTEMPTS",))
        self.assertEqual(seal.malformed_position_count, 1)

    def test_malformed_position_with_concurrent_failure_has_both_reason_codes(self) -> None:
        ledger = self._fully_successful_ledger()
        ledger.append(_success_attempt(10, 0, attempt_number=2))
        ledger.append(_failure_attempt(10, 0, attempt_number=3))
        seal = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.CONTAINS_DURABLE_FAILURE,
            seal_reason="MALFORMED_LINEAGE_WITH_FAILURE_AT_POSITION_0",
        )
        self.assertEqual(seal.position_classifications[0].classification, LkgPositionStatus.MALFORMED)
        self.assertEqual(
            seal.position_classifications[0].reason_codes,
            ("MULTIPLE_SUCCESSFUL_ATTEMPTS", "DURABLE_FAILURES_ALSO_PRESENT"),
        )

    def test_one_failed_attempt_at_position_classifies_failed(self) -> None:
        ledger = self._ledger()
        ledger.append(_failure_attempt(10, 0))
        for index, query_id in list(enumerate(_ORDERED_QUERY_IDS))[1:]:
            ledger.append(_success_attempt(query_id, index))
        seal = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.CONTAINS_DURABLE_FAILURE,
            seal_reason="POSITION_0_FAILED",
        )
        self.assertEqual(seal.position_classifications[0].classification, LkgPositionStatus.FAILED)
        self.assertEqual(seal.position_classifications[0].reason_codes, ("DURABLE_FAILURE_PRESENT",))

    def test_one_clean_success_classifies_clean_success(self) -> None:
        ledger = self._fully_successful_ledger()
        seal = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        self.assertEqual(seal.position_classifications[0].classification, LkgPositionStatus.CLEAN_SUCCESS)
        self.assertEqual(seal.position_classifications[0].reason_codes, ())

    def test_zero_attempts_classifies_missing(self) -> None:
        ledger = self._ledger()
        for index, query_id in list(enumerate(_ORDERED_QUERY_IDS))[:4]:
            ledger.append(_success_attempt(query_id, index))
        seal = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
            seal_reason="POSITION_4_NEVER_ATTEMPTED",
        )
        self.assertEqual(seal.position_classifications[4].classification, LkgPositionStatus.MISSING)
        self.assertEqual(seal.position_classifications[4].reason_codes, ())

    def test_combined_scenario_position_counts_partition_expected_query_count(self) -> None:
        ledger = self._ledger()
        ledger.append(_success_attempt(10, 0))
        ledger.append(_failure_attempt(20, 1))
        ledger.append(_success_attempt(30, 2))
        ledger.append(_success_attempt(30, 2, attempt_number=2))
        # position 3 (query_id=40) intentionally left untouched -> MISSING
        ledger.append(_success_attempt(50, 4))

        seal = seal_lkg_qualification_run(
            ledger,
            expected_completion_state=LkgSealCompletionState.CONTAINS_DURABLE_FAILURE,
            seal_reason="MIXED_CLASSIFICATION_SCENARIO",
        )
        self.assertEqual(seal.successful_position_count, 2)
        self.assertEqual(seal.failed_position_count, 1)
        self.assertEqual(seal.malformed_position_count, 1)
        self.assertEqual(seal.missing_position_count, 1)
        self.assertEqual(
            seal.successful_position_count
            + seal.failed_position_count
            + seal.malformed_position_count
            + seal.missing_position_count,
            len(_ORDERED_QUERY_IDS),
        )
        self.assertEqual(
            seal.successful_attempt_count + seal.failed_attempt_count, seal.total_durable_attempt_count
        )
        self.assertEqual(seal.total_durable_attempt_count, len(ledger.records()))
        self.assertEqual(len(seal.position_classifications), len(_ORDERED_QUERY_IDS))
        for index, entry in enumerate(seal.position_classifications):
            self.assertEqual(entry.attempt_sequence, index)

    # -- concurrency: real APIs, real synchronization ----------------------

    def test_A_real_seal_holds_transaction_blocks_append_then_append_retry_rejected_by_trigger(self) -> None:
        ledger = self._fully_successful_ledger()
        blocked_ledger = self._ledger(lock_timeout_seconds=0.5)

        lock_held = threading.Event()
        release = threading.Event()
        original = lkg_qualification_ledger._classify_positions

        def paused_classify(*args, **kwargs):
            result = original(*args, **kwargs)
            lock_held.set()
            if not release.wait(timeout=10):
                raise AssertionError("seal was never released")
            return result

        seal_results: list = []
        seal_errors: list = []

        def run_seal() -> None:
            try:
                seal_results.append(
                    seal_lkg_qualification_run(
                        ledger,
                        expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
                        seal_reason="ALL_5_POSITIONS_ATTEMPTED",
                    )
                )
            except Exception as exc:  # - captured for assertion below  # noqa: BLE001
                seal_errors.append(exc)

        with mock.patch.object(
            lkg_qualification_ledger, "_classify_positions", side_effect=paused_classify
        ):
            seal_thread = threading.Thread(target=run_seal, name="seal-A")
            seal_thread.start()
            self.assertTrue(lock_held.wait(timeout=5), "seal never acquired its lock in time")

            with self.assertRaises(LkgQualificationLedgerError) as first_cm:
                blocked_ledger.append(_success_attempt(10, 0, attempt_number=2))
            self.assertEqual(str(first_cm.exception), "LKG_LEDGER_UNAVAILABLE")

            release.set()
            seal_thread.join(timeout=10)

        self.assertEqual(seal_errors, [])
        self.assertEqual(len(seal_results), 1)
        self.assertEqual(seal_results[0].completion_state, LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL)

        with self.assertRaises(LkgQualificationLedgerError) as second_cm:
            blocked_ledger.append(_success_attempt(10, 0, attempt_number=2))
        self.assertEqual(str(second_cm.exception), "LKG_LEDGER_CONSTRAINT_VIOLATION")

    def test_B_real_append_holds_transaction_blocks_seal_then_seal_reflects_committed_attempt(self) -> None:
        ledger = self._ledger()
        for index, query_id in list(enumerate(_ORDERED_QUERY_IDS))[:4]:
            ledger.append(_success_attempt(query_id, index))
        seal_ledger = self._ledger(lock_timeout_seconds=0.5)

        lock_held = threading.Event()
        release = threading.Event()
        original = lkg_qualification_ledger._chain_record_sha256

        def paused_chain_record_sha256(*args, **kwargs):
            result = original(*args, **kwargs)
            lock_held.set()
            if not release.wait(timeout=10):
                raise AssertionError("append was never released")
            return result

        append_results: list = []
        append_errors: list = []

        def run_append() -> None:
            try:
                append_results.append(ledger.append(_success_attempt(50, 4)))
            except Exception as exc:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
                append_errors.append(exc)

        with mock.patch.object(
            lkg_qualification_ledger, "_chain_record_sha256", side_effect=paused_chain_record_sha256
        ):
            append_thread = threading.Thread(target=run_append, name="append-B")
            append_thread.start()
            self.assertTrue(lock_held.wait(timeout=5), "append never acquired its lock in time")

            with self.assertRaises(LkgQualificationLedgerError) as cm:
                seal_lkg_qualification_run(
                    seal_ledger,
                    expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
                    seal_reason="PROBE_WHILE_APPEND_HOLDS_LOCK",
                )
            self.assertEqual(str(cm.exception), "LKG_LEDGER_UNAVAILABLE")

            release.set()
            append_thread.join(timeout=10)

        self.assertEqual(append_errors, [])
        self.assertEqual(len(append_results), 1)
        self.assertTrue(append_results[0].accepted)

        seal = seal_lkg_qualification_run(
            seal_ledger,
            expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
            seal_reason="ALL_5_POSITIONS_ATTEMPTED",
        )
        self.assertEqual(seal.successful_position_count, 5)
        self.assertEqual(seal.missing_position_count, 0)
        self.assertEqual(seal.final_chain_head_sha256, ledger.chain_state().chain_head_sha256)
        self.assertEqual(seal.position_classifications[4].classification, LkgPositionStatus.CLEAN_SUCCESS)

    def test_C_two_concurrent_first_seal_calls_serialize_to_one_original_and_one_idempotent(self) -> None:
        ledger_a = self._fully_successful_ledger()
        ledger_b = self._ledger(lock_timeout_seconds=3.0)

        lock_held = threading.Event()
        release = threading.Event()
        original = lkg_qualification_ledger._classify_positions
        first_thread_name = "seal-first"

        def thread_aware_classify(*args, **kwargs):
            result = original(*args, **kwargs)
            if threading.current_thread().name == first_thread_name:
                lock_held.set()
                if not release.wait(timeout=10):
                    raise AssertionError("first seal was never released")
            return result

        results: dict[str, LkgRunSeal] = {}
        errors: dict[str, Exception] = {}

        def call_seal(name: str, ledger: LkgQualificationLedger) -> None:
            try:
                results[name] = seal_lkg_qualification_run(
                    ledger,
                    expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
                    seal_reason="ALL_5_POSITIONS_ATTEMPTED",
                )
            except Exception as exc:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
                errors[name] = exc

        with mock.patch.object(
            lkg_qualification_ledger, "_classify_positions", side_effect=thread_aware_classify
        ):
            first_thread = threading.Thread(target=call_seal, args=("first", ledger_a), name=first_thread_name)
            first_thread.start()
            self.assertTrue(lock_held.wait(timeout=5), "first seal never acquired its lock in time")

            second_thread = threading.Thread(target=call_seal, args=("second", ledger_b), name="seal-second")
            second_thread.start()
            second_thread.join(timeout=0.3)
            self.assertTrue(second_thread.is_alive(), "second seal should still be blocked on the first's lock")

            release.set()
            first_thread.join(timeout=10)
            second_thread.join(timeout=10)
            self.assertFalse(second_thread.is_alive())

        self.assertEqual(errors, {})
        self.assertEqual(set(results), {"first", "second"})
        self.assertEqual(results["first"], results["second"])
        self.assertEqual(results["first"].sealed_at_utc, results["second"].sealed_at_utc)
        self.assertEqual(
            results["first"].canonical_seal_document_digest,
            results["second"].canonical_seal_document_digest,
        )

        raw = sqlite3.connect(self.db_path)
        try:
            count = raw.execute("SELECT COUNT(*) FROM lkg_qualification_seal").fetchone()[0]
        finally:
            raw.close()
        self.assertEqual(count, 1)

    # -- argument/document validation ---------------------------------------

    def test_invalid_seal_reason_raises_contract_violation_before_any_session(self) -> None:
        ledger = self._fully_successful_ledger()
        with self.assertRaises(ContractViolation):
            seal_lkg_qualification_run(
                ledger,
                expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
                seal_reason="not a canonical reason code",
            )
        # nothing was ever written
        with self.assertRaises(LkgQualificationLedgerError) as cm:
            verify_seal(ledger)
        self.assertEqual(str(cm.exception), "LKG_SEAL_MISSING")

    def test_invalid_expected_completion_state_type_raises_contract_violation(self) -> None:
        ledger = self._fully_successful_ledger()
        with self.assertRaises(ContractViolation):
            seal_lkg_qualification_run(
                ledger,
                expected_completion_state="ALL_POSITIONS_SUCCESSFUL",  # plain str, not the enum
                seal_reason="ALL_5_POSITIONS_ATTEMPTED",
            )

    def test_invalid_ledger_argument_raises_contract_violation(self) -> None:
        with self.assertRaises(ContractViolation):
            seal_lkg_qualification_run(
                object(),
                expected_completion_state=LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL,
                seal_reason="ALL_5_POSITIONS_ATTEMPTED",
            )
        with self.assertRaises(ContractViolation):
            verify_seal(object())


if __name__ == "__main__":
    unittest.main()
