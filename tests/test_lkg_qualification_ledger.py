"""TDD coverage for the dedicated, append-only LKG-qualification ledger."""

from __future__ import annotations

import hashlib
import sqlite3
import stat
import tempfile
import threading
import unittest
from pathlib import Path

from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.lkg_qualification_evidence import (
    LkgAttemptStatus,
    build_lkg_query_attempt,
    build_lkg_query_observation,
)
from vdbench.lkg_qualification_ledger import (
    LkgQualificationLedger,
    LkgQualificationLedgerError,
)
from vdbench.lkg_run_binding import LkgRunBinding

_ORDERED_QUERY_IDS = (10, 20, 30, 40, 50)


def _ordered_query_ids_sha256(ordered_query_ids: tuple[int, ...]) -> str:
    """Independently reproduces lkg_run_binding.lkg_ordered_query_ids_sha256's
    explicit binary contract -- domain prefix + 8-byte little-endian signed
    count + each query ID as an 8-byte little-endian signed integer.
    Deliberately duplicated here (not imported from the module under test)
    so this test file's own expected-value computation is independent of
    it. Never NumPy .npy serialization -- that computes a completely
    different field, qualification_query_id_array_sha256, which this
    ledger never reconstructs or verifies at all."""

    from vdbench.lkg_run_binding import ORDERED_QUERY_IDS_DIGEST_DOMAIN

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
        # The raw .npy artifact hash is unrelated to this ledger's own
        # validation and deliberately given a value that would NOT match
        # any real DATASET-003 artifact -- the ledger never checks it.
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


class LkgQualificationLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "lkg.sqlite3"

    def _ledger(
        self,
        *,
        binding: LkgRunBinding = _BINDING,
        ordered_query_ids=_ORDERED_QUERY_IDS,
        **kwargs,
    ) -> LkgQualificationLedger:
        return LkgQualificationLedger(
            self.db_path, run_binding=binding, ordered_query_ids=ordered_query_ids, **kwargs
        )

    def test_fresh_ledger_starts_empty(self) -> None:
        ledger = self._ledger()
        state = ledger.chain_state()
        self.assertEqual(state.record_count, 0)
        self.assertEqual(state.run_id, "run-1")
        self.assertEqual(state.run_binding_sha256, _BINDING.sha256)
        self.assertEqual(ledger.records(), ())

    def test_constructor_rejects_non_binding(self) -> None:
        with self.assertRaises(TypeError):
            LkgQualificationLedger(
                self.db_path, run_binding=object(), ordered_query_ids=_ORDERED_QUERY_IDS
            )

    def test_append_success_then_read_back(self) -> None:
        ledger = self._ledger()
        result = ledger.append(_success_attempt(10, 0))
        self.assertTrue(result.accepted)
        self.assertIsNone(result.conflict_reason)
        records = ledger.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].query_id, 10)
        self.assertEqual(records[0].status, LkgAttemptStatus.SUCCESS)

    def test_append_failure_attempt_persists_it_explicitly(self) -> None:
        ledger = self._ledger()
        result = ledger.append(_failure_attempt(10, 0))
        self.assertTrue(result.accepted)
        records = ledger.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].status, LkgAttemptStatus.TIMEOUT)
        self.assertIsNone(records[0].observation)

    def test_byte_identical_replay_is_idempotent(self) -> None:
        ledger = self._ledger()
        attempt = _success_attempt(10, 0)
        first = ledger.append(attempt)
        second = ledger.append(attempt)
        self.assertTrue(first.accepted)
        self.assertTrue(second.accepted)
        self.assertEqual(len(ledger.records()), 1)

    # -- blocker 1: ordered_query_ids cryptographically bound to the binding -----
    #
    # Every check here targets qualification_ordered_query_ids_sha256 (the
    # semantic ordered-ID digest this ledger owns) -- never
    # qualification_query_id_array_sha256 (the raw DATASET-003 .npy artifact
    # hash), which this ledger never reconstructs, verifies, or depends on
    # at all; that hash's own verification is exclusively
    # lkg_dataset003_loader.py's job.

    def test_mismatched_ordered_query_ids_digest_is_rejected_before_any_file_write(self) -> None:
        wrong_binding = _binding(qualification_ordered_query_ids_sha256="f" * 64)
        with self.assertRaises(ValueError):
            LkgQualificationLedger(
                self.db_path, run_binding=wrong_binding, ordered_query_ids=_ORDERED_QUERY_IDS
            )
        self.assertFalse(self.db_path.exists())

    def test_wrong_count_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self._ledger(ordered_query_ids=_ORDERED_QUERY_IDS[:-1])

    def test_duplicate_query_ids_is_rejected(self) -> None:
        wrong_binding = _binding(
            qualification_expected_query_count=2,
            qualification_ordered_query_ids_sha256=_ordered_query_ids_sha256((1, 1)),
        )
        with self.assertRaises(ValueError):
            LkgQualificationLedger(
                self.db_path, run_binding=wrong_binding, ordered_query_ids=(1, 1)
            )

    def test_reordered_ids_change_the_digest_and_are_rejected(self) -> None:
        """Same set of IDs, different order -- the recomputed digest must
        differ from the original order's, so the mismatched binding (still
        declaring the original order's digest) rejects it."""

        reordered = tuple(reversed(_ORDERED_QUERY_IDS))
        self.assertNotEqual(_ordered_query_ids_sha256(reordered), _ORDERED_QUERY_IDS_SHA256)
        with self.assertRaises(ValueError):
            self._ledger(ordered_query_ids=reordered)

    def test_string_query_id_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self._ledger(ordered_query_ids=(10, "20", 30, 40, 50))

    def test_bool_query_id_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self._ledger(ordered_query_ids=(10, True, 30, 40, 50))

    def test_int64_overflow_query_id_is_rejected(self) -> None:
        wrong_binding = _binding(
            qualification_expected_query_count=5,
            qualification_ordered_query_ids_sha256="a" * 64,
        )
        with self.assertRaises(ValueError):
            LkgQualificationLedger(
                self.db_path,
                run_binding=wrong_binding,
                ordered_query_ids=(10, 20, 30, 40, 2**63),
            )

    def test_valid_ordered_query_ids_construct_correctly(self) -> None:
        ledger = self._ledger()
        self.assertEqual(ledger.stored_ordered_query_ids(), _ORDERED_QUERY_IDS)

    def test_raw_artifact_hash_field_is_never_checked_by_this_ledger(self) -> None:
        """A binding whose qualification_query_id_array_sha256 is
        completely unrelated to the real ordered_query_ids still
        constructs successfully -- this ledger has no involvement in
        verifying that field at all."""

        unrelated_binding = _binding(qualification_query_id_array_sha256="0" * 64)
        ledger = LkgQualificationLedger(
            self.db_path, run_binding=unrelated_binding, ordered_query_ids=_ORDERED_QUERY_IDS
        )
        self.assertEqual(ledger.stored_ordered_query_ids(), _ORDERED_QUERY_IDS)

    # -- workload-position persistence and reconstruction -------------------------

    def test_workload_positions_populated_atomically_at_creation(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            rows = connection.execute(
                "SELECT attempt_sequence, query_id FROM lkg_qualification_workload_positions "
                "ORDER BY attempt_sequence ASC"
            ).fetchall()
        finally:
            connection.close()
        self.assertEqual(rows, list(enumerate(_ORDERED_QUERY_IDS)))

    def test_stored_ordered_query_ids_reconstructs_after_reopen_with_no_in_memory_state(self) -> None:
        self._ledger()
        reopened = self._ledger()
        self.assertEqual(reopened.stored_ordered_query_ids(), _ORDERED_QUERY_IDS)

    def test_reopen_with_different_query_id_order_is_rejected(self) -> None:
        self._ledger()
        # Internally consistent (its own digest matches its own order), but
        # a genuinely different binding than the one already persisted --
        # rejected as a binding mismatch on reopen, not a constructor-level
        # digest mismatch.
        different_binding = _binding(
            producer_identity="producer-v2",
            qualification_ordered_query_ids_sha256=_ordered_query_ids_sha256((50, 40, 30, 20, 10)),
        )
        with self.assertRaises(LkgQualificationLedgerError):
            LkgQualificationLedger(
                self.db_path,
                run_binding=different_binding,
                ordered_query_ids=(50, 40, 30, 20, 10),
            )

    def test_tampered_workload_position_row_is_rejected_on_reopen(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                "DROP TRIGGER IF EXISTS lkg_qualification_workload_positions_no_update"
            )
            connection.execute(
                "UPDATE lkg_qualification_workload_positions SET query_id = 999 "
                "WHERE attempt_sequence = 0"
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger()

    def test_attempted_workload_position_update_is_rejected_by_the_trigger(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE lkg_qualification_workload_positions SET query_id = query_id"
                )
        finally:
            connection.close()

    def test_attempted_workload_position_delete_is_rejected_by_the_trigger(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM lkg_qualification_workload_positions")
        finally:
            connection.close()

    # -- blocker 2: composite FK enforces the query-to-sequence mapping ----------

    def test_correct_query_id_at_wrong_sequence_is_rejected_by_python_check(self) -> None:
        ledger = self._ledger()
        with self.assertRaises(LkgQualificationLedgerError):
            ledger.append(_success_attempt(20, 0))  # 20 belongs at sequence 1

    def test_correct_query_id_at_wrong_sequence_is_rejected_at_sql_level(self) -> None:
        """Bypasses the Python-level append() pre-check entirely via a raw
        connection -- the composite foreign key must still reject it."""

        self._ledger()
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_attempts(
                        run_id, query_id, attempt_sequence, attempt_number, status,
                        document_json, previous_chain_sha256, chain_sha256
                    ) VALUES ('run-1', 20, 0, 1, 'SUCCESS', '{}', ?, ?)
                    """,
                    ("a" * 64, "b" * 64),
                )
        finally:
            connection.close()

    def test_different_query_id_at_a_valid_sequence_is_rejected_at_sql_level(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_attempts(
                        run_id, query_id, attempt_sequence, attempt_number, status,
                        document_json, previous_chain_sha256, chain_sha256
                    ) VALUES ('run-1', 999, 0, 1, 'SUCCESS', '{}', ?, ?)
                    """,
                    ("a" * 64, "c" * 64),
                )
        finally:
            connection.close()

    def test_same_query_id_at_two_sequences_is_rejected_at_sql_level(self) -> None:
        """query_id=10 is only ever registered at sequence 0 in
        workload_positions -- attempting to insert it at sequence 1 must
        fail the composite FK (no (run_1, 1, 10) parent row exists)."""

        self._ledger()
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_attempts(
                        run_id, query_id, attempt_sequence, attempt_number, status,
                        document_json, previous_chain_sha256, chain_sha256
                    ) VALUES ('run-1', 10, 1, 1, 'SUCCESS', '{}', ?, ?)
                    """,
                    ("a" * 64, "d" * 64),
                )
        finally:
            connection.close()

    def test_unknown_query_id_is_rejected_at_sql_level(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_attempts(
                        run_id, query_id, attempt_sequence, attempt_number, status,
                        document_json, previous_chain_sha256, chain_sha256
                    ) VALUES ('run-1', 77777, 2, 1, 'SUCCESS', '{}', ?, ?)
                    """,
                    ("a" * 64, "e" * 64),
                )
        finally:
            connection.close()

    def test_attempt_for_another_run_is_rejected_at_sql_level(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_attempts(
                        run_id, query_id, attempt_sequence, attempt_number, status,
                        document_json, previous_chain_sha256, chain_sha256
                    ) VALUES ('a-different-run', 10, 0, 1, 'SUCCESS', '{}', ?, ?)
                    """,
                    ("a" * 64, "f" * 64),
                )
        finally:
            connection.close()

    def test_sequence_outside_populated_range_is_rejected_at_sql_level(self) -> None:
        """Sequence 5 was never populated (only 0..4 exist for this
        5-query workload) -- no (run_id, 5, *) parent row can exist, so the
        composite FK rejects any query_id at that sequence."""

        self._ledger()
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_attempts(
                        run_id, query_id, attempt_sequence, attempt_number, status,
                        document_json, previous_chain_sha256, chain_sha256
                    ) VALUES ('run-1', 10, 5, 1, 'SUCCESS', '{}', ?, ?)
                    """,
                    ("a" * 64, "g" * 64),
                )
        finally:
            connection.close()

    def test_negative_sequence_is_rejected_by_check_constraint(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_attempts(
                        run_id, query_id, attempt_sequence, attempt_number, status,
                        document_json, previous_chain_sha256, chain_sha256
                    ) VALUES ('run-1', 10, -1, 1, 'SUCCESS', '{}', ?, ?)
                    """,
                    ("a" * 64, "h" * 64),
                )
        finally:
            connection.close()

    def test_zero_attempt_number_is_rejected_by_check_constraint(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_attempts(
                        run_id, query_id, attempt_sequence, attempt_number, status,
                        document_json, previous_chain_sha256, chain_sha256
                    ) VALUES ('run-1', 10, 0, 0, 'SUCCESS', '{}', ?, ?)
                    """,
                    ("a" * 64, "i" * 64),
                )
        finally:
            connection.close()

    def test_second_run_row_is_rejected_by_the_single_row_trigger(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "INSERT INTO lkg_qualification_run("
                    "run_id, schema_version, binding_document_json, run_binding_sha256"
                    ") VALUES ('run-2', 3, '{}', ?)",
                    ("a" * 64,),
                )
        finally:
            connection.close()

    def test_mismatched_run_binding_digest_is_rejected_at_append(self) -> None:
        ledger = self._ledger()
        wrong = _success_attempt(10, 0, binding=_binding(producer_identity="other"))
        with self.assertRaises(LkgQualificationLedgerError):
            ledger.append(wrong)

    # -- blocker 3: denormalized-column cross-checking and chain binding ---------

    def _seed_two_rows(self) -> LkgQualificationLedger:
        ledger = self._ledger()
        ledger.append(_success_attempt(10, 0))
        ledger.append(_success_attempt(20, 1))
        return ledger

    def _tamper_row_one(self, sql: str, params: tuple = ()) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_attempts_no_update")
            connection.execute(sql, params)
            connection.commit()
        finally:
            connection.close()

    def _restore_attempts_no_update_trigger(self, connection: sqlite3.Connection) -> None:
        """DDL (DROP TRIGGER) auto-commits immediately in Python's sqlite3
        module -- connection.rollback() does NOT undo it. When a test drops
        this trigger only to prove some *other* constraint blocks the
        tamper, it must explicitly recreate the trigger afterward to leave
        the schema exactly as _EXPECTED_SCHEMA_OBJECTS requires."""

        connection.execute(
            """
            CREATE TRIGGER lkg_qualification_attempts_no_update
            BEFORE UPDATE ON lkg_qualification_attempts
            BEGIN SELECT RAISE(ABORT, 'lkg qualification attempts are append-only'); END
            """
        )
        connection.commit()

    def test_tampered_run_id_column_is_rejected_by_the_foreign_key_itself(self) -> None:
        """run_id carries two foreign keys (to lkg_qualification_run and,
        compositely, to lkg_qualification_workload_positions), and the
        single-row trigger guarantees exactly one valid run_id ('run-1')
        can ever exist -- so even with the no_update trigger dropped,
        SQLite's own FK enforcement rejects any UPDATE of run_id to a
        different value at the UPDATE itself; the tamper can never be
        written for chain verification to later discover."""

        self._seed_two_rows()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_attempts_no_update")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE lkg_qualification_attempts SET run_id = 'a-different-run' "
                    "WHERE insertion_seq = 1"
                )
        finally:
            connection.rollback()
            self._restore_attempts_no_update_trigger(connection)
            connection.close()
        # Untampered: the ledger still verifies cleanly.
        self.assertEqual(len(self._ledger().records()), 2)

    def test_tampered_query_id_column_is_rejected_by_a_constraint(self) -> None:
        """Retargeting row 1 (query_id=10 @ sequence=0) to query_id=20
        collides with row 2's own (run_id, query_id, attempt_number) key --
        rejected by a UNIQUE constraint at the UPDATE itself, even with the
        no_update trigger dropped."""

        self._seed_two_rows()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_attempts_no_update")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE lkg_qualification_attempts SET query_id = 20 WHERE insertion_seq = 1"
                )
        finally:
            connection.rollback()
            self._restore_attempts_no_update_trigger(connection)
            connection.close()
        self.assertEqual(len(self._ledger().records()), 2)

    def test_tampered_attempt_sequence_column_is_rejected_by_a_constraint(self) -> None:
        """Retargeting row 1's attempt_sequence from 0 to 1 collides with
        row 2's own (run_id, attempt_sequence, attempt_number) key --
        rejected by a UNIQUE constraint at the UPDATE itself, even with the
        no_update trigger dropped."""

        self._seed_two_rows()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_attempts_no_update")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE lkg_qualification_attempts SET attempt_sequence = 1 "
                    "WHERE insertion_seq = 1"
                )
        finally:
            connection.rollback()
            self._restore_attempts_no_update_trigger(connection)
            connection.close()
        self.assertEqual(len(self._ledger().records()), 2)

    def test_tampered_attempt_sequence_to_an_unmatched_value_is_rejected_by_the_composite_fk(self) -> None:
        """A tampered attempt_sequence that does NOT collide with another
        row's UNIQUE key (sequence=4, unused by any existing attempt) is
        instead caught by the composite foreign key: query_id=10 was only
        ever registered at sequence 0 in workload_positions, never at 4."""

        self._seed_two_rows()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_attempts_no_update")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE lkg_qualification_attempts SET attempt_sequence = 4 "
                    "WHERE insertion_seq = 1"
                )
        finally:
            connection.rollback()
            self._restore_attempts_no_update_trigger(connection)
            connection.close()
        self.assertEqual(len(self._ledger().records()), 2)

    def test_tampered_attempt_number_column_is_detected(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one(
            "UPDATE lkg_qualification_attempts SET attempt_number = 2 WHERE insertion_seq = 1"
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_tampered_status_column_is_detected(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one(
            "UPDATE lkg_qualification_attempts SET status = 'TIMEOUT' WHERE insertion_seq = 1"
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_tampered_document_json_is_detected(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one(
            "UPDATE lkg_qualification_attempts SET document_json = replace("
            "document_json, '\"recall\":1.0', '\"recall\":0.5') WHERE insertion_seq = 1"
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_tampered_previous_chain_sha256_is_detected(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one(
            "UPDATE lkg_qualification_attempts SET previous_chain_sha256 = ? "
            "WHERE insertion_seq = 1",
            ("0" * 64,),
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_tampered_chain_sha256_is_detected(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one(
            "UPDATE lkg_qualification_attempts SET chain_sha256 = ? WHERE insertion_seq = 1",
            ("1" * 64,),
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_chain_hash_binds_identity_columns_not_only_document(self) -> None:
        """Direct proof that the chain hash input includes the raw
        columns: tampering with `status` alone (leaving document_json
        byte-for-byte untouched) still changes what the chain hash must
        recompute to, and is therefore caught -- this could not be true if
        the chain hash were computed from document_json alone."""

        self._seed_two_rows()
        connection = sqlite3.connect(self.db_path)
        original = connection.execute(
            "SELECT document_json, chain_sha256 FROM lkg_qualification_attempts "
            "WHERE insertion_seq = 1"
        ).fetchone()
        connection.close()

        self._tamper_row_one(
            "UPDATE lkg_qualification_attempts SET status = 'TIMEOUT' WHERE insertion_seq = 1"
        )

        connection = sqlite3.connect(self.db_path)
        after = connection.execute(
            "SELECT document_json, chain_sha256 FROM lkg_qualification_attempts "
            "WHERE insertion_seq = 1"
        ).fetchone()
        connection.close()

        self.assertEqual(original[0], after[0])  # document_json is untouched
        self.assertEqual(original[1], after[1])  # chain_sha256 was NOT recomputed by us
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()  # yet verification still fails

    # -- blocker 2: verification survives an external writer that disables ------
    # -- BOTH foreign keys AND the append-only triggers for its own connection --
    #
    # SQLite foreign-key enforcement is per-connection: PRAGMA foreign_keys=ON
    # in this ledger's own _connection() does not protect against a writer
    # that opens its own connection with PRAGMA foreign_keys=OFF. Every test
    # below does exactly that (plus dropping the relevant no_update/no_delete
    # trigger) and proves this ledger's *verification* -- strict document
    # reconstruction, SQL-column/document cross-checks, stored run-binding
    # re-verification, and chain-hash recomputation -- is what actually
    # catches the tamper, never merely the UPDATE/INSERT being rejected.

    def _tamper_row_one_fk_off(self, sql: str, params: tuple = ()) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_attempts_no_update")
            connection.execute(sql, params)
            connection.commit()
        finally:
            self._restore_attempts_no_update_trigger(connection)
            connection.close()

    def test_fk_off_tampered_run_id_is_detected_by_verification(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one_fk_off(
            "UPDATE lkg_qualification_attempts SET run_id = 'a-different-run' "
            "WHERE insertion_seq = 1"
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_fk_off_tampered_query_id_is_detected_by_verification(self) -> None:
        self._seed_two_rows()
        # 999 collides with no other row's (run_id, query_id, attempt_number)
        # key and, with the composite FK disabled, is not rejected at write
        # time either -- only chain verification catches it.
        self._tamper_row_one_fk_off(
            "UPDATE lkg_qualification_attempts SET query_id = 999 WHERE insertion_seq = 1"
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_fk_off_tampered_attempt_sequence_is_detected_by_verification(self) -> None:
        self._seed_two_rows()
        # sequence=4 collides with no other row's (run_id, attempt_sequence,
        # attempt_number) key and, with the composite FK disabled, is not
        # rejected at write time either.
        self._tamper_row_one_fk_off(
            "UPDATE lkg_qualification_attempts SET attempt_sequence = 4 "
            "WHERE insertion_seq = 1"
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_fk_off_tampered_attempt_number_is_detected_by_verification(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one_fk_off(
            "UPDATE lkg_qualification_attempts SET attempt_number = 2 WHERE insertion_seq = 1"
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_fk_off_tampered_status_is_detected_by_verification(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one_fk_off(
            "UPDATE lkg_qualification_attempts SET status = 'TIMEOUT' WHERE insertion_seq = 1"
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_fk_off_tampered_document_json_is_detected_by_verification(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one_fk_off(
            "UPDATE lkg_qualification_attempts SET document_json = replace("
            "document_json, '\"recall\":1.0', '\"recall\":0.5') WHERE insertion_seq = 1"
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_fk_off_tampered_previous_chain_sha256_is_detected_by_verification(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one_fk_off(
            "UPDATE lkg_qualification_attempts SET previous_chain_sha256 = ? "
            "WHERE insertion_seq = 1",
            ("0" * 64,),
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    def test_fk_off_tampered_chain_sha256_is_detected_by_verification(self) -> None:
        self._seed_two_rows()
        self._tamper_row_one_fk_off(
            "UPDATE lkg_qualification_attempts SET chain_sha256 = ? WHERE insertion_seq = 1",
            ("1" * 64,),
        )
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().records()

    # -- blocker 2: run-row and workload-position corruption, FK/triggers bypassed --

    def test_fk_off_second_run_row_is_detected_on_reopen(self) -> None:
        """The single-row trigger normally blocks a second INSERT; with it
        dropped, the row lands -- and must be caught by an explicit
        COUNT(*) check on reopen, not silently ignored by fetchone()."""

        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("DROP TRIGGER IF EXISTS lkg_qualification_run_single_row")
            connection.execute(
                "INSERT INTO lkg_qualification_run("
                "run_id, schema_version, binding_document_json, run_binding_sha256"
                ") VALUES ('run-2', 4, '{}', ?)",
                ("a" * 64,),
            )
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_run_single_row
                BEFORE INSERT ON lkg_qualification_run
                WHEN (SELECT COUNT(*) FROM lkg_qualification_run) >= 1
                BEGIN SELECT RAISE(ABORT, 'lkg qualification ledger holds exactly one run'); END
                """
            )
            connection.commit()
            connection.close()

        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger()
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().stored_run_binding()

    def test_fk_off_missing_workload_position_is_detected_on_reopen(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "DROP TRIGGER IF EXISTS lkg_qualification_workload_positions_no_delete"
            )
            connection.execute(
                "DELETE FROM lkg_qualification_workload_positions WHERE attempt_sequence = 4"
            )
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_workload_positions_no_delete
                BEFORE DELETE ON lkg_qualification_workload_positions
                BEGIN SELECT RAISE(ABORT, 'lkg qualification workload positions are append-only'); END
                """
            )
            connection.commit()
            connection.close()

        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger()
        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger().stored_ordered_query_ids()

    def test_fk_off_extra_workload_position_causes_count_mismatch(self) -> None:
        """Inserting an extra position row needs no trigger bypass at all
        (there is no "no_insert" trigger) -- it is caught purely by the
        length check against qualification_expected_query_count."""

        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "INSERT INTO lkg_qualification_workload_positions(run_id, attempt_sequence, query_id) "
                "VALUES ('run-1', 5, 9999)"
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger()

    def test_fk_off_changed_query_mapping_is_detected_via_digest_mismatch(self) -> None:
        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "DROP TRIGGER IF EXISTS lkg_qualification_workload_positions_no_update"
            )
            connection.execute(
                "UPDATE lkg_qualification_workload_positions SET query_id = 9999 "
                "WHERE attempt_sequence = 0"
            )
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_workload_positions_no_update
                BEFORE UPDATE ON lkg_qualification_workload_positions
                BEGIN SELECT RAISE(ABORT, 'lkg qualification workload positions are append-only'); END
                """
            )
            connection.commit()
            connection.close()

        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger()

    def test_fk_off_swapped_query_mapping_changes_the_ordered_digest(self) -> None:
        """Swapping two positions' query_id values (10<->20) preserves each
        position's own UNIQUE(run_id, query_id) and the row count/
        contiguity, isolating a pure reordering -- the ordered-ID digest
        alone must catch it."""

        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "DROP TRIGGER IF EXISTS lkg_qualification_workload_positions_no_update"
            )
            connection.execute(
                "UPDATE lkg_qualification_workload_positions SET query_id = -1 "
                "WHERE attempt_sequence = 0"
            )
            connection.execute(
                "UPDATE lkg_qualification_workload_positions SET query_id = 10 "
                "WHERE attempt_sequence = 1"
            )
            connection.execute(
                "UPDATE lkg_qualification_workload_positions SET query_id = 20 "
                "WHERE attempt_sequence = 0"
            )
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_workload_positions_no_update
                BEFORE UPDATE ON lkg_qualification_workload_positions
                BEGIN SELECT RAISE(ABORT, 'lkg qualification workload positions are append-only'); END
                """
            )
            connection.commit()
            connection.close()

        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger()

    def test_fk_off_duplicate_query_mapping_is_rejected_by_the_positions_own_unique_constraint(self) -> None:
        """A hostile writer cannot even create a duplicate query_id mapping
        within lkg_qualification_workload_positions: UNIQUE(run_id,
        query_id) is a plain UNIQUE constraint, unconditionally enforced
        regardless of the foreign_keys pragma -- unlike a FOREIGN KEY
        constraint, disabling PRAGMA foreign_keys has no effect on it."""

        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "DROP TRIGGER IF EXISTS lkg_qualification_workload_positions_no_update"
            )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE lkg_qualification_workload_positions SET query_id = 20 "
                    "WHERE attempt_sequence = 0"
                )
        finally:
            connection.rollback()
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_workload_positions_no_update
                BEFORE UPDATE ON lkg_qualification_workload_positions
                BEGIN SELECT RAISE(ABORT, 'lkg qualification workload positions are append-only'); END
                """
            )
            connection.commit()
            connection.close()

        # Untampered: the ledger still verifies cleanly.
        self.assertEqual(self._ledger().stored_ordered_query_ids(), _ORDERED_QUERY_IDS)

    def test_fk_off_non_contiguous_positions_is_detected_on_reopen(self) -> None:
        """Retargeting the last position's own attempt_sequence to a value
        outside 0..4 (while every other position stays exactly as-is)
        keeps the row count at 5 but breaks contiguity -- isolated from a
        pure count mismatch."""

        self._ledger()
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "DROP TRIGGER IF EXISTS lkg_qualification_workload_positions_no_update"
            )
            connection.execute(
                "UPDATE lkg_qualification_workload_positions SET attempt_sequence = 10 "
                "WHERE attempt_sequence = 4"
            )
            connection.commit()
        finally:
            connection.execute(
                """
                CREATE TRIGGER lkg_qualification_workload_positions_no_update
                BEFORE UPDATE ON lkg_qualification_workload_positions
                BEGIN SELECT RAISE(ABORT, 'lkg qualification workload positions are append-only'); END
                """
            )
            connection.commit()
            connection.close()

        with self.assertRaises(LkgQualificationLedgerError):
            self._ledger()

    # -- restart / partial-run continuation -------------------------------------

    def test_reopen_preserves_records_and_chain_head(self) -> None:
        ledger = self._ledger()
        ledger.append(_success_attempt(10, 0))
        ledger.append(_success_attempt(20, 1))
        state_before = ledger.chain_state()
        del ledger

        reopened = self._ledger()
        state_after = reopened.chain_state()
        self.assertEqual(state_after.chain_head_sha256, state_before.chain_head_sha256)
        self.assertEqual(len(reopened.records()), 2)

    def test_reopen_with_different_run_id_is_rejected(self) -> None:
        self._ledger()
        other = _binding(run_id="run-2")
        with self.assertRaises(LkgQualificationLedgerError):
            LkgQualificationLedger(
                self.db_path, run_binding=other, ordered_query_ids=_ORDERED_QUERY_IDS
            )

    # -- retry semantics ------------------------------------------------------------

    def test_retry_of_a_failed_query_is_a_distinct_row(self) -> None:
        ledger = self._ledger()
        ledger.append(_failure_attempt(10, 0, attempt_number=1))
        ledger.append(_success_attempt(10, 0, attempt_number=2))
        records = ledger.records()
        self.assertEqual(len(records), 2)
        self.assertEqual([r.attempt_number for r in records], [1, 2])

    def test_conflicting_duplicate_same_key_different_content_is_refused(self) -> None:
        ledger = self._ledger()
        ledger.append(_success_attempt(10, 0, attempt_number=1, ef=400))
        result = ledger.append(_success_attempt(10, 0, attempt_number=1, ef=800))
        self.assertFalse(result.accepted)
        self.assertEqual(result.conflict_reason, "QUERY_ID_CONFLICTING_DUPLICATE")
        self.assertEqual(len(ledger.records()), 1)

    def test_type_error_for_non_attempt_append(self) -> None:
        ledger = self._ledger()
        with self.assertRaises(TypeError):
            ledger.append(object())  # type: ignore[arg-type]

    # -- chain ordering is strictly insertion_seq -----------------------------------

    def test_out_of_order_dispatch_preserves_true_arrival_order(self) -> None:
        ledger = self._ledger()
        ledger.append(_success_attempt(40, 3))
        ledger.append(_success_attempt(10, 0))
        ledger.append(_success_attempt(30, 2))
        ledger.append(_success_attempt(20, 1))
        records = ledger.records()
        self.assertEqual([r.query_id for r in records], [40, 10, 30, 20])

    # -- genuine append failure, distinct from conflicting retry -----------------

    def test_genuine_append_failure_raises_a_distinct_exception_type(self) -> None:
        ledger = self._ledger(lock_timeout_seconds=0.05)
        ledger.append(_success_attempt(10, 0))
        state_before = ledger.chain_state()

        lock_connection = sqlite3.connect(self.db_path)
        lock_connection.execute("BEGIN EXCLUSIVE")
        try:
            with self.assertRaises(LkgQualificationLedgerError):
                ledger.append(_success_attempt(20, 1))
        finally:
            lock_connection.rollback()
            lock_connection.close()

        state_after = ledger.chain_state()
        self.assertEqual(state_before, state_after)

    def test_append_failure_does_not_create_an_ambiguous_row(self) -> None:
        ledger = self._ledger(lock_timeout_seconds=0.05)
        lock_connection = sqlite3.connect(self.db_path)
        lock_connection.execute("BEGIN EXCLUSIVE")
        try:
            with self.assertRaises(LkgQualificationLedgerError):
                ledger.append(_success_attempt(10, 0))
        finally:
            lock_connection.rollback()
            lock_connection.close()

        fresh = self._ledger()
        self.assertEqual(fresh.records(), ())

    # -- private file mode --------------------------------------------------------

    def test_database_file_is_created_with_private_permissions(self) -> None:
        self._ledger()
        mode = stat.S_IMODE(self.db_path.stat().st_mode)
        self.assertEqual(mode, 0o600)

    def test_world_readable_parent_directory_is_rejected(self) -> None:
        world_dir = Path(self._tmp.name) / "open"
        world_dir.mkdir()
        world_dir.chmod(0o777)
        with self.assertRaises(LkgQualificationLedgerError):
            LkgQualificationLedger(
                world_dir / "lkg.sqlite3",
                run_binding=_BINDING,
                ordered_query_ids=_ORDERED_QUERY_IDS,
            )

    # -- constructor validation -----------------------------------------------

    def test_out_of_range_lock_timeout_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            LkgQualificationLedger(
                self.db_path,
                run_binding=_BINDING,
                ordered_query_ids=_ORDERED_QUERY_IDS,
                lock_timeout_seconds=100.0,
            )

    # -- concurrent writers -----------------------------------------------------

    def test_concurrent_writers_serialize_without_corruption(self) -> None:
        self._ledger()
        barrier = threading.Barrier(2)
        errors: list[BaseException] = []

        def _write(query_id: int, sequence: int) -> None:
            try:
                barrier.wait(timeout=5)
                ledger = self._ledger(lock_timeout_seconds=5.0)
                ledger.append(_success_attempt(query_id, sequence))
            except BaseException as exc:  # captured for the main thread  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=_write, args=(qid, seq))
            for seq, qid in ((0, 10), (1, 20))
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(errors, [])
        final = self._ledger()
        self.assertEqual({record.query_id for record in final.records()}, {10, 20})


if __name__ == "__main__":
    unittest.main()
