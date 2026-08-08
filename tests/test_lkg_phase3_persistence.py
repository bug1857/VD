"""Focused D2 tests for the append-only Phase-3 authority-reference store."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

from vdbench.artifacts import canonical_json_bytes
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.lkg_phase2_readiness_ledger import Phase2ReadinessLedger
from vdbench.lkg_phase3_authority import (
    LkgPhase3Authority,
    resolve_lkg_phase3_authority,
)
from vdbench.lkg_phase3_persistence import (
    LKG_PHASE3_REFERENCE_HASH_DOMAIN,
    LkgPhase3AuthorityReferenceStore,
    LkgPhase3PersistenceError,
    PersistedLkgPhase3AuthorityReference,
)
from vdbench.lkg_qualification_evaluation import (
    LkgQualificationEvaluation,
    LkgQualificationStatus,
)
from vdbench.lkg_qualification_evaluation_ledger import (
    LkgQualificationEvaluationLedger,
)
from vdbench.lkg_qualification_ledger import LkgQualificationLedger
from vdbench.lkg_run_binding import LkgRunBinding
from vdbench.search_configuration_digest import search_configuration_sha256


_TIMESTAMP_1 = "2026-08-08T13:00:00.000000Z"
_TIMESTAMP_2 = "2026-08-08T13:01:00.000000Z"
_TABLE = "lkg_phase3_authority_references"


def _authority(identifier: int = 1) -> LkgPhase3Authority:
    evaluation_digest = f"{identifier:064x}"
    run_binding = LkgRunBinding(
        run_id=f"run-phase3-{identifier:03d}",
        producer_identity="checkpoint-a-producer-v1",
        search_configuration=SearchConfiguration(
            metric=Metric.L2,
            threshold_label="target-075",
            radius=1.0,
            index_track=IndexTrack.HNSW,
            ef=400,
            limit=100,
            consistency_level="Strong",
        ),
        collection_name=f"lkg-l2-hnsw-{identifier}",
        base_data_identity="base-data-v1",
        index_identity="hnsw-index-v1",
        qualification_dataset_id="DATASET-003",
        qualification_dataset_version="DATASET-003-v1",
        qualification_manifest_sha256="a" * 64,
        qualification_query_role="lkg_qualification",
        qualification_query_id_array_sha256="b" * 64,
        qualification_ordered_query_ids_sha256="c" * 64,
        qualification_query_array_sha256="d" * 64,
        qualification_expected_query_count=2_400,
        environment_identity="env-001",
        source_revision=f"revision-{identifier}",
    )
    evaluation = mock.create_autospec(LkgQualificationEvaluation, instance=True)
    values = {
        "canonical_evaluation_digest": evaluation_digest,
        "source_run_id": run_binding.run_id,
        "source_run_binding_sha256": run_binding.sha256,
        "source_run_seal_digest": "1" * 64,
        "source_sealed_phase1_chain_head_sha256": "2" * 64,
        "phase2_source_binding_digest": "3" * 64,
        "evaluated_ef": 400,
        "search_configuration_digest": search_configuration_sha256(
            run_binding.search_configuration
        ),
        "qualification_dataset_id": run_binding.qualification_dataset_id,
        "qualification_dataset_version": run_binding.qualification_dataset_version,
        "qualification_manifest_sha256": run_binding.qualification_manifest_sha256,
        "qualification_query_role": run_binding.qualification_query_role,
        "qualification_ordered_query_ids_sha256": (
            run_binding.qualification_ordered_query_ids_sha256
        ),
        "status": LkgQualificationStatus.PASSING,
        "qualified": True,
        "status_reason_codes": (),
        "evaluator_identity": "checkpoint-c-evaluator-v1",
        "evaluator_source_revision": "checkpoint-c-revision-v1",
        "evaluated_at_utc": "2026-08-08T12:00:00.000000Z",
    }
    for field, value in values.items():
        setattr(evaluation, field, value)

    c_ledger = mock.create_autospec(
        LkgQualificationEvaluationLedger, instance=True
    )
    c_ledger.get_final_evaluation.return_value = evaluation
    c_ledger.evaluate_and_finalize.return_value = evaluation
    phase1 = mock.create_autospec(LkgQualificationLedger, instance=True)
    phase2 = mock.create_autospec(Phase2ReadinessLedger, instance=True)
    resolution = resolve_lkg_phase3_authority(
        evaluation_ledger=c_ledger,
        phase1_ledger=phase1,
        phase2_readiness_ledger=phase2,
        run_binding=run_binding,
        expected_canonical_evaluation_digest=evaluation_digest,
    )
    if resolution.authority is None:
        raise AssertionError(f"D1 fixture authority failed: {resolution.reason_codes}")
    return resolution.authority


def _raw_mutate_preserving_triggers(
    path: Path,
    sql: str,
    parameters: tuple[object, ...] = (),
) -> None:
    connection = sqlite3.connect(path)
    try:
        trigger_rows = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger' ORDER BY name"
        ).fetchall()
        for name, _trigger_sql in trigger_rows:
            connection.execute(f'DROP TRIGGER "{name}"')
        connection.execute(sql, parameters)
        for _name, trigger_sql in trigger_rows:
            connection.execute(trigger_sql)
        connection.commit()
    finally:
        connection.close()


def _canonical_digest(document: dict[str, object]) -> str:
    return hashlib.sha256(
        LKG_PHASE3_REFERENCE_HASH_DOMAIN + canonical_json_bytes(document)
    ).hexdigest()


class LkgPhase3PersistenceTests(unittest.TestCase):
    def test_append_and_load_persists_identity_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            authority = _authority()
            with LkgPhase3AuthorityReferenceStore(path) as store:
                result = store.append(authority, persisted_at_utc=_TIMESTAMP_1)
                loaded = store.load_latest()

            self.assertTrue(result.appended)
            self.assertEqual(loaded, result.reference)
            assert loaded is not None
            self.assertEqual(
                loaded.canonical_evaluation_digest,
                authority.canonical_evaluation_digest,
            )
            self.assertEqual(loaded.source_run_id, authority.source_run_id)
            self.assertEqual(
                loaded.source_run_binding_sha256,
                authority.source_run_binding_sha256,
            )
            self.assertEqual(loaded.evaluated_ef, 400)
            self.assertEqual(loaded.persisted_at_utc, _TIMESTAMP_1)
            self.assertNotIsInstance(loaded, LkgPhase3Authority)
            self.assertFalse(hasattr(loaded, "qualified"))
            self.assertFalse(hasattr(loaded, "usable"))

    def test_restart_revalidates_and_loads_exact_reference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path) as first:
                expected = first.append(
                    _authority(), persisted_at_utc=_TIMESTAMP_1
                ).reference
            with LkgPhase3AuthorityReferenceStore(path) as reopened:
                self.assertEqual(reopened.load_all(), (expected,))

    def test_preexisting_empty_or_unversioned_database_is_refused(self) -> None:
        for kind in ("empty", "sqlite-unversioned"):
            with self.subTest(kind=kind):
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "phase3-authority.db"
                    if kind == "empty":
                        path.touch(mode=0o600)
                    else:
                        connection = sqlite3.connect(path)
                        try:
                            connection.execute("CREATE TABLE transient(value INTEGER)")
                            connection.execute("DROP TABLE transient")
                            connection.commit()
                        finally:
                            connection.close()
                        path.chmod(0o600)
                    before = path.read_bytes()

                    with self.assertRaisesRegex(
                        LkgPhase3PersistenceError,
                        "pre-existing database",
                    ):
                        LkgPhase3AuthorityReferenceStore(path)

                    self.assertEqual(path.read_bytes(), before)
                    self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_hardlink_is_refused_without_modifying_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            original = Path(directory) / "original.db"
            candidate = Path(directory) / "hardlink.db"
            original.write_bytes(b"user-owned-not-a-database")
            original.chmod(0o600)
            candidate.hardlink_to(original)
            before = original.read_bytes()
            before_mode = original.stat().st_mode & 0o777

            with self.assertRaisesRegex(
                LkgPhase3PersistenceError, "single-link regular file"
            ):
                LkgPhase3AuthorityReferenceStore(candidate)

            self.assertEqual(original.read_bytes(), before)
            self.assertEqual(candidate.read_bytes(), before)
            self.assertEqual(original.stat().st_mode & 0o777, before_mode)
            self.assertEqual(original.stat().st_nlink, 2)

    def test_existing_wal_database_is_rejected_without_mode_normalization(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path):
                pass
            connection = sqlite3.connect(path)
            try:
                changed_mode = connection.execute(
                    "PRAGMA journal_mode = WAL;"
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(str(changed_mode).lower(), "wal")

            with self.assertRaisesRegex(
                LkgPhase3PersistenceError, "journal mode is unsupported"
            ):
                LkgPhase3AuthorityReferenceStore(path)

            read_only_uri = f"{path.resolve().as_uri()}?mode=ro"
            read_only = sqlite3.connect(read_only_uri, uri=True)
            try:
                observed_mode = read_only.execute(
                    "PRAGMA journal_mode;"
                ).fetchone()[0]
            finally:
                read_only.close()
            self.assertEqual(str(observed_mode).lower(), "wal")

    def test_same_authority_is_idempotent_without_timestamp_rewrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            authority = _authority()
            with LkgPhase3AuthorityReferenceStore(path) as store:
                first = store.append(authority, persisted_at_utc=_TIMESTAMP_1)
                retry = store.append(authority, persisted_at_utc=_TIMESTAMP_2)
                records = store.load_all()

            self.assertTrue(first.appended)
            self.assertFalse(retry.appended)
            self.assertEqual(retry.reference, first.reference)
            self.assertEqual(retry.reference.persisted_at_utc, _TIMESTAMP_1)
            self.assertEqual(len(records), 1)

    def test_distinct_authority_requires_strictly_later_timestamp(self) -> None:
        for rejected_timestamp in (_TIMESTAMP_1, "2026-08-08T12:59:59.999999Z"):
            with self.subTest(rejected_timestamp=rejected_timestamp):
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "phase3-authority.db"
                    with LkgPhase3AuthorityReferenceStore(path) as store:
                        first = store.append(
                            _authority(1), persisted_at_utc=_TIMESTAMP_1
                        )
                        with self.assertRaisesRegex(
                            LkgPhase3PersistenceError,
                            "must be later than the previous record",
                        ):
                            store.append(
                                _authority(2),
                                persisted_at_utc=rejected_timestamp,
                            )
                        self.assertEqual(store.load_all(), (first.reference,))

    def test_chain_tamper_is_detected_after_coherent_record_rehash(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path) as store:
                store.append(_authority(1), persisted_at_utc=_TIMESTAMP_1)
                store.append(_authority(2), persisted_at_utc=_TIMESTAMP_2)
            connection = sqlite3.connect(path)
            try:
                stored_json = connection.execute(
                    f"SELECT record_document_json FROM {_TABLE} WHERE sequence_number=1"
                ).fetchone()[0]
            finally:
                connection.close()
            document = json.loads(stored_json)
            document["previous_record_digest"] = "a" * 64
            replacement_digest = _canonical_digest(document)
            _raw_mutate_preserving_triggers(
                path,
                f"UPDATE {_TABLE} SET previous_record_digest=?, "
                "canonical_record_digest=?, record_document_json=? "
                "WHERE sequence_number=1",
                (
                    "a" * 64,
                    replacement_digest,
                    canonical_json_bytes(document).decode("utf-8"),
                ),
            )

            with self.assertRaisesRegex(
                LkgPhase3PersistenceError, "previous digest is invalid"
            ):
                LkgPhase3AuthorityReferenceStore(path)

    def test_denormalized_row_tamper_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path) as store:
                store.append(_authority(), persisted_at_utc=_TIMESTAMP_1)
            _raw_mutate_preserving_triggers(
                path,
                f"UPDATE {_TABLE} SET source_run_id='tampered-run'",
            )

            with self.assertRaisesRegex(
                LkgPhase3PersistenceError, "does not match canonical JSON"
            ):
                LkgPhase3AuthorityReferenceStore(path)

    def test_json_and_digest_tamper_are_detected(self) -> None:
        for tamper_kind in ("json", "digest"):
            with self.subTest(tamper_kind=tamper_kind):
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "phase3-authority.db"
                    with LkgPhase3AuthorityReferenceStore(path) as store:
                        store.append(_authority(), persisted_at_utc=_TIMESTAMP_1)
                    if tamper_kind == "json":
                        connection = sqlite3.connect(path)
                        try:
                            stored_json = connection.execute(
                                f"SELECT record_document_json FROM {_TABLE}"
                            ).fetchone()[0]
                        finally:
                            connection.close()
                        document = json.loads(stored_json)
                        document["source_run_id"] = "tampered-run"
                        _raw_mutate_preserving_triggers(
                            path,
                            f"UPDATE {_TABLE} SET record_document_json=?",
                            (canonical_json_bytes(document).decode("utf-8"),),
                        )
                    else:
                        _raw_mutate_preserving_triggers(
                            path,
                            f"UPDATE {_TABLE} SET canonical_record_digest=?",
                            ("f" * 64,),
                        )

                    with self.assertRaises(LkgPhase3PersistenceError):
                        LkgPhase3AuthorityReferenceStore(path)

    def test_noncanonical_json_is_detected_even_when_semantics_match(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path) as store:
                store.append(_authority(), persisted_at_utc=_TIMESTAMP_1)
            connection = sqlite3.connect(path)
            try:
                document = json.loads(
                    connection.execute(
                        f"SELECT record_document_json FROM {_TABLE}"
                    ).fetchone()[0]
                )
            finally:
                connection.close()
            noncanonical = json.dumps(document, sort_keys=False, indent=2) + "\n"
            _raw_mutate_preserving_triggers(
                path,
                f"UPDATE {_TABLE} SET record_document_json=?",
                (noncanonical,),
            )

            with self.assertRaisesRegex(
                LkgPhase3PersistenceError, "not byte-identical canonical JSON"
            ):
                LkgPhase3AuthorityReferenceStore(path)

    def test_historical_threshold_stratum_does_not_depend_on_live_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path) as store:
                store.append(_authority(), persisted_at_utc=_TIMESTAMP_1)
            connection = sqlite3.connect(path)
            try:
                document = json.loads(
                    connection.execute(
                        f"SELECT record_document_json FROM {_TABLE}"
                    ).fetchone()[0]
                )
            finally:
                connection.close()
            document["threshold_stratum"] = "retired-historical-stratum"
            replacement_digest = _canonical_digest(document)
            _raw_mutate_preserving_triggers(
                path,
                f"UPDATE {_TABLE} SET threshold_stratum=?, "
                "canonical_record_digest=?, record_document_json=?",
                (
                    "retired-historical-stratum",
                    replacement_digest,
                    canonical_json_bytes(document).decode("utf-8"),
                ),
            )

            with LkgPhase3AuthorityReferenceStore(path) as reopened:
                reference = reopened.load_latest()
            assert reference is not None
            self.assertEqual(
                reference.threshold_stratum, "retired-historical-stratum"
            )

    def test_update_and_delete_are_rejected_by_sql_triggers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path) as store:
                store.append(_authority(), persisted_at_utc=_TIMESTAMP_1)
            connection = sqlite3.connect(path)
            try:
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        f"UPDATE {_TABLE} SET persisted_at_utc=?",
                        (_TIMESTAMP_2,),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(f"DELETE FROM {_TABLE}")
            finally:
                connection.close()

    def test_schema_tamper_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path):
                pass
            connection = sqlite3.connect(path)
            try:
                connection.execute("DROP TRIGGER trg_lkg_phase3_reference_no_update")
                connection.commit()
            finally:
                connection.close()

            with self.assertRaisesRegex(
                LkgPhase3PersistenceError, "schema"
            ):
                LkgPhase3AuthorityReferenceStore(path)

    def test_concurrent_distinct_appends_serialize_into_one_valid_chain(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path):
                pass
            barrier = threading.Barrier(2)
            first_appended = threading.Event()

            def append(identifier: int, timestamp: str):
                barrier.wait(timeout=5)
                if identifier == 2:
                    first_appended.wait(timeout=5)
                with LkgPhase3AuthorityReferenceStore(path) as store:
                    result = store.append(
                        _authority(identifier), persisted_at_utc=timestamp
                    )
                if identifier == 1:
                    first_appended.set()
                return result

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = (
                    executor.submit(append, 1, _TIMESTAMP_1),
                    executor.submit(append, 2, _TIMESTAMP_2),
                )
                results = tuple(future.result(timeout=10) for future in futures)

            self.assertTrue(all(result.appended for result in results))
            with LkgPhase3AuthorityReferenceStore(path) as store:
                records = store.load_all()
            self.assertEqual(len(records), 2)
            self.assertEqual(records[0].sequence_number, 0)
            self.assertEqual(records[1].sequence_number, 1)
            self.assertEqual(
                records[1].previous_record_digest,
                records[0].canonical_record_digest,
            )

    def test_concurrent_same_authority_is_one_append_and_one_idempotent_replay(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path):
                pass
            barrier = threading.Barrier(2)
            authority = _authority()

            def append(timestamp: str):
                barrier.wait(timeout=5)
                with LkgPhase3AuthorityReferenceStore(path) as store:
                    return store.append(authority, persisted_at_utc=timestamp)

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = (
                    executor.submit(append, _TIMESTAMP_1),
                    executor.submit(append, _TIMESTAMP_2),
                )
                results = tuple(future.result(timeout=10) for future in futures)

            self.assertEqual(sorted(result.appended for result in results), [False, True])
            self.assertEqual(results[0].reference, results[1].reference)
            with LkgPhase3AuthorityReferenceStore(path) as store:
                self.assertEqual(store.load_all(), (results[0].reference,))

    def test_storage_read_errors_use_persistence_error_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path) as store:
                with mock.patch.object(
                    store,
                    "_verify_schema",
                    side_effect=sqlite3.OperationalError("simulated read failure"),
                ):
                    with self.assertRaisesRegex(
                        LkgPhase3PersistenceError,
                        "failed to load authority references",
                    ):
                        store.load_all()

    def test_malformed_or_non_d1_authority_is_rejected_before_db_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            malformed = object.__new__(LkgPhase3Authority)
            with LkgPhase3AuthorityReferenceStore(path) as store:
                for candidate in (object(), malformed):
                    with self.subTest(candidate=type(candidate).__name__):
                        with self.assertRaises(LkgPhase3PersistenceError):
                            store.append(  # type: ignore[arg-type]
                                candidate,
                                persisted_at_utc=_TIMESTAMP_1,
                            )
                self.assertEqual(store.load_all(), ())

    def test_invalid_timestamp_is_rejected_without_append(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path) as store:
                with self.assertRaises(LkgPhase3PersistenceError):
                    store.append(
                        _authority(), persisted_at_utc="2026-13-99T00:00:00Z"
                    )
                self.assertEqual(store.load_all(), ())

    def test_sql_schema_contains_no_qualification_verdict_columns(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path):
                pass
            connection = sqlite3.connect(path)
            try:
                columns = {
                    row[1]
                    for row in connection.execute(
                        f"PRAGMA table_info({_TABLE})"
                    ).fetchall()
                }
            finally:
                connection.close()
            self.assertNotIn("qualified", columns)
            self.assertNotIn("status", columns)
            self.assertNotIn("recall", columns)
            self.assertNotIn("latency", columns)

    def test_loaded_reference_cannot_be_passed_as_d1_authority(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase3-authority.db"
            with LkgPhase3AuthorityReferenceStore(path) as store:
                reference = store.append(
                    _authority(), persisted_at_utc=_TIMESTAMP_1
                ).reference
                with self.assertRaises(LkgPhase3PersistenceError):
                    store.append(  # type: ignore[arg-type]
                        reference,
                        persisted_at_utc=_TIMESTAMP_2,
                    )
            self.assertIsInstance(reference, PersistedLkgPhase3AuthorityReference)
            self.assertNotIsInstance(reference, LkgPhase3Authority)


if __name__ == "__main__":
    unittest.main()
