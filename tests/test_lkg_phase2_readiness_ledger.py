"""TDD coverage for Checkpoint B: Phase2ReadinessLedger.

Every test follows the mandated ordering discipline, matching the real
orchestration contract documented in ``lkg_window_readiness.py``'s module
docstring: create an unsealed Phase-1 ledger, append the relevant window's
200 positions, THEN capture readiness for that window (via the real
provider's ``capture_or_return``) -- never the reverse -- seal Phase 1,
construct/bind the Phase-2 ledger, then ingest (which reaches evidence
only via ``provider.lookup``). No test ever captures readiness for the
first time during post-seal ingestion, and no test ever captures readiness
before its window's attempts are durably appended.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from vdbench import lkg_phase2_readiness_ledger
from vdbench.config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from vdbench.lkg_phase2_readiness_ledger import (
    Phase2ReadinessLedger,
    Phase2ReadinessLedgerError,
    _require_readiness_chronology,
)
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
from vdbench.lkg_qualification_seal import LkgSealCompletionState
from vdbench.lkg_run_binding import LkgRunBinding, lkg_ordered_query_ids_sha256
from vdbench.lkg_window_readiness import FakeLkgWindowOperationalReadinessProvider

_ORDERED_QUERY_IDS = tuple(range(10, 2410))  # 2400 queries, the fixed geometry


def _ordered_query_ids_sha256(ids: tuple[int, ...]) -> str:
    return lkg_ordered_query_ids_sha256(ids)


def _configuration(**overrides) -> SearchConfiguration:
    fields = {
        "metric": Metric.L2, "threshold_label": "target-075", "radius": 5.0, "index_track": IndexTrack.HNSW, "ef": 400
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
        "qualification_ordered_query_ids_sha256": _ordered_query_ids_sha256(_ORDERED_QUERY_IDS),
        "qualification_query_array_sha256": "c" * 64,
        "qualification_expected_query_count": len(_ORDERED_QUERY_IDS),
        "environment_identity": "env-v1",
        "source_revision": "deadbeef",
    }
    fields.update(overrides)
    return LkgRunBinding(**fields)


_BINDING = _binding()


def _success_attempt(query_id, attempt_sequence, binding=_BINDING):
    observation = build_lkg_query_observation(
        query_id=query_id, metric=Metric.L2, threshold_stratum="target-075", ef=400,
        recall=1.0, latency_ms=1.0, start_ns=attempt_sequence, end_ns=attempt_sequence + 1,
        exact_cardinality=10, threshold_violation_count=0,
    )
    return build_lkg_query_attempt(
        query_id=query_id, attempt_sequence=attempt_sequence, attempt_number=1,
        status=LkgAttemptStatus.SUCCESS, run_binding_sha256=binding.sha256, observation=observation,
    )


class Phase2ReadinessLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.phase1_path = Path(self._tmp.name) / "phase1.sqlite3"
        self.phase2_path = Path(self._tmp.name) / "phase2.sqlite3"

    def _phase1_ledger(self) -> LkgQualificationLedger:
        return LkgQualificationLedger(self.phase1_path, run_binding=_BINDING, ordered_query_ids=_ORDERED_QUERY_IDS)

    def _append_window(self, ledger: LkgQualificationLedger, window_index: int) -> None:
        base = window_index * 200
        for offset in range(200):
            sequence = base + offset
            result = ledger.append(_success_attempt(_ORDERED_QUERY_IDS[sequence], sequence))
            self.assertTrue(result.accepted)

    def _capture_window(self, provider, window_index: int, check_id: str):
        return provider.capture_or_return(
            readiness_check_id=check_id,
            source_run_id="run-1",
            source_run_binding_sha256=_BINDING.sha256,
            window_index=window_index,
            epoch_index=window_index // 6,
            first_attempt_sequence=window_index * 200,
            last_attempt_sequence=window_index * 200 + 199,
        )

    def _prepare_sealed_run_with_window_0(self):
        """The common baseline: append window 0's 200 positions, capture
        its readiness (real orchestration order -- readiness capture
        happens only after the window's durable attempt traversal
        completes), seal (INCOMPLETE_NO_FAILURE -- only 1 of 12 windows
        attempted), and bind a Phase2ReadinessLedger. Returns (phase1,
        provider, phase2)."""

        phase1 = self._phase1_ledger()
        provider = FakeLkgWindowOperationalReadinessProvider()
        self._append_window(phase1, 0)
        self._capture_window(provider, 0, "chk-w0")
        seal_lkg_qualification_run(
            phase1, expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
            seal_reason="WINDOW_0_ONLY_FOR_TEST",
        )
        phase2 = Phase2ReadinessLedger(self.phase2_path, phase1_ledger=phase1)
        return phase1, provider, phase2

    def _tamper_raw(self, path: Path, statements: list[tuple[str, tuple]], *, drop_triggers: list[str], restore_triggers: list[str], fk_off: bool = False) -> None:
        connection = sqlite3.connect(path)
        try:
            if fk_off:
                connection.execute("PRAGMA foreign_keys = OFF")
            for trigger in drop_triggers:
                connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
            for sql, params in statements:
                connection.execute(sql, params)
            connection.commit()
        finally:
            for restore_sql in restore_triggers:
                connection.execute(restore_sql)
            connection.commit()
            connection.close()

    _RESTORE_SOURCE_BINDING_NO_UPDATE = """
        CREATE TRIGGER phase2_source_binding_no_update
        BEFORE UPDATE ON phase2_source_binding
        BEGIN SELECT RAISE(ABORT, 'phase2 source binding is append-only'); END
        """
    _RESTORE_INGESTION_NO_UPDATE = """
        CREATE TRIGGER window_readiness_ingestion_no_update
        BEFORE UPDATE ON window_readiness_ingestion
        BEGIN SELECT RAISE(ABORT, 'window readiness ingestion is append-only'); END
        """

    # -- alias guard -------------------------------------------------------

    def test_path_aliasing_with_phase1_ledger_is_rejected(self) -> None:
        phase1 = self._phase1_ledger()
        with self.assertRaises(ContractViolation):
            Phase2ReadinessLedger(self.phase1_path, phase1_ledger=phase1)

    def _assert_alias_rejected_before_schema_init(self, aliased_path) -> None:
        phase1 = self._phase1_ledger()
        with self.assertRaises(ContractViolation):
            Phase2ReadinessLedger(aliased_path, phase1_ledger=phase1)
        connection = sqlite3.connect(self.phase1_path)
        try:
            objects = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table', 'trigger')"
                )
            }
        finally:
            connection.close()
        self.assertNotIn("phase2_source_binding", objects)
        self.assertNotIn("window_readiness_ingestion", objects)

    def test_path_aliasing_relative_dotdot_spelling_is_rejected(self) -> None:
        aliased = self.phase1_path.parent / "nonexistent_subdir" / ".." / "phase1.sqlite3"
        self.assertNotEqual(str(aliased), str(self.phase1_path))
        self.assertEqual(aliased.resolve(), self.phase1_path.resolve())
        self._assert_alias_rejected_before_schema_init(aliased)

    def test_path_aliasing_symlink_is_rejected(self) -> None:
        symlink_path = self.phase1_path.parent / "phase1_symlink.sqlite3"
        try:
            symlink_path.symlink_to(self.phase1_path)
        except (OSError, NotImplementedError) as exc:
            self.skipTest(f"platform does not support symlink creation: {exc}")
        self._assert_alias_rejected_before_schema_init(symlink_path)

    # -- binding -------------------------------------------------------------

    def test_binding_against_freshly_sealed_run_succeeds(self) -> None:
        _phase1, _provider, _phase2 = self._prepare_sealed_run_with_window_0()
        self.assertTrue(self.phase2_path.exists())

    def test_binding_against_unsealed_run_raises_source_seal_missing(self) -> None:
        phase1 = self._phase1_ledger()
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            Phase2ReadinessLedger(self.phase2_path, phase1_ledger=phase1)
        self.assertEqual(str(cm.exception), "PHASE2_SOURCE_SEAL_MISSING")
        self.assertIsInstance(cm.exception.__cause__, LkgQualificationLedgerError)
        self.assertEqual(str(cm.exception.__cause__), "LKG_SEAL_MISSING")

    def test_reopening_validates_instead_of_rebinding(self) -> None:
        phase1, _provider, _phase2 = self._prepare_sealed_run_with_window_0()
        Phase2ReadinessLedger(self.phase2_path, phase1_ledger=phase1)  # reopen, must not raise
        connection = sqlite3.connect(self.phase2_path)
        try:
            count = connection.execute("SELECT COUNT(*) FROM phase2_source_binding").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(count, 1)

    def test_binding_detects_tampered_phase1_seal(self) -> None:
        _phase1, _provider, phase2 = self._prepare_sealed_run_with_window_0()
        self._tamper_raw(
            self.phase1_path,
            [("UPDATE lkg_qualification_attempts SET query_id = 999999 WHERE insertion_seq = 1", ())],
            drop_triggers=["lkg_qualification_attempts_no_update"],
            restore_triggers=[
                """
                CREATE TRIGGER lkg_qualification_attempts_no_update
                BEFORE UPDATE ON lkg_qualification_attempts
                BEGIN SELECT RAISE(ABORT, 'lkg qualification attempts are append-only'); END
                """
            ],
            fk_off=True,
        )
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "PHASE2_SOURCE_SEAL_UNVERIFIABLE")
        self.assertIsInstance(cm.exception.__cause__, LkgQualificationLedgerError)

    # -- orchestration ordering (capture-point observation) --------------------

    def test_capture_observes_window_complete_and_run_unsealed(self) -> None:
        """Proves the CURRENT orchestration contract documented in
        lkg_window_readiness.py's module docstring -- it does NOT claim the
        provider mechanically enforces Phase-1 completeness. A builder
        closure, invoked synchronously inside capture_or_return, observes
        via the real Phase-1 ledger's own public read API that: (1) exactly
        the window's 200 durable positions are already present, each
        SUCCESS, at the exact expected sequence numbers, and (2) the run
        is still unsealed (verify_seal raises LKG_SEAL_MISSING) at the
        moment of capture. No caller-supplied flag asserts this -- the
        closure independently re-derives both facts from Phase-1 itself."""

        phase1 = self._phase1_ledger()
        self._append_window(phase1, 0)

        observed: dict[str, object] = {}

        def observing_builder(**kwargs):
            window_index = kwargs["window_index"]
            base = window_index * 200
            records = phase1.records()
            window_records = [r for r in records if base <= r.attempt_sequence < base + 200]
            observed["window_record_count"] = len(window_records)
            observed["all_success"] = all(r.status == LkgAttemptStatus.SUCCESS for r in window_records)
            observed["sequence_set_matches"] = {r.attempt_sequence for r in window_records} == set(
                range(base, base + 200)
            )
            try:
                verify_seal(phase1)
                observed["still_unsealed"] = False
            except LkgQualificationLedgerError as exc:
                observed["still_unsealed"] = str(exc) == "LKG_SEAL_MISSING"

            from vdbench.lkg_window_readiness import _default_readiness_builder

            return _default_readiness_builder(**kwargs)

        provider = FakeLkgWindowOperationalReadinessProvider(builder=observing_builder)
        self._capture_window(provider, 0, "chk-w0")

        self.assertEqual(observed["window_record_count"], 200)
        self.assertTrue(observed["all_success"])
        self.assertTrue(observed["sequence_set_matches"])
        self.assertTrue(observed["still_unsealed"])

    # -- ingestion -------------------------------------------------------------

    def test_first_ingestion_succeeds_and_chain_verifies(self) -> None:
        _phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        ingestion = phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
        self.assertEqual(ingestion.window_index, 0)
        verified = phase2.verify_window_readiness_ingestion(0)
        self.assertEqual(verified, ingestion)

    def test_out_of_window_index_order_ingestion_both_verify_and_chain_is_insertion_order(self) -> None:
        """Captures readiness for windows 0 and 1 (append-then-capture,
        both before sealing), then ingests the HIGHER window index first
        and the LOWER window index second -- proving the chain never
        assumes insertion order matches window_index order."""

        phase1 = self._phase1_ledger()
        provider = FakeLkgWindowOperationalReadinessProvider()
        self._append_window(phase1, 0)
        self._capture_window(provider, 0, "chk-w0")
        self._append_window(phase1, 1)
        self._capture_window(provider, 1, "chk-w1")
        seal_lkg_qualification_run(
            phase1, expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
            seal_reason="WINDOWS_0_AND_1_OUT_OF_ORDER_FOR_TEST",
        )
        phase2 = Phase2ReadinessLedger(self.phase2_path, phase1_ledger=phase1)

        ingested_window_1 = phase2.ingest_window_readiness(
            provider=provider, readiness_check_id="chk-w1", window_index=1
        )
        ingested_window_0 = phase2.ingest_window_readiness(
            provider=provider, readiness_check_id="chk-w0", window_index=0
        )

        verified_1 = phase2.verify_window_readiness_ingestion(1)
        verified_0 = phase2.verify_window_readiness_ingestion(0)
        self.assertEqual(verified_1, ingested_window_1)
        self.assertEqual(verified_0, ingested_window_0)

        all_ingestions = phase2.all_verified_ingestions()
        self.assertEqual([i.window_index for i in all_ingestions], [1, 0])

        raw = sqlite3.connect(self.phase2_path)
        try:
            rows = raw.execute(
                "SELECT window_index FROM window_readiness_ingestion ORDER BY insertion_seq ASC"
            ).fetchall()
        finally:
            raw.close()
        self.assertEqual([row[0] for row in rows], [1, 0])

    def test_crash_after_commit_before_response_provider_never_reached_on_retry(self) -> None:
        _phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        first = phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)

        class _ExplodingProvider:
            def lookup(self, *, readiness_check_id):
                raise AssertionError("provider.lookup must not be reached on durable replay")

        second = phase2.ingest_window_readiness(provider=_ExplodingProvider(), readiness_check_id="chk-w0", window_index=0)
        self.assertEqual(second, first)
        self.assertEqual(second.ingested_at_utc, first.ingested_at_utc)

    def test_reingest_different_check_id_rejected(self) -> None:
        _phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-other", window_index=0)
        self.assertEqual(str(cm.exception), "WINDOW_READINESS_INGESTION_CHECK_ID_MISMATCH")

    def test_evidence_checked_after_seal_is_rejected(self) -> None:
        phase1 = self._phase1_ledger()
        self._append_window(phase1, 0)
        seal = seal_lkg_qualification_run(
            phase1, expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
            seal_reason="WINDOW_0_ONLY_FOR_TEST",
        )

        # Hand-craft evidence whose checked_at_utc is deliberately AFTER
        # the seal's sealed_at_utc -- captured (per the API) before
        # sealing in wall-clock terms is not enforceable by the fake
        # provider's own clock, so this test constructs the violation
        # directly to prove the ledger's own re-check catches it.
        from vdbench.lkg_window_readiness import (
            lkg_window_operational_readiness_evidence_from_payload,
            readiness_payload_document_digest,
        )

        after_seal_instant = datetime.fromisoformat(seal.sealed_at_utc).replace(
            year=datetime.fromisoformat(seal.sealed_at_utc).year + 1
        )
        after_seal_utc = after_seal_instant.strftime("%Y-%m-%dT%H:%M:%S.") + f"{after_seal_instant.microsecond:06d}Z"

        payload = {
            "readiness_schema_version": 1, "source_run_id": "run-1", "source_run_binding_sha256": _BINDING.sha256,
            "window_index": 0, "epoch_index": 0, "first_attempt_sequence": 0, "last_attempt_sequence": 199,
            "readiness_check_id": "chk-late", "provider_run_id": "provider-run-late",
            "health_checked": True, "health_passed": True,
            "health_evidence_source_identity": "x", "health_evidence_source_digest": "a" * 64,
            "rollback_tested": True, "rollback_ready": True,
            "rollback_evidence_source_identity": "y", "rollback_evidence_source_digest": "b" * 64,
            "checked_at_utc": after_seal_utc, "check_start_ns": 0, "check_end_ns": 1, "reason_codes": [],
        }
        digest = readiness_payload_document_digest(payload)
        late_evidence = lkg_window_operational_readiness_evidence_from_payload(payload, canonical_document_digest=digest)

        class _StaticProvider:
            def lookup(self, *, readiness_check_id):
                return late_evidence

        phase2 = Phase2ReadinessLedger(self.phase2_path, phase1_ledger=phase1)
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.ingest_window_readiness(provider=_StaticProvider(), readiness_check_id="chk-late", window_index=0)
        self.assertEqual(str(cm.exception), "READINESS_CHECKED_AFTER_SEAL")

    def test_ingested_at_utc_generated_before_seal_is_rejected(self) -> None:
        _phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        with (
            mock.patch.object(
            lkg_phase2_readiness_ledger, "_current_rfc3339_utc", return_value="2000-01-01T00:00:00Z"
        ),
            self.assertRaises(Phase2ReadinessLedgerError) as cm,
        ):
            phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
        self.assertEqual(str(cm.exception), "INGESTION_TIMESTAMP_BEFORE_SEAL")

    # -- chronology helper (direct, exhaustive boundary matrix) -------------

    def test_chronology_checked_before_sealed_before_ingested_accepted(self) -> None:
        _require_readiness_chronology(
            checked_at_utc="2026-01-01T00:00:00Z", sealed_at_utc="2026-01-01T00:00:01Z",
            ingested_at_utc="2026-01-01T00:00:02Z",
        )

    def test_chronology_checked_equals_sealed_accepted(self) -> None:
        _require_readiness_chronology(
            checked_at_utc="2026-01-01T00:00:00Z", sealed_at_utc="2026-01-01T00:00:00Z",
            ingested_at_utc="2026-01-01T00:00:01Z",
        )

    def test_chronology_sealed_equals_ingested_accepted(self) -> None:
        _require_readiness_chronology(
            checked_at_utc="2026-01-01T00:00:00Z", sealed_at_utc="2026-01-01T00:00:01Z",
            ingested_at_utc="2026-01-01T00:00:01Z",
        )

    def test_chronology_checked_after_sealed_rejected(self) -> None:
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            _require_readiness_chronology(
                checked_at_utc="2026-01-01T00:00:02Z", sealed_at_utc="2026-01-01T00:00:01Z",
                ingested_at_utc="2026-01-01T00:00:03Z",
            )
        self.assertEqual(str(cm.exception), "READINESS_CHECKED_AFTER_SEAL")

    def test_chronology_ingested_before_sealed_rejected(self) -> None:
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            _require_readiness_chronology(
                checked_at_utc="2026-01-01T00:00:00Z", sealed_at_utc="2026-01-01T00:00:02Z",
                ingested_at_utc="2026-01-01T00:00:01Z",
            )
        self.assertEqual(str(cm.exception), "INGESTION_TIMESTAMP_BEFORE_SEAL")

    def test_chronology_mixed_fractional_precision_uses_parsed_instants(self) -> None:
        # checked has no fraction, sealed has one -- chronologically equal
        # instants that would misorder under naive string comparison.
        _require_readiness_chronology(
            checked_at_utc="2026-01-01T00:00:00Z", sealed_at_utc="2026-01-01T00:00:00.000000Z",
            ingested_at_utc="2026-01-01T00:00:00.000001Z",
        )

    # -- trigger enforcement, no bypass ---------------------------------------

    def test_source_binding_update_rejected_by_trigger(self) -> None:
        self._prepare_sealed_run_with_window_0()
        connection = sqlite3.connect(self.phase2_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE phase2_source_binding SET source_run_id = 'x'")
        finally:
            connection.rollback()
            connection.close()

    def test_source_binding_delete_rejected_by_trigger(self) -> None:
        self._prepare_sealed_run_with_window_0()
        connection = sqlite3.connect(self.phase2_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM phase2_source_binding")
        finally:
            connection.rollback()
            connection.close()

    def test_ingestion_update_rejected_by_trigger(self) -> None:
        _phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
        connection = sqlite3.connect(self.phase2_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE window_readiness_ingestion SET readiness_check_id = 'x'")
        finally:
            connection.rollback()
            connection.close()

    def test_ingestion_delete_rejected_by_trigger(self) -> None:
        _phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
        connection = sqlite3.connect(self.phase2_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM window_readiness_ingestion")
        finally:
            connection.rollback()
            connection.close()

    def test_second_source_binding_row_different_run_id_fk_off_rejected_by_single_row_trigger(self) -> None:
        """A duplicate primary key alone would not prove the single-row
        trigger fires -- uses a DIFFERENT source_run_id with foreign_keys
        disabled, so only the trigger stands in the way."""

        self._prepare_sealed_run_with_window_0()
        connection = sqlite3.connect(self.phase2_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO phase2_source_binding(
                        source_run_id, source_binding_schema_version, source_run_binding_sha256,
                        source_phase1_ledger_schema_version, source_seal_schema_version,
                        source_run_seal_digest, source_sealed_chain_head_sha256,
                        qualification_ordered_query_ids_sha256, expected_query_count,
                        binding_document_json, canonical_source_binding_digest
                    ) VALUES ('a-different-run-id', 1, ?, 5, 1, ?, ?, ?, 2400, '{}', ?)
                    """,
                    ("a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64),
                )
        finally:
            connection.rollback()
            connection.close()

    # -- external-writer tamper detection (FK off, triggers dropped) ---------

    def test_binding_document_json_missing_field_corrupted(self) -> None:
        _phase1, _provider, phase2 = self._prepare_sealed_run_with_window_0()
        self._tamper_binding_document(lambda doc: {k: v for k, v in doc.items() if k != "source_run_seal_digest"})
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "PHASE2_SOURCE_BINDING_CORRUPTED")

    def test_binding_document_json_unknown_field_corrupted(self) -> None:
        _phase1, _provider, phase2 = self._prepare_sealed_run_with_window_0()
        def add_field(doc):
            doc = dict(doc)
            doc["bogus"] = "x"
            return doc
        self._tamper_binding_document(add_field)
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "PHASE2_SOURCE_BINDING_CORRUPTED")

    def test_binding_document_json_noncanonical_corrupted(self) -> None:
        _phase1, _provider, phase2 = self._prepare_sealed_run_with_window_0()
        connection = sqlite3.connect(self.phase2_path)
        try:
            connection.execute("DROP TRIGGER IF EXISTS phase2_source_binding_no_update")
            row = connection.execute("SELECT binding_document_json FROM phase2_source_binding").fetchone()
            reordered = dict(reversed(list(json.loads(row[0]).items())))
            connection.execute(
                "UPDATE phase2_source_binding SET binding_document_json = ?", (json.dumps(reordered, indent=2),)
            )
            connection.commit()
        finally:
            connection.execute(self._RESTORE_SOURCE_BINDING_NO_UPDATE)
            connection.commit()
            connection.close()
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "PHASE2_SOURCE_BINDING_CORRUPTED")

    def test_binding_document_json_value_tamper_columns_unchanged_corrupted(self) -> None:
        _phase1, _provider, phase2 = self._prepare_sealed_run_with_window_0()
        def change_value(doc):
            doc = dict(doc)
            doc["source_run_id"] = "run-1-but-tampered"
            return doc
        self._tamper_binding_document(change_value, canonical=True)
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "PHASE2_SOURCE_BINDING_CORRUPTED")

    def test_binding_denormalized_column_tamper_with_document_unchanged_is_column_mismatch(self) -> None:
        _phase1, _provider, phase2 = self._prepare_sealed_run_with_window_0()
        connection = sqlite3.connect(self.phase2_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("DROP TRIGGER IF EXISTS phase2_source_binding_no_update")
            connection.execute(
                "UPDATE phase2_source_binding SET source_run_seal_digest = ?", ("9" * 64,)
            )
            connection.commit()
        finally:
            connection.execute(self._RESTORE_SOURCE_BINDING_NO_UPDATE)
            connection.commit()
            connection.close()
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "PHASE2_SOURCE_BINDING_COLUMN_MISMATCH")

    def _tamper_binding_document(self, mutator, *, canonical: bool = False) -> None:
        from vdbench.artifacts import canonical_json_bytes

        connection = sqlite3.connect(self.phase2_path)
        try:
            connection.execute("DROP TRIGGER IF EXISTS phase2_source_binding_no_update")
            row = connection.execute("SELECT binding_document_json FROM phase2_source_binding").fetchone()
            mutated = mutator(json.loads(row[0]))
            tampered_json = (
                canonical_json_bytes(mutated).decode("utf-8") if canonical else json.dumps(mutated)
            )
            connection.execute("UPDATE phase2_source_binding SET binding_document_json = ?", (tampered_json,))
            connection.commit()
        finally:
            connection.execute(self._RESTORE_SOURCE_BINDING_NO_UPDATE)
            connection.commit()
            connection.close()

    def _tamper_ingestion_document(self, mutator, *, canonical: bool = False) -> None:
        from vdbench.artifacts import canonical_json_bytes

        connection = sqlite3.connect(self.phase2_path)
        try:
            connection.execute("DROP TRIGGER IF EXISTS window_readiness_ingestion_no_update")
            row = connection.execute(
                "SELECT ingestion_document_json FROM window_readiness_ingestion WHERE window_index = 0"
            ).fetchone()
            mutated = mutator(json.loads(row[0]))
            tampered_json = (
                canonical_json_bytes(mutated).decode("utf-8") if canonical else json.dumps(mutated)
            )
            connection.execute(
                "UPDATE window_readiness_ingestion SET ingestion_document_json = ? WHERE window_index = 0",
                (tampered_json,),
            )
            connection.commit()
        finally:
            connection.execute(self._RESTORE_INGESTION_NO_UPDATE)
            connection.commit()
            connection.close()

    def test_ingestion_document_json_missing_field_corrupted(self) -> None:
        _phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
        self._tamper_ingestion_document(lambda doc: {k: v for k, v in doc.items() if k != "ingested_at_utc"})
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "WINDOW_READINESS_INGESTION_CORRUPTED")

    def test_ingestion_document_json_unknown_field_corrupted(self) -> None:
        _phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
        def add_field(doc):
            doc = dict(doc)
            doc["bogus"] = "x"
            return doc
        self._tamper_ingestion_document(add_field)
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "WINDOW_READINESS_INGESTION_CORRUPTED")

    def test_ingestion_document_json_noncanonical_corrupted(self) -> None:
        _phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
        self._tamper_ingestion_document(lambda doc: dict(reversed(list(doc.items()))))
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "WINDOW_READINESS_INGESTION_CORRUPTED")

    def test_ingestion_denormalized_column_tamper_is_column_mismatch(self) -> None:
        _phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
        connection = sqlite3.connect(self.phase2_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("DROP TRIGGER IF EXISTS window_readiness_ingestion_no_update")
            connection.execute(
                "UPDATE window_readiness_ingestion SET source_run_seal_digest = ? WHERE window_index = 0",
                ("9" * 64,),
            )
            connection.commit()
        finally:
            connection.execute(self._RESTORE_INGESTION_NO_UPDATE)
            connection.commit()
            connection.close()
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "WINDOW_READINESS_INGESTION_COLUMN_MISMATCH")

    # -- chain tamper, isolated to chain columns only -------------------------

    def _prepare_two_ingested_windows(self):
        phase1 = self._phase1_ledger()
        provider = FakeLkgWindowOperationalReadinessProvider()
        self._append_window(phase1, 0)
        self._capture_window(provider, 0, "chk-w0")
        self._append_window(phase1, 1)
        self._capture_window(provider, 1, "chk-w1")
        seal_lkg_qualification_run(
            phase1, expected_completion_state=LkgSealCompletionState.INCOMPLETE_NO_FAILURE,
            seal_reason="WINDOWS_0_AND_1_FOR_TEST",
        )
        phase2 = Phase2ReadinessLedger(self.phase2_path, phase1_ledger=phase1)
        phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
        phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w1", window_index=1)
        return phase1, provider, phase2

    def _tamper_chain_column_only(self, column: str) -> None:
        connection = sqlite3.connect(self.phase2_path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("DROP TRIGGER IF EXISTS window_readiness_ingestion_no_update")
            connection.execute(
                f"UPDATE window_readiness_ingestion SET {column} = ? WHERE window_index = 0", ("9" * 64,)
            )
            connection.commit()
        finally:
            connection.execute(self._RESTORE_INGESTION_NO_UPDATE)
            connection.commit()
            connection.close()

    def test_chain_previous_tamper_isolated_is_chain_invalid(self) -> None:
        _phase1, _provider, phase2 = self._prepare_two_ingested_windows()
        self._tamper_chain_column_only("previous_chain_sha256")
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "WINDOW_READINESS_INGESTION_CHAIN_INVALID")

    def test_chain_sha256_tamper_isolated_is_chain_invalid(self) -> None:
        _phase1, _provider, phase2 = self._prepare_two_ingested_windows()
        self._tamper_chain_column_only("chain_sha256")
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.all_verified_ingestions()
        self.assertEqual(str(cm.exception), "WINDOW_READINESS_INGESTION_CHAIN_INVALID")

    def test_untampered_window_verification_fails_when_another_row_chain_damaged(self) -> None:
        _phase1, _provider, phase2 = self._prepare_two_ingested_windows()
        # Damage window 0's chain_sha256; verifying window 1 (untouched)
        # must still fail, since verification always re-derives the
        # COMPLETE chain, not merely the requested row's own link.
        self._tamper_chain_column_only("chain_sha256")
        with self.assertRaises(Phase2ReadinessLedgerError) as cm:
            phase2.verify_window_readiness_ingestion(1)
        self.assertEqual(str(cm.exception), "WINDOW_READINESS_INGESTION_CHAIN_INVALID")

    # -- concurrency: real APIs, real synchronization -------------------------

    def test_ingestion_holds_lock_blocks_concurrent_operation(self) -> None:
        phase1, provider, phase2 = self._prepare_sealed_run_with_window_0()
        blocked_phase2 = Phase2ReadinessLedger(self.phase2_path, phase1_ledger=phase1, lock_timeout_seconds=0.5)

        lock_held = threading.Event()
        release = threading.Event()
        original = lkg_phase2_readiness_ledger.Phase2ReadinessLedger._verified_ingestion_chain_locked

        def paused(self, connection, binding):
            result = original(self, connection, binding)
            lock_held.set()
            if not release.wait(timeout=10):
                raise AssertionError("ingestion was never released")
            return result

        results: list = []
        errors: list = []

        def run_ingest() -> None:
            try:
                results.append(
                    phase2.ingest_window_readiness(provider=provider, readiness_check_id="chk-w0", window_index=0)
                )
            except Exception as exc:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
                errors.append(exc)

        with mock.patch.object(
            lkg_phase2_readiness_ledger.Phase2ReadinessLedger, "_verified_ingestion_chain_locked", paused
        ):
            thread = threading.Thread(target=run_ingest, name="ingest-A")
            thread.start()
            self.assertTrue(lock_held.wait(timeout=5), "ingestion never acquired its lock in time")

            with self.assertRaises(Phase2ReadinessLedgerError) as cm:
                blocked_phase2.verify_window_readiness_ingestion(0)
            self.assertEqual(str(cm.exception), "PHASE2_LEDGER_UNAVAILABLE")

            release.set()
            thread.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].window_index, 0)

    def test_two_concurrent_first_ingestions_serialize_to_one_original_and_one_idempotent(self) -> None:
        phase1, provider, phase2_a = self._prepare_sealed_run_with_window_0()
        phase2_b = Phase2ReadinessLedger(self.phase2_path, phase1_ledger=phase1, lock_timeout_seconds=3.0)

        lock_held = threading.Event()
        release = threading.Event()
        original = lkg_phase2_readiness_ledger.Phase2ReadinessLedger._verified_ingestion_chain_locked
        first_thread_name = "ingest-first"

        def thread_aware_paused(self, connection, binding):
            result = original(self, connection, binding)
            if threading.current_thread().name == first_thread_name:
                lock_held.set()
                if not release.wait(timeout=10):
                    raise AssertionError("first ingestion was never released")
            return result

        results: dict[str, object] = {}
        errors: dict[str, Exception] = {}

        def call_ingest(name: str, ledger: Phase2ReadinessLedger) -> None:
            try:
                results[name] = ledger.ingest_window_readiness(
                    provider=provider, readiness_check_id="chk-w0", window_index=0
                )
            except Exception as exc:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
                errors[name] = exc

        with mock.patch.object(
            lkg_phase2_readiness_ledger.Phase2ReadinessLedger,
            "_verified_ingestion_chain_locked",
            thread_aware_paused,
        ):
            first_thread = threading.Thread(target=call_ingest, args=("first", phase2_a), name=first_thread_name)
            first_thread.start()
            self.assertTrue(lock_held.wait(timeout=5), "first ingestion never acquired its lock in time")

            second_thread = threading.Thread(target=call_ingest, args=("second", phase2_b), name="ingest-second")
            second_thread.start()
            second_thread.join(timeout=0.3)
            self.assertTrue(second_thread.is_alive(), "second ingestion should still be blocked")

            release.set()
            first_thread.join(timeout=10)
            second_thread.join(timeout=10)
            self.assertFalse(second_thread.is_alive())

        self.assertEqual(errors, {})
        self.assertEqual(set(results), {"first", "second"})
        self.assertEqual(results["first"], results["second"])
        self.assertEqual(results["first"].ingested_at_utc, results["second"].ingested_at_utc)

        raw = sqlite3.connect(self.phase2_path)
        try:
            count = raw.execute("SELECT COUNT(*) FROM window_readiness_ingestion").fetchone()[0]
        finally:
            raw.close()
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
