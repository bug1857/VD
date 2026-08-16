"""Hand-checkable tests for the disjoint EXP-009 recall-audit evidence ledger.

This ledger is new evidence infrastructure for the 1,200-query background
recall-audit stream ADR-008 already assigns the recall bound to. It is
deliberately separate from ``Stage4ExecutionLedger`` and never touches that
sealed, hash-chained schema.

Design decisions this file locks in:
- No statistical interpretation (alpha, estimator version) lives here; that
  belongs exclusively to ``Stage4RecallAuditEvaluation``.
- Identity/configuration binding reuses this repository's existing types
  (``SearchConfiguration`` from config.py, ``WorkloadIdentityBinding`` from
  canary_workload.py) instead of inventing a new free-form identity string.
- ``matched_count``/``capped_recall`` are derived by calling
  ``oracle.py::capped_threshold_recall`` directly, never by duplicating its
  formula (its denominator is ``len(oracle_result_ids)``, not a fixed cap).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from vdbench.artifacts import sha256_file
from vdbench.canary_recall_audit_ledger import (
    CanaryRecallAuditLedger,
    RecallAuditAppendResult,
    RecallAuditChainState,
    RecallAuditLedgerError,
    RecallAuditObservation,
    publish_recall_audit_manifest,
)
from vdbench.canary_workload import WorkloadIdentityBinding
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.dataset002 import DATASET002_SCHEMA_VERSION
from vdbench.oracle import capped_threshold_recall


def _search_configuration(**overrides) -> SearchConfiguration:
    fields = {
        "metric": Metric.L2,
        "threshold_label": "target-075",
        "radius": 0.6,
        "index_track": IndexTrack.HNSW,
        "ef": 800,
        "limit": 100,
        "consistency_level": "Strong",
    }
    fields.update(overrides)
    return SearchConfiguration(**fields)


def _identity(**overrides) -> WorkloadIdentityBinding:
    fields = {
        "configuration_identity": "a" * 16,
        "data_identity": "DATASET-001-v1:sha256:" + "b" * 64,
        "flat_binding_id": "c" * 16,
        "hnsw_binding_id": "d" * 16,
    }
    fields.update(overrides)
    return WorkloadIdentityBinding(**fields)


def _digest(ids: tuple[int, ...]) -> str:
    canonical = ",".join(str(i) for i in sorted(set(ids)))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _observation(
    *,
    query_id: int = 7,
    search_configuration: SearchConfiguration | None = None,
    identity: WorkloadIdentityBinding | None = None,
    dataset002_manifest_sha256: str = "e" * 64,
    dataset002_schema_version: int = DATASET002_SCHEMA_VERSION,
    oracle_result_ids: tuple[int, ...] = tuple(range(95)),
    candidate_result_ids: tuple[int, ...] | None = None,
    producer_run_id: str = "fake-run-001",
    recorded_at_utc: str = "2026-08-04T00:00:00Z",
) -> RecallAuditObservation:
    if candidate_result_ids is None:
        candidate_result_ids = oracle_result_ids  # perfect recall by default
    return RecallAuditObservation(
        query_id=query_id,
        search_configuration=search_configuration or _search_configuration(),
        identity=identity or _identity(),
        dataset002_manifest_sha256=dataset002_manifest_sha256,
        dataset002_schema_version=dataset002_schema_version,
        oracle_result_ids=oracle_result_ids,
        candidate_result_ids=candidate_result_ids,
        producer_run_id=producer_run_id,
        recorded_at_utc=recorded_at_utc,
    )


class RecallAuditObservationValidationTests(unittest.TestCase):
    def test_no_statistical_fields_on_the_raw_observation(self) -> None:
        obs = _observation()
        self.assertFalse(hasattr(obs, "alpha"))
        self.assertFalse(hasattr(obs, "estimator_method_version"))

    def test_matched_count_and_recall_derived_via_oracle_module(self) -> None:
        oracle_ids = (1, 2, 3, 4, 5)
        candidate_ids = (1, 2, 3, 999, 998)
        obs = _observation(oracle_result_ids=oracle_ids, candidate_result_ids=candidate_ids)
        expected_recall = capped_threshold_recall(candidate_ids, oracle_ids)
        self.assertEqual(obs.matched_count, 3)
        self.assertAlmostEqual(obs.capped_recall, expected_recall, places=15)

    def test_sparse_oracle_result_uses_true_count_not_the_configured_limit(self) -> None:
        # Oracle finds only 10 true neighbours within threshold (a sparse query
        # region); recall must be measured against 10, never against limit=100.
        oracle_ids = tuple(range(10))
        candidate_ids = tuple(range(8))  # 8 of the 10 true neighbours found
        obs = _observation(oracle_result_ids=oracle_ids, candidate_result_ids=candidate_ids)
        self.assertEqual(obs.matched_count, 8)
        self.assertAlmostEqual(obs.capped_recall, 8 / 10, places=15)  # not 8/100

    def test_empty_oracle_and_empty_candidate_is_perfect_recall(self) -> None:
        obs = _observation(oracle_result_ids=(), candidate_result_ids=())
        self.assertEqual(obs.capped_recall, 1.0)

    def test_empty_oracle_with_nonempty_candidate_is_zero_recall(self) -> None:
        obs = _observation(oracle_result_ids=(), candidate_result_ids=(1, 2))
        self.assertEqual(obs.capped_recall, 0.0)

    def test_oracle_size_above_limit_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "limit"):
            _observation(
                oracle_result_ids=tuple(range(150)),
                candidate_result_ids=tuple(range(10)),
            )

    def test_candidate_size_above_limit_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "limit"):
            _observation(
                oracle_result_ids=tuple(range(10)),
                candidate_result_ids=tuple(range(150)),
            )

    def test_duplicate_oracle_ids_delegate_to_oracle_module_contract(self) -> None:
        with self.assertRaises(ValueError):
            _observation(oracle_result_ids=(1, 1, 2), candidate_result_ids=(1, 2))

    def test_reordered_equal_sets_canonicalize_identically(self) -> None:
        a = _observation(oracle_result_ids=(3, 1, 2), candidate_result_ids=(2, 3, 1))
        b = _observation(oracle_result_ids=(1, 2, 3), candidate_result_ids=(1, 2, 3))
        self.assertEqual(a.oracle_result_ids, b.oracle_result_ids)
        self.assertEqual(a.candidate_result_sha256, b.candidate_result_sha256)

    def test_invalid_id_type_rejected(self) -> None:
        with self.assertRaises((ValueError, TypeError)):
            _observation(oracle_result_ids=(1, "2", 3), candidate_result_ids=(1, 2, 3))

    def test_result_digests_are_derived(self) -> None:
        oracle_ids = (10, 20, 30)
        candidate_ids = (10, 20, 40)
        obs = _observation(oracle_result_ids=oracle_ids, candidate_result_ids=candidate_ids)
        self.assertEqual(obs.oracle_result_sha256, _digest(oracle_ids))
        self.assertEqual(obs.candidate_result_sha256, _digest(candidate_ids))

    def test_negative_query_id_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "query_id"):
            _observation(query_id=-1)

    def test_wrong_dataset002_schema_version_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "dataset002_schema_version"):
            _observation(dataset002_schema_version=DATASET002_SCHEMA_VERSION + 1)

    def test_malformed_manifest_digest_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "dataset002_manifest_sha256"):
            _observation(dataset002_manifest_sha256="not-a-digest")

    def test_invalid_search_configuration_rejected(self) -> None:
        with self.assertRaises(Exception):  # the boundary raises several distinct domain error types  # noqa: B017
            _observation(search_configuration=_search_configuration(ef=999))  # not in HNSW_EF_SWEEP

    def test_invalid_identity_rejected(self) -> None:
        with self.assertRaises(Exception):  # the boundary raises several distinct domain error types  # noqa: B017
            _observation(identity=_identity(configuration_identity=""))


_BINDING_SHA256 = "1" * 64
_OTHER_BINDING_SHA256 = "2" * 64


class CanaryRecallAuditLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)
        self.path = Path(self._tempdir.name) / "recall_audit.sqlite3"

    def _ledger(
        self, run_id: str = "run-001", binding_sha256: str = _BINDING_SHA256
    ) -> CanaryRecallAuditLedger:
        return CanaryRecallAuditLedger(self.path, run_id=run_id, binding_sha256=binding_sha256)

    def test_append_and_read_back_preserves_structured_fields(self) -> None:
        ledger = self._ledger()
        result = ledger.append(_observation(oracle_result_ids=(1, 2, 3)))
        self.assertIsInstance(result, RecallAuditAppendResult)
        self.assertTrue(result.accepted)
        records = ledger.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].oracle_result_ids, (1, 2, 3))
        self.assertEqual(records[0].search_configuration, _search_configuration())
        self.assertEqual(records[0].identity, _identity())

    def test_identical_replay_is_idempotent(self) -> None:
        ledger = self._ledger()
        first = ledger.append(_observation())
        second = ledger.append(_observation())
        self.assertTrue(first.accepted)
        self.assertTrue(second.accepted)
        self.assertEqual(len(ledger.records()), 1)

    def test_conflicting_duplicate_fails_closed(self) -> None:
        ledger = self._ledger()
        ledger.append(_observation(query_id=3, oracle_result_ids=(1, 2, 3)))
        result = ledger.append(_observation(query_id=3, oracle_result_ids=(4, 5, 6)))
        self.assertFalse(result.accepted)
        self.assertEqual(result.reason_code, "QUERY_ID_CONFLICTING_DUPLICATE")
        records = ledger.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].oracle_result_ids, (1, 2, 3))

    def test_restart_durability(self) -> None:
        ledger = self._ledger()
        ledger.append(_observation(query_id=42, oracle_result_ids=(9, 8, 7)))
        del ledger
        reopened = self._ledger()
        records = reopened.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].oracle_result_ids, (7, 8, 9))

    def test_mismatched_run_id_fails_closed(self) -> None:
        self._ledger(run_id="run-001").append(_observation())
        with self.assertRaises(RecallAuditLedgerError):
            self._ledger(run_id="run-002")

    def test_corrupt_database_raises_ledger_error(self) -> None:
        self.path.write_bytes(b"not a sqlite database")
        with self.assertRaises(RecallAuditLedgerError):
            self._ledger()

    # -- binding-digest persistence (ADR-008 Stage-4 evidence-binding repair) -

    def test_binding_sha256_is_persisted_and_readable(self) -> None:
        ledger = self._ledger(binding_sha256=_BINDING_SHA256)
        self.assertEqual(ledger.binding_sha256, _BINDING_SHA256)

    def test_binding_sha256_survives_restart(self) -> None:
        self._ledger(binding_sha256=_BINDING_SHA256)
        reopened = self._ledger(binding_sha256=_BINDING_SHA256)
        self.assertEqual(reopened.binding_sha256, _BINDING_SHA256)

    def test_mismatched_binding_sha256_fails_closed(self) -> None:
        self._ledger(run_id="run-001", binding_sha256=_BINDING_SHA256)
        with self.assertRaises(RecallAuditLedgerError):
            self._ledger(run_id="run-001", binding_sha256=_OTHER_BINDING_SHA256)

    def test_malformed_binding_sha256_is_rejected_before_any_write(self) -> None:
        with self.assertRaises(ValueError):
            CanaryRecallAuditLedger(self.path, run_id="run-001", binding_sha256="not-a-digest")
        self.assertFalse(self.path.exists())

    # -- hash chain --------------------------------------------------------

    def test_chain_head_is_genesis_when_empty(self) -> None:
        ledger = self._ledger()
        state = ledger.chain_state()
        self.assertIsInstance(state, RecallAuditChainState)
        self.assertEqual(state.record_count, 0)
        self.assertEqual(len(state.chain_head_sha256), 64)

    def test_genesis_depends_on_binding_sha256(self) -> None:
        path_a = Path(self._tempdir.name) / "a.sqlite3"
        path_b = Path(self._tempdir.name) / "b.sqlite3"
        ledger_a = CanaryRecallAuditLedger(path_a, run_id="run-001", binding_sha256=_BINDING_SHA256)
        ledger_b = CanaryRecallAuditLedger(
            path_b, run_id="run-001", binding_sha256=_OTHER_BINDING_SHA256
        )
        self.assertNotEqual(
            ledger_a.chain_state().chain_head_sha256, ledger_b.chain_state().chain_head_sha256
        )

    def test_chain_head_advances_on_each_genuinely_new_append(self) -> None:
        ledger = self._ledger()
        genesis = ledger.chain_state().chain_head_sha256
        ledger.append(_observation(query_id=1))
        after_one = ledger.chain_state()
        self.assertEqual(after_one.record_count, 1)
        self.assertNotEqual(after_one.chain_head_sha256, genesis)
        ledger.append(_observation(query_id=2))
        after_two = ledger.chain_state()
        self.assertEqual(after_two.record_count, 2)
        self.assertNotEqual(after_two.chain_head_sha256, after_one.chain_head_sha256)

    def test_chain_head_unchanged_by_idempotent_replay(self) -> None:
        ledger = self._ledger()
        ledger.append(_observation(query_id=1))
        head_before = ledger.chain_state().chain_head_sha256
        ledger.append(_observation(query_id=1))  # byte-identical replay
        state_after = ledger.chain_state()
        self.assertEqual(state_after.chain_head_sha256, head_before)
        self.assertEqual(state_after.record_count, 1)

    def test_chain_head_unchanged_by_rejected_conflicting_duplicate(self) -> None:
        ledger = self._ledger()
        ledger.append(_observation(query_id=1, oracle_result_ids=(1, 2, 3)))
        head_before = ledger.chain_state().chain_head_sha256
        ledger.append(_observation(query_id=1, oracle_result_ids=(4, 5, 6)))
        self.assertEqual(ledger.chain_state().chain_head_sha256, head_before)

    def test_chain_is_deterministic_and_restart_durable(self) -> None:
        ledger = self._ledger()
        ledger.append(_observation(query_id=1))
        ledger.append(_observation(query_id=2))
        head_before = ledger.chain_state().chain_head_sha256
        del ledger
        reopened = self._ledger()
        self.assertEqual(reopened.chain_state().chain_head_sha256, head_before)

    def test_chain_order_depends_on_insertion_order_not_query_id(self) -> None:
        path_a = Path(self._tempdir.name) / "order_a.sqlite3"
        path_b = Path(self._tempdir.name) / "order_b.sqlite3"
        ledger_a = CanaryRecallAuditLedger(path_a, run_id="r", binding_sha256=_BINDING_SHA256)
        ledger_a.append(_observation(query_id=5))
        ledger_a.append(_observation(query_id=1))
        ledger_b = CanaryRecallAuditLedger(path_b, run_id="r", binding_sha256=_BINDING_SHA256)
        ledger_b.append(_observation(query_id=1))
        ledger_b.append(_observation(query_id=5))
        self.assertNotEqual(
            ledger_a.chain_state().chain_head_sha256, ledger_b.chain_state().chain_head_sha256
        )

    def test_direct_update_of_a_stored_row_is_rejected_by_the_database(self) -> None:
        ledger = self._ledger()
        ledger.append(_observation(query_id=1, oracle_result_ids=(1, 2, 3)))
        raw = sqlite3.connect(self.path)
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                raw.execute(
                    "UPDATE recall_audit_observations SET chain_sha256 = 'x' WHERE query_id = 1"
                )
        finally:
            raw.close()

    def test_direct_delete_of_a_stored_row_is_rejected_by_the_database(self) -> None:
        ledger = self._ledger()
        ledger.append(_observation(query_id=1))
        raw = sqlite3.connect(self.path)
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                raw.execute("DELETE FROM recall_audit_observations WHERE query_id = 1")
        finally:
            raw.close()

    def test_tampered_chain_field_is_detected_on_next_read(self) -> None:
        """A hostile writer with raw file access (not going through sqlite3's
        trigger-enforced connection, e.g. editing the file with a different
        tool entirely) must still be caught on the next verified read."""
        ledger = self._ledger()
        ledger.append(_observation(query_id=1))
        # Simulate bypassing the append-only triggers: open a fresh raw
        # connection and disable/ignore them is not possible for triggers,
        # but rewriting the stored JSON directly and matching a forged
        # chain_sha256 is what an out-of-band file editor could attempt.
        raw = sqlite3.connect(self.path)
        try:
            raw.execute("PRAGMA writable_schema = 1")
            raw.execute("DROP TRIGGER IF EXISTS recall_audit_observations_no_update")
            raw.execute(
                "UPDATE recall_audit_observations SET oracle_ids_json = ? WHERE query_id = 1",
                (json.dumps([999]),),
            )
            raw.commit()
        finally:
            raw.close()
        with self.assertRaises(RecallAuditLedgerError):
            self._ledger().records()

    # -- external immutable manifest ----------------------------------------

    def test_publish_manifest_writes_matching_external_digest(self) -> None:
        ledger = self._ledger(run_id="run-manifest")
        ledger.append(_observation(query_id=1))
        manifest_path = Path(self._tempdir.name) / "manifest.json"
        result = publish_recall_audit_manifest(ledger, manifest_path)
        self.assertEqual(result["run_id"], "run-manifest")
        self.assertEqual(result["binding_sha256"], _BINDING_SHA256)
        self.assertEqual(result["record_count"], 1)
        self.assertEqual(result["chain_head_sha256"], ledger.chain_state().chain_head_sha256)
        self.assertEqual(result["manifest_sha256"], sha256_file(manifest_path))

    def test_publish_manifest_refuses_to_overwrite_existing_evidence(self) -> None:
        ledger = self._ledger()
        manifest_path = Path(self._tempdir.name) / "manifest.json"
        publish_recall_audit_manifest(ledger, manifest_path)
        with self.assertRaises(Exception):  # the boundary raises several distinct domain error types  # noqa: B017
            publish_recall_audit_manifest(ledger, manifest_path)


if __name__ == "__main__":
    unittest.main()
