"""ADR-020 durable readiness provider/store tests.

Every test uses an injected in-memory observer. No Milvus client, no
Docker socket, no vector search, no ef/index/route/grant/canary/rollback
actuation, and no LKG/Phase-1/Phase-2/Checkpoint-C/D1/D2 state.
"""

from __future__ import annotations

import json
import hashlib
import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch

from tests.test_lkg_window_readiness_observation import (
    _observe as _canonical_health, _rollback as _canonical_rollback,
    _container, _route_record,
)
from vdbench.canary_route_state import RouteState

from vdbench.artifacts import canonical_json_bytes
from vdbench.config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from vdbench.lkg_run_binding import LkgRunBinding, lkg_run_binding_sha256
from vdbench.lkg_window_readiness import (
    LkgWindowOperationalReadinessProviderError,
    readiness_payload_document,
)
from vdbench.lkg_window_readiness_observation import (
    LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY,
    LKG_ROLLBACK_READINESS_SOURCE_IDENTITY,
    LkgWindowHealthObservation,
    LkgWindowReadinessObservationError,
    LkgWindowRollbackReadiness,
    derive_lkg_window_provider_run_id,
    derive_lkg_window_readiness_check_id,
)
from vdbench.lkg_window_readiness_store import (
    SqliteLkgWindowOperationalReadinessProvider,
)

_ORDERED_QUERY_IDS = tuple(range(10000, 12400))
_NOW = "2026-08-30T00:00:00.000000Z"


def _ordered_ids_sha256():
    from vdbench.lkg_run_binding import lkg_ordered_query_ids_sha256

    return lkg_ordered_query_ids_sha256(_ORDERED_QUERY_IDS)


def _configuration() -> SearchConfiguration:
    return SearchConfiguration(
        metric=Metric.L2, threshold_label="target-075", radius=191.85897352125554,
        index_track=IndexTrack.HNSW, ef=400, limit=100, consistency_level="Strong",
    )


def _binding(**overrides) -> LkgRunBinding:
    fields = {
        "run_id": "lkg-run-1",
        "producer_identity": "producer-v1",
        "search_configuration": _configuration(),
        "collection_name": "vd_hnsw",
        "base_data_identity": "DATASET-001-v1:sha256:" + "9" * 64,
        "index_identity": "index-v1",
        "qualification_dataset_id": "DATASET-003",
        "qualification_dataset_version": "DATASET-003-v1",
        "qualification_manifest_sha256": "a" * 64,
        "qualification_query_role": "lkg_qualification",
        "qualification_query_id_array_sha256": "b" * 64,
        "qualification_ordered_query_ids_sha256": _ordered_ids_sha256(),
        "qualification_query_array_sha256": "c" * 64,
        "qualification_expected_query_count": len(_ORDERED_QUERY_IDS),
        "environment_identity": _canonical_health().document["observed_environment_identity"],
        "source_revision": "1fbf66cc499cebfe1df85450d4c9f222c985a0c4",
    }
    fields.update(overrides)
    return LkgRunBinding(**fields)


def _health(passed=True, codes=()) -> LkgWindowHealthObservation:
    result = _canonical_health(
        source_run_binding_sha256=_binding().sha256,
        healthz="MILVUS_HEALTHZ_FAILED" not in codes,
        container=lambda: _container(health="unhealthy" if "CONTAINER_UNHEALTHY" in codes else "healthy"),
    )
    assert result.passed == passed and result.reason_codes == tuple(codes)
    return result


def _rollback(ready=True, codes=()) -> LkgWindowRollbackReadiness:
    route = None
    if "CANDIDATE_ROUTE_ACTIVE" in codes:
        route = _route_record(RouteState.ACTIVATING)
    elif "BOOTSTRAP_LKG_ROUTE_MARKER_PRESENT" in codes:
        route = _route_record(RouteState.LKG_ONLY)
    result = _canonical_rollback(
        source_run_binding_sha256=_binding().sha256, route_state_record=route,
    )
    assert result.ready == ready and result.reason_codes == tuple(codes)
    return result


class _Observer:
    """Counting observer. `calls` is the exactly-one-observation probe."""

    def __init__(self, *, health=None, rollback=None, raises=None, barrier=None):
        self.calls = 0
        self._health = health or _health()
        self._rollback = rollback or _rollback()
        self._raises = raises
        self._barrier = barrier

    def observe(self, *, source_run_id, source_run_binding_sha256, window_index,
                readiness_check_id):
        self.calls += 1
        if self._barrier is not None:
            try:
                self._barrier.wait(timeout=2.0)
            except threading.BrokenBarrierError:
                pass
        if self._raises is not None:
            raise self._raises
        return self._health, self._rollback


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.path = self.root / "readiness.sqlite3"
        self.binding = _binding()
        self.binding_sha = lkg_run_binding_sha256(self.binding)
        self.check_id = derive_lkg_window_readiness_check_id(
            source_run_id=self.binding.run_id,
            source_run_binding_sha256=self.binding_sha,
            window_index=0,
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def provider(self, observer, *, path=None, binding=None):
        return SqliteLkgWindowOperationalReadinessProvider(
            path or self.path,
            run_binding=binding or self.binding,
            observer=observer,
            clock=lambda: _NOW,
            monotonic_ns=lambda: 1,
            lock_timeout_seconds=5.0,
        )

    def context(self, window_index=0):
        return {
            "source_run_id": self.binding.run_id,
            "source_run_binding_sha256": self.binding_sha,
            "window_index": window_index,
            "epoch_index": window_index // 6,
            "first_attempt_sequence": window_index * 200,
            "last_attempt_sequence": window_index * 200 + 199,
        }


class CaptureTests(_Base):
    def test_fresh_capture_passes_and_persists(self) -> None:
        observer = _Observer()
        provider = self.provider(observer)
        evidence = provider.capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        self.assertEqual(observer.calls, 1)
        self.assertTrue(evidence.health_checked)
        self.assertTrue(evidence.health_passed)
        self.assertTrue(evidence.rollback_tested)
        self.assertTrue(evidence.rollback_ready)
        self.assertEqual(
            evidence.health_evidence_source_identity,
            LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY,
        )
        self.assertEqual(
            evidence.rollback_evidence_source_identity,
            LKG_ROLLBACK_READINESS_SOURCE_IDENTITY,
        )
        self.assertEqual(evidence.reason_codes, ())

    def test_observed_health_failure_persists_real_evidence(self) -> None:
        observer = _Observer(health=_health(False, ("CONTAINER_UNHEALTHY",)))
        evidence = self.provider(observer).capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        self.assertTrue(evidence.health_checked)
        self.assertFalse(evidence.health_passed)
        self.assertIn("CONTAINER_UNHEALTHY", evidence.reason_codes)
        # durable: a fresh provider must return it unchanged
        again = self.provider(_Observer()).lookup(readiness_check_id=self.check_id)
        self.assertFalse(again.health_passed)

    def test_observed_rollback_failure_persists_real_evidence(self) -> None:
        observer = _Observer(rollback=_rollback(False, ("CANDIDATE_ROUTE_ACTIVE",)))
        evidence = self.provider(observer).capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        self.assertTrue(evidence.rollback_tested)
        self.assertFalse(evidence.rollback_ready)
        self.assertIn("CANDIDATE_ROUTE_ACTIVE", evidence.reason_codes)

    def test_both_observed_failures_persist(self) -> None:
        observer = _Observer(
            health=_health(False, ("MILVUS_HEALTHZ_FAILED",)),
            rollback=_rollback(False, ("BOOTSTRAP_LKG_ROUTE_MARKER_PRESENT",)),
        )
        evidence = self.provider(observer).capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        self.assertFalse(evidence.health_passed)
        self.assertFalse(evidence.rollback_ready)
        self.assertEqual(
            evidence.reason_codes,
            ("BOOTSTRAP_LKG_ROUTE_MARKER_PRESENT", "MILVUS_HEALTHZ_FAILED"),
        )

    def test_provider_inability_persists_nothing_and_is_retryable(self) -> None:
        failing = _Observer(
            raises=LkgWindowReadinessObservationError("LKG_READINESS_CONTAINER_UNAVAILABLE")
        )
        provider = self.provider(failing)
        with self.assertRaises(LkgWindowReadinessObservationError):
            provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
        # nothing persisted -> a retry performs a fresh observation
        good = _Observer()
        evidence = self.provider(good).capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        self.assertEqual(good.calls, 1)
        self.assertTrue(evidence.health_passed)

    def test_retry_returns_stored_evidence_without_reobserving(self) -> None:
        observer = _Observer()
        provider = self.provider(observer)
        first = provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
        second = provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
        self.assertEqual(observer.calls, 1, "retry must not re-observe")
        self.assertEqual(first.canonical_document_digest, second.canonical_document_digest)
        self.assertEqual(first.provider_run_id, second.provider_run_id)

    def test_noncanonical_check_id_refused_before_observation(self) -> None:
        observer = _Observer()
        provider = self.provider(observer)
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as caught:
            provider.capture_or_return(readiness_check_id="f" * 64, **self.context())
        self.assertEqual(str(caught.exception), "NONCANONICAL_READINESS_CHECK_ID")
        self.assertEqual(observer.calls, 0, "must refuse BEFORE observing")

    def test_window_already_checked_under_a_different_id(self) -> None:
        observer = _Observer()
        provider = self.provider(observer)
        provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
        # a different (still canonical for another window) id for window 0
        other = derive_lkg_window_readiness_check_id(
            source_run_id=self.binding.run_id,
            source_run_binding_sha256=self.binding_sha,
            window_index=1,
        )
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as caught:
            provider.capture_or_return(readiness_check_id=other, **self.context())
        # window 0 context with window 1's id is non-canonical, refused first
        self.assertEqual(str(caught.exception), "NONCANONICAL_READINESS_CHECK_ID")

    def test_conflicting_context_for_known_check_id(self) -> None:
        observer = _Observer()
        provider = self.provider(observer)
        provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
        conflicting = dict(self.context())
        conflicting["source_run_binding_sha256"] = "d" * 64
        with self.assertRaises(LkgWindowOperationalReadinessProviderError):
            provider.capture_or_return(readiness_check_id=self.check_id, **conflicting)

    def test_provider_run_id_is_canonical_and_preserved(self) -> None:
        provider = self.provider(_Observer())
        evidence = provider.capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        expected = derive_lkg_window_provider_run_id(
            readiness_check_id=self.check_id,
            source_run_id=self.binding.run_id,
            source_run_binding_sha256=self.binding_sha,
        )
        self.assertEqual(evidence.provider_run_id, expected)
        restarted = self.provider(_Observer()).lookup(readiness_check_id=self.check_id)
        self.assertEqual(restarted.provider_run_id, expected)

    def test_invalid_window_context_rejected(self) -> None:
        provider = self.provider(_Observer())
        bad = dict(self.context())
        bad["epoch_index"] = 1
        with self.assertRaises(ContractViolation):
            provider.capture_or_return(readiness_check_id=self.check_id, **bad)


class LookupTests(_Base):
    def test_lookup_after_restart_returns_exact_bytes_zero_observation(self) -> None:
        original = self.provider(_Observer()).capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        observer = _Observer()
        restarted = self.provider(observer)  # a new process would do exactly this
        recovered = restarted.lookup(readiness_check_id=self.check_id)
        self.assertEqual(observer.calls, 0, "lookup must never observe")
        self.assertEqual(
            canonical_json_bytes(readiness_payload_document(original)),
            canonical_json_bytes(readiness_payload_document(recovered)),
        )
        self.assertEqual(
            original.canonical_document_digest, recovered.canonical_document_digest
        )

    def test_unknown_check_id_is_not_recoverable(self) -> None:
        provider = self.provider(_Observer())
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as caught:
            provider.lookup(readiness_check_id="a" * 64)
        self.assertEqual(str(caught.exception), "RESULT_NOT_RECOVERABLE")

    def test_digest_tamper_is_not_recoverable(self) -> None:
        self.provider(_Observer()).capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("DROP TRIGGER lkg_window_readiness_evidence_no_update")
            connection.execute(
                "UPDATE lkg_window_readiness_evidence SET canonical_document_digest=?",
                ("0" * 64,),
            )
            connection.execute(
                "CREATE TRIGGER lkg_window_readiness_evidence_no_update BEFORE UPDATE ON "
                "lkg_window_readiness_evidence BEGIN SELECT RAISE(ABORT,'append-only'); END"
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as caught:
            self.provider(_Observer()).lookup(readiness_check_id=self.check_id)
        self.assertEqual(str(caught.exception), "RESULT_NOT_RECOVERABLE")

    def test_payload_tamper_is_not_recoverable(self) -> None:
        self.provider(_Observer()).capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("DROP TRIGGER lkg_window_readiness_evidence_no_update")
            row = connection.execute(
                "SELECT payload_document FROM lkg_window_readiness_evidence"
            ).fetchone()
            payload = json.loads(bytes(row[0]).decode())
            payload["health_passed"] = not payload["health_passed"]
            connection.execute(
                "UPDATE lkg_window_readiness_evidence SET payload_document=?",
                (canonical_json_bytes(payload),),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(LkgWindowOperationalReadinessProviderError):
            self.provider(_Observer()).lookup(readiness_check_id=self.check_id)


class StoreIntegrityTests(_Base):
    def test_append_only_update_and_delete_refused(self) -> None:
        self.provider(_Observer()).capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        connection = sqlite3.connect(self.path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE lkg_window_readiness_evidence SET source_run_id='x'"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM lkg_window_readiness_evidence")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE lkg_readiness_store_binding SET source_run_id='x'")
        finally:
            connection.close()

    def test_store_binding_holds_the_complete_run_binding_document(self) -> None:
        self.provider(_Observer())
        connection = sqlite3.connect(self.path)
        try:
            row = connection.execute(
                "SELECT source_run_id, source_run_binding_sha256, environment_identity, "
                "run_binding_document FROM lkg_readiness_store_binding"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(row[0], self.binding.run_id)
        self.assertEqual(row[1], self.binding_sha)
        self.assertEqual(row[2], self.binding.environment_identity)
        document = json.loads(bytes(row[3]).decode())
        self.assertEqual(document["run_id"], self.binding.run_id)
        self.assertIn("search_configuration", document)

    def test_reopen_with_a_different_run_refuses(self) -> None:
        self.provider(_Observer())
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as caught:
            self.provider(_Observer(), binding=_binding(run_id="lkg-run-2"))
        self.assertEqual(str(caught.exception), "READINESS_STORE_SOURCE_RUN_MISMATCH")

    def test_reopen_with_a_changed_binding_refuses(self) -> None:
        self.provider(_Observer())
        changed = _binding(
            environment_identity="lkg-env-identity-v1:sha256:" + "f" * 64
        )
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as caught:
            self.provider(_Observer(), binding=changed)
        self.assertEqual(str(caught.exception), "READINESS_STORE_BINDING_MISMATCH")

    def test_symlink_path_refused(self) -> None:
        real = self.root / "real.sqlite3"
        real.write_bytes(b"")
        link = self.root / "link.sqlite3"
        os.symlink(real, link)
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as caught:
            self.provider(_Observer(), path=link)
        self.assertEqual(str(caught.exception), "READINESS_STORE_UNSAFE_PATH")

    def test_hard_link_path_refused(self) -> None:
        real = self.root / "real2.sqlite3"
        real.write_bytes(b"")
        linked = self.root / "hard.sqlite3"
        os.link(real, linked)
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as caught:
            self.provider(_Observer(), path=linked)
        self.assertEqual(str(caught.exception), "READINESS_STORE_UNSAFE_PATH")

    def test_private_file_mode_enforced(self) -> None:
        self.provider(_Observer())
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_foreign_table_is_corruption(self) -> None:
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("CREATE TABLE intruder (x INTEGER)")
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as caught:
            self.provider(_Observer())
        self.assertEqual(str(caught.exception), "READINESS_STORE_CORRUPTED")


class ConcurrencyTests(_Base):
    def test_two_concurrent_callers_cause_exactly_one_observation(self) -> None:
        barrier = threading.Barrier(2, timeout=2.0)
        observer = _Observer(barrier=barrier)
        provider_a = self.provider(observer)
        provider_b = self.provider(observer)
        results: list[object] = []
        errors: list[BaseException] = []

        def run(provider):
            try:
                results.append(
                    provider.capture_or_return(
                        readiness_check_id=self.check_id, **self.context()
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        threads = [
            threading.Thread(target=run, args=(provider_a,)),
            threading.Thread(target=run, args=(provider_b,)),
        ]
        for thread in threads:
            thread.start()
        # release the barrier if only one observer ever arrives
        try:
            barrier.wait(timeout=1.0)
        except (threading.BrokenBarrierError, Exception):
            barrier.abort()
        for thread in threads:
            thread.join(timeout=10.0)

        self.assertEqual(errors, [], f"unexpected errors: {errors}")
        self.assertEqual(len(results), 2)
        self.assertEqual(
            observer.calls, 1, "exactly ONE logical observation per window"
        )
        self.assertEqual(
            results[0].canonical_document_digest,
            results[1].canonical_document_digest,
        )

    def test_only_one_durable_row_exists(self) -> None:
        provider = self.provider(_Observer())
        provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
        provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
        connection = sqlite3.connect(self.path)
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM lkg_window_readiness_evidence"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(count, 1)


class ActuationBoundaryTests(_Base):
    def test_observer_port_is_the_only_external_seam(self) -> None:
        import inspect

        import vdbench.lkg_window_readiness_store as module

        source = inspect.getsource(module)
        for forbidden in (
            ".search(", "MilvusClient", "load_collection", "release_collection",
            "create_index", "drop_index", "begin_activation", "clear_to_lkg",
            "reserve(", "record_terminal(",
        ):
            self.assertNotIn(forbidden, source, f"forbidden seam present: {forbidden}")

    def test_all_twelve_windows_capture_independently(self) -> None:
        observer = _Observer()
        provider = self.provider(observer)
        digests = set()
        for window in range(12):
            check_id = derive_lkg_window_readiness_check_id(
                source_run_id=self.binding.run_id,
                source_run_binding_sha256=self.binding_sha,
                window_index=window,
            )
            evidence = provider.capture_or_return(
                readiness_check_id=check_id, **self.context(window)
            )
            digests.add(evidence.canonical_document_digest)
            self.assertEqual(evidence.epoch_index, window // 6)
        self.assertEqual(observer.calls, 12)
        self.assertEqual(len(digests), 12)



class OptionalHardeningTests(_Base):
    """ADR-020 section 18/26 hardening. No production semantic change."""

    def test_sqlite_pragmas_are_exactly_as_specified(self) -> None:
        self.provider(_Observer())
        connection = sqlite3.connect(self.path)
        try:
            user_version = connection.execute("PRAGMA user_version").fetchone()[0]
            journal = connection.execute("PRAGMA journal_mode").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(user_version, 2)
        self.assertEqual(str(journal).lower(), "delete")

    def test_crash_after_insert_before_commit_persists_nothing(self) -> None:
        class _CrashAfterInsert(SqliteLkgWindowOperationalReadinessProvider):
            def _insert_evidence_locked(self, connection, evidence, health_bytes, rollback_bytes):
                super()._insert_evidence_locked(connection, evidence, health_bytes, rollback_bytes)
                raise RuntimeError("simulated crash after INSERT, before COMMIT")

        observer = _Observer()
        crashing = _CrashAfterInsert(
            self.path, run_binding=self.binding, observer=observer,
            clock=lambda: _NOW, monotonic_ns=lambda: 1, lock_timeout_seconds=5.0,
        )
        with self.assertRaises(RuntimeError):
            crashing.capture_or_return(readiness_check_id=self.check_id, **self.context())
        self.assertEqual(observer.calls, 1)
        connection = sqlite3.connect(self.path)
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM lkg_window_readiness_evidence"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(count, 0, "uncommitted observation must not persist")
        # and the window remains capturable afterwards
        retry = _Observer()
        evidence = self.provider(retry).capture_or_return(
            readiness_check_id=self.check_id, **self.context()
        )
        self.assertEqual(retry.calls, 1)
        self.assertTrue(evidence.health_passed)

    def test_no_committed_evidence_can_carry_health_checked_false(self) -> None:
        provider = self.provider(_Observer(health=_health(False, ("CONTAINER_UNHEALTHY",))))
        provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
        connection = sqlite3.connect(self.path)
        try:
            rows = connection.execute(
                "SELECT payload_document FROM lkg_window_readiness_evidence"
            ).fetchall()
        finally:
            connection.close()
        self.assertEqual(len(rows), 1)
        for (blob,) in rows:
            payload = json.loads(bytes(blob).decode())
            self.assertIs(payload["health_checked"], True)
            self.assertIs(payload["rollback_tested"], True)
            self.assertIs(payload["health_passed"], False)

class RetainedPreimageTests(_Base):
    def rows(self):
        with closing(sqlite3.connect(self.path)) as connection:
            return connection.execute(
                "SELECT payload_document,health_source_document_bytes,rollback_source_document_bytes "
                "FROM lkg_window_readiness_evidence ORDER BY window_index"
            ).fetchall()

    def test_inconsistent_observations_refuse_before_aggregate_and_insert(self):
        health, rollback = _health(), _rollback()
        cases = [
            (replace(health, digest="0" * 64), rollback),
            (health, replace(rollback, digest="0" * 64)),
            (replace(health, passed=False), rollback),
            (health, replace(rollback, ready=False)),
            (replace(health, reason_codes=("CONTAINER_UNHEALTHY",)), rollback),
            (health, replace(rollback, reason_codes=("CANDIDATE_ROUTE_ACTIVE",))),
            (replace(health, document={**health.document, "source_run_id": "other"}), rollback),
            (health, replace(rollback, document={**rollback.document, "source_run_binding_sha256": "0" * 64})),
            (replace(health, document={}), rollback),
            (health, replace(rollback, document={})),
        ]
        for h, r in cases:
            with self.subTest(health=h, rollback=r):
                observer = _Observer(health=h, rollback=r)
                provider = self.provider(observer)
                with patch.object(provider, "_build_evidence") as build:
                    with self.assertRaises(ContractViolation):
                        provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
                    build.assert_not_called()
                self.assertEqual(observer.calls, 1)
                self.assertEqual(self.rows(), [])

    def test_exact_bytes_restart_idempotence_and_twelve_windows(self):
        observer = _Observer()
        provider = self.provider(observer)
        evidence = []
        for window in range(12):
            context = self.context(window)
            check_id = derive_lkg_window_readiness_check_id(
                source_run_id=self.binding.run_id, source_run_binding_sha256=self.binding_sha,
                window_index=window,
            )
            evidence.append(provider.capture_or_return(readiness_check_id=check_id, **context))
        before = self.rows()
        self.assertEqual(len(before), 12)
        self.assertEqual(sum(len(row[1:]) for row in before), 24)
        for _, h, r in before:
            self.assertIs(type(h), bytes)
            self.assertIs(type(r), bytes)
            self.assertEqual(h, canonical_json_bytes(observer._health.document))
            self.assertEqual(r, canonical_json_bytes(observer._rollback.document))
            self.assertEqual(canonical_json_bytes(json.loads(h)), h)
            self.assertEqual(canonical_json_bytes(json.loads(r)), r)
        conflicting = _Observer(health=_health(False, ("MILVUS_HEALTHZ_FAILED",)))
        restarted = self.provider(conflicting)
        for original in evidence:
            self.assertEqual(restarted.lookup(readiness_check_id=original.readiness_check_id), original)
            self.assertEqual(restarted.capture_or_return(
                readiness_check_id=original.readiness_check_id, **self.context(original.window_index)
            ), original)
        self.assertEqual(conflicting.calls, 0)
        self.assertEqual(self.rows(), before)

    def test_mixed_container_retained_reason_inconsistency_cannot_persist(self):
        health = _canonical_health(container_inspector=lambda name: _container(
            oom=name == "milvus-etcd",
            health="unhealthy" if name == "milvus-minio" else "healthy",
        ))
        self.binding = _binding(environment_identity=health.document["observed_environment_identity"])
        self.binding_sha = self.binding.sha256
        self.check_id = derive_lkg_window_readiness_check_id(
            source_run_id=self.binding.run_id, source_run_binding_sha256=self.binding_sha,
            window_index=0,
        )
        doc = {**health.document, "source_run_binding_sha256": self.binding_sha,
               "reason_codes": ["CONTAINER_OOM_KILLED"]}
        malformed = replace(
            health, document=doc, reason_codes=("CONTAINER_OOM_KILLED",),
            digest=hashlib.sha256(
                b"vdbench.lkg-window-health-observation.v1\0" + canonical_json_bytes(doc)
            ).hexdigest(),
        )
        observer = _Observer(health=malformed, rollback=_canonical_rollback(
            source_run_binding_sha256=self.binding_sha,
        ))
        provider = self.provider(observer)
        with patch.object(provider, "_build_evidence", wraps=provider._build_evidence) as build:
            with self.assertRaises(ContractViolation):
                provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
            build.assert_not_called()
        self.assertEqual(observer.calls, 1)
        self.assertEqual(self.rows(), [])
        with self.assertRaises(LkgWindowOperationalReadinessProviderError):
            self.provider(observer).lookup(readiness_check_id=self.check_id)
        self.assertEqual(self.rows(), [])

    def _tamper(self, column, value):
        with closing(sqlite3.connect(self.path)) as connection:
            sql = connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='lkg_window_readiness_evidence_no_update'"
            ).fetchone()[0]
            connection.execute("DROP TRIGGER lkg_window_readiness_evidence_no_update")
            connection.execute(f"UPDATE lkg_window_readiness_evidence SET {column}=?", (value,))
            connection.execute(sql)
            connection.commit()

    def test_bad_preimages_refuse_on_lookup_and_provider_reopen(self):
        for column in ("health_source_document_bytes", "rollback_source_document_bytes"):
            for label in ("invalid-utf8", "malformed", "pretty", "duplicate", "wrong-digest", "wrong-shape"):
                with self.subTest(column=column, label=label):
                    self.path = self.root / f"{column}-{label}.sqlite3"
                    provider = self.provider(_Observer())
                    provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
                    raw = self.rows()[0][1 if column.startswith("health") else 2]
                    doc = json.loads(raw)
                    if label == "invalid-utf8":
                        bad = b"\xff"
                    elif label == "malformed":
                        bad = b"{"
                    elif label == "pretty":
                        bad = json.dumps(doc, indent=2).encode()
                    elif label == "duplicate":
                        bad = b'{"source_run_id":"duplicate",' + raw[1:]
                    elif label == "wrong-shape":
                        bad = b"[]\n"
                    else:
                        doc["source_run_id"] = "different-run"
                        bad = canonical_json_bytes(doc)
                    self._tamper(column, bad)
                    before = self.path.read_bytes()
                    with self.assertRaises(LkgWindowOperationalReadinessProviderError):
                        provider.lookup(readiness_check_id=self.check_id)
                    with self.assertRaises(LkgWindowOperationalReadinessProviderError):
                        self.provider(_Observer())
                    self.assertEqual(self.path.read_bytes(), before)

    def test_aggregate_source_verdict_reasons_and_identity_mismatches_refuse(self):
        from vdbench.lkg_window_readiness import readiness_payload_document_digest

        for field, value in (
            ("health_passed", False), ("rollback_ready", False),
            ("reason_codes", ["CONTAINER_UNHEALTHY"]),
            ("health_evidence_source_identity", "wrong-source"),
            ("rollback_evidence_source_identity", "wrong-source"),
            ("health_evidence_source_digest", "0" * 64),
            ("rollback_evidence_source_digest", "0" * 64),
        ):
            with self.subTest(field=field):
                self.path = self.root / f"{field}.sqlite3"
                provider = self.provider(_Observer())
                provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
                payload = json.loads(self.rows()[0][0])
                payload[field] = value
                self._tamper("payload_document", canonical_json_bytes(payload))
                self._tamper("canonical_document_digest", readiness_payload_document_digest(payload))
                with self.assertRaises(LkgWindowOperationalReadinessProviderError):
                    provider.lookup(readiness_check_id=self.check_id)

    def test_aggregate_shape_and_domain_exclude_retained_preimages(self):
        evidence = self.provider(_Observer()).capture_or_return(readiness_check_id=self.check_id, **self.context())
        payload = readiness_payload_document(evidence)
        self.assertEqual(set(payload), {
            "readiness_schema_version", "source_run_id", "source_run_binding_sha256",
            "window_index", "epoch_index", "first_attempt_sequence", "last_attempt_sequence",
            "readiness_check_id", "provider_run_id", "health_checked", "health_passed",
            "health_evidence_source_identity", "health_evidence_source_digest",
            "rollback_tested", "rollback_ready", "rollback_evidence_source_identity",
            "rollback_evidence_source_digest", "checked_at_utc", "check_start_ns",
            "check_end_ns", "reason_codes",
        })
        self.assertEqual(evidence.canonical_document_digest, hashlib.sha256(
            b"vdbench.lkg_window_operational_readiness.v1\0" + canonical_json_bytes(payload)
        ).hexdigest())


class ExactSchemaV2Tests(_Base):
    def test_versions_refuse_without_repair_including_unversioned_nonempty_store(self):
        for version in (0, 1, 3):
            with self.subTest(version=version):
                self.path = self.root / f"version-{version}.sqlite3"
                self.provider(_Observer())
                with closing(sqlite3.connect(self.path)) as connection:
                    connection.execute(f"PRAGMA user_version={version}")
                before = self.path.read_bytes()
                with self.assertRaises(LkgWindowOperationalReadinessProviderError) as caught:
                    self.provider(_Observer())
                if version == 1:
                    self.assertEqual(str(caught.exception), "READINESS_STORE_V1_NOT_SUPPORTED")
                self.assertEqual(self.path.read_bytes(), before)

    def test_exact_inventory_and_triggers_refuse_without_repair(self):
        statements = [
            "DROP TRIGGER lkg_window_readiness_evidence_no_update",
            "CREATE TABLE extra(x INTEGER)",
            "CREATE TABLE sqliteX_not_internal(x INTEGER)",
            "CREATE INDEX extra ON lkg_window_readiness_evidence(source_run_id)",
            "CREATE VIEW extra AS SELECT * FROM lkg_window_readiness_evidence",
            "CREATE TRIGGER extra BEFORE UPDATE ON lkg_window_readiness_evidence BEGIN SELECT 1; END",
        ]
        for i, statement in enumerate(statements):
            with self.subTest(statement=statement):
                self.path = self.root / f"inventory-{i}.sqlite3"
                self.provider(_Observer())
                with closing(sqlite3.connect(self.path)) as connection:
                    connection.execute(statement)
                before = self.path.read_bytes()
                with self.assertRaises(LkgWindowOperationalReadinessProviderError):
                    self.provider(_Observer())
                self.assertEqual(self.path.read_bytes(), before)
        for i, replacement in enumerate((
            "CREATE TRIGGER renamed BEFORE UPDATE ON lkg_window_readiness_evidence BEGIN SELECT RAISE(ABORT,'append-only'); END",
            "CREATE TRIGGER lkg_window_readiness_evidence_no_update BEFORE UPDATE ON lkg_window_readiness_evidence BEGIN SELECT 1; END",
            "CREATE TRIGGER lkg_window_readiness_evidence_no_update AFTER UPDATE ON lkg_window_readiness_evidence BEGIN SELECT RAISE(ABORT,'append-only'); END",
            "CREATE TRIGGER lkg_window_readiness_evidence_no_update BEFORE DELETE ON lkg_window_readiness_evidence BEGIN SELECT RAISE(ABORT,'append-only'); END",
            "CREATE TRIGGER lkg_window_readiness_evidence_no_update BEFORE UPDATE ON lkg_readiness_store_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
            "CREATE TRIGGER lkg_window_readiness_evidence_no_update BEFORE UPDATE ON lkg_window_readiness_evidence BEGIN SELECT RAISE(ABORT,'APPEND-ONLY'); END",
        )):
            with self.subTest(replacement=replacement):
                self.path = self.root / f"trigger-{i}.sqlite3"
                self.provider(_Observer())
                with closing(sqlite3.connect(self.path)) as connection:
                    connection.execute("DROP TRIGGER lkg_window_readiness_evidence_no_update")
                    connection.execute(replacement)
                before = self.path.read_bytes()
                with self.assertRaises(LkgWindowOperationalReadinessProviderError):
                    self.provider(_Observer())
                self.assertEqual(self.path.read_bytes(), before)

    def test_both_table_definitions_are_exact(self):
        for table in ("lkg_readiness_store_binding", "lkg_window_readiness_evidence"):
            for i, (old, new) in enumerate((
                ("source_run_id TEXT NOT NULL", "source_run_id TEXT"),
                ("source_run_id TEXT NOT NULL", "source_run_id TEXT NOT NULL DEFAULT 'other'"),
                ("source_run_id TEXT", "source_run_id BLOB"),
                ("source_run_id", "renamed_run_id"),
                ("=64", ">=1"),
                (") STRICT", ")"),
            )):
                with self.subTest(table=table, new=new):
                    self.path = self.root / f"{table}-{i}.sqlite3"
                    self.provider(_Observer())
                    with closing(sqlite3.connect(self.path)) as connection:
                        sql = connection.execute("SELECT sql FROM sqlite_master WHERE name=?", (table,)).fetchone()[0]
                        changed = sql.replace(old, new)
                        self.assertNotEqual(sql, changed)
                        connection.execute("PRAGMA writable_schema=ON")
                        connection.execute("UPDATE sqlite_master SET sql=? WHERE name=?", (changed, table))
                        version = connection.execute("PRAGMA schema_version").fetchone()[0]
                        connection.execute(f"PRAGMA schema_version={version + 1}")
                        connection.commit()
                    before = self.path.read_bytes()
                    with self.assertRaises(LkgWindowOperationalReadinessProviderError):
                        self.provider(_Observer())
                    self.assertEqual(self.path.read_bytes(), before)

    def test_literal_preserving_normalization_and_harmless_spacing(self):
        from vdbench.lkg_window_readiness_store import _normalize_schema_sql

        self.assertEqual(_normalize_schema_sql(" SELECT  'a  b' ; "), "SELECT 'a  b'")
        self.assertNotEqual(_normalize_schema_sql("SELECT 'a  b'"), _normalize_schema_sql("SELECT 'a b'"))
        self.assertNotEqual(_normalize_schema_sql("SELECT 'ABORT'"), _normalize_schema_sql("SELECT 'abort'"))
        self.provider(_Observer())
        with closing(sqlite3.connect(self.path)) as connection:
            sql = connection.execute("SELECT sql FROM sqlite_master WHERE name='lkg_window_readiness_evidence_no_update'").fetchone()[0]
            connection.execute("DROP TRIGGER lkg_window_readiness_evidence_no_update")
            connection.execute(sql.replace(" BEFORE UPDATE ", "  BEFORE\nUPDATE  ") + ";")
        self.provider(_Observer())

    def test_integrity_check_once_per_provider_and_failure_refuses(self):
        statements = []
        real_connect = sqlite3.connect

        class Traced(sqlite3.Connection):
            fail_integrity = False

            def execute(self, sql, *args, **kwargs):
                statements.append(sql)
                if self.fail_integrity and sql == "PRAGMA integrity_check":
                    return super().execute("SELECT 'injected corruption'")
                return super().execute(sql, *args, **kwargs)

        with patch("vdbench.lkg_window_readiness_store.sqlite3.connect",
                   side_effect=lambda *args, **kwargs: real_connect(*args, factory=Traced, **kwargs)):
            provider = self.provider(_Observer())
            self.assertEqual(statements.count("PRAGMA integrity_check"), 1)
            provider.capture_or_return(readiness_check_id=self.check_id, **self.context())
            provider.lookup(readiness_check_id=self.check_id)
            self.assertEqual(statements.count("PRAGMA integrity_check"), 1)
            self.provider(_Observer())
            self.assertEqual(statements.count("PRAGMA integrity_check"), 2)
            before = self.path.read_bytes()
            Traced.fail_integrity = True
            with self.assertRaises(LkgWindowOperationalReadinessProviderError):
                self.provider(_Observer())
            self.assertEqual(self.path.read_bytes(), before)
        self.assertNotIn("PRAGMA foreign_key_check", statements)

    def test_corrupt_database_refuses_without_repair(self):
        self.path.write_bytes(b"not sqlite")
        with self.assertRaises(LkgWindowOperationalReadinessProviderError):
            self.provider(_Observer())
        self.assertEqual(self.path.read_bytes(), b"not sqlite")

    def test_both_tables_refuse_update_and_delete(self):
        self.provider(_Observer()).capture_or_return(readiness_check_id=self.check_id, **self.context())
        with closing(sqlite3.connect(self.path)) as connection:
            for table in ("lkg_readiness_store_binding", "lkg_window_readiness_evidence"):
                for sql in (f"DELETE FROM {table}", f"UPDATE {table} SET source_run_id='changed'"):
                    with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                        connection.execute(sql)


if __name__ == "__main__":
    unittest.main()
