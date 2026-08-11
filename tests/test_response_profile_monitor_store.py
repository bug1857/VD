from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from tests.test_response_profile_detector_head import _provenance
from tests.test_workload_monitor import (
    DETECTOR_SEED,
    FakeAuditSink,
    FakeSource,
    RecordingPolicyInputProvider,
    _fast_evidence,
    _persist_events,
    _stream_key,
)
from vdbench.response_profile_detector_head import build_response_profile_detector_head
from vdbench.drift import DetectorState, DriftClassification
from vdbench.response_profile_monitor_store import (
    ResponseProfileMonitorStateStore,
    ResponseProfileMonitorStoreError,
    VerifiedLatestResponseProfileDetectorHead,
)
import vdbench.response_profile_monitor_store as monitor_store_module
from vdbench.workload_monitor import FileMonitorStateStore, MonitorStreamState, WorkloadMonitor


class ResponseProfileMonitorStateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.path = self.root / "monitor.sqlite3"
        self.stream = _stream_key()

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _store(self, *, utc_now=None) -> ResponseProfileMonitorStateStore:
        return ResponseProfileMonitorStateStore(
            self.path, expected_stream_key=self.stream, utc_now=utc_now
        )

    def _head(self, *, window_sequence: int = 2, current: str = "current"):
        return build_response_profile_detector_head(
            stream_key=self.stream,
            window_sequence=window_sequence,
            detector_state=DetectorState.NO_DRIFT,
            detector_classification=DriftClassification.NONE,
            detector_provenance=_provenance(
                current_window_id=current,
                current_manifest_sha256=("b" if window_sequence == 2 else "e") * 64,
                configuration_identity=self.stream.configuration_identity,
                data_identity=self.stream.data_identity,
                flat_binding_id=self.stream.flat_binding_id,
                hnsw_binding_id=self.stream.hnsw_binding_id,
            ),
        )

    def test_empty_state_and_latest_head_are_none(self) -> None:
        with self._store() as store:
            self.assertIsNone(store.load(self.stream))
            self.assertIsNone(store.load_verified_latest(self.stream))

    def test_state_and_new_head_are_atomic_and_restart_verified(self) -> None:
        first = MonitorStreamState(stream_key=self.stream)
        detector_head = self._head()
        second = replace(
            first,
            next_window_sequence=3,
            latest_detector_head=detector_head,
        )
        with self._store() as store:
            store.save(first)
            store.save(second)
            latest = store.load_verified_latest(self.stream)
            self.assertIs(type(latest), VerifiedLatestResponseProfileDetectorHead)
            self.assertEqual(latest.head, detector_head)
            self.assertEqual(store.load(self.stream).latest_detector_head, detector_head)
        with self._store() as reopened:
            latest = reopened.load_verified_latest(self.stream)
            self.assertEqual(latest.head.detector_head_sha256, detector_head.detector_head_sha256)
            self.assertEqual(latest.head_record_sequence, 0)

    def test_later_head_replaces_verified_latest_without_rewriting_history(self) -> None:
        first = self._head()
        second = self._head(window_sequence=3, current="later")
        with self._store() as store:
            store.save(MonitorStreamState(self.stream, latest_detector_head=first))
            original = store.load_verified_latest(self.stream)
            store.save(
                MonitorStreamState(
                    self.stream,
                    next_window_sequence=4,
                    latest_detector_head=second,
                )
            )
            latest = store.load_verified_latest(self.stream)
            self.assertNotEqual(original.head_record_sha256, latest.head_record_sha256)
            self.assertEqual(latest.head, second)
            self.assertEqual(latest.head_record_sequence, 1)

    def test_same_semantic_head_in_distinct_store_is_distinct_durable_record(self) -> None:
        first_root = self.root / "first"
        second_root = self.root / "second"
        first_root.mkdir()
        second_root.mkdir()
        head = self._head()
        with ResponseProfileMonitorStateStore(
            first_root / "monitor.sqlite3",
            expected_stream_key=self.stream,
            utc_now=lambda: "2026-08-11T00:00:00Z",
        ) as first, ResponseProfileMonitorStateStore(
            second_root / "monitor.sqlite3",
            expected_stream_key=self.stream,
            utc_now=lambda: "2026-08-11T00:00:00Z",
        ) as second:
            state = MonitorStreamState(self.stream, latest_detector_head=head)
            first.save(state)
            second.save(state)
            self.assertEqual(
                first.load_verified_latest(self.stream).head.detector_head_sha256,
                second.load_verified_latest(self.stream).head.detector_head_sha256,
            )
            self.assertNotEqual(
                first.load_verified_latest(self.stream).head_record_sha256,
                second.load_verified_latest(self.stream).head_record_sha256,
            )

    def test_append_only_triggers_and_chain_tamper_fail_closed(self) -> None:
        with self._store() as store:
            store.save(MonitorStreamState(self.stream, latest_detector_head=self._head()))
            connection = store._connection
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE monitor_state_records SET state_json='{}' WHERE state_record_sequence=0"
                )
        connection = sqlite3.connect(self.path)
        connection.execute("DROP TRIGGER monitor_state_records_no_update")
        connection.commit()
        connection.close()
        with self.assertRaises(ResponseProfileMonitorStoreError) as raised:
            self._store()
        self.assertEqual(raised.exception.code, "STORE_SCHEMA_INVALID")

    def test_path_lock_and_concurrent_owner_hardening(self) -> None:
        with self._store() as store:
            with self.assertRaises(ResponseProfileMonitorStoreError) as raised:
                self._store()
            self.assertEqual(raised.exception.code, "STORE_ALREADY_OPEN")
            lock_path = self.path.with_name(f"{self.path.name}.lock")
            moved = lock_path.with_suffix(".moved")
            lock_path.rename(moved)
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
            os.close(descriptor)
            with self.assertRaises(ResponseProfileMonitorStoreError) as raised:
                store.load(self.stream)
            self.assertEqual(raised.exception.code, "STORE_LOCK_DRIFT")

    def test_unsafe_database_mode_hardlink_and_symlink_are_refused(self) -> None:
        with self._store():
            pass
        self.path.chmod(0o644)
        with self.assertRaises(ResponseProfileMonitorStoreError):
            self._store()
        self.path.chmod(0o600)
        hardlink = self.root / "hardlink.sqlite3"
        os.link(self.path, hardlink)
        with self.assertRaises(ResponseProfileMonitorStoreError):
            ResponseProfileMonitorStateStore(
                hardlink, expected_stream_key=self.stream
            )
        hardlink.unlink()
        symlink = self.root / "symlink.sqlite3"
        symlink.symlink_to(self.path)
        with self.assertRaises((OSError, ResponseProfileMonitorStoreError)):
            ResponseProfileMonitorStateStore(
                symlink, expected_stream_key=self.stream
            )

    def test_coherent_external_append_poison_stale_owner(self) -> None:
        with self._store() as store:
            initial = MonitorStreamState(
                self.stream,
                next_window_sequence=3,
                latest_detector_head=self._head(),
            )
            store.save(initial)
            connection = sqlite3.connect(self.path)
            previous = connection.execute(
                "SELECT state_record_sha256 FROM monitor_state_records "
                "ORDER BY state_record_sequence DESC LIMIT 1"
            ).fetchone()[0]
            changed = replace(initial, next_window_sequence=4)
            document = monitor_store_module._state_document(changed)
            payload = {
                "schema_version": monitor_store_module.STATE_RECORD_SCHEMA_VERSION,
                "state_record_sequence": 1,
                "monitor_state": document,
                "latest_detector_head_sha256": self._head().detector_head_sha256,
                "previous_state_record_sha256": previous,
            }
            digest = monitor_store_module._digest(
                monitor_store_module.STATE_RECORD_HASH_DOMAIN, payload
            )
            connection.execute(
                "INSERT INTO monitor_state_records VALUES(?,?,?,?,?)",
                (
                    1,
                    monitor_store_module.canonical_json_bytes(document).decode("utf-8"),
                    self._head().detector_head_sha256,
                    previous,
                    digest,
                ),
            )
            connection.commit()
            connection.close()
            with self.assertRaises(ResponseProfileMonitorStoreError) as raised:
                store.load(self.stream)
            self.assertEqual(raised.exception.code, "STORE_HEAD_DRIFT")
            with self.assertRaises(ResponseProfileMonitorStoreError) as raised:
                store.load(self.stream)
            self.assertEqual(raised.exception.code, "STORE_POISONED")

    def test_head_regression_is_rejected_without_losing_prior_state(self) -> None:
        with self._store() as store:
            store.save(MonitorStreamState(self.stream, latest_detector_head=self._head()))
            with self.assertRaises(ResponseProfileMonitorStoreError) as raised:
                store.save(MonitorStreamState(self.stream))
            self.assertEqual(raised.exception.code, "STORE_HEAD_REGRESSION")
            self.assertEqual(store.load_verified_latest(self.stream).head, self._head())

    def test_manual_wrapper_and_plain_head_are_not_verified_latest(self) -> None:
        with self.assertRaises(TypeError):
            VerifiedLatestResponseProfileDetectorHead()
        self.assertIsNot(type(self._head()), VerifiedLatestResponseProfileDetectorHead)

    def test_legacy_file_store_does_not_issue_or_retain_latest_authority(self) -> None:
        store = FileMonitorStateStore(self.root / "legacy")
        store.save(MonitorStreamState(self.stream, latest_detector_head=self._head()))
        self.assertIsNone(store.load(self.stream).latest_detector_head)
        self.assertFalse(hasattr(store, "load_verified_latest"))

    def test_real_monitor_evaluation_persists_verified_latest_head(self) -> None:
        events = [
            *_persist_events(self.root, stream_key=self.stream, window_sequence=0),
            *_persist_events(self.root, stream_key=self.stream, window_sequence=1),
            *_persist_events(self.root, stream_key=self.stream, window_sequence=2),
        ]
        with self._store() as store, patch(
            "vdbench.workload_monitor.extract_window_evidence",
            side_effect=_fast_evidence,
        ):
            monitor = WorkloadMonitor(
                source=FakeSource(events),
                state_store=store,
                policy_input_provider=RecordingPolicyInputProvider(),
                audit_sink=FakeAuditSink(),
                detector_seed=DETECTOR_SEED,
            )
            results = monitor.run_once(max_events=12)
            self.assertEqual(sum(item.policy_decision is not None for item in results), 1)
            latest = store.load_verified_latest(self.stream)
            self.assertEqual(latest.head.window_sequence, 2)
            self.assertEqual(
                latest.head.detector_provenance,
                next(item.drift_decision for item in results if item.drift_decision).evidence_provenance,
            )


if __name__ == "__main__":
    unittest.main()
