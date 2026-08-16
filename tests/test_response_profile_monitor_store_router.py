"""Coverage for ResponseProfileMonitorStoreRouter as WorkloadMonitor's sole store."""

from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.test_workload_monitor import (
    DETECTOR_SEED,
    FakeAuditSink,
    FakeSource,
    RecordingPolicyInputProvider,
    _fast_evidence,
    _persist_events,
    _stream_key,
)
from vdbench.response_profile_monitor_store import ResponseProfileMonitorStoreError
from vdbench.response_profile_monitor_store_router import (
    ResponseProfileMonitorStoreRouter,
    ResponseProfileMonitorStoreRouterError,
)
from vdbench.workload_monitor import WorkloadMonitor


class ResponseProfileMonitorStoreRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self._events_dir = tempfile.TemporaryDirectory()
        self._store_dir = tempfile.TemporaryDirectory()
        self.events_root = Path(self._events_dir.name)
        self.store_root = Path(self._store_dir.name) / "response-profile-monitor-state"

    def tearDown(self) -> None:
        self._events_dir.cleanup()
        self._store_dir.cleanup()

    def _router(self, **kwargs) -> ResponseProfileMonitorStoreRouter:
        return ResponseProfileMonitorStoreRouter(self.store_root, **kwargs)

    def _run_stream_to_evaluated(self, router: ResponseProfileMonitorStoreRouter, stream):
        events = [
            *_persist_events(self.events_root, stream_key=stream, window_sequence=0),
            *_persist_events(self.events_root, stream_key=stream, window_sequence=1),
            *_persist_events(self.events_root, stream_key=stream, window_sequence=2),
        ]
        with patch(
            "vdbench.workload_monitor.extract_window_evidence",
            side_effect=_fast_evidence,
        ):
            monitor = WorkloadMonitor(
                source=FakeSource(events),
                state_store=router,
                policy_input_provider=RecordingPolicyInputProvider(),
                audit_sink=FakeAuditSink(),
                detector_seed=DETECTOR_SEED,
            )
            return monitor.run_once(max_events=12)

    def test_router_directory_created_owner_only(self) -> None:
        with self._router():
            mode = stat.S_IMODE(self.store_root.stat().st_mode)
            self.assertEqual(mode, 0o700)

    def test_router_serves_as_sole_workload_monitor_store(self) -> None:
        stream = _stream_key(stream_id="stream-solo")
        with self._router() as router:
            results = self._run_stream_to_evaluated(router, stream)
            self.assertEqual(sum(item.policy_decision is not None for item in results), 1)
            latest = router.load_verified_latest(stream)
            self.assertEqual(latest.head.window_sequence, 2)
            self.assertEqual(
                latest.head.detector_provenance,
                next(item.drift_decision for item in results if item.drift_decision).evidence_provenance,
            )
            loaded_state = router.load(stream)
            self.assertEqual(loaded_state.latest_detector_head, latest.head)

    def test_two_streams_route_to_distinct_files_without_cross_leakage(self) -> None:
        stream_a = _stream_key(stream_id="stream-a", configuration="config-a")
        stream_b = _stream_key(stream_id="stream-b", configuration="config-b")
        with self._router() as router:
            self._run_stream_to_evaluated(router, stream_a)
            self._run_stream_to_evaluated(router, stream_b)

            path_a = router._path_for(stream_a.stream_id)
            path_b = router._path_for(stream_b.stream_id)
            self.assertTrue(path_a.exists())
            self.assertTrue(path_b.exists())
            self.assertNotEqual(path_a, path_b)

            latest_a = router.load_verified_latest(stream_a)
            latest_b = router.load_verified_latest(stream_b)
            self.assertEqual(latest_a.head.stream_key, stream_a)
            self.assertEqual(latest_b.head.stream_key, stream_b)
            self.assertNotEqual(latest_a.head.detector_head_sha256, latest_b.head.detector_head_sha256)

    def test_restart_reopens_and_reverifies_existing_data(self) -> None:
        stream = _stream_key(stream_id="stream-restart")
        with self._router() as router:
            self._run_stream_to_evaluated(router, stream)
        # Router closed (flocks released); reopen fresh, simulating a process restart.
        with self._router() as reopened:
            latest = reopened.load_verified_latest(stream)
            self.assertEqual(latest.head.window_sequence, 2)
            state = reopened.load(stream)
            self.assertEqual(state.next_window_sequence, 3)

    def test_close_releases_locks_so_reopen_succeeds(self) -> None:
        stream = _stream_key(stream_id="stream-lock")
        router = self._router()
        self._run_stream_to_evaluated(router, stream)
        router.close()
        # If the flock were still held, opening a fresh store at the same path
        # would raise STORE_ALREADY_OPEN.
        with self._router() as reopened:
            self.assertIsNotNone(reopened.load(stream))

    def test_operations_after_close_raise_router_closed(self) -> None:
        stream = _stream_key(stream_id="stream-closed")
        router = self._router()
        router.close()
        with self.assertRaises(ResponseProfileMonitorStoreRouterError) as ctx:
            router.load(stream)
        self.assertEqual(ctx.exception.code, "ROUTER_CLOSED")

    def test_hardened_integrity_failure_propagates_uncaught_not_downgraded(self) -> None:
        """A store integrity violation must abort access, not be swallowed as
        a benign per-event MonitorStateCorruptedError reject-and-continue --
        that graceful path exists only for the legacy JSON store's failure
        modes (missing/malformed file), not for hardened-store tamper signals.
        """
        stream = _stream_key(stream_id="stream-corrupt")
        router = self._router()
        self._run_stream_to_evaluated(router, stream)
        db_path = router._path_for(stream.stream_id)
        router.close()
        # Same tamper technique as test_response_profile_monitor_store.py's
        # test_unsafe_database_mode_hardlink_and_symlink_are_refused: widen
        # the file mode so the store's owner-only-regular-file guard refuses
        # it outright on reopen -- a hardened-store-specific failure mode the
        # router must not paper over.
        db_path.chmod(0o644)
        with (
            self.assertRaises(ResponseProfileMonitorStoreError),
            self._router() as reopened,
        ):
            reopened.load_verified_latest(stream)

    def test_directory_must_be_owner_controlled(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "shared"
            path.mkdir(mode=0o777)
            os.chmod(path, 0o777)
            with self.assertRaises(ResponseProfileMonitorStoreRouterError) as ctx:
                ResponseProfileMonitorStoreRouter(path)
            self.assertEqual(ctx.exception.code, "ROUTER_DIRECTORY_INVALID")


if __name__ == "__main__":
    unittest.main()
