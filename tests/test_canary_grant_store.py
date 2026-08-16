"""Failure-first tests for the EXP-009 Stage-2 one-time grant ledger."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from vdbench.canary_grant_store import (
    CanaryGrantUseStore,
    GrantUseStatus,
    GrantUseStoreError,
)

_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64
_RESERVED_AT = "2026-08-04T06:00:00Z"
_TERMINAL_AT = "2026-08-04T06:01:00Z"


class CanaryGrantUseStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self._temporary.name)
        self.path = self.directory / "grant-use.sqlite3"

    def tearDown(self) -> None:
        self._temporary.cleanup()

    def _store(self) -> CanaryGrantUseStore:
        return CanaryGrantUseStore(self.path, lock_timeout_seconds=0.25)

    def test_reservation_survives_restart_and_loads_exactly(self) -> None:
        initial = self._store()
        result = initial.reserve(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_A,
            reserved_at_utc=_RESERVED_AT,
        )

        self.assertTrue(result.accepted)
        self.assertEqual(result.record.status, GrantUseStatus.RESERVED)
        restarted = CanaryGrantUseStore(self.path, lock_timeout_seconds=0.25)
        loaded = restarted.load("grant-exp009-001")

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.grant_id, "grant-exp009-001")
        self.assertEqual(loaded.signed_payload_sha256, _DIGEST_A)
        self.assertEqual(loaded.reserved_at_utc, _RESERVED_AT)
        self.assertEqual(loaded.status, GrantUseStatus.RESERVED)
        self.assertIsNone(loaded.terminal_reason_code)

    def test_same_grant_replay_and_same_payload_conflict_fail_closed(self) -> None:
        store = self._store()
        self.assertTrue(
            store.reserve(
                grant_id="grant-exp009-001",
                signed_payload_sha256=_DIGEST_A,
                reserved_at_utc=_RESERVED_AT,
            ).accepted
        )

        replay = store.reserve(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_A,
            reserved_at_utc=_TERMINAL_AT,
        )
        conflicting_payload = store.reserve(
            grant_id="grant-exp009-002",
            signed_payload_sha256=_DIGEST_A,
            reserved_at_utc=_TERMINAL_AT,
        )

        self.assertFalse(replay.accepted)
        self.assertEqual(replay.reason_code, "GRANT_ID_ALREADY_RESERVED")
        self.assertFalse(conflicting_payload.accepted)
        self.assertEqual(conflicting_payload.reason_code, "SIGNED_PAYLOAD_ALREADY_RESERVED")
        self.assertIsNone(store.load("grant-exp009-002"))

    def test_terminal_event_is_atomic_deterministic_and_immutable(self) -> None:
        store = self._store()
        store.reserve(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_A,
            reserved_at_utc=_RESERVED_AT,
        )

        terminal = store.record_terminal(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_A,
            reason_code="REFUSED_AUDIT_WRITE_FAILED",
            occurred_at_utc=_TERMINAL_AT,
        )
        replay = store.record_terminal(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_A,
            reason_code="RECOVERY_FAILBACK",
            occurred_at_utc=_TERMINAL_AT,
        )
        restarted = CanaryGrantUseStore(self.path, lock_timeout_seconds=0.25)
        loaded = restarted.load("grant-exp009-001")

        self.assertTrue(terminal.accepted)
        self.assertEqual(terminal.record.status, GrantUseStatus.TERMINAL)
        self.assertEqual(terminal.record.terminal_reason_code, "REFUSED_AUDIT_WRITE_FAILED")
        self.assertEqual(len(terminal.record.terminal_record_id or ""), 64)
        self.assertFalse(replay.accepted)
        self.assertEqual(replay.reason_code, "GRANT_ALREADY_TERMINAL")
        self.assertEqual(loaded, terminal.record)

    def test_terminal_requires_matching_reserved_payload_and_existing_grant(self) -> None:
        store = self._store()
        store.reserve(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_A,
            reserved_at_utc=_RESERVED_AT,
        )

        wrong_payload = store.record_terminal(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_B,
            reason_code="REFUSED_AUDIT_WRITE_FAILED",
            occurred_at_utc=_TERMINAL_AT,
        )
        missing = store.record_terminal(
            grant_id="grant-exp009-404",
            signed_payload_sha256=_DIGEST_B,
            reason_code="REFUSED_AUDIT_WRITE_FAILED",
            occurred_at_utc=_TERMINAL_AT,
        )

        self.assertFalse(wrong_payload.accepted)
        self.assertEqual(wrong_payload.reason_code, "SIGNED_PAYLOAD_MISMATCH")
        self.assertFalse(missing.accepted)
        self.assertEqual(missing.reason_code, "GRANT_NOT_RESERVED")
        self.assertEqual(store.load("grant-exp009-001").status, GrantUseStatus.RESERVED)

    def test_concurrent_same_grant_reservation_is_linearized(self) -> None:
        barrier = threading.Barrier(2)
        results: list[object] = []
        failures: list[BaseException] = []

        def reserve() -> None:
            try:
                store = CanaryGrantUseStore(self.path, lock_timeout_seconds=1.0)
                barrier.wait(timeout=2.0)
                results.append(
                    store.reserve(
                        grant_id="grant-exp009-001",
                        signed_payload_sha256=_DIGEST_A,
                        reserved_at_utc=_RESERVED_AT,
                    )
                )
            except BaseException as exc:  # test records concurrent failures explicitly  # noqa: BLE001
                failures.append(exc)

        first = threading.Thread(target=reserve)
        second = threading.Thread(target=reserve)
        first.start()
        second.start()
        first.join(timeout=5.0)
        second.join(timeout=5.0)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(sum(result.accepted for result in results), 1)
        self.assertEqual(
            sorted(result.reason_code for result in results if not result.accepted),
            ["GRANT_ID_ALREADY_RESERVED"],
        )

    def test_corrupt_or_schema_mismatched_store_fails_closed(self) -> None:
        self.path.write_bytes(b"not a sqlite database")
        with self.assertRaisesRegex(GrantUseStoreError, "GRANT_USE_STORE_CORRUPTED"):
            self._store()

        self.path.unlink()
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("PRAGMA user_version = 2")
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(GrantUseStoreError, "GRANT_USE_STORE_SCHEMA_MISMATCH"):
            self._store()

        self.path.unlink()
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("CREATE TABLE grant_reservations (grant_id TEXT)")
            connection.execute("CREATE TABLE grant_terminal_events (grant_id TEXT)")
            connection.execute("PRAGMA user_version = 1")
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(GrantUseStoreError, "GRANT_USE_STORE_SCHEMA_MISMATCH"):
            self._store()

    def test_audit_failure_reason_consumes_grant_across_restart(self) -> None:
        store = self._store()
        store.reserve(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_A,
            reserved_at_utc=_RESERVED_AT,
        )
        consumption = store.record_terminal(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_A,
            reason_code="REFUSED_AUDIT_WRITE_FAILED",
            occurred_at_utc=_TERMINAL_AT,
        )
        restarted = CanaryGrantUseStore(self.path, lock_timeout_seconds=0.25)
        replay = restarted.reserve(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_A,
            reserved_at_utc=_TERMINAL_AT,
        )

        self.assertTrue(consumption.accepted)
        self.assertFalse(replay.accepted)
        self.assertEqual(replay.reason_code, "GRANT_ID_ALREADY_RESERVED")
        self.assertEqual(
            restarted.load("grant-exp009-001").terminal_reason_code,
            "REFUSED_AUDIT_WRITE_FAILED",
        )

    def test_lock_contention_is_bounded_and_fails_closed(self) -> None:
        self._store()
        blocker = sqlite3.connect(self.path, isolation_level=None)
        try:
            blocker.execute("BEGIN EXCLUSIVE")
            with self.assertRaisesRegex(GrantUseStoreError, "GRANT_USE_STORE_UNAVAILABLE"):
                CanaryGrantUseStore(
                    self.path,
                    lock_timeout_seconds=0.001,
                ).reserve(
                    grant_id="grant-exp009-001",
                    signed_payload_sha256=_DIGEST_A,
                    reserved_at_utc=_RESERVED_AT,
                )
        finally:
            blocker.rollback()
            blocker.close()

    def test_non_private_parent_directory_is_rejected_before_opening_store(self) -> None:
        unsafe_directory = self.directory / "unsafe"
        unsafe_directory.mkdir()
        os.chmod(unsafe_directory, 0o755)

        with self.assertRaisesRegex(GrantUseStoreError, "GRANT_USE_STORE_DIRECTORY_NOT_PRIVATE"):
            CanaryGrantUseStore(unsafe_directory / "grant-use.sqlite3")

    def test_tampered_stored_values_fail_closed_as_store_corruption(self) -> None:
        store = self._store()
        store.reserve(
            grant_id="grant-exp009-001",
            signed_payload_sha256=_DIGEST_A,
            reserved_at_utc=_RESERVED_AT,
        )
        connection = sqlite3.connect(self.path)
        try:
            connection.execute(
                "UPDATE grant_reservations SET signed_payload_sha256 = 'not-a-digest'"
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaisesRegex(GrantUseStoreError, "GRANT_USE_STORE_CORRUPTED"):
            store.load("grant-exp009-001")


if __name__ == "__main__":
    unittest.main()
