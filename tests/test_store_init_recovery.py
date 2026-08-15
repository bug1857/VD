"""FINDING-003: a failed store construction must stay reopenable.

Every exclusive-writer SQLite store in the Gate-C composition takes an
exclusive `flock` and registers process-local inode ownership, and only *then*
performs the steps that can still fail -- database creation, path
re-verification, `sqlite3.connect`.  Before this fix, a failure in that window
propagated while the flock, lock descriptor, and ownership entry stayed held,
so the same process could never reopen the store: it saw `..._STORE_BUSY`
forever.  That is an availability trap, not a durability defect, but it makes a
Gate-C run unrecoverable without a process restart.

The fault injected here is a real production failure, not a monkeypatch: the
database path is left with mode `0o644`, which every one of these stores
rejects as unsafe *after* it has already claimed ownership.
"""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest

from vdbench.host_window_detector_v2 import SQLiteHostWindowDetectorV2Store
from vdbench.host_window_lineage import SQLiteHostResponseCommitStore
from vdbench.real_detector_attestation_store import (
    SQLiteRealDetectorAttestationStore,
)
from vdbench.shadow_attempt_store import SQLiteShadowAttemptStore
from vdbench.window_finalization import SQLiteWindowFinalizationStore
import vdbench.host_window_detector_v2 as host_window_detector_v2
import vdbench.host_window_lineage as host_window_lineage
import vdbench.real_detector_attestation_store as real_detector_attestation_store
import vdbench.shadow_attempt_store as shadow_attempt_store
import vdbench.window_finalization as window_finalization

from tests.test_real_detector_attestation import _ENVIRONMENT, _REVISION, _stream


#: `(label, module, path -> store)` for every exclusive-writer store the
#: Gate-C composition opens. The module is needed for its process-local
#: ownership registry, which is what a leak would poison.
_CASES = (
    (
        "shadow_attempt",
        shadow_attempt_store,
        lambda path: SQLiteShadowAttemptStore(
            path,
            stream_key=_stream(),
            source_revision=_REVISION,
            environment_manifest_sha256=_ENVIRONMENT,
        ),
    ),
    (
        "window_finalization",
        window_finalization,
        lambda path: SQLiteWindowFinalizationStore(
            path,
            stream_key=_stream(),
            source_revision=_REVISION,
            environment_manifest_sha256=_ENVIRONMENT,
        ),
    ),
    (
        "host_response_commit",
        host_window_lineage,
        lambda path: SQLiteHostResponseCommitStore(
            path,
            stream_key=_stream(),
            source_revision=_REVISION,
            environment_manifest_sha256=_ENVIRONMENT,
        ),
    ),
    (
        "host_window_detector_v2",
        host_window_detector_v2,
        lambda path: SQLiteHostWindowDetectorV2Store(path, stream_key=_stream()),
    ),
    (
        "real_detector_attestation",
        real_detector_attestation_store,
        lambda path: SQLiteRealDetectorAttestationStore(path, stream_key=_stream()),
    ),
)


class StoreInitFailureReleasesOwnershipTests(unittest.TestCase):
    def test_failure_after_ownership_releases_and_allows_reopen(self) -> None:
        for name, module, factory in _CASES:
            with self.subTest(store=name):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    os.chmod(root, 0o700)
                    path = root / "store.sqlite3"

                    # A world-readable database file: rejected only *after*
                    # the lock and the ownership entry are already claimed.
                    path.touch(mode=0o644)
                    os.chmod(path, 0o644)

                    before = set(module._OWNED_LOCK_INODES)
                    with self.assertRaises(Exception) as caught:
                        factory(path)
                    self.assertIn("UNSAFE", getattr(caught.exception, "code", ""))

                    # No ownership-table residue.
                    self.assertEqual(set(module._OWNED_LOCK_INODES), before)

                    # The same process reopens immediately: never ..._STORE_BUSY.
                    path.unlink()
                    store = factory(path)
                    try:
                        self.assertFalse(store._closed)
                    finally:
                        store.close()
                    self.assertEqual(set(module._OWNED_LOCK_INODES), before)

    def test_repeated_failures_do_not_accumulate_ownership(self) -> None:
        for name, module, factory in _CASES:
            with self.subTest(store=name):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    os.chmod(root, 0o700)
                    path = root / "store.sqlite3"
                    before = set(module._OWNED_LOCK_INODES)
                    for _ in range(3):
                        path.touch(mode=0o644)
                        os.chmod(path, 0o644)
                        with self.assertRaises(Exception):
                            factory(path)
                        self.assertEqual(set(module._OWNED_LOCK_INODES), before)
                        path.unlink()

    def test_a_second_distinct_failure_point_also_releases(self) -> None:
        """Injection at `os.open`, not `_verify_path`: same guarantee.

        The lock file already exists and opens fine, ownership is claimed, and
        only then does creating the database file fail because the parent
        directory is not writable -- a real disk-permission failure occurring
        at a different statement inside the vulnerable window.
        """

        for name, module, factory in _CASES:
            with self.subTest(store=name):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    os.chmod(root, 0o700)
                    path = root / "store.sqlite3"
                    # A first successful open leaves the lock file behind.
                    factory(path).close()
                    path.unlink()
                    before = set(module._OWNED_LOCK_INODES)
                    os.chmod(root, 0o500)
                    try:
                        with self.assertRaises(OSError):
                            factory(path)
                        self.assertEqual(set(module._OWNED_LOCK_INODES), before)
                    finally:
                        os.chmod(root, 0o700)
                    reopened = factory(path)
                    reopened.close()

    def test_lock_descriptor_is_not_leaked_on_failure(self) -> None:
        """A leaked descriptor would keep the kernel flock held after failure."""

        for name, module, factory in _CASES:
            with self.subTest(store=name):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    os.chmod(root, 0o700)
                    path = root / "store.sqlite3"
                    path.touch(mode=0o644)
                    os.chmod(path, 0o644)
                    with self.assertRaises(Exception):
                        factory(path)
                    lock_path = root / "store.sqlite3.lock"
                    self.assertTrue(lock_path.exists())
                    # If the flock survived, this exclusive re-lock would fail.
                    import fcntl

                    fd = os.open(lock_path, os.O_RDWR)
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        fcntl.flock(fd, fcntl.LOCK_UN)
                    finally:
                        os.close(fd)


class StoreInitSuccessPathUnchangedTests(unittest.TestCase):
    def test_normal_creation_still_succeeds_and_registers_ownership(self) -> None:
        for name, module, factory in _CASES:
            with self.subTest(store=name):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    os.chmod(root, 0o700)
                    path = root / "store.sqlite3"
                    before = set(module._OWNED_LOCK_INODES)
                    store = factory(path)
                    try:
                        self.assertGreater(
                            len(module._OWNED_LOCK_INODES), len(before)
                        )
                        self.assertTrue(path.exists())
                        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                    finally:
                        store.close()
                    self.assertEqual(set(module._OWNED_LOCK_INODES), before)

    def test_existing_store_reopen_is_unchanged(self) -> None:
        for name, module, factory in _CASES:
            with self.subTest(store=name):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    os.chmod(root, 0o700)
                    path = root / "store.sqlite3"
                    factory(path).close()
                    before = set(module._OWNED_LOCK_INODES)
                    store = factory(path)
                    try:
                        self.assertFalse(store._closed)
                    finally:
                        store.close()
                    self.assertEqual(set(module._OWNED_LOCK_INODES), before)

    def test_concurrent_same_process_open_is_still_refused(self) -> None:
        """The fix must not weaken exclusive-writer enforcement."""

        for name, module, factory in _CASES:
            with self.subTest(store=name):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    os.chmod(root, 0o700)
                    path = root / "store.sqlite3"
                    store = factory(path)
                    try:
                        with self.assertRaises(Exception) as caught:
                            factory(path)
                        self.assertIn(
                            "BUSY", getattr(caught.exception, "code", "")
                        )
                    finally:
                        store.close()
                    # And the refused second open left no residue of its own.
                    reopened = factory(path)
                    reopened.close()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
