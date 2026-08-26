from __future__ import annotations

import multiprocessing
import os
import tempfile
import unittest
from pathlib import Path

from vdbench.gate_c_checkpoint_lock import (
    GateCCampaignCheckpointLock,
    GateCCampaignCheckpointLockError,
    campaign_checkpoint_lock_path,
)


def _lock_worker(
    legacy_path: str,
    start: multiprocessing.synchronize.Event,
    release: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
) -> None:
    start.wait(10)
    try:
        with GateCCampaignCheckpointLock(Path(legacy_path)):
            results.put(("ACQUIRED", os.getpid()))
            release.wait(10)
    except GateCCampaignCheckpointLockError as exc:
        results.put((exc.code, os.getpid()))


class GateCCheckpointLockTests(unittest.TestCase):
    def test_two_process_creators_have_exactly_one_owner(self) -> None:
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            legacy = root / "checkpoints.sqlite3"
            start = context.Event()
            release = context.Event()
            results = context.Queue()
            processes = [
                context.Process(
                    target=_lock_worker,
                    args=(str(legacy), start, release, results),
                )
                for _ in range(2)
            ]
            for process in processes:
                process.start()
            start.set()
            first = results.get(timeout=15)
            second = results.get(timeout=15)
            self.assertEqual(
                sorted(value[0] for value in (first, second)),
                ["ACQUIRED", "GATE_C_CHECKPOINT_AUTHORITY_OWNED"],
            )
            release.set()
            for process in processes:
                process.join(15)
                self.assertEqual(process.exitcode, 0)
            with GateCCampaignCheckpointLock(legacy) as reopened:
                reopened.assert_owned(legacy)

    @unittest.skipUnless(hasattr(os, "fork"), "requires os.fork")
    def test_forked_child_close_cannot_release_parent_lock(self) -> None:
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            legacy = root / "checkpoints.sqlite3"
            owner = GateCCampaignCheckpointLock(legacy)
            child_pid = os.fork()
            if child_pid == 0:
                owner.close()
                os._exit(0)
            waited, status = os.waitpid(child_pid, 0)
            self.assertEqual(waited, child_pid)
            self.assertEqual(status, 0)

            start = context.Event()
            release = context.Event()
            results = context.Queue()
            contender = context.Process(
                target=_lock_worker,
                args=(str(legacy), start, release, results),
            )
            contender.start()
            start.set()
            self.assertEqual(
                results.get(timeout=15)[0],
                "GATE_C_CHECKPOINT_AUTHORITY_OWNED",
            )
            release.set()
            contender.join(15)
            self.assertEqual(contender.exitcode, 0)
            owner.assert_owned(legacy)
            owner.close()
            with GateCCampaignCheckpointLock(legacy):
                pass

    def test_symlink_nonregular_and_path_replacement_refuse(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            legacy = root / "checkpoints.sqlite3"
            sidecar = campaign_checkpoint_lock_path(legacy)
            target = root / "target"
            target.write_text("lock", encoding="utf-8")
            os.chmod(target, 0o600)
            sidecar.symlink_to(target)
            with self.assertRaises(GateCCampaignCheckpointLockError):
                GateCCampaignCheckpointLock(legacy)
            sidecar.unlink()
            sidecar.mkdir()
            with self.assertRaises(GateCCampaignCheckpointLockError):
                GateCCampaignCheckpointLock(legacy)
            sidecar.rmdir()

            owner = GateCCampaignCheckpointLock(legacy)
            displaced = root / "displaced.lock"
            sidecar.rename(displaced)
            sidecar.write_text("replacement", encoding="utf-8")
            os.chmod(sidecar, 0o600)
            with self.assertRaises(GateCCampaignCheckpointLockError):
                owner.assert_owned(legacy)
            owner.close()


if __name__ == "__main__":
    unittest.main()
