"""Campaign-global exclusion for legacy and v3 bounded Gate-C authority.

The lock deliberately uses the historical checkpoint sidecar path.  Every
prospective authority creator therefore contends on one OS lock regardless of
which checkpoint schema it writes.  It is cooperative local-process safety,
not protection against a hostile same-user filesystem or Docker actor.
"""

from __future__ import annotations

import fcntl
import os
import stat
import threading
from pathlib import Path
from typing import Self

__all__ = [
    "GateCCampaignCheckpointLock",
    "GateCCampaignCheckpointLockError",
    "campaign_checkpoint_lock_path",
]


class GateCCampaignCheckpointLockError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> GateCCampaignCheckpointLockError:
    return GateCCampaignCheckpointLockError(code)


def campaign_checkpoint_lock_path(legacy_checkpoint_path: Path) -> Path:
    path = Path(legacy_checkpoint_path)
    return path.with_suffix(path.suffix + ".lock")


_REGISTRY_MUTEX = threading.Lock()
_OWNED_INODES: set[tuple[int, int]] = set()
_LIVE_LOCKS: dict[int, "GateCCampaignCheckpointLock"] = {}


def _after_fork_child() -> None:
    # A child must not call LOCK_UN on the inherited open-file description.
    # Closing its duplicate leaves a live parent's descriptor/lock intact.
    for lock in tuple(_LIVE_LOCKS.values()):
        lock._invalidate_after_fork()
    _LIVE_LOCKS.clear()
    _OWNED_INODES.clear()


os.register_at_fork(after_in_child=_after_fork_child)


class GateCCampaignCheckpointLock:
    """One process-owned nonblocking lock spanning all checkpoint versions."""

    def __init__(self, legacy_checkpoint_path: Path) -> None:
        self.legacy_checkpoint_path = Path(legacy_checkpoint_path)
        self.path = campaign_checkpoint_lock_path(self.legacy_checkpoint_path)
        self._owner_pid = os.getpid()
        self._fd = -1
        self._inode: tuple[int, int] | None = None
        self._closed = False
        self._open()

    def _open(self) -> None:
        try:
            parent = self.path.parent.stat(follow_symlinks=False)
        except OSError as exc:
            raise _error("GATE_C_CHECKPOINT_LOCK_PATH_UNSAFE") from exc
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.geteuid()
            or stat.S_IMODE(parent.st_mode) & 0o022
        ):
            raise _error("GATE_C_CHECKPOINT_LOCK_PATH_UNSAFE")
        try:
            fd = os.open(
                self.path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        except OSError as exc:
            raise _error("GATE_C_CHECKPOINT_LOCK_PATH_UNSAFE") from exc
        try:
            info = os.fstat(fd)
            path_info = os.lstat(self.path)
            inode = (info.st_dev, info.st_ino)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600
                or (path_info.st_dev, path_info.st_ino) != inode
            ):
                raise _error("GATE_C_CHECKPOINT_LOCK_PATH_UNSAFE")
            with _REGISTRY_MUTEX:
                if inode in _OWNED_INODES:
                    raise _error("GATE_C_CHECKPOINT_AUTHORITY_OWNED")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    raise _error("GATE_C_CHECKPOINT_AUTHORITY_OWNED") from exc
                _OWNED_INODES.add(inode)
                _LIVE_LOCKS[id(self)] = self
            self._fd = fd
            self._inode = inode
        except BaseException:
            os.close(fd)
            raise

    def assert_owned(self, legacy_checkpoint_path: Path | None = None) -> None:
        if (
            self._closed
            or self._fd < 0
            or self._inode is None
            or os.getpid() != self._owner_pid
        ):
            raise _error("GATE_C_CHECKPOINT_AUTHORITY_NOT_OWNED")
        if (
            legacy_checkpoint_path is not None
            and Path(legacy_checkpoint_path) != self.legacy_checkpoint_path
        ):
            raise _error("GATE_C_CHECKPOINT_AUTHORITY_PATH_MISMATCH")
        try:
            info = os.fstat(self._fd)
            path_info = os.lstat(self.path)
        except OSError as exc:
            raise _error("GATE_C_CHECKPOINT_LOCK_PATH_DRIFT") from exc
        if (
            (info.st_dev, info.st_ino) != self._inode
            or (path_info.st_dev, path_info.st_ino) != self._inode
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise _error("GATE_C_CHECKPOINT_LOCK_PATH_DRIFT")

    def _invalidate_after_fork(self) -> None:
        if self._fd >= 0:
            try:
                os.close(self._fd)
            except OSError:
                pass
        self._fd = -1
        self._inode = None
        self._closed = True

    def close(self) -> None:
        if self._closed:
            return
        owner = os.getpid() == self._owner_pid
        inode = self._inode
        self._closed = True
        if owner and inode is not None:
            with _REGISTRY_MUTEX:
                _LIVE_LOCKS.pop(id(self), None)
                _OWNED_INODES.discard(inode)
        if self._fd >= 0:
            try:
                if owner:
                    fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = -1
        self._inode = None

    def __enter__(self) -> Self:
        self.assert_owned()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
