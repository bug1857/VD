"""Per-stream router that lets WorkloadMonitor use the hardened store directly.

``WorkloadMonitor`` (the ADR-005 DRY_RUN/offline monitoring loop) is injected
with a single ``MonitorStateStore``, but real deployments run more than one
monitor stream against one directory (see ``FileMonitorStateStore``, which
routes by ``sha256(stream_id)`` internally).  ``ResponseProfileMonitorStateStore``
binds exactly one SQLite file to exactly one stream, so it cannot be handed to
``WorkloadMonitor`` directly when more than one stream is in play.

This router is a thin per-stream multiplexer: it lazily opens and caches one
``ResponseProfileMonitorStateStore`` per ``stream_id`` (same hashing convention
as ``FileMonitorStateStore``) and otherwise conforms exactly to the
``MonitorStateStore`` protocol (``load``/``save``).

Deliberately NOT a dual-write design.  ``ResponseProfileMonitorStateStore``
already persists the complete ``MonitorStreamState`` (not just the detector
head) and already satisfies ``MonitorStateStore`` on its own -- so routing to
it makes it the *sole* source of truth for whichever streams are configured
through this router.  There is no second, independently-authoritative copy of
state to drift out of sync with, so no divergence-latch/reconciliation
machinery is needed: one store, one truth.

The legacy ``FileMonitorStateStore`` (JSON) remains available, unmodified, and
non-authorizing for callers who do not opt into hardened storage -- nothing
about this router changes that path.

Fail-closed note: unlike ``FileMonitorStateStore``, whose corruption surfaces
as ``MonitorStateCorruptedError`` and is handled by ``WorkloadMonitor`` as a
single-event reject-and-continue, this router does NOT translate
``ResponseProfileMonitorStoreError`` into that type. A hardened-store
integrity failure (poisoned instance, broken hash chain, lock/path drift,
concurrent-owner collision, ...) is a materially more serious signal than "a
JSON file is missing" and must abort the monitoring cycle loudly rather than
being silently downgraded into the JSON store's benign per-event recovery
path. Choosing this router as ``state_store=`` is therefore a deliberate
trade: hardened, freshness-capable persistence in exchange for losing the
JSON store's tolerance of one-off state-file corruption.
"""

from __future__ import annotations

import hashlib
import os
import stat
import threading
from pathlib import Path
from typing import Callable

from .response_profile_monitor_store import (
    ResponseProfileMonitorStateStore,
    VerifiedLatestResponseProfileDetectorHead,
)
from .shadow_event_types import MonitorStreamKey
from .workload_monitor import MonitorStreamState

__all__ = [
    "ResponseProfileMonitorStoreRouterError",
    "ResponseProfileMonitorStoreRouter",
]


class ResponseProfileMonitorStoreRouterError(RuntimeError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileMonitorStoreRouterError:
    return ResponseProfileMonitorStoreRouterError(message, code=code)


class ResponseProfileMonitorStoreRouter:
    """Lazily-opened, cached per-stream hardened store, keyed by ``stream_id``."""

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        utc_now: Callable[[], str] | None = None,
    ) -> None:
        self._directory = Path(directory)
        directory_existed = self._directory.exists()
        self._directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            if not directory_existed:
                # Only normalize permissions on a directory this call just
                # created (umask can otherwise widen the requested 0o700).
                # A pre-existing directory is verified as-is, never silently
                # corrected -- correcting it would defeat the check below.
                os.chmod(self._directory, 0o700)
            parent_stat = self._directory.stat()
        except OSError as exc:
            raise _error("ROUTER_DIRECTORY_INVALID", "router directory is unavailable") from exc
        if (
            not stat.S_ISDIR(parent_stat.st_mode)
            or parent_stat.st_uid != os.geteuid()
            or stat.S_IMODE(parent_stat.st_mode) & 0o022
        ):
            raise _error("ROUTER_DIRECTORY_INVALID", "router directory must be owner-controlled")
        self._utc_now = utc_now
        self._mutex = threading.RLock()
        self._stores: dict[str, ResponseProfileMonitorStateStore] = {}
        self._closed = False

    def __enter__(self) -> "ResponseProfileMonitorStoreRouter":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _path_for(self, stream_id: str) -> Path:
        digest = hashlib.sha256(stream_id.encode("utf-8")).hexdigest()
        return self._directory / f"{digest}.sqlite3"

    def _store_for(self, stream_key: MonitorStreamKey) -> ResponseProfileMonitorStateStore:
        with self._mutex:
            if self._closed:
                raise _error("ROUTER_CLOSED", "router is closed")
            existing = self._stores.get(stream_key.stream_id)
            if existing is not None:
                return existing
            store = ResponseProfileMonitorStateStore(
                self._path_for(stream_key.stream_id),
                expected_stream_key=stream_key,
                utc_now=self._utc_now,
            )
            self._stores[stream_key.stream_id] = store
            return store

    def store_for(self, stream_key: MonitorStreamKey) -> ResponseProfileMonitorStateStore:
        """Expose the underlying hardened per-stream store (lazily opened).

        Freshness-evidence issuance and future ADR-011 work need direct access
        to ``load_verified_latest`` on the exact store instance backing a
        stream, not just the ``MonitorStateStore``-shaped ``load``/``save``
        this router exposes for ``WorkloadMonitor``.
        """
        return self._store_for(stream_key)

    def load(self, stream_key: MonitorStreamKey) -> MonitorStreamState | None:
        return self._store_for(stream_key).load(stream_key)

    def save(self, state: MonitorStreamState) -> None:
        self._store_for(state.stream_key).save(state)

    def load_verified_latest(
        self, stream_key: MonitorStreamKey
    ) -> VerifiedLatestResponseProfileDetectorHead | None:
        return self._store_for(stream_key).load_verified_latest(stream_key)

    def close(self) -> None:
        with self._mutex:
            if self._closed:
                return
            self._closed = True
            stores = tuple(self._stores.values())
            self._stores.clear()
        for store in stores:
            store.close()
