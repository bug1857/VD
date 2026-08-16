"""Hardened atomic monitor-state/latest-detector-head SQLite store.

The legacy JSON monitor store remains suitable for DRY_RUN recovery only.  This
store is the ADR-010 boundary that atomically appends one canonical monitor
state snapshot and, when present, its new detector head in the same transaction.
It issues latest-head snapshots only after complete chain verification.
"""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import stat
import threading
from collections.abc import Callable
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Self

from .artifacts import canonical_json_bytes
from .lkg_window_readiness import parse_rfc3339_utc_instant, validate_rfc3339_utc
from .response_profile_detector_head import (
    ResponseProfileDetectorHead,
    response_profile_detector_head_document,
    response_profile_detector_head_from_document,
    verify_response_profile_detector_head,
)
from .shadow_event_types import MonitorStreamKey
from .workload_monitor import MonitorStreamState, _state_document, _state_from_document

STORE_SCHEMA_VERSION = 1
STORE_BINDING_SCHEMA_VERSION = "response-profile-monitor-store-binding-v1"
STATE_RECORD_SCHEMA_VERSION = "response-profile-monitor-state-record-v1"
HEAD_RECORD_SCHEMA_VERSION = "response-profile-detector-head-record-v1"
STORE_BINDING_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_MONITOR_STORE_BINDING::V1\x00"
STATE_RECORD_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_MONITOR_STATE_RECORD::V1\x00"
HEAD_RECORD_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_DETECTOR_HEAD_RECORD::V1\x00"

__all__ = [
    "ResponseProfileMonitorStateStore",
    "ResponseProfileMonitorStoreError",
    "VerifiedLatestResponseProfileDetectorHead",
]


class ResponseProfileMonitorStoreError(RuntimeError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileMonitorStoreError:
    return ResponseProfileMonitorStoreError(message, code=code)


_ISSUE_TOKEN = object()


@dataclass(frozen=True, slots=True, init=False)
class VerifiedLatestResponseProfileDetectorHead:
    """Store-issued snapshot of the head latest at one verified read instant."""

    head: ResponseProfileDetectorHead
    head_record_sequence: int
    head_record_sha256: str
    head_record_persisted_at_utc: str

    def __init__(self, *, _token: object, **values: object) -> None:
        if _token is not _ISSUE_TOKEN:
            raise TypeError("verified latest detector heads are store-issued")
        for name, value in values.items():
            object.__setattr__(self, name, value)


_OWNERSHIP_LOCK = threading.Lock()
_OWNED_LOCK_INODES: set[tuple[int, int]] = set()


_SCHEMA_SQL = (
    """CREATE TABLE store_binding (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        binding_json TEXT NOT NULL,
        binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256) = 64)
    ) STRICT""",
    """CREATE TABLE detector_head_records (
        head_record_sequence INTEGER PRIMARY KEY CHECK(head_record_sequence >= 0),
        state_record_sequence INTEGER NOT NULL UNIQUE CHECK(state_record_sequence >= 0),
        head_json TEXT NOT NULL,
        persisted_at_utc TEXT NOT NULL,
        previous_head_record_sha256 TEXT,
        head_record_sha256 TEXT NOT NULL UNIQUE CHECK(length(head_record_sha256) = 64),
        CHECK((head_record_sequence = 0 AND previous_head_record_sha256 IS NULL) OR
              (head_record_sequence > 0 AND length(previous_head_record_sha256) = 64))
    ) STRICT""",
    """CREATE TABLE monitor_state_records (
        state_record_sequence INTEGER PRIMARY KEY CHECK(state_record_sequence >= 0),
        state_json TEXT NOT NULL,
        latest_detector_head_sha256 TEXT,
        previous_state_record_sha256 TEXT,
        state_record_sha256 TEXT NOT NULL UNIQUE CHECK(length(state_record_sha256) = 64),
        CHECK(latest_detector_head_sha256 IS NULL OR length(latest_detector_head_sha256) = 64),
        CHECK((state_record_sequence = 0 AND previous_state_record_sha256 IS NULL) OR
              (state_record_sequence > 0 AND length(previous_state_record_sha256) = 64))
    ) STRICT""",
    """CREATE TRIGGER store_binding_no_update BEFORE UPDATE ON store_binding
    BEGIN SELECT RAISE(ABORT, 'store_binding is append-only'); END""",
    """CREATE TRIGGER store_binding_no_delete BEFORE DELETE ON store_binding
    BEGIN SELECT RAISE(ABORT, 'store_binding is append-only'); END""",
    """CREATE TRIGGER detector_head_records_no_update BEFORE UPDATE ON detector_head_records
    BEGIN SELECT RAISE(ABORT, 'detector_head_records is append-only'); END""",
    """CREATE TRIGGER detector_head_records_no_delete BEFORE DELETE ON detector_head_records
    BEGIN SELECT RAISE(ABORT, 'detector_head_records is append-only'); END""",
    """CREATE TRIGGER monitor_state_records_no_update BEFORE UPDATE ON monitor_state_records
    BEGIN SELECT RAISE(ABORT, 'monitor_state_records is append-only'); END""",
    """CREATE TRIGGER monitor_state_records_no_delete BEFORE DELETE ON monitor_state_records
    BEGIN SELECT RAISE(ABORT, 'monitor_state_records is append-only'); END""",
)


def _normalized_sql(value: str) -> str:
    return " ".join(value.strip().removesuffix(";").split())


def _stream_document(value: MonitorStreamKey) -> dict[str, object]:
    if type(value) is not MonitorStreamKey:
        raise _error("STORE_STREAM_INVALID", "stream key must be concrete")
    try:
        rebuilt = MonitorStreamKey(
            value.stream_id,
            value.metric,
            value.threshold_stratum,
            value.configuration_identity,
            value.data_identity,
            value.flat_binding_id,
            value.hnsw_binding_id,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("STORE_STREAM_INVALID", "stream key is invalid") from exc
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("STORE_STREAM_INVALID", "stream key is noncanonical")
    return {
        "stream_id": value.stream_id,
        "metric": value.metric.value,
        "threshold_stratum": value.threshold_stratum,
        "configuration_identity": value.configuration_identity,
        "data_identity": value.data_identity,
        "flat_binding_id": value.flat_binding_id,
        "hnsw_binding_id": value.hnsw_binding_id,
    }


def _binding_payload(
    stream_key: MonitorStreamKey, *, store_instance_id: str
) -> dict[str, object]:
    if type(store_instance_id) is not str or len(store_instance_id) != 64:
        raise _error("STORE_BINDING_INVALID", "store instance ID must be 256-bit hex")
    try:
        bytes.fromhex(store_instance_id)
    except ValueError as exc:
        raise _error("STORE_BINDING_INVALID", "store instance ID must be lowercase hex") from exc
    if store_instance_id.lower() != store_instance_id:
        raise _error("STORE_BINDING_INVALID", "store instance ID must be lowercase hex")
    return {
        "schema_version": STORE_BINDING_SCHEMA_VERSION,
        "store_instance_id": store_instance_id,
        "stream": _stream_document(stream_key),
    }


def _digest(domain: bytes, payload: object) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(payload)).hexdigest()


def _json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _load_json(value: str) -> object:
    return json.loads(value, object_pairs_hook=_json_object)


def _default_utc_now() -> str:
    now = datetime.now(UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"


def _timestamp(value: object, *, field: str) -> str:
    try:
        validate_rfc3339_utc(value, field=field)
    except (TypeError, ValueError) as exc:
        raise _error("STORE_TIMESTAMP_INVALID", f"{field} is invalid") from exc
    assert type(value) is str
    return value


def _private_regular(path: Path, *, code: str) -> os.stat_result:
    try:
        result = path.lstat()
    except OSError as exc:
        raise _error(code, f"cannot inspect {path.name}") from exc
    if not stat.S_ISREG(result.st_mode) or result.st_nlink != 1:
        raise _error(code, f"{path.name} must be a non-hardlinked regular file")
    if result.st_uid != os.geteuid() or stat.S_IMODE(result.st_mode) != 0o600:
        raise _error(code, f"{path.name} must be owner-only 0600")
    return result


class ResponseProfileMonitorStateStore:
    """One-stream append-only SQLite monitor state and detector-head store."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        expected_stream_key: MonitorStreamKey,
        utc_now: Callable[[], str] | None = None,
    ) -> None:
        self._path = Path(path)
        self._stream_key = expected_stream_key
        _stream_document(expected_stream_key)
        self._mutex = threading.RLock()
        self._pid = os.getpid()
        self._closed = False
        self._poisoned = False
        self._lock_fd = -1
        self._lock_inode: tuple[int, int] | None = None
        self._db_inode: tuple[int, int] | None = None
        self._connection: sqlite3.Connection | None = None
        self._store_instance_id: str | None = None
        self._expected_fingerprint: tuple[object, ...] | None = None
        self._utc_now = utc_now or _default_utc_now
        self._open()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _open(self) -> None:
        parent = self._path.parent
        try:
            parent_stat = parent.stat()
        except OSError as exc:
            raise _error("STORE_PARENT_INVALID", "store parent is unavailable") from exc
        if (
            not stat.S_ISDIR(parent_stat.st_mode)
            or parent_stat.st_uid != os.geteuid()
            or stat.S_IMODE(parent_stat.st_mode) & 0o022
        ):
            raise _error("STORE_PARENT_INVALID", "store parent must be owner-controlled")
        lock_path = self._path.with_name(f"{self._path.name}.lock")
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        lock_existed = lock_path.exists()
        try:
            self._lock_fd = os.open(lock_path, flags, 0o600)
            lock_stat = os.fstat(self._lock_fd)
            if not lock_existed:
                os.fchmod(self._lock_fd, 0o600)
                lock_stat = os.fstat(self._lock_fd)
            if (
                not stat.S_ISREG(lock_stat.st_mode)
                or lock_stat.st_nlink != 1
                or lock_stat.st_uid != os.geteuid()
                or stat.S_IMODE(lock_stat.st_mode) != 0o600
            ):
                raise _error("STORE_LOCK_INVALID", "lock file is unsafe")
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise _error(
                    "STORE_ALREADY_OPEN",
                    "store is already owned by another process or instance",
                ) from exc
            inode = (lock_stat.st_dev, lock_stat.st_ino)
            with _OWNERSHIP_LOCK:
                if inode in _OWNED_LOCK_INODES:
                    raise _error("STORE_ALREADY_OPEN", "store is already owned in this process")
                _OWNED_LOCK_INODES.add(inode)
            self._lock_inode = inode
            created = not self._path.exists()
            if created:
                descriptor = os.open(
                    self._path,
                    os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                os.close(descriptor)
            db_stat = _private_regular(self._path, code="STORE_PATH_INVALID")
            self._db_inode = (db_stat.st_dev, db_stat.st_ino)
            connection = sqlite3.connect(self._path, isolation_level=None, timeout=0.0)
            connection.row_factory = sqlite3.Row
            self._connection = connection
            if created:
                self._store_instance_id = secrets.token_hex(32)
                mode = connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
                if str(mode).lower() != "delete":
                    raise _error("STORE_JOURNAL_INVALID", "new store did not enter DELETE journal mode")
                connection.execute("PRAGMA synchronous=FULL")
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA trusted_schema=OFF")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_SQL:
                        connection.execute(statement)
                    binding = _binding_payload(
                        self._stream_key,
                        store_instance_id=self._store_instance_id,
                    )
                    connection.execute(
                        "INSERT INTO store_binding(singleton,binding_json,binding_sha256) VALUES(1,?,?)",
                        (canonical_json_bytes(binding).decode("utf-8"), _digest(STORE_BINDING_HASH_DOMAIN, binding)),
                    )
                    connection.execute(f"PRAGMA user_version={STORE_SCHEMA_VERSION}")
                    connection.execute("COMMIT")
                except BaseException:
                    connection.execute("ROLLBACK")
                    raise
            else:
                mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
                if str(mode).lower() != "delete":
                    raise _error("STORE_JOURNAL_INVALID", "existing store journal mode must already be DELETE")
                connection.execute("PRAGMA synchronous=FULL")
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA trusted_schema=OFF")
                self._store_instance_id = self._read_store_instance_id(connection)
            self._verify_all()
            self._expected_fingerprint = self._fingerprint(connection)
        except BaseException:
            self.close()
            raise

    def _guard(self) -> sqlite3.Connection:
        if self._closed or self._connection is None:
            raise _error("STORE_CLOSED", "store is closed")
        if os.getpid() != self._pid:
            self._poisoned = True
            raise _error("STORE_FORKED", "store cannot be used after fork")
        if self._poisoned:
            raise _error("STORE_POISONED", "store instance is poisoned")
        if self._lock_inode is None:
            self._poisoned = True
            raise _error("STORE_LOCK_DRIFT", "lock ownership is unavailable")
        lock_path = self._path.with_name(f"{self._path.name}.lock")
        lock_stat = _private_regular(lock_path, code="STORE_LOCK_DRIFT")
        if (lock_stat.st_dev, lock_stat.st_ino) != self._lock_inode:
            self._poisoned = True
            raise _error("STORE_LOCK_DRIFT", "lock path inode changed")
        db_stat = _private_regular(self._path, code="STORE_PATH_DRIFT")
        if (db_stat.st_dev, db_stat.st_ino) != self._db_inode:
            self._poisoned = True
            raise _error("STORE_PATH_DRIFT", "database path inode changed")
        return self._connection

    def _read_store_instance_id(self, connection: sqlite3.Connection) -> str:
        row = connection.execute(
            "SELECT binding_json FROM store_binding WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise _error("STORE_BINDING_INVALID", "store binding is missing")
        try:
            document = _load_json(row[0])
            if type(document) is not dict or frozenset(document) != {
                "schema_version",
                "store_instance_id",
                "stream",
            }:
                raise ValueError("binding fields differ")
            instance_id = document["store_instance_id"]
            _binding_payload(self._stream_key, store_instance_id=instance_id)
            return instance_id
        except (KeyError, TypeError, ValueError) as exc:
            raise _error("STORE_BINDING_INVALID", "store binding is malformed") from exc

    def _fingerprint(self, connection: sqlite3.Connection) -> tuple[object, ...]:
        state = connection.execute(
            "SELECT COUNT(*),MAX(state_record_sequence),"
            "COALESCE((SELECT state_record_sha256 FROM monitor_state_records "
            "ORDER BY state_record_sequence DESC LIMIT 1),'') FROM monitor_state_records"
        ).fetchone()
        head = connection.execute(
            "SELECT COUNT(*),MAX(head_record_sequence),"
            "COALESCE((SELECT head_record_sha256 FROM detector_head_records "
            "ORDER BY head_record_sequence DESC LIMIT 1),'') FROM detector_head_records"
        ).fetchone()
        return tuple(state) + tuple(head)

    def _assert_fingerprint(self, connection: sqlite3.Connection) -> None:
        if (
            self._expected_fingerprint is not None
            and self._fingerprint(connection) != self._expected_fingerprint
        ):
            self._poisoned = True
            raise _error("STORE_HEAD_DRIFT", "store changed outside this owner")

    def close(self) -> None:
        with self._mutex:
            if self._closed:
                return
            self._closed = True
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            if self._lock_fd >= 0:
                try:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                finally:
                    os.close(self._lock_fd)
                    self._lock_fd = -1
            if self._lock_inode is not None:
                with _OWNERSHIP_LOCK:
                    _OWNED_LOCK_INODES.discard(self._lock_inode)
                self._lock_inode = None

    def _verify_schema(self, connection: sqlite3.Connection) -> None:
        if connection.execute("PRAGMA user_version").fetchone()[0] != STORE_SCHEMA_VERSION:
            raise _error("STORE_SCHEMA_INVALID", "store schema version differs")
        rows = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        actual = {row["name"]: _normalized_sql(row["sql"]) for row in rows}
        expected: dict[str, str] = {}
        for statement in _SCHEMA_SQL:
            tokens = statement.split()
            name = tokens[2] if tokens[1] == "TABLE" else tokens[2]  # the explicit branch documents both governed outcomes  # noqa: RUF034
            expected[name] = _normalized_sql(statement)
        if actual != expected:
            raise _error("STORE_SCHEMA_INVALID", "store schema inventory differs")

    def _verify_all(
        self,
    ) -> tuple[
        list[MonitorStreamState],
        list[tuple[ResponseProfileDetectorHead, str, str]],
    ]:
        connection = self._guard()
        self._verify_schema(connection)
        if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise _error("STORE_INTEGRITY_INVALID", "SQLite quick_check failed")
        binding_row = connection.execute(
            "SELECT binding_json,binding_sha256 FROM store_binding WHERE singleton=1"
        ).fetchall()
        if len(binding_row) != 1:
            raise _error("STORE_BINDING_INVALID", "store binding is missing")
        binding = _load_json(binding_row[0]["binding_json"])
        if self._store_instance_id is None:
            raise _error("STORE_BINDING_INVALID", "store instance ID is unavailable")
        expected_binding = _binding_payload(
            self._stream_key,
            store_instance_id=self._store_instance_id,
        )
        if binding != expected_binding or not hmac.compare_digest(
            binding_row[0]["binding_sha256"], _digest(STORE_BINDING_HASH_DOMAIN, expected_binding)
        ):
            raise _error("STORE_BINDING_INVALID", "store binding differs")

        head_rows = connection.execute(
            "SELECT * FROM detector_head_records ORDER BY head_record_sequence"
        ).fetchall()
        heads: list[tuple[ResponseProfileDetectorHead, str, str]] = []
        previous: str | None = None
        prior_window_sequence: int | None = None
        prior_persisted_at = None
        head_state_sequence: dict[str, int] = {}
        for expected_sequence, row in enumerate(head_rows):
            if row["head_record_sequence"] != expected_sequence or row["previous_head_record_sha256"] != previous:
                raise _error("STORE_HEAD_CHAIN_INVALID", "detector-head chain order differs")
            document = _load_json(row["head_json"])
            head = response_profile_detector_head_from_document(document)
            if head.stream_key != self._stream_key:
                raise _error("STORE_HEAD_CHAIN_INVALID", "detector head stream differs")
            if prior_window_sequence is not None and head.window_sequence <= prior_window_sequence:
                raise _error("STORE_HEAD_CHAIN_INVALID", "detector head sequence is not increasing")
            persisted_at_utc = _timestamp(
                row["persisted_at_utc"], field="persisted_at_utc"
            )
            persisted_at = parse_rfc3339_utc_instant(persisted_at_utc)
            if prior_persisted_at is not None and persisted_at <= prior_persisted_at:
                raise _error(
                    "STORE_HEAD_CHAIN_INVALID",
                    "detector-head persistence timestamps are not increasing",
                )
            payload = {
                "schema_version": HEAD_RECORD_SCHEMA_VERSION,
                "head_record_sequence": expected_sequence,
                "state_record_sequence": row["state_record_sequence"],
                "store_binding_sha256": binding_row[0]["binding_sha256"],
                "detector_head": document,
                "persisted_at_utc": persisted_at_utc,
                "previous_head_record_sha256": previous,
            }
            record_digest = _digest(HEAD_RECORD_HASH_DOMAIN, payload)
            if not hmac.compare_digest(row["head_record_sha256"], record_digest):
                raise _error("STORE_HEAD_CHAIN_INVALID", "detector-head record digest differs")
            heads.append((head, record_digest, persisted_at_utc))
            head_state_sequence[head.detector_head_sha256] = row["state_record_sequence"]
            previous = record_digest
            prior_window_sequence = head.window_sequence
            prior_persisted_at = persisted_at

        state_rows = connection.execute(
            "SELECT * FROM monitor_state_records ORDER BY state_record_sequence"
        ).fetchall()
        states: list[MonitorStreamState] = []
        state_head_pointers: dict[int, str | None] = {}
        previous = None
        known_head_digests = {head.detector_head_sha256 for head, _, _ in heads}
        for expected_sequence, row in enumerate(state_rows):
            if row["state_record_sequence"] != expected_sequence or row["previous_state_record_sha256"] != previous:
                raise _error("STORE_STATE_CHAIN_INVALID", "monitor-state chain order differs")
            document = _load_json(row["state_json"])
            try:
                state = _state_from_document(document)
            except (TypeError, ValueError) as exc:
                raise _error("STORE_STATE_CHAIN_INVALID", "monitor state is invalid") from exc
            if state.stream_key != self._stream_key:
                raise _error("STORE_STATE_CHAIN_INVALID", "monitor state stream differs")
            head_digest = row["latest_detector_head_sha256"]
            if head_digest is not None and head_digest not in known_head_digests:
                raise _error("STORE_STATE_CHAIN_INVALID", "state points to an unknown detector head")
            payload = {
                "schema_version": STATE_RECORD_SCHEMA_VERSION,
                "state_record_sequence": expected_sequence,
                "monitor_state": document,
                "latest_detector_head_sha256": head_digest,
                "previous_state_record_sha256": previous,
            }
            record_digest = _digest(STATE_RECORD_HASH_DOMAIN, payload)
            if not hmac.compare_digest(row["state_record_sha256"], record_digest):
                raise _error("STORE_STATE_CHAIN_INVALID", "monitor-state record digest differs")
            attached = next(
                (
                    head
                    for head, _, _ in heads
                    if head.detector_head_sha256 == head_digest
                ),
                None,
            )
            states.append(replace(state, latest_detector_head=attached))
            state_head_pointers[expected_sequence] = head_digest
            previous = record_digest
        for head_digest, state_sequence in head_state_sequence.items():
            if state_head_pointers.get(state_sequence) != head_digest:
                raise _error("STORE_ATOMICITY_INVALID", "detector head is not atomically state-bound")
        latest_head_digest = heads[-1][0].detector_head_sha256 if heads else None
        latest_state_digest = state_head_pointers.get(len(states) - 1) if states else None
        if latest_head_digest != latest_state_digest:
            raise _error("STORE_ATOMICITY_INVALID", "latest state/head pointers differ")
        return states, heads

    def load(self, stream_key: MonitorStreamKey) -> MonitorStreamState | None:
        with self._mutex:
            if stream_key != self._stream_key:
                raise _error("STORE_STREAM_MISMATCH", "requested stream differs")
            try:
                connection = self._guard()
                connection.execute("BEGIN")
                self._assert_fingerprint(connection)
                states, _ = self._verify_all()
                connection.execute("COMMIT")
                return states[-1] if states else None
            except ResponseProfileMonitorStoreError:
                self._poisoned = True
                raise
            except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
                self._poisoned = True
                raise _error("STORE_READ_FAILED", "monitor store read failed") from exc

    def save(self, state: MonitorStreamState) -> None:
        with self._mutex:
            if type(state) is not MonitorStreamState or state.stream_key != self._stream_key:
                raise _error("STORE_STATE_INVALID", "monitor state differs from store stream")
            head = state.latest_detector_head
            if head is not None:
                try:
                    head = verify_response_profile_detector_head(head)
                except (TypeError, ValueError) as exc:
                    raise _error("STORE_STATE_INVALID", "latest detector head is invalid") from exc
                if head.stream_key != self._stream_key:
                    raise _error("STORE_STATE_INVALID", "latest detector head stream differs")
            document = _state_document(state)
            state_json = canonical_json_bytes(document).decode("utf-8")
            connection = self._guard()
            committed = False
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._assert_fingerprint(connection)
                states, heads = self._verify_all()
                previous_state_digest = connection.execute(
                    "SELECT state_record_sha256 FROM monitor_state_records ORDER BY state_record_sequence DESC LIMIT 1"
                ).fetchone()
                previous_state_digest = None if previous_state_digest is None else previous_state_digest[0]
                previous_head = heads[-1][0] if heads else None
                if head is None and previous_head is not None:
                    raise _error("STORE_HEAD_REGRESSION", "state cannot discard latest detector head")
                if (
                    head is not None
                    and previous_head is not None
                    and head.detector_head_sha256 != previous_head.detector_head_sha256
                    and head.window_sequence <= previous_head.window_sequence
                ):
                    raise _error("STORE_HEAD_REGRESSION", "detector head sequence did not advance")
                next_state_sequence = len(states)
                head_digest = None if head is None else head.detector_head_sha256
                state_payload = {
                    "schema_version": STATE_RECORD_SCHEMA_VERSION,
                    "state_record_sequence": next_state_sequence,
                    "monitor_state": document,
                    "latest_detector_head_sha256": head_digest,
                    "previous_state_record_sha256": previous_state_digest,
                }
                state_digest = _digest(STATE_RECORD_HASH_DOMAIN, state_payload)
                if states:
                    latest_row = connection.execute(
                        "SELECT state_json,latest_detector_head_sha256 FROM monitor_state_records "
                        "ORDER BY state_record_sequence DESC LIMIT 1"
                    ).fetchone()
                    if (
                        latest_row["state_json"] == state_json
                        and latest_row["latest_detector_head_sha256"] == head_digest
                    ):
                        connection.execute("ROLLBACK")
                        return
                if head is not None and (
                    previous_head is None
                    or head.detector_head_sha256 != previous_head.detector_head_sha256
                ):
                    head_sequence = len(heads)
                    previous_head_record = heads[-1][1] if heads else None
                    persisted_at_utc = _timestamp(
                        self._utc_now(), field="persisted_at_utc"
                    )
                    if heads and parse_rfc3339_utc_instant(
                        persisted_at_utc
                    ) <= parse_rfc3339_utc_instant(heads[-1][2]):
                        raise _error(
                            "STORE_TIMESTAMP_INVALID",
                            "new detector-head timestamp must advance",
                        )
                    head_document = response_profile_detector_head_document(head)
                    head_payload = {
                        "schema_version": HEAD_RECORD_SCHEMA_VERSION,
                        "head_record_sequence": head_sequence,
                        "state_record_sequence": next_state_sequence,
                        "store_binding_sha256": _digest(
                            STORE_BINDING_HASH_DOMAIN,
                            _binding_payload(
                                self._stream_key,
                                store_instance_id=self._store_instance_id,
                            ),
                        ),
                        "detector_head": head_document,
                        "persisted_at_utc": persisted_at_utc,
                        "previous_head_record_sha256": previous_head_record,
                    }
                    head_record_digest = _digest(HEAD_RECORD_HASH_DOMAIN, head_payload)
                    connection.execute(
                        "INSERT INTO detector_head_records VALUES(?,?,?,?,?,?)",
                        (
                            head_sequence,
                            next_state_sequence,
                            canonical_json_bytes(head_document).decode("utf-8"),
                            persisted_at_utc,
                            previous_head_record,
                            head_record_digest,
                        ),
                    )
                connection.execute(
                    "INSERT INTO monitor_state_records VALUES(?,?,?,?,?)",
                    (
                        next_state_sequence,
                        state_json,
                        head_digest,
                        previous_state_digest,
                        state_digest,
                    ),
                )
                connection.execute("COMMIT")
                committed = True
                self._expected_fingerprint = self._fingerprint(connection)
            except ResponseProfileMonitorStoreError as exc:
                if not committed:
                    try:
                        connection.execute("ROLLBACK")
                    except sqlite3.Error:
                        self._poisoned = True
                if exc.code in {
                    "STORE_SCHEMA_INVALID",
                    "STORE_INTEGRITY_INVALID",
                    "STORE_BINDING_INVALID",
                    "STORE_HEAD_CHAIN_INVALID",
                    "STORE_STATE_CHAIN_INVALID",
                    "STORE_ATOMICITY_INVALID",
                    "STORE_PATH_DRIFT",
                }:
                    self._poisoned = True
                raise
            except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
                if not committed:
                    try:
                        connection.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                self._poisoned = True
                raise _error("STORE_WRITE_FAILED", "monitor store write failed") from exc

    def load_verified_latest(
        self, stream_key: MonitorStreamKey
    ) -> VerifiedLatestResponseProfileDetectorHead | None:
        with self._mutex:
            if stream_key != self._stream_key:
                raise _error("STORE_STREAM_MISMATCH", "requested stream differs")
            try:
                connection = self._guard()
                connection.execute("BEGIN")
                self._assert_fingerprint(connection)
                _, heads = self._verify_all()
                result = None
                if heads:
                    head, record_digest, persisted_at_utc = heads[-1]
                    result = VerifiedLatestResponseProfileDetectorHead(
                        _token=_ISSUE_TOKEN,
                        head=head,
                        head_record_sequence=len(heads) - 1,
                        head_record_sha256=record_digest,
                        head_record_persisted_at_utc=persisted_at_utc,
                    )
                connection.execute("COMMIT")
                return result
            except ResponseProfileMonitorStoreError:
                self._poisoned = True
                raise
            except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
                self._poisoned = True
                raise _error("STORE_READ_FAILED", "verified latest read failed") from exc
