"""Prospective append-only durability for physical 50-query shadow attempts.

Each governed attempt commits STARTED before the injected executor may run and
then appends exactly one COMPLETED or FAILED event.  A durable STARTED without
a terminal event is exposed as ORPHANED / EXECUTION_OUTCOME_UNKNOWN and is
never retryable.  Returned trace envelopes use the repository's existing
canonical trace codec; this module introduces no second shadow schema.

This store is prospective only.  It performs no service I/O and never opens,
migrates, or repairs historical V1/V2/V3 evidence.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
import threading
import unicodedata
from dataclasses import dataclass, fields
from enum import StrEnum
from pathlib import Path
from typing import Self

from .artifacts import canonical_json_bytes
from .config import Metric
from .host_window_lineage import (
    CommittedHostObservation,
    verify_committed_host_observation,
)
from .response_profile_evidence import build_canonical_query_identity
from .shadow_artifacts import (
    ShadowTraceArtifactError,
    decode_persisted_shadow_trace_envelope,
    encode_persisted_shadow_trace_envelope,
)
from .shadow_event_types import MonitorStreamKey
from .shadow_window import (
    TRACE_QUERY_COUNT,
    WINDOW_QUERY_COUNT,
    PersistedShadowTraceEnvelope,
    validate_persisted_shadow_trace_envelope,
)

__all__ = [
    "SQLiteShadowAttemptStore",
    "ShadowAttemptIdentity",
    "ShadowAttemptPermit",
    "ShadowAttemptRecord",
    "ShadowAttemptStatus",
    "ShadowAttemptStoreError",
    "build_shadow_attempt_identity",
    "expected_shadow_trace_id",
]


_DB_VERSION = 1
_IDENTITY_SCHEMA = "shadow-physical-attempt-identity-v1"
_EVENT_SCHEMA = "shadow-physical-attempt-event-v1"
_BINDING_SCHEMA = "shadow-physical-attempt-store-v1"
_IDENTITY_DOMAIN = b"VD::SHADOW_PHYSICAL_ATTEMPT_IDENTITY::V1\x00"
_EVENT_DOMAIN = b"VD::SHADOW_PHYSICAL_ATTEMPT_EVENT::V1\x00"
_BINDING_DOMAIN = b"VD::SHADOW_PHYSICAL_ATTEMPT_STORE::V1\x00"
_TRACE_DOCUMENT_DOMAIN = b"VD::SHADOW_PHYSICAL_ATTEMPT_TRACE::V1\x00"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")
_OWNERSHIP_LOCK = threading.Lock()
_OWNED_LOCK_INODES: set[tuple[int, int]] = set()


class ShadowAttemptStoreError(RuntimeError):
    """Fail-closed shadow-attempt error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> ShadowAttemptStoreError:
    return ShadowAttemptStoreError(code, message)


class ShadowAttemptStatus(StrEnum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ORPHANED = "ORPHANED"


@dataclass(frozen=True, slots=True, init=False)
class ShadowAttemptIdentity:
    schema_version: str
    stream_key: MonitorStreamKey
    source_revision: str
    environment_manifest_sha256: str
    window_sequence: int
    trace_sequence_index: int
    source_sequences: tuple[int, ...]
    source_sha256: tuple[str, ...]
    query_id_sha256: tuple[str, ...]
    attempt_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("shadow-attempt identities are builder-issued")


def _make_identity(**values: object) -> ShadowAttemptIdentity:
    result = object.__new__(ShadowAttemptIdentity)
    for name, value in values.items():
        object.__setattr__(result, name, value)
    return result


@dataclass(frozen=True, slots=True)
class ShadowAttemptRecord:
    identity: ShadowAttemptIdentity
    status: ShadowAttemptStatus
    started_at_utc: str
    terminal_at_utc: str | None
    envelope: PersistedShadowTraceEnvelope | None
    reason_codes: tuple[str, ...]
    failure_code: str | None
    error_type: str | None


_PERMIT_TOKEN = object()


@dataclass(slots=True, init=False, repr=False)
class ShadowAttemptPermit:
    """Ephemeral one-shot authority to terminalize one live STARTED attempt.

    The value is deliberately process- and store-instance-bound.  It is API
    discipline, not cryptographic authority, and is never persisted or
    reconstructable after restart.
    """

    _token: object
    _store_token: object
    _pid: int
    _attempt_sha256: str
    _consumed: bool

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("shadow attempt permits are store-issued")


def _text(value: object, *, code: str) -> str:
    if type(value) is not str:
        raise _error(code)
    normalized = unicodedata.normalize("NFC", value)
    if not normalized or normalized != value:
        raise _error(code)
    return normalized


def _sha(value: object, *, code: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _error(code)
    return value


def _timestamp(value: object, *, code: str) -> str:
    if type(value) is not str or _RFC3339.fullmatch(value) is None:
        raise _error(code)
    return value


def _digest(domain: bytes, payload: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(payload)).hexdigest()


def _trace_document_digest(value: bytes) -> str:
    return hashlib.sha256(_TRACE_DOCUMENT_DOMAIN + value).hexdigest()


def _stream_document(value: MonitorStreamKey) -> dict[str, object]:
    if type(value) is not MonitorStreamKey:
        raise _error("SHADOW_ATTEMPT_STREAM_INVALID")
    try:
        rebuilt = MonitorStreamKey(
            stream_id=value.stream_id,
            metric=value.metric,
            threshold_stratum=value.threshold_stratum,
            configuration_identity=value.configuration_identity,
            data_identity=value.data_identity,
            flat_binding_id=value.flat_binding_id,
            hnsw_binding_id=value.hnsw_binding_id,
        )
    except (TypeError, ValueError) as exc:
        raise _error("SHADOW_ATTEMPT_STREAM_INVALID") from exc
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("SHADOW_ATTEMPT_STREAM_INVALID")
    return {
        "stream_id": value.stream_id,
        "metric": value.metric.value,
        "threshold_stratum": value.threshold_stratum,
        "configuration_identity": value.configuration_identity,
        "data_identity": value.data_identity,
        "flat_binding_id": value.flat_binding_id,
        "hnsw_binding_id": value.hnsw_binding_id,
    }


def _stream_from_document(value: object) -> MonitorStreamKey:
    required = {
        "stream_id", "metric", "threshold_stratum", "configuration_identity",
        "data_identity", "flat_binding_id", "hnsw_binding_id",
    }
    if type(value) is not dict or set(value) != required:
        raise _error("SHADOW_ATTEMPT_STREAM_INVALID")
    try:
        return MonitorStreamKey(
            stream_id=value["stream_id"], metric=Metric(value["metric"]),
            threshold_stratum=value["threshold_stratum"],
            configuration_identity=value["configuration_identity"],
            data_identity=value["data_identity"],
            flat_binding_id=value["flat_binding_id"],
            hnsw_binding_id=value["hnsw_binding_id"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("SHADOW_ATTEMPT_STREAM_INVALID") from exc


def _identity_payload(
    *,
    stream_key: MonitorStreamKey,
    source_revision: str,
    environment_manifest_sha256: str,
    window_sequence: int,
    trace_sequence_index: int,
    source_sequences: tuple[int, ...],
    source_sha256: tuple[str, ...],
    query_id_sha256: tuple[str, ...],
) -> dict[str, object]:
    return {
        "schema_version": _IDENTITY_SCHEMA,
        "stream": _stream_document(stream_key),
        "source_revision": _text(
            source_revision, code="SHADOW_ATTEMPT_SOURCE_REVISION_INVALID"
        ),
        "environment_manifest_sha256": _sha(
            environment_manifest_sha256,
            code="SHADOW_ATTEMPT_ENVIRONMENT_INVALID",
        ),
        "window_sequence": window_sequence,
        "trace_sequence_index": trace_sequence_index,
        "source_sequences": list(source_sequences),
        "source_sha256": list(source_sha256),
        "query_id_sha256": list(query_id_sha256),
    }


def _identity_from_payload(value: object) -> ShadowAttemptIdentity:
    required = {
        "schema_version", "stream", "source_revision",
        "environment_manifest_sha256", "window_sequence",
        "trace_sequence_index", "source_sequences", "source_sha256",
        "query_id_sha256",
    }
    if type(value) is not dict or set(value) != required:
        raise _error("SHADOW_ATTEMPT_IDENTITY_INVALID")
    if value["schema_version"] != _IDENTITY_SCHEMA:
        raise _error("SHADOW_ATTEMPT_IDENTITY_INVALID")
    window = value["window_sequence"]
    trace = value["trace_sequence_index"]
    sequences = value["source_sequences"]
    sources = value["source_sha256"]
    queries = value["query_id_sha256"]
    if (
        type(window) is not int
        or window < 0
        or type(trace) is not int
        or not 0 <= trace < WINDOW_QUERY_COUNT // TRACE_QUERY_COUNT
        or type(sequences) is not list
        or type(sources) is not list
        or type(queries) is not list
        or len(sequences) != TRACE_QUERY_COUNT
        or len(sources) != TRACE_QUERY_COUNT
        or len(queries) != TRACE_QUERY_COUNT
    ):
        raise _error("SHADOW_ATTEMPT_IDENTITY_INVALID")
    expected_start = window * WINDOW_QUERY_COUNT + trace * TRACE_QUERY_COUNT
    if any(type(item) is not int for item in sequences) or sequences != list(
        range(expected_start, expected_start + TRACE_QUERY_COUNT)
    ):
        raise _error("SHADOW_ATTEMPT_MEMBERSHIP_INVALID")
    source_tuple = tuple(_sha(item, code="SHADOW_ATTEMPT_MEMBERSHIP_INVALID") for item in sources)
    query_tuple = tuple(_sha(item, code="SHADOW_ATTEMPT_MEMBERSHIP_INVALID") for item in queries)
    if len(set(source_tuple)) != TRACE_QUERY_COUNT or len(set(query_tuple)) != TRACE_QUERY_COUNT:
        raise _error("SHADOW_ATTEMPT_MEMBERSHIP_INVALID")
    stream = _stream_from_document(value["stream"])
    revision = _text(
        value["source_revision"], code="SHADOW_ATTEMPT_SOURCE_REVISION_INVALID"
    )
    environment = _sha(
        value["environment_manifest_sha256"],
        code="SHADOW_ATTEMPT_ENVIRONMENT_INVALID",
    )
    canonical = _identity_payload(
        stream_key=stream,
        source_revision=revision,
        environment_manifest_sha256=environment,
        window_sequence=window,
        trace_sequence_index=trace,
        source_sequences=tuple(sequences),
        source_sha256=source_tuple,
        query_id_sha256=query_tuple,
    )
    if canonical_json_bytes(canonical) != canonical_json_bytes(value):
        raise _error("SHADOW_ATTEMPT_IDENTITY_INVALID")
    return _make_identity(
        schema_version=_IDENTITY_SCHEMA,
        stream_key=stream,
        source_revision=revision,
        environment_manifest_sha256=environment,
        window_sequence=window,
        trace_sequence_index=trace,
        source_sequences=tuple(sequences),
        source_sha256=source_tuple,
        query_id_sha256=query_tuple,
        attempt_sha256=_digest(_IDENTITY_DOMAIN, canonical),
    )


def _identity_document(value: ShadowAttemptIdentity) -> dict[str, object]:
    if type(value) is not ShadowAttemptIdentity:
        raise _error("SHADOW_ATTEMPT_IDENTITY_INVALID")
    payload = _identity_payload(
        stream_key=value.stream_key,
        source_revision=value.source_revision,
        environment_manifest_sha256=value.environment_manifest_sha256,
        window_sequence=value.window_sequence,
        trace_sequence_index=value.trace_sequence_index,
        source_sequences=value.source_sequences,
        source_sha256=value.source_sha256,
        query_id_sha256=value.query_id_sha256,
    )
    rebuilt = _identity_from_payload(payload)
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("SHADOW_ATTEMPT_IDENTITY_INVALID")
    return payload


def build_shadow_attempt_identity(
    sources: tuple[CommittedHostObservation, ...], *, trace_sequence_index: int
) -> ShadowAttemptIdentity:
    """Bind one exact ordered 50-source physical capture identity."""

    if type(sources) is not tuple or len(sources) != TRACE_QUERY_COUNT:
        raise _error("SHADOW_ATTEMPT_MEMBERSHIP_INVALID")
    verified = tuple(verify_committed_host_observation(item) for item in sources)
    first = verified[0]
    if type(trace_sequence_index) is not int:
        raise _error("SHADOW_ATTEMPT_TRACE_INDEX_INVALID")
    payload = _identity_payload(
        stream_key=first.stream_key,
        source_revision=first.source_revision,
        environment_manifest_sha256=first.environment_manifest_sha256,
        window_sequence=first.window_sequence,
        trace_sequence_index=trace_sequence_index,
        source_sequences=tuple(item.source_sequence for item in verified),
        source_sha256=tuple(item.source_sha256 for item in verified),
        query_id_sha256=tuple(item.query_id_sha256 for item in verified),
    )
    identity = _identity_from_payload(payload)
    if any(
        item.stream_key != identity.stream_key
        or item.source_revision != identity.source_revision
        or item.environment_manifest_sha256 != identity.environment_manifest_sha256
        or item.window_sequence != identity.window_sequence
        for item in verified
    ):
        raise _error("SHADOW_ATTEMPT_BINDING_MISMATCH")
    return identity


def expected_shadow_trace_id(
    *, window_sequence: int, trace_sequence_index: int
) -> str:
    """The one canonical trace id for a window/trace slot.

    Derived from the attempt slot alone, never from trace contents, so it is
    usable as a cross-binding check on any envelope offered as that slot's
    evidence.  `v2_shadow_worker` builds envelope ids through this same helper,
    so the format has exactly one definition.
    """

    return f"v2-window-{window_sequence}-trace-{trace_sequence_index}"


def _verify_terminal_envelope_binding(
    identity: ShadowAttemptIdentity, envelope: PersistedShadowTraceEnvelope
) -> None:
    """Prove any attached envelope is evidence for *this* attempt slot.

    A FAILED attempt may legitimately carry a partial, membership-mismatched,
    or otherwise-invalid trace as forensic evidence, so its *contents* cannot
    be required to agree with the attempt -- that disagreement is often the
    very reason for the failure.  Its *slot identity* must still agree.
    Without this, an envelope captured for a different window or trace index
    is attachable to this attempt as its failure evidence, and the forensic
    record would then describe physical work that never belonged to it.
    """

    if (
        type(envelope) is not PersistedShadowTraceEnvelope
        or envelope.trace_id
        != expected_shadow_trace_id(
            window_sequence=identity.window_sequence,
            trace_sequence_index=identity.trace_sequence_index,
        )
        or envelope.sequence_index != identity.trace_sequence_index
        or envelope.declared_observation_count != TRACE_QUERY_COUNT
    ):
        raise _error("SHADOW_ATTEMPT_TRACE_BINDING_INVALID")


def _verify_completed_envelope_binding(
    identity: ShadowAttemptIdentity,
    envelope: PersistedShadowTraceEnvelope,
    *,
    terminal_at_utc: str,
) -> None:
    """Prove a COMPLETED trace is the exact evidence for its attempt."""

    trace = envelope.trace
    if (
        type(envelope) is not PersistedShadowTraceEnvelope
        or trace is None
        or envelope.trace_id
        != expected_shadow_trace_id(
            window_sequence=identity.window_sequence,
            trace_sequence_index=identity.trace_sequence_index,
        )
        or envelope.sequence_index != identity.trace_sequence_index
        or envelope.declared_observation_count != TRACE_QUERY_COUNT
        or envelope.captured_at_utc != terminal_at_utc
        or validate_persisted_shadow_trace_envelope(envelope)
        or trace.metric is not identity.stream_key.metric
        or trace.threshold_stratum != identity.stream_key.threshold_stratum
        or trace.configuration_identity != identity.stream_key.configuration_identity
        or trace.data_identity != identity.stream_key.data_identity
        or trace.flat_identity.expected_binding_id != identity.stream_key.flat_binding_id
        or trace.hnsw_identity.expected_binding_id != identity.stream_key.hnsw_binding_id
    ):
        raise _error("SHADOW_ATTEMPT_TRACE_BINDING_INVALID")
    try:
        query_digests = tuple(
            build_canonical_query_identity(query.query_id).query_id_sha256
            for query in trace.queries
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("SHADOW_ATTEMPT_TRACE_BINDING_INVALID") from exc
    if query_digests != identity.query_id_sha256:
        raise _error("SHADOW_ATTEMPT_TRACE_BINDING_INVALID")


_SCHEMA_SQL = (
    "CREATE TABLE attempt_store_binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), binding_json BLOB NOT NULL, binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256)=64)) STRICT",
    "CREATE TABLE attempt_events (event_sequence INTEGER PRIMARY KEY CHECK(event_sequence>=0), attempt_sha256 TEXT NOT NULL CHECK(length(attempt_sha256)=64), window_sequence INTEGER NOT NULL CHECK(window_sequence>=0), trace_sequence_index INTEGER NOT NULL CHECK(trace_sequence_index>=0 AND trace_sequence_index<4), event_kind TEXT NOT NULL CHECK(event_kind IN ('STARTED','COMPLETED','FAILED')), event_json BLOB NOT NULL, trace_envelope_json BLOB, previous_event_sha256 TEXT, event_sha256 TEXT NOT NULL UNIQUE CHECK(length(event_sha256)=64), UNIQUE(attempt_sha256,event_kind)) STRICT",
    "CREATE UNIQUE INDEX attempt_slot_started ON attempt_events(window_sequence,trace_sequence_index) WHERE event_kind='STARTED'",
    "CREATE TRIGGER attempt_store_binding_no_update BEFORE UPDATE ON attempt_store_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER attempt_store_binding_no_delete BEFORE DELETE ON attempt_store_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER attempt_events_no_update BEFORE UPDATE ON attempt_events BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER attempt_events_no_delete BEFORE DELETE ON attempt_events BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER attempt_events_transition BEFORE INSERT ON attempt_events BEGIN SELECT CASE WHEN NEW.event_kind='STARTED' AND EXISTS(SELECT 1 FROM attempt_events WHERE attempt_sha256=NEW.attempt_sha256) THEN RAISE(ABORT,'illegal-transition') WHEN NEW.event_kind!='STARTED' AND NOT EXISTS(SELECT 1 FROM attempt_events WHERE attempt_sha256=NEW.attempt_sha256 AND event_kind='STARTED') THEN RAISE(ABORT,'illegal-transition') WHEN NEW.event_kind!='STARTED' AND EXISTS(SELECT 1 FROM attempt_events WHERE attempt_sha256=NEW.attempt_sha256 AND event_kind IN ('COMPLETED','FAILED')) THEN RAISE(ABORT,'illegal-transition') END; END",
)


def _normalize_sql(value: str) -> str:
    return " ".join(value.strip().removesuffix(";").split())


def _schema_object_name(statement: str) -> str:
    tokens = statement.split()
    return tokens[3] if tokens[:3] == ["CREATE", "UNIQUE", "INDEX"] else tokens[2]


def _no_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


class SQLiteShadowAttemptStore:
    """Exclusive-writer, append-only physical shadow-attempt journal."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        stream_key: MonitorStreamKey,
        source_revision: str,
        environment_manifest_sha256: str,
    ) -> None:
        self.path = Path(path)
        self.stream_key = stream_key
        self.source_revision = _text(
            source_revision, code="SHADOW_ATTEMPT_SOURCE_REVISION_INVALID"
        )
        self.environment_manifest_sha256 = _sha(
            environment_manifest_sha256,
            code="SHADOW_ATTEMPT_ENVIRONMENT_INVALID",
        )
        self._binding = {
            "schema_version": _BINDING_SCHEMA,
            "stream": _stream_document(stream_key),
            "source_revision": self.source_revision,
            "environment_manifest_sha256": self.environment_manifest_sha256,
        }
        self._mutex = threading.RLock()
        self._pid = os.getpid()
        self._closed = False
        self._lock_handle = None
        self._lock_inode: tuple[int, int] | None = None
        self._store_token = object()
        self._active_started: dict[str, ShadowAttemptPermit] = {}
        # ADR-015 recovery: `_open` registers process-local inode ownership and
        # an exclusive flock before the remaining steps (database creation,
        # path re-verification, `sqlite3.connect`) can still fail. Without this
        # guard such a failure propagates while the ownership entry, flock, and
        # lock descriptor stay held, and the same process can then never reopen
        # the store -- a fail-closed availability trap, not a durability
        # defect. `close()` is idempotent, so the inner `close()` calls in
        # `_open` remain correct and this is a pure backstop.
        try:
            self._open()
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _open(self) -> None:
        parent = self.path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent_info = parent.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(parent_info.st_mode)
            or parent_info.st_uid != os.geteuid()
            or stat.S_IMODE(parent_info.st_mode) & 0o022
        ):
            raise _error("SHADOW_ATTEMPT_PARENT_UNSAFE")
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        lock_fd = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        self._lock_handle = os.fdopen(lock_fd, "a+b")
        lock_info = os.fstat(lock_fd)
        if (
            not stat.S_ISREG(lock_info.st_mode)
            or lock_info.st_nlink != 1
            or lock_info.st_uid != os.geteuid()
            or stat.S_IMODE(lock_info.st_mode) != 0o600
        ):
            self.close()
            raise _error("SHADOW_ATTEMPT_PATH_UNSAFE")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.close()
            raise _error("SHADOW_ATTEMPT_STORE_BUSY") from exc
        inode = (lock_info.st_dev, lock_info.st_ino)
        with _OWNERSHIP_LOCK:
            if inode in _OWNED_LOCK_INODES:
                self.close()
                raise _error("SHADOW_ATTEMPT_STORE_BUSY")
            _OWNED_LOCK_INODES.add(inode)
        self._lock_inode = inode
        created = not self.path.exists()
        if created:
            descriptor = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            os.close(descriptor)
        self._verify_path()
        self._connection = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
        try:
            self._connection.execute("PRAGMA foreign_keys=ON")
            if created:
                self._connection.execute("PRAGMA journal_mode=DELETE")
                self._connection.execute("PRAGMA synchronous=FULL")
                self._connection.execute("PRAGMA trusted_schema=OFF")
                self._connection.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA_SQL:
                    self._connection.execute(statement)
                self._connection.execute(f"PRAGMA user_version={_DB_VERSION}")
                self._connection.execute(
                    "INSERT INTO attempt_store_binding VALUES(1,?,?)",
                    (
                        canonical_json_bytes(self._binding),
                        _digest(_BINDING_DOMAIN, self._binding),
                    ),
                )
                self._connection.execute("COMMIT")
            else:
                if str(self._connection.execute("PRAGMA journal_mode").fetchone()[0]).lower() != "delete":
                    raise _error("SHADOW_ATTEMPT_SCHEMA_INVALID")
                self._connection.execute("PRAGMA synchronous=FULL")
                self._connection.execute("PRAGMA trusted_schema=OFF")
            self._verify_all()
        except Exception:
            self.close()
            raise

    def _verify_path(self) -> None:
        info = os.lstat(self.path)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise _error("SHADOW_ATTEMPT_PATH_UNSAFE")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        connection = getattr(self, "_connection", None)
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                pass
        owner_process = os.getpid() == self._pid
        if self._lock_handle is not None and not self._lock_handle.closed:
            try:
                # A forked child shares the parent's open-file description.
                # Explicit LOCK_UN here would release the parent's lock too.
                if owner_process:
                    fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._lock_handle.close()
        if owner_process and self._lock_inode is not None:
            with _OWNERSHIP_LOCK:
                _OWNED_LOCK_INODES.discard(self._lock_inode)
            self._lock_inode = None
        for permit in self._active_started.values():
            permit._consumed = True
        self._active_started.clear()

    def _new_permit(self, identity: ShadowAttemptIdentity) -> ShadowAttemptPermit:
        permit = object.__new__(ShadowAttemptPermit)
        permit._token = _PERMIT_TOKEN
        permit._store_token = self._store_token
        permit._pid = self._pid
        permit._attempt_sha256 = identity.attempt_sha256
        permit._consumed = False
        return permit

    def _consume_permit(
        self, permit: object, identity: ShadowAttemptIdentity
    ) -> ShadowAttemptPermit:
        if self._active_started.get(identity.attempt_sha256) is None:
            persisted = next(
                (
                    item
                    for item in self._verify_all()
                    if item.identity.attempt_sha256 == identity.attempt_sha256
                ),
                None,
            )
            if persisted is not None and persisted.status in {
                ShadowAttemptStatus.STARTED,
                ShadowAttemptStatus.ORPHANED,
            }:
                raise _error("SHADOW_ATTEMPT_ORPHANED")
        if (
            type(permit) is not ShadowAttemptPermit
            or permit._token is not _PERMIT_TOKEN
            or permit._store_token is not self._store_token
            or permit._pid != os.getpid()
            or permit._attempt_sha256 != identity.attempt_sha256
            or permit._consumed
            or self._active_started.get(identity.attempt_sha256) is not permit
        ):
            raise _error("SHADOW_ATTEMPT_PERMIT_INVALID")
        permit._consumed = True
        del self._active_started[identity.attempt_sha256]
        return permit

    def _require_live(self) -> None:
        if self._closed:
            raise _error("SHADOW_ATTEMPT_STORE_CLOSED")
        if os.getpid() != self._pid:
            raise _error("SHADOW_ATTEMPT_STORE_FORKED")
        self._verify_path()

    def _verify_schema(self) -> None:
        if self._connection.execute("PRAGMA user_version").fetchone()[0] != _DB_VERSION:
            raise _error("SHADOW_ATTEMPT_SCHEMA_INVALID")
        actual = {
            row[0]: _normalize_sql(row[1])
            for row in self._connection.execute(
                "SELECT name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        expected = {
            _schema_object_name(statement): _normalize_sql(statement)
            for statement in _SCHEMA_SQL
        }
        if actual != expected:
            raise _error("SHADOW_ATTEMPT_SCHEMA_INVALID")
        row = self._connection.execute(
            "SELECT binding_json,binding_sha256 FROM attempt_store_binding WHERE singleton=1"
        ).fetchone()
        if (
            row is None
            or bytes(row[0]) != canonical_json_bytes(self._binding)
            or row[1] != _digest(_BINDING_DOMAIN, self._binding)
        ):
            raise _error("SHADOW_ATTEMPT_BINDING_MISMATCH")

    def _event_documents(self) -> tuple[tuple[dict[str, object], bytes | None], ...]:
        documents: list[tuple[dict[str, object], bytes | None]] = []
        previous = None
        for expected, row in enumerate(
            self._connection.execute(
                "SELECT event_sequence,attempt_sha256,window_sequence,trace_sequence_index,event_kind,event_json,trace_envelope_json,previous_event_sha256,event_sha256 FROM attempt_events ORDER BY event_sequence"
            )
        ):
            try:
                document = json.loads(
                    bytes(row[5]).decode("utf-8"),
                    object_pairs_hook=_no_duplicate_pairs,
                )
            except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
                raise _error("SHADOW_ATTEMPT_HISTORY_INVALID") from exc
            if (
                type(document) is not dict
                or row[0] != expected
                or row[7] != previous
                or canonical_json_bytes(document) != bytes(row[5])
                or document.get("event_sequence") != expected
                or document.get("previous_event_sha256") != previous
                or row[8] != _digest(_EVENT_DOMAIN, document)
            ):
                raise _error("SHADOW_ATTEMPT_HISTORY_INVALID")
            identity = _identity_from_payload(document.get("attempt_identity"))
            if (
                row[1] != identity.attempt_sha256
                or document.get("attempt_sha256") != identity.attempt_sha256
                or row[2] != identity.window_sequence
                or row[3] != identity.trace_sequence_index
                or row[4] != document.get("event_kind")
            ):
                raise _error("SHADOW_ATTEMPT_HISTORY_INVALID")
            trace_bytes = None if row[6] is None else bytes(row[6])
            expected_trace_digest = document.get("trace_envelope_sha256")
            if trace_bytes is None:
                if expected_trace_digest is not None:
                    raise _error("SHADOW_ATTEMPT_HISTORY_INVALID")
            else:
                if expected_trace_digest != _trace_document_digest(trace_bytes):
                    raise _error("SHADOW_ATTEMPT_TRACE_INVALID")
                try:
                    decode_persisted_shadow_trace_envelope(trace_bytes)
                except ShadowTraceArtifactError as exc:
                    raise _error("SHADOW_ATTEMPT_TRACE_INVALID") from exc
            previous = row[8]
            documents.append((document, trace_bytes))
        return tuple(documents)

    def _records(self) -> tuple[ShadowAttemptRecord, ...]:
        required = {
            "schema_version", "event_sequence", "attempt_identity",
            "attempt_sha256", "event_kind", "recorded_at_utc",
            "trace_envelope_sha256", "reason_codes", "failure_code",
            "error_type", "previous_event_sha256",
        }
        states: dict[str, ShadowAttemptRecord] = {}
        order: list[str] = []
        for document, trace_bytes in self._event_documents():
            if set(document) != required or document["schema_version"] != _EVENT_SCHEMA:
                raise _error("SHADOW_ATTEMPT_HISTORY_INVALID")
            identity = _identity_from_payload(document["attempt_identity"])
            try:
                kind = ShadowAttemptStatus(document["event_kind"])
            except (TypeError, ValueError) as exc:
                raise _error("SHADOW_ATTEMPT_HISTORY_INVALID") from exc
            if kind is ShadowAttemptStatus.ORPHANED:
                raise _error("SHADOW_ATTEMPT_HISTORY_INVALID")
            recorded = _timestamp(
                document["recorded_at_utc"], code="SHADOW_ATTEMPT_HISTORY_INVALID"
            )
            reasons = document["reason_codes"]
            if type(reasons) is not list:
                raise _error("SHADOW_ATTEMPT_HISTORY_INVALID")
            reason_tuple = tuple(
                _text(item, code="SHADOW_ATTEMPT_HISTORY_INVALID") for item in reasons
            )
            failure_code = document["failure_code"]
            error_type = document["error_type"]
            if failure_code is not None:
                failure_code = _text(failure_code, code="SHADOW_ATTEMPT_HISTORY_INVALID")
            if error_type is not None:
                error_type = _text(error_type, code="SHADOW_ATTEMPT_HISTORY_INVALID")
            envelope = (
                None
                if trace_bytes is None
                else decode_persisted_shadow_trace_envelope(trace_bytes)
            )
            existing = states.get(identity.attempt_sha256)
            if kind is ShadowAttemptStatus.STARTED:
                if (
                    existing is not None
                    or envelope is not None
                    or reason_tuple
                    or failure_code is not None
                    or error_type is not None
                ):
                    raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
                states[identity.attempt_sha256] = ShadowAttemptRecord(
                    identity, kind, recorded, None, None, (), None, None
                )
                order.append(identity.attempt_sha256)
                continue
            if existing is None or existing.status is not ShadowAttemptStatus.STARTED:
                raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
            if kind is ShadowAttemptStatus.COMPLETED:
                if envelope is None or envelope.trace is None or not envelope.trace.complete:
                    raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
                if failure_code is not None or error_type is not None:
                    raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
                _verify_completed_envelope_binding(
                    identity, envelope, terminal_at_utc=recorded
                )
            elif kind is ShadowAttemptStatus.FAILED:
                if failure_code is None:
                    raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
                if envelope is not None:
                    # FINDING-007 on the read path too, so a cross-bound
                    # envelope written by any other means is still refused
                    # rather than replayed as this attempt's evidence.
                    _verify_terminal_envelope_binding(identity, envelope)
            if envelope is not None and (
                envelope.sequence_index != identity.trace_sequence_index
                or envelope.declared_observation_count != TRACE_QUERY_COUNT
                or envelope.trace is None
                or tuple(envelope.trace.reason_codes) != reason_tuple
            ):
                raise _error("SHADOW_ATTEMPT_TRACE_INVALID")
            states[identity.attempt_sha256] = ShadowAttemptRecord(
                identity, kind, existing.started_at_utc, recorded, envelope,
                reason_tuple, failure_code, error_type,
            )
        return tuple(states[item] for item in order)

    def _verify_all(self) -> tuple[ShadowAttemptRecord, ...]:
        self._require_live()
        self._verify_schema()
        records = self._records()
        slots: set[tuple[int, int]] = set()
        for record in records:
            if (
                record.identity.stream_key != self.stream_key
                or record.identity.source_revision != self.source_revision
                or record.identity.environment_manifest_sha256
                != self.environment_manifest_sha256
            ):
                raise _error("SHADOW_ATTEMPT_BINDING_MISMATCH")
            slot = (
                record.identity.window_sequence,
                record.identity.trace_sequence_index,
            )
            if slot in slots:
                raise _error("SHADOW_ATTEMPT_SLOT_CONFLICT")
            slots.add(slot)
        return records

    def _event_payload(
        self,
        *,
        event_sequence: int,
        identity: ShadowAttemptIdentity,
        kind: ShadowAttemptStatus,
        recorded_at_utc: str,
        trace_bytes: bytes | None,
        reason_codes: tuple[str, ...],
        failure_code: str | None,
        error_type: str | None,
        previous_event_sha256: str | None,
    ) -> dict[str, object]:
        return {
            "schema_version": _EVENT_SCHEMA,
            "event_sequence": event_sequence,
            "attempt_identity": _identity_document(identity),
            "attempt_sha256": identity.attempt_sha256,
            "event_kind": kind.value,
            "recorded_at_utc": _timestamp(
                recorded_at_utc, code="SHADOW_ATTEMPT_TIMESTAMP_INVALID"
            ),
            "trace_envelope_sha256": (
                None if trace_bytes is None else _trace_document_digest(trace_bytes)
            ),
            "reason_codes": [
                _text(item, code="SHADOW_ATTEMPT_REASON_INVALID")
                for item in reason_codes
            ],
            "failure_code": (
                None
                if failure_code is None
                else _text(failure_code, code="SHADOW_ATTEMPT_REASON_INVALID")
            ),
            "error_type": (
                None
                if error_type is None
                else _text(error_type, code="SHADOW_ATTEMPT_REASON_INVALID")
            ),
            "previous_event_sha256": previous_event_sha256,
        }

    def _append(
        self,
        *,
        identity: ShadowAttemptIdentity,
        kind: ShadowAttemptStatus,
        recorded_at_utc: str,
        envelope: PersistedShadowTraceEnvelope | None = None,
        reason_codes: tuple[str, ...] = (),
        failure_code: str | None = None,
        error_type: str | None = None,
    ) -> ShadowAttemptRecord:
        with self._mutex:
            if type(reason_codes) is not tuple:
                raise _error("SHADOW_ATTEMPT_REASON_INVALID")
            if kind is ShadowAttemptStatus.STARTED and (
                envelope is not None
                or reason_codes
                or failure_code is not None
                or error_type is not None
            ):
                raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
            if kind is ShadowAttemptStatus.COMPLETED and (
                type(envelope) is not PersistedShadowTraceEnvelope
                or envelope.trace is None
                or not envelope.trace.complete
                or failure_code is not None
                or error_type is not None
            ):
                raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
            if kind is ShadowAttemptStatus.FAILED and failure_code is None:
                raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
            if envelope is not None and (
                envelope.sequence_index != identity.trace_sequence_index
                or envelope.declared_observation_count != TRACE_QUERY_COUNT
                or envelope.trace is None
                or tuple(envelope.trace.reason_codes) != reason_codes
            ):
                raise _error("SHADOW_ATTEMPT_TRACE_INVALID")
            canonical_identity = _identity_from_payload(_identity_document(identity))
            if kind is ShadowAttemptStatus.COMPLETED:
                # Explicit rather than `assert`: the COMPLETED guard above
                # already establishes this, but an assertion would vanish
                # under `python -O` and leave the narrowing unproven.
                if type(envelope) is not PersistedShadowTraceEnvelope:
                    raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
                _verify_completed_envelope_binding(
                    canonical_identity,
                    envelope,
                    terminal_at_utc=recorded_at_utc,
                )
            elif envelope is not None:
                # FINDING-007: a FAILED attempt's forensic envelope must still
                # belong to this exact attempt slot.
                _verify_terminal_envelope_binding(canonical_identity, envelope)
            if (
                canonical_identity.stream_key != self.stream_key
                or canonical_identity.source_revision != self.source_revision
                or canonical_identity.environment_manifest_sha256
                != self.environment_manifest_sha256
            ):
                raise _error("SHADOW_ATTEMPT_BINDING_MISMATCH")
            records = self._verify_all()
            prior = next(
                (
                    item
                    for item in records
                    if item.identity.attempt_sha256 == identity.attempt_sha256
                ),
                None,
            )
            slot = next(
                (
                    item
                    for item in records
                    if item.identity.window_sequence == identity.window_sequence
                    and item.identity.trace_sequence_index
                    == identity.trace_sequence_index
                ),
                None,
            )
            if slot is not None and slot.identity.attempt_sha256 != identity.attempt_sha256:
                raise _error("SHADOW_ATTEMPT_BINDING_MISMATCH")
            if kind is ShadowAttemptStatus.STARTED:
                if prior is not None:
                    raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
            elif (
                kind not in {ShadowAttemptStatus.COMPLETED, ShadowAttemptStatus.FAILED}
                or prior is None
                or prior.status is not ShadowAttemptStatus.STARTED
            ):
                raise _error("SHADOW_ATTEMPT_TRANSITION_INVALID")
            trace_bytes = (
                None
                if envelope is None
                else encode_persisted_shadow_trace_envelope(envelope)
            )
            event_sequence = len(self._event_documents())
            previous = None
            if event_sequence:
                previous = self._connection.execute(
                    "SELECT event_sha256 FROM attempt_events WHERE event_sequence=?",
                    (event_sequence - 1,),
                ).fetchone()[0]
            payload = self._event_payload(
                event_sequence=event_sequence,
                identity=canonical_identity,
                kind=kind,
                recorded_at_utc=recorded_at_utc,
                trace_bytes=trace_bytes,
                reason_codes=reason_codes,
                failure_code=failure_code,
                error_type=error_type,
                previous_event_sha256=previous,
            )
            digest = _digest(_EVENT_DOMAIN, payload)
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                if self._verify_all() != records:
                    raise _error("SHADOW_ATTEMPT_HEAD_DRIFT")
                self._connection.execute(
                    "INSERT INTO attempt_events VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        event_sequence,
                        canonical_identity.attempt_sha256,
                        canonical_identity.window_sequence,
                        canonical_identity.trace_sequence_index,
                        kind.value,
                        canonical_json_bytes(payload),
                        trace_bytes,
                        previous,
                        digest,
                    ),
                )
                self._connection.execute("COMMIT")
            except ShadowAttemptStoreError:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            except sqlite3.Error as exc:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise _error("SHADOW_ATTEMPT_WRITE_FAILED") from exc
            stored = next(
                item
                for item in self._verify_all()
                if item.identity.attempt_sha256 == identity.attempt_sha256
            )
            return stored

    def start_attempt(
        self, identity: ShadowAttemptIdentity, *, started_at_utc: str
    ) -> ShadowAttemptPermit:
        with self._mutex:
            permit = self._new_permit(identity)
            self._append(
                identity=identity,
                kind=ShadowAttemptStatus.STARTED,
                recorded_at_utc=started_at_utc,
            )
            self._active_started[identity.attempt_sha256] = permit
            return permit

    def complete_attempt(
        self,
        identity: ShadowAttemptIdentity,
        *,
        permit: ShadowAttemptPermit,
        envelope: PersistedShadowTraceEnvelope,
        completed_at_utc: str,
    ) -> ShadowAttemptRecord:
        with self._mutex:
            if type(envelope) is not PersistedShadowTraceEnvelope or envelope.trace is None:
                raise _error("SHADOW_ATTEMPT_TRACE_INVALID")
            self._consume_permit(permit, identity)
            try:
                return self._append(
                    identity=identity,
                    kind=ShadowAttemptStatus.COMPLETED,
                    recorded_at_utc=completed_at_utc,
                    envelope=envelope,
                    reason_codes=tuple(envelope.trace.reason_codes),
                )
            finally:
                # A failed/ambiguous terminal write is never retried through this
                # instance. The surviving STARTED becomes orphan evidence.
                permit._consumed = True

    def fail_attempt(
        self,
        identity: ShadowAttemptIdentity,
        *,
        permit: ShadowAttemptPermit,
        failed_at_utc: str,
        failure_code: str,
        reason_codes: tuple[str, ...] = (),
        error_type: str | None = None,
        envelope: PersistedShadowTraceEnvelope | None = None,
    ) -> ShadowAttemptRecord:
        with self._mutex:
            if type(reason_codes) is not tuple:
                raise _error("SHADOW_ATTEMPT_REASON_INVALID")
            self._consume_permit(permit, identity)
            try:
                return self._append(
                    identity=identity,
                    kind=ShadowAttemptStatus.FAILED,
                    recorded_at_utc=failed_at_utc,
                    envelope=envelope,
                    reason_codes=reason_codes,
                    failure_code=failure_code,
                    error_type=error_type,
                )
            finally:
                permit._consumed = True

    def load_slot(
        self, *, window_sequence: int, trace_sequence_index: int
    ) -> ShadowAttemptRecord | None:
        with self._mutex:
            if type(window_sequence) is not int or type(trace_sequence_index) is not int:
                raise _error("SHADOW_ATTEMPT_SLOT_INVALID")
            record = next(
                (
                    item
                    for item in self._verify_all()
                    if item.identity.window_sequence == window_sequence
                    and item.identity.trace_sequence_index == trace_sequence_index
                ),
                None,
            )
            if record is None:
                return None
            if (
                record.status is ShadowAttemptStatus.STARTED
                and record.identity.attempt_sha256 not in self._active_started
            ):
                return ShadowAttemptRecord(
                    record.identity,
                    ShadowAttemptStatus.ORPHANED,
                    record.started_at_utc,
                    None,
                    None,
                    ("EXECUTION_OUTCOME_UNKNOWN",),
                    "ORPHANED",
                    None,
                )
            return record

    def records_for_window(
        self, window_sequence: int
    ) -> tuple[ShadowAttemptRecord, ...]:
        with self._mutex:
            if type(window_sequence) is not int or window_sequence < 0:
                raise _error("SHADOW_ATTEMPT_SLOT_INVALID")
            records = tuple(
                item
                for item in self._verify_all()
                if item.identity.window_sequence == window_sequence
            )
            return tuple(
                ShadowAttemptRecord(
                    item.identity,
                    (
                        ShadowAttemptStatus.ORPHANED
                        if item.status is ShadowAttemptStatus.STARTED
                        and item.identity.attempt_sha256 not in self._active_started
                        else item.status
                    ),
                    item.started_at_utc,
                    item.terminal_at_utc,
                    item.envelope,
                    (
                        ("EXECUTION_OUTCOME_UNKNOWN",)
                        if item.status is ShadowAttemptStatus.STARTED
                        and item.identity.attempt_sha256 not in self._active_started
                        else item.reason_codes
                    ),
                    (
                        "ORPHANED"
                        if item.status is ShadowAttemptStatus.STARTED
                        and item.identity.attempt_sha256 not in self._active_started
                        else item.failure_code
                    ),
                    item.error_type,
                )
                for item in records
            )
