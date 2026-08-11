"""Hardened SQLite persistence for the R2-B1 response-profile lifecycle.

The ledger persists only canonical B1 run-binding, lifecycle-event, and opaque
evidence documents.  It owns durable reopen detection, cooperative
single-writer ownership, atomic event/blob insertion, and crash-boundary
evaluation.  Opaque evidence bytes remain uninterpreted; R2-C owns their
semantics.

An existing ledger is always reduced twice: first as an ordinary active prefix
and then as a recovery boundary.  Intrinsically invalid persisted state refuses
open.  A recovery-only orphan STARTED or partial block opens as terminal,
read-only crash evidence.  Other valid incomplete reopen states activate a
private fresh-epoch interlock that only a durably committed EPOCH_STARTED can
clear.

``fcntl.flock`` is cooperative process ownership among conforming producers,
not protection from a hostile same-user process.  File ownership, mode, inode,
schema, canonical documents, and the complete B1 reconstruction are verified
fail closed.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import stat
import threading
from typing import Any, Final

from .artifacts import canonical_json_bytes
from .response_profile_evidence import ReplayPosition
from .response_profile_lifecycle import (
    LIFECYCLE_EVENT_SCHEMA_VERSION,
    OPAQUE_EVIDENCE_SCHEMA_VERSION,
    RUN_BINDING_SCHEMA_VERSION,
    LifecycleEventKind,
    OpaqueEvidenceBlob,
    OpaqueEvidenceRole,
    ResponseProfileLifecycleContractError,
    ResponseProfileLifecycleEvent,
    ResponseProfileLifecycleSnapshot,
    ResponseProfileRunBinding,
    apply_next_lifecycle_event,
    build_opaque_evidence_blob,
    build_response_profile_lifecycle_event,
    initial_lifecycle_reducer_state,
    opaque_evidence_document,
    reduce_response_profile_lifecycle,
    response_profile_lifecycle_event_document,
    response_profile_lifecycle_event_payload,
    response_profile_run_binding_document,
    verify_opaque_evidence_blob,
    verify_response_profile_lifecycle_event,
    verify_response_profile_run_binding,
)


__all__ = [
    "ResponseProfileLifecycleLedgerError",
    "ResponseProfileLifecycleLedgerView",
    "ResponseProfileLifecycleExport",
    "MeasurementStartPermit",
    "ResponseProfileLifecycleLedger",
]


_DATABASE_SCHEMA_VERSION: Final = 1
_DATABASE_APPLICATION_ID: Final = 0x56445232  # ASCII "VDR2"
_TERMINAL_RECOVERY_REASONS: Final = frozenset(
    {"ORPHAN_MEASUREMENT_STARTED", "PARTIAL_MEASURED_BLOCK"}
)
_DIGEST_SQL = "length({name}) = 64 AND {name} NOT GLOB '*[^0-9a-f]*'"
_EVENT_KINDS_SQL = ", ".join(f"'{item.value}'" for item in LifecycleEventKind)
_EVIDENCE_ROLES_SQL = ", ".join(f"'{item.value}'" for item in OpaqueEvidenceRole)
_BLOB_EVENT_KINDS_SQL = ", ".join(
    f"'{item.value}'"
    for item in (
        LifecycleEventKind.WARMUP_COMPLETED,
        LifecycleEventKind.BLOCK_STARTED,
        LifecycleEventKind.MEASUREMENT_COMPLETED,
        LifecycleEventKind.BLOCK_CLOSED,
    )
)


def _digest_check(name: str) -> str:
    return _DIGEST_SQL.format(name=name)


_RUN_TABLE_SQL = f"""
CREATE TABLE response_profile_run_binding (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version TEXT NOT NULL CHECK (
        schema_version = '{RUN_BINDING_SCHEMA_VERSION}'
    ),
    run_binding_sha256 TEXT NOT NULL UNIQUE CHECK (
        {_digest_check('run_binding_sha256')}
    ),
    canonical_document BLOB NOT NULL CHECK (
        typeof(canonical_document) = 'blob' AND length(canonical_document) > 0
    )
) STRICT
""".strip()

_EVENT_TABLE_SQL = f"""
CREATE TABLE response_profile_lifecycle_events (
    event_seq INTEGER PRIMARY KEY CHECK (event_seq >= 0),
    schema_version TEXT NOT NULL CHECK (
        schema_version = '{LIFECYCLE_EVENT_SCHEMA_VERSION}'
    ),
    run_binding_sha256 TEXT NOT NULL CHECK (
        {_digest_check('run_binding_sha256')}
    ),
    event_kind TEXT NOT NULL CHECK (event_kind IN ({_EVENT_KINDS_SQL})),
    epoch_index INTEGER,
    block_index INTEGER,
    position_index INTEGER,
    previous_event_sha256 TEXT NOT NULL CHECK (
        {_digest_check('previous_event_sha256')}
    ),
    lifecycle_event_sha256 TEXT NOT NULL UNIQUE CHECK (
        {_digest_check('lifecycle_event_sha256')}
    ),
    referenced_blob_sha256 TEXT UNIQUE CHECK (
        referenced_blob_sha256 IS NULL OR ({_digest_check('referenced_blob_sha256')})
    ),
    canonical_document BLOB NOT NULL CHECK (
        typeof(canonical_document) = 'blob' AND length(canonical_document) > 0
    ),
    FOREIGN KEY (run_binding_sha256)
        REFERENCES response_profile_run_binding(run_binding_sha256)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (referenced_blob_sha256)
        REFERENCES response_profile_opaque_evidence(opaque_evidence_sha256)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (event_kind IN ({_BLOB_EVENT_KINDS_SQL}) AND referenced_blob_sha256 IS NOT NULL)
        OR
        (event_kind NOT IN ({_BLOB_EVENT_KINDS_SQL}) AND referenced_blob_sha256 IS NULL)
    )
) STRICT
""".strip()

_BLOB_TABLE_SQL = f"""
CREATE TABLE response_profile_opaque_evidence (
    opaque_evidence_sha256 TEXT PRIMARY KEY CHECK (
        {_digest_check('opaque_evidence_sha256')}
    ),
    schema_version TEXT NOT NULL CHECK (
        schema_version = '{OPAQUE_EVIDENCE_SCHEMA_VERSION}'
    ),
    run_binding_sha256 TEXT NOT NULL CHECK (
        {_digest_check('run_binding_sha256')}
    ),
    event_seq INTEGER NOT NULL UNIQUE CHECK (event_seq >= 0),
    evidence_role TEXT NOT NULL CHECK (evidence_role IN ({_EVIDENCE_ROLES_SQL})),
    byte_length INTEGER NOT NULL CHECK (byte_length > 0),
    evidence_bytes_sha256 TEXT NOT NULL CHECK (
        {_digest_check('evidence_bytes_sha256')}
    ),
    canonical_document BLOB NOT NULL CHECK (
        typeof(canonical_document) = 'blob' AND length(canonical_document) > 0
    ),
    evidence_bytes BLOB NOT NULL CHECK (
        typeof(evidence_bytes) = 'blob' AND length(evidence_bytes) = byte_length
    ),
    FOREIGN KEY (run_binding_sha256)
        REFERENCES response_profile_run_binding(run_binding_sha256)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (event_seq)
        REFERENCES response_profile_lifecycle_events(event_seq)
        DEFERRABLE INITIALLY DEFERRED
) STRICT
""".strip()

_TABLE_SQL: Final = {
    "response_profile_run_binding": _RUN_TABLE_SQL,
    "response_profile_lifecycle_events": _EVENT_TABLE_SQL,
    "response_profile_opaque_evidence": _BLOB_TABLE_SQL,
}

_TRIGGER_SQL: Final = {
    "response_profile_run_binding_singleton": """
        CREATE TRIGGER response_profile_run_binding_singleton
        BEFORE INSERT ON response_profile_run_binding
        WHEN EXISTS (SELECT 1 FROM response_profile_run_binding)
        BEGIN
            SELECT RAISE(ABORT, 'response-profile run binding is immutable');
        END
    """.strip(),
    "response_profile_run_binding_no_update": """
        CREATE TRIGGER response_profile_run_binding_no_update
        BEFORE UPDATE ON response_profile_run_binding
        BEGIN
            SELECT RAISE(ABORT, 'response-profile run binding cannot be updated');
        END
    """.strip(),
    "response_profile_run_binding_no_delete": """
        CREATE TRIGGER response_profile_run_binding_no_delete
        BEFORE DELETE ON response_profile_run_binding
        BEGIN
            SELECT RAISE(ABORT, 'response-profile run binding cannot be deleted');
        END
    """.strip(),
    "response_profile_lifecycle_events_chain": """
        CREATE TRIGGER response_profile_lifecycle_events_chain
        BEFORE INSERT ON response_profile_lifecycle_events
        WHEN
            NEW.event_seq != (SELECT COUNT(*) FROM response_profile_lifecycle_events)
            OR NEW.run_binding_sha256 != (
                SELECT run_binding_sha256 FROM response_profile_run_binding
                WHERE singleton = 1
            )
            OR NEW.previous_event_sha256 != CASE
                WHEN NEW.event_seq = 0 THEN (
                    SELECT run_binding_sha256 FROM response_profile_run_binding
                    WHERE singleton = 1
                )
                ELSE (
                    SELECT lifecycle_event_sha256
                    FROM response_profile_lifecycle_events
                    ORDER BY event_seq DESC LIMIT 1
                )
            END
        BEGIN
            SELECT RAISE(ABORT, 'response-profile lifecycle chain discontinuity');
        END
    """.strip(),
    "response_profile_lifecycle_events_no_update": """
        CREATE TRIGGER response_profile_lifecycle_events_no_update
        BEFORE UPDATE ON response_profile_lifecycle_events
        BEGIN
            SELECT RAISE(ABORT, 'response-profile lifecycle events cannot be updated');
        END
    """.strip(),
    "response_profile_lifecycle_events_no_delete": """
        CREATE TRIGGER response_profile_lifecycle_events_no_delete
        BEFORE DELETE ON response_profile_lifecycle_events
        BEGIN
            SELECT RAISE(ABORT, 'response-profile lifecycle events cannot be deleted');
        END
    """.strip(),
    "response_profile_opaque_evidence_binding": """
        CREATE TRIGGER response_profile_opaque_evidence_binding
        BEFORE INSERT ON response_profile_opaque_evidence
        WHEN NOT EXISTS (
            SELECT 1
            FROM response_profile_lifecycle_events AS event
            WHERE event.event_seq = NEW.event_seq
              AND event.run_binding_sha256 = NEW.run_binding_sha256
              AND event.referenced_blob_sha256 = NEW.opaque_evidence_sha256
              AND (
                  (event.event_kind = 'WARMUP_COMPLETED' AND NEW.evidence_role = 'WARMUP_EXECUTION')
                  OR (event.event_kind = 'BLOCK_STARTED' AND NEW.evidence_role = 'PRE_BLOCK_RUNTIME_SNAPSHOT')
                  OR (event.event_kind = 'MEASUREMENT_COMPLETED' AND NEW.evidence_role = 'MEASURED_RESULT')
                  OR (event.event_kind = 'BLOCK_CLOSED' AND NEW.evidence_role = 'POST_BLOCK_RUNTIME_SNAPSHOT')
              )
        )
        BEGIN
            SELECT RAISE(ABORT, 'response-profile opaque evidence binding mismatch');
        END
    """.strip(),
    "response_profile_opaque_evidence_no_update": """
        CREATE TRIGGER response_profile_opaque_evidence_no_update
        BEFORE UPDATE ON response_profile_opaque_evidence
        BEGIN
            SELECT RAISE(ABORT, 'response-profile opaque evidence cannot be updated');
        END
    """.strip(),
    "response_profile_opaque_evidence_no_delete": """
        CREATE TRIGGER response_profile_opaque_evidence_no_delete
        BEFORE DELETE ON response_profile_opaque_evidence
        BEGIN
            SELECT RAISE(ABORT, 'response-profile opaque evidence cannot be deleted');
        END
    """.strip(),
}

_EXPECTED_SCHEMA_OBJECTS: Final = frozenset((*_TABLE_SQL, *_TRIGGER_SQL))
_REGISTRY_LOCK = threading.Lock()
_OWNED_INODES: set[tuple[int, int, int]] = set()
_PRESERVE_ACTIVE_PERMIT = object()


class ResponseProfileLifecycleLedgerError(RuntimeError):
    """Stable fail-closed R2-B2 persistence error."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


class _DuplicateJsonField(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ResponseProfileLifecycleLedgerView:
    """Metadata-only derived ledger view; never raw evidence or authority."""

    run_binding_sha256: str
    opened_existing: bool
    terminal_recovery: bool
    terminal_reason_codes: tuple[str, ...]
    event_count: int
    last_event_sha256: str
    current_epoch_index: int | None
    warmup_completed_in_current_epoch: bool
    open_block_index: int | None
    open_measurement_position_index: int | None
    closed_block_count: int
    completed_position_count: int
    seen_epoch_indexes: tuple[int, ...]
    run_sealed_event_count: int
    run_invalidated_event_count: int
    requires_fresh_epoch_after_recovery: bool
    structurally_complete: bool


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileLifecycleExport:
    """One fully verified immutable lifecycle export; evidence, not authority."""

    run_binding: ResponseProfileRunBinding
    events: tuple[ResponseProfileLifecycleEvent, ...]
    opaque_evidence: tuple[OpaqueEvidenceBlob, ...]

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("lifecycle exports are issued only by the durable ledger")


def _make_lifecycle_export(
    *,
    run_binding: ResponseProfileRunBinding,
    events: tuple[ResponseProfileLifecycleEvent, ...],
    opaque_evidence: tuple[OpaqueEvidenceBlob, ...],
) -> ResponseProfileLifecycleExport:
    value = object.__new__(ResponseProfileLifecycleExport)
    object.__setattr__(value, "run_binding", run_binding)
    object.__setattr__(value, "events", events)
    object.__setattr__(value, "opaque_evidence", opaque_evidence)
    return value


@dataclass(frozen=True, slots=True, init=False)
class MeasurementStartPermit:
    """One current-instance permit for exactly one durable STARTED event."""

    owner_pid: int
    run_binding_sha256: str
    event_seq: int
    measurement_started_event_sha256: str
    block_index: int
    within_block_index: int
    position: ReplayPosition
    _instance_token: object = field(repr=False, compare=False)
    _permit_token: object = field(repr=False, compare=False)

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("MeasurementStartPermit is issued only by its ledger instance")


def _make_permit(**values: object) -> MeasurementStartPermit:
    instance = object.__new__(MeasurementStartPermit)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


def _error(code: str, message: str) -> ResponseProfileLifecycleLedgerError:
    return ResponseProfileLifecycleLedgerError(message, code=code)


def _normalize_sql(value: str) -> str:
    source = value.strip()
    if source.endswith(";"):
        source = source[:-1].rstrip()
    normalized: list[str] = []
    quote_end: str | None = None
    index = 0
    while index < len(source):
        character = source[index]
        if quote_end is not None:
            normalized.append(character)
            if character == quote_end:
                if index + 1 < len(source) and source[index + 1] == quote_end:
                    normalized.append(source[index + 1])
                    index += 1
                else:
                    quote_end = None
            index += 1
            continue
        if character in {"'", '"', "`"}:
            quote_end = character
            normalized.append(character)
        elif character == "[":
            quote_end = "]"
            normalized.append(character)
        elif character.isspace():
            if normalized and normalized[-1] != " ":
                normalized.append(" ")
        else:
            normalized.append(character)
        index += 1
    return "".join(normalized).rstrip()


def _no_duplicate_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(key)
        result[key] = value
    return result


def _parse_canonical_document(value: object, *, field_name: str) -> dict[str, object]:
    if type(value) is not bytes or not value:
        raise _error("CANONICAL_DOCUMENT_INVALID", f"{field_name} must be bytes")
    try:
        document = json.loads(
            value.decode("utf-8"), object_pairs_hook=_no_duplicate_fields
        )
    except (
        UnicodeError,
        json.JSONDecodeError,
        _DuplicateJsonField,
        TypeError,
        ValueError,
    ) as exc:
        raise _error(
            "CANONICAL_DOCUMENT_INVALID", f"{field_name} is malformed"
        ) from exc
    if type(document) is not dict or canonical_json_bytes(document) != value:
        raise _error(
            "CANONICAL_DOCUMENT_INVALID", f"{field_name} is not canonical"
        )
    return document


def _exact_mapping(value: object, *, fields_: frozenset[str], field_name: str) -> Mapping[str, object]:
    if type(value) is not dict or frozenset(value) != fields_:
        raise _error("CANONICAL_DOCUMENT_INVALID", f"{field_name} schema mismatch")
    return value


def _referenced_blob(event: ResponseProfileLifecycleEvent) -> str | None:
    payload = response_profile_lifecycle_event_payload(event)
    data = payload["event_data"]
    assert isinstance(data, Mapping)
    field_name = {
        LifecycleEventKind.WARMUP_COMPLETED: "warmup_execution_blob_sha256",
        LifecycleEventKind.BLOCK_STARTED: "pre_block_runtime_snapshot_blob_sha256",
        LifecycleEventKind.MEASUREMENT_COMPLETED: "measured_result_blob_sha256",
        LifecycleEventKind.BLOCK_CLOSED: "post_block_runtime_snapshot_blob_sha256",
    }.get(event.event_kind)
    return None if field_name is None else str(data[field_name])


def _open_private_regular_file(
    path: str, *, create: bool
) -> tuple[int, bool, tuple[int, int]]:
    flags = os.O_RDWR
    created = False
    if create:
        flags |= os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
        created = create
        if created:
            os.fchmod(descriptor, 0o600)
    except FileExistsError:
        existing = os.lstat(path)
        if (
            stat.S_ISLNK(existing.st_mode)
            or not stat.S_ISREG(existing.st_mode)
            or existing.st_nlink != 1
            or existing.st_uid != os.geteuid()
            or stat.S_IMODE(existing.st_mode) != 0o600
        ):
            raise _error(
                "LEDGER_PATH_HARDENING_FAILED",
                "existing ledger files must be owner-owned 0600 single-link regular files",
            )
        flags = os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)

    try:
        descriptor_status = os.fstat(descriptor)
        path_status = os.lstat(path)
        if (
            not stat.S_ISREG(descriptor_status.st_mode)
            or descriptor_status.st_nlink != 1
            or descriptor_status.st_uid != os.geteuid()
            or stat.S_IMODE(descriptor_status.st_mode) != 0o600
            or (descriptor_status.st_dev, descriptor_status.st_ino)
            != (path_status.st_dev, path_status.st_ino)
        ):
            raise _error(
                "LEDGER_PATH_HARDENING_FAILED",
                "ledger files must be owner-owned 0600 single-link regular files",
            )
        return (
            descriptor,
            created,
            (descriptor_status.st_dev, descriptor_status.st_ino),
        )
    except BaseException:
        os.close(descriptor)
        raise


def _prepare_path(
    raw_path: str | os.PathLike[str],
) -> tuple[str, bool, int, tuple[int, int], str, int, tuple[int, int]]:
    try:
        path = os.path.abspath(os.path.expanduser(os.fspath(raw_path)))
    except (TypeError, ValueError, OSError) as exc:
        raise _error("LEDGER_PATH_INVALID", "ledger path is invalid") from exc
    if not path.strip():
        raise _error("LEDGER_PATH_INVALID", "ledger path must be non-empty")
    parent = os.path.dirname(path)
    try:
        parent_status = os.lstat(parent)
        if (
            stat.S_ISLNK(parent_status.st_mode)
            or not stat.S_ISDIR(parent_status.st_mode)
            or parent_status.st_uid != os.geteuid()
            or stat.S_IMODE(parent_status.st_mode) & 0o022
        ):
            raise _error(
                "LEDGER_PATH_HARDENING_FAILED",
                "ledger parent must be owner-controlled and not group/world writable",
            )

        lock_path = f"{path}.lock"
        lock_descriptor, lock_created, lock_inode = _open_private_regular_file(
            lock_path, create=True
        )
        if lock_created:
            try:
                os.fsync(lock_descriptor)
                parent_descriptor = os.open(parent, os.O_RDONLY)
                try:
                    os.fsync(parent_descriptor)
                finally:
                    os.close(parent_descriptor)
            except BaseException:
                os.close(lock_descriptor)
                raise
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(lock_descriptor)
            raise _error(
                "LEDGER_OWNERSHIP_CONFLICT", "ledger already has an owner"
            ) from exc
        ownership_key = _claim_lock_ownership(lock_descriptor, lock_inode)
        descriptor: int | None = None
        try:
            descriptor, created, inode = _open_private_regular_file(
                path, create=True
            )
            if created:
                os.fsync(descriptor)
                parent_descriptor = os.open(parent, os.O_RDONLY)
                try:
                    os.fsync(parent_descriptor)
                finally:
                    os.close(parent_descriptor)
        except BaseException:
            if descriptor is not None:
                os.close(descriptor)
            with _REGISTRY_LOCK:
                _OWNED_INODES.discard(ownership_key)
            try:
                fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(lock_descriptor)
            raise
        return (
            path,
            created,
            descriptor,
            inode,
            lock_path,
            lock_descriptor,
            lock_inode,
        )
    except ResponseProfileLifecycleLedgerError:
        raise
    except OSError as exc:
        raise _error("LEDGER_PATH_HARDENING_FAILED", "ledger path inspection failed") from exc


def _claim_lock_ownership(
    lock_descriptor: int, lock_inode: tuple[int, int]
) -> tuple[int, int, int]:
    """Register a successfully flocked lock inode for this process."""

    ownership_key = (os.getpid(), *lock_inode)
    with _REGISTRY_LOCK:
        if ownership_key in _OWNED_INODES:
            fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
            os.close(lock_descriptor)
            raise _error(
                "LEDGER_OWNERSHIP_CONFLICT", "ledger already owned in process"
            )
        _OWNED_INODES.add(ownership_key)
    return ownership_key


class ResponseProfileLifecycleLedger:
    """One process-owned, append-only R2-B2 lifecycle ledger."""

    def __init__(
        self,
        db_path: str | os.PathLike[str],
        *,
        expected_run_binding: ResponseProfileRunBinding,
    ) -> None:
        try:
            self._run_binding = verify_response_profile_run_binding(
                expected_run_binding
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise _error("RUN_BINDING_INVALID", "expected run binding is invalid") from exc

        self._mutex = threading.RLock()
        self._owner_pid = os.getpid()
        self._instance_token = object()
        self._active_permit: MeasurementStartPermit | None = None
        self._poisoned = False
        self._closed = False
        self._conn: sqlite3.Connection | None = None
        self._file_descriptor: int | None = None
        self._lock_descriptor: int | None = None
        self._inode: tuple[int, int] | None = None
        self._lock_inode: tuple[int, int] | None = None
        self._lock_path = ""
        self._events: list[ResponseProfileLifecycleEvent] = []
        self._blobs: list[OpaqueEvidenceBlob] = []
        # B1.1: opaque O(1)-per-append writer handle, derived once at open by
        # replaying the already-verified prefix (see _load_and_reduce_locked).
        # Never the source of truth -- full replay is, and stays, that.
        self._writer_state: object | None = None
        self._terminal_recovery = False
        self._terminal_reason_codes: tuple[str, ...] = ()
        self._recovery_interlock = False
        self._opened_existing = False
        self._schema_version_cookie = 0
        self._data_version_cookie = 0

        try:
            (
                path,
                created,
                descriptor,
                inode,
                lock_path,
                lock_descriptor,
                lock_inode,
            ) = _prepare_path(db_path)
            self._path = path
            self._opened_existing = not created
            self._file_descriptor = descriptor
            self._inode = inode
            self._lock_path = lock_path
            self._lock_descriptor = lock_descriptor
            self._lock_inode = lock_inode
            self._open_connection(created=created)
        except BaseException:
            self.close()
            raise

    def _open_connection(self, *, created: bool) -> None:
        try:
            connection = sqlite3.connect(
                self._path,
                timeout=0.0,
                isolation_level=None,
                check_same_thread=False,
            )
            connection.row_factory = sqlite3.Row
            self._conn = connection
            connection.execute("PRAGMA busy_timeout = 0")
            connection.execute("PRAGMA trusted_schema = OFF")
            connection.execute("PRAGMA foreign_keys = ON")
            if created:
                mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
                if str(mode).lower() != "delete":
                    raise _error("LEDGER_PRAGMA_INVALID", "DELETE journal unavailable")
            else:
                mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
                if str(mode).lower() != "delete":
                    raise _error(
                        "LEDGER_PRAGMA_INVALID",
                        "existing ledger journal mode must already be DELETE",
                    )
            connection.execute("PRAGMA synchronous = FULL")
            if connection.execute("PRAGMA synchronous").fetchone()[0] != 2:
                raise _error("LEDGER_PRAGMA_INVALID", "FULL synchronous unavailable")

            connection.execute("BEGIN IMMEDIATE")
            try:
                if created:
                    self._initialize_schema_locked(connection)
                else:
                    self._verify_schema_locked(connection)
                self._verify_file_identity()
                self._load_and_reduce_locked(connection, created=created)
                connection.execute("COMMIT")
            except BaseException:
                self._rollback_quietly()
                raise
            self._schema_version_cookie = connection.execute(
                "PRAGMA schema_version"
            ).fetchone()[0]
            self._data_version_cookie = connection.execute(
                "PRAGMA data_version"
            ).fetchone()[0]
            if self._terminal_recovery:
                connection.execute("PRAGMA query_only = ON")
                if connection.execute("PRAGMA query_only").fetchone()[0] != 1:
                    raise _error(
                        "LEDGER_PRAGMA_INVALID",
                        "terminal recovery ledger could not become query-only",
                    )
            if created:
                parent_descriptor = os.open(os.path.dirname(self._path), os.O_RDONLY)
                try:
                    os.fsync(parent_descriptor)
                finally:
                    os.close(parent_descriptor)
        except ResponseProfileLifecycleLedgerError:
            raise
        except (sqlite3.Error, OSError) as exc:
            raise _error("LEDGER_OPEN_FAILED", "failed to open lifecycle ledger") from exc

    def _initialize_schema_locked(self, connection: sqlite3.Connection) -> None:
        if connection.execute("PRAGMA user_version").fetchone()[0] != 0:
            raise _error("LEDGER_SCHEMA_INVALID", "new ledger is not empty")
        objects = self._schema_object_names(connection)
        if objects:
            raise _error("LEDGER_SCHEMA_INVALID", "new ledger already has schema")
        # The event table references the blob table, so both are created before rows.
        connection.execute(_RUN_TABLE_SQL)
        connection.execute(_BLOB_TABLE_SQL)
        connection.execute(_EVENT_TABLE_SQL)
        for sql in _TRIGGER_SQL.values():
            connection.execute(sql)
        connection.execute(f"PRAGMA application_id = {_DATABASE_APPLICATION_ID}")
        connection.execute(f"PRAGMA user_version = {_DATABASE_SCHEMA_VERSION}")
        document = response_profile_run_binding_document(self._run_binding)
        connection.execute(
            "INSERT INTO response_profile_run_binding "
            "(singleton, schema_version, run_binding_sha256, canonical_document) "
            "VALUES (1, ?, ?, ?)",
            (
                RUN_BINDING_SCHEMA_VERSION,
                self._run_binding.run_binding_sha256,
                canonical_json_bytes(document),
            ),
        )
        self._verify_schema_locked(connection)

    @staticmethod
    def _schema_object_names(connection: sqlite3.Connection) -> frozenset[str]:
        rows = connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type IN ('table', 'index', 'trigger', 'view') "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return frozenset(str(row[0]) for row in rows)

    def _verify_schema_locked(self, connection: sqlite3.Connection) -> None:
        if connection.execute("PRAGMA user_version").fetchone()[0] != _DATABASE_SCHEMA_VERSION:
            raise _error("LEDGER_SCHEMA_INVALID", "ledger user_version mismatch")
        if connection.execute("PRAGMA application_id").fetchone()[0] != _DATABASE_APPLICATION_ID:
            raise _error("LEDGER_SCHEMA_INVALID", "ledger application_id mismatch")
        if self._schema_object_names(connection) != _EXPECTED_SCHEMA_OBJECTS:
            raise _error("LEDGER_SCHEMA_INVALID", "ledger schema inventory mismatch")
        for name, expected in _TABLE_SQL.items():
            row = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)
            ).fetchone()
            if row is None or _normalize_sql(str(row[0])) != _normalize_sql(expected):
                raise _error("LEDGER_SCHEMA_INVALID", f"table {name} schema mismatch")
        actual_triggers = {
            str(row[0]): _normalize_sql(str(row[1]))
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='trigger'"
            ).fetchall()
        }
        expected_triggers = {
            name: _normalize_sql(sql) for name, sql in _TRIGGER_SQL.items()
        }
        if actual_triggers != expected_triggers:
            raise _error("LEDGER_SCHEMA_INVALID", "ledger trigger schema mismatch")
        if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
            raise _error("LEDGER_PRAGMA_INVALID", "foreign_keys must be ON")
        if connection.execute("PRAGMA trusted_schema").fetchone()[0] != 0:
            raise _error("LEDGER_PRAGMA_INVALID", "trusted_schema must be OFF")
        if str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower() != "delete":
            raise _error("LEDGER_PRAGMA_INVALID", "journal mode must be DELETE")
        integrity = connection.execute("PRAGMA integrity_check").fetchall()
        if tuple(str(row[0]) for row in integrity) != ("ok",):
            raise _error("LEDGER_INTEGRITY_FAILED", "SQLite integrity check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise _error("LEDGER_INTEGRITY_FAILED", "foreign-key check failed")

    def _load_and_reduce_locked(
        self, connection: sqlite3.Connection, *, created: bool
    ) -> None:
        run_rows = connection.execute(
            "SELECT * FROM response_profile_run_binding ORDER BY singleton"
        ).fetchall()
        if len(run_rows) != 1:
            raise _error("RUN_BINDING_INVALID", "ledger must contain one run binding")
        run_row = run_rows[0]
        run_document = _parse_canonical_document(
            run_row["canonical_document"], field_name="run binding document"
        )
        expected_document = response_profile_run_binding_document(self._run_binding)
        if (
            run_row["canonical_document"] != canonical_json_bytes(expected_document)
            or run_row["schema_version"] != RUN_BINDING_SCHEMA_VERSION
            or run_row["run_binding_sha256"] != self._run_binding.run_binding_sha256
        ):
            raise _error("RUN_BINDING_MISMATCH", "persisted run binding mismatch")

        events = [self._event_from_row(row) for row in connection.execute(
            "SELECT * FROM response_profile_lifecycle_events ORDER BY event_seq"
        ).fetchall()]
        blobs = [self._blob_from_row(row) for row in connection.execute(
            "SELECT * FROM response_profile_opaque_evidence ORDER BY event_seq"
        ).fetchall()]

        active = reduce_response_profile_lifecycle(
            run_binding=self._run_binding,
            events=tuple(events),
            opaque_evidence=tuple(blobs),
            recovery_boundary=False,
        )
        if active.mechanically_invalid:
            raise _error(
                "PERSISTED_LIFECYCLE_INVALID",
                "persisted lifecycle is intrinsically invalid",
            )

        snapshot = active
        if not created:
            recovery = reduce_response_profile_lifecycle(
                run_binding=self._run_binding,
                events=tuple(events),
                opaque_evidence=tuple(blobs),
                recovery_boundary=True,
            )
            if recovery.mechanically_invalid:
                reasons = frozenset(recovery.reason_codes)
                if not reasons or not reasons.issubset(_TERMINAL_RECOVERY_REASONS):
                    raise _error(
                        "PERSISTED_LIFECYCLE_INVALID",
                        "recovery produced non-recovery invalidity",
                    )
                self._terminal_recovery = True
                self._terminal_reason_codes = recovery.reason_codes
            self._recovery_interlock = (
                recovery.requires_fresh_epoch_after_recovery
            )
            snapshot = recovery

        self._events = events
        self._blobs = blobs
        self._snapshot = snapshot
        if not self._terminal_recovery:
            self._writer_state = self._build_writer_state_locked(
                events, blobs, active=active
            )

    def _build_writer_state_locked(
        self,
        events: list[ResponseProfileLifecycleEvent],
        blobs: list[OpaqueEvidenceBlob],
        *,
        active: ResponseProfileLifecycleSnapshot,
    ) -> object:
        """Rebuild the O(1)-per-append B1.1 writer handle once, by replaying
        the already-verified durable prefix (one-time O(N) reopen cost, not
        on the append path). Full replay (``active`` above) remains the
        reference truth; this must converge to exactly the same snapshot.
        """

        blob_by_seq = {blob.event_seq: blob for blob in blobs}
        state = initial_lifecycle_reducer_state(self._run_binding)
        replayed = active
        for event in events:
            replayed = apply_next_lifecycle_event(
                run_binding=self._run_binding,
                reducer_state=state,
                event=event,
                blob=blob_by_seq.get(event.event_seq),
                recovery_boundary=False,
            )
        if replayed != active:
            raise _error(
                "PERSISTED_LIFECYCLE_INVALID",
                "incremental replay of persisted prefix diverged from full reducer",
            )
        return state

    def _event_from_row(self, row: sqlite3.Row) -> ResponseProfileLifecycleEvent:
        document = _parse_canonical_document(
            row["canonical_document"], field_name="lifecycle event document"
        )
        mapping = _exact_mapping(
            document,
            fields_=frozenset({"lifecycle_event_payload", "lifecycle_event_sha256"}),
            field_name="lifecycle event document",
        )
        payload = _exact_mapping(
            mapping["lifecycle_event_payload"],
            fields_=frozenset(
                {
                    "schema_version",
                    "run_binding_sha256",
                    "event_seq",
                    "event_kind",
                    "epoch_index",
                    "block_index",
                    "position_index",
                    "recorded_at_utc",
                    "event_data",
                    "previous_event_sha256",
                }
            ),
            field_name="lifecycle event payload",
        )
        try:
            event = build_response_profile_lifecycle_event(
                run_binding_sha256=payload["run_binding_sha256"],  # type: ignore[arg-type]
                event_seq=payload["event_seq"],  # type: ignore[arg-type]
                event_kind=LifecycleEventKind(payload["event_kind"]),
                epoch_index=payload["epoch_index"],  # type: ignore[arg-type]
                block_index=payload["block_index"],  # type: ignore[arg-type]
                position_index=payload["position_index"],  # type: ignore[arg-type]
                recorded_at_utc=payload["recorded_at_utc"],  # type: ignore[arg-type]
                event_data=payload["event_data"],  # type: ignore[arg-type]
                previous_event_sha256=payload["previous_event_sha256"],  # type: ignore[arg-type]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise _error("LIFECYCLE_EVENT_INVALID", "event row is malformed") from exc
        verified = verify_response_profile_lifecycle_event(event)
        if canonical_json_bytes(response_profile_lifecycle_event_document(verified)) != row[
            "canonical_document"
        ]:
            raise _error("LIFECYCLE_EVENT_INVALID", "event document mismatch")
        expected = (
            verified.event_seq,
            verified.schema_version,
            verified.run_binding_sha256,
            verified.event_kind.value,
            verified.epoch_index,
            verified.block_index,
            verified.position_index,
            verified.previous_event_sha256,
            verified.lifecycle_event_sha256,
            _referenced_blob(verified),
        )
        actual = tuple(
            row[name]
            for name in (
                "event_seq",
                "schema_version",
                "run_binding_sha256",
                "event_kind",
                "epoch_index",
                "block_index",
                "position_index",
                "previous_event_sha256",
                "lifecycle_event_sha256",
                "referenced_blob_sha256",
            )
        )
        if actual != expected or type(actual[0]) is not int:
            raise _error("LIFECYCLE_EVENT_INVALID", "event row projection mismatch")
        return verified

    def _blob_from_row(self, row: sqlite3.Row) -> OpaqueEvidenceBlob:
        document = _parse_canonical_document(
            row["canonical_document"], field_name="opaque evidence document"
        )
        mapping = _exact_mapping(
            document,
            fields_=frozenset({"opaque_evidence_descriptor", "opaque_evidence_sha256"}),
            field_name="opaque evidence document",
        )
        descriptor = _exact_mapping(
            mapping["opaque_evidence_descriptor"],
            fields_=frozenset(
                {
                    "schema_version",
                    "run_binding_sha256",
                    "event_seq",
                    "evidence_role",
                    "byte_length",
                    "evidence_bytes_sha256",
                }
            ),
            field_name="opaque evidence descriptor",
        )
        evidence_bytes = row["evidence_bytes"]
        if type(evidence_bytes) is not bytes:
            raise _error("OPAQUE_EVIDENCE_INVALID", "opaque evidence bytes malformed")
        try:
            blob = build_opaque_evidence_blob(
                run_binding_sha256=descriptor["run_binding_sha256"],  # type: ignore[arg-type]
                event_seq=descriptor["event_seq"],  # type: ignore[arg-type]
                evidence_role=OpaqueEvidenceRole(descriptor["evidence_role"]),
                evidence_bytes=evidence_bytes,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise _error("OPAQUE_EVIDENCE_INVALID", "opaque evidence row malformed") from exc
        verified = verify_opaque_evidence_blob(blob)
        if canonical_json_bytes(opaque_evidence_document(verified)) != row[
            "canonical_document"
        ]:
            raise _error("OPAQUE_EVIDENCE_INVALID", "opaque evidence document mismatch")
        expected = (
            verified.opaque_evidence_sha256,
            verified.schema_version,
            verified.run_binding_sha256,
            verified.event_seq,
            verified.evidence_role.value,
            verified.byte_length,
            verified.evidence_bytes_sha256,
        )
        actual = tuple(
            row[name]
            for name in (
                "opaque_evidence_sha256",
                "schema_version",
                "run_binding_sha256",
                "event_seq",
                "evidence_role",
                "byte_length",
                "evidence_bytes_sha256",
            )
        )
        if actual != expected or type(actual[3]) is not int or type(actual[5]) is not int:
            raise _error("OPAQUE_EVIDENCE_INVALID", "opaque row projection mismatch")
        return verified

    def _verify_file_identity(self) -> None:
        descriptor = self._file_descriptor
        inode = self._inode
        if descriptor is None or inode is None:
            raise _error("LEDGER_CLOSED", "ledger ownership descriptor is closed")
        try:
            descriptor_status = os.fstat(descriptor)
            path_status = os.lstat(self._path)
        except OSError as exc:
            raise _error("LEDGER_FILE_DRIFT", "ledger file identity unavailable") from exc
        if (
            (descriptor_status.st_dev, descriptor_status.st_ino) != inode
            or (path_status.st_dev, path_status.st_ino) != inode
            or not stat.S_ISREG(path_status.st_mode)
            or path_status.st_nlink != 1
            or path_status.st_uid != os.geteuid()
            or stat.S_IMODE(path_status.st_mode) != 0o600
        ):
            raise _error("LEDGER_FILE_DRIFT", "ledger file identity changed")
        lock_descriptor = self._lock_descriptor
        lock_inode = self._lock_inode
        if lock_descriptor is None or lock_inode is None:
            raise _error("LEDGER_CLOSED", "ledger ownership lock is closed")
        try:
            lock_status = os.fstat(lock_descriptor)
            lock_path_status = os.lstat(self._lock_path)
        except OSError as exc:
            raise _error("LEDGER_FILE_DRIFT", "ledger lock identity unavailable") from exc
        if (
            (lock_status.st_dev, lock_status.st_ino) != lock_inode
            or (lock_path_status.st_dev, lock_path_status.st_ino) != lock_inode
            or not stat.S_ISREG(lock_path_status.st_mode)
            or lock_path_status.st_nlink != 1
            or lock_path_status.st_uid != os.geteuid()
            or stat.S_IMODE(lock_path_status.st_mode) != 0o600
        ):
            raise _error("LEDGER_FILE_DRIFT", "ledger lock identity changed")

    def _require_operational(self) -> sqlite3.Connection:
        if os.getpid() != self._owner_pid:
            raise _error("LEDGER_FORK_INVALID", "ledger cannot be used after fork")
        if self._closed or self._conn is None:
            raise _error("LEDGER_CLOSED", "ledger is closed")
        if self._poisoned:
            raise _error("LEDGER_POISONED", "ledger is poisoned")
        try:
            self._verify_file_identity()
        except ResponseProfileLifecycleLedgerError:
            self._poisoned = True
            raise
        return self._conn

    def _require_mutation(self, *, begin_epoch: bool = False) -> sqlite3.Connection:
        connection = self._require_operational()
        if self._terminal_recovery:
            raise _error("LEDGER_TERMINAL", "terminal recovery evidence is read-only")
        if self._recovery_interlock and not begin_epoch:
            raise _error(
                "RECOVERY_FRESH_EPOCH_REQUIRED",
                "fresh EPOCH_STARTED must commit before any other mutation",
            )
        if self._active_permit is not None and not begin_epoch:
            raise _error(
                "MEASUREMENT_COMPLETION_REQUIRED",
                "the active STARTED permit must complete before another mutation",
            )
        return connection

    def _next_anchor(self) -> tuple[int, str]:
        return (
            len(self._events),
            self._events[-1].lifecycle_event_sha256
            if self._events
            else self._run_binding.run_binding_sha256,
        )

    def _build_event(
        self,
        *,
        event_kind: LifecycleEventKind,
        epoch_index: int | None,
        block_index: int | None,
        position_index: int | None,
        recorded_at_utc: str,
        event_data: Mapping[str, object],
    ) -> ResponseProfileLifecycleEvent:
        sequence, previous = self._next_anchor()
        try:
            return build_response_profile_lifecycle_event(
                run_binding_sha256=self._run_binding.run_binding_sha256,
                event_seq=sequence,
                event_kind=event_kind,
                epoch_index=epoch_index,
                block_index=block_index,
                position_index=position_index,
                recorded_at_utc=recorded_at_utc,
                event_data=event_data,
                previous_event_sha256=previous,
            )
        except (AttributeError, TypeError, ResponseProfileLifecycleContractError) as exc:
            raise _error(
                "LIFECYCLE_INPUT_INVALID", "lifecycle event input is invalid"
            ) from exc

    def _build_blob(
        self, *, event_seq: int, role: OpaqueEvidenceRole, evidence_bytes: bytes
    ) -> OpaqueEvidenceBlob:
        try:
            return build_opaque_evidence_blob(
                run_binding_sha256=self._run_binding.run_binding_sha256,
                event_seq=event_seq,
                evidence_role=role,
                evidence_bytes=evidence_bytes,
            )
        except (AttributeError, TypeError, ResponseProfileLifecycleContractError) as exc:
            raise _error(
                "LIFECYCLE_INPUT_INVALID", "opaque evidence input is invalid"
            ) from exc

    def _candidate_snapshot(
        self,
        event: ResponseProfileLifecycleEvent,
        blob: OpaqueEvidenceBlob | None,
    ) -> ResponseProfileLifecycleSnapshot:
        """B1.1: derive the candidate next state in O(1), not by re-deriving
        the whole prior history. On rejection, ``self._writer_state`` is
        guaranteed left byte-identical to before this call (proved by this
        module's own equivalence/rejection tests), so a declined candidate
        never corrupts the writer state a later, valid attempt will use.
        """

        if self._writer_state is None:
            raise _error(
                "LEDGER_TERMINAL", "writer state unavailable for mutation"
            )
        snapshot = apply_next_lifecycle_event(
            run_binding=self._run_binding,
            reducer_state=self._writer_state,
            event=event,
            blob=blob,
            recovery_boundary=False,
        )
        if snapshot.mechanically_invalid:
            raise _error(
                "LIFECYCLE_TRANSITION_INVALID",
                ",".join(snapshot.reason_codes) or "lifecycle transition invalid",
            )
        return snapshot

    def _verify_runtime_head_locked(self, connection: sqlite3.Connection) -> None:
        if connection.execute("PRAGMA schema_version").fetchone()[0] != self._schema_version_cookie:
            raise _error("LEDGER_SCHEMA_DRIFT", "schema changed after open")
        if connection.execute("PRAGMA data_version").fetchone()[0] != self._data_version_cookie:
            raise _error("LEDGER_HEAD_DRIFT", "database changed outside this connection")
        row = connection.execute(
            "SELECT COUNT(*) AS count, MAX(event_seq) AS max_seq "
            "FROM response_profile_lifecycle_events"
        ).fetchone()
        if row["count"] != len(self._events) or row["max_seq"] != (
            len(self._events) - 1 if self._events else None
        ):
            raise _error("LEDGER_HEAD_DRIFT", "event count changed after verification")
        if self._events:
            digest = connection.execute(
                "SELECT lifecycle_event_sha256 FROM response_profile_lifecycle_events "
                "ORDER BY event_seq DESC LIMIT 1"
            ).fetchone()[0]
            if digest != self._events[-1].lifecycle_event_sha256:
                raise _error("LEDGER_HEAD_DRIFT", "event head digest changed")

    def _commit_candidate(
        self,
        *,
        event: ResponseProfileLifecycleEvent,
        blob: OpaqueEvidenceBlob | None,
        snapshot: ResponseProfileLifecycleSnapshot,
        active_permit_after_commit: MeasurementStartPermit | None | object = (
            _PRESERVE_ACTIVE_PERMIT
        ),
    ) -> None:
        connection = self._require_operational()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._verify_runtime_head_locked(connection)
            connection.execute(
                "INSERT INTO response_profile_lifecycle_events "
                "(event_seq, schema_version, run_binding_sha256, event_kind, "
                "epoch_index, block_index, position_index, previous_event_sha256, "
                "lifecycle_event_sha256, referenced_blob_sha256, canonical_document) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    event.event_seq,
                    event.schema_version,
                    event.run_binding_sha256,
                    event.event_kind.value,
                    event.epoch_index,
                    event.block_index,
                    event.position_index,
                    event.previous_event_sha256,
                    event.lifecycle_event_sha256,
                    _referenced_blob(event),
                    canonical_json_bytes(response_profile_lifecycle_event_document(event)),
                ),
            )
            if blob is not None:
                connection.execute(
                    "INSERT INTO response_profile_opaque_evidence "
                    "(opaque_evidence_sha256, schema_version, run_binding_sha256, "
                    "event_seq, evidence_role, byte_length, evidence_bytes_sha256, "
                    "canonical_document, evidence_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        blob.opaque_evidence_sha256,
                        blob.schema_version,
                        blob.run_binding_sha256,
                        blob.event_seq,
                        blob.evidence_role.value,
                        blob.byte_length,
                        blob.evidence_bytes_sha256,
                        canonical_json_bytes(opaque_evidence_document(blob)),
                        blob.evidence_bytes,
                    ),
                )
            if connection.execute("PRAGMA foreign_key_check").fetchall():
                raise _error("LEDGER_TRANSACTION_INVALID", "foreign-key check failed")
            connection.execute("COMMIT")
        except BaseException as exc:
            self._rollback_quietly()
            self._poisoned = True
            self._active_permit = None
            if isinstance(exc, ResponseProfileLifecycleLedgerError):
                raise exc
            raise _error("LEDGER_TRANSACTION_FAILED", "ledger transaction failed") from exc
        try:
            self._reconcile_committed_candidate(
                event=event,
                blob=blob,
                snapshot=snapshot,
                active_permit_after_commit=active_permit_after_commit,
            )
        except BaseException as exc:
            # COMMIT is already durable.  Never roll it back or retry it; make the
            # current instance unusable so reopen must reconstruct from SQLite.
            self._poisoned = True
            self._active_permit = None
            raise _error(
                "LEDGER_POST_COMMIT_RECONCILIATION_FAILED",
                "durable commit could not be reconciled in memory",
            ) from exc

    def _reconcile_committed_candidate(
        self,
        *,
        event: ResponseProfileLifecycleEvent,
        blob: OpaqueEvidenceBlob | None,
        snapshot: ResponseProfileLifecycleSnapshot,
        active_permit_after_commit: MeasurementStartPermit | None | object,
    ) -> None:
        """Install one already-durable append; callers poison on any failure."""

        self._events.append(event)
        if blob is not None:
            self._blobs.append(blob)
        self._snapshot = snapshot
        if active_permit_after_commit is not _PRESERVE_ACTIVE_PERMIT:
            self._active_permit = active_permit_after_commit  # type: ignore[assignment]

    def current_view(self) -> ResponseProfileLifecycleLedgerView:
        with self._mutex:
            self._require_operational()
            snapshot = self._snapshot
            return ResponseProfileLifecycleLedgerView(
                run_binding_sha256=snapshot.run_binding_sha256,
                opened_existing=self._opened_existing,
                terminal_recovery=self._terminal_recovery,
                terminal_reason_codes=self._terminal_reason_codes,
                event_count=snapshot.event_count,
                last_event_sha256=snapshot.last_event_sha256,
                current_epoch_index=snapshot.current_epoch_index,
                warmup_completed_in_current_epoch=(
                    snapshot.warmup_completed_in_current_epoch
                ),
                open_block_index=snapshot.open_block_index,
                open_measurement_position_index=(
                    snapshot.open_measurement_position_index
                ),
                closed_block_count=snapshot.closed_block_count,
                completed_position_count=snapshot.completed_position_count,
                seen_epoch_indexes=snapshot.seen_epoch_indexes,
                run_sealed_event_count=snapshot.run_sealed_event_count,
                run_invalidated_event_count=snapshot.run_invalidated_event_count,
                requires_fresh_epoch_after_recovery=self._recovery_interlock,
                structurally_complete=snapshot.structurally_complete,
            )

    def export_verified_lifecycle(self) -> ResponseProfileLifecycleExport:
        """Return a coherent, complete, fully reconstructed evidence snapshot.

        This is the only raw-byte export boundary.  It deliberately performs a
        fresh database reconstruction instead of exposing the writer's cached
        lists, and it never turns evidence into semantic or candidate authority.
        """

        with self._mutex:
            connection = self._require_operational()
            if (
                self._terminal_recovery
                or self._recovery_interlock
                or self._active_permit is not None
                or not self._snapshot.structurally_complete
            ):
                raise _error(
                    "LIFECYCLE_EXPORT_UNAVAILABLE",
                    "only a complete active lifecycle can be exported",
                )
            try:
                self._verify_file_identity()
                connection.execute("BEGIN")
                self._verify_schema_locked(connection)
                self._verify_runtime_head_locked(connection)
                events = tuple(
                    self._event_from_row(row)
                    for row in connection.execute(
                        "SELECT * FROM response_profile_lifecycle_events "
                        "ORDER BY event_seq"
                    ).fetchall()
                )
                opaque_evidence = tuple(
                    self._blob_from_row(row)
                    for row in connection.execute(
                        "SELECT * FROM response_profile_opaque_evidence "
                        "ORDER BY event_seq"
                    ).fetchall()
                )
                reconstructed = reduce_response_profile_lifecycle(
                    run_binding=self._run_binding,
                    events=events,
                    opaque_evidence=opaque_evidence,
                    recovery_boundary=False,
                )
                if (
                    reconstructed.mechanically_invalid
                    or not reconstructed.structurally_complete
                    or reconstructed != self._snapshot
                    or events != tuple(self._events)
                    or opaque_evidence != tuple(self._blobs)
                ):
                    raise _error(
                        "LIFECYCLE_EXPORT_INVALID",
                        "durable lifecycle export did not match verified state",
                    )
                connection.execute("COMMIT")
            except BaseException as exc:
                self._rollback_quietly()
                self._poisoned = True
                self._active_permit = None
                if isinstance(exc, ResponseProfileLifecycleLedgerError):
                    raise exc
                raise _error(
                    "LIFECYCLE_EXPORT_FAILED",
                    "durable lifecycle export failed",
                ) from exc
            return _make_lifecycle_export(
                run_binding=self._run_binding,
                events=events,
                opaque_evidence=opaque_evidence,
            )

    def begin_epoch(
        self, *, epoch_index: int, recorded_at_utc: str
    ) -> ResponseProfileLifecycleEvent:
        with self._mutex:
            self._require_mutation(begin_epoch=True)
            if self._active_permit is not None:
                raise _error(
                    "MEASUREMENT_COMPLETION_REQUIRED",
                    "cannot begin an epoch with an active permit",
                )
            event = self._build_event(
                event_kind=LifecycleEventKind.EPOCH_STARTED,
                epoch_index=epoch_index,
                block_index=None,
                position_index=None,
                recorded_at_utc=recorded_at_utc,
                event_data={},
            )
            snapshot = self._candidate_snapshot(event, None)
            self._commit_candidate(event=event, blob=None, snapshot=snapshot)
            self._recovery_interlock = False
            return event

    def complete_warmup(
        self, *, evidence_bytes: bytes, recorded_at_utc: str
    ) -> ResponseProfileLifecycleEvent:
        with self._mutex:
            self._require_mutation()
            epoch = self._snapshot.current_epoch_index
            event_seq, _ = self._next_anchor()
            blob = self._build_blob(
                event_seq=event_seq,
                role=OpaqueEvidenceRole.WARMUP_EXECUTION,
                evidence_bytes=evidence_bytes,
            )
            event = self._build_event(
                event_kind=LifecycleEventKind.WARMUP_COMPLETED,
                epoch_index=epoch,
                block_index=None,
                position_index=None,
                recorded_at_utc=recorded_at_utc,
                event_data={
                    "warmup_role_manifest_sha256": (
                        self._run_binding.warmup_role_manifest_sha256
                    ),
                    "warmup_execution_blob_sha256": blob.opaque_evidence_sha256,
                },
            )
            snapshot = self._candidate_snapshot(event, blob)
            self._commit_candidate(event=event, blob=blob, snapshot=snapshot)
            return event

    def start_block(
        self, *, evidence_bytes: bytes, recorded_at_utc: str
    ) -> ResponseProfileLifecycleEvent:
        with self._mutex:
            self._require_mutation()
            event_seq, _ = self._next_anchor()
            blob = self._build_blob(
                event_seq=event_seq,
                role=OpaqueEvidenceRole.PRE_BLOCK_RUNTIME_SNAPSHOT,
                evidence_bytes=evidence_bytes,
            )
            event = self._build_event(
                event_kind=LifecycleEventKind.BLOCK_STARTED,
                epoch_index=self._snapshot.current_epoch_index,
                block_index=self._snapshot.closed_block_count,
                position_index=None,
                recorded_at_utc=recorded_at_utc,
                event_data={
                    "pre_block_runtime_snapshot_blob_sha256": (
                        blob.opaque_evidence_sha256
                    )
                },
            )
            snapshot = self._candidate_snapshot(event, blob)
            self._commit_candidate(event=event, blob=blob, snapshot=snapshot)
            return event

    def _current_block_completion_events(self) -> tuple[ResponseProfileLifecycleEvent, ...]:
        block_index = self._snapshot.open_block_index
        if block_index is None:
            return ()
        completed: list[ResponseProfileLifecycleEvent] = []
        for event in reversed(self._events):
            if event.event_kind is LifecycleEventKind.BLOCK_STARTED:
                break
            if (
                event.event_kind is LifecycleEventKind.MEASUREMENT_COMPLETED
                and event.block_index == block_index
            ):
                completed.append(event)
        return tuple(reversed(completed))

    def start_measurement(
        self, *, started_monotonic_ns: int, recorded_at_utc: str
    ) -> MeasurementStartPermit:
        with self._mutex:
            self._require_mutation()
            block_index = self._snapshot.open_block_index
            if block_index is None:
                raise _error("BLOCK_REQUIRED", "measurement requires an open block")
            completions = self._current_block_completion_events()
            within = len(completions)
            if within >= 4:
                raise _error("BLOCK_INCOMPLETE", "block must close before another start")
            position = self._run_binding.replay_schedule.blocks[
                block_index
            ].positions[within]
            event = self._build_event(
                event_kind=LifecycleEventKind.MEASUREMENT_STARTED,
                epoch_index=self._snapshot.current_epoch_index,
                block_index=block_index,
                position_index=position.position_index,
                recorded_at_utc=recorded_at_utc,
                event_data={
                    "within_block_index": within,
                    "canonical_query_index": position.canonical_query_index,
                    "query_id": position.query_id,
                    "query_id_sha256": position.query_id_sha256,
                    "observation_identity_sha256": (
                        position.observation_identity_sha256
                    ),
                    "ef": position.ef,
                    "started_monotonic_ns": started_monotonic_ns,
                },
            )
            # Build the permit before deriving the candidate snapshot: the
            # candidate step (B1.1) tentatively advances the writer state as
            # a side effect of succeeding, so nothing that can still fail
            # (like permit construction) may run between it and commit --
            # otherwise a permit-construction failure here would leave the
            # writer state advanced for an event that was never persisted.
            try:
                permit = _make_permit(
                    owner_pid=self._owner_pid,
                    run_binding_sha256=self._run_binding.run_binding_sha256,
                    event_seq=event.event_seq,
                    measurement_started_event_sha256=event.lifecycle_event_sha256,
                    block_index=block_index,
                    within_block_index=within,
                    position=position,
                    _instance_token=self._instance_token,
                    _permit_token=object(),
                )
            except Exception as exc:
                raise _error(
                    "MEASUREMENT_PERMIT_CONSTRUCTION_FAILED",
                    "measurement permit could not be constructed",
                ) from exc
            snapshot = self._candidate_snapshot(event, None)
            self._commit_candidate(
                event=event,
                blob=None,
                snapshot=snapshot,
                active_permit_after_commit=permit,
            )
            return permit

    def _validate_permit(self, permit: object) -> MeasurementStartPermit:
        active = self._active_permit
        if (
            type(permit) is not MeasurementStartPermit
            or permit is not active
            or active is None
            or permit._instance_token is not self._instance_token
            or permit.owner_pid != self._owner_pid
            or permit.owner_pid != os.getpid()
            or permit.run_binding_sha256 != self._run_binding.run_binding_sha256
        ):
            raise _error("MEASUREMENT_PERMIT_INVALID", "measurement permit is invalid")
        started = self._events[-1] if self._events else None
        if (
            started is None
            or started.event_kind is not LifecycleEventKind.MEASUREMENT_STARTED
            or started.event_seq != permit.event_seq
            or started.lifecycle_event_sha256
            != permit.measurement_started_event_sha256
            or started.position_index != permit.position.position_index
            or started.block_index != permit.block_index
        ):
            raise _error("MEASUREMENT_PERMIT_INVALID", "permit STARTED binding mismatch")
        return permit

    def complete_measurement(
        self,
        *,
        permit: MeasurementStartPermit,
        evidence_bytes: bytes,
        completed_monotonic_ns: int,
        recorded_at_utc: str,
    ) -> ResponseProfileLifecycleEvent:
        with self._mutex:
            self._require_operational()
            if self._terminal_recovery or self._recovery_interlock:
                raise _error("LEDGER_TERMINAL", "measurement completion unavailable")
            active = self._validate_permit(permit)
            # Build and full-reduce before entering SQLite.  Deterministic failure
            # leaves the same durable STARTED and permit available for correction.
            event_seq, _ = self._next_anchor()
            blob = self._build_blob(
                event_seq=event_seq,
                role=OpaqueEvidenceRole.MEASURED_RESULT,
                evidence_bytes=evidence_bytes,
            )
            started = self._events[-1]
            event = self._build_event(
                event_kind=LifecycleEventKind.MEASUREMENT_COMPLETED,
                epoch_index=started.epoch_index,
                block_index=started.block_index,
                position_index=started.position_index,
                recorded_at_utc=recorded_at_utc,
                event_data={
                    "measurement_started_event_sha256": (
                        active.measurement_started_event_sha256
                    ),
                    "measured_result_blob_sha256": blob.opaque_evidence_sha256,
                    "completed_monotonic_ns": completed_monotonic_ns,
                },
            )
            snapshot = self._candidate_snapshot(event, blob)
            self._commit_candidate(
                event=event,
                blob=blob,
                snapshot=snapshot,
                active_permit_after_commit=None,
            )
            return event

    def close_block(
        self, *, evidence_bytes: bytes, recorded_at_utc: str
    ) -> ResponseProfileLifecycleEvent:
        with self._mutex:
            self._require_mutation()
            block_index = self._snapshot.open_block_index
            if block_index is None:
                raise _error("BLOCK_REQUIRED", "block close requires an open block")
            completions = self._current_block_completion_events()
            started = next(
                (
                    event
                    for event in reversed(self._events)
                    if event.event_kind is LifecycleEventKind.BLOCK_STARTED
                    and event.block_index == block_index
                ),
                None,
            )
            if started is None:
                raise _error("BLOCK_REQUIRED", "block STARTED event is missing")
            event_seq, _ = self._next_anchor()
            blob = self._build_blob(
                event_seq=event_seq,
                role=OpaqueEvidenceRole.POST_BLOCK_RUNTIME_SNAPSHOT,
                evidence_bytes=evidence_bytes,
            )
            event = self._build_event(
                event_kind=LifecycleEventKind.BLOCK_CLOSED,
                epoch_index=self._snapshot.current_epoch_index,
                block_index=block_index,
                position_index=None,
                recorded_at_utc=recorded_at_utc,
                event_data={
                    "block_started_event_sha256": started.lifecycle_event_sha256,
                    "measurement_completed_event_sha256": [
                        item.lifecycle_event_sha256 for item in completions
                    ],
                    "post_block_runtime_snapshot_blob_sha256": (
                        blob.opaque_evidence_sha256
                    ),
                },
            )
            snapshot = self._candidate_snapshot(event, blob)
            self._commit_candidate(event=event, blob=blob, snapshot=snapshot)
            return event

    def _append_run_event(
        self,
        *,
        kind: LifecycleEventKind,
        recorded_at_utc: str,
        event_data: Mapping[str, object],
    ) -> ResponseProfileLifecycleEvent:
        with self._mutex:
            self._require_mutation()
            event = self._build_event(
                event_kind=kind,
                epoch_index=None,
                block_index=None,
                position_index=None,
                recorded_at_utc=recorded_at_utc,
                event_data=event_data,
            )
            snapshot = self._candidate_snapshot(event, None)
            self._commit_candidate(event=event, blob=None, snapshot=snapshot)
            return event

    def append_run_sealed(
        self, *, recorded_at_utc: str
    ) -> ResponseProfileLifecycleEvent:
        return self._append_run_event(
            kind=LifecycleEventKind.RUN_SEALED,
            recorded_at_utc=recorded_at_utc,
            event_data={},
        )

    def append_run_invalidated(
        self, *, reason_code: str, recorded_at_utc: str
    ) -> ResponseProfileLifecycleEvent:
        return self._append_run_event(
            kind=LifecycleEventKind.RUN_INVALIDATED,
            recorded_at_utc=recorded_at_utc,
            event_data={"reason_code": reason_code},
        )

    def _rollback_quietly(self) -> None:
        connection = self._conn
        if connection is None:
            return
        try:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    def close(self) -> None:
        with getattr(self, "_mutex", threading.RLock()):
            if getattr(self, "_closed", False):
                return
            self._closed = True
            self._active_permit = None
            same_process = os.getpid() == getattr(self, "_owner_pid", None)
            connection = getattr(self, "_conn", None)
            self._conn = None
            if connection is not None and same_process:
                try:
                    connection.close()
                except sqlite3.Error:
                    pass
            descriptor = getattr(self, "_lock_descriptor", None)
            lock_inode = getattr(self, "_lock_inode", None)
            self._lock_descriptor = None
            if descriptor is not None:
                if same_process:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                    except OSError:
                        pass
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            file_descriptor = getattr(self, "_file_descriptor", None)
            self._file_descriptor = None
            if file_descriptor is not None:
                try:
                    os.close(file_descriptor)
                except OSError:
                    pass
            if lock_inode is not None and same_process:
                with _REGISTRY_LOCK:
                    _OWNED_INODES.discard((self._owner_pid, *lock_inode))

    def __enter__(self) -> ResponseProfileLifecycleLedger:
        self._require_operational()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()
