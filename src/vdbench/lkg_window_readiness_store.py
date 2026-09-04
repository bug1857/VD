"""ADR-020 production durable LKG window operational-readiness provider.

Purpose:
    Implement the frozen ``LkgWindowOperationalReadinessProvider``
    protocol with restart durability, so readiness evidence captured
    before a qualification run is sealed survives to the post-seal
    ``lookup`` Phase 2 performs. The shipped
    ``FakeLkgWindowOperationalReadinessProvider`` keeps its state in
    process memory and therefore cannot bridge that interval; this store
    is the production replacement.
Durability model (ADR-020 sections 15-20):
    One dedicated SQLite database per ``source_run_id``, owned by this
    provider and distinct from the Phase-1 and Phase-2 ledgers. On first
    open it persists the COMPLETE canonical ``LkgRunBinding`` document --
    not merely its digest -- in one immutable binding row, and
    re-validates it on every subsequent open.
Exactly one logical observation (ADR-020 sections 21-22):
    The external readiness observation is performed WHILE this provider
    holds the same exclusive ``BEGIN IMMEDIATE`` transaction that
    enforces first-writer uniqueness. A second concurrent caller for the
    same window blocks, then returns the committed historical evidence
    having performed zero observation. The shipped fake achieves the same
    guarantee by invoking its builder under its own lock; this store
    achieves it under the database write lock.
Observed failure versus provider inability (ADR-020 section 26):
    An observed health or rollback-readiness failure is real evidence:
    it is persisted with the checked/tested flag true and the
    passed/ready flag false, and permanently invalidates the window and
    run. An inability to obtain a trustworthy observation raises and
    persists NOTHING, leaving the window retryable. Persisting an
    inability as ``health_checked=false`` is forbidden, because
    Checkpoint C treats that as FAILING while absent readiness is only
    INCOMPLETE.
Zero actuation (ADR-020 sections 41-42):
    This module issues no vector, ANN, or hybrid search, changes no
    ``ef``, rebuilds no index, mutates no route, creates or reserves no
    grant, activates no candidate, runs no canary, and performs no
    rollback actuation. It opens exactly one SQLite file it owns.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Protocol

from .artifacts import canonical_json_bytes
from .config import ContractViolation
from .lkg_run_binding import (
    LkgRunBinding,
    lkg_run_binding_document,
    lkg_run_binding_from_document,
    lkg_run_binding_sha256,
)
from .lkg_window_readiness import (
    READINESS_SCHEMA_VERSION,
    LkgWindowOperationalReadinessEvidence,
    LkgWindowOperationalReadinessProviderError,
    lkg_window_operational_readiness_evidence_from_payload,
    readiness_payload_document_digest,
)
from .lkg_window_readiness_observation import (
    LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY,
    LKG_ROLLBACK_READINESS_SOURCE_IDENTITY,
    LkgWindowHealthObservation,
    LkgWindowReadinessObservationError,
    LkgWindowRollbackReadiness,
    derive_lkg_window_provider_run_id,
    derive_lkg_window_readiness_check_id,
    validate_lkg_window_health_observation,
    validate_lkg_window_rollback_readiness,
)

__all__ = [
    "LKG_READINESS_STORE_SCHEMA_VERSION",
    "LkgWindowReadinessObserver",
    "SqliteLkgWindowOperationalReadinessProvider",
]

LKG_READINESS_STORE_SCHEMA_VERSION = 2

_WINDOWS_PER_RUN = 12
_WINDOWS_PER_EPOCH = 6
_POSITIONS_PER_WINDOW = 200
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")

_BINDING_TABLE = "lkg_readiness_store_binding"
_EVIDENCE_TABLE = "lkg_window_readiness_evidence"
_TABLE_SQL = {
    _BINDING_TABLE: f"CREATE TABLE {_BINDING_TABLE} ("
    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
    "source_run_id TEXT NOT NULL,"
    "source_run_binding_sha256 TEXT NOT NULL CHECK(length(source_run_binding_sha256)=64),"
    "environment_identity TEXT NOT NULL,run_binding_document BLOB NOT NULL,"
    "bound_at_utc TEXT NOT NULL) STRICT",
    _EVIDENCE_TABLE: f"CREATE TABLE {_EVIDENCE_TABLE} ("
    "readiness_check_id TEXT PRIMARY KEY CHECK(length(readiness_check_id)=64),"
    "source_run_id TEXT NOT NULL,"
    "window_index INTEGER NOT NULL CHECK(window_index>=0 AND window_index<12),"
    "payload_document BLOB NOT NULL,"
    "canonical_document_digest TEXT NOT NULL CHECK(length(canonical_document_digest)=64),"
    "health_source_document_bytes BLOB NOT NULL,"
    "rollback_source_document_bytes BLOB NOT NULL,"
    "UNIQUE(source_run_id, window_index)) STRICT",
}
_TRIGGER_SQL = {
    f"{table}_no_{event}": (
        f"CREATE TRIGGER {table}_no_{event} BEFORE {event.upper()} ON {table} "
        "BEGIN SELECT RAISE(ABORT,'append-only'); END"
    )
    for table in _TABLE_SQL for event in ("update", "delete")
}
# name, declared type, not-null, default, primary-key ordinal, hidden.
_TABLE_COLUMNS = {
    _BINDING_TABLE: (
        ("singleton", "INTEGER", 0, None, 1, 0),
        ("source_run_id", "TEXT", 1, None, 0, 0),
        ("source_run_binding_sha256", "TEXT", 1, None, 0, 0),
        ("environment_identity", "TEXT", 1, None, 0, 0),
        ("run_binding_document", "BLOB", 1, None, 0, 0),
        ("bound_at_utc", "TEXT", 1, None, 0, 0),
    ),
    _EVIDENCE_TABLE: (
        ("readiness_check_id", "TEXT", 1, None, 1, 0),
        ("source_run_id", "TEXT", 1, None, 0, 0),
        ("window_index", "INTEGER", 1, None, 0, 0),
        ("payload_document", "BLOB", 1, None, 0, 0),
        ("canonical_document_digest", "TEXT", 1, None, 0, 0),
        ("health_source_document_bytes", "BLOB", 1, None, 0, 0),
        ("rollback_source_document_bytes", "BLOB", 1, None, 0, 0),
    ),
}


def _normalize_schema_sql(sql: str) -> str:
    """LKG-local DDL comparison, preserving quoted literal case and whitespace."""

    sql = sql.strip().removesuffix(";").rstrip()
    quoted = r"('(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|`(?:``|[^`])*`|\[[^\]]*\])"
    return "".join(
        part if index % 2 else re.sub(r"\s+", " ", part)
        for index, part in enumerate(re.split(quoted, sql))
    )


def _provider_error(code: str) -> LkgWindowOperationalReadinessProviderError:
    return LkgWindowOperationalReadinessProviderError(code)


class LkgWindowReadinessObserver(Protocol):
    """The single zero-actuation observation port.

    Exactly one ``observe`` call happens per logical readiness check, and
    it happens inside the provider's write transaction. Counting calls on
    a test double is therefore a direct check of ADR-020 section 21.
    """

    def observe(
        self,
        *,
        source_run_id: str,
        source_run_binding_sha256: str,
        window_index: int,
        readiness_check_id: str,
    ) -> tuple[LkgWindowHealthObservation, LkgWindowRollbackReadiness]: ...


class SqliteLkgWindowOperationalReadinessProvider:
    """Restart-durable production readiness provider for one LKG run."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        run_binding: LkgRunBinding,
        observer: LkgWindowReadinessObserver,
        clock: Callable[[], str],
        monotonic_ns: Callable[[], int],
        lock_timeout_seconds: float = 30.0,
    ) -> None:
        if not isinstance(run_binding, LkgRunBinding):
            raise ContractViolation("run_binding must be an LkgRunBinding")
        if not callable(clock) or not callable(monotonic_ns):
            raise ContractViolation("clock and monotonic_ns must be callable")
        if not hasattr(observer, "observe"):
            raise ContractViolation("observer must provide observe()")
        if type(lock_timeout_seconds) is not float or not (
            0.0 < lock_timeout_seconds <= 600.0
        ):
            raise ContractViolation("lock_timeout_seconds must be a float in (0, 600]")

        self.path = Path(path)
        self._run_binding = run_binding
        self._run_binding_document = lkg_run_binding_document(run_binding)
        self._run_binding_sha256 = lkg_run_binding_sha256(run_binding)
        self._observer = observer
        self._clock = clock
        self._monotonic_ns = monotonic_ns
        self._lock_timeout_seconds = lock_timeout_seconds

        self._validate_path()
        try:
            with self._session() as connection:
                self._initialize_schema(connection)
            self._enforce_private_database_mode()
        except LkgWindowOperationalReadinessProviderError:
            raise
        except sqlite3.OperationalError as exc:
            raise _provider_error("READINESS_STORE_UNAVAILABLE") from exc
        except sqlite3.DatabaseError as exc:
            raise _provider_error("READINESS_STORE_CORRUPTED") from exc
        except (OSError, sqlite3.Error) as exc:
            raise _provider_error("READINESS_STORE_UNAVAILABLE") from exc

    # -- public API ----------------------------------------------------

    def capture_or_return(
        self,
        *,
        readiness_check_id: str,
        source_run_id: str,
        source_run_binding_sha256: str,
        window_index: int,
        epoch_index: int,
        first_attempt_sequence: int,
        last_attempt_sequence: int,
    ) -> LkgWindowOperationalReadinessEvidence:
        """Pre-seal capture, or return the committed historical result.

        The observation happens inside this method's write transaction,
        so two concurrent callers for one window produce exactly one
        external observation and exactly one durable row.
        """

        self._require_window_context(
            window_index=window_index,
            epoch_index=epoch_index,
            first_attempt_sequence=first_attempt_sequence,
            last_attempt_sequence=last_attempt_sequence,
        )
        if not isinstance(source_run_id, str) or not source_run_id:
            raise ContractViolation("source_run_id must be a non-empty string")
        if _SHA256_RE.fullmatch(source_run_binding_sha256 or "") is None:
            raise ContractViolation("source_run_binding_sha256 must be a sha256 hex")
        if not isinstance(readiness_check_id, str) or not readiness_check_id:
            raise ContractViolation("readiness_check_id must be a non-empty string")

        # ADR-020 section 22: refuse a non-canonical id BEFORE observing.
        expected_check_id = derive_lkg_window_readiness_check_id(
            source_run_id=source_run_id,
            source_run_binding_sha256=source_run_binding_sha256,
            window_index=window_index,
        )
        if readiness_check_id != expected_check_id:
            raise _provider_error("NONCANONICAL_READINESS_CHECK_ID")

        context = (
            source_run_id,
            window_index,
            epoch_index,
            first_attempt_sequence,
            last_attempt_sequence,
            source_run_binding_sha256,
        )

        try:
            with self._session() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._bind_or_validate_locked(connection)
                    if source_run_id != self._run_binding.run_id:
                        raise _provider_error("READINESS_STORE_SOURCE_RUN_MISMATCH")
                    if source_run_binding_sha256 != self._run_binding_sha256:
                        raise _provider_error("READINESS_STORE_BINDING_MISMATCH")

                    existing = self._evidence_by_check_id_locked(
                        connection, readiness_check_id
                    )
                    if existing is not None:
                        existing_context = (
                            existing.source_run_id,
                            existing.window_index,
                            existing.epoch_index,
                            existing.first_attempt_sequence,
                            existing.last_attempt_sequence,
                            existing.source_run_binding_sha256,
                        )
                        if existing_context != context:
                            raise _provider_error(
                                "READINESS_CHECK_ID_CONFLICTING_RESULT"
                            )
                        connection.commit()
                        return existing

                    row = connection.execute(
                        f"SELECT readiness_check_id FROM {_EVIDENCE_TABLE} "
                        "WHERE source_run_id=? AND window_index=?",
                        (source_run_id, window_index),
                    ).fetchone()
                    if row is not None and row[0] != readiness_check_id:
                        raise _provider_error("READINESS_WINDOW_ALREADY_CHECKED")

                    # -- the ONE logical observation, under the write lock
                    check_start_ns = self._exact_ns(self._monotonic_ns())
                    health, rollback = self._observer.observe(
                        source_run_id=source_run_id,
                        source_run_binding_sha256=source_run_binding_sha256,
                        window_index=window_index,
                        readiness_check_id=readiness_check_id,
                    )
                    check_end_ns = self._exact_ns(self._monotonic_ns())
                    if check_end_ns < check_start_ns:
                        raise _provider_error("READINESS_OBSERVATION_INVALID")
                    health, health_bytes = validate_lkg_window_health_observation(
                        health, source_identity=LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY,
                        source_run_id=source_run_id,
                        source_run_binding_sha256=source_run_binding_sha256,
                        run_bound_environment_identity=self._run_binding.environment_identity,
                    )
                    rollback, rollback_bytes = validate_lkg_window_rollback_readiness(
                        rollback, source_identity=LKG_ROLLBACK_READINESS_SOURCE_IDENTITY,
                        source_run_id=source_run_id,
                        source_run_binding_sha256=source_run_binding_sha256,
                    )

                    evidence = self._build_evidence(
                        readiness_check_id=readiness_check_id,
                        source_run_id=source_run_id,
                        source_run_binding_sha256=source_run_binding_sha256,
                        window_index=window_index,
                        epoch_index=epoch_index,
                        first_attempt_sequence=first_attempt_sequence,
                        last_attempt_sequence=last_attempt_sequence,
                        health=health,
                        rollback=rollback,
                        check_start_ns=check_start_ns,
                        check_end_ns=check_end_ns,
                    )
                    self._insert_evidence_locked(connection, evidence, health_bytes, rollback_bytes)
                    connection.commit()
                    return evidence
                except BaseException:
                    self._rollback_quietly(connection)
                    raise
        except (
            LkgWindowOperationalReadinessProviderError,
            LkgWindowReadinessObservationError,
            ContractViolation,
        ):
            raise
        except sqlite3.OperationalError as exc:
            raise _provider_error("READINESS_STORE_UNAVAILABLE") from exc
        except sqlite3.DatabaseError as exc:
            raise _provider_error("READINESS_STORE_CORRUPTED") from exc
        except (OSError, sqlite3.Error) as exc:
            raise _provider_error("READINESS_STORE_UNAVAILABLE") from exc

    def lookup(
        self, *, readiness_check_id: str
    ) -> LkgWindowOperationalReadinessEvidence:
        """Post-seal retrieval. Performs NO observation, ever."""

        if not isinstance(readiness_check_id, str) or not readiness_check_id:
            raise ContractViolation("readiness_check_id must be a non-empty string")
        try:
            with self._session() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._bind_or_validate_locked(connection)
                    evidence = self._evidence_by_check_id_locked(
                        connection, readiness_check_id
                    )
                    connection.commit()
                except BaseException:
                    self._rollback_quietly(connection)
                    raise
        except LkgWindowOperationalReadinessProviderError:
            raise
        except (sqlite3.Error, OSError, ContractViolation) as exc:
            raise _provider_error("RESULT_NOT_RECOVERABLE") from exc
        if evidence is None:
            raise _provider_error("RESULT_NOT_RECOVERABLE")
        return evidence

    # -- evidence construction -----------------------------------------

    @staticmethod
    def _exact_ns(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise _provider_error("READINESS_OBSERVATION_INVALID")
        return value

    def _build_evidence(
        self,
        *,
        readiness_check_id: str,
        source_run_id: str,
        source_run_binding_sha256: str,
        window_index: int,
        epoch_index: int,
        first_attempt_sequence: int,
        last_attempt_sequence: int,
        health: LkgWindowHealthObservation,
        rollback: LkgWindowRollbackReadiness,
        check_start_ns: int,
        check_end_ns: int,
    ) -> LkgWindowOperationalReadinessEvidence:
        checked_at_utc = self._clock()
        provider_run_id = derive_lkg_window_provider_run_id(
            readiness_check_id=readiness_check_id,
            source_run_id=source_run_id,
            source_run_binding_sha256=source_run_binding_sha256,
        )
        reason_codes = tuple(sorted(set(health.reason_codes) | set(rollback.reason_codes)))
        payload = {
            "readiness_schema_version": READINESS_SCHEMA_VERSION,
            "source_run_id": source_run_id,
            "source_run_binding_sha256": source_run_binding_sha256,
            "window_index": window_index,
            "epoch_index": epoch_index,
            "first_attempt_sequence": first_attempt_sequence,
            "last_attempt_sequence": last_attempt_sequence,
            "readiness_check_id": readiness_check_id,
            "provider_run_id": provider_run_id,
            # health_checked is true whenever a trustworthy observation was
            # obtained at all; an inability never reaches this point.
            "health_checked": True,
            "health_passed": bool(health.passed),
            "health_evidence_source_identity": LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY,
            "health_evidence_source_digest": health.digest,
            "rollback_tested": True,
            "rollback_ready": bool(rollback.ready),
            "rollback_evidence_source_identity": (
                LKG_ROLLBACK_READINESS_SOURCE_IDENTITY
            ),
            "rollback_evidence_source_digest": rollback.digest,
            "checked_at_utc": checked_at_utc,
            "check_start_ns": check_start_ns,
            "check_end_ns": check_end_ns,
            "reason_codes": list(reason_codes),
        }
        digest = readiness_payload_document_digest(payload)
        return lkg_window_operational_readiness_evidence_from_payload(
            payload, canonical_document_digest=digest
        )

    @staticmethod
    def _require_window_context(
        *,
        window_index: int,
        epoch_index: int,
        first_attempt_sequence: int,
        last_attempt_sequence: int,
    ) -> None:
        for name, value in (
            ("window_index", window_index),
            ("epoch_index", epoch_index),
            ("first_attempt_sequence", first_attempt_sequence),
            ("last_attempt_sequence", last_attempt_sequence),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ContractViolation(f"{name} must be a non-negative int")
        if not 0 <= window_index < _WINDOWS_PER_RUN:
            raise ContractViolation(f"window_index must be in [0, {_WINDOWS_PER_RUN})")
        if epoch_index != window_index // _WINDOWS_PER_EPOCH:
            raise ContractViolation("epoch_index must equal window_index // 6")
        if first_attempt_sequence != window_index * _POSITIONS_PER_WINDOW:
            raise ContractViolation("first_attempt_sequence must equal window_index * 200")
        if last_attempt_sequence != first_attempt_sequence + _POSITIONS_PER_WINDOW - 1:
            raise ContractViolation(
                "last_attempt_sequence must equal first_attempt_sequence + 199"
            )

    # -- storage -------------------------------------------------------

    def _validate_path(self) -> None:
        if self.path.exists() or self.path.is_symlink():
            try:
                info = os.lstat(self.path)
            except OSError as exc:
                raise _provider_error("READINESS_STORE_UNSAFE_PATH") from exc
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_uid != os.geteuid()
            ):
                raise _provider_error("READINESS_STORE_UNSAFE_PATH")

    def _enforce_private_database_mode(self) -> None:
        try:
            os.chmod(self.path, 0o600)
        except OSError as exc:
            raise _provider_error("READINESS_STORE_UNAVAILABLE") from exc

    def _connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path, timeout=self._lock_timeout_seconds, isolation_level=None
        )
        try:
            connection.execute(
                f"PRAGMA busy_timeout = {int(self._lock_timeout_seconds * 1000)}"
            )
            connection.execute("PRAGMA trusted_schema = OFF")
            connection.execute("PRAGMA foreign_keys = ON")
            # Never change the persistent journal mode of a refused database.
            if connection.execute("PRAGMA user_version").fetchone()[0] == 1:
                raise _provider_error("READINESS_STORE_V1_NOT_SUPPORTED")
            journal = connection.execute("PRAGMA journal_mode").fetchone()
            if journal is None or str(journal[0]).lower() != "delete":
                raise _provider_error("READINESS_STORE_UNAVAILABLE")
            connection.execute("PRAGMA synchronous = FULL")
        except BaseException:
            connection.close()
            raise
        return connection

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        connection = self._connection()
        try:
            yield connection
        finally:
            connection.close()

    def _initialize_schema(self, connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            objects = connection.execute("SELECT name FROM sqlite_schema").fetchall()
            fresh = version == 0 and not objects
            if fresh:
                for sql in (*_TABLE_SQL.values(), *_TRIGGER_SQL.values()):
                    connection.execute(sql)
                connection.execute(
                    f"PRAGMA user_version = {LKG_READINESS_STORE_SCHEMA_VERSION}"
                )
            self._verify_schema(connection)
            if connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise _provider_error("READINESS_STORE_CORRUPTED")
            self._bind_or_validate_locked(connection, allow_create=fresh)
            # At most twelve rows in a valid run. Verify retained documents on
            # reopen, not just when a particular window is subsequently requested.
            for (check_id,) in connection.execute(
                f"SELECT readiness_check_id FROM {_EVIDENCE_TABLE}"
            ).fetchall():
                self._evidence_by_check_id_locked(connection, check_id)
            connection.commit()
        except BaseException:
            self._rollback_quietly(connection)
            raise

    @staticmethod
    def _verify_schema(connection: sqlite3.Connection) -> None:
        """Exact inventory, DDL and PRAGMA structure; no repair or migration."""

        if connection.execute("PRAGMA user_version").fetchone()[0] != 2:
            raise _provider_error("READINESS_STORE_CORRUPTED")
        actual = {
            (kind, name, table): _normalize_schema_sql(sql)
            for kind, name, table, sql in connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_schema "
                "WHERE name NOT GLOB 'sqlite_*'"
            )
        }
        expected = {
            ("table", name, name): _normalize_schema_sql(sql)
            for name, sql in _TABLE_SQL.items()
        }
        expected.update({
            ("trigger", name, name.rsplit("_no_", 1)[0]): _normalize_schema_sql(sql)
            for name, sql in _TRIGGER_SQL.items()
        })
        if actual != expected:
            raise _provider_error("READINESS_STORE_CORRUPTED")
        for table, columns in _TABLE_COLUMNS.items():
            actual_columns = tuple(row[1:] for row in connection.execute(f"PRAGMA table_xinfo({table})"))
            table_info = [row for row in connection.execute("PRAGMA table_list")
                          if row[0] == "main" and row[1] == table]
            if actual_columns != columns or table_info != [("main", table, "table", len(columns), 0, 1)]:
                raise _provider_error("READINESS_STORE_CORRUPTED")
            indexes = {row[1]: row[2:] for row in connection.execute(f"PRAGMA index_list({table})")}
            expected_indexes = {} if table == _BINDING_TABLE else {
                f"sqlite_autoindex_{table}_1": (1, "pk", 0),
                f"sqlite_autoindex_{table}_2": (1, "u", 0),
            }
            if indexes != expected_indexes:
                raise _provider_error("READINESS_STORE_CORRUPTED")
            for name in indexes:
                expected_names = ("readiness_check_id",) if name.endswith("_1") else ("source_run_id", "window_index")
                if tuple(row[2] for row in connection.execute(f"PRAGMA index_info({name})")) != expected_names:
                    raise _provider_error("READINESS_STORE_CORRUPTED")

    def _bind_or_validate_locked(self, connection: sqlite3.Connection, *, allow_create: bool = False) -> None:
        """Persist the COMPLETE run binding once; re-validate every open."""

        self._verify_schema(connection)
        row = connection.execute(
            f"SELECT source_run_id, source_run_binding_sha256, environment_identity, "
            f"run_binding_document FROM {_BINDING_TABLE} WHERE singleton=1"
        ).fetchone()
        if row is None:
            if not allow_create:
                raise _provider_error("READINESS_STORE_CORRUPTED")
            connection.execute(
                f"INSERT INTO {_BINDING_TABLE} (singleton, source_run_id, "
                "source_run_binding_sha256, environment_identity, "
                "run_binding_document, bound_at_utc) VALUES (1,?,?,?,?,?)",
                (
                    self._run_binding.run_id,
                    self._run_binding_sha256,
                    self._run_binding.environment_identity,
                    canonical_json_bytes(self._run_binding_document),
                    self._clock(),
                ),
            )
            return
        stored_run_id, stored_digest, stored_env, stored_document = row
        if stored_run_id != self._run_binding.run_id:
            raise _provider_error("READINESS_STORE_SOURCE_RUN_MISMATCH")
        if (
            stored_digest != self._run_binding_sha256
            or stored_env != self._run_binding.environment_identity
        ):
            raise _provider_error("READINESS_STORE_BINDING_MISMATCH")
        try:
            document = json.loads(bytes(stored_document).decode("utf-8"))
            rebuilt = lkg_run_binding_from_document(document)
        except (ContractViolation, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise _provider_error("READINESS_STORE_CORRUPTED") from exc
        if (
            lkg_run_binding_sha256(rebuilt) != self._run_binding_sha256
            or canonical_json_bytes(lkg_run_binding_document(rebuilt))
            != canonical_json_bytes(self._run_binding_document)
        ):
            raise _provider_error("READINESS_STORE_BINDING_MISMATCH")

    def _evidence_by_check_id_locked(
        self, connection: sqlite3.Connection, readiness_check_id: str
    ) -> LkgWindowOperationalReadinessEvidence | None:
        row = connection.execute(
            f"SELECT payload_document, canonical_document_digest, "
            f"health_source_document_bytes, rollback_source_document_bytes, "
            f"source_run_id, window_index FROM {_EVIDENCE_TABLE} "
            "WHERE readiness_check_id=?",
            (readiness_check_id,),
        ).fetchone()
        if row is None:
            return None
        payload_blob, stored_digest, health_blob, rollback_blob, stored_run, stored_window = row
        try:
            payload = json.loads(bytes(payload_blob).decode("utf-8"))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise _provider_error("RESULT_NOT_RECOVERABLE") from exc
        if not isinstance(payload, dict):
            raise _provider_error("RESULT_NOT_RECOVERABLE")
        recomputed = readiness_payload_document_digest(payload)
        if recomputed != stored_digest:
            raise _provider_error("RESULT_NOT_RECOVERABLE")
        try:
            evidence = lkg_window_operational_readiness_evidence_from_payload(
                payload, canonical_document_digest=stored_digest
            )
            health_doc = self._canonical_source_document(health_blob)
            rollback_doc = self._canonical_source_document(rollback_blob)
            context = {"source_run_id": self._run_binding.run_id,
                       "source_run_binding_sha256": self._run_binding_sha256}
            health, _ = validate_lkg_window_health_observation(
                LkgWindowHealthObservation(health_doc, evidence.health_evidence_source_digest,
                                           evidence.health_passed, tuple(health_doc["reason_codes"])),
                source_identity=evidence.health_evidence_source_identity,
                run_bound_environment_identity=self._run_binding.environment_identity, **context,
            )
            rollback, _ = validate_lkg_window_rollback_readiness(
                LkgWindowRollbackReadiness(rollback_doc, evidence.rollback_evidence_source_digest,
                                           evidence.rollback_ready, tuple(rollback_doc["reason_codes"])),
                source_identity=evidence.rollback_evidence_source_identity, **context,
            )
            if (
                evidence.reason_codes != tuple(sorted(set(health.reason_codes) | set(rollback.reason_codes)))
                or evidence.health_checked is not True or evidence.rollback_tested is not True
                or evidence.source_run_id != self._run_binding.run_id
                or evidence.source_run_binding_sha256 != self._run_binding_sha256
                or evidence.source_run_id != stored_run or evidence.window_index != stored_window
                or evidence.readiness_check_id != readiness_check_id
                or readiness_check_id != derive_lkg_window_readiness_check_id(
                    **context, window_index=evidence.window_index
                )
            ):
                raise _provider_error("RESULT_NOT_RECOVERABLE")
            return evidence
        except (ContractViolation, KeyError, TypeError, ValueError, RecursionError) as exc:
            raise _provider_error("RESULT_NOT_RECOVERABLE") from exc

    @staticmethod
    def _canonical_source_document(raw: object) -> dict:
        if type(raw) is not bytes:
            raise ContractViolation("readiness source preimage must be a BLOB")
        document = json.loads(raw.decode("utf-8"))
        if type(document) is not dict or canonical_json_bytes(document) != raw:
            raise ContractViolation("readiness source preimage is not canonical JSON")
        return document

    def _insert_evidence_locked(
        self,
        connection: sqlite3.Connection,
        evidence: LkgWindowOperationalReadinessEvidence,
        health_bytes: bytes,
        rollback_bytes: bytes,
    ) -> None:
        from .lkg_window_readiness import readiness_payload_document

        payload = readiness_payload_document(evidence)
        try:
            connection.execute(
                f"INSERT INTO {_EVIDENCE_TABLE} (readiness_check_id, source_run_id, "
                "window_index, payload_document, canonical_document_digest, "
                "health_source_document_bytes, rollback_source_document_bytes) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    evidence.readiness_check_id,
                    evidence.source_run_id,
                    evidence.window_index,
                    canonical_json_bytes(payload),
                    evidence.canonical_document_digest,
                    health_bytes,
                    rollback_bytes,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise _provider_error("READINESS_WINDOW_ALREADY_CHECKED") from exc

    @staticmethod
    def _rollback_quietly(connection: sqlite3.Connection) -> None:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
