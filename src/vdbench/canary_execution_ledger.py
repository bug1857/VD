"""Restart-durable, append-only Stage-4 execution ledger for EXP-009.

Purpose:
    Record an attempted immutable 1,200-slot Stage-4 schedule in strict order
    without dispatching, retrying, or interpreting any query.
Inputs:
    A validated ``Stage4ExecutionSchedule``, a canonical run ID, and compact
    non-sensitive per-slot observations supplied by a later serial runner.
Outputs:
    Immutable hash-chained records, restart-safe progress, and explicit
    fail-closed refusals for invalid continuation attempts.
Dependencies:
    The standard-library SQLite implementation and the pure schedule values.
    This module has no Milvus, serving executor, approval, route authority,
    query-source, policy, or activation dependency.
Complexity:
    Each append revalidates at most 1,200 records, so this bounded reference
    contract is O(1,200) time and O(1,200) storage per run.
Failure modes:
    Corrupt/unavailable storage raises ``Stage4LedgerError``.  Invalid next
    observations receive stable refusal codes and are not persisted.  A safely
    persisted failed outcome terminally blocks further slots.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
from typing import Any, Iterator
import unicodedata

from .artifacts import canonical_json_bytes
from .canary_schedule import Stage4ExecutionSchedule


__all__ = [
    "Stage4ExecutionLedger",
    "Stage4ExecutionRecord",
    "Stage4LedgerAppendResult",
    "Stage4LedgerError",
    "Stage4LedgerProgress",
    "Stage4LedgerStatus",
    "Stage4SlotObservation",
]


_SCHEMA_VERSION = 1
_RECORD_SCHEMA_VERSION = "exp009-stage4-execution-record-v1"
_GENESIS_DOMAIN = b"vdbench.exp009.stage4-ledger-genesis/v1\0"
_RECORD_DOMAIN = b"vdbench.exp009.stage4-ledger-record/v1\0"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_CODE_RE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_EXPECTED_SCHEMA_OBJECTS = frozenset(
    {
        "execution_run",
        "execution_records",
        "execution_run_no_delete",
        "execution_run_no_update",
        "execution_records_no_delete",
        "execution_records_no_update",
    }
)
_RECORD_FIELDS = frozenset(
    {
        "schema_version",
        "run_id",
        "schedule_sha256",
        "execution_index",
        "observed_ef",
        "started_monotonic_ns",
        "finished_monotonic_ns",
        "recorded_at_utc",
        "success",
        "timed_out",
        "threshold_semantics_valid",
        "health_before_ok",
        "health_after_ok",
        "identity_before_ok",
        "identity_after_ok",
        "result_count",
        "latency_ms",
        "reason_code",
    }
)


class Stage4LedgerError(RuntimeError):
    """A durable ledger condition that must prevent Stage-4 continuation."""


class Stage4LedgerStatus(StrEnum):
    """The only derived statuses of one immutable schedule ledger."""

    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True, slots=True)
class Stage4SlotObservation:
    """One compact, raw-payload-free outcome supplied after a scheduled slot."""

    execution_index: int
    observed_ef: int
    started_monotonic_ns: int
    finished_monotonic_ns: int
    recorded_at_utc: str
    success: bool
    timed_out: bool
    threshold_semantics_valid: bool
    health_before_ok: bool
    health_after_ok: bool
    identity_before_ok: bool
    identity_after_ok: bool
    result_count: int
    latency_ms: float
    reason_code: str | None

    def __post_init__(self) -> None:
        _observation_document(self)


@dataclass(frozen=True, slots=True)
class Stage4ExecutionRecord:
    """One immutable persisted observation plus its predecessor-chain binding."""

    observation: Stage4SlotObservation
    previous_record_sha256: str
    record_sha256: str


@dataclass(frozen=True, slots=True)
class Stage4LedgerAppendResult:
    """An append outcome; a refusal has no durable record."""

    accepted: bool
    reason_code: str | None
    record_sha256: str | None


@dataclass(frozen=True, slots=True)
class Stage4LedgerProgress:
    """Restart-safe derived status of one bounded serial schedule."""

    status: Stage4LedgerStatus
    record_count: int
    reason_code: str | None
    chain_head_sha256: str


class _DuplicateJsonField(ValueError):
    """Internal marker for duplicate fields in a stored JSON record."""


def _no_duplicate_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(key)
        result[key] = value
    return result


class Stage4ExecutionLedger:
    """Single-host strict SQLite ledger with no query or route capability."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        run_id: str,
        schedule: Stage4ExecutionSchedule,
        lock_timeout_seconds: float = 1.0,
    ) -> None:
        self.path = Path(path)
        self._run_id = _canonical_text(run_id, field="run_id")
        if not isinstance(schedule, Stage4ExecutionSchedule):
            raise TypeError("schedule must be a Stage4ExecutionSchedule")
        self._schedule = schedule
        self._lock_timeout_seconds = _lock_timeout(lock_timeout_seconds)
        self._validate_path()
        try:
            with self._session() as connection:
                self._initialize_schema(connection)
                self._bind_or_validate_run(connection)
                self._load_verified_records(connection)
            self._enforce_private_database_mode()
        except Stage4LedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("LEDGER_STORE_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("LEDGER_STORE_CORRUPTED", exc) from exc
        except (OSError, UnicodeError) as exc:
            raise _ledger_error("LEDGER_STORE_UNAVAILABLE", exc) from exc

    def append(self, observation: object) -> Stage4LedgerAppendResult:
        """Append exactly one safe next slot, or refuse without a write.

        A persisted failed observation is retained as evidence but leaves the
        run terminally failed.  The caller cannot use this boundary to retry,
        skip, reorder, or resume the schedule.
        """

        if not isinstance(observation, Stage4SlotObservation):
            return Stage4LedgerAppendResult(False, "OBSERVATION_INVALID", None)
        try:
            _observation_document(observation)
            with self._session() as connection:
                connection.execute("BEGIN IMMEDIATE")
                self._require_current_schema(connection)
                self._require_matching_run(connection)
                records, genesis = self._load_verified_records(connection)
                progress = _progress(records, genesis, len(self._schedule.steps))
                if progress.status is not Stage4LedgerStatus.IN_PROGRESS:
                    connection.commit()
                    return Stage4LedgerAppendResult(False, "RUN_NOT_ACTIVE", None)
                expected_index = len(records)
                if observation.execution_index != expected_index:
                    connection.commit()
                    return Stage4LedgerAppendResult(
                        False, "EXECUTION_INDEX_UNEXPECTED", None
                    )
                expected_step = self._schedule.steps[expected_index]
                if observation.observed_ef != expected_step.expected_ef:
                    connection.commit()
                    return Stage4LedgerAppendResult(False, "OBSERVED_EF_MISMATCH", None)
                if records and observation.started_monotonic_ns <= records[-1].observation.finished_monotonic_ns:
                    connection.commit()
                    return Stage4LedgerAppendResult(
                        False, "MONOTONIC_INTERVAL_VIOLATION", None
                    )
                previous = progress.chain_head_sha256
                payload = _record_document(
                    run_id=self._run_id,
                    schedule_sha256=self._schedule.schedule_sha256,
                    observation=observation,
                )
                record_sha256 = _record_sha256(previous, payload)
                serialized = json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
                connection.execute(
                    """
                    INSERT INTO execution_records(
                        execution_index, previous_record_sha256, record_sha256, record_json
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        observation.execution_index,
                        previous,
                        record_sha256,
                        serialized,
                    ),
                )
                connection.commit()
                return Stage4LedgerAppendResult(True, None, record_sha256)
        except Stage4LedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("LEDGER_STORE_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("LEDGER_STORE_CORRUPTED", exc) from exc

    def progress(self) -> Stage4LedgerProgress:
        """Return verified derived state; corruption is never treated as empty."""

        try:
            with self._session() as connection:
                self._require_current_schema(connection)
                self._require_matching_run(connection)
                records, genesis = self._load_verified_records(connection)
                return _progress(records, genesis, len(self._schedule.steps))
        except Stage4LedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("LEDGER_STORE_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("LEDGER_STORE_CORRUPTED", exc) from exc

    def records(self) -> tuple[Stage4ExecutionRecord, ...]:
        """Return the validated immutable ledger history in execution order."""

        try:
            with self._session() as connection:
                self._require_current_schema(connection)
                self._require_matching_run(connection)
                records, _ = self._load_verified_records(connection)
                return records
        except Stage4LedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("LEDGER_STORE_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("LEDGER_STORE_CORRUPTED", exc) from exc

    def _validate_path(self) -> None:
        parent = self.path.parent
        try:
            parent_lstat = parent.lstat()
            parent_stat = parent.stat()
        except OSError as exc:
            raise _ledger_error("LEDGER_DIRECTORY_UNAVAILABLE", exc) from exc
        if (
            stat.S_ISLNK(parent_lstat.st_mode)
            or not stat.S_ISDIR(parent_stat.st_mode)
            or stat.S_IMODE(parent_stat.st_mode) & 0o077
        ):
            raise _ledger_error("LEDGER_DIRECTORY_NOT_PRIVATE")
        try:
            file_stat = self.path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise _ledger_error("LEDGER_STORE_UNAVAILABLE", exc) from exc
        if not stat.S_ISREG(file_stat.st_mode) or stat.S_ISLNK(file_stat.st_mode):
            raise _ledger_error("LEDGER_PATH_INVALID")

    def _enforce_private_database_mode(self) -> None:
        try:
            os.chmod(self.path, 0o600)
            if stat.S_IMODE(self.path.stat().st_mode) & 0o077:
                raise _ledger_error("LEDGER_FILE_NOT_PRIVATE")
        except Stage4LedgerError:
            raise
        except OSError as exc:
            raise _ledger_error("LEDGER_STORE_UNAVAILABLE", exc) from exc

    def _connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self._lock_timeout_seconds,
            isolation_level=None,
        )
        try:
            connection.execute(
                f"PRAGMA busy_timeout = {int(self._lock_timeout_seconds * 1000)}"
            )
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA trusted_schema = OFF")
            journal_mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()
            if journal_mode is None or journal_mode[0] != "delete":
                raise _ledger_error("LEDGER_JOURNAL_MODE_INVALID")
            connection.execute("PRAGMA synchronous = FULL")
            return connection
        except BaseException:
            connection.close()
            raise

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        connection = self._connection()
        try:
            yield connection
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _initialize_schema(connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            version = connection.execute("PRAGMA user_version").fetchone()
            if version is None or type(version[0]) is not int:
                raise _ledger_error("LEDGER_STORE_CORRUPTED")
            objects = _schema_objects(connection)
            if version[0] == 0 and not objects:
                connection.execute(
                    """
                    CREATE TABLE execution_run (
                        singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
                        schema_version INTEGER NOT NULL,
                        run_id TEXT UNIQUE NOT NULL,
                        schedule_sha256 TEXT NOT NULL,
                        genesis_record_sha256 TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE execution_records (
                        execution_index INTEGER PRIMARY KEY NOT NULL,
                        previous_record_sha256 TEXT NOT NULL,
                        record_sha256 TEXT UNIQUE NOT NULL,
                        record_json TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER execution_run_no_update
                    BEFORE UPDATE ON execution_run
                    BEGIN SELECT RAISE(ABORT, 'execution run is append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER execution_run_no_delete
                    BEFORE DELETE ON execution_run
                    BEGIN SELECT RAISE(ABORT, 'execution run is append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER execution_records_no_update
                    BEFORE UPDATE ON execution_records
                    BEGIN SELECT RAISE(ABORT, 'execution records are append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER execution_records_no_delete
                    BEFORE DELETE ON execution_records
                    BEGIN SELECT RAISE(ABORT, 'execution records are append-only'); END
                    """
                )
                connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            elif version[0] != _SCHEMA_VERSION:
                raise _ledger_error("LEDGER_SCHEMA_MISMATCH")
            if not _schema_matches(connection):
                raise _ledger_error("LEDGER_SCHEMA_MISMATCH")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def _bind_or_validate_run(self, connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            row = connection.execute(
                "SELECT schema_version, run_id, schedule_sha256, genesis_record_sha256 FROM execution_run"
            ).fetchone()
            if row is None:
                genesis = _genesis_sha256(
                    run_id=self._run_id,
                    schedule_sha256=self._schedule.schedule_sha256,
                )
                connection.execute(
                    """
                    INSERT INTO execution_run(
                        singleton, schema_version, run_id, schedule_sha256, genesis_record_sha256
                    ) VALUES (1, ?, ?, ?, ?)
                    """,
                    (
                        _SCHEMA_VERSION,
                        self._run_id,
                        self._schedule.schedule_sha256,
                        genesis,
                    ),
                )
            else:
                self._validate_run_row(row)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def _require_current_schema(self, connection: sqlite3.Connection) -> None:
        if not _schema_matches(connection):
            raise _ledger_error("LEDGER_SCHEMA_MISMATCH")

    def _require_matching_run(self, connection: sqlite3.Connection) -> None:
        row = connection.execute(
            "SELECT schema_version, run_id, schedule_sha256, genesis_record_sha256 FROM execution_run"
        ).fetchone()
        if row is None:
            raise _ledger_error("LEDGER_STORE_CORRUPTED")
        self._validate_run_row(row)

    def _validate_run_row(self, row: object) -> None:
        if not isinstance(row, tuple) or len(row) != 4:
            raise _ledger_error("LEDGER_STORE_CORRUPTED")
        version, run_id, schedule_sha256, genesis = row
        expected_genesis = _genesis_sha256(
            run_id=self._run_id,
            schedule_sha256=self._schedule.schedule_sha256,
        )
        if (
            version != _SCHEMA_VERSION
            or run_id != self._run_id
            or schedule_sha256 != self._schedule.schedule_sha256
            or genesis != expected_genesis
        ):
            raise _ledger_error("LEDGER_SCHEDULE_MISMATCH")

    def _load_verified_records(
        self, connection: sqlite3.Connection
    ) -> tuple[tuple[Stage4ExecutionRecord, ...], str]:
        row = connection.execute(
            "SELECT schema_version, run_id, schedule_sha256, genesis_record_sha256 FROM execution_run"
        ).fetchone()
        if row is None:
            raise _ledger_error("LEDGER_STORE_CORRUPTED")
        self._validate_run_row(row)
        genesis = row[3]
        if not _sha256(genesis):
            raise _ledger_error("LEDGER_STORE_CORRUPTED")
        rows = connection.execute(
            """
            SELECT execution_index, previous_record_sha256, record_sha256, record_json
            FROM execution_records ORDER BY execution_index ASC
            """
        ).fetchall()
        previous = genesis
        records: list[Stage4ExecutionRecord] = []
        for expected_index, row_value in enumerate(rows):
            record = _record_from_row(
                row_value,
                run_id=self._run_id,
                schedule_sha256=self._schedule.schedule_sha256,
                expected_index=expected_index,
                previous=previous,
            )
            expected_step = self._schedule.steps[expected_index]
            if record.observation.observed_ef != expected_step.expected_ef:
                raise _ledger_error("LEDGER_HISTORY_INVALID")
            if records and (
                record.observation.started_monotonic_ns
                <= records[-1].observation.finished_monotonic_ns
            ):
                raise _ledger_error("LEDGER_HISTORY_INVALID")
            if not _observation_safe(record.observation) and expected_index != len(rows) - 1:
                raise _ledger_error("LEDGER_HISTORY_INVALID")
            records.append(record)
            previous = record.record_sha256
        if len(records) > len(self._schedule.steps):
            raise _ledger_error("LEDGER_HISTORY_INVALID")
        return tuple(records), genesis


def _schema_objects(connection: sqlite3.Connection) -> frozenset[str]:
    return frozenset(
        row[0]
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%' AND type IN ('table', 'view', 'trigger')
            """
        )
    )


def _schema_matches(connection: sqlite3.Connection) -> bool:
    if _schema_objects(connection) != _EXPECTED_SCHEMA_OBJECTS:
        return False

    def columns(table: str) -> tuple[tuple[str, str, int, int], ...]:
        return tuple(
            (str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5]))
            for row in connection.execute(f"PRAGMA table_info({table})")
        )

    return (
        columns("execution_run")
        == (
            ("singleton", "INTEGER", 1, 1),
            ("schema_version", "INTEGER", 1, 0),
            ("run_id", "TEXT", 1, 0),
            ("schedule_sha256", "TEXT", 1, 0),
            ("genesis_record_sha256", "TEXT", 1, 0),
        )
        and columns("execution_records")
        == (
            ("execution_index", "INTEGER", 1, 1),
            ("previous_record_sha256", "TEXT", 1, 0),
            ("record_sha256", "TEXT", 1, 0),
            ("record_json", "TEXT", 1, 0),
        )
    )


def _record_from_row(
    row: object,
    *,
    run_id: str,
    schedule_sha256: str,
    expected_index: int,
    previous: str,
) -> Stage4ExecutionRecord:
    if not isinstance(row, tuple) or len(row) != 4:
        raise _ledger_error("LEDGER_STORE_CORRUPTED")
    execution_index, stored_previous, stored_sha256, serialized = row
    if (
        execution_index != expected_index
        or stored_previous != previous
        or not _sha256(stored_previous)
        or not _sha256(stored_sha256)
        or not isinstance(serialized, str)
    ):
        raise _ledger_error("LEDGER_HISTORY_INVALID")
    try:
        document = json.loads(serialized, object_pairs_hook=_no_duplicate_fields)
    except (json.JSONDecodeError, _DuplicateJsonField, TypeError, ValueError) as exc:
        raise _ledger_error("LEDGER_HISTORY_INVALID", exc) from exc
    observation = _observation_from_document(
        document,
        run_id=run_id,
        schedule_sha256=schedule_sha256,
    )
    expected_document = _record_document(
        run_id=run_id,
        schedule_sha256=schedule_sha256,
        observation=observation,
    )
    if document != expected_document or _record_sha256(previous, document) != stored_sha256:
        raise _ledger_error("LEDGER_HISTORY_INVALID")
    return Stage4ExecutionRecord(observation, stored_previous, stored_sha256)


def _observation_from_document(
    document: object,
    *,
    run_id: str,
    schedule_sha256: str,
) -> Stage4SlotObservation:
    if not isinstance(document, dict) or frozenset(document) != _RECORD_FIELDS:
        raise _ledger_error("LEDGER_HISTORY_INVALID")
    if (
        document["schema_version"] != _RECORD_SCHEMA_VERSION
        or document["run_id"] != run_id
        or document["schedule_sha256"] != schedule_sha256
    ):
        raise _ledger_error("LEDGER_HISTORY_INVALID")
    try:
        return Stage4SlotObservation(
            execution_index=document["execution_index"],
            observed_ef=document["observed_ef"],
            started_monotonic_ns=document["started_monotonic_ns"],
            finished_monotonic_ns=document["finished_monotonic_ns"],
            recorded_at_utc=document["recorded_at_utc"],
            success=document["success"],
            timed_out=document["timed_out"],
            threshold_semantics_valid=document["threshold_semantics_valid"],
            health_before_ok=document["health_before_ok"],
            health_after_ok=document["health_after_ok"],
            identity_before_ok=document["identity_before_ok"],
            identity_after_ok=document["identity_after_ok"],
            result_count=document["result_count"],
            latency_ms=document["latency_ms"],
            reason_code=document["reason_code"],
        )
    except (TypeError, ValueError) as exc:
        raise _ledger_error("LEDGER_HISTORY_INVALID", exc) from exc


def _record_document(
    *,
    run_id: str,
    schedule_sha256: str,
    observation: Stage4SlotObservation,
) -> dict[str, object]:
    values = _observation_document(observation)
    return {
        "schema_version": _RECORD_SCHEMA_VERSION,
        "run_id": run_id,
        "schedule_sha256": schedule_sha256,
        **values,
    }


def _observation_document(observation: object) -> dict[str, object]:
    if not isinstance(observation, Stage4SlotObservation):
        raise ValueError("observation is invalid")
    _nonnegative_integer(observation.execution_index, field="execution_index")
    _positive_integer(observation.observed_ef, field="observed_ef")
    start = _nonnegative_integer(
        observation.started_monotonic_ns, field="started_monotonic_ns"
    )
    finish = _nonnegative_integer(
        observation.finished_monotonic_ns, field="finished_monotonic_ns"
    )
    if finish <= start:
        raise ValueError("monotonic interval is invalid")
    recorded_at = _timestamp(observation.recorded_at_utc)
    booleans = (
        observation.success,
        observation.timed_out,
        observation.threshold_semantics_valid,
        observation.health_before_ok,
        observation.health_after_ok,
        observation.identity_before_ok,
        observation.identity_after_ok,
    )
    if not all(isinstance(value, bool) for value in booleans):
        raise ValueError("observation booleans are invalid")
    if observation.success and observation.timed_out:
        raise ValueError("successful observation cannot time out")
    _nonnegative_integer(observation.result_count, field="result_count")
    if (
        isinstance(observation.latency_ms, bool)
        or not isinstance(observation.latency_ms, (int, float))
        or not math.isfinite(float(observation.latency_ms))
        or float(observation.latency_ms) < 0.0
    ):
        raise ValueError("latency_ms is invalid")
    safe = _observation_safe(observation)
    if safe:
        if observation.reason_code is not None:
            raise ValueError("successful observation must not have a reason code")
    elif _reason_code(observation.reason_code) is None:
        raise ValueError("unsafe observation requires a stable reason code")
    return {
        "execution_index": observation.execution_index,
        "observed_ef": observation.observed_ef,
        "started_monotonic_ns": start,
        "finished_monotonic_ns": finish,
        "recorded_at_utc": recorded_at,
        "success": observation.success,
        "timed_out": observation.timed_out,
        "threshold_semantics_valid": observation.threshold_semantics_valid,
        "health_before_ok": observation.health_before_ok,
        "health_after_ok": observation.health_after_ok,
        "identity_before_ok": observation.identity_before_ok,
        "identity_after_ok": observation.identity_after_ok,
        "result_count": observation.result_count,
        "latency_ms": float(observation.latency_ms),
        "reason_code": observation.reason_code,
    }


def _observation_safe(observation: Stage4SlotObservation) -> bool:
    return (
        observation.success
        and not observation.timed_out
        and observation.threshold_semantics_valid
        and observation.health_before_ok
        and observation.health_after_ok
        and observation.identity_before_ok
        and observation.identity_after_ok
    )


def _progress(
    records: tuple[Stage4ExecutionRecord, ...], genesis: str, expected_count: int
) -> Stage4LedgerProgress:
    if not records:
        return Stage4LedgerProgress(Stage4LedgerStatus.IN_PROGRESS, 0, None, genesis)
    last = records[-1]
    if not _observation_safe(last.observation):
        return Stage4LedgerProgress(
            Stage4LedgerStatus.FAILED,
            len(records),
            last.observation.reason_code,
            last.record_sha256,
        )
    if len(records) == expected_count:
        return Stage4LedgerProgress(
            Stage4LedgerStatus.COMPLETE,
            len(records),
            None,
            last.record_sha256,
        )
    return Stage4LedgerProgress(
        Stage4LedgerStatus.IN_PROGRESS,
        len(records),
        None,
        last.record_sha256,
    )


def _genesis_sha256(*, run_id: str, schedule_sha256: str) -> str:
    return hashlib.sha256(
        _GENESIS_DOMAIN
        + canonical_json_bytes(
            {
                "schema_version": _RECORD_SCHEMA_VERSION,
                "run_id": run_id,
                "schedule_sha256": schedule_sha256,
            }
        )
    ).hexdigest()


def _record_sha256(previous: str, document: dict[str, object]) -> str:
    return hashlib.sha256(
        _RECORD_DOMAIN + previous.encode("ascii") + b"\0" + canonical_json_bytes(document)
    ).hexdigest()


def _canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or normalized != value
        or value.strip() != value
        or len(value) > 512
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError(f"{field} is not canonical")
    return value


def _timestamp(value: object) -> str:
    if not isinstance(value, str) or _UTC_RE.fullmatch(value) is None:
        raise ValueError("recorded_at_utc is invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("recorded_at_utc is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("recorded_at_utc is invalid")
    return value


def _reason_code(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _CODE_RE.fullmatch(value) is None:
        raise ValueError("reason_code is invalid")
    return value


def _sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _nonnegative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _positive_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _lock_timeout(value: object) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.001 <= float(value) <= 10.0
    ):
        raise ValueError("lock_timeout_seconds must be finite and between 0.001 and 10")
    return float(value)


def _ledger_error(code: str, cause: BaseException | None = None) -> Stage4LedgerError:
    error = Stage4LedgerError(code)
    if cause is not None:
        error.__cause__ = cause
    return error
