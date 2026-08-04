"""Restart-durable one-time approval-grant ledger for EXP-009 Stage 2.

Purpose:
    Reserve one verified signed-grant digest exactly once, then record at most
    one immutable terminal outcome for it.
Inputs:
    Canonical grant ID, signed-payload SHA-256, externally supplied RFC3339
    UTC timestamps, and a stable terminal reason code.
Outputs:
    Immutable reservation/terminal records or explicit fail-closed refusals.
Dependencies:
    Python's standard-library SQLite implementation only; never PyMilvus,
    routing, policy, or actuation.
Failure modes:
    Duplicate IDs/payloads refuse deterministically. Corrupt, unexpected,
    unavailable, or lock-contended storage raises ``GrantUseStoreError`` so a
    future coordinator cannot install a candidate route.

This module deliberately owns no route state. A later coordinator must compose
this ledger with approval verification, independent audit persistence, and an
atomic LKG-only route authority.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import hashlib
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
from typing import Final
import unicodedata


__all__ = [
    "CanaryGrantUseStore",
    "GrantUseRecord",
    "GrantUseResult",
    "GrantUseStatus",
    "GrantUseStoreError",
]


_SCHEMA_VERSION: Final = 1
_TERMINAL_RECORD_DOMAIN: Final = b"vdbench.canary-grant-terminal/v1\0"
_SHA256_RE: Final = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339_UTC_RE: Final = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_REASON_CODE_RE: Final = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_MAX_GRANT_ID_CODEPOINTS: Final = 512
_EXPECTED_TABLES: Final = frozenset({"grant_reservations", "grant_terminal_events"})


class GrantUseStoreError(RuntimeError):
    """Fail-closed durable-ledger error with a stable non-sensitive code."""


class GrantUseStatus(StrEnum):
    """The only durable lifecycle states for one reserved grant."""

    RESERVED = "RESERVED"
    TERMINAL = "TERMINAL"


@dataclass(frozen=True, slots=True)
class GrantUseRecord:
    """One strict durable reservation plus its optional immutable terminal fact."""

    grant_id: str
    signed_payload_sha256: str
    reserved_at_utc: str
    status: GrantUseStatus
    terminal_reason_code: str | None = None
    terminal_at_utc: str | None = None
    terminal_record_id: str | None = None


@dataclass(frozen=True, slots=True)
class GrantUseResult:
    """A non-exceptional reserve/terminal result; false always fails closed."""

    accepted: bool
    reason_code: str | None
    record: GrantUseRecord | None


def _store_error(code: str, cause: BaseException | None = None) -> GrantUseStoreError:
    error = GrantUseStoreError(code)
    if cause is not None:
        error.__cause__ = cause
    return error


def _canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or normalized != value
        or value.strip() != value
        or len(value) > _MAX_GRANT_ID_CODEPOINTS
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError(f"{field} is not canonical")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 hexadecimal value")
    return value


def _timestamp(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _RFC3339_UTC_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be RFC3339 UTC ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field} must be a valid RFC3339 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError(f"{field} must use UTC")
    return value


def _reason_code(value: object) -> str:
    if not isinstance(value, str) or _REASON_CODE_RE.fullmatch(value) is None:
        raise ValueError("reason_code must be an uppercase stable reason code")
    return value


def _terminal_record_id(
    *,
    grant_id: str,
    signed_payload_sha256: str,
    reason_code: str,
) -> str:
    material = "\0".join((grant_id, signed_payload_sha256, reason_code)).encode("utf-8")
    return hashlib.sha256(_TERMINAL_RECORD_DOMAIN + material).hexdigest()


class CanaryGrantUseStore:
    """Strict local SQLite ledger with bounded-lock, restart-safe transitions."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        lock_timeout_seconds: float = 1.0,
    ) -> None:
        if (
            isinstance(lock_timeout_seconds, bool)
            or not isinstance(lock_timeout_seconds, (int, float))
            or not math.isfinite(float(lock_timeout_seconds))
            or not 0.001 <= float(lock_timeout_seconds) <= 10.0
        ):
            raise ValueError("lock_timeout_seconds must be finite and between 0.001 and 10")
        self.path = Path(path)
        self._lock_timeout_seconds = float(lock_timeout_seconds)
        self._validate_path()
        try:
            with self._session() as connection:
                self._initialize_schema(connection)
            self._enforce_private_database_mode()
        except GrantUseStoreError:
            raise
        except sqlite3.OperationalError as exc:
            raise _store_error("GRANT_USE_STORE_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _store_error("GRANT_USE_STORE_CORRUPTED", exc) from exc
        except (OSError, sqlite3.Error) as exc:
            raise _store_error("GRANT_USE_STORE_UNAVAILABLE", exc) from exc

    def reserve(
        self,
        *,
        grant_id: str,
        signed_payload_sha256: str,
        reserved_at_utc: str,
    ) -> GrantUseResult:
        """Reserve one ID/payload pair once; duplicate identity never retries."""

        canonical_grant_id = _canonical_text(grant_id, field="grant_id")
        canonical_payload_digest = _sha256(
            signed_payload_sha256,
            field="signed_payload_sha256",
        )
        canonical_timestamp = _timestamp(reserved_at_utc, field="reserved_at_utc")
        try:
            with self._session() as connection:
                connection.execute("BEGIN IMMEDIATE")
                same_id = connection.execute(
                    "SELECT 1 FROM grant_reservations WHERE grant_id = ?",
                    (canonical_grant_id,),
                ).fetchone()
                if same_id is not None:
                    connection.commit()
                    return GrantUseResult(False, "GRANT_ID_ALREADY_RESERVED", None)
                same_payload = connection.execute(
                    "SELECT 1 FROM grant_reservations WHERE signed_payload_sha256 = ?",
                    (canonical_payload_digest,),
                ).fetchone()
                if same_payload is not None:
                    connection.commit()
                    return GrantUseResult(False, "SIGNED_PAYLOAD_ALREADY_RESERVED", None)
                connection.execute(
                    """
                    INSERT INTO grant_reservations(
                        grant_id, signed_payload_sha256, reserved_at_utc, status
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        canonical_grant_id,
                        canonical_payload_digest,
                        canonical_timestamp,
                        GrantUseStatus.RESERVED.value,
                    ),
                )
                connection.commit()
        except sqlite3.OperationalError as exc:
            raise _store_error("GRANT_USE_STORE_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _store_error("GRANT_USE_STORE_CORRUPTED", exc) from exc
        record = GrantUseRecord(
            grant_id=canonical_grant_id,
            signed_payload_sha256=canonical_payload_digest,
            reserved_at_utc=canonical_timestamp,
            status=GrantUseStatus.RESERVED,
        )
        return GrantUseResult(True, None, record)

    def record_terminal(
        self,
        *,
        grant_id: str,
        signed_payload_sha256: str,
        reason_code: str,
        occurred_at_utc: str,
    ) -> GrantUseResult:
        """Append the sole terminal fact for one matching prior reservation."""

        canonical_grant_id = _canonical_text(grant_id, field="grant_id")
        canonical_payload_digest = _sha256(
            signed_payload_sha256,
            field="signed_payload_sha256",
        )
        canonical_reason = _reason_code(reason_code)
        canonical_timestamp = _timestamp(occurred_at_utc, field="occurred_at_utc")
        terminal_id = _terminal_record_id(
            grant_id=canonical_grant_id,
            signed_payload_sha256=canonical_payload_digest,
            reason_code=canonical_reason,
        )
        try:
            with self._session() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    """
                    SELECT signed_payload_sha256, reserved_at_utc, status,
                           terminal_reason_code, terminal_at_utc, terminal_record_id
                    FROM grant_reservations WHERE grant_id = ?
                    """,
                    (canonical_grant_id,),
                ).fetchone()
                if row is None:
                    connection.commit()
                    return GrantUseResult(False, "GRANT_NOT_RESERVED", None)
                if row[0] != canonical_payload_digest:
                    connection.commit()
                    return GrantUseResult(False, "SIGNED_PAYLOAD_MISMATCH", None)
                if row[2] == GrantUseStatus.TERMINAL.value:
                    connection.commit()
                    return GrantUseResult(False, "GRANT_ALREADY_TERMINAL", None)
                if (
                    row[2] != GrantUseStatus.RESERVED.value
                    or row[3] is not None
                    or row[4] is not None
                    or row[5] is not None
                ):
                    raise _store_error("GRANT_USE_STORE_CORRUPTED")
                connection.execute(
                    """
                    INSERT INTO grant_terminal_events(
                        terminal_record_id, grant_id, signed_payload_sha256,
                        reason_code, occurred_at_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        terminal_id,
                        canonical_grant_id,
                        canonical_payload_digest,
                        canonical_reason,
                        canonical_timestamp,
                    ),
                )
                updated = connection.execute(
                    """
                    UPDATE grant_reservations
                    SET status = ?, terminal_reason_code = ?, terminal_at_utc = ?,
                        terminal_record_id = ?
                    WHERE grant_id = ? AND status = ?
                    """,
                    (
                        GrantUseStatus.TERMINAL.value,
                        canonical_reason,
                        canonical_timestamp,
                        terminal_id,
                        canonical_grant_id,
                        GrantUseStatus.RESERVED.value,
                    ),
                )
                if updated.rowcount != 1:
                    raise _store_error("GRANT_USE_STORE_CORRUPTED")
                connection.commit()
        except GrantUseStoreError:
            raise
        except sqlite3.OperationalError as exc:
            raise _store_error("GRANT_USE_STORE_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _store_error("GRANT_USE_STORE_CORRUPTED", exc) from exc
        return GrantUseResult(
            True,
            None,
            GrantUseRecord(
                grant_id=canonical_grant_id,
                signed_payload_sha256=canonical_payload_digest,
                reserved_at_utc=row[1],
                status=GrantUseStatus.TERMINAL,
                terminal_reason_code=canonical_reason,
                terminal_at_utc=canonical_timestamp,
                terminal_record_id=terminal_id,
            ),
        )

    def load(self, grant_id: str) -> GrantUseRecord | None:
        """Read one exact durable record or return ``None`` only when absent."""

        canonical_grant_id = _canonical_text(grant_id, field="grant_id")
        try:
            with self._session() as connection:
                row = connection.execute(
                    """
                    SELECT r.grant_id, r.signed_payload_sha256, r.reserved_at_utc,
                           r.status, r.terminal_reason_code, r.terminal_at_utc,
                           r.terminal_record_id, e.terminal_record_id,
                           e.signed_payload_sha256, e.reason_code, e.occurred_at_utc
                    FROM grant_reservations AS r
                    LEFT JOIN grant_terminal_events AS e ON e.grant_id = r.grant_id
                    WHERE r.grant_id = ?
                    """,
                    (canonical_grant_id,),
                ).fetchone()
        except sqlite3.OperationalError as exc:
            raise _store_error("GRANT_USE_STORE_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _store_error("GRANT_USE_STORE_CORRUPTED", exc) from exc
        if row is None:
            return None
        return self._record_from_row(row)

    def _validate_path(self) -> None:
        parent = self.path.parent
        try:
            parent_status = parent.stat()
        except OSError as exc:
            raise _store_error("GRANT_USE_STORE_DIRECTORY_UNAVAILABLE", exc) from exc
        if not stat.S_ISDIR(parent_status.st_mode) or stat.S_IMODE(parent_status.st_mode) & 0o077:
            raise _store_error("GRANT_USE_STORE_DIRECTORY_NOT_PRIVATE")
        try:
            file_status = self.path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise _store_error("GRANT_USE_STORE_UNAVAILABLE", exc) from exc
        if not stat.S_ISREG(file_status.st_mode) or stat.S_ISLNK(file_status.st_mode):
            raise _store_error("GRANT_USE_STORE_PATH_INVALID")

    def _enforce_private_database_mode(self) -> None:
        try:
            os.chmod(self.path, 0o600)
            if stat.S_IMODE(self.path.stat().st_mode) & 0o077:
                raise _store_error("GRANT_USE_STORE_FILE_NOT_PRIVATE")
        except GrantUseStoreError:
            raise
        except OSError as exc:
            raise _store_error("GRANT_USE_STORE_UNAVAILABLE", exc) from exc

    def _connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self._lock_timeout_seconds,
            isolation_level=None,
        )
        try:
            connection.execute(f"PRAGMA busy_timeout = {int(self._lock_timeout_seconds * 1000)}")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA trusted_schema = OFF")
            journal_mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()
            if journal_mode is None or journal_mode[0] != "delete":
                raise _store_error("GRANT_USE_STORE_JOURNAL_MODE_INVALID")
            connection.execute("PRAGMA synchronous = FULL")
            return connection
        except BaseException:
            connection.close()
            raise

    @contextmanager
    def _session(self):
        """Yield one configured connection and close it on every control path."""

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
                raise _store_error("GRANT_USE_STORE_CORRUPTED")
            schema_objects = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE name NOT LIKE 'sqlite_%' AND type IN ('table', 'view', 'trigger')
                    """
                )
            }
            if version[0] == 0 and not schema_objects:
                connection.execute(
                    """
                    CREATE TABLE grant_reservations (
                        grant_id TEXT PRIMARY KEY NOT NULL,
                        signed_payload_sha256 TEXT UNIQUE NOT NULL,
                        reserved_at_utc TEXT NOT NULL,
                        status TEXT NOT NULL,
                        terminal_reason_code TEXT,
                        terminal_at_utc TEXT,
                        terminal_record_id TEXT UNIQUE
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE grant_terminal_events (
                        terminal_record_id TEXT PRIMARY KEY NOT NULL,
                        grant_id TEXT UNIQUE NOT NULL,
                        signed_payload_sha256 TEXT NOT NULL,
                        reason_code TEXT NOT NULL,
                        occurred_at_utc TEXT NOT NULL,
                        FOREIGN KEY(grant_id) REFERENCES grant_reservations(grant_id)
                    )
                    """
                )
                connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            elif version[0] != _SCHEMA_VERSION:
                raise _store_error("GRANT_USE_STORE_SCHEMA_MISMATCH")
            if not CanaryGrantUseStore._schema_matches(connection):
                raise _store_error("GRANT_USE_STORE_SCHEMA_MISMATCH")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    @staticmethod
    def _schema_matches(connection: sqlite3.Connection) -> bool:
        """Reject same-named tables that weaken a required ledger invariant."""

        schema_objects = {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%' AND type IN ('table', 'view', 'trigger')
                """
            )
        }
        if schema_objects != _EXPECTED_TABLES:
            return False

        def columns(table: str) -> tuple[tuple[str, str, int, int], ...]:
            return tuple(
                (str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5]))
                for row in connection.execute(f"PRAGMA table_info({table})")
            )

        expected_reservations = (
            ("grant_id", "TEXT", 1, 1),
            ("signed_payload_sha256", "TEXT", 1, 0),
            ("reserved_at_utc", "TEXT", 1, 0),
            ("status", "TEXT", 1, 0),
            ("terminal_reason_code", "TEXT", 0, 0),
            ("terminal_at_utc", "TEXT", 0, 0),
            ("terminal_record_id", "TEXT", 0, 0),
        )
        expected_events = (
            ("terminal_record_id", "TEXT", 1, 1),
            ("grant_id", "TEXT", 1, 0),
            ("signed_payload_sha256", "TEXT", 1, 0),
            ("reason_code", "TEXT", 1, 0),
            ("occurred_at_utc", "TEXT", 1, 0),
        )
        if columns("grant_reservations") != expected_reservations or columns(
            "grant_terminal_events"
        ) != expected_events:
            return False

        def has_unique_single_column_index(table: str, column: str) -> bool:
            for index in connection.execute(f"PRAGMA index_list({table})"):
                if int(index[2]) != 1:
                    continue
                index_columns = tuple(
                    str(row[2])
                    for row in connection.execute(f"PRAGMA index_info({index[1]})")
                )
                if index_columns == (column,):
                    return True
            return False

        required_unique_indexes = (
            ("grant_reservations", "grant_id"),
            ("grant_reservations", "signed_payload_sha256"),
            ("grant_reservations", "terminal_record_id"),
            ("grant_terminal_events", "terminal_record_id"),
            ("grant_terminal_events", "grant_id"),
        )
        if not all(
            has_unique_single_column_index(table, column)
            for table, column in required_unique_indexes
        ):
            return False
        foreign_keys = tuple(
            (str(row[2]), str(row[3]), str(row[4]))
            for row in connection.execute("PRAGMA foreign_key_list(grant_terminal_events)")
        )
        return foreign_keys == (("grant_reservations", "grant_id", "grant_id"),)

    @staticmethod
    def _record_from_row(row: tuple[object, ...]) -> GrantUseRecord:
        if len(row) != 11:
            raise _store_error("GRANT_USE_STORE_CORRUPTED")
        try:
            grant_id = _canonical_text(row[0], field="stored grant_id")
            payload_digest = _sha256(row[1], field="stored signed_payload_sha256")
            reserved_at = _timestamp(row[2], field="stored reserved_at_utc")
            status = GrantUseStatus(row[3])
            terminal_fields = row[4:]
            if status is GrantUseStatus.RESERVED:
                if any(value is not None for value in terminal_fields):
                    raise _store_error("GRANT_USE_STORE_CORRUPTED")
                return GrantUseRecord(
                    grant_id=grant_id,
                    signed_payload_sha256=payload_digest,
                    reserved_at_utc=reserved_at,
                    status=status,
                )
            if status is not GrantUseStatus.TERMINAL:
                raise _store_error("GRANT_USE_STORE_CORRUPTED")
            reason = _reason_code(row[4])
            terminal_at = _timestamp(row[5], field="stored terminal_at_utc")
            terminal_id = _sha256(row[6], field="stored terminal_record_id")
            if (
                row[7] != terminal_id
                or row[8] != payload_digest
                or row[9] != reason
                or row[10] != terminal_at
                or terminal_id
                != _terminal_record_id(
                    grant_id=grant_id,
                    signed_payload_sha256=payload_digest,
                    reason_code=reason,
                )
            ):
                raise _store_error("GRANT_USE_STORE_CORRUPTED")
            return GrantUseRecord(
                grant_id=grant_id,
                signed_payload_sha256=payload_digest,
                reserved_at_utc=reserved_at,
                status=status,
                terminal_reason_code=reason,
                terminal_at_utc=terminal_at,
                terminal_record_id=terminal_id,
            )
        except (TypeError, ValueError) as exc:
            raise _store_error("GRANT_USE_STORE_CORRUPTED", exc) from exc
