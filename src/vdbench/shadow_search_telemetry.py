"""Append-only per-search telemetry for EXP-012-SCALE Gate C.

The ledger is additive to the frozen shadow trace/attempt evidence.  It records
the exact physical FLAT and sentinel-HNSW search interval and outcome for each
source position, but creates no detector, qualification, policy, admission, or
execution authority.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import threading
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Self

from .artifacts import canonical_json_bytes
from .exp012_scale_contract import (
    Exp012ScaleContract,
    verify_exp012_scale_contract,
)
from .host_window_lineage import CommittedHostObservation
from .shadow_attempt_store import build_shadow_attempt_identity
from .shadow_event_types import MonitorStreamKey
from .shadow_window import TRACE_QUERY_COUNT, WINDOW_QUERY_COUNT

__all__ = [
    "SHADOW_SEARCH_TELEMETRY_SCHEMA_VERSION",
    "SQLiteShadowSearchTelemetryStore",
    "ShadowSearchOutcome",
    "ShadowSearchRole",
    "ShadowSearchTelemetryBinding",
    "ShadowSearchTelemetryError",
    "ShadowSearchTelemetryRecord",
    "ShadowSearchTelemetrySummary",
]


SHADOW_SEARCH_TELEMETRY_SCHEMA_VERSION = "exp012-shadow-search-telemetry-v1"
_DB_VERSION = 1
_BINDING_DOMAIN = b"VD::EXP012_SHADOW_TELEMETRY_BINDING::V1\x00"
_RECORD_DOMAIN = b"VD::EXP012_SHADOW_TELEMETRY_RECORD::V1\x00"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_OWNERSHIP_LOCK = threading.Lock()
_OWNED_LOCK_INODES: set[tuple[int, int]] = set()


class ShadowSearchTelemetryError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> ShadowSearchTelemetryError:
    return ShadowSearchTelemetryError(code)


class ShadowSearchRole(StrEnum):
    FLAT_REFERENCE = "FLAT_REFERENCE"
    HNSW_SENTINEL = "HNSW_SENTINEL"


class ShadowSearchOutcome(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


def _text(value: object, code: str) -> str:
    if type(value) is not str:
        raise _error(code)
    normalized = unicodedata.normalize("NFC", value)
    if not normalized or normalized != value:
        raise _error(code)
    return value


def _sha(value: object, code: str) -> str:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise _error(code)
    return value


def _digest(domain: bytes, payload: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(payload)).hexdigest()


def _reconstruct_stream_key(stream: MonitorStreamKey) -> MonitorStreamKey:
    """Re-run the stream value type's complete constructor contract."""

    if type(stream) is not MonitorStreamKey:
        raise _error("TELEMETRY_STREAM_INVALID")
    try:
        rebuilt = MonitorStreamKey(
            stream_id=stream.stream_id,
            metric=stream.metric,
            threshold_stratum=stream.threshold_stratum,
            configuration_identity=stream.configuration_identity,
            data_identity=stream.data_identity,
            flat_binding_id=stream.flat_binding_id,
            hnsw_binding_id=stream.hnsw_binding_id,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("TELEMETRY_STREAM_INVALID") from exc
    if rebuilt != stream:
        raise _error("TELEMETRY_STREAM_INVALID")
    return rebuilt


def _stream_payload(stream: MonitorStreamKey) -> dict[str, object]:
    stream = _reconstruct_stream_key(stream)
    return {
        "stream_id": stream.stream_id,
        "metric": stream.metric.value,
        "threshold_stratum": stream.threshold_stratum,
        "configuration_identity": stream.configuration_identity,
        "data_identity": stream.data_identity,
        "flat_binding_id": stream.flat_binding_id,
        "hnsw_binding_id": stream.hnsw_binding_id,
    }


@dataclass(frozen=True, slots=True)
class ShadowSearchTelemetryBinding:
    campaign_id: str
    scale_contract: Exp012ScaleContract
    stream_key: MonitorStreamKey
    source_revision: str
    environment_manifest_sha256: str

    def __post_init__(self) -> None:
        _text(self.campaign_id, "TELEMETRY_CAMPAIGN_INVALID")
        verify_exp012_scale_contract(self.scale_contract)
        _reconstruct_stream_key(self.stream_key)
        _text(self.source_revision, "TELEMETRY_SOURCE_REVISION_INVALID")
        _sha(self.environment_manifest_sha256, "TELEMETRY_ENVIRONMENT_INVALID")


def _binding_payload(binding: ShadowSearchTelemetryBinding) -> dict[str, object]:
    if type(binding) is not ShadowSearchTelemetryBinding:
        raise _error("TELEMETRY_BINDING_INVALID")
    stream_key = _reconstruct_stream_key(binding.stream_key)
    rebuilt = ShadowSearchTelemetryBinding(
        campaign_id=binding.campaign_id,
        scale_contract=verify_exp012_scale_contract(binding.scale_contract),
        stream_key=stream_key,
        source_revision=binding.source_revision,
        environment_manifest_sha256=binding.environment_manifest_sha256,
    )
    if rebuilt != binding:
        raise _error("TELEMETRY_BINDING_INVALID")
    return {
        "schema_version": "exp012-shadow-search-telemetry-binding-v1",
        "campaign_id": binding.campaign_id,
        "scale_contract_sha256": binding.scale_contract.contract_sha256,
        "stream": _stream_payload(stream_key),
        "source_revision": binding.source_revision,
        "environment_manifest_sha256": binding.environment_manifest_sha256,
    }


@dataclass(frozen=True, slots=True)
class ShadowSearchTelemetryRecord:
    record_sequence: int
    campaign_id: str
    window_sequence: int
    trace_sequence_index: int
    attempt_sha256: str
    source_sequence: int
    source_sha256: str
    query_id_sha256: str
    role: ShadowSearchRole
    started_monotonic_ns: int
    completed_monotonic_ns: int
    latency_ns: int
    latency_ms: float
    outcome: ShadowSearchOutcome
    error_classification: str | None
    result_count: int | None
    previous_record_sha256: str | None
    record_sha256: str


@dataclass(frozen=True, slots=True)
class ShadowSearchTelemetrySummary:
    record_count: int
    succeeded_count: int
    failed_count: int
    head_sha256: str | None
    complete: bool


def _record_payload(
    *,
    record_sequence: int,
    binding: ShadowSearchTelemetryBinding,
    window_sequence: int,
    trace_sequence_index: int,
    attempt_sha256: str,
    source_sequence: int,
    source_sha256: str,
    query_id_sha256: str,
    role: ShadowSearchRole,
    started_monotonic_ns: int,
    completed_monotonic_ns: int,
    outcome: ShadowSearchOutcome,
    error_classification: str | None,
    result_count: int | None,
    previous_record_sha256: str | None,
) -> dict[str, object]:
    if type(record_sequence) is not int or record_sequence < 0:
        raise _error("TELEMETRY_RECORD_INVALID")
    if type(window_sequence) is not int or window_sequence < 0:
        raise _error("TELEMETRY_POSITION_INVALID")
    if type(trace_sequence_index) is not int or not 0 <= trace_sequence_index < 4:
        raise _error("TELEMETRY_POSITION_INVALID")
    if type(source_sequence) is not int or source_sequence < 0:
        raise _error("TELEMETRY_POSITION_INVALID")
    if source_sequence // WINDOW_QUERY_COUNT != window_sequence:
        raise _error("TELEMETRY_POSITION_INVALID")
    if (source_sequence % WINDOW_QUERY_COUNT) // TRACE_QUERY_COUNT != trace_sequence_index:
        raise _error("TELEMETRY_POSITION_INVALID")
    _sha(attempt_sha256, "TELEMETRY_ATTEMPT_INVALID")
    _sha(source_sha256, "TELEMETRY_SOURCE_INVALID")
    _sha(query_id_sha256, "TELEMETRY_QUERY_INVALID")
    if type(role) is not ShadowSearchRole or type(outcome) is not ShadowSearchOutcome:
        raise _error("TELEMETRY_RECORD_INVALID")
    if (
        type(started_monotonic_ns) is not int
        or type(completed_monotonic_ns) is not int
        or started_monotonic_ns < 0
        or completed_monotonic_ns < started_monotonic_ns
    ):
        raise _error("TELEMETRY_TIMING_INVALID")
    latency_ns = completed_monotonic_ns - started_monotonic_ns
    latency_ms = float(latency_ns) / 1_000_000.0
    if not math.isfinite(latency_ms):
        raise _error("TELEMETRY_TIMING_INVALID")
    if outcome is ShadowSearchOutcome.SUCCEEDED:
        if error_classification is not None:
            raise _error("TELEMETRY_OUTCOME_INVALID")
        if type(result_count) is not int or result_count < 0:
            raise _error("TELEMETRY_OUTCOME_INVALID")
    else:
        _text(error_classification, "TELEMETRY_OUTCOME_INVALID")
        if result_count is not None:
            raise _error("TELEMETRY_OUTCOME_INVALID")
    if previous_record_sha256 is not None:
        _sha(previous_record_sha256, "TELEMETRY_CHAIN_INVALID")
    return {
        "schema_version": SHADOW_SEARCH_TELEMETRY_SCHEMA_VERSION,
        "record_sequence": record_sequence,
        "campaign_id": binding.campaign_id,
        "scale_contract_sha256": binding.scale_contract.contract_sha256,
        "window_sequence": window_sequence,
        "trace_sequence_index": trace_sequence_index,
        "attempt_sha256": attempt_sha256,
        "source_sequence": source_sequence,
        "source_sha256": source_sha256,
        "query_id_sha256": query_id_sha256,
        "role": role.value,
        "started_monotonic_ns": started_monotonic_ns,
        "completed_monotonic_ns": completed_monotonic_ns,
        "latency_ns": latency_ns,
        "latency_ms": latency_ms,
        "outcome": outcome.value,
        "error_classification": error_classification,
        "result_count": result_count,
        "previous_record_sha256": previous_record_sha256,
    }


_SCHEMA_SQL = (
    "CREATE TABLE telemetry_binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), binding_json BLOB NOT NULL, binding_sha256 TEXT NOT NULL UNIQUE CHECK(length(binding_sha256)=64)) STRICT",
    "CREATE TABLE telemetry_records (record_sequence INTEGER PRIMARY KEY CHECK(record_sequence>=0), source_sequence INTEGER NOT NULL CHECK(source_sequence>=0), role TEXT NOT NULL CHECK(role IN ('FLAT_REFERENCE','HNSW_SENTINEL')), record_json BLOB NOT NULL, previous_record_sha256 TEXT, record_sha256 TEXT NOT NULL UNIQUE CHECK(length(record_sha256)=64), UNIQUE(source_sequence,role)) STRICT",
    "CREATE TRIGGER telemetry_binding_no_update BEFORE UPDATE ON telemetry_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER telemetry_binding_no_delete BEFORE DELETE ON telemetry_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER telemetry_records_no_update BEFORE UPDATE ON telemetry_records BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER telemetry_records_no_delete BEFORE DELETE ON telemetry_records BEGIN SELECT RAISE(ABORT,'append-only'); END",
)


def _normalize_sql(value: str) -> str:
    return " ".join(value.strip().removesuffix(";").split())


class SQLiteShadowSearchTelemetryStore:
    """Exclusive-writer hardened SQLite telemetry ledger."""

    def __init__(
        self, path: str | os.PathLike[str], *, binding: ShadowSearchTelemetryBinding
    ) -> None:
        self.path = Path(path)
        self.binding = binding
        self._binding_payload = _binding_payload(binding)
        self._mutex = threading.RLock()
        self._pid = os.getpid()
        self._closed = False
        self._poisoned = False
        self._lock_handle = None
        self._lock_inode: tuple[int, int] | None = None
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
            raise _error("TELEMETRY_PARENT_UNSAFE")
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
            raise _error("TELEMETRY_PATH_UNSAFE")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise _error("TELEMETRY_STORE_BUSY") from exc
        inode = (lock_info.st_dev, lock_info.st_ino)
        with _OWNERSHIP_LOCK:
            if inode in _OWNED_LOCK_INODES:
                raise _error("TELEMETRY_STORE_BUSY")
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
        self._connection.execute("PRAGMA foreign_keys=ON")
        if created:
            self._connection.execute("PRAGMA journal_mode=DELETE")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA trusted_schema=OFF")
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                for statement in _SCHEMA_SQL:
                    self._connection.execute(statement)
                self._connection.execute(f"PRAGMA user_version={_DB_VERSION}")
                self._connection.execute(
                    "INSERT INTO telemetry_binding VALUES(1,?,?)",
                    (
                        canonical_json_bytes(self._binding_payload),
                        _digest(_BINDING_DOMAIN, self._binding_payload),
                    ),
                )
                self._connection.execute("COMMIT")
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
        else:
            mode = str(self._connection.execute("PRAGMA journal_mode").fetchone()[0])
            if mode.lower() != "delete":
                raise _error("TELEMETRY_SCHEMA_INVALID")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA trusted_schema=OFF")
        self._records = self._verify_all()
        self._data_version = self._connection.execute("PRAGMA data_version").fetchone()[0]

    def _verify_path(self) -> None:
        info = os.lstat(self.path)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise _error("TELEMETRY_PATH_UNSAFE")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        connection = getattr(self, "_connection", None)
        if connection is not None:
            connection.close()
        if self._lock_handle is not None and not self._lock_handle.closed:
            try:
                fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._lock_handle.close()
        if self._lock_inode is not None:
            with _OWNERSHIP_LOCK:
                _OWNED_LOCK_INODES.discard(self._lock_inode)
            self._lock_inode = None

    def _require_live(self) -> None:
        if self._closed:
            raise _error("TELEMETRY_STORE_CLOSED")
        if self._poisoned:
            raise _error("TELEMETRY_STORE_POISONED")
        if os.getpid() != self._pid:
            raise _error("TELEMETRY_STORE_FORKED")
        self._verify_path()

    def _verify_schema(self) -> None:
        if self._connection.execute("PRAGMA user_version").fetchone()[0] != _DB_VERSION:
            raise _error("TELEMETRY_SCHEMA_INVALID")
        actual = {
            row[0]: _normalize_sql(row[1])
            for row in self._connection.execute(
                "SELECT name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        expected = {
            statement.split()[2]: _normalize_sql(statement)
            for statement in _SCHEMA_SQL
        }
        if actual != expected:
            raise _error("TELEMETRY_SCHEMA_INVALID")

    def _verify_binding(self) -> None:
        row = self._connection.execute(
            "SELECT binding_json,binding_sha256 FROM telemetry_binding WHERE singleton=1"
        ).fetchone()
        encoded = canonical_json_bytes(self._binding_payload)
        if (
            row is None
            or bytes(row[0]) != encoded
            or row[1] != _digest(_BINDING_DOMAIN, self._binding_payload)
        ):
            raise _error("TELEMETRY_BINDING_MISMATCH")

    def _record_from_document(
        self, document: object, *, expected_sequence: int, previous: str | None
    ) -> ShadowSearchTelemetryRecord:
        if type(document) is not dict or set(document) != {"record_payload", "record_sha256"}:
            raise _error("TELEMETRY_RECORD_INVALID")
        payload = document["record_payload"]
        if type(payload) is not dict:
            raise _error("TELEMETRY_RECORD_INVALID")
        try:
            role = ShadowSearchRole(payload["role"])
            outcome = ShadowSearchOutcome(payload["outcome"])
            rebuilt = _record_payload(
                record_sequence=expected_sequence,
                binding=self.binding,
                window_sequence=payload["window_sequence"],
                trace_sequence_index=payload["trace_sequence_index"],
                attempt_sha256=payload["attempt_sha256"],
                source_sequence=payload["source_sequence"],
                source_sha256=payload["source_sha256"],
                query_id_sha256=payload["query_id_sha256"],
                role=role,
                started_monotonic_ns=payload["started_monotonic_ns"],
                completed_monotonic_ns=payload["completed_monotonic_ns"],
                outcome=outcome,
                error_classification=payload["error_classification"],
                result_count=payload["result_count"],
                previous_record_sha256=previous,
            )
        except (KeyError, TypeError, ValueError, ShadowSearchTelemetryError) as exc:
            raise _error("TELEMETRY_RECORD_INVALID") from exc
        digest = _digest(_RECORD_DOMAIN, rebuilt)
        if payload != rebuilt or document["record_sha256"] != digest:
            raise _error("TELEMETRY_RECORD_INVALID")
        return ShadowSearchTelemetryRecord(
            record_sequence=expected_sequence,
            campaign_id=self.binding.campaign_id,
            window_sequence=rebuilt["window_sequence"],
            trace_sequence_index=rebuilt["trace_sequence_index"],
            attempt_sha256=rebuilt["attempt_sha256"],
            source_sequence=rebuilt["source_sequence"],
            source_sha256=rebuilt["source_sha256"],
            query_id_sha256=rebuilt["query_id_sha256"],
            role=role,
            started_monotonic_ns=rebuilt["started_monotonic_ns"],
            completed_monotonic_ns=rebuilt["completed_monotonic_ns"],
            latency_ns=rebuilt["latency_ns"],
            latency_ms=rebuilt["latency_ms"],
            outcome=outcome,
            error_classification=rebuilt["error_classification"],
            result_count=rebuilt["result_count"],
            previous_record_sha256=previous,
            record_sha256=digest,
        )

    def _verify_all(self) -> tuple[ShadowSearchTelemetryRecord, ...]:
        self._require_live()
        self._verify_schema()
        self._verify_binding()
        records: list[ShadowSearchTelemetryRecord] = []
        previous = None
        for expected, row in enumerate(
            self._connection.execute(
                "SELECT record_sequence,source_sequence,role,record_json,previous_record_sha256,record_sha256 FROM telemetry_records ORDER BY record_sequence"
            )
        ):
            try:
                document = json.loads(bytes(row[3]).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _error("TELEMETRY_RECORD_INVALID") from exc
            if canonical_json_bytes(document) != bytes(row[3]):
                raise _error("TELEMETRY_RECORD_INVALID")
            record = self._record_from_document(
                document, expected_sequence=expected, previous=previous
            )
            if (
                row[0] != expected
                or row[1] != record.source_sequence
                or row[2] != record.role.value
                or row[4] != previous
                or row[5] != record.record_sha256
            ):
                raise _error("TELEMETRY_CHAIN_INVALID")
            records.append(record)
            previous = record.record_sha256
        return tuple(records)

    def _verify_cached_head(self) -> None:
        self._require_live()
        self._verify_schema()
        self._verify_binding()
        if self._connection.execute("PRAGMA data_version").fetchone()[0] != self._data_version:
            raise _error("TELEMETRY_HEAD_DRIFT")
        row = self._connection.execute(
            "SELECT record_sequence,record_sha256 FROM telemetry_records "
            "ORDER BY record_sequence DESC LIMIT 1"
        ).fetchone()
        if not self._records:
            if row is not None:
                raise _error("TELEMETRY_HEAD_DRIFT")
        elif row != (len(self._records) - 1, self._records[-1].record_sha256):
            raise _error("TELEMETRY_HEAD_DRIFT")

    def append(
        self,
        *,
        window_sequence: int,
        trace_sequence_index: int,
        attempt_sha256: str,
        source_sequence: int,
        source_sha256: str,
        query_id_sha256: str,
        role: ShadowSearchRole,
        started_monotonic_ns: int,
        completed_monotonic_ns: int,
        outcome: ShadowSearchOutcome,
        error_classification: str | None,
        result_count: int | None,
    ) -> ShadowSearchTelemetryRecord:
        with self._mutex:
            self._verify_cached_head()
            sequence = len(self._records)
            previous = None if not self._records else self._records[-1].record_sha256
            payload = _record_payload(
                record_sequence=sequence,
                binding=self.binding,
                window_sequence=window_sequence,
                trace_sequence_index=trace_sequence_index,
                attempt_sha256=attempt_sha256,
                source_sequence=source_sequence,
                source_sha256=source_sha256,
                query_id_sha256=query_id_sha256,
                role=role,
                started_monotonic_ns=started_monotonic_ns,
                completed_monotonic_ns=completed_monotonic_ns,
                outcome=outcome,
                error_classification=error_classification,
                result_count=result_count,
                previous_record_sha256=previous,
            )
            digest = _digest(_RECORD_DOMAIN, payload)
            document = {"record_payload": payload, "record_sha256": digest}
            # Complete canonical reconstruction before any durable write. A
            # successful COMMIT can therefore fail only during in-memory
            # reconciliation, which poisons this instance and requires reopen.
            record = self._record_from_document(
                document, expected_sequence=sequence, previous=previous
            )
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                self._verify_cached_head()
                self._connection.execute(
                    "INSERT INTO telemetry_records VALUES(?,?,?,?,?,?)",
                    (
                        sequence,
                        source_sequence,
                        role.value,
                        canonical_json_bytes(document),
                        previous,
                        digest,
                    ),
                )
                self._connection.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise _error("TELEMETRY_POSITION_DUPLICATE") from exc
            except (sqlite3.Error, ShadowSearchTelemetryError) as exc:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                if isinstance(exc, ShadowSearchTelemetryError):
                    raise
                raise _error("TELEMETRY_DURABILITY_FAILED") from exc
            try:
                self._records = (*self._records, record)
            except BaseException as exc:
                self._poisoned = True
                raise _error("TELEMETRY_RECONCILIATION_FAILED") from exc
            return record

    def records(self) -> tuple[ShadowSearchTelemetryRecord, ...]:
        with self._mutex:
            self._records = self._verify_all()
            self._data_version = self._connection.execute("PRAGMA data_version").fetchone()[0]
            return self._records

    def summary(self) -> ShadowSearchTelemetrySummary:
        records = self.records()
        succeeded = sum(item.outcome is ShadowSearchOutcome.SUCCEEDED for item in records)
        contract = verify_exp012_scale_contract(self.binding.scale_contract)
        complete = self._is_complete(records, contract)
        return ShadowSearchTelemetrySummary(
            record_count=len(records),
            succeeded_count=succeeded,
            failed_count=len(records) - succeeded,
            head_sha256=None if not records else records[-1].record_sha256,
            complete=complete,
        )

    def verify_completion(
        self, sources: tuple[CommittedHostObservation, ...]
    ) -> ShadowSearchTelemetrySummary:
        """Cross-check exact telemetry membership against verified source rows.

        The caller must obtain ``sources`` from the canonical source store's
        full verification path.  This method then recomputes every governed
        trace-attempt identity and refuses a telemetry chain that merely has
        the right count while naming substituted source/query/attempt values.
        """

        contract = verify_exp012_scale_contract(self.binding.scale_contract)
        if type(sources) is not tuple or len(sources) != contract.target_source_records:
            raise _error("TELEMETRY_SOURCE_SET_INVALID")
        for expected, source in enumerate(sources):
            if type(source) is not CommittedHostObservation or source.source_sequence != expected:
                raise _error("TELEMETRY_SOURCE_SET_INVALID")
        records = self.records()
        if not self._is_complete(records, contract):
            raise _error("TELEMETRY_COMPLETION_INVALID")
        attempts: dict[int, str] = {}
        for start in range(0, len(sources), TRACE_QUERY_COUNT):
            trace_sources = sources[start : start + TRACE_QUERY_COUNT]
            trace_index = (start % WINDOW_QUERY_COUNT) // TRACE_QUERY_COUNT
            attempts[start // TRACE_QUERY_COUNT] = build_shadow_attempt_identity(
                trace_sources, trace_sequence_index=trace_index
            ).attempt_sha256
        for record in records:
            source = sources[record.source_sequence]
            expected_attempt = attempts[record.source_sequence // TRACE_QUERY_COUNT]
            if (
                record.source_sha256 != source.source_sha256
                or record.query_id_sha256 != source.query_id_sha256
                or record.window_sequence != source.window_sequence
                or record.trace_sequence_index
                != (source.within_window_index // TRACE_QUERY_COUNT)
                or record.attempt_sha256 != expected_attempt
            ):
                raise _error("TELEMETRY_SOURCE_BINDING_MISMATCH")
        return ShadowSearchTelemetrySummary(
            record_count=len(records),
            succeeded_count=len(records),
            failed_count=0,
            head_sha256=None if not records else records[-1].record_sha256,
            complete=True,
        )

    @staticmethod
    def _is_complete(
        records: tuple[ShadowSearchTelemetryRecord, ...],
        contract: Exp012ScaleContract,
    ) -> bool:
        if len(records) != contract.expected_physical_searches:
            return False
        if any(item.outcome is not ShadowSearchOutcome.SUCCEEDED for item in records):
            return False
        by_position = {(item.source_sequence, item.role) for item in records}
        expected = {
            (source_sequence, role)
            for source_sequence in range(contract.target_source_records)
            for role in ShadowSearchRole
        }
        return by_position == expected
