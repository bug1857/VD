"""ADR-012/013 durable source membership for the reference v2 host.

The reference v2 host makes a response visible only after
``SQLiteHostResponseCommitStore.commit_response`` durably appends the exact
completed response.  The immutable source row is also the outbox item;
independent consumers append acknowledgements without deleting or mutating
source evidence.  This module never imports detector, policy, actuation,
Milvus, grant, or routing code.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields
import base64
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import unicodedata

import numpy as np

from .artifacts import canonical_json_bytes
from .config import Metric
from .host_observation import (
    CompletedRangeQueryObservation,
    RangeQueryRequest,
    RangeServingExecutor,
    ServedQueryOutcome,
)
from .response_profile_evidence import (
    build_canonical_query_identity,
    build_query_vector_identity,
)
from .response_profile_workload_capture import (
    CaptureEnvironmentIdentity,
    GenuineWorkloadObservation,
    build_capture_environment_identity,
)
from .shadow_event_types import MonitorStreamKey

__all__ = [
    "HOST_WINDOW_LINEAGE_SCHEMA_VERSION",
    "CommittedHostObservation",
    "HostResponseCommitError",
    "InjectedReadOnlyCaptureMetadataProvider",
    "ReferenceV2Host",
    "SQLiteHostResponseCommitStore",
    "V2GenuineWorkloadObservationSource",
    "V2VisibleResponse",
    "verify_committed_host_observation",
]

HOST_WINDOW_LINEAGE_SCHEMA_VERSION = "response-profile-host-window-lineage-v2"
_DB_VERSION = 2
_SOURCE_DOMAIN = b"VD::HOST_RESPONSE_SOURCE::V2\x00"
_OUTBOX_DOMAIN = b"VD::HOST_RESPONSE_SOURCE_OUTBOX::V2\x00"
_ACK_DOMAIN = b"VD::HOST_RESPONSE_SOURCE_ACK::V2\x00"
_WINDOW_DOMAIN = b"VD::HOST_RESPONSE_WINDOW::V2\x00"
_BINDING_DOMAIN = b"VD::HOST_RESPONSE_STORE_BINDING::V2\x00"
_RFC3339 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_OWNERSHIP_LOCK = threading.Lock()
_OWNED_LOCK_INODES: set[tuple[int, int]] = set()


class HostResponseCommitError(RuntimeError):
    """Fail-closed v2 host error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> HostResponseCommitError:
    return HostResponseCommitError(code, message)


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


def _digest(domain: bytes, payload: Mapping[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(dict(payload))).hexdigest()


def _stream_document(value: MonitorStreamKey) -> dict[str, object]:
    if type(value) is not MonitorStreamKey:
        raise _error("HOST_SOURCE_STREAM_INVALID")
    rebuilt = MonitorStreamKey(
        stream_id=value.stream_id,
        metric=value.metric,
        threshold_stratum=value.threshold_stratum,
        configuration_identity=value.configuration_identity,
        data_identity=value.data_identity,
        flat_binding_id=value.flat_binding_id,
        hnsw_binding_id=value.hnsw_binding_id,
    )
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("HOST_SOURCE_STREAM_INVALID")
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
        raise _error("HOST_SOURCE_STREAM_INVALID")
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
        raise _error("HOST_SOURCE_STREAM_INVALID") from exc


@dataclass(frozen=True, slots=True, init=False)
class CommittedHostObservation:
    """One immutable v2 source member reconstructed from the durable ledger."""

    schema_version: str
    event_id: str
    source_sequence: int
    window_sequence: int
    within_window_index: int
    stream_key: MonitorStreamKey
    source_revision: str
    environment_manifest_sha256: str
    consistency_level: str
    committed_at_utc: str
    query_id: int | str
    query_id_sha256: str
    query_vector: tuple[float, ...]
    vector_sha256: str
    threshold_radius: float
    range_filter: float
    limit: int
    served_ef: int
    served_outcome: ServedQueryOutcome
    previous_source_sha256: str | None
    source_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("committed observations are store-issued")


def _make_committed(**values: object) -> CommittedHostObservation:
    result = object.__new__(CommittedHostObservation)
    for name, value in values.items():
        object.__setattr__(result, name, value)
    return result


def _source_payload(
    *,
    event_id: str,
    source_sequence: int,
    stream_key: MonitorStreamKey,
    source_revision: str,
    environment_manifest_sha256: str,
    consistency_level: str,
    committed_at_utc: str,
    observation: CompletedRangeQueryObservation,
    previous_source_sha256: str | None,
) -> dict[str, object]:
    _text(event_id, code="HOST_SOURCE_EVENT_ID_INVALID")
    if type(source_sequence) is not int or source_sequence < 0:
        raise _error("HOST_SOURCE_SEQUENCE_INVALID")
    if type(observation) is not CompletedRangeQueryObservation:
        raise _error("HOST_SOURCE_OBSERVATION_INVALID")
    if observation.stream_key != stream_key:
        raise _error("HOST_SOURCE_STREAM_MISMATCH")
    query = build_canonical_query_identity(observation.request_id)
    vector = build_query_vector_identity(np.asarray(observation.query_vector, dtype="<f4"))
    return {
        "schema_version": HOST_WINDOW_LINEAGE_SCHEMA_VERSION,
        "event_id": event_id,
        "source_sequence": source_sequence,
        "window_sequence": source_sequence // 200,
        "within_window_index": source_sequence % 200,
        "stream": _stream_document(stream_key),
        "source_revision": _text(source_revision, code="HOST_SOURCE_REVISION_INVALID"),
        "environment_manifest_sha256": _sha(
            environment_manifest_sha256, code="HOST_SOURCE_ENVIRONMENT_INVALID"
        ),
        "consistency_level": _text(
            consistency_level, code="HOST_SOURCE_CONSISTENCY_INVALID"
        ),
        "committed_at_utc": _timestamp(
            committed_at_utc, code="HOST_SOURCE_TIMESTAMP_INVALID"
        ),
        "query_id": query.query_id,
        "query_id_sha256": query.query_id_sha256,
        "canonical_vector_bytes_base64": base64.b64encode(
            vector.canonical_vector_bytes
        ).decode("ascii"),
        "vector_sha256": vector.vector_sha256,
        "threshold_radius": observation.threshold_radius,
        "range_filter": observation.range_filter,
        "limit": observation.limit,
        "served_ef": observation.served_ef,
        "served_outcome": {
            "success": observation.served_outcome.success,
            "timed_out": observation.served_outcome.timed_out,
            "result_count": observation.served_outcome.result_count,
            "latency_ms": observation.served_outcome.latency_ms,
            "error_code": observation.served_outcome.error_code,
        },
        "previous_source_sha256": previous_source_sha256,
    }


def _source_from_document(value: object, *, expected_sequence: int | None = None) -> CommittedHostObservation:
    required = {
        "source_payload", "source_sha256",
    }
    if type(value) is not dict or set(value) != required:
        raise _error("HOST_SOURCE_RECORD_INVALID")
    payload = value["source_payload"]
    fields_required = {
        "schema_version", "event_id", "source_sequence", "window_sequence",
        "within_window_index", "stream", "source_revision",
        "environment_manifest_sha256", "consistency_level", "committed_at_utc",
        "query_id", "query_id_sha256", "canonical_vector_bytes_base64",
        "vector_sha256", "threshold_radius", "range_filter", "limit", "served_ef",
        "served_outcome", "previous_source_sha256",
    }
    if type(payload) is not dict or set(payload) != fields_required:
        raise _error("HOST_SOURCE_RECORD_INVALID")
    if payload["schema_version"] != HOST_WINDOW_LINEAGE_SCHEMA_VERSION:
        raise _error("HOST_SOURCE_SCHEMA_INVALID")
    sequence = payload["source_sequence"]
    if (
        type(sequence) is not int or sequence < 0
        or (expected_sequence is not None and sequence != expected_sequence)
        or payload["window_sequence"] != sequence // 200
        or payload["within_window_index"] != sequence % 200
    ):
        raise _error("HOST_SOURCE_SEQUENCE_INVALID")
    previous = payload["previous_source_sha256"]
    if previous is not None:
        _sha(previous, code="HOST_SOURCE_CHAIN_INVALID")
    expected_digest = _digest(_SOURCE_DOMAIN, payload)
    if type(value["source_sha256"]) is not str or not hmac.compare_digest(
        value["source_sha256"], expected_digest
    ):
        raise _error("HOST_SOURCE_DIGEST_INVALID")
    try:
        raw = base64.b64decode(payload["canonical_vector_bytes_base64"], validate=True)
        vector_array = np.frombuffer(raw, dtype="<f4")
        vector = build_query_vector_identity(vector_array)
        query = build_canonical_query_identity(payload["query_id"])
        outcome_doc = payload["served_outcome"]
        if type(outcome_doc) is not dict or set(outcome_doc) != {
            "success", "timed_out", "result_count", "latency_ms", "error_code"
        }:
            raise ValueError
        outcome = ServedQueryOutcome(
            success=outcome_doc["success"], timed_out=outcome_doc["timed_out"],
            result_count=outcome_doc["result_count"], latency_ms=outcome_doc["latency_ms"],
            error_code=outcome_doc["error_code"],
        )
        stream = _stream_from_document(payload["stream"])
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("HOST_SOURCE_RECORD_INVALID") from exc
    if (
        payload["query_id_sha256"] != query.query_id_sha256
        or payload["vector_sha256"] != vector.vector_sha256
    ):
        raise _error("HOST_SOURCE_IDENTITY_INVALID")
    _text(payload["event_id"], code="HOST_SOURCE_EVENT_ID_INVALID")
    _text(payload["source_revision"], code="HOST_SOURCE_REVISION_INVALID")
    _sha(payload["environment_manifest_sha256"], code="HOST_SOURCE_ENVIRONMENT_INVALID")
    _text(payload["consistency_level"], code="HOST_SOURCE_CONSISTENCY_INVALID")
    _timestamp(payload["committed_at_utc"], code="HOST_SOURCE_TIMESTAMP_INVALID")
    for name in ("threshold_radius", "range_filter"):
        if type(payload[name]) is not float or not np.isfinite(payload[name]):
            raise _error("HOST_SOURCE_RECORD_INVALID")
    if type(payload["limit"]) is not int or payload["limit"] <= 0:
        raise _error("HOST_SOURCE_RECORD_INVALID")
    if type(payload["served_ef"]) is not int or payload["served_ef"] <= 0:
        raise _error("HOST_SOURCE_RECORD_INVALID")
    return _make_committed(
        schema_version=HOST_WINDOW_LINEAGE_SCHEMA_VERSION,
        event_id=payload["event_id"], source_sequence=sequence,
        window_sequence=sequence // 200, within_window_index=sequence % 200,
        stream_key=stream, source_revision=payload["source_revision"],
        environment_manifest_sha256=payload["environment_manifest_sha256"],
        consistency_level=payload["consistency_level"],
        committed_at_utc=payload["committed_at_utc"], query_id=query.query_id,
        query_id_sha256=query.query_id_sha256,
        query_vector=tuple(float(item) for item in vector_array),
        vector_sha256=vector.vector_sha256,
        threshold_radius=payload["threshold_radius"],
        range_filter=payload["range_filter"], limit=payload["limit"],
        served_ef=payload["served_ef"], served_outcome=outcome,
        previous_source_sha256=previous, source_sha256=expected_digest,
    )


def _source_document(value: CommittedHostObservation) -> dict[str, object]:
    if type(value) is not CommittedHostObservation:
        raise _error("HOST_SOURCE_RECORD_INVALID")
    observation = CompletedRangeQueryObservation(
        request_id=value.query_id, captured_at_utc=value.committed_at_utc,
        stream_key=value.stream_key, query_vector=value.query_vector,
        threshold_radius=value.threshold_radius, range_filter=value.range_filter,
        limit=value.limit, served_ef=value.served_ef, served_outcome=value.served_outcome,
    )
    payload = _source_payload(
        event_id=value.event_id, source_sequence=value.source_sequence,
        stream_key=value.stream_key, source_revision=value.source_revision,
        environment_manifest_sha256=value.environment_manifest_sha256,
        consistency_level=value.consistency_level,
        committed_at_utc=value.committed_at_utc, observation=observation,
        previous_source_sha256=value.previous_source_sha256,
    )
    rebuilt = _source_from_document(
        {"source_payload": payload, "source_sha256": _digest(_SOURCE_DOMAIN, payload)},
        expected_sequence=value.source_sequence,
    )
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("HOST_SOURCE_RECORD_INVALID")
    return {"source_payload": payload, "source_sha256": value.source_sha256}


def verify_committed_host_observation(
    value: object,
) -> CommittedHostObservation:
    """Fully reconstruct one store-shaped source value before downstream use."""

    if type(value) is not CommittedHostObservation:
        raise _error("HOST_SOURCE_RECORD_INVALID")
    return _source_from_document(
        _source_document(value), expected_sequence=value.source_sequence
    )


_SCHEMA_SQL = (
    "CREATE TABLE store_binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), binding_json BLOB NOT NULL, binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256)=64)) STRICT",
    # `query_id_sha256 UNIQUE` (schema v2) makes canonical query-id uniqueness a
    # durable, transactional invariant of source membership itself, rather than
    # a mutable side table. Without it the ledger accepted a repeated request id
    # (event_id differs because source_sequence differs), and the duplicate only
    # surfaced up to 1,400 observations later when
    # `build_calibration_population_manifest` raised CALIBRATION_QUERY_ID_DUPLICATE.
    "CREATE TABLE source_records (source_sequence INTEGER PRIMARY KEY CHECK(source_sequence>=0), event_id TEXT NOT NULL UNIQUE, query_id_sha256 TEXT NOT NULL UNIQUE CHECK(length(query_id_sha256)=64), source_json BLOB NOT NULL, previous_source_sha256 TEXT, source_sha256 TEXT NOT NULL UNIQUE CHECK(length(source_sha256)=64)) STRICT",
    "CREATE TABLE source_outbox (source_sequence INTEGER PRIMARY KEY CHECK(source_sequence>=0), event_id TEXT NOT NULL UNIQUE, source_sha256 TEXT NOT NULL UNIQUE CHECK(length(source_sha256)=64), previous_outbox_sha256 TEXT, outbox_sha256 TEXT NOT NULL UNIQUE CHECK(length(outbox_sha256)=64), FOREIGN KEY(source_sequence) REFERENCES source_records(source_sequence)) STRICT",
    "CREATE TABLE consumer_acknowledgements (consumer_id TEXT NOT NULL, ack_sequence INTEGER NOT NULL CHECK(ack_sequence>=0), source_sequence INTEGER NOT NULL CHECK(source_sequence>=0), event_id TEXT NOT NULL, acknowledged_at_utc TEXT NOT NULL, previous_ack_sha256 TEXT, ack_sha256 TEXT NOT NULL UNIQUE CHECK(length(ack_sha256)=64), PRIMARY KEY(consumer_id,ack_sequence), UNIQUE(consumer_id,source_sequence), FOREIGN KEY(source_sequence) REFERENCES source_records(source_sequence)) STRICT",
    "CREATE TRIGGER store_binding_no_update BEFORE UPDATE ON store_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER store_binding_no_delete BEFORE DELETE ON store_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER source_records_no_update BEFORE UPDATE ON source_records BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER source_records_no_delete BEFORE DELETE ON source_records BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER source_outbox_no_update BEFORE UPDATE ON source_outbox BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER source_outbox_no_delete BEFORE DELETE ON source_outbox BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER consumer_acknowledgements_no_update BEFORE UPDATE ON consumer_acknowledgements BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER consumer_acknowledgements_no_delete BEFORE DELETE ON consumer_acknowledgements BEGIN SELECT RAISE(ABORT,'append-only'); END",
)


def _normalize_sql(value: str) -> str:
    return " ".join(value.strip().removesuffix(";").split())


class SQLiteHostResponseCommitStore:
    """Exclusive-writer, append-only response/source commit and outbox store."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        stream_key: MonitorStreamKey,
        source_revision: str,
        environment_manifest_sha256: str,
        consistency_level: str = "Strong",
    ) -> None:
        self.path = Path(path)
        self.stream_key = stream_key
        self.source_revision = _text(source_revision, code="HOST_SOURCE_REVISION_INVALID")
        self.environment_manifest_sha256 = _sha(
            environment_manifest_sha256, code="HOST_SOURCE_ENVIRONMENT_INVALID"
        )
        self.consistency_level = _text(
            consistency_level, code="HOST_SOURCE_CONSISTENCY_INVALID"
        )
        self._mutex = threading.RLock()
        self._pid = os.getpid()
        self._closed = False
        self._lock_handle = None
        self._lock_inode: tuple[int, int] | None = None
        self._binding = {
            "schema_version": HOST_WINDOW_LINEAGE_SCHEMA_VERSION,
            "stream": _stream_document(stream_key),
            "source_revision": self.source_revision,
            "environment_manifest_sha256": self.environment_manifest_sha256,
            "consistency_level": self.consistency_level,
        }
        self._open()

    def __enter__(self) -> "SQLiteHostResponseCommitStore":
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
            raise _error("HOST_SOURCE_PARENT_UNSAFE")
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        lock_fd = os.open(lock_path, flags, 0o600)
        self._lock_handle = os.fdopen(lock_fd, "a+b")
        info = os.fstat(lock_fd)
        if (
            not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600
        ):
            self.close()
            raise _error("HOST_SOURCE_PATH_UNSAFE")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._lock_handle.close()
            raise _error("HOST_SOURCE_STORE_BUSY") from exc
        inode = (info.st_dev, info.st_ino)
        with _OWNERSHIP_LOCK:
            if inode in _OWNED_LOCK_INODES:
                self.close()
                raise _error("HOST_SOURCE_STORE_BUSY")
            _OWNED_LOCK_INODES.add(inode)
        self._lock_inode = inode
        created = not self.path.exists()
        if created:
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            os.close(fd)
        self._verify_path()
        self._connection = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
        self._connection.execute("PRAGMA foreign_keys=ON")
        try:
            if created:
                self._connection.execute("PRAGMA journal_mode=DELETE")
                self._connection.execute("PRAGMA synchronous=FULL")
                self._connection.execute("PRAGMA trusted_schema=OFF")
                self._connection.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA_SQL:
                    self._connection.execute(statement)
                self._connection.execute(f"PRAGMA user_version={_DB_VERSION}")
                self._connection.execute(
                    "INSERT INTO store_binding VALUES(1,?,?)",
                    (
                        canonical_json_bytes(self._binding),
                        _digest(_BINDING_DOMAIN, self._binding),
                    ),
                )
                self._connection.execute("COMMIT")
            else:
                mode = str(self._connection.execute("PRAGMA journal_mode").fetchone()[0])
                if mode.lower() != "delete":
                    raise _error("HOST_SOURCE_SCHEMA_INVALID")
                self._connection.execute("PRAGMA synchronous=FULL")
                self._connection.execute("PRAGMA trusted_schema=OFF")
            self._sources = self._verify_all()
        except Exception:
            self.close()
            raise

    def _verify_path(self) -> None:
        info = os.lstat(self.path)
        if (
            not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise _error("HOST_SOURCE_PATH_UNSAFE")

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
            raise _error("HOST_SOURCE_STORE_CLOSED")
        if os.getpid() != self._pid:
            raise _error("HOST_SOURCE_STORE_FORKED")
        self._verify_path()

    def _verify_schema(self) -> None:
        if self._connection.execute("PRAGMA user_version").fetchone()[0] != _DB_VERSION:
            raise _error("HOST_SOURCE_SCHEMA_INVALID")
        actual = {
            row[0]: _normalize_sql(row[1])
            for row in self._connection.execute(
                "SELECT name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        expected: dict[str, str] = {}
        for statement in _SCHEMA_SQL:
            expected[statement.split()[2]] = _normalize_sql(statement)
        if actual != expected:
            raise _error("HOST_SOURCE_SCHEMA_INVALID")

    def _verify_binding(self) -> None:
        row = self._connection.execute(
            "SELECT binding_json,binding_sha256 FROM store_binding WHERE singleton=1"
        ).fetchone()
        expected = canonical_json_bytes(self._binding)
        if (
            row is None or bytes(row[0]) != expected
            or row[1] != _digest(_BINDING_DOMAIN, self._binding)
        ):
            raise _error("HOST_SOURCE_BINDING_MISMATCH")

    def _load_sources(self) -> tuple[CommittedHostObservation, ...]:
        records: list[CommittedHostObservation] = []
        previous = None
        for expected, row in enumerate(
            self._connection.execute(
                "SELECT source_sequence,event_id,source_json,previous_source_sha256,source_sha256,query_id_sha256 FROM source_records ORDER BY source_sequence"
            )
        ):
            try:
                document = json.loads(bytes(row[2]).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _error("HOST_SOURCE_RECORD_INVALID") from exc
            if canonical_json_bytes(document) != bytes(row[2]):
                raise _error("HOST_SOURCE_RECORD_INVALID")
            record = _source_from_document(document, expected_sequence=expected)
            # The relational `query_id_sha256` column is a uniqueness mechanism,
            # never an authority. The canonical source record stays the sole
            # authority, so on every verification the column must equal the
            # value reconstructed from source_json (which source_sha256 covers).
            # Without this, the column could be tampered independently while the
            # digests stayed valid, turning uniqueness into an unverified
            # side-channel.
            if (
                row[0] != expected or row[1] != record.event_id
                or row[3] != previous or row[4] != record.source_sha256
                or row[5] != record.query_id_sha256
                or record.previous_source_sha256 != previous
                or record.stream_key != self.stream_key
                or record.source_revision != self.source_revision
                or record.environment_manifest_sha256 != self.environment_manifest_sha256
                or record.consistency_level != self.consistency_level
            ):
                raise _error("HOST_SOURCE_CHAIN_INVALID")
            records.append(record)
            previous = record.source_sha256
        return tuple(records)

    def _verify_acknowledgements(self, sources: tuple[CommittedHostObservation, ...]) -> None:
        heads: dict[str, tuple[int, int, str | None]] = {}
        for row in self._connection.execute(
            "SELECT consumer_id,ack_sequence,source_sequence,event_id,acknowledged_at_utc,previous_ack_sha256,ack_sha256 FROM consumer_acknowledgements ORDER BY consumer_id,ack_sequence"
        ):
            consumer = _text(row[0], code="HOST_SOURCE_ACK_INVALID")
            prior = heads.get(consumer)
            expected_ack = 0 if prior is None else prior[0]
            expected_source = row[2] if prior is None else prior[1]
            previous = None if prior is None else prior[2]
            payload = {
                "schema_version": "response-profile-host-window-ack-v2",
                "consumer_id": consumer,
                "ack_sequence": row[1], "source_sequence": row[2],
                "event_id": row[3], "acknowledged_at_utc": row[4],
                "previous_ack_sha256": row[5],
            }
            if (
                row[1] != expected_ack or row[2] != expected_source
                or row[2] >= len(sources) or row[3] != sources[row[2]].event_id
                or row[5] != previous or row[6] != _digest(_ACK_DOMAIN, payload)
            ):
                raise _error("HOST_SOURCE_ACK_INVALID")
            _timestamp(row[4], code="HOST_SOURCE_ACK_INVALID")
            heads[consumer] = (expected_ack + 1, expected_source + 1, row[6])

    def _verify_outbox(self, sources: tuple[CommittedHostObservation, ...]) -> None:
        rows = self._connection.execute(
            "SELECT source_sequence,event_id,source_sha256,previous_outbox_sha256,outbox_sha256 FROM source_outbox ORDER BY source_sequence"
        ).fetchall()
        if len(rows) != len(sources):
            raise _error("HOST_SOURCE_OUTBOX_INVALID")
        previous = None
        for expected, (row, source) in enumerate(zip(rows, sources, strict=True)):
            payload = {
                "schema_version": "response-profile-host-source-outbox-v2",
                "source_sequence": expected,
                "event_id": source.event_id,
                "source_sha256": source.source_sha256,
                "previous_outbox_sha256": previous,
            }
            digest = _digest(_OUTBOX_DOMAIN, payload)
            if (
                row[0] != expected or row[1] != source.event_id
                or row[2] != source.source_sha256 or row[3] != previous
                or row[4] != digest
            ):
                raise _error("HOST_SOURCE_OUTBOX_INVALID")
            previous = digest

    def _verify_all(self) -> tuple[CommittedHostObservation, ...]:
        self._require_live()
        self._verify_schema()
        self._verify_binding()
        sources = self._load_sources()
        self._verify_outbox(sources)
        self._verify_acknowledgements(sources)
        return sources

    def _verify_cached_source_head(self) -> None:
        """Verify the durable head without replaying the growing chain per append."""

        self._require_live()
        self._verify_schema()
        self._verify_binding()
        count, maximum = self._connection.execute(
            "SELECT COUNT(*),MAX(source_sequence) FROM source_records"
        ).fetchone()
        expected = len(self._sources)
        if count != expected or maximum != (expected - 1 if expected else None):
            raise _error("HOST_SOURCE_HEAD_DRIFT")
        if expected:
            row = self._connection.execute(
                "SELECT source_sha256 FROM source_records WHERE source_sequence=?",
                (expected - 1,),
            ).fetchone()
            if row is None or row[0] != self._sources[-1].source_sha256:
                raise _error("HOST_SOURCE_HEAD_DRIFT")
            outbox = self._connection.execute(
                "SELECT source_sha256 FROM source_outbox WHERE source_sequence=?",
                (expected - 1,),
            ).fetchone()
            if outbox is None or outbox[0] != self._sources[-1].source_sha256:
                raise _error("HOST_SOURCE_HEAD_DRIFT")
        outbox_count = self._connection.execute(
            "SELECT COUNT(*) FROM source_outbox"
        ).fetchone()[0]
        if outbox_count != expected:
            raise _error("HOST_SOURCE_HEAD_DRIFT")

    def commit_response(
        self,
        observation: CompletedRangeQueryObservation,
        *,
        committed_at_utc: str,
    ) -> CommittedHostObservation:
        """Atomically allocate membership and commit it before response visibility."""

        with self._mutex:
            self._verify_cached_source_head()
            sources = self._sources
            sequence = len(sources)
            previous = sources[-1].source_sha256 if sources else None
            event_seed = {
                "stream": _stream_document(self.stream_key),
                "source_sequence": sequence,
                "query_id": build_canonical_query_identity(observation.request_id).query_id,
                "committed_at_utc": committed_at_utc,
            }
            event_id = _digest(b"VD::HOST_RESPONSE_EVENT::V2\x00", event_seed)
            payload = _source_payload(
                event_id=event_id, source_sequence=sequence,
                stream_key=self.stream_key, source_revision=self.source_revision,
                environment_manifest_sha256=self.environment_manifest_sha256,
                consistency_level=self.consistency_level,
                committed_at_utc=committed_at_utc, observation=observation,
                previous_source_sha256=previous,
            )
            document = {
                "source_payload": payload,
                "source_sha256": _digest(_SOURCE_DOMAIN, payload),
            }
            record = _source_from_document(document, expected_sequence=sequence)
            previous_outbox = None
            if sequence:
                previous_outbox = self._connection.execute(
                    "SELECT outbox_sha256 FROM source_outbox WHERE source_sequence=?",
                    (sequence - 1,),
                ).fetchone()[0]
            outbox_payload = {
                "schema_version": "response-profile-host-source-outbox-v2",
                "source_sequence": sequence,
                "event_id": record.event_id,
                "source_sha256": record.source_sha256,
                "previous_outbox_sha256": previous_outbox,
            }
            outbox_digest = _digest(_OUTBOX_DOMAIN, outbox_payload)
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                self._verify_cached_source_head()
                # Durable, transactional query-id uniqueness. The explicit
                # SELECT yields the stable reason code; the UNIQUE constraint
                # below is the concurrency-safe backstop that makes the
                # invariant hold even under a race. A duplicate rolls the whole
                # transaction back, so no source_sequence is consumed and
                # contiguity is preserved.
                if self._connection.execute(
                    "SELECT 1 FROM source_records WHERE query_id_sha256=? LIMIT 1",
                    (record.query_id_sha256,),
                ).fetchone() is not None:
                    raise _error("HOST_SOURCE_QUERY_ID_DUPLICATE")
                self._connection.execute(
                    "INSERT INTO source_records VALUES(?,?,?,?,?,?)",
                    (
                        sequence, record.event_id, record.query_id_sha256,
                        canonical_json_bytes(document), previous,
                        record.source_sha256,
                    ),
                )
                self._connection.execute(
                    "INSERT INTO source_outbox VALUES(?,?,?,?,?)",
                    (
                        sequence, record.event_id, record.source_sha256,
                        previous_outbox, outbox_digest,
                    ),
                )
                self._connection.execute("COMMIT")
            except HostResponseCommitError:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            except sqlite3.IntegrityError as exc:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                if "query_id_sha256" in str(exc):
                    raise _error("HOST_SOURCE_QUERY_ID_DUPLICATE") from exc
                raise _error("HOST_RESPONSE_DURABILITY_FAILED") from exc
            except sqlite3.Error as exc:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise _error("HOST_RESPONSE_DURABILITY_FAILED") from exc
            self._sources = (*sources, record)
            return record

    def poll(
        self, *, consumer_id: str, limit: int, start_source_sequence: int = 0
    ) -> tuple[CommittedHostObservation, ...]:
        with self._mutex:
            sources = self._verify_all()
            consumer = _text(consumer_id, code="HOST_SOURCE_CONSUMER_INVALID")
            if type(limit) is not int or limit <= 0:
                raise _error("HOST_SOURCE_LIMIT_INVALID")
            if type(start_source_sequence) is not int or start_source_sequence < 0:
                raise _error("HOST_SOURCE_SEQUENCE_INVALID")
            row = self._connection.execute(
                "SELECT COUNT(*),MIN(source_sequence),MAX(source_sequence) FROM consumer_acknowledgements WHERE consumer_id=?",
                (consumer,),
            ).fetchone()
            count = row[0]
            if count and row[1] != start_source_sequence:
                raise _error("HOST_SOURCE_CONSUMER_OFFSET_MISMATCH")
            next_sequence = start_source_sequence if not count else row[2] + 1
            return sources[next_sequence : next_sequence + limit]

    def acknowledge(
        self,
        *,
        consumer_id: str,
        event_ids: tuple[str, ...],
        acknowledged_at_utc: str,
        start_source_sequence: int = 0,
    ) -> None:
        with self._mutex:
            sources = self._verify_all()
            consumer = _text(consumer_id, code="HOST_SOURCE_CONSUMER_INVALID")
            _timestamp(acknowledged_at_utc, code="HOST_SOURCE_ACK_INVALID")
            if type(event_ids) is not tuple or not event_ids:
                raise _error("HOST_SOURCE_ACK_INVALID")
            if type(start_source_sequence) is not int or start_source_sequence < 0:
                raise _error("HOST_SOURCE_SEQUENCE_INVALID")
            rows = self._connection.execute(
                "SELECT ack_sequence,source_sequence,ack_sha256 FROM consumer_acknowledgements WHERE consumer_id=? ORDER BY ack_sequence",
                (consumer,),
            ).fetchall()
            if rows and rows[0][1] != start_source_sequence:
                raise _error("HOST_SOURCE_CONSUMER_OFFSET_MISMATCH")
            start = len(rows)
            source_start = start_source_sequence if not rows else rows[-1][1] + 1
            expected = sources[source_start : source_start + len(event_ids)]
            if tuple(item.event_id for item in expected) != event_ids:
                raise _error("HOST_SOURCE_ACK_INVALID")
            previous = rows[-1][2] if rows else None
            documents = []
            for offset, item in enumerate(expected):
                payload = {
                    "schema_version": "response-profile-host-window-ack-v2",
                    "consumer_id": consumer, "ack_sequence": start + offset,
                    "source_sequence": item.source_sequence,
                    "event_id": item.event_id,
                    "acknowledged_at_utc": acknowledged_at_utc,
                    "previous_ack_sha256": previous,
                }
                digest = _digest(_ACK_DOMAIN, payload)
                documents.append((payload, digest))
                previous = digest
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                for payload, digest in documents:
                    self._connection.execute(
                        "INSERT INTO consumer_acknowledgements VALUES(?,?,?,?,?,?,?)",
                        (
                            consumer, payload["ack_sequence"], payload["source_sequence"],
                            payload["event_id"], acknowledged_at_utc,
                            payload["previous_ack_sha256"], digest,
                        ),
                    )
                self._connection.execute("COMMIT")
            except sqlite3.Error as exc:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise _error("HOST_SOURCE_ACK_FAILED") from exc

    def window_sha256(self, window_sequence: int) -> str | None:
        sources = self._verify_all()
        if type(window_sequence) is not int or window_sequence < 0:
            raise _error("HOST_SOURCE_WINDOW_INVALID")
        start = window_sequence * 200
        members = sources[start : start + 200]
        if len(members) != 200:
            return None
        payload = {
            "schema_version": "response-profile-host-source-window-v2",
            "stream": _stream_document(self.stream_key),
            "window_sequence": window_sequence,
            "source_sha256": [item.source_sha256 for item in members],
        }
        return _digest(_WINDOW_DOMAIN, payload)


@dataclass(frozen=True, slots=True)
class V2VisibleResponse:
    """A response that became visible only after its source commit completed."""

    served_outcome: ServedQueryOutcome
    committed_observation: CommittedHostObservation


class ReferenceV2Host:
    """Offline-testable v2 host composition; intentionally separate from v1."""

    def __init__(
        self,
        *,
        serving_executor: RangeServingExecutor,
        response_store: SQLiteHostResponseCommitStore,
        clock: Callable[[], str],
    ) -> None:
        if type(response_store) is not SQLiteHostResponseCommitStore:
            raise TypeError("response_store must be SQLiteHostResponseCommitStore")
        self._executor = serving_executor
        self._store = response_store
        self._clock = clock

    def execute(self, request: RangeQueryRequest) -> V2VisibleResponse:
        outcome = self._executor.execute(request)
        if type(outcome) is not ServedQueryOutcome:
            raise _error("HOST_RESPONSE_OUTCOME_INVALID")
        committed_at = self._clock()
        observation = CompletedRangeQueryObservation(
            request_id=request.request_id, captured_at_utc=committed_at,
            stream_key=request.stream_key, query_vector=request.query_vector,
            threshold_radius=request.threshold_radius, range_filter=request.range_filter,
            limit=request.limit, served_ef=request.served_ef,
            served_outcome=outcome,
        )
        committed = self._store.commit_response(
            observation, committed_at_utc=committed_at
        )
        return V2VisibleResponse(outcome, committed)


class V2GenuineWorkloadObservationSource:
    """Restart-safe EXP-010 adapter over one independent v2 consumer cursor."""

    def __init__(
        self,
        *,
        store: SQLiteHostResponseCommitStore,
        consumer_id: str,
        clock: Callable[[], str],
        start_source_sequence: int = 0,
    ) -> None:
        if type(store) is not SQLiteHostResponseCommitStore:
            raise TypeError("store must be SQLiteHostResponseCommitStore")
        self._store = store
        self._consumer_id = _text(consumer_id, code="HOST_SOURCE_CONSUMER_INVALID")
        self._clock = clock
        if type(start_source_sequence) is not int or start_source_sequence < 0:
            raise _error("HOST_SOURCE_SEQUENCE_INVALID")
        self._start_source_sequence = start_source_sequence

    def poll(self, *, limit: int) -> tuple[GenuineWorkloadObservation, ...]:
        records = self._store.poll(
            consumer_id=self._consumer_id,
            limit=limit,
            start_source_sequence=self._start_source_sequence,
        )
        return tuple(
            GenuineWorkloadObservation(
                event_id=item.event_id, source_sequence=item.source_sequence,
                window_sequence=item.window_sequence,
                within_window_index=item.within_window_index,
                query_id=item.query_id, observed_at_utc=item.committed_at_utc,
                stream_key=item.stream_key, source_revision=item.source_revision,
                environment_manifest_sha256=item.environment_manifest_sha256,
                query_vector=item.query_vector,
                threshold_radius=item.threshold_radius,
                range_filter=item.range_filter, limit=item.limit,
                consistency_level=item.consistency_level,
            )
            for item in records
        )

    def acknowledge(self, event_ids: tuple[str, ...]) -> None:
        self._store.acknowledge(
            consumer_id=self._consumer_id, event_ids=event_ids,
            acknowledged_at_utc=self._clock(),
            start_source_sequence=self._start_source_sequence,
        )


class InjectedReadOnlyCaptureMetadataProvider:
    """Concrete metadata adapter over one injected, read-only snapshot callable."""

    def __init__(self, snapshot: Callable[[], Mapping[str, object]]) -> None:
        if not callable(snapshot):
            raise TypeError("snapshot must be callable")
        self._snapshot = snapshot

    def capture(self) -> CaptureEnvironmentIdentity:
        value = self._snapshot()
        if not isinstance(value, Mapping):
            raise _error("CAPTURE_METADATA_UNAVAILABLE")
        try:
            return build_capture_environment_identity(
                milvus_uri=value["milvus_uri"],
                deployment_identity=value["deployment_identity"],
                collection_name=value["collection_name"],
                dimensions=value["dimensions"],
                metric=value["metric"],
                hnsw_index_identity=value["hnsw_index_identity"],
                data_identity=value["data_identity"],
                source_revision=value["source_revision"],
                observed_at_utc=value["observed_at_utc"],
                environment_manifest=value["environment_manifest"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise _error("CAPTURE_METADATA_INVALID") from exc
