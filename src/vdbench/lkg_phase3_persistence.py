"""Restart-durable append-only references to Phase-3 LKG authorities.

Purpose:
    Persist only the immutable identity and lineage of authorities already
    produced by :mod:`vdbench.lkg_phase3_authority`.  This ledger never creates,
    reloads, or recomputes a qualification verdict.  A loaded record is only a
    reference; a later integration must resolve it through D1 again before use.
Inputs:
    A D1 ``LkgPhase3Authority`` and an externally supplied RFC3339-UTC
    persistence timestamp.
Outputs:
    Canonical, hash-chained ``PersistedLkgPhase3AuthorityReference`` values and
    append results that distinguish a new append from an idempotent replay. A
    store-issued ``VerifiedLatestLkgPhase3AuthorityReference`` identifies the
    fully verified chain head observed during one coherent read transaction;
    it remains an identity-only snapshot and is never qualification authority.
Durability and concurrency:
    SQLite ``BEGIN IMMEDIATE`` serializes writers.  FULL synchronous mode,
    DELETE journaling, private file permissions, canonical JSON, immutable SQL
    triggers, and complete-chain verification make every accepted append
    restart durable and tamper evident.
Trust boundary:
    Authority projection occurs before the write transaction.  This module has
    no Phase-1, Phase-2, Checkpoint-C ledger, policy, actuation, Milvus, or
    network dependency and performs no source replay while holding its lock.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Self

from .artifacts import canonical_json_bytes
from .config import Metric
from .lkg_phase3_authority import LkgPhase3Authority
from .search_configuration_digest import search_configuration_sha256

__all__ = [
    "LKG_PHASE3_REFERENCE_HASH_DOMAIN",
    "LKG_PHASE3_REFERENCE_SCHEMA_VERSION",
    "LkgPhase3AuthorityAppendResult",
    "LkgPhase3AuthorityReferenceStore",
    "LkgPhase3PersistenceError",
    "PersistedLkgPhase3AuthorityReference",
    "VerifiedLatestLkgPhase3AuthorityReference",
]


LKG_PHASE3_REFERENCE_SCHEMA_VERSION = "lkg-phase3-authority-reference-v1"
LKG_PHASE3_REFERENCE_HASH_DOMAIN = b"vdbench.lkg-phase3-authority-reference.v1\0"

_DATABASE_SCHEMA_VERSION = 1
_TABLE_NAME = "lkg_phase3_authority_references"
_GENESIS_DIGEST = "0" * 64
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339_UTC_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z"
)
_VERIFIED_LATEST_CONSTRUCTION_TOKEN = object()

_DOCUMENT_FIELDS = frozenset(
    {
        "record_schema_version",
        "sequence_number",
        "canonical_evaluation_digest",
        "source_run_id",
        "source_run_binding_sha256",
        "source_run_seal_digest",
        "source_sealed_phase1_chain_head_sha256",
        "phase2_source_binding_digest",
        "evaluated_ef",
        "search_configuration_digest",
        "metric",
        "threshold_stratum",
        "collection_name",
        "index_identity",
        "data_identity",
        "qualification_dataset_id",
        "qualification_dataset_version",
        "qualification_manifest_sha256",
        "qualification_query_role",
        "qualification_ordered_query_ids_sha256",
        "qualification_query_id_array_sha256",
        "qualification_query_array_sha256",
        "qualification_expected_query_count",
        "environment_identity",
        "source_revision",
        "persisted_at_utc",
        "previous_record_digest",
    }
)

_IDENTITY_FIELDS = _DOCUMENT_FIELDS - frozenset(
    {
        "record_schema_version",
        "sequence_number",
        "persisted_at_utc",
        "previous_record_digest",
    }
)

_COLUMN_NAMES = (
    "sequence_number",
    "record_schema_version",
    "canonical_evaluation_digest",
    "source_run_id",
    "source_run_binding_sha256",
    "source_run_seal_digest",
    "source_sealed_phase1_chain_head_sha256",
    "phase2_source_binding_digest",
    "evaluated_ef",
    "search_configuration_digest",
    "metric",
    "threshold_stratum",
    "collection_name",
    "index_identity",
    "data_identity",
    "qualification_dataset_id",
    "qualification_dataset_version",
    "qualification_manifest_sha256",
    "qualification_query_role",
    "qualification_ordered_query_ids_sha256",
    "qualification_query_id_array_sha256",
    "qualification_query_array_sha256",
    "qualification_expected_query_count",
    "environment_identity",
    "source_revision",
    "persisted_at_utc",
    "previous_record_digest",
    "canonical_record_digest",
    "record_document_json",
)


def _digest_check(column: str) -> str:
    return (
        f"length({column}) = 64 AND "
        f"{column} NOT GLOB '*[^0-9a-f]*'"
    )


_EXPECTED_TABLE_SQL = f"""
    CREATE TABLE {_TABLE_NAME} (
        sequence_number INTEGER PRIMARY KEY NOT NULL CHECK (sequence_number >= 0),
        record_schema_version TEXT NOT NULL
            CHECK (record_schema_version = '{LKG_PHASE3_REFERENCE_SCHEMA_VERSION}'),
        canonical_evaluation_digest TEXT NOT NULL UNIQUE
            CHECK ({_digest_check('canonical_evaluation_digest')}),
        source_run_id TEXT NOT NULL CHECK (length(source_run_id) BETWEEN 1 AND 512),
        source_run_binding_sha256 TEXT NOT NULL
            CHECK ({_digest_check('source_run_binding_sha256')}),
        source_run_seal_digest TEXT NOT NULL
            CHECK ({_digest_check('source_run_seal_digest')}),
        source_sealed_phase1_chain_head_sha256 TEXT NOT NULL
            CHECK ({_digest_check('source_sealed_phase1_chain_head_sha256')}),
        phase2_source_binding_digest TEXT NOT NULL
            CHECK ({_digest_check('phase2_source_binding_digest')}),
        evaluated_ef INTEGER NOT NULL CHECK (evaluated_ef > 0),
        search_configuration_digest TEXT NOT NULL
            CHECK ({_digest_check('search_configuration_digest')}),
        metric TEXT NOT NULL CHECK (metric IN ('L2', 'COSINE')),
        threshold_stratum TEXT NOT NULL
            CHECK (length(threshold_stratum) BETWEEN 1 AND 512),
        collection_name TEXT NOT NULL CHECK (length(collection_name) BETWEEN 1 AND 512),
        index_identity TEXT NOT NULL CHECK (length(index_identity) BETWEEN 1 AND 512),
        data_identity TEXT NOT NULL CHECK (length(data_identity) BETWEEN 1 AND 512),
        qualification_dataset_id TEXT NOT NULL
            CHECK (length(qualification_dataset_id) BETWEEN 1 AND 512),
        qualification_dataset_version TEXT NOT NULL
            CHECK (length(qualification_dataset_version) BETWEEN 1 AND 512),
        qualification_manifest_sha256 TEXT NOT NULL
            CHECK ({_digest_check('qualification_manifest_sha256')}),
        qualification_query_role TEXT NOT NULL
            CHECK (length(qualification_query_role) BETWEEN 1 AND 512),
        qualification_ordered_query_ids_sha256 TEXT NOT NULL
            CHECK ({_digest_check('qualification_ordered_query_ids_sha256')}),
        qualification_query_id_array_sha256 TEXT NOT NULL
            CHECK ({_digest_check('qualification_query_id_array_sha256')}),
        qualification_query_array_sha256 TEXT NOT NULL
            CHECK ({_digest_check('qualification_query_array_sha256')}),
        qualification_expected_query_count INTEGER NOT NULL
            CHECK (qualification_expected_query_count > 0),
        environment_identity TEXT NOT NULL
            CHECK (length(environment_identity) BETWEEN 1 AND 512),
        source_revision TEXT NOT NULL CHECK (length(source_revision) BETWEEN 1 AND 512),
        persisted_at_utc TEXT NOT NULL CHECK (length(persisted_at_utc) BETWEEN 20 AND 64),
        previous_record_digest TEXT NOT NULL
            CHECK ({_digest_check('previous_record_digest')}),
        canonical_record_digest TEXT NOT NULL UNIQUE
            CHECK ({_digest_check('canonical_record_digest')}),
        record_document_json TEXT NOT NULL,
        CHECK (
            (sequence_number = 0 AND previous_record_digest = '{_GENESIS_DIGEST}')
            OR (sequence_number > 0 AND previous_record_digest <> '{_GENESIS_DIGEST}')
        )
    ) STRICT;
"""

_EXPECTED_TRIGGER_SQL = {
    "trg_lkg_phase3_reference_insert_chain": f"""
        CREATE TRIGGER trg_lkg_phase3_reference_insert_chain
        BEFORE INSERT ON {_TABLE_NAME}
        WHEN NEW.sequence_number <> COALESCE(
            (SELECT MAX(sequence_number) + 1 FROM {_TABLE_NAME}), 0
        ) OR NEW.previous_record_digest <> COALESCE(
            (SELECT canonical_record_digest FROM {_TABLE_NAME}
             ORDER BY sequence_number DESC LIMIT 1), '{_GENESIS_DIGEST}'
        )
        BEGIN
            SELECT RAISE(ABORT, 'Phase-3 authority reference chain discontinuity');
        END;
    """,
    "trg_lkg_phase3_reference_no_update": f"""
        CREATE TRIGGER trg_lkg_phase3_reference_no_update
        BEFORE UPDATE ON {_TABLE_NAME}
        BEGIN
            SELECT RAISE(ABORT, 'Phase-3 authority references are immutable');
        END;
    """,
    "trg_lkg_phase3_reference_no_delete": f"""
        CREATE TRIGGER trg_lkg_phase3_reference_no_delete
        BEFORE DELETE ON {_TABLE_NAME}
        BEGIN
            SELECT RAISE(ABORT, 'Phase-3 authority references cannot be deleted');
        END;
    """,
}

_EXPECTED_SCHEMA_OBJECTS = frozenset({_TABLE_NAME, *_EXPECTED_TRIGGER_SQL})


class LkgPhase3PersistenceError(RuntimeError):
    """Stable fail-closed error boundary for D2 persistence."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(f"[{code}] {message}")
        self.message = message
        self.code = code


@dataclass(frozen=True, slots=True)
class PersistedLkgPhase3AuthorityReference:
    """Identity-only persisted reference; never a usable LKG authority."""

    record_schema_version: str
    sequence_number: int
    canonical_evaluation_digest: str
    source_run_id: str
    source_run_binding_sha256: str
    source_run_seal_digest: str
    source_sealed_phase1_chain_head_sha256: str
    phase2_source_binding_digest: str
    evaluated_ef: int
    search_configuration_digest: str
    metric: str
    threshold_stratum: str
    collection_name: str
    index_identity: str
    data_identity: str
    qualification_dataset_id: str
    qualification_dataset_version: str
    qualification_manifest_sha256: str
    qualification_query_role: str
    qualification_ordered_query_ids_sha256: str
    qualification_query_id_array_sha256: str
    qualification_query_array_sha256: str
    qualification_expected_query_count: int
    environment_identity: str
    source_revision: str
    persisted_at_utc: str
    previous_record_digest: str
    canonical_record_digest: str


@dataclass(frozen=True, slots=True, init=False)
class VerifiedLatestLkgPhase3AuthorityReference:
    """Store-issued snapshot of one fully verified D2 chain head.

    Private construction is API discipline, not cryptographic authenticity.
    The wrapper proves only that its exact immutable ``reference`` was the
    current head of one complete, coherent D2 verification transaction. It
    does not promise that the reference remains latest after that refresh and
    cannot become or substitute for a D1 ``LkgPhase3Authority``.
    """

    _reference: PersistedLkgPhase3AuthorityReference

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "VerifiedLatestLkgPhase3AuthorityReference can only be issued by "
            "LkgPhase3AuthorityReferenceStore.load_verified_latest()"
        )

    @classmethod
    def _from_verified_head(
        cls,
        *,
        reference: PersistedLkgPhase3AuthorityReference,
        construction_token: object,
    ) -> VerifiedLatestLkgPhase3AuthorityReference:
        if construction_token is not _VERIFIED_LATEST_CONSTRUCTION_TOKEN:
            raise TypeError("verified-latest construction token is invalid")
        if not isinstance(reference, PersistedLkgPhase3AuthorityReference):
            raise TypeError("reference must be a persisted Phase-3 authority reference")
        value = object.__new__(cls)
        object.__setattr__(value, "_reference", reference)
        return value

    @property
    def reference(self) -> PersistedLkgPhase3AuthorityReference:
        """Return the exact immutable persisted head verified by the store."""

        return self._reference

    @property
    def canonical_record_digest(self) -> str:
        """Return the verified head record digest for stable-lineage binding."""

        return self._reference.canonical_record_digest

    @property
    def sequence_number(self) -> int:
        """Return the verified head sequence observed during this refresh."""

        return self._reference.sequence_number


@dataclass(frozen=True, slots=True)
class LkgPhase3AuthorityAppendResult:
    """Result of a new append or exact idempotent authority replay."""

    reference: PersistedLkgPhase3AuthorityReference
    appended: bool


class _DuplicateJsonField(ValueError):
    pass


def _normalize_schema_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().rstrip(";")


def _canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise LkgPhase3PersistenceError(
            f"{field} must be a string", code="LKG_PHASE3_REFERENCE_MALFORMED"
        )
    if (
        not value
        or len(value) > 512
        or value != value.strip()
        or value != unicodedata.normalize("NFC", value)
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise LkgPhase3PersistenceError(
            f"{field} is not canonical", code="LKG_PHASE3_REFERENCE_MALFORMED"
        )
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise LkgPhase3PersistenceError(
            f"{field} must be a lower-case SHA-256 digest",
            code="LKG_PHASE3_REFERENCE_MALFORMED",
        )
    return value


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LkgPhase3PersistenceError(
            f"{field} must be a positive integer",
            code="LKG_PHASE3_REFERENCE_MALFORMED",
        )
    return value


def _sequence_number(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LkgPhase3PersistenceError(
            "sequence_number must be a non-negative integer",
            code="LKG_PHASE3_REFERENCE_MALFORMED",
        )
    return value


def _parse_rfc3339_utc(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or _RFC3339_UTC_RE.fullmatch(value) is None:
        raise LkgPhase3PersistenceError(
            f"{field} must be an RFC3339 UTC timestamp ending in Z",
            code="LKG_PHASE3_REFERENCE_MALFORMED",
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise LkgPhase3PersistenceError(
            f"{field} is not a valid calendar timestamp",
            code="LKG_PHASE3_REFERENCE_MALFORMED",
        ) from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise LkgPhase3PersistenceError(
            f"{field} must be UTC", code="LKG_PHASE3_REFERENCE_MALFORMED"
        )
    return parsed


def _rfc3339_utc(value: object, *, field: str) -> str:
    _parse_rfc3339_utc(value, field=field)
    assert isinstance(value, str)
    return value


def _object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(key)
        result[key] = value
    return result


def _record_document_digest(document: Mapping[str, object]) -> str:
    return hashlib.sha256(
        LKG_PHASE3_REFERENCE_HASH_DOMAIN + canonical_json_bytes(document)
    ).hexdigest()


def _validate_document(document: object) -> dict[str, object]:
    if not isinstance(document, dict) or frozenset(document) != _DOCUMENT_FIELDS:
        raise LkgPhase3PersistenceError(
            "authority reference document fields do not match schema",
            code="LKG_PHASE3_REFERENCE_MALFORMED",
        )
    if document["record_schema_version"] != LKG_PHASE3_REFERENCE_SCHEMA_VERSION:
        raise LkgPhase3PersistenceError(
            "authority reference schema version is unsupported",
            code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
        )
    _sequence_number(document["sequence_number"])
    for field in (
        "canonical_evaluation_digest",
        "source_run_binding_sha256",
        "source_run_seal_digest",
        "source_sealed_phase1_chain_head_sha256",
        "phase2_source_binding_digest",
        "search_configuration_digest",
        "qualification_manifest_sha256",
        "qualification_ordered_query_ids_sha256",
        "qualification_query_id_array_sha256",
        "qualification_query_array_sha256",
        "previous_record_digest",
    ):
        _sha256(document[field], field=field)
    for field in (
        "source_run_id",
        "threshold_stratum",
        "collection_name",
        "index_identity",
        "data_identity",
        "qualification_dataset_id",
        "qualification_dataset_version",
        "qualification_query_role",
        "environment_identity",
        "source_revision",
    ):
        _canonical_text(document[field], field=field)
    try:
        metric = Metric(document["metric"])
    except (TypeError, ValueError) as exc:
        raise LkgPhase3PersistenceError(
            "metric is unsupported", code="LKG_PHASE3_REFERENCE_MALFORMED"
        ) from exc
    if metric.value != document["metric"]:
        raise LkgPhase3PersistenceError(
            "metric is not canonical", code="LKG_PHASE3_REFERENCE_MALFORMED"
        )
    _positive_int(document["evaluated_ef"], field="evaluated_ef")
    _positive_int(
        document["qualification_expected_query_count"],
        field="qualification_expected_query_count",
    )
    _rfc3339_utc(document["persisted_at_utc"], field="persisted_at_utc")
    sequence = document["sequence_number"]
    previous = document["previous_record_digest"]
    if (sequence == 0) != (previous == _GENESIS_DIGEST):
        raise LkgPhase3PersistenceError(
            "sequence_number and previous_record_digest are inconsistent",
            code="LKG_PHASE3_REFERENCE_CHAIN_INVALID",
        )
    return dict(document)


def _reference_from_document(
    document: object,
    *,
    canonical_record_digest: object,
) -> PersistedLkgPhase3AuthorityReference:
    validated = _validate_document(document)
    digest = _sha256(canonical_record_digest, field="canonical_record_digest")
    if _record_document_digest(validated) != digest:
        raise LkgPhase3PersistenceError(
            "canonical record digest does not match its document",
            code="LKG_PHASE3_REFERENCE_DIGEST_MISMATCH",
        )
    return PersistedLkgPhase3AuthorityReference(
        **validated,
        canonical_record_digest=digest,
    )


def _document_from_reference(
    reference: PersistedLkgPhase3AuthorityReference,
) -> dict[str, object]:
    return {field: getattr(reference, field) for field in _DOCUMENT_FIELDS}


def _authority_identity_document(authority: object) -> dict[str, object]:
    if not isinstance(authority, LkgPhase3Authority):
        raise LkgPhase3PersistenceError(
            "only a D1 LkgPhase3Authority may be persisted",
            code="LKG_PHASE3_AUTHORITY_REQUIRED",
        )
    try:
        run_binding = authority.run_binding
        if authority.source_run_binding_sha256 != run_binding.sha256:
            raise ValueError("run-binding digest mismatch")
        if authority.search_configuration_digest != search_configuration_sha256(
            authority.search_configuration
        ):
            raise ValueError("search-configuration digest mismatch")
        values: dict[str, object] = {
            "canonical_evaluation_digest": authority.canonical_evaluation_digest,
            "source_run_id": authority.source_run_id,
            "source_run_binding_sha256": authority.source_run_binding_sha256,
            "source_run_seal_digest": authority.source_run_seal_digest,
            "source_sealed_phase1_chain_head_sha256": (
                authority.source_sealed_phase1_chain_head_sha256
            ),
            "phase2_source_binding_digest": authority.phase2_source_binding_digest,
            "evaluated_ef": authority.evaluated_ef,
            "search_configuration_digest": authority.search_configuration_digest,
            "metric": authority.metric.value,
            "threshold_stratum": authority.threshold_stratum,
            "collection_name": authority.collection_name,
            "index_identity": authority.index_identity,
            "data_identity": authority.data_identity,
            "qualification_dataset_id": authority.qualification_dataset_id,
            "qualification_dataset_version": authority.qualification_dataset_version,
            "qualification_manifest_sha256": authority.qualification_manifest_sha256,
            "qualification_query_role": authority.qualification_query_role,
            "qualification_ordered_query_ids_sha256": (
                authority.qualification_ordered_query_ids_sha256
            ),
            "qualification_query_id_array_sha256": (
                authority.qualification_query_id_array_sha256
            ),
            "qualification_query_array_sha256": (
                authority.qualification_query_array_sha256
            ),
            "qualification_expected_query_count": (
                authority.qualification_expected_query_count
            ),
            "environment_identity": authority.environment_identity,
            "source_revision": authority.source_revision,
        }
    except (AttributeError, TypeError, ValueError) as exc:
        raise LkgPhase3PersistenceError(
            "D1 authority identity is malformed",
            code="LKG_PHASE3_AUTHORITY_MALFORMED",
        ) from exc

    # Reuse the strict persisted-document validator before any DB lock is held.
    probe = {
        "record_schema_version": LKG_PHASE3_REFERENCE_SCHEMA_VERSION,
        "sequence_number": 0,
        **values,
        "persisted_at_utc": "2000-01-01T00:00:00Z",
        "previous_record_digest": _GENESIS_DIGEST,
    }
    _validate_document(probe)
    return values


def _prepare_database_path(
    raw_path: str | os.PathLike[str],
) -> tuple[str, bool]:
    try:
        path = os.fspath(raw_path)
    except TypeError as exc:
        raise LkgPhase3PersistenceError(
            "database path is invalid", code="LKG_PHASE3_REFERENCE_INVALID_PATH"
        ) from exc
    if not isinstance(path, str) or not path.strip():
        raise LkgPhase3PersistenceError(
            "database path must be non-empty",
            code="LKG_PHASE3_REFERENCE_INVALID_PATH",
        )
    expanded = os.path.abspath(os.path.expanduser(path))
    parent = os.path.dirname(expanded)
    try:
        if os.path.islink(expanded) or os.path.islink(parent):
            raise LkgPhase3PersistenceError(
                "database path and immediate parent must not be symlinks",
                code="LKG_PHASE3_REFERENCE_INVALID_PATH",
            )
        if not os.path.isdir(parent):
            raise LkgPhase3PersistenceError(
                "database parent directory must exist",
                code="LKG_PHASE3_REFERENCE_INVALID_PATH",
            )

        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(expanded, flags, 0o600)
        except FileExistsError:
            file_stat = os.lstat(expanded)
            if (
                not stat.S_ISREG(file_stat.st_mode)
                or file_stat.st_nlink != 1
                or stat.S_IMODE(file_stat.st_mode) != 0o600
            ):
                raise LkgPhase3PersistenceError(
                    "existing database must be a private 0600 single-link regular file",
                    code="LKG_PHASE3_REFERENCE_FILE_HARDENING_FAILED",
                )
            return os.path.realpath(expanded), False

        try:
            # The path is exclusively ours, so tightening an umask-reduced mode
            # cannot mutate pre-existing user data.
            os.fchmod(descriptor, 0o600)
            file_stat = os.fstat(descriptor)
            if (
                not stat.S_ISREG(file_stat.st_mode)
                or file_stat.st_nlink != 1
                or stat.S_IMODE(file_stat.st_mode) != 0o600
            ):
                raise LkgPhase3PersistenceError(
                    "new database file hardening failed",
                    code="LKG_PHASE3_REFERENCE_FILE_HARDENING_FAILED",
                )
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        parent_descriptor = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(parent_descriptor)
        finally:
            os.close(parent_descriptor)
        return os.path.realpath(expanded), True
    except LkgPhase3PersistenceError:
        raise
    except OSError as exc:
        raise LkgPhase3PersistenceError(
            f"failed to inspect or create database path: {exc}",
            code="LKG_PHASE3_REFERENCE_INVALID_PATH",
        ) from exc


class LkgPhase3AuthorityReferenceStore:
    """Hardened SQLite append-only D2 authority-reference ledger."""

    def __init__(
        self,
        db_path: str | os.PathLike[str],
        *,
        lock_timeout_seconds: float = 30.0,
    ) -> None:
        if (
            isinstance(lock_timeout_seconds, bool)
            or not isinstance(lock_timeout_seconds, (int, float))
            or not math.isfinite(float(lock_timeout_seconds))
            or lock_timeout_seconds <= 0
        ):
            raise LkgPhase3PersistenceError(
                "lock_timeout_seconds must be finite and positive",
                code="LKG_PHASE3_REFERENCE_INVALID_TIMEOUT",
            )
        self._db_path, self._created_new_database = _prepare_database_path(db_path)
        self._conn: sqlite3.Connection | None = None
        self._lock_timeout_seconds = float(lock_timeout_seconds)
        if not self._created_new_database:
            self._preflight_existing_database_read_only()
        self._initialize()

    def _preflight_existing_database_read_only(self) -> None:
        """Reject unversioned/missing schemas before any writable SQLite open."""

        uri = f"{Path(self._db_path).as_uri()}?mode=ro"
        try:
            connection = sqlite3.connect(uri, uri=True, isolation_level=None)
            try:
                user_version = connection.execute(
                    "PRAGMA user_version;"
                ).fetchone()[0]
                rows = connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type IN ('table', 'trigger') "
                    "AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
                journal_mode = connection.execute(
                    "PRAGMA journal_mode;"
                ).fetchone()[0]
            finally:
                connection.close()
        except (sqlite3.Error, OSError) as exc:
            raise LkgPhase3PersistenceError(
                f"pre-existing database is not a readable D2 schema: {exc}",
                code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
            ) from exc
        if (
            user_version != _DATABASE_SCHEMA_VERSION
            or frozenset(row[0] for row in rows) != _EXPECTED_SCHEMA_OBJECTS
            or str(journal_mode).lower() != "delete"
        ):
            raise LkgPhase3PersistenceError(
                "pre-existing database schema or journal mode is unsupported",
                code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
            )

    def _initialize(self) -> None:
        try:
            self._conn = sqlite3.connect(
                self._db_path,
                timeout=self._lock_timeout_seconds,
                isolation_level=None,
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.execute(
                f"PRAGMA busy_timeout = {int(self._lock_timeout_seconds * 1000)};"
            )
            if self._created_new_database:
                self._conn.execute("PRAGMA journal_mode = DELETE;")
            self._conn.execute("PRAGMA synchronous = FULL;")
            self._conn.execute("PRAGMA foreign_keys = ON;")
            self._conn.execute("PRAGMA trusted_schema = OFF;")
            self._verify_file_hardening()
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                user_version = self._conn.execute(
                    "PRAGMA user_version;"
                ).fetchone()[0]
                if self._created_new_database:
                    if user_version != 0 or self._schema_object_names():
                        raise LkgPhase3PersistenceError(
                            "new database was mutated before schema initialization",
                            code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
                        )
                    self._conn.execute(_EXPECTED_TABLE_SQL)
                    for trigger_sql in _EXPECTED_TRIGGER_SQL.values():
                        self._conn.execute(trigger_sql)
                    self._conn.execute(
                        f"PRAGMA user_version = {_DATABASE_SCHEMA_VERSION};"
                    )
                elif user_version == 0:
                    raise LkgPhase3PersistenceError(
                        "pre-existing unversioned database is not eligible for initialization",
                        code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
                    )
                self._verify_schema()
                self._load_all_locked()
                self._conn.execute("COMMIT;")
                self._fsync_parent_directory()
            except BaseException:
                self._rollback_quietly()
                raise
        except LkgPhase3PersistenceError:
            self.close()
            raise
        except (sqlite3.Error, OSError) as exc:
            self._rollback_quietly()
            self.close()
            raise LkgPhase3PersistenceError(
                f"failed to initialize D2 database: {exc}",
                code="LKG_PHASE3_REFERENCE_DB_INIT_FAILED",
            ) from exc

    def _require_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise LkgPhase3PersistenceError(
                "authority reference store is closed",
                code="LKG_PHASE3_REFERENCE_CLOSED",
            )
        return self._conn

    def _verify_file_hardening(self) -> None:
        try:
            file_stat = os.stat(self._db_path, follow_symlinks=False)
        except OSError as exc:
            raise LkgPhase3PersistenceError(
                f"failed to inspect database file: {exc}",
                code="LKG_PHASE3_REFERENCE_FILE_HARDENING_FAILED",
            ) from exc
        if (
            not stat.S_ISREG(file_stat.st_mode)
            or stat.S_IMODE(file_stat.st_mode) != 0o600
            or file_stat.st_nlink != 1
        ):
            raise LkgPhase3PersistenceError(
                "database must be a private 0600 single-link regular file",
                code="LKG_PHASE3_REFERENCE_FILE_HARDENING_FAILED",
            )

    def _schema_object_names(self) -> frozenset[str]:
        connection = self._require_connection()
        rows = connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type IN ('table', 'trigger') AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return frozenset(row[0] for row in rows)

    def _verify_schema(self) -> None:
        connection = self._require_connection()
        if self._schema_object_names() != _EXPECTED_SCHEMA_OBJECTS:
            raise LkgPhase3PersistenceError(
                "database schema-object inventory mismatch",
                code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
            )
        table_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (_TABLE_NAME,),
        ).fetchone()
        if (
            table_row is None
            or _normalize_schema_sql(table_row[0])
            != _normalize_schema_sql(_EXPECTED_TABLE_SQL)
        ):
            raise LkgPhase3PersistenceError(
                "database table SQL mismatch",
                code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
            )
        trigger_sql = {
            row[0]: _normalize_schema_sql(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='trigger'"
            ).fetchall()
        }
        expected_triggers = {
            name: _normalize_schema_sql(sql)
            for name, sql in _EXPECTED_TRIGGER_SQL.items()
        }
        if trigger_sql != expected_triggers:
            raise LkgPhase3PersistenceError(
                "database trigger SQL mismatch",
                code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
            )
        actual_columns = tuple(
            row[1]
            for row in connection.execute(f"PRAGMA table_info({_TABLE_NAME})").fetchall()
        )
        if actual_columns != _COLUMN_NAMES:
            raise LkgPhase3PersistenceError(
                "database columns mismatch",
                code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
            )
        if connection.execute("PRAGMA user_version;").fetchone()[0] != 1:
            raise LkgPhase3PersistenceError(
                "database user_version mismatch",
                code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
            )
        for pragma, expected in (
            ("foreign_keys", 1),
            ("trusted_schema", 0),
            ("synchronous", 2),
        ):
            if connection.execute(f"PRAGMA {pragma};").fetchone()[0] != expected:
                raise LkgPhase3PersistenceError(
                    f"database PRAGMA {pragma} mismatch",
                    code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
                )
        journal = connection.execute("PRAGMA journal_mode;").fetchone()[0]
        if str(journal).lower() != "delete":
            raise LkgPhase3PersistenceError(
                "database journal_mode mismatch",
                code="LKG_PHASE3_REFERENCE_SCHEMA_MISMATCH",
            )

    def _reference_from_row(
        self,
        row: sqlite3.Row,
    ) -> PersistedLkgPhase3AuthorityReference:
        try:
            document = json.loads(
                row["record_document_json"],
                object_pairs_hook=_object_without_duplicate_keys,
            )
        except (
            json.JSONDecodeError,
            _DuplicateJsonField,
            TypeError,
            ValueError,
            UnicodeError,
        ) as exc:
            raise LkgPhase3PersistenceError(
                "stored record JSON is malformed",
                code="LKG_PHASE3_REFERENCE_MALFORMED",
            ) from exc
        reference = _reference_from_document(
            document,
            canonical_record_digest=row["canonical_record_digest"],
        )
        expected_json = canonical_json_bytes(_document_from_reference(reference)).decode(
            "utf-8"
        )
        if row["record_document_json"] != expected_json:
            raise LkgPhase3PersistenceError(
                "stored JSON is not byte-identical canonical JSON",
                code="LKG_PHASE3_REFERENCE_NONCANONICAL",
            )
        for column in _COLUMN_NAMES[:-2]:
            if row[column] != getattr(reference, column):
                raise LkgPhase3PersistenceError(
                    f"stored column {column} does not match canonical JSON",
                    code="LKG_PHASE3_REFERENCE_ROW_MISMATCH",
                )
        return reference

    def _load_all_locked(self) -> tuple[PersistedLkgPhase3AuthorityReference, ...]:
        connection = self._require_connection()
        rows = connection.execute(
            f"SELECT * FROM {_TABLE_NAME} ORDER BY sequence_number"
        ).fetchall()
        references: list[PersistedLkgPhase3AuthorityReference] = []
        expected_previous = _GENESIS_DIGEST
        previous_timestamp: datetime | None = None
        for expected_sequence, row in enumerate(rows):
            reference = self._reference_from_row(row)
            if reference.sequence_number != expected_sequence:
                raise LkgPhase3PersistenceError(
                    "authority reference sequence is discontinuous",
                    code="LKG_PHASE3_REFERENCE_CHAIN_INVALID",
                )
            if reference.previous_record_digest != expected_previous:
                raise LkgPhase3PersistenceError(
                    "authority reference previous digest is invalid",
                    code="LKG_PHASE3_REFERENCE_CHAIN_INVALID",
                )
            timestamp = _parse_rfc3339_utc(
                reference.persisted_at_utc,
                field="persisted_at_utc",
            )
            if previous_timestamp is not None and timestamp <= previous_timestamp:
                raise LkgPhase3PersistenceError(
                    "distinct authority timestamps are not strictly increasing",
                    code="LKG_PHASE3_REFERENCE_TIMESTAMP_ORDER_INVALID",
                )
            references.append(reference)
            expected_previous = reference.canonical_record_digest
            previous_timestamp = timestamp
        return tuple(references)

    def append(
        self,
        authority: LkgPhase3Authority,
        *,
        persisted_at_utc: str,
    ) -> LkgPhase3AuthorityAppendResult:
        """Append once or return the exact existing reference idempotently."""

        # Deliberately outside BEGIN IMMEDIATE: authority projection must never
        # cause upstream ledger work while the D2 write lock is held.
        identity = _authority_identity_document(authority)
        timestamp = _rfc3339_utc(persisted_at_utc, field="persisted_at_utc")
        connection = self._require_connection()
        self._verify_file_hardening()
        try:
            connection.execute("BEGIN IMMEDIATE;")
            self._verify_schema()
            references = self._load_all_locked()
            for reference in references:
                if (
                    reference.canonical_evaluation_digest
                    == identity["canonical_evaluation_digest"]
                ):
                    existing_document = _document_from_reference(reference)
                    if any(
                        existing_document[field] != identity[field]
                        for field in _IDENTITY_FIELDS
                    ):
                        raise LkgPhase3PersistenceError(
                            "evaluation digest is bound to different identity data",
                            code="LKG_PHASE3_REFERENCE_IDENTITY_COLLISION",
                        )
                    connection.execute("COMMIT;")
                    return LkgPhase3AuthorityAppendResult(
                        reference=reference,
                        appended=False,
                    )

            sequence = len(references)
            if references and _parse_rfc3339_utc(
                timestamp,
                field="persisted_at_utc",
            ) <= _parse_rfc3339_utc(
                references[-1].persisted_at_utc,
                field="previous persisted_at_utc",
            ):
                raise LkgPhase3PersistenceError(
                    "new authority timestamp must be later than the previous record",
                    code="LKG_PHASE3_REFERENCE_TIMESTAMP_ORDER_INVALID",
                )
            previous_digest = (
                references[-1].canonical_record_digest
                if references
                else _GENESIS_DIGEST
            )
            document: dict[str, object] = {
                "record_schema_version": LKG_PHASE3_REFERENCE_SCHEMA_VERSION,
                "sequence_number": sequence,
                **identity,
                "persisted_at_utc": timestamp,
                "previous_record_digest": previous_digest,
            }
            digest = _record_document_digest(document)
            reference = _reference_from_document(
                document,
                canonical_record_digest=digest,
            )
            canonical_json = canonical_json_bytes(document).decode("utf-8")
            row_values = [getattr(reference, column) for column in _COLUMN_NAMES[:-2]]
            row_values.extend((reference.canonical_record_digest, canonical_json))
            placeholders = ",".join("?" for _ in _COLUMN_NAMES)
            connection.execute(
                f"INSERT INTO {_TABLE_NAME} ({','.join(_COLUMN_NAMES)}) "
                f"VALUES ({placeholders})",
                tuple(row_values),
            )
            connection.execute("COMMIT;")
            self._fsync_parent_directory()
            return LkgPhase3AuthorityAppendResult(reference=reference, appended=True)
        except BaseException as exc:
            self._rollback_quietly()
            if isinstance(exc, LkgPhase3PersistenceError):
                raise
            if isinstance(exc, (sqlite3.Error, OSError)):
                raise LkgPhase3PersistenceError(
                    f"failed to append authority reference: {exc}",
                    code="LKG_PHASE3_REFERENCE_APPEND_FAILED",
                ) from exc
            raise

    def load_all(self) -> tuple[PersistedLkgPhase3AuthorityReference, ...]:
        """Load and verify every reference without creating authority."""

        connection = self._require_connection()
        self._verify_file_hardening()
        try:
            connection.execute("BEGIN;")
            self._verify_schema()
            references = self._load_all_locked()
            connection.execute("COMMIT;")
            return references
        except BaseException as exc:
            self._rollback_quietly()
            if isinstance(exc, LkgPhase3PersistenceError):
                raise
            if isinstance(exc, (sqlite3.Error, OSError)):
                raise LkgPhase3PersistenceError(
                    f"failed to load authority references: {exc}",
                    code="LKG_PHASE3_REFERENCE_LOAD_FAILED",
                ) from exc
            raise

    def load_latest(self) -> PersistedLkgPhase3AuthorityReference | None:
        """Return the latest verified identity reference, never authority."""

        references = self.load_all()
        return references[-1] if references else None

    def load_verified_latest(
        self,
    ) -> VerifiedLatestLkgPhase3AuthorityReference | None:
        """Issue a snapshot of the head from one complete D2 verification.

        The wrapper is constructed before the coherent read transaction ends.
        It records what was latest at that refresh instant only; a later append
        does not mutate or invalidate the already-issued immutable snapshot.
        This path performs no D1, Checkpoint-C, Phase-1, or Phase-2 work.
        """

        connection = self._require_connection()
        self._verify_file_hardening()
        try:
            connection.execute("BEGIN;")
            self._verify_schema()
            references = self._load_all_locked()
            verified_latest = (
                None
                if not references
                else VerifiedLatestLkgPhase3AuthorityReference._from_verified_head(
                    reference=references[-1],
                    construction_token=_VERIFIED_LATEST_CONSTRUCTION_TOKEN,
                )
            )
            connection.execute("COMMIT;")
            return verified_latest
        except BaseException as exc:
            self._rollback_quietly()
            if isinstance(exc, LkgPhase3PersistenceError):
                raise
            if isinstance(exc, (sqlite3.Error, OSError)):
                raise LkgPhase3PersistenceError(
                    f"failed to load verified latest authority reference: {exc}",
                    code="LKG_PHASE3_REFERENCE_LOAD_FAILED",
                ) from exc
            raise

    def _rollback_quietly(self) -> None:
        if self._conn is not None:
            try:
                self._conn.execute("ROLLBACK;")
            except sqlite3.Error:
                pass

    def _fsync_parent_directory(self) -> None:
        descriptor = os.open(os.path.dirname(self._db_path), os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()
