"""Restart-durable evidence ledger for the EXP-009 1,200-query recall audit.

Purpose:
    Persist one capped-recall observation per DATASET-002 background
    recall-audit query, for exactly one run, so the mean-capped-recall
    Hoeffding bound ADR-008 already assigns to this stream can be evaluated
    from durable, restart-safe evidence rather than an in-memory list.
Inputs:
    A per-query ``RecallAuditObservation`` supplied by a later live/fake
    producer. This module never executes a query itself.
Outputs:
    Immutable stored observations or an explicit fail-closed refusal.
Dependencies:
    Python's standard-library SQLite implementation, this repository's
    existing ``SearchConfiguration``/``WorkloadIdentityBinding``/
    ``canonical_json_bytes`` contracts, and ``oracle.py::capped_threshold_
    recall`` for the authoritative recall formula. Never PyMilvus, routing,
    policy, or the sealed ``Stage4ExecutionLedger`` schema: this ledger is
    deliberately disjoint from that schema and never reads, writes, or
    reinterprets its hash chain.
Failure modes:
    A byte-identical replay of an already-stored ``query_id`` is accepted as
    an idempotent no-op. A different observation under the same ``query_id``
    is a fail-closed conflicting-duplicate refusal; the original record is
    retained unchanged. Corrupt, unexpected, unavailable, lock-contended, or
    run-mismatched storage raises ``RecallAuditLedgerError``.
Scope:
    No statistical interpretation (alpha, estimator/method version, pass/fail
    result) lives here. That belongs exclusively to
    ``canary_recall_audit_evaluation.Stage4RecallAuditEvaluation``, so this
    raw evidence remains reusable by a future, different evaluation method.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import unicodedata

from .artifacts import canonical_json_bytes, sha256_file, write_immutable_json
from .canary_workload import WorkloadIdentityBinding
from .config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from .oracle import capped_threshold_recall


__all__ = [
    "CanaryRecallAuditLedger",
    "RecallAuditAppendResult",
    "RecallAuditChainState",
    "RecallAuditLedgerError",
    "RecallAuditObservation",
    "publish_recall_audit_manifest",
]


_SCHEMA_VERSION = 2
_MANIFEST_SCHEMA_VERSION = "recall-audit-manifest-v1"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_MAX_TEXT_CODEPOINTS = 256
_EXPECTED_SCHEMA_OBJECTS = frozenset(
    {
        "recall_audit_run",
        "recall_audit_observations",
        "recall_audit_run_no_update",
        "recall_audit_run_no_delete",
        "recall_audit_observations_no_update",
        "recall_audit_observations_no_delete",
    }
)

# Domain separation matches canary_execution_ledger.py's proven hash-chain
# convention. The chain is over insertion (rowid) order, not query_id order,
# because recall-audit observations may legitimately arrive out of order from
# a live producer -- unlike the strictly sequential Stage4ExecutionLedger.
_GENESIS_DOMAIN = b"vdbench.canary_recall_audit_ledger.genesis.v1\0"
_RECORD_DOMAIN = b"vdbench.canary_recall_audit_ledger.record.v1\0"


class RecallAuditLedgerError(RuntimeError):
    """A durable ledger condition that must prevent recall-audit evaluation."""


def _ledger_error(code: str, cause: BaseException | None = None) -> RecallAuditLedgerError:
    error = RecallAuditLedgerError(code)
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
        or len(value) > _MAX_TEXT_CODEPOINTS
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError(f"{field} is not canonical")
    return value


def _sha256_text(value: object, *, field: str) -> str:
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


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _typed_result_ids(value: object, *, field: str) -> tuple[int, ...]:
    """Validate element types/non-negativity only; duplicates are left for
    ``capped_threshold_recall`` itself to reject, so that check is never
    duplicated here."""

    if not isinstance(value, (tuple, list)):
        raise ValueError(f"{field} must be a tuple or list of integers")
    normalized: list[int] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise ValueError(f"{field} must contain only non-negative integers")
        normalized.append(item)
    return tuple(normalized)


def _result_digest(ids: tuple[int, ...]) -> str:
    canonical = ",".join(str(value) for value in ids)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _search_configuration_document(configuration: SearchConfiguration) -> dict[str, object]:
    return {
        "metric": configuration.metric.value,
        "threshold_label": configuration.threshold_label,
        "radius": configuration.radius,
        "index_track": configuration.index_track.value,
        "ef": configuration.ef,
        "limit": configuration.limit,
        "consistency_level": configuration.consistency_level,
    }


def _search_configuration_from_document(document: dict[str, object]) -> SearchConfiguration:
    return SearchConfiguration(
        metric=Metric(document["metric"]),
        threshold_label=document["threshold_label"],
        radius=document["radius"],
        index_track=IndexTrack(document["index_track"]),
        ef=document["ef"],
        limit=document["limit"],
        consistency_level=document["consistency_level"],
    )


@dataclass(frozen=True, slots=True)
class RecallAuditObservation:
    """One capped-recall observation for one DATASET-002 recall-audit query.

    ``matched_count``, ``capped_recall``, and both result digests are
    derived at construction, never independently asserted:
    ``capped_recall`` is computed by calling
    ``oracle.py::capped_threshold_recall`` directly (its denominator is the
    oracle's true within-threshold count for *this* query, not a fixed cap),
    so this never duplicates or drifts from that formula.
    """

    query_id: int
    search_configuration: SearchConfiguration
    identity: WorkloadIdentityBinding
    dataset002_manifest_sha256: str
    dataset002_schema_version: int
    oracle_result_ids: tuple[int, ...]
    candidate_result_ids: tuple[int, ...]
    producer_run_id: str
    recorded_at_utc: str
    matched_count: int = 0
    capped_recall: float = 0.0
    oracle_result_sha256: str = ""
    candidate_result_sha256: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "query_id", _non_negative_int(self.query_id, field="query_id"))

        if not isinstance(self.search_configuration, SearchConfiguration):
            raise ValueError("search_configuration must be a SearchConfiguration")
        self.search_configuration.validate()
        if self.search_configuration.index_track is not IndexTrack.HNSW:
            raise ValueError("search_configuration.index_track must be HNSW for a candidate ef")
        if self.search_configuration.ef is None:
            raise ValueError("search_configuration.ef must be set for a candidate observation")

        if not isinstance(self.identity, WorkloadIdentityBinding):
            raise ValueError("identity must be a WorkloadIdentityBinding")
        try:
            self.identity.validate()
        except ContractViolation as exc:
            raise ValueError(f"identity is invalid: {exc}") from exc

        object.__setattr__(
            self,
            "dataset002_manifest_sha256",
            _sha256_text(self.dataset002_manifest_sha256, field="dataset002_manifest_sha256"),
        )
        from .dataset002 import DATASET002_SCHEMA_VERSION

        if self.dataset002_schema_version != DATASET002_SCHEMA_VERSION:
            raise ValueError(
                f"dataset002_schema_version must equal {DATASET002_SCHEMA_VERSION}"
            )

        raw_oracle_ids = _typed_result_ids(self.oracle_result_ids, field="oracle_result_ids")
        raw_candidate_ids = _typed_result_ids(
            self.candidate_result_ids, field="candidate_result_ids"
        )
        limit = self.search_configuration.limit
        if len(raw_oracle_ids) > limit:
            raise ValueError("oracle_result_ids must not exceed search_configuration.limit")
        if len(raw_candidate_ids) > limit:
            raise ValueError("candidate_result_ids must not exceed search_configuration.limit")

        try:
            capped_recall = capped_threshold_recall(raw_candidate_ids, raw_oracle_ids)
        except ContractViolation as exc:
            raise ValueError(f"result IDs are invalid: {exc}") from exc

        # No duplicates were possible past this point (capped_threshold_recall
        # would have rejected them), so sorting is a pure canonicalization,
        # never a silent data-loss dedup.
        oracle_ids = tuple(sorted(raw_oracle_ids))
        candidate_ids = tuple(sorted(raw_candidate_ids))

        object.__setattr__(self, "oracle_result_ids", oracle_ids)
        object.__setattr__(self, "candidate_result_ids", candidate_ids)
        object.__setattr__(self, "matched_count", len(set(candidate_ids) & set(oracle_ids)))
        object.__setattr__(self, "capped_recall", capped_recall)
        object.__setattr__(self, "oracle_result_sha256", _result_digest(oracle_ids))
        object.__setattr__(self, "candidate_result_sha256", _result_digest(candidate_ids))

        object.__setattr__(
            self, "producer_run_id", _canonical_text(self.producer_run_id, field="producer_run_id")
        )
        object.__setattr__(
            self, "recorded_at_utc", _timestamp(self.recorded_at_utc, field="recorded_at_utc")
        )


@dataclass(frozen=True, slots=True)
class RecallAuditAppendResult:
    """A non-exceptional append outcome; ``accepted=False`` never persists."""

    accepted: bool
    reason_code: str | None
    observation: RecallAuditObservation | None


@dataclass(frozen=True, slots=True)
class RecallAuditChainState:
    """Restart-safe, independently re-verified hash-chain state.

    ``chain_head_sha256`` is the ADR-008 "verified evidence digest" for this
    ledger's raw observations: it can only advance by re-deriving the chain
    from every stored row in insertion order, so it cannot be forged by
    editing a single row without invalidating everything built on top of it.
    """

    run_id: str
    binding_sha256: str
    record_count: int
    chain_head_sha256: str


def _genesis_sha256(*, run_id: str, binding_sha256: str) -> str:
    """The chain root binds both the run and the declared evidence binding,
    so two runs (or the same run under a different declared binding) can
    never coincidentally share a chain head."""

    return hashlib.sha256(
        _GENESIS_DOMAIN
        + canonical_json_bytes({"run_id": run_id, "binding_sha256": binding_sha256})
    ).hexdigest()


def _chain_document(observation: RecallAuditObservation) -> dict[str, object]:
    """The canonical, hashed content of one row -- derived from the already
    fully-validated ``RecallAuditObservation``, never from raw SQL text, so a
    chain check can never pass on data that would fail construction."""

    return {
        "query_id": observation.query_id,
        "search_configuration": _search_configuration_document(observation.search_configuration),
        "identity": observation.identity.to_document(),
        "dataset002_manifest_sha256": observation.dataset002_manifest_sha256,
        "dataset002_schema_version": observation.dataset002_schema_version,
        "oracle_result_ids": list(observation.oracle_result_ids),
        "candidate_result_ids": list(observation.candidate_result_ids),
        "producer_run_id": observation.producer_run_id,
        "recorded_at_utc": observation.recorded_at_utc,
    }


def _chain_record_sha256(previous: str, document: dict[str, object]) -> str:
    return hashlib.sha256(
        _RECORD_DOMAIN + previous.encode("ascii") + b"\0" + canonical_json_bytes(document)
    ).hexdigest()


class CanaryRecallAuditLedger:
    """Single-host SQLite ledger bound to exactly one recall-audit run."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        run_id: str,
        binding_sha256: str,
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
        self._run_id = _canonical_text(run_id, field="run_id")
        # Validated before any file/session is touched: a malformed binding
        # digest must never create even an empty database on disk.
        self._binding_sha256 = _sha256_text(binding_sha256, field="binding_sha256")
        self._lock_timeout_seconds = float(lock_timeout_seconds)
        self._validate_path()
        try:
            with self._session() as connection:
                self._initialize_schema(connection)
                self._bind_or_validate_run(connection)
            self._enforce_private_database_mode()
        except RecallAuditLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_CORRUPTED", exc) from exc
        except (OSError, sqlite3.Error) as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_UNAVAILABLE", exc) from exc

    @property
    def binding_sha256(self) -> str:
        """The immutable ADR-008 evidence binding this run is bound to."""

        return self._binding_sha256

    def append(self, observation: RecallAuditObservation) -> RecallAuditAppendResult:
        """Append one observation once; a conflicting duplicate fails closed.

        A genuinely new row extends the hash chain from a freshly
        re-verified head, computed under the same transaction, so the new
        link can never be built on top of an unverified or tampered chain.
        """

        if not isinstance(observation, RecallAuditObservation):
            raise TypeError("observation must be a RecallAuditObservation")
        search_configuration_json = canonical_json_bytes(
            _search_configuration_document(observation.search_configuration)
        ).decode("utf-8")
        identity_json = canonical_json_bytes(observation.identity.to_document()).decode("utf-8")
        oracle_ids_json = canonical_json_bytes(list(observation.oracle_result_ids)).decode("utf-8")
        candidate_ids_json = canonical_json_bytes(
            list(observation.candidate_result_ids)
        ).decode("utf-8")
        try:
            with self._session() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT search_configuration_json, identity_json, dataset002_manifest_sha256,
                           dataset002_schema_version, oracle_ids_json, candidate_ids_json,
                           producer_run_id, recorded_at_utc
                    FROM recall_audit_observations WHERE query_id = ?
                    """,
                    (observation.query_id,),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    expected = (
                        search_configuration_json,
                        identity_json,
                        observation.dataset002_manifest_sha256,
                        observation.dataset002_schema_version,
                        oracle_ids_json,
                        candidate_ids_json,
                        observation.producer_run_id,
                        observation.recorded_at_utc,
                    )
                    if tuple(existing) == expected:
                        return RecallAuditAppendResult(True, None, observation)
                    return RecallAuditAppendResult(
                        False, "QUERY_ID_CONFLICTING_DUPLICATE", None
                    )
                _, verified_head = self._verified_chain(connection)
                chain_sha256 = _chain_record_sha256(verified_head, _chain_document(observation))
                connection.execute(
                    """
                    INSERT INTO recall_audit_observations(
                        query_id, search_configuration_json, identity_json,
                        dataset002_manifest_sha256, dataset002_schema_version,
                        oracle_ids_json, candidate_ids_json, producer_run_id, recorded_at_utc,
                        previous_chain_sha256, chain_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        observation.query_id,
                        search_configuration_json,
                        identity_json,
                        observation.dataset002_manifest_sha256,
                        observation.dataset002_schema_version,
                        oracle_ids_json,
                        candidate_ids_json,
                        observation.producer_run_id,
                        observation.recorded_at_utc,
                        verified_head,
                        chain_sha256,
                    ),
                )
                connection.commit()
        except RecallAuditLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_CORRUPTED", exc) from exc
        return RecallAuditAppendResult(True, None, observation)

    def records(self) -> tuple[RecallAuditObservation, ...]:
        """Return every stored observation for this run, in insertion order.

        Every call re-verifies the complete hash chain; a tampered row is
        raised as ``RECALL_AUDIT_LEDGER_CHAIN_INVALID``/``_CORRUPTED``, never
        silently returned as if it were trustworthy evidence.
        """

        try:
            with self._session() as connection:
                observations, _ = self._verified_chain(connection)
                return observations
        except RecallAuditLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_CORRUPTED", exc) from exc

    def chain_state(self) -> RecallAuditChainState:
        """Return the restart-safe, independently re-verified chain head."""

        try:
            with self._session() as connection:
                observations, head = self._verified_chain(connection)
        except RecallAuditLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_CORRUPTED", exc) from exc
        return RecallAuditChainState(
            run_id=self._run_id,
            binding_sha256=self._binding_sha256,
            record_count=len(observations),
            chain_head_sha256=head,
        )

    def _verified_chain(
        self, connection: sqlite3.Connection
    ) -> tuple[tuple[RecallAuditObservation, ...], str]:
        """Re-derive every stored row's chain link from scratch and raise on
        any mismatch. A single-row edit therefore cannot be substituted
        without invalidating this pass, independent of the append-only
        triggers -- this is the check that matters against a writer that
        bypasses ``sqlite3`` entirely."""

        genesis = _genesis_sha256(run_id=self._run_id, binding_sha256=self._binding_sha256)
        rows = connection.execute(
            """
            SELECT query_id, search_configuration_json, identity_json,
                   dataset002_manifest_sha256, dataset002_schema_version,
                   oracle_ids_json, candidate_ids_json, producer_run_id, recorded_at_utc,
                   previous_chain_sha256, chain_sha256
            FROM recall_audit_observations ORDER BY rowid ASC
            """
        ).fetchall()
        observations: list[RecallAuditObservation] = []
        previous = genesis
        for row in rows:
            if not isinstance(row, tuple) or len(row) != 11:
                raise _ledger_error("RECALL_AUDIT_LEDGER_CORRUPTED")
            (
                query_id,
                search_configuration_json,
                identity_json,
                manifest_sha256,
                schema_version,
                oracle_ids_json,
                candidate_ids_json,
                producer_run_id,
                recorded_at_utc,
                stored_previous,
                stored_chain,
            ) = row
            if (
                stored_previous != previous
                or not isinstance(stored_chain, str)
                or _SHA256_RE.fullmatch(stored_chain) is None
            ):
                raise _ledger_error("RECALL_AUDIT_LEDGER_CHAIN_INVALID")
            try:
                observation = RecallAuditObservation(
                    query_id=query_id,
                    search_configuration=_search_configuration_from_document(
                        json.loads(search_configuration_json)
                    ),
                    identity=WorkloadIdentityBinding(**json.loads(identity_json)),
                    dataset002_manifest_sha256=manifest_sha256,
                    dataset002_schema_version=schema_version,
                    oracle_result_ids=tuple(json.loads(oracle_ids_json)),
                    candidate_result_ids=tuple(json.loads(candidate_ids_json)),
                    producer_run_id=producer_run_id,
                    recorded_at_utc=recorded_at_utc,
                )
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise _ledger_error("RECALL_AUDIT_LEDGER_CORRUPTED", exc) from exc
            if _chain_record_sha256(previous, _chain_document(observation)) != stored_chain:
                raise _ledger_error("RECALL_AUDIT_LEDGER_CHAIN_INVALID")
            observations.append(observation)
            previous = stored_chain
        return tuple(observations), previous

    def _validate_path(self) -> None:
        parent = self.path.parent
        try:
            parent_status = parent.stat()
        except OSError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_DIRECTORY_UNAVAILABLE", exc) from exc
        if not stat.S_ISDIR(parent_status.st_mode) or stat.S_IMODE(parent_status.st_mode) & 0o077:
            raise _ledger_error("RECALL_AUDIT_LEDGER_DIRECTORY_NOT_PRIVATE")
        try:
            file_status = self.path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_UNAVAILABLE", exc) from exc
        if not stat.S_ISREG(file_status.st_mode) or stat.S_ISLNK(file_status.st_mode):
            raise _ledger_error("RECALL_AUDIT_LEDGER_PATH_INVALID")

    def _enforce_private_database_mode(self) -> None:
        try:
            os.chmod(self.path, 0o600)
            if stat.S_IMODE(self.path.stat().st_mode) & 0o077:
                raise _ledger_error("RECALL_AUDIT_LEDGER_FILE_NOT_PRIVATE")
        except RecallAuditLedgerError:
            raise
        except OSError as exc:
            raise _ledger_error("RECALL_AUDIT_LEDGER_UNAVAILABLE", exc) from exc

    def _connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self._lock_timeout_seconds,
            isolation_level=None,
        )
        try:
            connection.execute(f"PRAGMA busy_timeout = {int(self._lock_timeout_seconds * 1000)}")
            connection.execute("PRAGMA trusted_schema = OFF")
            journal_mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()
            if journal_mode is None or journal_mode[0] != "delete":
                raise _ledger_error("RECALL_AUDIT_LEDGER_JOURNAL_MODE_INVALID")
            connection.execute("PRAGMA synchronous = FULL")
            return connection
        except BaseException:
            connection.close()
            raise

    @contextmanager
    def _session(self):
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
                raise _ledger_error("RECALL_AUDIT_LEDGER_CORRUPTED")
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
                    CREATE TABLE recall_audit_run (
                        run_id TEXT PRIMARY KEY NOT NULL,
                        binding_sha256 TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    # insertion_seq (not query_id) is the declared INTEGER
                    # PRIMARY KEY: SQLite aliases rowid to whichever column
                    # is declared that way, so making query_id the primary
                    # key would make "ORDER BY rowid" silently sort by
                    # query_id value instead of true arrival order.
                    """
                    CREATE TABLE recall_audit_observations (
                        insertion_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                        query_id INTEGER NOT NULL UNIQUE,
                        search_configuration_json TEXT NOT NULL,
                        identity_json TEXT NOT NULL,
                        dataset002_manifest_sha256 TEXT NOT NULL,
                        dataset002_schema_version INTEGER NOT NULL,
                        oracle_ids_json TEXT NOT NULL,
                        candidate_ids_json TEXT NOT NULL,
                        producer_run_id TEXT NOT NULL,
                        recorded_at_utc TEXT NOT NULL,
                        previous_chain_sha256 TEXT NOT NULL,
                        chain_sha256 TEXT UNIQUE NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER recall_audit_run_no_update
                    BEFORE UPDATE ON recall_audit_run
                    BEGIN SELECT RAISE(ABORT, 'recall audit run is append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER recall_audit_run_no_delete
                    BEFORE DELETE ON recall_audit_run
                    BEGIN SELECT RAISE(ABORT, 'recall audit run is append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER recall_audit_observations_no_update
                    BEFORE UPDATE ON recall_audit_observations
                    BEGIN SELECT RAISE(ABORT, 'recall audit observations are append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER recall_audit_observations_no_delete
                    BEFORE DELETE ON recall_audit_observations
                    BEGIN SELECT RAISE(ABORT, 'recall audit observations are append-only'); END
                    """
                )
                connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            elif version[0] != _SCHEMA_VERSION:
                raise _ledger_error("RECALL_AUDIT_LEDGER_SCHEMA_MISMATCH")
            schema_objects = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE name NOT LIKE 'sqlite_%' AND type IN ('table', 'view', 'trigger')
                    """
                )
            }
            if schema_objects != _EXPECTED_SCHEMA_OBJECTS:
                raise _ledger_error("RECALL_AUDIT_LEDGER_SCHEMA_MISMATCH")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def _bind_or_validate_run(self, connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            row = connection.execute(
                "SELECT run_id, binding_sha256 FROM recall_audit_run"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO recall_audit_run(run_id, binding_sha256) VALUES (?, ?)",
                    (self._run_id, self._binding_sha256),
                )
            elif row[0] != self._run_id:
                raise _ledger_error("RECALL_AUDIT_LEDGER_RUN_MISMATCH")
            elif row[1] != self._binding_sha256:
                raise _ledger_error("RECALL_AUDIT_LEDGER_BINDING_MISMATCH")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise


def publish_recall_audit_manifest(
    ledger: CanaryRecallAuditLedger, path: str | os.PathLike[str]
) -> dict[str, object]:
    """Write an external, immutable manifest of one ledger's current,
    independently re-verified chain state.

    This is the ADR-008 "external immutable manifest hash" requirement: the
    SQLite file's own permissions/triggers are not a signature against a
    hostile writer with raw file access, so the chain head is additionally
    committed to a separate, refuse-to-overwrite artifact whose own bytes are
    hashed and returned for the caller to record.
    """

    if not isinstance(ledger, CanaryRecallAuditLedger):
        raise TypeError("ledger must be a CanaryRecallAuditLedger")
    state = ledger.chain_state()
    document = {
        "schema_version": _MANIFEST_SCHEMA_VERSION,
        "run_id": state.run_id,
        "binding_sha256": state.binding_sha256,
        "record_count": state.record_count,
        "chain_head_sha256": state.chain_head_sha256,
    }
    target = Path(path)
    write_immutable_json(target, document)
    return {**document, "manifest_sha256": sha256_file(target)}
