"""Restart-durable, append-only ledger for typed LKG-qualification attempts.

Purpose:
    Persist one ``LkgQueryAttempt`` -- success or a specific typed failure
    -- per dispatched-query attempt, for exactly one qualification run, so
    Phase 1's raw evidence (and every non-silent failure) survives process
    restart instead of living only in memory. This module is deliberately
    disjoint from every existing canary/actuation ledger and schema: it
    never reads, writes, or reinterprets ``canary_recall_audit_ledger.py``'s
    hash chain, the Stage-4 evidence ledger, or any persisted shadow-trace
    schema. It structurally mirrors ``canary_recall_audit_ledger.py``'s
    proven append-only/hash-chain idiom (private-file-mode enforcement,
    ``BEGIN IMMEDIATE`` atomic transactions, append-only triggers, full
    chain re-verification on every read) without importing or subclassing
    any of its code.
Inputs:
    A complete, already-validated ``LkgRunBinding`` and its exact ordered
    DATASET-003 query-ID sequence (plain Python ``int`` values -- DATASET-003
    query IDs are always ``<i8``, never a string; see "Query-ID typing"
    below), matching ``qualification_ordered_query_ids_sha256``, plus one
    already-constructed ``LkgQueryAttempt`` per ``append`` call. This
    module never executes a query itself.
Outputs:
    Immutable stored attempts, or an explicit fail-closed refusal.
Complete run-binding persistence:
    The run table stores the *complete* canonical ``LkgRunBinding``
    document (``binding_document_json``), not merely its digest. On every
    open, the stored document is parsed strictly via
    ``lkg_run_binding.lkg_run_binding_from_document`` (which itself rejects
    unknown/missing/malformed/noncanonical fields and enforces a whole-
    document canonical byte round-trip), its canonical bytes are
    regenerated, ``run_binding_sha256`` is recomputed from those bytes, and
    the recomputed digest is checked against both the stored
    ``run_binding_sha256`` column and the caller-supplied binding's own
    digest. Exactly one row is ever permitted in the run table -- both by
    trigger (see "Single-run-per-file design" below) and by an explicit
    ``COUNT(*)`` check on every open/verify, since a hostile writer that
    bypasses the trigger could otherwise insert a second row that
    ``fetchone()`` would silently ignore.
Two distinct DATASET-003 workload identities, never conflated:
    ``qualification_query_id_array_sha256`` is the raw DATASET-003
    ``lkg_qualification_ids.npy`` artifact's own byte-exact SHA-256 --
    computed and verified exclusively by ``lkg_dataset003_loader.py``/
    ``sha256_file`` against the actual file on disk. This module never
    recomputes or re-verifies that hash (doing so by reserializing a NumPy
    array would conflate byte-exact artifact identity with semantic
    ordered-query identity, and would silently depend on NumPy
    serialization/header behavior across environments or versions).
    ``qualification_ordered_query_ids_sha256`` is a *separate*, semantic
    digest of the ordered query-ID sequence itself
    (``lkg_run_binding.lkg_ordered_query_ids_sha256`` -- domain prefix +
    fixed-width count + each ID as a signed 8-byte little-endian integer,
    with no array-serialization dependency at all). This is the digest
    ``ordered_query_ids`` is checked against here. Never trusted merely
    because a caller supplied it: this module recomputes
    ``qualification_ordered_query_ids_sha256`` from ``ordered_query_ids``
    and requires exact equality, and requires its length to equal
    ``run_binding.qualification_expected_query_count``, before a ledger
    file is opened or created at all. The exact ordered mapping is then
    persisted in ``lkg_qualification_workload_positions`` (all positions
    populated atomically with the run row), an append-only table
    independent of the attempts table. On reopen, the stored mapping is
    read back (``ORDER BY attempt_sequence ASC``), its semantic digest is
    independently recomputed from what is actually stored (not from the
    caller's in-memory argument), and checked again -- proving the
    persisted sequence, not just the constructor argument, still matches
    the run binding.
Query-ID typing:
    ``query_id`` is a plain SQLite ``INTEGER`` throughout this module's
    schema (``lkg_qualification_workload_positions.query_id`` and
    ``lkg_qualification_attempts.query_id``), not a JSON-encoded/text
    column. DATASET-003 query IDs are always ``numpy`` ``<i8`` integers
    (enforced by ``lkg_dataset003_loader.py``'s own array-dtype check); no
    approved contract for this ledger requires a string query ID, so the
    broader ``QueryId = int | str`` alias used elsewhere in this repository
    (for unrelated, non-DATASET-003 evidence types) is deliberately
    narrowed to plain ``int`` here.
Sequence identity, enforced at the SQL layer:
    Every attempt row carries a composite foreign key
    ``(run_id, attempt_sequence, query_id) ->
    lkg_qualification_workload_positions(run_id, attempt_sequence, query_id)``
    (``PRAGMA foreign_keys = ON``). Because
    ``lkg_qualification_workload_positions`` itself enforces a true
    bijection for each run (``PRIMARY KEY (run_id, attempt_sequence)`` and
    ``UNIQUE (run_id, query_id)``), this single foreign key is sufficient,
    at the database engine level, to make "the correct query ID at the
    wrong sequence", "a different query ID at a valid sequence", "the same
    query ID at two sequences", "an unknown query ID", and "an attempt for
    another run" all structurally impossible to insert -- independent of
    this module's own Python-level pre-check in ``append``, and
    independent of whether a writer bypasses ``sqlite3``'s Python API
    entirely (as long as it still goes through the same SQLite connection
    with foreign keys enabled).
Denormalized-column cross-checking:
    ``lkg_qualification_attempts`` stores ``run_id``/``query_id``/
    ``attempt_sequence``/``attempt_number``/``status`` as explicit columns
    *in addition to* the complete ``document_json``. Every append and every
    chain verification pass strictly reconstructs the ``LkgQueryAttempt``
    from ``document_json`` and compares each of those five columns against
    the reconstructed document's corresponding field, and separately
    re-verifies ``run_binding_sha256`` against this ledger's own bound
    binding. All five identity columns (plus the parsed document) are also
    bound directly into the per-record chain-hash input -- not merely
    ``document_json`` -- so a writer that tampers with a raw SQL column
    while leaving ``document_json`` byte-for-byte untouched (bypassing this
    ledger's own triggers) still invalidates the hash chain on the next
    ``records()``/``chain_state()`` call.
Single-run-per-file design:
    This ledger is, and has always been, one SQLite file per qualification
    run. ``lkg_qualification_run`` carries a
    ``BEFORE INSERT ... WHEN (SELECT COUNT(*) ...) >= 1`` trigger enforcing
    *at most one run row can ever exist* at the schema level, not merely by
    producer-side convention. The explicit ``run_id`` column on every
    attempt/workload-position row is therefore always constant within one
    file, but is still carried for three reasons: it lets the foreign key
    constraints above actually do their job at the SQL engine level; it
    keeps every row fully self-describing outside the file's own context;
    and it is forward-compatible with a possible future multi-run layout
    without a schema migration, should Phase 2/3 ever need one -- no such
    layout exists or is planned today.
Row identity and retry semantics:
    An attempt row's identity is the compound
    ``(run_id, query_id, attempt_number)`` -- *not* ``query_id`` alone --
    because a query that failed must be retryable as a genuinely new,
    distinctly numbered attempt without disturbing the durable record of
    the attempt(s) that already failed (see
    ``lkg_qualification_producer.py``'s crash-before-append semantics). A
    byte-identical replay of an already-stored
    ``(run_id, query_id, attempt_number)`` is accepted as an idempotent
    no-op. A *different* attempt under that same key is a fail-closed
    conflicting-duplicate refusal (``LkgAppendResult(accepted=False,
    conflict_reason="QUERY_ID_CONFLICTING_DUPLICATE")``); the original row
    is retained unchanged. This is distinct from a genuine append failure:
    a SQLite write/transaction/commit/connection/persistence fault raises
    ``LkgQualificationLedgerError`` -- an exception, never a silently
    returned ``accepted=False``.
Chain ordering:
    Every read orders strictly by the explicit ``insertion_seq`` column
    (``ORDER BY insertion_seq ASC``), never by SQLite's implicit rowid
    aliasing behavior for other columns, and never by ``query_id`` or
    ``attempt_sequence`` -- the chain is over true arrival order.
Failure modes:
    Corrupt, unexpected, unavailable, lock-contended, run-mismatched,
    workload-position-mismatched, or sequence-mismatched storage raises
    ``LkgQualificationLedgerError``. Every read re-verifies the complete
    hash chain from genesis; a tampered row is caught there, independent
    of this ledger's own append-only triggers.
Scope:
    No constituent-window or epoch assembly, sequencing enforcement, or
    PASSING/FAILING evaluation lives here -- that is LKG-qualification
    Phase 2/3, not yet implemented anywhere in this repository.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat

from .artifacts import canonical_json_bytes
from .config import ContractViolation, Metric
from .lkg_qualification_evidence import (
    LkgAttemptStatus,
    LkgQueryAttempt,
    LkgQueryObservation,
)
from .lkg_run_binding import (
    LkgRunBinding,
    lkg_ordered_query_ids_sha256,
    lkg_run_binding_document,
    lkg_run_binding_from_document,
    lkg_run_binding_sha256,
)


__all__ = [
    "LkgChainState",
    "LkgAppendResult",
    "LkgQualificationLedger",
    "LkgQualificationLedgerError",
]


_SCHEMA_VERSION = 4
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_EXPECTED_SCHEMA_OBJECTS = frozenset(
    {
        "lkg_qualification_run",
        "lkg_qualification_workload_positions",
        "lkg_qualification_attempts",
        "lkg_qualification_run_no_update",
        "lkg_qualification_run_no_delete",
        "lkg_qualification_run_single_row",
        "lkg_qualification_workload_positions_no_update",
        "lkg_qualification_workload_positions_no_delete",
        "lkg_qualification_attempts_no_update",
        "lkg_qualification_attempts_no_delete",
    }
)

# Domain separation is disjoint from every other ledger in this repository
# (e.g. canary_recall_audit_ledger.py's own genesis/record domains); no byte
# sequence produced here can ever be replayed as evidence for another chain.
_GENESIS_DOMAIN = b"vdbench.lkg_qualification_ledger.genesis.v4\0"
_RECORD_DOMAIN = b"vdbench.lkg_qualification_ledger.record.v4\0"


class LkgQualificationLedgerError(RuntimeError):
    """A durable ledger condition that must prevent LKG-qualification use."""


def _ledger_error(code: str, cause: BaseException | None = None) -> LkgQualificationLedgerError:
    error = LkgQualificationLedgerError(code)
    if cause is not None:
        error.__cause__ = cause
    return error


def _canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValueError(f"{field} must be a non-empty string of at most 256 characters")
    return value


_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _validate_ordered_query_ids(value: object) -> tuple[int, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("ordered_query_ids must be a sequence of integer query IDs")
    ids = tuple(value)
    if not ids:
        raise ValueError("ordered_query_ids must be non-empty")
    for query_id in ids:
        if isinstance(query_id, bool) or not isinstance(query_id, int):
            raise ValueError(
                "every ordered_query_ids entry must be a plain int "
                "(DATASET-003 query IDs are never strings)"
            )
        if not _INT64_MIN <= query_id <= _INT64_MAX:
            raise ValueError("every ordered_query_ids entry must fit in a signed 64-bit integer")
    if len(set(ids)) != len(ids):
        raise ValueError("ordered_query_ids must not contain duplicates")
    return ids


def _observation_document(observation: LkgQueryObservation) -> dict[str, object]:
    return {
        "query_id": observation.query_id,
        "metric": observation.metric.value,
        "threshold_stratum": observation.threshold_stratum,
        "ef": observation.ef,
        "recall": observation.recall,
        "latency_ms": observation.latency_ms,
        "start_ns": observation.start_ns,
        "end_ns": observation.end_ns,
        "exact_cardinality": observation.exact_cardinality,
        "threshold_violation_count": observation.threshold_violation_count,
    }


def _observation_from_document(document: dict[str, object]) -> LkgQueryObservation:
    return LkgQueryObservation(
        query_id=document["query_id"],
        metric=Metric(document["metric"]),
        threshold_stratum=document["threshold_stratum"],
        ef=document["ef"],
        recall=document["recall"],
        latency_ms=document["latency_ms"],
        start_ns=document["start_ns"],
        end_ns=document["end_ns"],
        exact_cardinality=document["exact_cardinality"],
        threshold_violation_count=document["threshold_violation_count"],
    )


def _attempt_document(attempt: LkgQueryAttempt) -> dict[str, object]:
    return {
        "query_id": attempt.query_id,
        "attempt_sequence": attempt.attempt_sequence,
        "attempt_number": attempt.attempt_number,
        "status": attempt.status.value,
        "error_code": attempt.error_code,
        "run_binding_sha256": attempt.run_binding_sha256,
        "observation": (
            _observation_document(attempt.observation)
            if attempt.observation is not None
            else None
        ),
    }


def _attempt_from_document(document: dict[str, object]) -> LkgQueryAttempt:
    observation_document = document["observation"]
    return LkgQueryAttempt(
        query_id=document["query_id"],
        attempt_sequence=document["attempt_sequence"],
        attempt_number=document["attempt_number"],
        status=LkgAttemptStatus(document["status"]),
        error_code=document["error_code"],
        run_binding_sha256=document["run_binding_sha256"],
        observation=(
            _observation_from_document(observation_document)
            if observation_document is not None
            else None
        ),
    )


def _genesis_sha256(*, run_id: str, run_binding_sha256: str) -> str:
    return hashlib.sha256(
        _GENESIS_DOMAIN
        + canonical_json_bytes({"run_id": run_id, "run_binding_sha256": run_binding_sha256})
    ).hexdigest()


def _chain_record_sha256(
    previous: str,
    *,
    run_id: str,
    query_id: int,
    attempt_sequence: int,
    attempt_number: int,
    status: str,
    document: dict[str, object],
) -> str:
    """Bind every denormalized identity column -- not just ``document`` --
    into the chain hash, so tampering with a raw SQL column while leaving
    ``document_json`` untouched (bypassing this ledger's own triggers)
    still invalidates the chain on the next verification pass."""

    payload = {
        "previous": previous,
        "run_id": run_id,
        "query_id": query_id,
        "attempt_sequence": attempt_sequence,
        "attempt_number": attempt_number,
        "status": status,
        "document": document,
    }
    return hashlib.sha256(_RECORD_DOMAIN + canonical_json_bytes(payload)).hexdigest()


@dataclass(frozen=True, slots=True)
class LkgAppendResult:
    accepted: bool
    conflict_reason: str | None
    attempt: LkgQueryAttempt | None


@dataclass(frozen=True, slots=True)
class LkgChainState:
    run_id: str
    run_binding_sha256: str
    record_count: int
    chain_head_sha256: str


class LkgQualificationLedger:
    """Single-host SQLite ledger bound to exactly one LKG-qualification run."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        run_binding: LkgRunBinding,
        ordered_query_ids: Sequence[int],
        lock_timeout_seconds: float = 1.0,
    ) -> None:
        if (
            isinstance(lock_timeout_seconds, bool)
            or not isinstance(lock_timeout_seconds, (int, float))
            or not math.isfinite(float(lock_timeout_seconds))
            or not 0.001 <= float(lock_timeout_seconds) <= 10.0
        ):
            raise ValueError("lock_timeout_seconds must be finite and between 0.001 and 10")
        if not isinstance(run_binding, LkgRunBinding):
            raise TypeError("run_binding must be an LkgRunBinding")
        self.path = Path(path)
        self._run_id = _canonical_text(run_binding.run_id, field="run_binding.run_id")
        self._run_binding = run_binding
        self._run_binding_sha256 = lkg_run_binding_sha256(run_binding)
        self._binding_document_json = canonical_json_bytes(
            lkg_run_binding_document(run_binding)
        ).decode("utf-8")

        self._ordered_query_ids = _validate_ordered_query_ids(ordered_query_ids)
        if len(self._ordered_query_ids) != run_binding.qualification_expected_query_count:
            raise ValueError(
                "ordered_query_ids length must equal "
                "run_binding.qualification_expected_query_count"
            )
        # Never trust a caller-supplied sequence merely because it was
        # passed in: cryptographically require it to be the exact sequence
        # the run binding committed to, before any file is opened. This
        # checks the semantic ordered-ID digest, never the raw .npy
        # artifact hash (qualification_query_id_array_sha256), which this
        # module has no involvement in verifying at all.
        computed_ordered_query_ids_sha256 = lkg_ordered_query_ids_sha256(self._ordered_query_ids)
        if computed_ordered_query_ids_sha256 != run_binding.qualification_ordered_query_ids_sha256:
            raise ValueError(
                "ordered_query_ids does not match "
                "run_binding.qualification_ordered_query_ids_sha256"
            )

        self._lock_timeout_seconds = float(lock_timeout_seconds)
        self._validate_path()
        try:
            with self._session() as connection:
                self._initialize_schema(connection)
                self._bind_or_validate_run(connection)
            self._enforce_private_database_mode()
        except LkgQualificationLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("LKG_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("LKG_LEDGER_CORRUPTED", exc) from exc
        except (OSError, sqlite3.Error) as exc:
            raise _ledger_error("LKG_LEDGER_UNAVAILABLE", exc) from exc

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def run_binding_sha256(self) -> str:
        """The complete LkgRunBinding digest every attempt in this run must share."""

        return self._run_binding_sha256

    def stored_run_binding(self) -> LkgRunBinding:
        """Independently reconstruct and verify the persisted LkgRunBinding.

        Reads the complete canonical document back from
        ``lkg_qualification_run``, strictly re-parses it (rejecting
        unknown/missing/malformed/noncanonical fields), and recomputes its
        digest -- proving the binding is reconstructable from the ledger
        file alone, not from this process's in-memory object.
        """

        try:
            with self._session() as connection:
                row_count = connection.execute(
                    "SELECT COUNT(*) FROM lkg_qualification_run"
                ).fetchone()[0]
                if row_count > 1:
                    raise _ledger_error("LKG_LEDGER_MULTIPLE_RUN_ROWS")
                row = connection.execute(
                    "SELECT run_id, schema_version, binding_document_json, run_binding_sha256 "
                    "FROM lkg_qualification_run"
                ).fetchone()
        except LkgQualificationLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("LKG_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("LKG_LEDGER_CORRUPTED", exc) from exc
        if row is None:
            raise _ledger_error("LKG_LEDGER_CORRUPTED")
        stored_run_id, stored_schema_version, binding_document_json, stored_sha256 = row
        if stored_schema_version != _SCHEMA_VERSION:
            raise _ledger_error("LKG_LEDGER_SCHEMA_MISMATCH")
        try:
            document = json.loads(binding_document_json)
            binding = lkg_run_binding_from_document(document)
        except (TypeError, ValueError, ContractViolation, json.JSONDecodeError) as exc:
            raise _ledger_error("LKG_LEDGER_CORRUPTED", exc) from exc
        recomputed_sha256 = lkg_run_binding_sha256(binding)
        if recomputed_sha256 != stored_sha256 or binding.run_id != stored_run_id:
            raise _ledger_error("LKG_LEDGER_CORRUPTED")
        return binding

    def stored_ordered_query_ids(self) -> tuple[int, ...]:
        """Independently reconstruct the ordered workload-position sequence.

        Reads ``lkg_qualification_workload_positions`` back
        (``ORDER BY attempt_sequence ASC``), and recomputes/verifies its
        semantic digest against the stored run binding's own
        ``qualification_ordered_query_ids_sha256`` -- proving the persisted
        sequence, not merely this process's constructor argument, still
        matches the binding. Never touches
        ``qualification_query_id_array_sha256`` (the raw ``.npy`` artifact
        hash), which this module has no involvement in verifying.
        """

        try:
            with self._session() as connection:
                rows = connection.execute(
                    """
                    SELECT attempt_sequence, query_id
                    FROM lkg_qualification_workload_positions
                    WHERE run_id = ?
                    ORDER BY attempt_sequence ASC
                    """,
                    (self._run_id,),
                ).fetchall()
        except sqlite3.OperationalError as exc:
            raise _ledger_error("LKG_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("LKG_LEDGER_CORRUPTED", exc) from exc
        expected_length = self._run_binding.qualification_expected_query_count
        if len(rows) != expected_length:
            raise _ledger_error("LKG_LEDGER_WORKLOAD_POSITIONS_CORRUPTED")
        for index, (attempt_sequence, query_id) in enumerate(rows):
            if attempt_sequence != index:
                raise _ledger_error("LKG_LEDGER_WORKLOAD_POSITIONS_CORRUPTED")
        ids = tuple(query_id for _, query_id in rows)
        recomputed = lkg_ordered_query_ids_sha256(ids)
        if recomputed != self._run_binding.qualification_ordered_query_ids_sha256:
            raise _ledger_error("LKG_LEDGER_WORKLOAD_POSITIONS_CORRUPTED")
        return ids

    def append(self, attempt: LkgQueryAttempt) -> LkgAppendResult:
        """Append one attempt once; a conflicting duplicate fails closed.

        Before any SQL statement runs, verifies (in Python) that
        ``attempt.query_id`` is exactly the query ID this ledger's bound,
        ordered DATASET-003 workload commits to at
        ``attempt.attempt_sequence``. The INSERT itself is additionally
        guarded by the SQL-level composite foreign key to
        ``lkg_qualification_workload_positions`` -- a raw writer that
        bypasses this Python check entirely is still rejected by SQLite. A
        genuinely new row extends the hash chain from a freshly
        re-verified head, computed under the same transaction. Any
        SQLite-level failure (lock contention, corruption, unavailability,
        or a constraint violation) raises ``LkgQualificationLedgerError`` --
        it is never conflated with a conflicting-duplicate refusal, and it
        never leaves a partially written row.
        """

        if not isinstance(attempt, LkgQueryAttempt):
            raise TypeError("attempt must be an LkgQueryAttempt")
        if attempt.run_binding_sha256 != self._run_binding_sha256:
            raise _ledger_error("LKG_LEDGER_BINDING_MISMATCH")
        if isinstance(attempt.query_id, bool) or not isinstance(attempt.query_id, int):
            raise _ledger_error("LKG_LEDGER_QUERY_ID_NOT_INTEGER")
        if (
            attempt.attempt_sequence < 0
            or attempt.attempt_sequence >= len(self._ordered_query_ids)
        ):
            raise _ledger_error("LKG_LEDGER_SEQUENCE_OUT_OF_RANGE")
        expected_query_id = self._ordered_query_ids[attempt.attempt_sequence]
        if attempt.query_id != expected_query_id:
            raise _ledger_error("LKG_LEDGER_SEQUENCE_QUERY_ID_MISMATCH")

        document = _attempt_document(attempt)
        try:
            with self._session() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT run_id, query_id, attempt_sequence, attempt_number, status,
                           document_json
                    FROM lkg_qualification_attempts
                    WHERE run_id = ? AND query_id = ? AND attempt_number = ?
                    """,
                    (self._run_id, attempt.query_id, attempt.attempt_number),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    existing_document = json.loads(existing[5])
                    if existing_document == document:
                        return LkgAppendResult(True, None, attempt)
                    return LkgAppendResult(False, "QUERY_ID_CONFLICTING_DUPLICATE", None)
                _, verified_head = self._verified_chain(connection)
                chain_sha256 = _chain_record_sha256(
                    verified_head,
                    run_id=self._run_id,
                    query_id=attempt.query_id,
                    attempt_sequence=attempt.attempt_sequence,
                    attempt_number=attempt.attempt_number,
                    status=attempt.status.value,
                    document=document,
                )
                document_json = canonical_json_bytes(document).decode("utf-8")
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_attempts(
                        run_id, query_id, attempt_sequence, attempt_number, status,
                        document_json, previous_chain_sha256, chain_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self._run_id,
                        attempt.query_id,
                        attempt.attempt_sequence,
                        attempt.attempt_number,
                        attempt.status.value,
                        document_json,
                        verified_head,
                        chain_sha256,
                    ),
                )
                connection.commit()
        except LkgQualificationLedgerError:
            raise
        except sqlite3.IntegrityError as exc:
            raise _ledger_error("LKG_LEDGER_CONSTRAINT_VIOLATION", exc) from exc
        except sqlite3.OperationalError as exc:
            raise _ledger_error("LKG_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("LKG_LEDGER_CORRUPTED", exc) from exc
        return LkgAppendResult(True, None, attempt)

    def records(self) -> tuple[LkgQueryAttempt, ...]:
        """Return every stored attempt for this run, in true insertion order.

        Every call re-verifies the complete hash chain and cross-checks
        every denormalized column against the reconstructed document; a
        tampered row is raised as ``LKG_LEDGER_CHAIN_INVALID``/
        ``_CORRUPTED``, never silently returned as if it were trustworthy
        evidence.
        """

        try:
            with self._session() as connection:
                attempts, _ = self._verified_chain(connection)
                return attempts
        except LkgQualificationLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("LKG_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("LKG_LEDGER_CORRUPTED", exc) from exc

    def chain_state(self) -> LkgChainState:
        """Return the restart-safe, independently re-verified chain head."""

        try:
            with self._session() as connection:
                attempts, head = self._verified_chain(connection)
        except LkgQualificationLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _ledger_error("LKG_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _ledger_error("LKG_LEDGER_CORRUPTED", exc) from exc
        return LkgChainState(
            run_id=self._run_id,
            run_binding_sha256=self._run_binding_sha256,
            record_count=len(attempts),
            chain_head_sha256=head,
        )

    def _verified_chain(
        self, connection: sqlite3.Connection
    ) -> tuple[tuple[LkgQueryAttempt, ...], str]:
        """Re-derive every stored row's chain link from scratch, in true
        ``insertion_seq`` arrival order, cross-checking every denormalized
        SQL column against the strictly-reconstructed document before
        trusting it, and raise on any mismatch. A single-row edit --
        whether to ``document_json`` or to any of the separate ``run_id``/
        ``query_id``/``attempt_sequence``/``attempt_number``/``status``
        columns -- therefore cannot be substituted without invalidating
        this pass, independent of the append-only triggers."""

        genesis = _genesis_sha256(
            run_id=self._run_id,
            run_binding_sha256=self._run_binding_sha256,
        )
        rows = connection.execute(
            """
            SELECT run_id, query_id, attempt_sequence, attempt_number, status,
                   document_json, previous_chain_sha256, chain_sha256
            FROM lkg_qualification_attempts ORDER BY insertion_seq ASC
            """
        ).fetchall()
        attempts: list[LkgQueryAttempt] = []
        previous = genesis
        for row in rows:
            if not isinstance(row, tuple) or len(row) != 8:
                raise _ledger_error("LKG_LEDGER_CORRUPTED")
            (
                row_run_id,
                row_query_id,
                row_attempt_sequence,
                row_attempt_number,
                row_status,
                document_json,
                stored_previous,
                stored_chain,
            ) = row
            if (
                stored_previous != previous
                or not isinstance(stored_chain, str)
                or _SHA256_RE.fullmatch(stored_chain) is None
            ):
                raise _ledger_error("LKG_LEDGER_CHAIN_INVALID")
            try:
                document = json.loads(document_json)
                if not isinstance(document, dict):
                    raise ValueError("stored document must be a JSON object")
                attempt = _attempt_from_document(document)
            except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
                raise _ledger_error("LKG_LEDGER_CORRUPTED", exc) from exc

            # Cross-check every denormalized column against the strictly
            # reconstructed document -- a mismatch here means a raw SQL
            # column was edited independently of document_json.
            if (
                row_run_id != self._run_id
                or row_query_id != document["query_id"]
                or row_attempt_sequence != document["attempt_sequence"]
                or row_attempt_number != document["attempt_number"]
                or row_status != document["status"]
            ):
                raise _ledger_error("LKG_LEDGER_COLUMN_MISMATCH")
            if attempt.run_binding_sha256 != self._run_binding_sha256:
                raise _ledger_error("LKG_LEDGER_BINDING_MISMATCH")

            recomputed_chain = _chain_record_sha256(
                previous,
                run_id=row_run_id,
                query_id=row_query_id,
                attempt_sequence=row_attempt_sequence,
                attempt_number=row_attempt_number,
                status=row_status,
                document=document,
            )
            if recomputed_chain != stored_chain:
                raise _ledger_error("LKG_LEDGER_CHAIN_INVALID")
            attempts.append(attempt)
            previous = stored_chain
        return tuple(attempts), previous

    def _validate_path(self) -> None:
        parent = self.path.parent
        try:
            parent_status = parent.stat()
        except OSError as exc:
            raise _ledger_error("LKG_LEDGER_DIRECTORY_UNAVAILABLE", exc) from exc
        if not stat.S_ISDIR(parent_status.st_mode) or stat.S_IMODE(parent_status.st_mode) & 0o077:
            raise _ledger_error("LKG_LEDGER_DIRECTORY_NOT_PRIVATE")
        try:
            file_status = self.path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise _ledger_error("LKG_LEDGER_UNAVAILABLE", exc) from exc
        if not stat.S_ISREG(file_status.st_mode) or stat.S_ISLNK(file_status.st_mode):
            raise _ledger_error("LKG_LEDGER_PATH_INVALID")

    def _enforce_private_database_mode(self) -> None:
        try:
            os.chmod(self.path, 0o600)
            if stat.S_IMODE(self.path.stat().st_mode) & 0o077:
                raise _ledger_error("LKG_LEDGER_FILE_NOT_PRIVATE")
        except LkgQualificationLedgerError:
            raise
        except OSError as exc:
            raise _ledger_error("LKG_LEDGER_UNAVAILABLE", exc) from exc

    def _connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self._lock_timeout_seconds,
            isolation_level=None,
        )
        try:
            connection.execute(f"PRAGMA busy_timeout = {int(self._lock_timeout_seconds * 1000)}")
            connection.execute("PRAGMA trusted_schema = OFF")
            connection.execute("PRAGMA foreign_keys = ON")
            journal_mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()
            if journal_mode is None or journal_mode[0] != "delete":
                raise _ledger_error("LKG_LEDGER_JOURNAL_MODE_INVALID")
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
                raise _ledger_error("LKG_LEDGER_CORRUPTED")
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
                    CREATE TABLE lkg_qualification_run (
                        run_id TEXT PRIMARY KEY NOT NULL,
                        schema_version INTEGER NOT NULL,
                        binding_document_json TEXT NOT NULL,
                        run_binding_sha256 TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE lkg_qualification_workload_positions (
                        run_id TEXT NOT NULL,
                        attempt_sequence INTEGER NOT NULL,
                        query_id INTEGER NOT NULL,
                        PRIMARY KEY (run_id, attempt_sequence),
                        UNIQUE (run_id, query_id),
                        UNIQUE (run_id, attempt_sequence, query_id),
                        FOREIGN KEY (run_id) REFERENCES lkg_qualification_run(run_id),
                        CHECK (attempt_sequence >= 0)
                    )
                    """
                )
                connection.execute(
                    # insertion_seq (not any other column) is the declared
                    # INTEGER PRIMARY KEY: SQLite aliases rowid to whichever
                    # column is declared that way, so making a different
                    # column the primary key would make "ORDER BY
                    # insertion_seq" silently sort by that column's value
                    # instead of true arrival order.
                    """
                    CREATE TABLE lkg_qualification_attempts (
                        insertion_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT NOT NULL,
                        query_id INTEGER NOT NULL,
                        attempt_sequence INTEGER NOT NULL,
                        attempt_number INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        document_json TEXT NOT NULL,
                        previous_chain_sha256 TEXT NOT NULL,
                        chain_sha256 TEXT UNIQUE NOT NULL,
                        FOREIGN KEY (run_id) REFERENCES lkg_qualification_run(run_id),
                        FOREIGN KEY (run_id, attempt_sequence, query_id)
                            REFERENCES lkg_qualification_workload_positions(
                                run_id, attempt_sequence, query_id
                            ),
                        UNIQUE (run_id, query_id, attempt_number),
                        UNIQUE (run_id, attempt_sequence, attempt_number),
                        CHECK (attempt_sequence >= 0),
                        CHECK (attempt_number >= 1)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER lkg_qualification_run_no_update
                    BEFORE UPDATE ON lkg_qualification_run
                    BEGIN SELECT RAISE(ABORT, 'lkg qualification run is append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER lkg_qualification_run_no_delete
                    BEFORE DELETE ON lkg_qualification_run
                    BEGIN SELECT RAISE(ABORT, 'lkg qualification run is append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER lkg_qualification_run_single_row
                    BEFORE INSERT ON lkg_qualification_run
                    WHEN (SELECT COUNT(*) FROM lkg_qualification_run) >= 1
                    BEGIN SELECT RAISE(ABORT, 'lkg qualification ledger holds exactly one run'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER lkg_qualification_workload_positions_no_update
                    BEFORE UPDATE ON lkg_qualification_workload_positions
                    BEGIN SELECT RAISE(ABORT, 'lkg qualification workload positions are append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER lkg_qualification_workload_positions_no_delete
                    BEFORE DELETE ON lkg_qualification_workload_positions
                    BEGIN SELECT RAISE(ABORT, 'lkg qualification workload positions are append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER lkg_qualification_attempts_no_update
                    BEFORE UPDATE ON lkg_qualification_attempts
                    BEGIN SELECT RAISE(ABORT, 'lkg qualification attempts are append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER lkg_qualification_attempts_no_delete
                    BEFORE DELETE ON lkg_qualification_attempts
                    BEGIN SELECT RAISE(ABORT, 'lkg qualification attempts are append-only'); END
                    """
                )
                connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            elif version[0] != _SCHEMA_VERSION:
                raise _ledger_error("LKG_LEDGER_SCHEMA_MISMATCH")
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
                raise _ledger_error("LKG_LEDGER_SCHEMA_MISMATCH")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def _bind_or_validate_run(self, connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            # An explicit COUNT, not just fetchone(): the single-row
            # trigger blocks a second INSERT through this ledger's own
            # Python API, but a hostile writer that bypasses that trigger
            # (and/or PRAGMA foreign_keys=OFF, which does not affect this
            # trigger at all) could otherwise insert a second row that
            # fetchone() would silently ignore, taking whichever row SQLite
            # happens to return first.
            row_count = connection.execute(
                "SELECT COUNT(*) FROM lkg_qualification_run"
            ).fetchone()[0]
            if row_count > 1:
                raise _ledger_error("LKG_LEDGER_MULTIPLE_RUN_ROWS")
            row = connection.execute(
                "SELECT run_id, schema_version, binding_document_json, run_binding_sha256 "
                "FROM lkg_qualification_run"
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO lkg_qualification_run(
                        run_id, schema_version, binding_document_json, run_binding_sha256
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        self._run_id,
                        _SCHEMA_VERSION,
                        self._binding_document_json,
                        self._run_binding_sha256,
                    ),
                )
                # Populate all workload positions atomically, in the same
                # transaction as the run row itself.
                connection.executemany(
                    """
                    INSERT INTO lkg_qualification_workload_positions(
                        run_id, attempt_sequence, query_id
                    ) VALUES (?, ?, ?)
                    """,
                    (
                        (self._run_id, sequence, query_id)
                        for sequence, query_id in enumerate(self._ordered_query_ids)
                    ),
                )
            else:
                stored_run_id, stored_schema_version, stored_document_json, stored_sha256 = row
                if stored_run_id != self._run_id:
                    raise _ledger_error("LKG_LEDGER_RUN_MISMATCH")
                if stored_schema_version != _SCHEMA_VERSION:
                    raise _ledger_error("LKG_LEDGER_SCHEMA_MISMATCH")
                try:
                    stored_document = json.loads(stored_document_json)
                    stored_binding = lkg_run_binding_from_document(stored_document)
                except (TypeError, ValueError, ContractViolation, json.JSONDecodeError) as exc:
                    raise _ledger_error("LKG_LEDGER_CORRUPTED", exc) from exc
                recomputed_sha256 = lkg_run_binding_sha256(stored_binding)
                if recomputed_sha256 != stored_sha256:
                    raise _ledger_error("LKG_LEDGER_CORRUPTED")
                if (
                    recomputed_sha256 != self._run_binding_sha256
                    or stored_document_json != self._binding_document_json
                ):
                    raise _ledger_error("LKG_LEDGER_BINDING_MISMATCH")

                # Reconstruct the persisted workload-position sequence and
                # re-verify its digest -- proving what is actually stored,
                # not merely this constructor's argument, still matches.
                position_rows = connection.execute(
                    """
                    SELECT attempt_sequence, query_id
                    FROM lkg_qualification_workload_positions
                    WHERE run_id = ?
                    ORDER BY attempt_sequence ASC
                    """,
                    (self._run_id,),
                ).fetchall()
                if len(position_rows) != len(self._ordered_query_ids):
                    raise _ledger_error("LKG_LEDGER_WORKLOAD_POSITIONS_CORRUPTED")
                for index, (stored_sequence, stored_query_id) in enumerate(position_rows):
                    if stored_sequence != index:
                        raise _ledger_error("LKG_LEDGER_WORKLOAD_POSITIONS_CORRUPTED")
                stored_ids = tuple(query_id for _, query_id in position_rows)
                if (
                    lkg_ordered_query_ids_sha256(stored_ids)
                    != self._run_binding.qualification_ordered_query_ids_sha256
                ):
                    raise _ledger_error("LKG_LEDGER_WORKLOAD_POSITIONS_CORRUPTED")
                if stored_ids != self._ordered_query_ids:
                    raise _ledger_error("LKG_LEDGER_WORKLOAD_POSITIONS_MISMATCH")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
