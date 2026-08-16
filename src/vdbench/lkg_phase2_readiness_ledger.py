"""Restart-durable Phase-2 ledger: sealed-source binding + readiness ingestion.

Purpose:
    Bind a Phase-2 ledger to exactly one verified Checkpoint-A ``LkgRunSeal``
    (``Phase2SourceBinding``), and durably ingest each constituent window's
    pre-seal ``LkgWindowOperationalReadinessEvidence`` into a chain-linked,
    append-only ``LkgWindowReadinessIngestion`` record after that seal
    exists. This module is deliberately a separate SQLite file from the
    Phase-1 ledger it binds to -- it never writes to that file, and it
    never trusts a cached seal: every durable operation freshly re-calls
    Checkpoint A's ``verify_seal()`` and re-derives its own chain from
    genesis before comparing anything.
Scope:
    No window/epoch statistical evaluation, no recall/latency
    computation, no PASSING/FAILING verdict -- that is Checkpoint C, not
    implemented anywhere in this module.
Chain-lineage caveat:
    ``window_readiness_ingestion``'s hash chain provides tamper-*evidence*
    only -- any modification is detectable by a verifier that re-derives
    the whole chain from genesis -- never cryptographic *authenticity*
    against a fully hostile writer with raw file access and unlimited
    compute. No keyed MAC or signature is used, identical in kind to
    Checkpoint A's own attempt/seal chains.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import stat
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .artifacts import canonical_json_bytes
from .config import ContractViolation
from .lkg_phase2_source_binding import (
    EXPECTED_QUERY_COUNT,
    INGESTION_SCHEMA_VERSION,
    PHASE1_LEDGER_SCHEMA_VERSION,
    SEAL_SCHEMA_VERSION_PIN,
    SOURCE_BINDING_SCHEMA_VERSION,
    LkgWindowReadinessIngestion,
    Phase2SourceBinding,
    ingestion_payload_document,
    ingestion_payload_document_digest,
    lkg_window_readiness_ingestion_from_payload,
    phase2_source_binding_from_payload,
    source_binding_payload_document,
    source_binding_payload_document_digest,
)
from .lkg_qualification_ledger import (
    LkgQualificationLedger,
    LkgQualificationLedgerError,
    verify_seal,
)
from .lkg_qualification_seal import LkgRunSeal
from .lkg_window_readiness import (
    LkgWindowOperationalReadinessProvider,
    parse_rfc3339_utc_instant,
    readiness_payload_document,
)

__all__ = [
    "Phase2ReadinessLedger",
    "Phase2ReadinessLedgerError",
]


_PHASE2_SCHEMA_VERSION = 1
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_WINDOWS_PER_RUN = 12
_WINDOWS_PER_EPOCH = 6
_POSITIONS_PER_WINDOW = 200

_PHASE2_EXPECTED_SCHEMA_OBJECTS = frozenset(
    {
        "phase2_source_binding",
        "phase2_source_binding_no_update",
        "phase2_source_binding_no_delete",
        "phase2_source_binding_single_row",
        "window_readiness_ingestion",
        "window_readiness_ingestion_no_update",
        "window_readiness_ingestion_no_delete",
    }
)

_INGESTION_CHAIN_GENESIS_DOMAIN = b"vdbench.window_readiness_ingestion.genesis.v1\0"
_INGESTION_CHAIN_RECORD_DOMAIN = b"vdbench.window_readiness_ingestion.record.v1\0"


class Phase2ReadinessLedgerError(RuntimeError):
    """A durable Phase-2 persistence/verification failure."""


def _readiness_ledger_error(code: str, cause: BaseException | None = None) -> Phase2ReadinessLedgerError:
    error = Phase2ReadinessLedgerError(code)
    if cause is not None:
        error.__cause__ = cause
    return error


def _verify_phase1_seal_or_translate(phase1_ledger: LkgQualificationLedger) -> LkgRunSeal:
    """Every durable Phase-2 operation calls this, never a cached seal.
    Translates Checkpoint A's exception into a stable Checkpoint-B code
    while preserving the original as __cause__."""

    try:
        return verify_seal(phase1_ledger)
    except LkgQualificationLedgerError as exc:
        if str(exc) == "LKG_SEAL_MISSING":
            raise _readiness_ledger_error("PHASE2_SOURCE_SEAL_MISSING", exc) from exc
        raise _readiness_ledger_error("PHASE2_SOURCE_SEAL_UNVERIFIABLE", exc) from exc


def _current_rfc3339_utc() -> str:
    now = datetime.now(UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"


def _require_readiness_chronology(*, checked_at_utc: str, sealed_at_utc: str, ingested_at_utc: str) -> None:
    """original_evidence.checked_at_utc <= source_run_seal.sealed_at_utc
    <= ingestion.ingested_at_utc, compared as parsed instants, never raw
    strings. Called at first ingestion AND at every subsequent
    verification -- chronology is never trusted merely because it passed
    once at insertion time."""

    checked = parse_rfc3339_utc_instant(checked_at_utc)
    sealed = parse_rfc3339_utc_instant(sealed_at_utc)
    ingested = parse_rfc3339_utc_instant(ingested_at_utc)
    if checked > sealed:
        raise _readiness_ledger_error("READINESS_CHECKED_AFTER_SEAL")
    if sealed > ingested:
        raise _readiness_ledger_error("INGESTION_TIMESTAMP_BEFORE_SEAL")


def _ingestion_genesis_sha256(*, source_run_id: str, canonical_source_binding_digest: str) -> str:
    return hashlib.sha256(
        _INGESTION_CHAIN_GENESIS_DOMAIN
        + canonical_json_bytes(
            {"source_run_id": source_run_id, "canonical_source_binding_digest": canonical_source_binding_digest}
        )
    ).hexdigest()


def _ingestion_chain_record_sha256(
    previous: str,
    *,
    source_run_id: str,
    window_index: int,
    epoch_index: int,
    readiness_check_id: str,
    canonical_ingestion_digest: str,
) -> str:
    payload = {
        "previous": previous,
        "source_run_id": source_run_id,
        "window_index": window_index,
        "epoch_index": epoch_index,
        "readiness_check_id": readiness_check_id,
        "canonical_ingestion_digest": canonical_ingestion_digest,
    }
    return hashlib.sha256(_INGESTION_CHAIN_RECORD_DOMAIN + canonical_json_bytes(payload)).hexdigest()


def _build_source_binding_from_seal(seal: LkgRunSeal) -> Phase2SourceBinding:
    payload = {
        "source_binding_schema_version": SOURCE_BINDING_SCHEMA_VERSION,
        "source_run_id": seal.run_id,
        "source_run_binding_sha256": seal.run_binding_sha256,
        "source_phase1_ledger_schema_version": seal.phase1_ledger_schema_version,
        "source_seal_schema_version": seal.seal_schema_version,
        "source_run_seal_digest": seal.canonical_seal_document_digest,
        "source_sealed_chain_head_sha256": seal.final_chain_head_sha256,
        "workload_identity": {
            "dataset_id": seal.workload_identity.dataset_id,
            "dataset_version": seal.workload_identity.dataset_version,
            "manifest_sha256": seal.workload_identity.manifest_sha256,
            "query_role": seal.workload_identity.query_role,
        },
        "qualification_ordered_query_ids_sha256": seal.qualification_ordered_query_ids_sha256,
        "expected_query_count": seal.expected_query_count,
    }
    digest = source_binding_payload_document_digest(payload)
    return phase2_source_binding_from_payload(payload, canonical_source_binding_digest=digest)


def _binding_matches_seal(binding: Phase2SourceBinding, seal: LkgRunSeal) -> bool:
    return (
        binding.source_run_id == seal.run_id
        and binding.source_run_binding_sha256 == seal.run_binding_sha256
        and binding.source_phase1_ledger_schema_version == seal.phase1_ledger_schema_version
        and binding.source_seal_schema_version == seal.seal_schema_version
        and binding.source_run_seal_digest == seal.canonical_seal_document_digest
        and binding.source_sealed_chain_head_sha256 == seal.final_chain_head_sha256
        and binding.workload_identity == seal.workload_identity
        and binding.qualification_ordered_query_ids_sha256 == seal.qualification_ordered_query_ids_sha256
        and binding.expected_query_count == seal.expected_query_count
    )


class Phase2ReadinessLedger:
    """Single-host SQLite ledger bound to exactly one verified,
    sealed Phase-1 qualification run."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        phase1_ledger: LkgQualificationLedger,
        lock_timeout_seconds: float = 1.0,
    ) -> None:
        if not isinstance(phase1_ledger, LkgQualificationLedger):
            raise ContractViolation("phase1_ledger must be an LkgQualificationLedger")
        if (
            isinstance(lock_timeout_seconds, bool)
            or not isinstance(lock_timeout_seconds, (int, float))
            or not math.isfinite(float(lock_timeout_seconds))
            or not 0.001 <= float(lock_timeout_seconds) <= 10.0
        ):
            raise ContractViolation("lock_timeout_seconds must be finite and between 0.001 and 10")

        self.path = Path(path)
        self._phase1_ledger = phase1_ledger
        self._lock_timeout_seconds = float(lock_timeout_seconds)

        # Fail closed before any schema initialization if the two ledgers
        # would alias the same file -- a hard prerequisite, not merely a
        # storage-layer verification concern.
        if self.path.expanduser().resolve() == Path(phase1_ledger.path).expanduser().resolve():
            raise ContractViolation(
                "Phase2ReadinessLedger.path must not resolve to the same file as phase1_ledger.path"
            )

        self._validate_path()
        try:
            with self._session() as connection:
                self._initialize_schema(connection)
                self._bind_or_validate_source(connection)
            self._enforce_private_database_mode()
        except Phase2ReadinessLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_CORRUPTED", exc) from exc
        except (OSError, sqlite3.Error) as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc

    # -- public API -----------------------------------------------------

    def ingest_window_readiness(
        self,
        *,
        provider: LkgWindowOperationalReadinessProvider,
        readiness_check_id: str,
        window_index: int,
    ) -> LkgWindowReadinessIngestion:
        """Ingest (or idempotently re-verify) exactly one window's
        readiness evidence, post-seal. ``provider.lookup`` is called only
        when the window has never been durably ingested before -- a retry
        after a crash-after-commit/before-response never reaches the
        provider at all."""

        if isinstance(window_index, bool) or not isinstance(window_index, int) or not (
            0 <= window_index < _WINDOWS_PER_RUN
        ):
            raise ContractViolation(f"window_index must be an int in [0, {_WINDOWS_PER_RUN})")
        if not isinstance(readiness_check_id, str) or not readiness_check_id:
            raise ContractViolation("readiness_check_id must be a non-empty string")

        epoch_index = window_index // _WINDOWS_PER_EPOCH
        first_attempt_sequence = window_index * _POSITIONS_PER_WINDOW
        last_attempt_sequence = first_attempt_sequence + _POSITIONS_PER_WINDOW - 1

        try:
            with self._session() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    seal = _verify_phase1_seal_or_translate(self._phase1_ledger)
                    binding = self._read_and_verify_binding_locked(connection)
                    if not _binding_matches_seal(binding, seal):
                        raise _readiness_ledger_error("PHASE2_SOURCE_BINDING_MISMATCH")

                    ingestions, chain_head = self._verified_ingestion_chain_locked(connection, binding)
                    existing = next((i for i in ingestions if i.window_index == window_index), None)

                    if existing is not None:
                        if existing.original_evidence.readiness_check_id != readiness_check_id:
                            raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_CHECK_ID_MISMATCH")
                        if (
                            existing.source_run_seal_digest != seal.canonical_seal_document_digest
                            or existing.phase2_source_binding_digest != binding.canonical_source_binding_digest
                        ):
                            raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_SOURCE_CHANGED")
                        _require_readiness_chronology(
                            checked_at_utc=existing.original_evidence.checked_at_utc,
                            sealed_at_utc=seal.sealed_at_utc,
                            ingested_at_utc=existing.ingested_at_utc,
                        )
                        connection.commit()
                        return existing

                    # First ingestion for this window: provider.lookup is
                    # called exactly once, here, and nowhere else.
                    evidence = provider.lookup(readiness_check_id=readiness_check_id)
                    expected_context = (
                        binding.source_run_id, window_index, epoch_index,
                        first_attempt_sequence, last_attempt_sequence, binding.source_run_binding_sha256,
                    )
                    actual_context = (
                        evidence.source_run_id, evidence.window_index, evidence.epoch_index,
                        evidence.first_attempt_sequence, evidence.last_attempt_sequence,
                        evidence.source_run_binding_sha256,
                    )
                    if expected_context != actual_context:
                        raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_EVIDENCE_MISMATCH")

                    ingested_at_utc = _current_rfc3339_utc()
                    _require_readiness_chronology(
                        checked_at_utc=evidence.checked_at_utc,
                        sealed_at_utc=seal.sealed_at_utc,
                        ingested_at_utc=ingested_at_utc,
                    )

                    payload = {
                        "ingestion_schema_version": INGESTION_SCHEMA_VERSION,
                        "source_run_id": binding.source_run_id,
                        "window_index": window_index,
                        "epoch_index": epoch_index,
                        "original_evidence": readiness_payload_document(evidence),
                        "original_evidence_digest": evidence.canonical_document_digest,
                        "source_run_seal_digest": seal.canonical_seal_document_digest,
                        "phase2_source_binding_digest": binding.canonical_source_binding_digest,
                        "ingested_at_utc": ingested_at_utc,
                    }
                    digest = ingestion_payload_document_digest(payload)
                    new_ingestion = lkg_window_readiness_ingestion_from_payload(
                        payload, canonical_ingestion_digest=digest
                    )
                    chain_sha256 = _ingestion_chain_record_sha256(
                        chain_head,
                        source_run_id=binding.source_run_id,
                        window_index=window_index,
                        epoch_index=epoch_index,
                        readiness_check_id=readiness_check_id,
                        canonical_ingestion_digest=digest,
                    )
                    document_json = canonical_json_bytes(payload).decode("utf-8")
                    connection.execute(
                        """
                        INSERT INTO window_readiness_ingestion(
                            source_run_id, window_index, epoch_index, readiness_check_id,
                            source_run_seal_digest, phase2_source_binding_digest,
                            ingestion_document_json, canonical_ingestion_digest,
                            previous_chain_sha256, chain_sha256
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            binding.source_run_id, window_index, epoch_index, readiness_check_id,
                            seal.canonical_seal_document_digest, binding.canonical_source_binding_digest,
                            document_json, digest, chain_head, chain_sha256,
                        ),
                    )
                    connection.commit()
                    return new_ingestion
                except BaseException:
                    connection.rollback()
                    raise
        except Phase2ReadinessLedgerError:
            raise
        except sqlite3.IntegrityError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_CONSTRAINT_VIOLATION", exc) from exc
        except sqlite3.OperationalError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_CORRUPTED", exc) from exc
        except (OSError, sqlite3.Error) as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc

    def verify_window_readiness_ingestion(self, window_index: int) -> LkgWindowReadinessIngestion:
        """Pure re-verification -- never calls a provider. Re-derives the
        complete ingestion chain from genesis, re-verifies the bound
        Phase-1 seal fresh, and re-checks chronology every time."""

        if isinstance(window_index, bool) or not isinstance(window_index, int) or not (
            0 <= window_index < _WINDOWS_PER_RUN
        ):
            raise ContractViolation(f"window_index must be an int in [0, {_WINDOWS_PER_RUN})")

        try:
            with self._session() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    seal = _verify_phase1_seal_or_translate(self._phase1_ledger)
                    binding = self._read_and_verify_binding_locked(connection)
                    if not _binding_matches_seal(binding, seal):
                        raise _readiness_ledger_error("PHASE2_SOURCE_BINDING_MISMATCH")
                    ingestions, _ = self._verified_ingestion_chain_locked(connection, binding)
                    target = next((i for i in ingestions if i.window_index == window_index), None)
                    if target is None:
                        raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_MISSING")
                    _require_readiness_chronology(
                        checked_at_utc=target.original_evidence.checked_at_utc,
                        sealed_at_utc=seal.sealed_at_utc,
                        ingested_at_utc=target.ingested_at_utc,
                    )
                    if (
                        target.source_run_seal_digest != seal.canonical_seal_document_digest
                        or target.phase2_source_binding_digest != binding.canonical_source_binding_digest
                    ):
                        raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_SOURCE_CHANGED")
                    connection.commit()
                    return target
                except BaseException:
                    connection.rollback()
                    raise
        except Phase2ReadinessLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_CORRUPTED", exc) from exc
        except (OSError, sqlite3.Error) as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc

    def all_verified_ingestions(self) -> tuple[LkgWindowReadinessIngestion, ...]:
        """Re-verify and return every stored ingestion, in insertion
        order -- the same full-chain, full-chronology discipline as
        ``verify_window_readiness_ingestion``, for every row."""

        try:
            with self._session() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    seal = _verify_phase1_seal_or_translate(self._phase1_ledger)
                    binding = self._read_and_verify_binding_locked(connection)
                    if not _binding_matches_seal(binding, seal):
                        raise _readiness_ledger_error("PHASE2_SOURCE_BINDING_MISMATCH")
                    ingestions, _ = self._verified_ingestion_chain_locked(connection, binding)
                    for ingestion in ingestions:
                        _require_readiness_chronology(
                            checked_at_utc=ingestion.original_evidence.checked_at_utc,
                            sealed_at_utc=seal.sealed_at_utc,
                            ingested_at_utc=ingestion.ingested_at_utc,
                        )
                        if (
                            ingestion.source_run_seal_digest != seal.canonical_seal_document_digest
                            or ingestion.phase2_source_binding_digest != binding.canonical_source_binding_digest
                        ):
                            raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_SOURCE_CHANGED")
                    connection.commit()
                    return ingestions
                except BaseException:
                    connection.rollback()
                    raise
        except Phase2ReadinessLedgerError:
            raise
        except sqlite3.OperationalError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc
        except sqlite3.DatabaseError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_CORRUPTED", exc) from exc
        except (OSError, sqlite3.Error) as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc

    # -- connection-scoped internals -------------------------------------

    def _read_and_verify_binding_locked(self, connection: sqlite3.Connection) -> Phase2SourceBinding:
        row_count = connection.execute("SELECT COUNT(*) FROM phase2_source_binding").fetchone()[0]
        if row_count > 1:
            raise _readiness_ledger_error("PHASE2_MULTIPLE_SOURCE_BINDING_ROWS")
        row = connection.execute(
            """
            SELECT source_run_id, source_binding_schema_version, source_run_binding_sha256,
                   source_phase1_ledger_schema_version, source_seal_schema_version,
                   source_run_seal_digest, source_sealed_chain_head_sha256,
                   qualification_ordered_query_ids_sha256, expected_query_count,
                   binding_document_json, canonical_source_binding_digest
            FROM phase2_source_binding
            """
        ).fetchone()
        if row is None:
            raise _readiness_ledger_error("PHASE2_SOURCE_BINDING_CORRUPTED")
        return self._reconstruct_and_verify_binding_row(row)

    @staticmethod
    def _reconstruct_and_verify_binding_row(row: tuple) -> Phase2SourceBinding:
        (
            row_source_run_id,
            row_schema_version,
            row_run_binding_sha256,
            row_phase1_schema_version,
            row_seal_schema_version,
            row_seal_digest,
            row_chain_head,
            row_ordered_ids_sha256,
            row_expected_count,
            binding_document_json,
            row_canonical_digest,
        ) = row
        try:
            document = json.loads(binding_document_json)
            if not isinstance(document, dict):
                raise ValueError("stored binding document must be a JSON object")  # domain error type carries the governed reason code  # noqa: TRY004
            binding = phase2_source_binding_from_payload(
                document, canonical_source_binding_digest=row_canonical_digest
            )
        except (TypeError, ValueError, ContractViolation, json.JSONDecodeError) as exc:
            raise _readiness_ledger_error("PHASE2_SOURCE_BINDING_CORRUPTED", exc) from exc

        rebuilt_payload = source_binding_payload_document(binding)
        if canonical_json_bytes(rebuilt_payload) != binding_document_json.encode("utf-8"):
            raise _readiness_ledger_error("PHASE2_SOURCE_BINDING_CORRUPTED")
        recomputed_digest = source_binding_payload_document_digest(rebuilt_payload)
        if recomputed_digest != row_canonical_digest:
            raise _readiness_ledger_error("PHASE2_SOURCE_BINDING_CORRUPTED")

        if (
            row_source_run_id != binding.source_run_id
            or row_schema_version != binding.source_binding_schema_version
            or row_run_binding_sha256 != binding.source_run_binding_sha256
            or row_phase1_schema_version != binding.source_phase1_ledger_schema_version
            or row_seal_schema_version != binding.source_seal_schema_version
            or row_seal_digest != binding.source_run_seal_digest
            or row_chain_head != binding.source_sealed_chain_head_sha256
            or row_ordered_ids_sha256 != binding.qualification_ordered_query_ids_sha256
            or row_expected_count != binding.expected_query_count
        ):
            raise _readiness_ledger_error("PHASE2_SOURCE_BINDING_COLUMN_MISMATCH")
        return binding

    def _verified_ingestion_chain_locked(
        self, connection: sqlite3.Connection, binding: Phase2SourceBinding
    ) -> tuple[tuple[LkgWindowReadinessIngestion, ...], str]:
        genesis = _ingestion_genesis_sha256(
            source_run_id=binding.source_run_id,
            canonical_source_binding_digest=binding.canonical_source_binding_digest,
        )
        rows = connection.execute(
            """
            SELECT source_run_id, window_index, epoch_index, readiness_check_id,
                   source_run_seal_digest, phase2_source_binding_digest,
                   ingestion_document_json, canonical_ingestion_digest,
                   previous_chain_sha256, chain_sha256
            FROM window_readiness_ingestion ORDER BY insertion_seq ASC
            """
        ).fetchall()
        ingestions: list[LkgWindowReadinessIngestion] = []
        previous = genesis
        for row in rows:
            if not isinstance(row, tuple) or len(row) != 10:
                raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_CORRUPTED")
            (
                row_run_id,
                row_window_index,
                row_epoch_index,
                row_check_id,
                row_seal_digest,
                row_binding_digest,
                ingestion_document_json,
                row_canonical_digest,
                stored_previous,
                stored_chain,
            ) = row
            if (
                stored_previous != previous
                or not isinstance(stored_chain, str)
                or _SHA256_RE.fullmatch(stored_chain) is None
            ):
                raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_CHAIN_INVALID")
            try:
                document = json.loads(ingestion_document_json)
                if not isinstance(document, dict):
                    raise ValueError("stored ingestion document must be a JSON object")  # domain error type carries the governed reason code  # noqa: TRY004
                ingestion = lkg_window_readiness_ingestion_from_payload(
                    document, canonical_ingestion_digest=row_canonical_digest
                )
            except (TypeError, ValueError, ContractViolation, json.JSONDecodeError) as exc:
                raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_CORRUPTED", exc) from exc

            rebuilt_payload = ingestion_payload_document(ingestion)
            if canonical_json_bytes(rebuilt_payload) != ingestion_document_json.encode("utf-8"):
                raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_CORRUPTED")
            recomputed_digest = ingestion_payload_document_digest(rebuilt_payload)
            if recomputed_digest != row_canonical_digest:
                raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_CORRUPTED")

            if (
                row_run_id != ingestion.source_run_id
                or row_window_index != ingestion.window_index
                or row_epoch_index != ingestion.epoch_index
                or row_check_id != ingestion.original_evidence.readiness_check_id
                or row_seal_digest != ingestion.source_run_seal_digest
                or row_binding_digest != ingestion.phase2_source_binding_digest
            ):
                raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_COLUMN_MISMATCH")

            recomputed_chain = _ingestion_chain_record_sha256(
                previous,
                source_run_id=row_run_id,
                window_index=row_window_index,
                epoch_index=row_epoch_index,
                readiness_check_id=row_check_id,
                canonical_ingestion_digest=row_canonical_digest,
            )
            if recomputed_chain != stored_chain:
                raise _readiness_ledger_error("WINDOW_READINESS_INGESTION_CHAIN_INVALID")
            ingestions.append(ingestion)
            previous = stored_chain
        return tuple(ingestions), previous

    def _bind_or_validate_source(self, connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            seal = _verify_phase1_seal_or_translate(self._phase1_ledger)
            row_count = connection.execute("SELECT COUNT(*) FROM phase2_source_binding").fetchone()[0]
            if row_count > 1:
                raise _readiness_ledger_error("PHASE2_MULTIPLE_SOURCE_BINDING_ROWS")
            row = connection.execute(
                """
                SELECT source_run_id, source_binding_schema_version, source_run_binding_sha256,
                       source_phase1_ledger_schema_version, source_seal_schema_version,
                       source_run_seal_digest, source_sealed_chain_head_sha256,
                       qualification_ordered_query_ids_sha256, expected_query_count,
                       binding_document_json, canonical_source_binding_digest
                FROM phase2_source_binding
                """
            ).fetchone()
            if row is None:
                binding = _build_source_binding_from_seal(seal)
                document_json = canonical_json_bytes(source_binding_payload_document(binding)).decode("utf-8")
                connection.execute(
                    """
                    INSERT INTO phase2_source_binding(
                        source_run_id, source_binding_schema_version, source_run_binding_sha256,
                        source_phase1_ledger_schema_version, source_seal_schema_version,
                        source_run_seal_digest, source_sealed_chain_head_sha256,
                        qualification_ordered_query_ids_sha256, expected_query_count,
                        binding_document_json, canonical_source_binding_digest
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        binding.source_run_id,
                        binding.source_binding_schema_version,
                        binding.source_run_binding_sha256,
                        binding.source_phase1_ledger_schema_version,
                        binding.source_seal_schema_version,
                        binding.source_run_seal_digest,
                        binding.source_sealed_chain_head_sha256,
                        binding.qualification_ordered_query_ids_sha256,
                        binding.expected_query_count,
                        document_json,
                        binding.canonical_source_binding_digest,
                    ),
                )
            else:
                binding = self._reconstruct_and_verify_binding_row(row)
                if not _binding_matches_seal(binding, seal):
                    raise _readiness_ledger_error("PHASE2_SOURCE_BINDING_MISMATCH")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def _validate_path(self) -> None:
        parent = self.path.parent
        try:
            parent_status = parent.stat()
        except OSError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc
        if not stat.S_ISDIR(parent_status.st_mode) or stat.S_IMODE(parent_status.st_mode) & 0o077:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE")
        try:
            file_status = self.path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc
        if not stat.S_ISREG(file_status.st_mode) or stat.S_ISLNK(file_status.st_mode):
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE")

    def _enforce_private_database_mode(self) -> None:
        try:
            os.chmod(self.path, 0o600)
            if stat.S_IMODE(self.path.stat().st_mode) & 0o077:
                raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE")
        except Phase2ReadinessLedgerError:
            raise
        except OSError as exc:
            raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE", exc) from exc

    def _connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=self._lock_timeout_seconds, isolation_level=None)
        try:
            connection.execute(f"PRAGMA busy_timeout = {int(self._lock_timeout_seconds * 1000)}")
            connection.execute("PRAGMA trusted_schema = OFF")
            connection.execute("PRAGMA foreign_keys = ON")
            journal_mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()
            if journal_mode is None or journal_mode[0] != "delete":
                raise _readiness_ledger_error("PHASE2_LEDGER_UNAVAILABLE")
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
                raise _readiness_ledger_error("PHASE2_LEDGER_CORRUPTED")
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
                    f"""
                    CREATE TABLE phase2_source_binding (
                        source_run_id TEXT PRIMARY KEY NOT NULL,
                        source_binding_schema_version INTEGER NOT NULL,
                        source_run_binding_sha256 TEXT NOT NULL,
                        source_phase1_ledger_schema_version INTEGER NOT NULL,
                        source_seal_schema_version INTEGER NOT NULL,
                        source_run_seal_digest TEXT NOT NULL,
                        source_sealed_chain_head_sha256 TEXT NOT NULL,
                        qualification_ordered_query_ids_sha256 TEXT NOT NULL,
                        expected_query_count INTEGER NOT NULL,
                        binding_document_json TEXT NOT NULL,
                        canonical_source_binding_digest TEXT NOT NULL,
                        CHECK (source_binding_schema_version = {SOURCE_BINDING_SCHEMA_VERSION}),
                        CHECK (source_phase1_ledger_schema_version = {PHASE1_LEDGER_SCHEMA_VERSION}),
                        CHECK (source_seal_schema_version = {SEAL_SCHEMA_VERSION_PIN}),
                        CHECK (expected_query_count = {EXPECTED_QUERY_COUNT}),
                        CHECK (length(source_run_binding_sha256) = 64),
                        CHECK (length(source_run_seal_digest) = 64),
                        CHECK (length(source_sealed_chain_head_sha256) = 64),
                        CHECK (length(qualification_ordered_query_ids_sha256) = 64),
                        CHECK (length(canonical_source_binding_digest) = 64)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER phase2_source_binding_no_update
                    BEFORE UPDATE ON phase2_source_binding
                    BEGIN SELECT RAISE(ABORT, 'phase2 source binding is append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER phase2_source_binding_no_delete
                    BEFORE DELETE ON phase2_source_binding
                    BEGIN SELECT RAISE(ABORT, 'phase2 source binding is append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER phase2_source_binding_single_row
                    BEFORE INSERT ON phase2_source_binding
                    WHEN (SELECT COUNT(*) FROM phase2_source_binding) >= 1
                    BEGIN SELECT RAISE(ABORT, 'phase2 ledger holds exactly one source binding'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE window_readiness_ingestion (
                        insertion_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_run_id TEXT NOT NULL,
                        window_index INTEGER NOT NULL,
                        epoch_index INTEGER NOT NULL,
                        readiness_check_id TEXT NOT NULL,
                        source_run_seal_digest TEXT NOT NULL,
                        phase2_source_binding_digest TEXT NOT NULL,
                        ingestion_document_json TEXT NOT NULL,
                        canonical_ingestion_digest TEXT UNIQUE NOT NULL,
                        previous_chain_sha256 TEXT NOT NULL,
                        chain_sha256 TEXT UNIQUE NOT NULL,
                        FOREIGN KEY (source_run_id) REFERENCES phase2_source_binding(source_run_id),
                        UNIQUE (source_run_id, window_index),
                        UNIQUE (source_run_id, readiness_check_id),
                        CHECK (window_index >= 0 AND window_index < 12),
                        CHECK (epoch_index = window_index / 6),
                        CHECK (length(readiness_check_id) > 0),
                        CHECK (length(source_run_seal_digest) = 64),
                        CHECK (length(phase2_source_binding_digest) = 64),
                        CHECK (length(canonical_ingestion_digest) = 64),
                        CHECK (length(previous_chain_sha256) = 64),
                        CHECK (length(chain_sha256) = 64)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER window_readiness_ingestion_no_update
                    BEFORE UPDATE ON window_readiness_ingestion
                    BEGIN SELECT RAISE(ABORT, 'window readiness ingestion is append-only'); END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER window_readiness_ingestion_no_delete
                    BEFORE DELETE ON window_readiness_ingestion
                    BEGIN SELECT RAISE(ABORT, 'window readiness ingestion is append-only'); END
                    """
                )
                connection.execute(f"PRAGMA user_version = {_PHASE2_SCHEMA_VERSION}")
            elif version[0] != _PHASE2_SCHEMA_VERSION:
                raise _readiness_ledger_error("PHASE2_LEDGER_SCHEMA_MISMATCH")
            schema_objects = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE name NOT LIKE 'sqlite_%' AND type IN ('table', 'view', 'trigger')
                    """
                )
            }
            if schema_objects != _PHASE2_EXPECTED_SCHEMA_OBJECTS:
                raise _readiness_ledger_error("PHASE2_LEDGER_SCHEMA_MISMATCH")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
