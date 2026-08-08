"""SQLite storage, identity verification, evidence orchestration, and lifecycle finalization for Checkpoint C (LKG Qualification Evaluation Ledger).

Lock Ordering:
    C -> B -> A. LkgQualificationEvaluationLedger acquires its write
    transaction FIRST before reading phase1_ledger (A) or
    phase2_readiness_ledger (B).

Security & Isolation:
    - Path hardening prevents aliasing to Phase-1 or Phase-2 database paths,
      rejects symlinks, and enforces POSIX permissions 0600.
    - Schema uses single-row append-only triggers, PRAGMA user_version = 1,
      foreign_keys = ON, and trusted_schema = OFF.
    - Durable JSON bytes are verified character-for-character against canonical
      JSON re-serialization on replay.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
from dataclasses import dataclass
from typing import Any

from .artifacts import canonical_json_bytes
from .config import ContractViolation
from .lkg_qualification_evidence import LkgQueryAttempt
from .lkg_qualification_seal import LkgRunSeal
from .lkg_qualification_ledger import LkgQualificationLedger, LkgQualificationLedgerError, verify_seal
from .lkg_phase2_readiness_ledger import Phase2ReadinessLedger, Phase2ReadinessLedgerError
from .lkg_phase2_source_binding import (
    SOURCE_BINDING_SCHEMA_VERSION,
    LkgWindowReadinessIngestion,
    Phase2SourceBinding,
    phase2_source_binding_from_payload,
    source_binding_payload_document_digest,
)
from .lkg_run_binding import LkgRunBinding, lkg_ordered_query_ids_sha256
from .search_configuration_digest import search_configuration_sha256
from .lkg_qualification_evaluation import (
    EVALUATION_CONTRACT_SCHEMA_VERSION,
    LkgQualificationEvaluation,
    LkgQualificationStatus,
    default_lkg_ef_eligibility_rule,
    default_lkg_qualification_semantics_rule,
    evaluation_contract_payload_document_digest,
    evaluation_payload_document,
    evaluate_run,
    lkg_qualification_evaluation_contract_from_payload,
    lkg_qualification_evaluation_from_payload,
    LkgQualificationEvaluationContract,
    LkgEfEligibilityRule,
    LkgQualificationSemanticsRule,
)


_EVALUATION_SCHEMA_VERSION = 1
_EXPECTED_SCHEMA_OBJECTS = frozenset(
    {
        "lkg_qualification_final_evaluation",
        "trg_single_row_final_eval",
        "trg_no_update_final_eval",
        "trg_no_delete_final_eval",
    }
)
_WINDOWS_PER_RUN = 12


def _normalize_schema_sql(sql: str) -> str:
    """Return the canonical comparison form for SQLite schema SQL."""

    # Preserve character case because quoted SQL literals are case-sensitive;
    # only insignificant formatting whitespace and a trailing terminator may
    # be normalized safely.
    return re.sub(r"\s+", " ", sql).strip().rstrip(";")


_EXPECTED_TABLE_SQL = """
    CREATE TABLE lkg_qualification_final_evaluation (
        source_run_id TEXT PRIMARY KEY NOT NULL,
        evaluation_schema_version INTEGER NOT NULL CHECK (evaluation_schema_version = 1),
        status TEXT NOT NULL CHECK (status IN ('PASSING', 'FAILING', 'INCOMPLETE')),
        qualified INTEGER NOT NULL CHECK (qualified IN (0, 1)),
        evaluated_ef INTEGER NOT NULL CHECK (evaluated_ef > 0),
        evaluation_contract_digest TEXT NOT NULL CHECK (length(evaluation_contract_digest) = 64),
        ef_eligibility_rule_digest TEXT NOT NULL CHECK (length(ef_eligibility_rule_digest) = 64),
        qualification_semantics_rule_digest TEXT NOT NULL CHECK (length(qualification_semantics_rule_digest) = 64),
        source_run_binding_sha256 TEXT NOT NULL CHECK (length(source_run_binding_sha256) = 64),
        source_run_seal_digest TEXT NOT NULL CHECK (length(source_run_seal_digest) = 64),
        source_sealed_phase1_chain_head_sha256 TEXT NOT NULL CHECK (length(source_sealed_phase1_chain_head_sha256) = 64),
        phase2_source_binding_digest TEXT NOT NULL CHECK (length(phase2_source_binding_digest) = 64),
        canonical_evaluation_digest TEXT NOT NULL CHECK (length(canonical_evaluation_digest) = 64),
        evaluation_document_json TEXT NOT NULL,
        evaluator_identity TEXT NOT NULL,
        evaluator_source_revision TEXT NOT NULL,
        evaluated_at_utc TEXT NOT NULL,
        CHECK (
            (status = 'PASSING' AND qualified = 1)
            OR (status IN ('FAILING', 'INCOMPLETE') AND qualified = 0)
        )
    );
"""


_EXPECTED_TRIGGER_SQL = {
    "trg_single_row_final_eval": """
        CREATE TRIGGER trg_single_row_final_eval
        BEFORE INSERT ON lkg_qualification_final_evaluation
        WHEN (SELECT COUNT(*) FROM lkg_qualification_final_evaluation) >= 1
        BEGIN
            SELECT RAISE(ABORT, 'LKG qualification final evaluation ledger is single-row write-once');
        END;
    """,
    "trg_no_update_final_eval": """
        CREATE TRIGGER trg_no_update_final_eval
        BEFORE UPDATE ON lkg_qualification_final_evaluation
        BEGIN
            SELECT RAISE(ABORT, 'LKG qualification final evaluation ledger rows are append-only and immutable');
        END;
    """,
    "trg_no_delete_final_eval": """
        CREATE TRIGGER trg_no_delete_final_eval
        BEFORE DELETE ON lkg_qualification_final_evaluation
        BEGIN
            SELECT RAISE(ABORT, 'LKG qualification final evaluation ledger rows cannot be deleted');
        END;
    """,
}


def _derive_phase2_source_binding(seal: LkgRunSeal) -> Phase2SourceBinding:
    """Independently derive the expected Phase2SourceBinding identity from a
    freshly verified seal's own public fields, using only the public
    canonical-digest contract exported by ``lkg_phase2_source_binding.py``.

    Deliberately duplicates the field-construction shape of Checkpoint B's
    own (module-private) binding builder rather than importing it -- the
    same "duplicate the shape, never cross the module-privacy boundary"
    convention already used throughout this codebase (e.g.
    ``_workload_identity_document``).
    """

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
    return phase2_source_binding_from_payload(
        payload, canonical_source_binding_digest=digest
    )


@dataclass(frozen=True, slots=True)
class _VerifiedSourceSnapshot:
    seal: LkgRunSeal
    attempts: tuple[LkgQueryAttempt, ...]
    ingestions: tuple[LkgWindowReadinessIngestion, ...]
    run_binding: LkgRunBinding
    ordered_query_ids: tuple[int, ...]
    phase2_source_binding: Phase2SourceBinding


__all__ = [
    "LkgQualificationEvaluationError",
    "LkgQualificationEvaluationLedger",
]


class LkgQualificationEvaluationError(RuntimeError):
    """Fail-closed exception domain for Checkpoint C evaluation ledger errors."""

    def __init__(self, message: str, *, code: str):
        super().__init__(f"[{code}] {message}")
        self.message = message
        self.code = code


def _check_path_alias(raw_path: str, *, name: str) -> str:
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise LkgQualificationEvaluationError(
            f"{name} path must be a non-empty string", code="LKG_QUAL_EVAL_INVALID_PATH"
        )
    expanded = os.path.abspath(os.path.expanduser(raw_path))
    parent = os.path.dirname(expanded)
    if os.path.islink(expanded) or os.path.islink(parent):
        raise LkgQualificationEvaluationError(
            f"{name} path and immediate parent must not be symlinks",
            code="LKG_QUAL_EVAL_PATH_ALIAS_REJECTED",
        )
    if os.path.lexists(expanded):
        mode = os.lstat(expanded).st_mode
        if not stat.S_ISREG(mode):
            raise LkgQualificationEvaluationError(
                f"existing {name} path must be a regular file",
                code="LKG_QUAL_EVAL_INVALID_PATH",
            )
    if not os.path.isdir(parent):
        raise LkgQualificationEvaluationError(
            f"{name} parent directory must already exist",
            code="LKG_QUAL_EVAL_INVALID_PATH",
        )
    return os.path.realpath(expanded)


def _paths_alias(first: str, second: str) -> bool:
    if os.path.exists(first) and os.path.exists(second):
        try:
            return os.path.samefile(first, second)
        except OSError:
            pass
    return os.path.realpath(first) == os.path.realpath(second)


class LkgQualificationEvaluationLedger:
    """Hardened SQLite storage engine for Checkpoint C evaluation artifacts."""

    def __init__(
        self,
        db_path: str,
        *,
        phase1_ledger_path: str,
        phase2_readiness_ledger_path: str,
    ) -> None:
        self._db_path = _check_path_alias(db_path, name="evaluation ledger")

        p1_resolved = _check_path_alias(phase1_ledger_path, name="Phase-1 ledger")
        p2_resolved = _check_path_alias(
            phase2_readiness_ledger_path, name="Phase-2 readiness ledger"
        )
        if _paths_alias(p1_resolved, self._db_path):
            raise LkgQualificationEvaluationError(
                "Evaluation ledger path cannot alias Phase-1 ledger path",
                code="LKG_QUAL_EVAL_PATH_ALIAS_REJECTED",
            )
        if _paths_alias(p2_resolved, self._db_path):
            raise LkgQualificationEvaluationError(
                "Evaluation ledger path cannot alias Phase-2 readiness ledger path",
                code="LKG_QUAL_EVAL_PATH_ALIAS_REJECTED",
            )

        self._conn: sqlite3.Connection | None = None
        self._init_database()

    def _init_database(self) -> None:
        try:
            self._conn = sqlite3.connect(self._db_path, timeout=30.0, isolation_level=None)
            self._conn.execute("PRAGMA busy_timeout = 30000;")
            self._conn.execute("PRAGMA journal_mode = DELETE;")
            self._conn.execute("PRAGMA synchronous = FULL;")
            self._conn.execute("PRAGMA foreign_keys = ON;")
            self._conn.execute("PRAGMA trusted_schema = OFF;")

            os.chmod(self._db_path, 0o600)
            if stat.S_IMODE(os.stat(self._db_path).st_mode) != 0o600:
                raise LkgQualificationEvaluationError(
                    "Evaluation ledger file mode must be 0600",
                    code="LKG_QUAL_EVAL_FILE_MODE_INVALID",
                )

            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                user_ver = self._conn.execute("PRAGMA user_version;").fetchone()[0]
                if user_ver == 0:
                    existing_objects = self._schema_object_names()
                    if existing_objects:
                        raise LkgQualificationEvaluationError(
                            "Unversioned evaluation database contains schema objects",
                            code="LKG_QUAL_EVAL_INVALID_SCHEMA",
                        )
                    self._conn.execute(_EXPECTED_TABLE_SQL)
                    for trigger_sql in _EXPECTED_TRIGGER_SQL.values():
                        self._conn.execute(trigger_sql)
                    self._conn.execute(
                        f"PRAGMA user_version = {_EVALUATION_SCHEMA_VERSION};"
                    )
                elif user_ver != _EVALUATION_SCHEMA_VERSION:
                    raise LkgQualificationEvaluationError(
                        f"Unsupported database user_version {user_ver}",
                        code="LKG_QUAL_EVAL_INVALID_SCHEMA",
                    )
                self._verify_schema()
                self._conn.execute("COMMIT;")
            except BaseException:
                try:
                    self._conn.execute("ROLLBACK;")
                except Exception:
                    pass
                raise

        except LkgQualificationEvaluationError:
            self.close()
            raise
        except (sqlite3.Error, OSError) as exc:
            if self._conn:
                try:
                    self._conn.execute("ROLLBACK;")
                except Exception:
                    pass
                self.close()
            raise LkgQualificationEvaluationError(
                f"Failed to initialize evaluation ledger database: {exc}",
                code="LKG_QUAL_EVAL_DB_INIT_FAILED",
            ) from exc

    def _schema_object_names(self) -> frozenset[str]:
        if self._conn is None:
            raise LkgQualificationEvaluationError(
                "Ledger is closed", code="LKG_QUAL_EVAL_CLOSED"
            )
        rows = self._conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type IN ('table', 'trigger') AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return frozenset(row[0] for row in rows)

    def _verify_schema(self) -> None:
        if self._conn is None:
            raise LkgQualificationEvaluationError(
                "Ledger is closed", code="LKG_QUAL_EVAL_CLOSED"
            )
        if self._schema_object_names() != _EXPECTED_SCHEMA_OBJECTS:
            raise LkgQualificationEvaluationError(
                "Evaluation ledger schema-object inventory mismatch",
                code="LKG_QUAL_EVAL_INVALID_SCHEMA",
            )
        expected_columns = (
            ("source_run_id", "TEXT", 1, 1),
            ("evaluation_schema_version", "INTEGER", 1, 0),
            ("status", "TEXT", 1, 0),
            ("qualified", "INTEGER", 1, 0),
            ("evaluated_ef", "INTEGER", 1, 0),
            ("evaluation_contract_digest", "TEXT", 1, 0),
            ("ef_eligibility_rule_digest", "TEXT", 1, 0),
            ("qualification_semantics_rule_digest", "TEXT", 1, 0),
            ("source_run_binding_sha256", "TEXT", 1, 0),
            ("source_run_seal_digest", "TEXT", 1, 0),
            ("source_sealed_phase1_chain_head_sha256", "TEXT", 1, 0),
            ("phase2_source_binding_digest", "TEXT", 1, 0),
            ("canonical_evaluation_digest", "TEXT", 1, 0),
            ("evaluation_document_json", "TEXT", 1, 0),
            ("evaluator_identity", "TEXT", 1, 0),
            ("evaluator_source_revision", "TEXT", 1, 0),
            ("evaluated_at_utc", "TEXT", 1, 0),
        )
        actual_columns = tuple(
            (row[1], row[2].upper(), row[3], row[5])
            for row in self._conn.execute(
                "PRAGMA table_info(lkg_qualification_final_evaluation)"
            ).fetchall()
        )
        if actual_columns != expected_columns:
            raise LkgQualificationEvaluationError(
                "Evaluation ledger table definition mismatch",
                code="LKG_QUAL_EVAL_INVALID_SCHEMA",
            )
        stored_table_sql_row = self._conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='lkg_qualification_final_evaluation'"
        ).fetchone()
        if (
            stored_table_sql_row is None
            or _normalize_schema_sql(stored_table_sql_row[0])
            != _normalize_schema_sql(_EXPECTED_TABLE_SQL)
        ):
            raise LkgQualificationEvaluationError(
                "Evaluation ledger table SQL definition mismatch",
                code="LKG_QUAL_EVAL_INVALID_SCHEMA",
            )
        trigger_sql = {
            name: _normalize_schema_sql(sql)
            for name, sql in self._conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='trigger'"
            ).fetchall()
        }
        expected_trigger_sql = {
            name: _normalize_schema_sql(sql)
            for name, sql in _EXPECTED_TRIGGER_SQL.items()
        }
        if trigger_sql != expected_trigger_sql:
            raise LkgQualificationEvaluationError(
                "Evaluation ledger trigger definition mismatch",
                code="LKG_QUAL_EVAL_INVALID_SCHEMA",
            )
        if self._conn.execute("PRAGMA user_version;").fetchone()[0] != _EVALUATION_SCHEMA_VERSION:
            raise LkgQualificationEvaluationError(
                "Evaluation ledger user_version mismatch",
                code="LKG_QUAL_EVAL_INVALID_SCHEMA",
            )
        pragma_expectations = {
            "foreign_keys": 1,
            "trusted_schema": 0,
            "synchronous": 2,
        }
        for pragma, expected in pragma_expectations.items():
            actual = self._conn.execute(f"PRAGMA {pragma};").fetchone()[0]
            if actual != expected:
                raise LkgQualificationEvaluationError(
                    f"Evaluation ledger PRAGMA {pragma} mismatch",
                    code="LKG_QUAL_EVAL_INVALID_SCHEMA",
                )
        journal_mode = self._conn.execute("PRAGMA journal_mode;").fetchone()[0]
        if str(journal_mode).lower() != "delete":
            raise LkgQualificationEvaluationError(
                "Evaluation ledger journal_mode must be DELETE",
                code="LKG_QUAL_EVAL_INVALID_SCHEMA",
            )

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def __enter__(self) -> LkgQualificationEvaluationLedger:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def get_final_evaluation(self) -> LkgQualificationEvaluation | None:
        if self._conn is None:
            raise LkgQualificationEvaluationError("Ledger is closed", code="LKG_QUAL_EVAL_CLOSED")

        self._verify_schema()
        cursor = self._conn.execute(
            "SELECT * FROM lkg_qualification_final_evaluation ORDER BY source_run_id"
        )
        rows = cursor.fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise LkgQualificationEvaluationError(
                "Evaluation ledger must contain at most one terminal row",
                code="LKG_QUAL_EVAL_FINAL_CORRUPTED",
            )

        colnames = [desc[0] for desc in cursor.description]
        row_dict = dict(zip(colnames, rows[0]))
        return self._reconstruct_and_verify_evaluation_row(row_dict)

    def _reconstruct_and_verify_evaluation_row(
        self, row_dict: dict[str, Any]
    ) -> LkgQualificationEvaluation:
        json_text = row_dict["evaluation_document_json"]
        try:
            payload_doc = json.loads(json_text)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise LkgQualificationEvaluationError(
                f"Corrupted stored evaluation document JSON: {exc}",
                code="LKG_QUAL_EVAL_FINAL_CORRUPTED",
            ) from exc

        try:
            evaluation = lkg_qualification_evaluation_from_payload(
                payload_doc, canonical_evaluation_digest=row_dict["canonical_evaluation_digest"]
            )
        except ContractViolation as exc:
            raise LkgQualificationEvaluationError(
                f"Stored evaluation payload document violates contract: {exc}",
                code="LKG_QUAL_EVAL_FINAL_CORRUPTED",
            ) from exc

        # Verify exact byte canonicality on stored JSON text
        recomputed_bytes = canonical_json_bytes(evaluation_payload_document(evaluation))
        if json_text.encode("utf-8") != recomputed_bytes:
            raise LkgQualificationEvaluationError(
                "Stored evaluation JSON text is not byte-identical to canonical JSON re-serialization",
                code="LKG_QUAL_EVAL_FINAL_CORRUPTED",
            )

        # Cross-check denormalized SQL columns against reconstructed evaluation
        if row_dict["source_run_id"] != evaluation.source_run_id:
            raise LkgQualificationEvaluationError("source_run_id column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["evaluation_schema_version"] != evaluation.evaluation_schema_version:
            raise LkgQualificationEvaluationError("evaluation_schema_version column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["status"] != evaluation.status.value:
            raise LkgQualificationEvaluationError("status column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["qualified"] != (1 if evaluation.qualified else 0):
            raise LkgQualificationEvaluationError("qualified column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["evaluated_ef"] != evaluation.evaluated_ef:
            raise LkgQualificationEvaluationError("evaluated_ef column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["evaluation_contract_digest"] != evaluation.evaluation_contract.canonical_contract_digest:
            raise LkgQualificationEvaluationError("evaluation_contract_digest column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["ef_eligibility_rule_digest"] != evaluation.ef_eligibility_rule.canonical_rule_digest:
            raise LkgQualificationEvaluationError("ef_eligibility_rule_digest column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["qualification_semantics_rule_digest"] != evaluation.qualification_semantics_rule.canonical_rule_digest:
            raise LkgQualificationEvaluationError("qualification_semantics_rule_digest column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["source_run_binding_sha256"] != evaluation.source_run_binding_sha256:
            raise LkgQualificationEvaluationError("source_run_binding_sha256 column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["source_run_seal_digest"] != evaluation.source_run_seal_digest:
            raise LkgQualificationEvaluationError("source_run_seal_digest column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["source_sealed_phase1_chain_head_sha256"] != evaluation.source_sealed_phase1_chain_head_sha256:
            raise LkgQualificationEvaluationError("source_sealed_phase1_chain_head_sha256 column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["phase2_source_binding_digest"] != evaluation.phase2_source_binding_digest:
            raise LkgQualificationEvaluationError("phase2_source_binding_digest column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["evaluator_identity"] != evaluation.evaluator_identity:
            raise LkgQualificationEvaluationError("evaluator_identity column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["evaluator_source_revision"] != evaluation.evaluator_source_revision:
            raise LkgQualificationEvaluationError("evaluator_source_revision column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")
        if row_dict["evaluated_at_utc"] != evaluation.evaluated_at_utc:
            raise LkgQualificationEvaluationError("evaluated_at_utc column mismatch", code="LKG_QUAL_EVAL_FINAL_CORRUPTED")

        return evaluation

    def _assert_runtime_path_separation(
        self,
        *,
        phase1_ledger: LkgQualificationLedger,
        phase2_readiness_ledger: Phase2ReadinessLedger,
    ) -> None:
        phase1_path = _check_path_alias(str(phase1_ledger.path), name="Phase-1 ledger")
        phase2_path = _check_path_alias(
            str(phase2_readiness_ledger.path), name="Phase-2 readiness ledger"
        )
        if _paths_alias(self._db_path, phase1_path) or _paths_alias(
            self._db_path, phase2_path
        ):
            raise LkgQualificationEvaluationError(
                "Evaluation ledger path aliases an upstream ledger",
                code="LKG_QUAL_EVAL_PATH_ALIAS_REJECTED",
            )

    def _load_verified_sources(
        self,
        *,
        phase1_ledger: LkgQualificationLedger,
        phase2_readiness_ledger: Phase2ReadinessLedger,
    ) -> _VerifiedSourceSnapshot:
        """Load a coherent fail-closed source snapshot in C -> B -> A order.

        Checkpoint B's public verifier acquires B and verifies A internally.
        Once it returns, Checkpoint A is freshly verified again and its
        complete public evidence is loaded. Phase-1 sealing makes A immutable,
        while B's legal 12-slot geometry makes a fully closed B immutable in
        practice; an early terminal failure intentionally permits later B
        appends, which replay handles by verifying only its frozen consumed
        slots plus all currently available rows.
        """

        self._assert_runtime_path_separation(
            phase1_ledger=phase1_ledger,
            phase2_readiness_ledger=phase2_readiness_ledger,
        )
        try:
            ingestions = phase2_readiness_ledger.all_verified_ingestions()
        except Phase2ReadinessLedgerError as exc:
            raise LkgQualificationEvaluationError(
                f"Phase-2 readiness ledger verification failed: {exc}",
                code="LKG_QUAL_EVAL_PHASE2_VERIFICATION_FAILED",
            ) from exc

        try:
            seal = verify_seal(phase1_ledger)
            run_binding = phase1_ledger.stored_run_binding()
            ordered_query_ids = phase1_ledger.stored_ordered_query_ids()
            attempts = phase1_ledger.records()
        except LkgQualificationLedgerError as exc:
            raise LkgQualificationEvaluationError(
                f"Phase-1 ledger verification failed: {exc}",
                code="LKG_QUAL_EVAL_PHASE1_VERIFICATION_FAILED",
            ) from exc

        ordered_digest = lkg_ordered_query_ids_sha256(ordered_query_ids)
        identity_mismatch = (
            seal.run_id != run_binding.run_id
            or seal.run_binding_sha256 != run_binding.sha256
            or seal.expected_query_count != 2400
            or run_binding.qualification_expected_query_count != 2400
            or len(ordered_query_ids) != 2400
            or ordered_digest != seal.qualification_ordered_query_ids_sha256
            or ordered_digest != run_binding.qualification_ordered_query_ids_sha256
            or seal.workload_identity.dataset_id
            != run_binding.qualification_dataset_id
            or seal.workload_identity.dataset_version
            != run_binding.qualification_dataset_version
            or seal.workload_identity.manifest_sha256
            != run_binding.qualification_manifest_sha256
            or seal.workload_identity.query_role
            != run_binding.qualification_query_role
        )
        if identity_mismatch:
            raise LkgQualificationEvaluationError(
                "Phase-1 seal, run binding, and ordered DATASET-003 identity disagree",
                code="LKG_QUAL_EVAL_SOURCE_IDENTITY_MISMATCH",
            )

        source_binding = _derive_phase2_source_binding(seal)
        seen_windows: set[int] = set()
        for ingestion in ingestions:
            if (
                ingestion.window_index in seen_windows
                or not 0 <= ingestion.window_index < _WINDOWS_PER_RUN
                or ingestion.source_run_id != seal.run_id
                or ingestion.source_run_seal_digest
                != seal.canonical_seal_document_digest
                or ingestion.phase2_source_binding_digest
                != source_binding.canonical_source_binding_digest
                or ingestion.original_evidence.source_run_binding_sha256
                != run_binding.sha256
            ):
                raise LkgQualificationEvaluationError(
                    "Verified Phase-2 ingestion contradicts the freshly verified source binding",
                    code="LKG_QUAL_EVAL_SOURCE_IDENTITY_MISMATCH",
                )
            seen_windows.add(ingestion.window_index)

        return _VerifiedSourceSnapshot(
            seal=seal,
            attempts=attempts,
            ingestions=ingestions,
            run_binding=run_binding,
            ordered_query_ids=ordered_query_ids,
            phase2_source_binding=source_binding,
        )

    @staticmethod
    def _verify_terminal_replay(
        *,
        evaluation: LkgQualificationEvaluation,
        snapshot: _VerifiedSourceSnapshot,
        contract: LkgQualificationEvaluationContract,
        ef_rule: LkgEfEligibilityRule,
        semantics_rule: LkgQualificationSemanticsRule,
    ) -> None:
        seal = snapshot.seal
        binding = snapshot.run_binding
        source_binding = snapshot.phase2_source_binding
        current_ingestions = {
            ingestion.window_index: ingestion for ingestion in snapshot.ingestions
        }

        if (
            evaluation.evaluation_contract != contract
            or evaluation.ef_eligibility_rule != ef_rule
            or evaluation.qualification_semantics_rule != semantics_rule
        ):
            raise LkgQualificationEvaluationError(
                "Persisted evaluation contract/rules do not match the supported replay contract",
                code="LKG_QUAL_EVAL_REPLAY_MISMATCH",
            )

        expected_static_values = (
            (evaluation.source_run_id, seal.run_id),
            (evaluation.source_run_binding_sha256, binding.sha256),
            (evaluation.source_run_seal_digest, seal.canonical_seal_document_digest),
            (
                evaluation.source_sealed_phase1_chain_head_sha256,
                seal.final_chain_head_sha256,
            ),
            (evaluation.qualification_dataset_id, binding.qualification_dataset_id),
            (
                evaluation.qualification_dataset_version,
                binding.qualification_dataset_version,
            ),
            (
                evaluation.qualification_manifest_sha256,
                binding.qualification_manifest_sha256,
            ),
            (evaluation.qualification_query_role, binding.qualification_query_role),
            (
                evaluation.qualification_ordered_query_ids_sha256,
                lkg_ordered_query_ids_sha256(snapshot.ordered_query_ids),
            ),
            (evaluation.evaluated_ef, binding.search_configuration.ef),
            (
                evaluation.search_configuration_digest,
                search_configuration_sha256(binding.search_configuration),
            ),
            (
                evaluation.phase2_source_binding_digest,
                source_binding.canonical_source_binding_digest,
            ),
        )
        if any(actual != expected for actual, expected in expected_static_values):
            raise LkgQualificationEvaluationError(
                "Persisted evaluation lineage no longer matches freshly verified source evidence",
                code="LKG_QUAL_EVAL_REPLAY_MISMATCH",
            )

        for window_index, frozen_digest in enumerate(
            evaluation.window_ingestion_digests
        ):
            if frozen_digest is None:
                continue
            current = current_ingestions.get(window_index)
            if current is None or current.canonical_ingestion_digest != frozen_digest:
                raise LkgQualificationEvaluationError(
                    "A frozen readiness-ingestion digest no longer matches Phase 2",
                    code="LKG_QUAL_EVAL_REPLAY_MISMATCH",
                )

        if evaluation.status in {
            LkgQualificationStatus.PASSING,
            LkgQualificationStatus.INCOMPLETE,
        }:
            if set(current_ingestions) != set(range(_WINDOWS_PER_RUN)) or any(
                digest is None for digest in evaluation.window_ingestion_digests
            ):
                raise LkgQualificationEvaluationError(
                    "Terminal PASSING/INCOMPLETE replay requires exact 12-slot Phase-2 closure",
                    code="LKG_QUAL_EVAL_REPLAY_MISMATCH",
                )

        # Canonical SHA-256 proves accidental corruption, not authenticity:
        # a raw-database attacker could rewrite a nested statistic and its
        # unkeyed digest together. Re-evaluate only the readiness slots frozen
        # into the original artifact and compare the complete deterministic
        # result. This is verification, never replacement: the persisted
        # artifact is still the object returned. Selecting frozen slots also
        # preserves early-FAILING replay when later valid B rows were appended.
        frozen_ingestions = tuple(
            current_ingestions[window_index]
            for window_index, frozen_digest in enumerate(
                evaluation.window_ingestion_digests
            )
            if frozen_digest is not None
        )
        re_evaluated = evaluate_run(
            seal=seal,
            attempts=snapshot.attempts,
            ingestions=frozen_ingestions,
            contract=contract,
            ef_rule=ef_rule,
            semantics_rule=semantics_rule,
            search_configuration=binding.search_configuration,
            phase2_source_binding_digest=(
                source_binding.canonical_source_binding_digest
            ),
            evaluator_identity=evaluation.evaluator_identity,
            evaluator_source_revision=evaluation.evaluator_source_revision,
            evaluated_at_utc=evaluation.evaluated_at_utc,
        )
        if re_evaluated != evaluation:
            raise LkgQualificationEvaluationError(
                "Persisted evaluation verdict does not reproduce from its freshly verified frozen source evidence",
                code="LKG_QUAL_EVAL_REPLAY_MISMATCH",
            )

    def evaluate_and_finalize(
        self,
        *,
        phase1_ledger: LkgQualificationLedger,
        phase2_readiness_ledger: Phase2ReadinessLedger,
        evaluator_identity: str,
        evaluator_source_revision: str,
        evaluated_at_utc: str,
        contract: LkgQualificationEvaluationContract | None = None,
        ef_rule: LkgEfEligibilityRule | None = None,
        semantics_rule: LkgQualificationSemanticsRule | None = None,
    ) -> LkgQualificationEvaluation:
        """Evaluate evidence from Phase-1 and Phase-2 ledgers and finalize durable result.

        Lock Ordering:
            C -> B -> A. LkgQualificationEvaluationLedger acquires its write
            transaction FIRST before reading phase1_ledger (A) or
            phase2_readiness_ledger (B).
        """
        if self._conn is None:
            raise LkgQualificationEvaluationError("Ledger is closed", code="LKG_QUAL_EVAL_CLOSED")

        if contract is None:
            raw_contract = {
                "contract_schema_version": EVALUATION_CONTRACT_SCHEMA_VERSION,
                "expected_query_count": 2400,
                "windows_per_run": 12,
                "positions_per_window": 200,
                "epoch_count": 2,
                "windows_per_epoch": 6,
                "observations_per_epoch": 1200,
                "recall_floor": 0.95,
                "latency_ceiling_ms": 10.0,
                "latency_percentile": 0.95,
                "arithmetic_mean_formula_version": "fsum_arithmetic_mean.v1",
                "nearest_rank_formula_version": "nearest_rank_ceil.v1",
            }
            c_digest = evaluation_contract_payload_document_digest(raw_contract)
            contract = lkg_qualification_evaluation_contract_from_payload(
                raw_contract, canonical_contract_digest=c_digest
            )
        if ef_rule is None:
            ef_rule = default_lkg_ef_eligibility_rule()
        if semantics_rule is None:
            semantics_rule = default_lkg_qualification_semantics_rule()

        try:
            # Acquire the C write lock before reading B or A. Keeping lock
            # acquisition inside this guarded boundary also gives SQLite
            # acquisition failures the ledger's stable error contract.
            self._conn.execute("BEGIN IMMEDIATE;")
            existing = self.get_final_evaluation()
            snapshot = self._load_verified_sources(
                phase1_ledger=phase1_ledger,
                phase2_readiness_ledger=phase2_readiness_ledger,
            )
            if existing is not None:
                self._verify_terminal_replay(
                    evaluation=existing,
                    snapshot=snapshot,
                    contract=contract,
                    ef_rule=ef_rule,
                    semantics_rule=semantics_rule,
                )
                self._conn.execute("COMMIT;")
                return existing

            # Pure evaluation computation from the freshly verified snapshot.
            run_binding = snapshot.run_binding
            evaluation = evaluate_run(
                seal=snapshot.seal,
                attempts=snapshot.attempts,
                ingestions=snapshot.ingestions,
                contract=contract,
                ef_rule=ef_rule,
                semantics_rule=semantics_rule,
                search_configuration=run_binding.search_configuration,
                phase2_source_binding_digest=(
                    snapshot.phase2_source_binding.canonical_source_binding_digest
                ),
                evaluator_identity=evaluator_identity,
                evaluator_source_revision=evaluator_source_revision,
                evaluated_at_utc=evaluated_at_utc,
            )

            # Exact Phase-2 closure is twelve unique legal slots. Before
            # closure only an irreversible FAILING result is terminal.
            phase2_closed = (
                len(snapshot.ingestions) == _WINDOWS_PER_RUN
                and {item.window_index for item in snapshot.ingestions}
                == set(range(_WINDOWS_PER_RUN))
            )
            if (
                evaluation.status is LkgQualificationStatus.INCOMPLETE
                and not phase2_closed
            ):
                self._conn.execute("COMMIT;")
                return evaluation

            # Terminal state: Persist row
            json_text = canonical_json_bytes(evaluation_payload_document(evaluation)).decode("utf-8")
            self._conn.execute(
                """
                INSERT INTO lkg_qualification_final_evaluation (
                    source_run_id, evaluation_schema_version, status, qualified, evaluated_ef,
                    evaluation_contract_digest, ef_eligibility_rule_digest, qualification_semantics_rule_digest,
                    source_run_binding_sha256, source_run_seal_digest, source_sealed_phase1_chain_head_sha256,
                    phase2_source_binding_digest, canonical_evaluation_digest, evaluation_document_json,
                    evaluator_identity, evaluator_source_revision, evaluated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    evaluation.source_run_id,
                    evaluation.evaluation_schema_version,
                    evaluation.status.value,
                    1 if evaluation.qualified else 0,
                    evaluation.evaluated_ef,
                    evaluation.evaluation_contract.canonical_contract_digest,
                    evaluation.ef_eligibility_rule.canonical_rule_digest,
                    evaluation.qualification_semantics_rule.canonical_rule_digest,
                    evaluation.source_run_binding_sha256,
                    evaluation.source_run_seal_digest,
                    evaluation.source_sealed_phase1_chain_head_sha256,
                    evaluation.phase2_source_binding_digest,
                    evaluation.canonical_evaluation_digest,
                    json_text,
                    evaluation.evaluator_identity,
                    evaluation.evaluator_source_revision,
                    evaluation.evaluated_at_utc,
                ),
            )
            self._conn.execute("COMMIT;")
            return evaluation

        except BaseException as exc:
            # Rollback must happen on ANY exception to keep the transaction
            # consistent, but only expected failure categories are
            # translated into LkgQualificationEvaluationError below --
            # unexpected exceptions (programming bugs) propagate as
            # themselves rather than being masked as a generic
            # LKG_QUAL_EVAL_FINALIZATION_FAILED.
            try:
                self._conn.execute("ROLLBACK;")
            except Exception:
                pass
            if isinstance(exc, LkgQualificationEvaluationError):
                raise
            if isinstance(exc, ContractViolation):
                raise LkgQualificationEvaluationError(
                    f"Evaluation contract violation: {exc}", code="LKG_QUAL_EVAL_CONTRACT_VIOLATION"
                ) from exc
            if isinstance(exc, (sqlite3.Error, OSError)):
                raise LkgQualificationEvaluationError(
                    f"Failed to finalize evaluation ledger: {exc}", code="LKG_QUAL_EVAL_FINALIZATION_FAILED"
                ) from exc
            raise
