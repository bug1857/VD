"""The committed canonical production operator for one real LKG qualification.

Purpose:
    ADR-020 section 39 deliberately left one seam open: the readiness
    provider protects *same-file* identity and concurrency, but cannot by
    itself establish that exactly one authoritative readiness store exists
    for a ``source_run_id``. ADR-020 section 40 named the owner of that
    seam -- "the LKG preparation and operator authority" -- and forbade
    real execution until it exists. This module is that authority, and it
    is deliberately the smallest one that can be audited: it invents no
    runner, no ledger, no seal, no evaluator, no readiness model, and no
    Phase-3 semantics. Every governed decision is delegated to the module
    that already owns it.
Inputs:
    One exact-keyed operand JSON file (``OPERAND_FIELDS``), a mode, and --
    for live execution only -- two further explicit operator actions: the
    physical-search confirmation and the exact prepared-authority digest
    being authorized. No operand names a deployment-global path: the
    canonical route-state marker and the verified-latest LKG (D2) authority
    store are DERIVED from ``deployment_governance`` (ADR-022).
Outputs:
    A canonical plan document (preflight/prepare), a terminal
    Checkpoint-C report (execute), or a bound D1/D2 authority pair
    (phase3). Every mode writes its result to stdout as strict canonical
    JSON and returns 0, or raises ``LkgQualificationOperatorError``
    carrying one stable reason code.
Four modes, never fewer:
    ``preflight`` resolves and prints the plan, contacts nothing, and
    creates no file of any kind. ``prepare`` additionally prints the
    frozen ``prepared_authority_sha256`` a human is asked to authorize;
    it too creates nothing. ``execute`` re-derives that identical
    authority, refuses unless the caller supplied both the physical-search
    confirmation and the exact matching digest, and only then runs
    Phase-1 -> readiness -> seal -> Phase-2 -> terminal Checkpoint C --
    and then STOPS. ``phase3`` is a separate, no-search invocation that
    requires an externally reviewed Checkpoint-C digest.
Prepared authority (why it is derived, never stored):
    ``build_lkg_qualification_plan`` is a pure function of the operand
    file, mirroring ``exp010_gate_c_operator.build_gate_c_plan``. The
    ``source_run_id``, the run root, and every store path are operands or
    are *derived* from them -- never generated, never discovered, never
    defaulted. A retry, restart, spent run, or failed attempt therefore
    cannot mint a new run identity: there is no code path that invents
    one. A different operand file is a different authority digest and is
    refused against the digest a human actually authorized.
Complexity:
    One live execution issues exactly ``expected_query_count`` searches
    (one per not-yet-successful position, through the injected runner) and
    exactly 12 readiness observations -- one per constituent window, each
    after that window's 200 positions are canonically complete. Preflight
    and prepare issue zero of both.
Failure modes:
    Every refusal is a typed ``LkgQualificationOperatorError`` with a
    stable code, raised before the boundary it protects. Operand,
    authority-digest, execution-revision, dataset-identity,
    configuration, path, and environment-continuity failures all refuse
    *before* the first physical search and before any durable store is
    opened. A readiness provider inability persists nothing and fails
    closed; a durably observed readiness failure stops further dispatch
    and is carried into the seal and evaluation exactly as ADR-020
    section 38 requires.
Extension points:
    ``LkgOperatorDependencies`` is the single injection seam: tests supply
    fakes for the workload, runner, readiness observer, environment
    observation, and source verification. ``production_dependencies`` is
    the only place a real Milvus client, Docker socket, or health endpoint
    is ever constructed, and it is reached only from ``main``.
Authority NOT granted here:
    None of candidate generation, admission, grant reservation, routing,
    activation, canary, rollback, ``ef`` mutation, or index rebuild. This
    module imports no canary/actuation module except the read-only
    route-state record types ADR-020 section 33 requires as evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .canonical_serialization import strict_canonical_digest, strict_canonical_json_bytes
from .config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from .deployment_governance import (
    DeploymentGovernanceScope,
    canonical_deployment_governance_scope,
    ensure_deployment_scope_directory,
)
from .lkg_dataset003_loader import LkgDataset003Workload, load_dataset003_workload
from .lkg_phase2_readiness_ledger import Phase2ReadinessLedger
from .lkg_phase3_binding import LkgPhase3AuthorityPair, bind_lkg_phase3_authority
from .lkg_phase3_persistence import LkgPhase3AuthorityReferenceStore
from .lkg_phase3_authority import resolve_lkg_phase3_authority
from .lkg_qualification_evaluation_ledger import LkgQualificationEvaluationLedger
from .lkg_qualification_evidence import LkgAttemptStatus
from .lkg_qualification_ledger import LkgQualificationLedger, seal_lkg_qualification_run
from .lkg_qualification_producer import LkgQualificationProducer
from .lkg_qualification_runner import LkgQualificationRunner
from .lkg_qualification_seal import LkgSealCompletionState, derive_completion_state
from .lkg_run_binding import LkgRunBinding, lkg_ordered_query_ids_sha256
from .lkg_window_readiness_observation import (
    LkgEnvironmentObservationSpec,
    LkgMetadataReader,
    LkgWindowHealthObservation,
    LkgWindowReadinessObservationError,
    LkgWindowRollbackReadiness,
    derive_lkg_window_readiness_check_id,
    observe_lkg_window_health,
    verify_lkg_window_rollback_readiness,
)
from .lkg_window_readiness_store import SqliteLkgWindowOperationalReadinessProvider
from .search_configuration_digest import search_configuration_document, search_configuration_sha256

__all__ = [
    "LKG_QUALIFICATION_AUTHORITY_SCHEMA_VERSION",
    "OPERAND_FIELDS",
    "POSITIONS_PER_WINDOW",
    "WINDOWS_PER_RUN",
    "LkgOperatorDependencies",
    "LkgProductionWindowReadinessObserver",
    "LkgQualificationOperands",
    "LkgQualificationOperatorError",
    "MetadataOnlyMilvusReader",
    "build_lkg_qualification_plan",
    "checkpoint_c_ledger_path",
    "execute_lkg_qualification",
    "load_operands",
    "main",
    "phase1_ledger_path",
    "phase2_readiness_ledger_path",
    "production_dependencies",
    "read_route_state_record",
    "readiness_store_path",
    "resolve_and_persist_phase3_authority",
    "run_preflight",
    "verified_latest_lkg_present",
]


LKG_QUALIFICATION_AUTHORITY_SCHEMA_VERSION = "lkg-qualification-prepared-authority-v2"

# A fixed, versioned byte prefix -- never a JSON field. Disjoint from every
# EXP-012 domain, from every ADR-020/ADR-020a identity domain, and from the
# deployment-namespace domain: a prepared operator authority is not, and can
# never be mistaken for, an environment, collection-schema, index, readiness,
# seal, evaluation, or deployment-namespace identity.
#
# V2 (ADR-022). V1 bound two caller-selected global paths, which is exactly
# what made P1-A/P1-B representable; V2 binds the DERIVED canonical deployment
# scope instead. No real prepared authority and no real LKG execution ever
# existed under V1, so no compatibility path is owed -- but the document is
# materially different, and two different documents must never share one
# schema identity.
_PREPARED_AUTHORITY_DOMAIN = b"VD::LKG_QUALIFICATION_PREPARED_AUTHORITY::V2\x00"

#: The exact, closed operand set. Anything else is refused, never defaulted.
#: ``limit`` and ``consistency_level`` are deliberately absent: both are
#: contract-fixed by ``SearchConfiguration.validate()``, so accepting them as
#: operands could only ever introduce a way to disagree with the contract.
#:
#: ``route_state_path`` and ``lkg_authority_store_path`` are absent for a
#: stronger reason (ADR-022 sections 9-10): both are deployment-global serving
#: authority, and while they were operands, naming a second path created a
#: second governance universe (P1-A) or hid an ACTIVATING route behind a decoy
#: (P1-B). They are now DERIVED from the canonical deployment governance
#: scope. ``deployment_identity`` is deliberately NOT added in their place --
#: replacing a caller-chosen path with a caller-chosen identity string would
#: preserve the defect in a new spelling. It is a source fact.
OPERAND_FIELDS = (
    "base_data_identity",
    "database_name",
    "dataset001_dir",
    "dataset002_dir",
    "dataset003_dir",
    "dimensions",
    "environment_identity",
    "etcd_container",
    "execution_source_revision",
    "expected_entity_count",
    "expected_query_count",
    "flat_collection_name",
    "hnsw_collection_name",
    "index_identity",
    "index_name",
    "metric",
    "milvus_container",
    "milvus_uri",
    "minio_container",
    "producer_identity",
    "qualification_dataset_id",
    "qualification_dataset_version",
    "qualification_manifest_sha256",
    "qualification_ordered_query_ids_sha256",
    "qualification_query_array_sha256",
    "qualification_query_id_array_sha256",
    "qualification_query_role",
    "served_ef",
    "serving_configuration_identity",
    "source_run_id",
    "threshold_radius",
    "threshold_stratum",
)

WINDOWS_PER_RUN = 12
WINDOWS_PER_EPOCH = 6
POSITIONS_PER_WINDOW = 200

#: The one canonical store layout. Every path is DERIVED from ``run_root``;
#: none is separately supplied. An alternate readiness store therefore cannot
#: be selected by a caller -- it is not an input (ADR-020 section 39).
_PHASE1_FILENAME = "phase1_qualification.sqlite3"
_READINESS_FILENAME = "window_readiness.sqlite3"
_PHASE2_FILENAME = "phase2_readiness.sqlite3"
_CHECKPOINT_C_FILENAME = "checkpoint_c.sqlite3"

#: Exactly the run-scoped stores. The verified-latest LKG authority store and
#: the canonical route-state marker are deliberately NOT here: both are global
#: serving state, so a run-scoped copy of either would be permanently empty and
#: would silently defeat ADR-020 sections 4 and 28-31. Both live at the
#: deployment scope instead, DERIVED by ``deployment_governance`` and frozen in
#: the prepared authority as source facts a caller cannot choose.
_STORE_FILENAMES = (
    _PHASE1_FILENAME,
    _READINESS_FILENAME,
    _PHASE2_FILENAME,
    _CHECKPOINT_C_FILENAME,
)

#: ``source_run_id`` becomes one path component of the derived run root, so it
#: is constrained to a strict, path-safe charset. A leading alphanumeric rules
#: out ``.`` and ``..``, and the charset rules out separators, so no
#: ``source_run_id`` can escape its scope root.
_SOURCE_RUN_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")

_SEAL_REASON_COMPLETE = "LKG_QUALIFICATION_RUN_COMPLETE"
_SEAL_REASON_HALTED = "LKG_QUALIFICATION_RUN_HALTED"


class LkgQualificationOperatorError(RuntimeError):
    """Fail-closed operator error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(code if message is None else f"{code}: {message}")
        self.code = code


def _error(code: str, message: str | None = None) -> LkgQualificationOperatorError:
    return LkgQualificationOperatorError(code, message)


# -- operands -----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LkgQualificationOperands:
    """One immutable, fully cross-validated operand set."""

    base_data_identity: str
    database_name: str
    dataset001_dir: str
    dataset002_dir: str
    dataset003_dir: str
    dimensions: int
    environment_identity: str
    etcd_container: str
    execution_source_revision: str
    expected_entity_count: int
    expected_query_count: int
    flat_collection_name: str
    hnsw_collection_name: str
    index_identity: str
    index_name: str
    metric: Metric
    milvus_container: str
    milvus_uri: str
    minio_container: str
    producer_identity: str
    qualification_dataset_id: str
    qualification_dataset_version: str
    qualification_manifest_sha256: str
    qualification_ordered_query_ids_sha256: str
    qualification_query_array_sha256: str
    qualification_query_id_array_sha256: str
    qualification_query_role: str
    served_ef: int
    serving_configuration_identity: str
    source_run_id: str
    threshold_radius: float
    threshold_stratum: str

    @property
    def search_configuration(self) -> SearchConfiguration:
        """The exact candidate configuration this run qualifies.

        ``limit`` and ``consistency_level`` are left at their
        contract-fixed defaults deliberately (see ``OPERAND_FIELDS``).
        """

        return SearchConfiguration(
            metric=self.metric,
            threshold_label=self.threshold_stratum,
            radius=self.threshold_radius,
            index_track=IndexTrack.HNSW,
            ef=self.served_ef,
        )

    @property
    def environment_observation_spec(self) -> LkgEnvironmentObservationSpec:
        return LkgEnvironmentObservationSpec(
            milvus_uri=self.milvus_uri,
            database_name=self.database_name,
            etcd_container=self.etcd_container,
            minio_container=self.minio_container,
            milvus_container=self.milvus_container,
            flat_collection_name=self.flat_collection_name,
            hnsw_collection_name=self.hnsw_collection_name,
            index_name=self.index_name,
            metric=self.metric.value,
            dimensions=self.dimensions,
            expected_entity_count=self.expected_entity_count,
        )


def _text(values: Mapping[str, Any], name: str) -> str:
    value = values[name]
    if type(value) is not str or not value or len(value) > 256:
        raise _error("LKG_OPERAND_INVALID", name)
    return value


def _exact_int(values: Mapping[str, Any], name: str) -> int:
    value = values[name]
    if type(value) is not int or value <= 0:
        raise _error("LKG_OPERAND_INVALID", name)
    return value


def _real(values: Mapping[str, Any], name: str) -> float:
    value = values[name]
    if type(value) is bool or not isinstance(value, (int, float)):
        raise _error("LKG_OPERAND_INVALID", name)
    return float(value)


def _is_sha256_hex(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _sha256(values: Mapping[str, Any], name: str) -> str:
    value = values[name]
    if not _is_sha256_hex(value):
        raise _error("LKG_OPERAND_INVALID", name)
    return str(value)


def load_operands(path: str | os.PathLike[str]) -> LkgQualificationOperands:
    """Load and fully cross-validate one operand file, contacting nothing.

    Every operand is required; an unexpected key is refused rather than
    ignored. The ``SearchConfiguration`` these operands describe is
    validated here, before any store, client, or workload exists, so an
    invalid candidate configuration can never reach a live dispatch.
    """

    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise _error("LKG_OPERANDS_UNREADABLE", str(exc)) from exc
    try:
        values = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _error("LKG_OPERANDS_MALFORMED", str(exc)) from exc
    if type(values) is not dict:
        raise _error("LKG_OPERANDS_MALFORMED", "operands must be a JSON object")
    missing = sorted(set(OPERAND_FIELDS) - set(values))
    if missing:
        raise _error("LKG_OPERANDS_INCOMPLETE", ",".join(missing))
    unexpected = sorted(set(values) - set(OPERAND_FIELDS))
    if unexpected:
        raise _error("LKG_OPERANDS_UNEXPECTED", ",".join(unexpected))

    try:
        metric = Metric(_text(values, "metric"))
    except ValueError as exc:
        raise _error("LKG_OPERAND_INVALID", "metric") from exc

    operands = LkgQualificationOperands(
        base_data_identity=_text(values, "base_data_identity"),
        database_name=_text(values, "database_name"),
        dataset001_dir=_text(values, "dataset001_dir"),
        dataset002_dir=_text(values, "dataset002_dir"),
        dataset003_dir=_text(values, "dataset003_dir"),
        dimensions=_exact_int(values, "dimensions"),
        environment_identity=_text(values, "environment_identity"),
        etcd_container=_text(values, "etcd_container"),
        execution_source_revision=_text(values, "execution_source_revision"),
        expected_entity_count=_exact_int(values, "expected_entity_count"),
        expected_query_count=_exact_int(values, "expected_query_count"),
        flat_collection_name=_text(values, "flat_collection_name"),
        hnsw_collection_name=_text(values, "hnsw_collection_name"),
        index_identity=_text(values, "index_identity"),
        index_name=_text(values, "index_name"),
        metric=metric,
        milvus_container=_text(values, "milvus_container"),
        milvus_uri=_text(values, "milvus_uri"),
        minio_container=_text(values, "minio_container"),
        producer_identity=_text(values, "producer_identity"),
        qualification_dataset_id=_text(values, "qualification_dataset_id"),
        qualification_dataset_version=_text(values, "qualification_dataset_version"),
        qualification_manifest_sha256=_sha256(values, "qualification_manifest_sha256"),
        qualification_ordered_query_ids_sha256=_sha256(
            values, "qualification_ordered_query_ids_sha256"
        ),
        qualification_query_array_sha256=_sha256(values, "qualification_query_array_sha256"),
        qualification_query_id_array_sha256=_sha256(
            values, "qualification_query_id_array_sha256"
        ),
        qualification_query_role=_text(values, "qualification_query_role"),
        served_ef=_exact_int(values, "served_ef"),
        serving_configuration_identity=_text(values, "serving_configuration_identity"),
        source_run_id=_text(values, "source_run_id"),
        threshold_radius=_real(values, "threshold_radius"),
        threshold_stratum=_text(values, "threshold_stratum"),
    )

    try:
        operands.search_configuration.validate()
    except ContractViolation as exc:
        raise _error("LKG_SEARCH_CONFIGURATION_INVALID", str(exc)) from exc
    if operands.flat_collection_name == operands.hnsw_collection_name:
        raise _error("LKG_OPERAND_INVALID", "collection names must differ")
    if len({operands.etcd_container, operands.minio_container, operands.milvus_container}) != 3:
        raise _error("LKG_OPERAND_INVALID", "container names must differ")
    if operands.expected_query_count != WINDOWS_PER_RUN * POSITIONS_PER_WINDOW:
        raise _error(
            "LKG_OPERAND_INVALID",
            f"expected_query_count must equal {WINDOWS_PER_RUN * POSITIONS_PER_WINDOW}",
        )
    if _SOURCE_RUN_ID_RE.fullmatch(operands.source_run_id) is None:
        raise _error(
            "LKG_OPERAND_INVALID",
            "source_run_id must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}",
        )
    return operands


# -- the one canonical store layout ------------------------------------


def _store_path(run_root: str, filename: str) -> str:
    return str(Path(run_root) / filename)


def phase1_ledger_path(run_root: str) -> str:
    """The single Phase-1 ledger path for a run root."""

    return _store_path(run_root, _PHASE1_FILENAME)


def readiness_store_path(run_root: str) -> str:
    """The single authoritative readiness store path for a run root.

    ADR-020 section 39 requires exactly one such path per
    ``source_run_id``, frozen before any live dispatch. This function is
    the only place that path is ever produced; ``execute_lkg_qualification``
    calls it once and refuses if anything disagrees.
    """

    return _store_path(run_root, _READINESS_FILENAME)


def phase2_readiness_ledger_path(run_root: str) -> str:
    """The single Phase-2 readiness ledger path for a run root."""

    return _store_path(run_root, _PHASE2_FILENAME)


def checkpoint_c_ledger_path(run_root: str) -> str:
    """The single terminal Checkpoint-C ledger path for a run root."""

    return _store_path(run_root, _CHECKPOINT_C_FILENAME)


def _resolve_scope(
    governance_scope: DeploymentGovernanceScope | None,
) -> DeploymentGovernanceScope:
    """The canonical deployment governance scope for this invocation.

    ``None`` -- the only value any production caller can produce, because no
    CLI flag, operand, or environment variable reaches this parameter -- means
    the source-owned canonical ENV-001 scope. Tests inject an alternate scope
    in-process so they never touch real deployment state (ADR-022 section 12).
    """

    if governance_scope is None:
        return canonical_deployment_governance_scope()
    if not isinstance(governance_scope, DeploymentGovernanceScope):
        raise _error("LKG_GOVERNANCE_SCOPE_INVALID")
    return governance_scope


# -- prepared authority (derived, never stored) ------------------------


def build_lkg_qualification_plan(
    operands: LkgQualificationOperands,
    *,
    governance_scope: DeploymentGovernanceScope | None = None,
) -> dict[str, Any]:
    """Derive the complete prepared authority (V2) for one future run.

    A pure function of the operand file and source authority: no clock, no
    filesystem write, no client, no randomness. The same operands always
    produce the same ``source_run_id``, the same deployment scope, the same
    five store paths, and the same ``prepared_authority_sha256`` -- which is
    exactly why a restart, a retry, or a spent run can never acquire a new run
    identity, and why a human can authorize one exact digest.

    The document separates CALLER AUTHORITY INPUTS from DERIVED/SOURCE-GOVERNED
    AUTHORITY FACTS. Every path below is a derived fact: the deployment
    identity, its namespace digest, the governance root and scope, both
    canonical global paths, the run root, and the four run-scoped stores. No
    operand names any of them (ADR-022 section 15).

    It binds WHERE canonical deployment-global authority lives, and
    deliberately never the mutable CONTENT of that authority: freezing the
    current route-state record or the current verified-latest D2 digest into a
    human authorization would turn every legitimate live state transition into
    a re-authorization event and would convert live fail-closed revalidation
    into a stale snapshot comparison (ADR-022 section 18).
    """

    if not isinstance(operands, LkgQualificationOperands):
        raise _error("LKG_OPERANDS_INVALID", "operands must be LkgQualificationOperands")
    scope = _resolve_scope(governance_scope)
    configuration = operands.search_configuration
    configuration.validate()
    run_root = scope.run_root(operands.source_run_id)
    document: dict[str, Any] = {
        "schema_version": LKG_QUALIFICATION_AUTHORITY_SCHEMA_VERSION,
        "source_run_id": operands.source_run_id,
        "producer_identity": operands.producer_identity,
        "execution_source_revision": operands.execution_source_revision,
        "deployment_identity": scope.deployment_identity,
        "deployment_namespace_digest": scope.namespace_digest,
        "deployment_governance_root": scope.canonical_root,
        "deployment_governance_scope_root": scope.scope_root,
        "run_root": run_root,
        "store_paths": {
            "phase1_ledger": phase1_ledger_path(run_root),
            "readiness_store": readiness_store_path(run_root),
            "phase2_readiness_ledger": phase2_readiness_ledger_path(run_root),
            "checkpoint_c_ledger": checkpoint_c_ledger_path(run_root),
        },
        "canonical_global_paths": {
            "route_state": scope.route_state_path,
            "lkg_authority_store": scope.lkg_authority_store_path,
        },
        "search_configuration": search_configuration_document(configuration),
        "search_configuration_sha256": search_configuration_sha256(configuration),
        "serving_configuration_identity": operands.serving_configuration_identity,
        "environment_identity": operands.environment_identity,
        "collection_name": operands.hnsw_collection_name,
        "base_data_identity": operands.base_data_identity,
        "index_identity": operands.index_identity,
        "milvus": {
            "uri": operands.milvus_uri,
            "database_name": operands.database_name,
            "flat_collection_name": operands.flat_collection_name,
            "hnsw_collection_name": operands.hnsw_collection_name,
            "index_name": operands.index_name,
            "dimensions": operands.dimensions,
            "expected_entity_count": operands.expected_entity_count,
        },
        "containers": {
            "etcd": operands.etcd_container,
            "minio": operands.minio_container,
            "milvus": operands.milvus_container,
        },
        "dataset003": {
            "dataset_id": operands.qualification_dataset_id,
            "dataset_version": operands.qualification_dataset_version,
            "manifest_sha256": operands.qualification_manifest_sha256,
            "query_role": operands.qualification_query_role,
            "query_id_array_sha256": operands.qualification_query_id_array_sha256,
            "ordered_query_ids_sha256": operands.qualification_ordered_query_ids_sha256,
            "query_array_sha256": operands.qualification_query_array_sha256,
            "expected_query_count": operands.expected_query_count,
        },
        "dataset_dirs": {
            "dataset001_dir": operands.dataset001_dir,
            "dataset002_dir": operands.dataset002_dir,
            "dataset003_dir": operands.dataset003_dir,
        },
        "windows_per_run": WINDOWS_PER_RUN,
        "windows_per_epoch": WINDOWS_PER_EPOCH,
        "positions_per_window": POSITIONS_PER_WINDOW,
    }
    document["prepared_authority_sha256"] = strict_canonical_digest(
        _PREPARED_AUTHORITY_DOMAIN, document
    )
    return document


def run_preflight(
    operands: LkgQualificationOperands,
    *,
    governance_scope: DeploymentGovernanceScope | None = None,
) -> dict[str, Any]:
    """Resolve and report the prospective plan. Contacts nothing, writes nothing.

    Deliberately does not instantiate a ledger, a readiness provider, an
    evaluation ledger, or a Phase-3 store: every one of those constructors
    creates a durable SQLite file on construction, so preflight reports
    which paths *would* be used and whether they already exist, rather
    than opening them (ADR-020 section 41's spirit, applied to the
    operator). Scope derivation is likewise a pure read, so preflight also
    creates no deployment scope directory.

    The deployment-global existence flags are read-only *reporting*. They are
    not part of the prepared authority and never become caller authority: what
    is authorized is the canonical location, not what happens to be there now.
    """

    scope = _resolve_scope(governance_scope)
    plan = build_lkg_qualification_plan(operands, governance_scope=scope)
    run_root = scope.run_root(operands.source_run_id)
    existing = sorted(
        filename
        for filename in _STORE_FILENAMES
        if (Path(run_root) / filename).exists()
    )
    return {
        "mode": "preflight",
        "plan": plan,
        "deployment_scope_root_exists": Path(scope.scope_root).exists(),
        "run_root_exists": Path(run_root).exists(),
        "existing_store_files": existing,
        "canonical_route_state_exists": Path(scope.route_state_path).exists(),
        "canonical_lkg_authority_store_exists": Path(
            scope.lkg_authority_store_path
        ).exists(),
    }


# -- production readiness observer -------------------------------------


class MetadataOnlyMilvusReader:
    """A structurally metadata-only view over one search-capable client.

    ADR-020 section 42 requires the readiness reader to be metadata-only *by
    type*, so that a vector search is impossible by construction rather than by
    discipline. The production read-only client factory returns a real
    ``pymilvus.MilvusClient``: read-only by USE, but it still exposes
    ``search``, ``insert``, ``delete``, ``load_collection`` and index mutation.
    Handing that object straight to the observer through a weakly typed seam
    left section 42's guarantee unsatisfied -- behaviourally metadata-only, but
    only because no call site happened to reach for another method. Independent
    review classified that gap and ADR-022 section 26 deferred it to source
    convergence; ADR-023 sections 23-24 close it here.

    This is the smallest mechanism that actually closes it: forward exactly the
    four ``LkgMetadataReader`` methods and define nothing else. The provider
    receives an object on which ``search`` does not exist, so the zero-search
    boundary is a property of the type rather than of reviewer vigilance.
    ``__slots__`` and the absence of ``__getattr__`` mean no additional client
    surface can be reached or injected through it.

    A pure forwarding view. It caches nothing, normalizes nothing and re-derives
    nothing, so it can never become a second source of metadata truth alongside
    the client it wraps.
    """

    __slots__ = ("_client",)

    def __init__(self, client: Any) -> None:
        self._client = client

    def describe_collection(self, *, collection_name: str) -> object:
        return self._client.describe_collection(collection_name=collection_name)

    def describe_index(self, *, collection_name: str, index_name: str) -> object:
        return self._client.describe_index(
            collection_name=collection_name, index_name=index_name
        )

    def get_collection_stats(self, *, collection_name: str) -> object:
        return self._client.get_collection_stats(collection_name=collection_name)

    def get_load_state(self, *, collection_name: str) -> object:
        return self._client.get_load_state(collection_name=collection_name)


class LkgProductionWindowReadinessObserver:
    """The one production ``LkgWindowReadinessObserver`` for ADR-020.

    Composes the two frozen ADR-020 observation payloads -- the
    metadata-only health observation and the first-LKG bootstrap
    rollback-readiness verification -- into the single ``observe`` call
    the converged provider makes inside its write transaction. It
    performs no vector search (its ``metadata_reader`` seam is typed
    ``LkgMetadataReader``, which structurally has no search method), no
    ``ef`` change, no index rebuild, no route mutation, no grant access,
    no candidate activation, no canary, and no rollback actuation. The
    route-state record is *read* through the caller-supplied no-argument
    reader; an unreadable marker is converted to provider inability, so
    nothing is persisted (ADR-020 sections 26 and 31).
    """

    def __init__(
        self,
        *,
        spec: LkgEnvironmentObservationSpec,
        run_bound_environment_identity: str,
        baseline_search_configuration: SearchConfiguration,
        expected_baseline_search_configuration_sha256: str,
        expected_serving_configuration_identity: str,
        serving_configuration_identity_reader: Callable[[], str],
        metadata_reader: LkgMetadataReader,
        container_inspector: Callable[[str], object],
        image_inspector: Callable[[str], object],
        healthz_probe: Callable[[], bool],
        route_state_reader: Callable[[], object | None],
        verified_latest_lkg_reader: Callable[[], bool],
        clock: Callable[[], str],
    ) -> None:
        self._spec = spec
        self._environment_identity = run_bound_environment_identity
        self._baseline = baseline_search_configuration
        self._expected_baseline_sha256 = expected_baseline_search_configuration_sha256
        self._expected_serving_identity = expected_serving_configuration_identity
        self._serving_identity_reader = serving_configuration_identity_reader
        self._metadata_reader = metadata_reader
        self._container_inspector = container_inspector
        self._image_inspector = image_inspector
        self._healthz_probe = healthz_probe
        self._route_state_reader = route_state_reader
        self._verified_latest_lkg_reader = verified_latest_lkg_reader
        self._clock = clock

    def observe(
        self,
        *,
        source_run_id: str,
        source_run_binding_sha256: str,
        window_index: int,
        readiness_check_id: str,
    ) -> tuple[LkgWindowHealthObservation, LkgWindowRollbackReadiness]:
        """Perform the ONE logical readiness observation for one window."""

        observed_at_utc = self._clock()
        health = observe_lkg_window_health(
            spec=self._spec,
            run_bound_environment_identity=self._environment_identity,
            source_run_id=source_run_id,
            source_run_binding_sha256=source_run_binding_sha256,
            metadata_reader=self._metadata_reader,
            container_inspector=self._container_inspector,
            image_inspector=self._image_inspector,
            healthz_probe=self._healthz_probe,
            observed_at_utc=observed_at_utc,
        )
        try:
            route_state_record = self._route_state_reader()
        except LkgWindowReadinessObservationError:
            raise
        except Exception as exc:  # noqa: BLE001 - fail closed as provider inability
            raise LkgWindowReadinessObservationError(
                "LKG_READINESS_ROUTE_STATE_UNREADABLE"
            ) from exc
        try:
            verified_latest_present = self._verified_latest_lkg_reader()
        except Exception as exc:  # noqa: BLE001 - fail closed as provider inability
            raise LkgWindowReadinessObservationError(
                "LKG_READINESS_PHASE3_AUTHORITY_UNREADABLE"
            ) from exc
        try:
            serving_identity = self._serving_identity_reader()
        except Exception as exc:  # noqa: BLE001 - fail closed as provider inability
            raise LkgWindowReadinessObservationError(
                "LKG_READINESS_SERVING_IDENTITY_UNREADABLE"
            ) from exc
        rollback = verify_lkg_window_rollback_readiness(
            source_run_id=source_run_id,
            source_run_binding_sha256=source_run_binding_sha256,
            baseline_search_configuration=self._baseline,
            expected_baseline_search_configuration_sha256=self._expected_baseline_sha256,
            serving_configuration_identity=serving_identity,
            expected_serving_configuration_identity=self._expected_serving_identity,
            verified_latest_lkg_present=bool(verified_latest_present),
            route_state_record=route_state_record,
            verified_at_utc=observed_at_utc,
        )
        return health, rollback


# -- injection seam ----------------------------------------------------


@dataclass(frozen=True, slots=True)
class LkgOperatorDependencies:
    """The single injection seam between composition and the outside world.

    Production wiring lives in ``production_dependencies`` and is reached
    only from ``main``; tests supply deterministic fakes, so no test can
    reach Milvus, Docker, or a health endpoint even by mistake.
    """

    workload_loader: Callable[[], LkgDataset003Workload]
    runner_factory: Callable[[SearchConfiguration], LkgQualificationRunner]
    observer_factory: Callable[[LkgRunBinding], object]
    environment_identity_observer: Callable[[LkgRunBinding], str]
    execution_source_verifier: Callable[[str], None]
    clock: Callable[[], str]
    monotonic_ns: Callable[[], int]


def _utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


# -- live execution ----------------------------------------------------


def _verify_workload_identity(
    operands: LkgQualificationOperands, workload: LkgDataset003Workload
) -> tuple[int, ...]:
    """Cross-check the freshly loaded DATASET-003 against the frozen authority.

    The loader has already proven the artifacts internally consistent;
    this proves they are the *same* artifacts the prepared authority a
    human authorized was built from.
    """

    if workload.dataset_id != operands.qualification_dataset_id:
        raise _error("LKG_DATASET003_IDENTITY_MISMATCH", "dataset_id")
    if workload.dataset_version != operands.qualification_dataset_version:
        raise _error("LKG_DATASET003_IDENTITY_MISMATCH", "dataset_version")
    if workload.manifest_sha256 != operands.qualification_manifest_sha256:
        raise _error("LKG_DATASET003_IDENTITY_MISMATCH", "manifest_sha256")
    if workload.query_role != operands.qualification_query_role:
        raise _error("LKG_DATASET003_IDENTITY_MISMATCH", "query_role")
    if workload.query_id_array_sha256 != operands.qualification_query_id_array_sha256:
        raise _error("LKG_DATASET003_IDENTITY_MISMATCH", "query_id_array_sha256")
    if workload.query_array_sha256 != operands.qualification_query_array_sha256:
        raise _error("LKG_DATASET003_IDENTITY_MISMATCH", "query_array_sha256")

    query_ids = tuple(workload.query_ids)
    if len(query_ids) != operands.expected_query_count:
        raise _error("LKG_DATASET003_POPULATION_MISMATCH", "expected_query_count")
    if len(set(query_ids)) != len(query_ids):
        raise _error("LKG_DATASET003_POPULATION_MISMATCH", "duplicate query id")
    if list(query_ids) != sorted(query_ids):
        raise _error("LKG_DATASET003_POPULATION_MISMATCH", "query ids are not ordered")
    ordered_digest = lkg_ordered_query_ids_sha256(list(query_ids))
    if ordered_digest != operands.qualification_ordered_query_ids_sha256:
        raise _error("LKG_DATASET003_IDENTITY_MISMATCH", "ordered_query_ids_sha256")
    return query_ids


def _build_run_binding(
    operands: LkgQualificationOperands, query_ids: Sequence[int]
) -> LkgRunBinding:
    try:
        return LkgRunBinding(
            run_id=operands.source_run_id,
            producer_identity=operands.producer_identity,
            search_configuration=operands.search_configuration,
            collection_name=operands.hnsw_collection_name,
            base_data_identity=operands.base_data_identity,
            index_identity=operands.index_identity,
            qualification_dataset_id=operands.qualification_dataset_id,
            qualification_dataset_version=operands.qualification_dataset_version,
            qualification_manifest_sha256=operands.qualification_manifest_sha256,
            qualification_query_role=operands.qualification_query_role,
            qualification_query_id_array_sha256=(
                operands.qualification_query_id_array_sha256
            ),
            qualification_ordered_query_ids_sha256=lkg_ordered_query_ids_sha256(
                list(query_ids)
            ),
            qualification_query_array_sha256=operands.qualification_query_array_sha256,
            qualification_expected_query_count=operands.expected_query_count,
            environment_identity=operands.environment_identity,
            source_revision=operands.execution_source_revision,
        )
    except ContractViolation as exc:
        raise _error("LKG_RUN_BINDING_INVALID", str(exc)) from exc


def _window_position_counts(
    ledger: LkgQualificationLedger,
) -> tuple[dict[int, int], dict[int, int]]:
    """Per-position durable (success_count, failure_count), as the seal defines it."""

    successes: dict[int, int] = {}
    failures: dict[int, int] = {}
    for record in ledger.records():
        position = record.attempt_sequence
        if record.status is LkgAttemptStatus.SUCCESS:
            successes[position] = successes.get(position, 0) + 1
        else:
            failures[position] = failures.get(position, 0) + 1
    return successes, failures


def _window_is_canonically_complete(
    successes: Mapping[int, int], failures: Mapping[int, int], window_index: int
) -> bool:
    """Exactly the 200 positions of one window are each a single clean success."""

    start = window_index * POSITIONS_PER_WINDOW
    for position in range(start, start + POSITIONS_PER_WINDOW):
        if successes.get(position, 0) != 1 or failures.get(position, 0) != 0:
            return False
    return True


def _expected_completion_state(
    successes: Mapping[int, int], failures: Mapping[int, int], population_size: int
) -> LkgSealCompletionState:
    """Re-derive the seal's own completion state from durable evidence.

    Applies ``lkg_qualification_ledger._classify_positions``'s documented
    total function of ``(success_count, failure_count)`` and then the
    public ``derive_completion_state``. Deliberately independent: if this
    derivation were wrong, ``seal_lkg_qualification_run`` refuses rather
    than sealing a state the caller did not intend.
    """

    failed = malformed = missing = 0
    for position in range(population_size):
        success_count = successes.get(position, 0)
        failure_count = failures.get(position, 0)
        if success_count > 1:
            malformed += 1
        elif failure_count > 0:
            failed += 1
        elif success_count == 0:
            missing += 1
    return derive_completion_state(
        failed_position_count=failed,
        malformed_position_count=malformed,
        missing_position_count=missing,
    )


def _require_execution_source(
    operands: LkgQualificationOperands, dependencies: LkgOperatorDependencies
) -> None:
    try:
        dependencies.execution_source_verifier(operands.execution_source_revision)
    except LkgQualificationOperatorError:
        raise
    except Exception as exc:  # noqa: BLE001 - external verifier boundary
        code = getattr(exc, "code", None)
        raise _error(
            "LKG_EXECUTION_SOURCE_UNVERIFIED", str(code) if code else str(exc)
        ) from exc


def execute_lkg_qualification(
    operands: LkgQualificationOperands,
    *,
    dependencies: LkgOperatorDependencies,
    confirm_live_lkg_qualification_searches: bool,
    expected_prepared_authority_sha256: str | None,
    governance_scope: DeploymentGovernanceScope | None = None,
) -> dict[str, Any]:
    """Run one authorized live qualification and STOP at terminal Checkpoint C.

    Ordering is the contract. Confirmation, prepared-authority identity,
    execution source revision, DATASET-003 identity, the run binding, the
    store layout, and environment continuity are all proven *before* the
    first physical search and before any durable store is opened. No D1
    and no D2 is created here under any outcome: Phase 3 is a separate
    invocation that requires an externally reviewed digest.

    The deployment governance scope is INDEPENDENTLY re-derived here from
    source authority and the plan is rebuilt from it; the serialized paths a
    V2 document happens to contain are never trusted as inputs. The externally
    supplied expected digest must then match that freshly derived authority
    (ADR-022 section 15).
    """

    if confirm_live_lkg_qualification_searches is not True:
        raise _error("LKG_LIVE_EXECUTION_NOT_CONFIRMED")
    scope = _resolve_scope(governance_scope)
    plan = build_lkg_qualification_plan(operands, governance_scope=scope)
    authority = plan["prepared_authority_sha256"]
    if not isinstance(expected_prepared_authority_sha256, str) or not (
        expected_prepared_authority_sha256
    ):
        raise _error("LKG_PREPARED_AUTHORITY_REQUIRED")
    if expected_prepared_authority_sha256 != authority:
        raise _error(
            "LKG_PREPARED_AUTHORITY_MISMATCH",
            f"operands derive {authority}",
        )

    _require_execution_source(operands, dependencies)

    workload = dependencies.workload_loader()
    if not isinstance(workload, LkgDataset003Workload):
        raise _error("LKG_DATASET003_LOADER_INVALID")
    query_ids = _verify_workload_identity(operands, workload)
    run_binding = _build_run_binding(operands, query_ids)

    # ADR-020 sections 5 and 7: environment continuity, proven immediately
    # before dispatch and BEFORE any durable store is opened. A mismatch
    # refuses; window 0 can never become a replacement baseline because the
    # run-bound identity is the one frozen in the prepared authority.
    observed_identity = dependencies.environment_identity_observer(run_binding)
    if not isinstance(observed_identity, str) or (
        observed_identity != operands.environment_identity
    ):
        raise _error(
            "LKG_ENVIRONMENT_IDENTITY_MISMATCH",
            f"observed {observed_identity!r}",
        )

    # Every governed store this run owns lives under one private run root,
    # itself inside one private deployment governance scope. The Phase-1,
    # Phase-2 and Checkpoint-C ledgers each independently refuse a group- or
    # world-accessible parent directory, so the operator creates both private
    # and refuses an existing non-private one here, with its own reason code,
    # rather than surfacing a downstream store error. Scope creation happens
    # only now -- after every identity and continuity check -- so no read-only
    # mode can bring deployment state into being.
    ensure_deployment_scope_directory(scope)
    run_root_path = scope.run_root(operands.source_run_id)
    run_root = Path(run_root_path)
    try:
        run_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        run_root_mode = stat.S_IMODE(run_root.stat().st_mode)
    except OSError as exc:
        raise _error("LKG_RUN_ROOT_UNUSABLE", str(exc)) from exc
    if not run_root.is_dir():
        raise _error("LKG_RUN_ROOT_UNUSABLE", "run_root is not a directory")
    if run_root_mode & 0o077:
        raise _error("LKG_RUN_ROOT_NOT_PRIVATE", oct(run_root_mode))

    frozen_readiness_path = readiness_store_path(run_root_path)
    if frozen_readiness_path != plan["store_paths"]["readiness_store"]:
        raise _error("LKG_READINESS_STORE_PATH_CONFLICT")

    observer = dependencies.observer_factory(run_binding)
    runner = dependencies.runner_factory(operands.search_configuration)
    if not isinstance(runner, LkgQualificationRunner):
        raise _error("LKG_RUNNER_INVALID")

    phase1 = LkgQualificationLedger(
        phase1_ledger_path(run_root_path),
        run_binding=run_binding,
        ordered_query_ids=list(query_ids),
    )
    provider = SqliteLkgWindowOperationalReadinessProvider(
        frozen_readiness_path,
        run_binding=run_binding,
        observer=observer,  # type: ignore[arg-type]
        clock=dependencies.clock,
        monotonic_ns=dependencies.monotonic_ns,
    )
    producer = LkgQualificationProducer(
        run_binding=run_binding,
        workload=workload,
        runner=runner,
        ledger=phase1,
    )

    windows: list[dict[str, Any]] = []
    halt_reasons: list[str] = []
    readiness_failed = False
    population = len(query_ids)

    for window_index in range(WINDOWS_PER_RUN):
        successes, failures = _window_position_counts(phase1)
        if any(failures.values()):
            # ADR-020 section 38: a durable position failure permanently
            # invalidates this run. A later success can never repair it -- the
            # seal classifies the position FAILED regardless -- so dispatching
            # anything further would be a live search that cannot change the
            # outcome. Recovery requires a new prepared, reviewed, authorized
            # run, never a retry inside this spent lineage.
            halt_reasons.append("LKG_RUN_LINEAGE_SPENT")
            break
        while not _window_is_canonically_complete(successes, failures, window_index):
            result = producer.run(max_queries=POSITIONS_PER_WINDOW)
            if result.reason_codes:
                halt_reasons.extend(result.reason_codes)
                break
            if result.dispatched_query_count == 0:
                halt_reasons.append("LKG_PRODUCER_MADE_NO_PROGRESS")
                break
            successes, failures = _window_position_counts(phase1)
        if halt_reasons:
            break

        readiness_check_id = derive_lkg_window_readiness_check_id(
            source_run_id=run_binding.run_id,
            source_run_binding_sha256=run_binding.sha256,
            window_index=window_index,
        )
        first_attempt_sequence = window_index * POSITIONS_PER_WINDOW
        evidence = provider.capture_or_return(
            readiness_check_id=readiness_check_id,
            source_run_id=run_binding.run_id,
            source_run_binding_sha256=run_binding.sha256,
            window_index=window_index,
            epoch_index=window_index // WINDOWS_PER_EPOCH,
            first_attempt_sequence=first_attempt_sequence,
            last_attempt_sequence=first_attempt_sequence + POSITIONS_PER_WINDOW - 1,
        )
        windows.append(
            {
                "window_index": window_index,
                "readiness_check_id": evidence.readiness_check_id,
                "provider_run_id": evidence.provider_run_id,
                "health_passed": bool(evidence.health_passed),
                "rollback_ready": bool(evidence.rollback_ready),
                "reason_codes": list(evidence.reason_codes),
            }
        )
        if not (evidence.health_passed and evidence.rollback_ready):
            # ADR-020 section 38: stop dispatching further positions and
            # capture no further readiness. The run lineage is spent.
            readiness_failed = True
            break

    successes, failures = _window_position_counts(phase1)
    completion_state = _expected_completion_state(successes, failures, population)
    seal_reason = (
        _SEAL_REASON_COMPLETE
        if completion_state is LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL
        else _SEAL_REASON_HALTED
    )
    seal = seal_lkg_qualification_run(
        phase1,
        expected_completion_state=completion_state,
        seal_reason=seal_reason,
    )

    phase2 = Phase2ReadinessLedger(
        phase2_readiness_ledger_path(run_root_path), phase1_ledger=phase1
    )
    ingested: list[int] = []
    for window in windows:
        window_index = int(window["window_index"])
        phase2.ingest_window_readiness(
            provider=provider,  # type: ignore[arg-type]
            readiness_check_id=str(window["readiness_check_id"]),
            window_index=window_index,
        )
        ingested.append(window_index)

    evaluation_ledger = LkgQualificationEvaluationLedger(
        checkpoint_c_ledger_path(run_root_path),
        phase1_ledger_path=phase1_ledger_path(run_root_path),
        phase2_readiness_ledger_path=phase2_readiness_ledger_path(run_root_path),
    )
    try:
        evaluation = evaluation_ledger.evaluate_and_finalize(
            phase1_ledger=phase1,
            phase2_readiness_ledger=phase2,
            evaluator_identity=operands.producer_identity,
            evaluator_source_revision=operands.execution_source_revision,
            evaluated_at_utc=dependencies.clock(),
        )
    finally:
        evaluation_ledger.close()

    # TERMINAL. No D1, no D2, no candidate, no grant, no route, no canary.
    return {
        "mode": "execute",
        "prepared_authority_sha256": authority,
        "deployment_identity": scope.deployment_identity,
        "deployment_namespace_digest": scope.namespace_digest,
        "source_run_id": run_binding.run_id,
        "run_binding_sha256": run_binding.sha256,
        "store_paths": plan["store_paths"],
        "readiness_windows": windows,
        "readiness_observed_failure": readiness_failed,
        "halt_reasons": sorted(set(halt_reasons)),
        "phase1_seal_digest": seal.canonical_seal_document_digest,
        "phase1_completion_state": completion_state.value,
        "phase2_ingested_windows": ingested,
        "checkpoint_c": {
            "status": evaluation.status.value,
            "qualified": bool(evaluation.qualified),
            "canonical_evaluation_digest": evaluation.canonical_evaluation_digest,
            "evaluated_ef": evaluation.evaluated_ef,
            "source_run_binding_sha256": evaluation.source_run_binding_sha256,
            "source_run_seal_digest": evaluation.source_run_seal_digest,
            "phase2_source_binding_digest": evaluation.phase2_source_binding_digest,
        },
        "d1_created": False,
        "d2_created": False,
        "next_step": (
            "STOP. Independent review of the exact canonical_evaluation_digest is "
            "required before --mode phase3 may be invoked."
        ),
    }


# -- separate Phase-3 authority persistence ----------------------------


def resolve_and_persist_phase3_authority(
    operands: LkgQualificationOperands,
    *,
    dependencies: LkgOperatorDependencies,
    expected_checkpoint_c_digest: str,
    governance_scope: DeploymentGovernanceScope | None = None,
) -> dict[str, Any]:
    """Resolve D1 against an externally reviewed digest and persist D2.

    Issues no search and mutates no serving, routing, grant, candidate,
    canary, or rollback state. The expected digest is mandatory and is
    never read back from the run's own freshly produced evaluation: it is
    supplied by the operator after independent review, which is the whole
    point of the D1 boundary (ADR-020's Phase-3 separation).

    The run binding is rebuilt from the same operands and the same
    DATASET-003 artifacts the run executed against; the Phase-1 ledger
    itself then re-validates it against the binding it durably stored, so
    a changed operand file cannot silently rebind an executed run.
    """

    if not _is_sha256_hex(expected_checkpoint_c_digest):
        raise _error("LKG_EXPECTED_CHECKPOINT_C_DIGEST_REQUIRED")

    _require_execution_source(operands, dependencies)

    scope = _resolve_scope(governance_scope)
    run_root = scope.run_root(operands.source_run_id)
    for filename in (
        _PHASE1_FILENAME,
        _READINESS_FILENAME,
        _PHASE2_FILENAME,
        _CHECKPOINT_C_FILENAME,
    ):
        if not (Path(run_root) / filename).exists():
            raise _error("LKG_PHASE3_SOURCE_STORE_MISSING", filename)

    workload = dependencies.workload_loader()
    if not isinstance(workload, LkgDataset003Workload):
        raise _error("LKG_DATASET003_LOADER_INVALID")
    query_ids = _verify_workload_identity(operands, workload)
    run_binding = _build_run_binding(operands, query_ids)

    phase1 = LkgQualificationLedger(
        phase1_ledger_path(run_root),
        run_binding=run_binding,
        ordered_query_ids=list(query_ids),
    )
    phase2 = Phase2ReadinessLedger(
        phase2_readiness_ledger_path(run_root), phase1_ledger=phase1
    )
    evaluation_ledger = LkgQualificationEvaluationLedger(
        checkpoint_c_ledger_path(run_root),
        phase1_ledger_path=phase1_ledger_path(run_root),
        phase2_readiness_ledger_path=phase2_readiness_ledger_path(run_root),
    )
    try:
        resolution = resolve_lkg_phase3_authority(
            evaluation_ledger=evaluation_ledger,
            phase1_ledger=phase1,
            phase2_readiness_ledger=phase2,
            run_binding=run_binding,
            expected_canonical_evaluation_digest=expected_checkpoint_c_digest,
        )
        if resolution.authority is None:
            raise _error(
                "LKG_PHASE3_AUTHORITY_REFUSED",
                ",".join(sorted(resolution.reason_codes)),
            )
        store = LkgPhase3AuthorityReferenceStore(scope.lkg_authority_store_path)
        try:
            append = store.append(
                resolution.authority, persisted_at_utc=dependencies.clock()
            )
            verified_latest = store.load_verified_latest()
            if verified_latest is None:
                raise _error("LKG_PHASE3_VERIFIED_LATEST_MISSING")
            pair = bind_lkg_phase3_authority(
                authority=resolution.authority,
                verified_latest_reference=verified_latest,
            )
        finally:
            store.close()
    finally:
        evaluation_ledger.close()

    return {
        "mode": "phase3",
        "source_run_id": operands.source_run_id,
        "run_binding_sha256": run_binding.sha256,
        "expected_checkpoint_c_digest": expected_checkpoint_c_digest,
        "d1_resolved": True,
        "d2_appended": bool(append.appended),
        "verified_latest_present": True,
        "authority_pair_bound": isinstance(pair, LkgPhase3AuthorityPair),
        "deployment_identity": scope.deployment_identity,
        "lkg_authority_store_path": scope.lkg_authority_store_path,
    }


# -- production wiring (reached only from main) ------------------------


def read_route_state_record(route_state_path: str) -> object | None:
    """The no-argument route-state read (ADR-020 section 28).

    A MECHANISM, not the authority: production callers pass
    ``DeploymentGovernanceScope.route_state_path`` and nothing else, so the
    marker read is always this deployment's one canonical marker. The explicit
    path argument remains because unit tests and future composition roots
    inject a store path directly (ADR-022 sections 11-12).

    Reads the deployment-global marker, never a run-scoped file. ``load`` is
    the only method called; the store's ``begin_activation``/``clear_to_lkg``
    mutators are never reached from this module. Absence returns ``None``; an
    unreadable or corrupt marker raises, which ADR-020 section 31 classifies as
    provider inability -- fail closed, persist nothing.
    """

    from .canary_route_state import FileCanaryRouteStateStore

    return FileCanaryRouteStateStore(route_state_path).load()


def verified_latest_lkg_present(lkg_authority_store_path: str) -> bool:
    """Does a verified-latest Phase-3 D1/D2 authority already exist?

    A MECHANISM, not the authority: production callers pass
    ``DeploymentGovernanceScope.lkg_authority_store_path``, so the question is
    always asked of this deployment's one canonical D2 store. A missing store
    file is genuine absence -- that IS the first-LKG bootstrap case. An
    existing store yielding a verified latest makes ADR-020 section 4's
    first-LKG-only refusal fire. A corrupt or unreadable store raises rather
    than reporting absence, so it fails closed.
    """

    path = Path(lkg_authority_store_path)
    if not path.exists():
        return False
    store = LkgPhase3AuthorityReferenceStore(str(path))
    try:
        return store.load_verified_latest() is not None
    finally:
        store.close()


def production_dependencies(
    operands: LkgQualificationOperands,
    *,
    governance_scope: DeploymentGovernanceScope | None = None,
) -> LkgOperatorDependencies:
    """Build the real, live dependency set. Reached only from ``main``.

    This is the single place a PyMilvus client, a Docker socket, or a
    health endpoint is ever constructed in this module. Exactly one
    search-capable client is ever built here, by ``_runner_factory``, for the
    authorized qualification searches themselves. Every readiness path instead
    receives a ``MetadataOnlyMilvusReader``, so the object reaching the
    observer structurally has no search method (ADR-020 section 42, ADR-023
    sections 23-24).

    The route-state and verified-latest readers close over the CANONICAL
    deployment scope paths, so the readiness observer can only ever consult
    this deployment's one route-state marker and its one D2 authority store
    (ADR-022 sections 9-11). Both remain LIVE: the observer re-reads them at
    every readiness window under ADR-020's unchanged cadence.
    """

    from time import monotonic_ns

    import numpy as np

    from .artifacts import verify_dataset_artifacts
    from .gate_c_execution_environment import DockerExecutionMetadataInspector
    from .gate_c_execution_source import derive_gate_c_execution_source
    from .v2_milvus_shadow_capture import build_readonly_milvus_client

    def _load_workload() -> LkgDataset003Workload:
        return load_dataset003_workload(
            operands.dataset003_dir,
            dataset001_dir=operands.dataset001_dir,
            dataset002_dir=operands.dataset002_dir,
        )

    def _base_arrays() -> tuple[Any, Any]:
        dataset001 = Path(operands.dataset001_dir)
        verify_dataset_artifacts(dataset001)
        base_ids = np.load(dataset001 / "base_ids.npy", allow_pickle=False)
        base_vectors = np.load(dataset001 / "base_vectors.npy", allow_pickle=False)
        return (
            np.asarray(base_vectors, dtype="<f4"),
            np.asarray(base_ids, dtype=np.int64),
        )

    def _runner_factory(configuration: SearchConfiguration) -> LkgQualificationRunner:
        base_vectors, base_ids = _base_arrays()
        return LkgQualificationRunner.from_uri(
            operands.milvus_uri,
            dimensions=operands.dimensions,
            hnsw_collection_name=operands.hnsw_collection_name,
            base_vectors=base_vectors,
            base_ids=base_ids,
        )

    def _healthz_probe() -> bool:
        import urllib.request

        from .config import ENV001_PINS

        try:
            with urllib.request.urlopen(ENV001_PINS.health_uri, timeout=2.0) as response:
                return response.status == 200
        except (OSError, ValueError):
            return False

    def _metadata_only_reader() -> LkgMetadataReader:
        """The ONE readiness-facing Milvus surface, metadata-only by type.

        Every readiness and environment-observation path goes through here, so
        a raw search-capable client cannot reach the observer even by a future
        edit that forgets to wrap one.
        """

        return MetadataOnlyMilvusReader(build_readonly_milvus_client(operands.milvus_uri))

    scope = _resolve_scope(governance_scope)

    def _route_state_reader() -> object | None:
        return read_route_state_record(scope.route_state_path)

    def _verified_latest_lkg_reader() -> bool:
        return verified_latest_lkg_present(scope.lkg_authority_store_path)

    def _observe_environment_identity(run_binding: LkgRunBinding) -> str:
        """Re-observe the stable LKG environment identity before dispatch.

        Uses the canonical public ADR-020 observation entry point, so the
        identity compared here is produced by exactly the machinery that
        will produce it again at every window boundary. Metadata reads
        only: the reader is a ``MetadataOnlyMilvusReader`` exposing no search
        method, so no search is possible by type (ADR-020 sections 41-42).
        """

        client = _metadata_only_reader()
        inspector = DockerExecutionMetadataInspector()
        observation = observe_lkg_window_health(
            spec=operands.environment_observation_spec,
            run_bound_environment_identity=run_binding.environment_identity,
            source_run_id=run_binding.run_id,
            source_run_binding_sha256=run_binding.sha256,
            metadata_reader=client,
            container_inspector=inspector.inspect_container,
            image_inspector=inspector.inspect_image,
            healthz_probe=_healthz_probe,
            observed_at_utc=_utc_now(),
        )
        return str(observation.document["observed_environment_identity"])

    def _observer_factory(run_binding: LkgRunBinding) -> object:
        client = _metadata_only_reader()
        inspector = DockerExecutionMetadataInspector()
        return LkgProductionWindowReadinessObserver(
            spec=operands.environment_observation_spec,
            run_bound_environment_identity=run_binding.environment_identity,
            baseline_search_configuration=operands.search_configuration,
            expected_baseline_search_configuration_sha256=search_configuration_sha256(
                operands.search_configuration
            ),
            expected_serving_configuration_identity=(
                operands.serving_configuration_identity
            ),
            serving_configuration_identity_reader=(
                lambda: operands.serving_configuration_identity
            ),
            metadata_reader=client,
            container_inspector=inspector.inspect_container,
            image_inspector=inspector.inspect_image,
            healthz_probe=_healthz_probe,
            route_state_reader=_route_state_reader,
            verified_latest_lkg_reader=_verified_latest_lkg_reader,
            clock=_utc_now,
        )

    def _verify_execution_source(expected_revision: str) -> None:
        derive_gate_c_execution_source(expected_revision=expected_revision)

    return LkgOperatorDependencies(
        workload_loader=_load_workload,
        runner_factory=_runner_factory,
        observer_factory=_observer_factory,
        environment_identity_observer=_observe_environment_identity,
        execution_source_verifier=_verify_execution_source,
        clock=_utc_now,
        monotonic_ns=monotonic_ns,
    )


# -- CLI ---------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operands",
        type=Path,
        required=True,
        help="Path to the exact-keyed LKG qualification operand JSON file.",
    )
    parser.add_argument(
        "--mode",
        choices=("preflight", "prepare", "execute", "phase3"),
        required=True,
        help=(
            "preflight and prepare contact nothing, create nothing, and issue "
            "zero searches; execute runs the live qualification and stops at "
            "terminal Checkpoint C; phase3 persists reviewed D1/D2 authority "
            "and issues zero searches."
        ),
    )
    parser.add_argument(
        "--confirm-live-lkg-qualification-searches",
        action="store_true",
        help=(
            "Required with --mode execute. A second, explicit operator action, "
            "deliberately separate from choosing the mode, acknowledging that "
            "real DATASET-003 HNSW searches will be issued."
        ),
    )
    parser.add_argument(
        "--expect-prepared-authority-sha256",
        default=None,
        help=(
            "Required with --mode execute. The exact prepared-authority digest "
            "a human authorized, as printed by --mode prepare. Confirmation "
            "alone is never sufficient."
        ),
    )
    parser.add_argument(
        "--expected-checkpoint-c-digest",
        default=None,
        help=(
            "Required with --mode phase3. The externally reviewed canonical "
            "Checkpoint-C evaluation digest. Never read back from the run's own "
            "freshly produced result."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Real operator entry point; never invoked by this repository's own code."""

    args = _parser().parse_args(argv)
    operands = load_operands(args.operands)

    if args.mode == "preflight":
        report: dict[str, Any] = run_preflight(operands)
    elif args.mode == "prepare":
        report = {"mode": "prepare", "plan": build_lkg_qualification_plan(operands)}
    elif args.mode == "execute":
        report = execute_lkg_qualification(
            operands,
            dependencies=production_dependencies(operands),
            confirm_live_lkg_qualification_searches=bool(
                args.confirm_live_lkg_qualification_searches
            ),
            expected_prepared_authority_sha256=args.expect_prepared_authority_sha256,
        )
    else:
        report = resolve_and_persist_phase3_authority(
            operands,
            dependencies=production_dependencies(operands),
            expected_checkpoint_c_digest=args.expected_checkpoint_c_digest or "",
        )

    sys.stdout.write(strict_canonical_json_bytes(report).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
