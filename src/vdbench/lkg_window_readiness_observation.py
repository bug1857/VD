"""ADR-020 first-LKG operational-readiness observation payloads.

Purpose:
    Give ADR-020's production readiness provider
    (``lkg_window_readiness_store.py``) the two zero-actuation evidence
    payloads its ``LkgWindowOperationalReadinessEvidence`` must cite --
    one health observation and one first-LKG bootstrap
    rollback-readiness verification -- plus the deterministic
    ``readiness_check_id`` / ``provider_run_id`` derivations and the
    stable run-bound LKG environment identity those payloads compare
    against. This module owns ADR-020's canonical documents, digest
    domains, and PASS/FAIL predicates. It never persists anything, never
    opens a database, and never mutates serving, routing, grant,
    candidate, canary, or index state.
Zero-actuation boundary (ADR-020 section 41):
    Every observation here is a metadata/control-plane read performed
    through an injected port: a metadata-only Milvus reader that
    structurally has no ``search`` method, read-only Docker container and
    image inspection, and a health-endpoint probe. There is no vector,
    ANN, or hybrid search anywhere in this module, no ``ef`` change, no
    index rebuild, no route mutation, no grant creation or reservation,
    no candidate activation, no canary, and no rollback actuation.
Stable identity versus transient health (ADR-020 sections 5-8):
    ``LkgRunBinding.environment_identity``, fixed before any client
    dispatch, is the sole stable environment authority. Every window --
    including window 0 -- compares against it; window 0 never establishes
    a replacement baseline. The stable environment document carries only
    identity-bearing facts; the four readiness predicate groups and the
    observation timestamp are transient and are never folded into the
    stable identity.
Normalization and identity ownership (ADR-020 section 9, Amendment ADR-020a):
    The Gate-C execution-environment module exports its metadata reader
    protocol and Docker inspector publicly, but keeps its container,
    data-plane and endpoint normalizers -- and its collection-schema and
    index-identity digest builders -- private. Those private helpers are
    NOT ADR-020 dependencies, so this module performs its own
    deterministic LKG-local normalization and computes its own
    LKG-specific sub-digests under
    ``vdbench.lkg-collection-schema.v1`` and
    ``vdbench.lkg-index-identity.v1``. EXP-012 digest domains are never
    reused, a Gate-C digest value is never interchangeable with an LKG
    digest value, and the Gate-C attestation -- which binds an execution
    source revision and EXP-012 governed campaign bindings meaningless
    inside an LKG window record -- is not reused either.
Failure modes:
    Structural/contract violations raise ``ContractViolation``. An
    inability to obtain a trustworthy observation raises
    ``LkgWindowReadinessObservationError``, which ADR-020 section 26
    classifies as provider inability: the caller must persist nothing.
    An observation that succeeds but disagrees with the run-bound
    authority is an *observed failure*, returned as a real document with
    a real digest and ``passed``/``ready`` false -- never an exception.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from .artifacts import canonical_json_bytes
from .config import ContractViolation, HNSW_EF_SWEEP, Metric, SearchConfiguration, THRESHOLD_LABELS
from .lkg_window_readiness import validate_rfc3339_utc
from .search_configuration_digest import (
    search_configuration_document,
    search_configuration_from_document,
    search_configuration_sha256,
)

__all__ = [
    "LKG_COLLECTION_SCHEMA_DOMAIN",
    "LKG_COLLECTION_SCHEMA_SCHEMA_VERSION",
    "LKG_ENVIRONMENT_IDENTITY_DOMAIN",
    "LKG_ENVIRONMENT_IDENTITY_PREFIX",
    "LKG_ENVIRONMENT_IDENTITY_SCHEMA_VERSION",
    "LKG_HEALTH_OBSERVATION_DOMAIN",
    "LKG_HEALTH_OBSERVATION_SCHEMA_VERSION",
    "LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY",
    "LKG_INDEX_IDENTITY_DOMAIN",
    "LKG_INDEX_IDENTITY_SCHEMA_VERSION",
    "LKG_PROVIDER_RUN_ID_DOMAIN",
    "LKG_READINESS_CHECK_ID_DOMAIN",
    "LKG_ROLLBACK_READINESS_DOMAIN",
    "LKG_ROLLBACK_READINESS_SCHEMA_VERSION",
    "LKG_ROLLBACK_READINESS_SOURCE_IDENTITY",
    "LKG_ROLLBACK_VERIFICATION_MODE",
    "PROVIDER_IMPLEMENTATION_IDENTITY",
    "READINESS_REASON_CODES",
    "LkgEnvironmentObservationSpec",
    "LkgMetadataReader",
    "LkgWindowHealthObservation",
    "LkgWindowReadinessObservationError",
    "LkgWindowRollbackReadiness",
    "build_lkg_stable_environment_document",
    "derive_lkg_window_provider_run_id",
    "derive_lkg_window_readiness_check_id",
    "lkg_collection_schema_document",
    "lkg_collection_schema_sha256",
    "lkg_environment_identity",
    "lkg_index_identity_document",
    "lkg_index_identity_sha256",
    "observe_lkg_window_health",
    "validate_lkg_window_health_observation",
    "validate_lkg_window_rollback_readiness",
    "verify_lkg_window_rollback_readiness",
]


# -- ADR-020 frozen constants ------------------------------------------

LKG_ENVIRONMENT_IDENTITY_SCHEMA_VERSION = "lkg-environment-identity-v1"
LKG_ENVIRONMENT_IDENTITY_DOMAIN = b"vdbench.lkg-environment-identity.v1\0"
LKG_ENVIRONMENT_IDENTITY_PREFIX = "lkg-env-identity-v1"

# Amendment (ADR-020a): LKG-specific sub-digest identities. These are NEVER the
# EXP-012 Gate-C collection-schema/index-identity values, whose domains this
# module must not reuse and whose private builders it must not import.
LKG_COLLECTION_SCHEMA_SCHEMA_VERSION = "lkg-collection-schema-v1"
LKG_COLLECTION_SCHEMA_DOMAIN = b"vdbench.lkg-collection-schema.v1\0"
LKG_INDEX_IDENTITY_SCHEMA_VERSION = "lkg-index-identity-v1"
LKG_INDEX_IDENTITY_DOMAIN = b"vdbench.lkg-index-identity.v1\0"

LKG_HEALTH_OBSERVATION_SCHEMA_VERSION = 1
LKG_HEALTH_OBSERVATION_DOMAIN = b"vdbench.lkg-window-health-observation.v1\0"
LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY = "vdbench.lkg-window-health-observation.v1"

LKG_ROLLBACK_READINESS_SCHEMA_VERSION = 1
LKG_ROLLBACK_READINESS_DOMAIN = b"vdbench.lkg-window-rollback-readiness.v1\0"
LKG_ROLLBACK_VERIFICATION_MODE = "FIRST_LKG_BOOTSTRAP_BASELINE_RESTORABILITY"
LKG_ROLLBACK_READINESS_SOURCE_IDENTITY = (
    "vdbench.lkg-window-rollback-readiness.v1:"
    "FIRST_LKG_BOOTSTRAP_BASELINE_RESTORABILITY"
)

LKG_READINESS_CHECK_ID_DOMAIN = b"vdbench.lkg-window-readiness-check-id.v1\0"
LKG_PROVIDER_RUN_ID_DOMAIN = b"vdbench.lkg-window-readiness-provider-run.v1\0"

PROVIDER_IMPLEMENTATION_IDENTITY = (
    "vdbench.lkg_window_readiness_store."
    "SqliteLkgWindowOperationalReadinessProvider"
)

# ADR-020 section 37. Sorted, unique, canonical.
READINESS_REASON_CODES = (
    "BASELINE_CONFIGURATION_DIGEST_MISMATCH",
    "BASELINE_CONFIGURATION_UNRECONSTRUCTABLE",
    "BOOTSTRAP_LKG_ROUTE_MARKER_PRESENT",
    "CANDIDATE_ROUTE_ACTIVE",
    "COLLECTION_NOT_LOADED",
    "CONTAINER_NOT_RUNNING",
    "CONTAINER_OOM_KILLED",
    "CONTAINER_UNHEALTHY",
    "ENTITY_COUNT_MISMATCH",
    "ENVIRONMENT_IDENTITY_MISMATCH",
    "INDEX_NOT_READY",
    "MILVUS_HEALTHZ_FAILED",
    "RESTORATION_TARGET_UNRESOLVED",
    "SERVING_CONFIGURATION_IDENTITY_MISMATCH",
)

_READINESS_SCHEMA_VERSION = 1
_WINDOWS_PER_RUN = 12
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_IDENTITY_RE = re.compile(
    rf"{re.escape(LKG_ENVIRONMENT_IDENTITY_PREFIX)}:sha256:[0-9a-f]{{64}}\Z"
)
_MAX_TEXT_CODEPOINTS = 256
# Ordering frozen by Amendment (ADR-020a) so the identity is independently
# reproducible by a future operator before dispatch.
_CONTAINER_ROLES = ("etcd", "minio", "milvus")
_DATA_PLANE_ROLES = ("flat", "hnsw")


class LkgWindowReadinessObservationError(RuntimeError):
    """Provider inability to obtain a trustworthy observation.

    ADR-020 section 26: the caller persists nothing for this. It is not
    an observed readiness failure.
    """

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> LkgWindowReadinessObservationError:
    return LkgWindowReadinessObservationError(code)


# -- small local validators (no module-private imports) ----------------


def _text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ContractViolation(f"{field} must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or normalized != value
        or value.strip() != value
        or len(value) > _MAX_TEXT_CODEPOINTS
        or any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value)
    ):
        raise ContractViolation(f"{field} is not canonical")
    return value


def _sha256_hex(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ContractViolation(
            f"{field} must be a lowercase 64-character hex SHA-256 digest"
        )
    return value


def _exact_int(value: object, *, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ContractViolation(f"{field} must be an int >= {minimum}")
    return value


def _rfc3339_utc(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _RFC3339_UTC_RE.fullmatch(value) is None:
        raise ContractViolation(f"{field} must be a canonical RFC3339 UTC timestamp")
    return value


def _sorted_reason_codes(codes: Sequence[str]) -> tuple[str, ...]:
    unknown = sorted(set(codes) - set(READINESS_REASON_CODES))
    if unknown:
        raise ContractViolation(f"unknown readiness reason codes: {unknown}")
    return tuple(sorted(set(codes)))


# -- deterministic identifiers (ADR-020 sections 13-14) ----------------


def derive_lkg_window_readiness_check_id(
    *, source_run_id: str, source_run_binding_sha256: str, window_index: int
) -> str:
    """ADR-020 section 13's deterministic, domain-separated check id."""

    payload = {
        "source_run_id": _text(source_run_id, field="source_run_id"),
        "source_run_binding_sha256": _sha256_hex(
            source_run_binding_sha256, field="source_run_binding_sha256"
        ),
        "window_index": _exact_int(window_index, field="window_index"),
    }
    if payload["window_index"] >= _WINDOWS_PER_RUN:
        raise ContractViolation(f"window_index must be in [0, {_WINDOWS_PER_RUN})")
    return hashlib.sha256(
        LKG_READINESS_CHECK_ID_DOMAIN + canonical_json_bytes(payload)
    ).hexdigest()


def derive_lkg_window_provider_run_id(
    *,
    readiness_check_id: str,
    source_run_id: str,
    source_run_binding_sha256: str,
    provider_implementation_identity: str = PROVIDER_IMPLEMENTATION_IDENTITY,
) -> str:
    """ADR-020 section 14's per-logical-check provider provenance.

    Deliberately carries no process-start timestamp, so a retry or a
    restart of the same logical check reproduces the same value and a
    committed capture can never be redefined.
    """

    payload = {
        "provider_implementation_identity": _text(
            provider_implementation_identity, field="provider_implementation_identity"
        ),
        "readiness_schema_version": _READINESS_SCHEMA_VERSION,
        "readiness_check_id": _sha256_hex(
            readiness_check_id, field="readiness_check_id"
        ),
        "source_run_id": _text(source_run_id, field="source_run_id"),
        "source_run_binding_sha256": _sha256_hex(
            source_run_binding_sha256, field="source_run_binding_sha256"
        ),
    }
    return hashlib.sha256(
        LKG_PROVIDER_RUN_ID_DOMAIN + canonical_json_bytes(payload)
    ).hexdigest()


# -- observation ports --------------------------------------------------


class LkgMetadataReader(Protocol):
    """Metadata-only Milvus surface; deliberately has no search method.

    Structurally identical to the Gate-C metadata reader protocol, so a
    single read-only client satisfies both. Typing the provider against
    this makes a vector search impossible by type rather than by
    discipline (ADR-020 section 42).
    """

    def describe_collection(self, *, collection_name: str) -> object: ...
    def describe_index(self, *, collection_name: str, index_name: str) -> object: ...
    def get_collection_stats(self, *, collection_name: str) -> object: ...
    def get_load_state(self, *, collection_name: str) -> object: ...


@dataclass(frozen=True, slots=True)
class LkgEnvironmentObservationSpec:
    """Exactly what one LKG readiness observation inspects."""

    milvus_uri: str
    database_name: str
    etcd_container: str
    minio_container: str
    milvus_container: str
    flat_collection_name: str
    hnsw_collection_name: str
    index_name: str
    metric: str
    dimensions: int
    expected_entity_count: int

    def __post_init__(self) -> None:
        for field in (
            "milvus_uri", "database_name", "etcd_container", "minio_container",
            "milvus_container", "flat_collection_name", "hnsw_collection_name",
            "index_name", "metric",
        ):
            _text(getattr(self, field), field=field)
        _exact_int(self.dimensions, field="dimensions", minimum=1)
        _exact_int(self.expected_entity_count, field="expected_entity_count", minimum=1)


@dataclass(frozen=True, slots=True)
class LkgWindowHealthObservation:
    """One completed health observation: document, digest, verdict."""

    document: dict[str, object]
    digest: str
    passed: bool
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LkgWindowRollbackReadiness:
    """One completed bootstrap rollback-readiness verification."""

    document: dict[str, object]
    digest: str
    ready: bool
    reason_codes: tuple[str, ...]


# -- retained-evidence validation (Amendment ADR-020b) -------------------


def _document_fields(document: object, fields: Mapping[str, object]) -> dict:
    """Exact JSON shapes/types; never coerce bools, tuples or custom objects."""

    if type(document) is not dict or document.keys() != fields.keys():
        raise ContractViolation("readiness source document fields differ")
    for name, allowed in fields.items():
        types = allowed if isinstance(allowed, tuple) else (allowed,)
        if type(document[name]) not in types:
            raise ContractViolation(f"readiness source {name} has invalid type")
    return document


def _source_result_bytes(
    result: LkgWindowHealthObservation | LkgWindowRollbackReadiness,
    *, domain: bytes, verdict: bool, allowed_reasons: set[str],
    source_run_id: str, source_run_binding_sha256: str,
) -> bytes:
    document = result.document
    if document["source_run_id"] != _text(source_run_id, field="source_run_id"):
        raise ContractViolation("readiness source run identity differs")
    if document["source_run_binding_sha256"] != _sha256_hex(
        source_run_binding_sha256, field="source_run_binding_sha256"
    ):
        raise ContractViolation("readiness source binding identity differs")
    reasons = document["reason_codes"]
    if (
        any(type(code) is not str for code in reasons)
        or not set(reasons) <= allowed_reasons
        or tuple(reasons) != _sorted_reason_codes(reasons)
        or type(result.reason_codes) is not tuple
        or result.reason_codes != tuple(reasons)
    ):
        raise ContractViolation("readiness source reason codes are inconsistent")
    # Both existing builders define success EXACTLY as an empty reason set.
    # Recorded reasons are not regenerated from omitted runtime inputs.
    if type(verdict) is not bool or verdict != (not reasons):
        raise ContractViolation("readiness source verdict is inconsistent")
    try:
        raw = canonical_json_bytes(document)
        restored = json.loads(raw.decode("utf-8"))
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise ContractViolation("readiness source is not canonical JSON") from exc
    if canonical_json_bytes(restored) != raw:
        raise ContractViolation("readiness source is not canonical JSON")
    if _sha256_hex(result.digest, field="digest") != hashlib.sha256(domain + raw).hexdigest():
        raise ContractViolation("readiness source digest differs")
    return raw


def _require_recorded_predicate(reasons: list[str], code: str, failed: bool) -> None:
    if (code in reasons) != failed:
        raise ContractViolation(f"readiness retained predicate disagrees with {code}")


def validate_lkg_window_health_observation(
    result: LkgWindowHealthObservation, *, source_identity: str,
    source_run_id: str, source_run_binding_sha256: str,
    run_bound_environment_identity: str,
) -> tuple[LkgWindowHealthObservation, bytes]:
    """Validate retained health evidence; return a detached result and preimage.

    Pure, O(n log n) time and O(n) space for n retained document bytes.
    Raises ContractViolation on mismatch.
    Source identity is aggregate metadata, not an extra source-document field.
    Raw container status/health strings and sub-digest inputs are NOT retained;
    this validates their recorded evidence, not the omitted observations.
    """

    if type(result) is not LkgWindowHealthObservation:
        raise ContractViolation("health result type differs")
    if source_identity != LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY:
        raise ContractViolation("health source identity differs")
    doc = _document_fields(result.document, {
        "observation_schema_version": int, "source_run_id": str,
        "source_run_binding_sha256": str, "run_bound_environment_identity": str,
        "observed_environment_identity": str, "environment_identity_matches": bool,
        "observed_stable_environment_document": dict, "expected_entity_count": int,
        "container_health": dict, "milvus_healthz": bool,
        "collection_readiness": dict, "index_readiness": dict,
        "observed_at_utc": str, "reason_codes": list,
    })
    if doc["observation_schema_version"] != LKG_HEALTH_OBSERVATION_SCHEMA_VERSION:
        raise ContractViolation("health schema differs")
    if (
        type(run_bound_environment_identity) is not str
        or _IDENTITY_RE.fullmatch(run_bound_environment_identity) is None
        or doc["run_bound_environment_identity"] != run_bound_environment_identity
    ):
        raise ContractViolation("health run-bound environment identity differs")
    validate_rfc3339_utc(doc["observed_at_utc"], field="observed_at_utc")
    stable = _document_fields(doc["observed_stable_environment_document"], {
        "schema_version": str, "endpoint": dict, "containers": list,
        "data_plane": list, "expected_entity_count": int, "metric": str,
        "dimensions": int,
    })
    if stable["schema_version"] != LKG_ENVIRONMENT_IDENTITY_SCHEMA_VERSION:
        raise ContractViolation("stable environment schema differs")
    _exact_int(stable["expected_entity_count"], field="expected_entity_count", minimum=1)
    _exact_int(stable["dimensions"], field="dimensions", minimum=1)
    _text(stable["metric"], field="metric")
    if doc["expected_entity_count"] != stable["expected_entity_count"]:
        raise ContractViolation("health expected entity count differs")
    endpoint = _document_fields(stable["endpoint"], {
        "scheme": str, "host": str, "port": int, "transport_security": str,
    })
    if (
        endpoint["scheme"] not in {"http", "https"} or not endpoint["host"]
        or not 0 <= endpoint["port"] <= 65535
        or endpoint["transport_security"] != (
            "TLS" if endpoint["scheme"] == "https" else "PLAINTEXT"
        )
    ):
        raise ContractViolation("health endpoint is invalid")
    for name, roles in (("container_health", _CONTAINER_ROLES),
                        ("collection_readiness", _DATA_PLANE_ROLES),
                        ("index_readiness", _DATA_PLANE_ROLES)):
        _document_fields(doc[name], dict.fromkeys(roles, bool))
    if len(stable["containers"]) != 3 or len(stable["data_plane"]) != 2:
        raise ContractViolation("health environment population differs")
    for role, entry in zip(_CONTAINER_ROLES, stable["containers"], strict=True):
        _document_fields(entry, {
            "role": str, "container_name": str, "container_id": str,
            "image_id": str, "repository_digests": list, "restart_count": int,
            "oom_killed": bool, "started_at": str,
        })
        _text(entry["container_name"], field="container_name")
        repos = entry["repository_digests"]
        if (entry["role"] != role or not entry["image_id"]
                or any(type(value) is not str for value in repos)
                or repos != sorted(set(repos))
                or (entry["oom_killed"] and doc["container_health"][role])):
            raise ContractViolation("health container evidence is inconsistent")
    for entry in stable["data_plane"]:
        _document_fields(entry, {
            "collection_name": str, "collection_schema_sha256": str,
            "index_identity_sha256": str, "index_type": str,
            "index_parameters": list, "metric": str,
            "dimensions": (int, type(None)), "entity_count": int,
        })
        _text(entry["collection_name"], field="collection_name")
        _text(entry["index_type"], field="index_type")
        for name in ("collection_schema_sha256", "index_identity_sha256"):
            _sha256_hex(entry[name], field=name)
        if entry["metric"] != stable["metric"]:
            raise ContractViolation("health collection metric differs")
        for parameter in entry["index_parameters"]:
            _document_fields(parameter, {"name": str, "value": str})
        names = [item["name"] for item in entry["index_parameters"]]
        if names != sorted(names):
            raise ContractViolation("health index parameters are not canonical")
    identity = lkg_environment_identity(stable)
    if (doc["observed_environment_identity"] != identity
            or doc["environment_identity_matches"] != (identity == run_bound_environment_identity)):
        raise ContractViolation("health observed environment identity differs")
    reasons = doc["reason_codes"]
    predicates = {
        "ENVIRONMENT_IDENTITY_MISMATCH": not doc["environment_identity_matches"],
        "MILVUS_HEALTHZ_FAILED": not doc["milvus_healthz"],
        "COLLECTION_NOT_LOADED": not all(doc["collection_readiness"].values()),
        "INDEX_NOT_READY": not all(doc["index_readiness"].values()),
        "ENTITY_COUNT_MISMATCH": any(
            entry["entity_count"] != doc["expected_entity_count"] for entry in stable["data_plane"]
        ),
        "CONTAINER_OOM_KILLED": any(entry["oom_killed"] for entry in stable["containers"]),
    }
    non_oom_codes = {"CONTAINER_NOT_RUNNING", "CONTAINER_UNHEALTHY"}
    raw = _source_result_bytes(
        result, domain=LKG_HEALTH_OBSERVATION_DOMAIN, verdict=result.passed,
        allowed_reasons=set(predicates) | non_oom_codes,
        source_run_id=source_run_id, source_run_binding_sha256=source_run_binding_sha256,
    )
    for code, failed in predicates.items():
        _require_recorded_predicate(reasons, code, failed)
    non_oom_reason_present = bool(set(reasons) & non_oom_codes)
    # A failed non-OOM container requires a status/health cause, but the
    # omitted raw status cannot distinguish which. OOM containers may ALSO
    # have either cause, so the converse cannot require a non-OOM container.
    if not non_oom_reason_present and any(
        not entry["oom_killed"] and not doc["container_health"][entry["role"]]
        for entry in stable["containers"]
    ):
        raise ContractViolation("health non-OOM container lacks a recorded failure cause")
    if non_oom_reason_present and all(doc["container_health"].values()):
        raise ContractViolation("health container verdict disagrees with recorded reasons")
    return LkgWindowHealthObservation(json.loads(raw), result.digest, result.passed, tuple(reasons)), raw


def validate_lkg_window_rollback_readiness(
    result: LkgWindowRollbackReadiness, *, source_identity: str,
    source_run_id: str, source_run_binding_sha256: str,
) -> tuple[LkgWindowRollbackReadiness, bytes]:
    """Validate exact retained rollback evidence, without re-observation.

    O(n log n) time and O(n) space for n retained document bytes;
    ContractViolation refuses inconsistent input.
    Expected serving/baseline comparison operands are absent: their mismatch
    reasons remain recorded observer evidence, not independently replayed facts.
    """

    if type(result) is not LkgWindowRollbackReadiness:
        raise ContractViolation("rollback result type differs")
    if source_identity != LKG_ROLLBACK_READINESS_SOURCE_IDENTITY:
        raise ContractViolation("rollback source identity differs")
    route_types = {
        "route_state_state": str, "route_state_metric": str,
        "route_state_threshold_stratum": str, "route_state_last_known_good_ef": int,
        "route_state_configuration_identity": str, "route_state_data_identity": str,
        "route_state_flat_binding_id": str, "route_state_hnsw_binding_id": str,
        "route_state_grant_id": str, "route_state_plan_sha256": str,
        "route_state_changed_at_utc": str, "route_state_reason_code": str,
    }
    doc = _document_fields(result.document, {
        "rollback_schema_version": int, "verification_mode": str,
        "source_run_id": str, "source_run_binding_sha256": str,
        "baseline_search_configuration_document": (dict, type(None)),
        "baseline_search_configuration_sha256": (str, type(None)),
        "serving_configuration_identity": str, "verified_latest_lkg_present": bool,
        "route_state_present": bool,
        **{key: (kind, type(None)) for key, kind in route_types.items()},
        "restoration_target_digest": (str, type(None)),
        "verified_at_utc": str, "reason_codes": list,
    })
    if (doc["rollback_schema_version"] != LKG_ROLLBACK_READINESS_SCHEMA_VERSION
            or doc["verification_mode"] != LKG_ROLLBACK_VERIFICATION_MODE
            or doc["verified_latest_lkg_present"] is not False):
        raise ContractViolation("rollback bootstrap identity differs")
    _text(doc["serving_configuration_identity"], field="serving_configuration_identity")
    validate_rfc3339_utc(doc["verified_at_utc"], field="verified_at_utc")
    baseline = doc["baseline_search_configuration_document"]
    digest = None
    if baseline is not None:
        configuration = search_configuration_from_document(baseline)
        if canonical_json_bytes(search_configuration_document(configuration)) != canonical_json_bytes(baseline):
            raise ContractViolation("rollback configuration is not canonical")
        digest = search_configuration_sha256(configuration)
    if (doc["baseline_search_configuration_sha256"] != digest
            or doc["restoration_target_digest"] != digest):
        raise ContractViolation("rollback baseline/restoration digest differs")
    if not doc["route_state_present"]:
        if any(doc[key] is not None for key in route_types):
            raise ContractViolation("absent rollback route carries fields")
    else:
        # These are ADR-020's retained public provenance fields, not a copy of
        # the route store's private serializer or a new route-state digest.
        for key, kind in route_types.items():
            if key not in {"route_state_grant_id", "route_state_plan_sha256"}:
                if type(doc[key]) is not kind:
                    raise ContractViolation("present rollback route lacks fields")
        if doc["route_state_state"] not in {"ACTIVATING", "LKG_ONLY"}:
            raise ContractViolation("rollback route state differs")
        if (doc["route_state_metric"] not in {metric.value for metric in Metric}
                or doc["route_state_threshold_stratum"] not in THRESHOLD_LABELS
                or doc["route_state_last_known_good_ef"] not in tuple(ef for ef in HNSW_EF_SWEEP if ef != 100)):
            raise ContractViolation("rollback retained route configuration is invalid")
        for key in ("route_state_configuration_identity", "route_state_data_identity",
                    "route_state_flat_binding_id", "route_state_hnsw_binding_id"):
            value = doc[key]
            if (not value or value.strip() != value or unicodedata.normalize("NFC", value) != value
                    or any(ord(char) < 0x20 or ord(char) == 0x7F for char in value)):
                raise ContractViolation("rollback retained route identity is not canonical")
        validate_rfc3339_utc(doc["route_state_changed_at_utc"], field="route_state_changed_at_utc")
        if re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", doc["route_state_reason_code"]) is None:
            raise ContractViolation("rollback retained route reason is invalid")
        activating = doc["route_state_state"] == "ACTIVATING"
        if activating:
            _text(doc["route_state_grant_id"], field="route_state_grant_id")
            _sha256_hex(doc["route_state_plan_sha256"], field="route_state_plan_sha256")
        elif doc["route_state_grant_id"] is not None or doc["route_state_plan_sha256"] is not None:
            raise ContractViolation("LKG_ONLY route carries activation fields")
    reasons = doc["reason_codes"]
    predicates = {
        "BASELINE_CONFIGURATION_UNRECONSTRUCTABLE": baseline is None,
        "RESTORATION_TARGET_UNRESOLVED": digest is None,
        "CANDIDATE_ROUTE_ACTIVE": doc["route_state_state"] == "ACTIVATING",
        "BOOTSTRAP_LKG_ROUTE_MARKER_PRESENT": doc["route_state_state"] == "LKG_ONLY",
    }
    raw = _source_result_bytes(
        result, domain=LKG_ROLLBACK_READINESS_DOMAIN, verdict=result.ready,
        allowed_reasons=set(predicates) | {
            "BASELINE_CONFIGURATION_DIGEST_MISMATCH", "SERVING_CONFIGURATION_IDENTITY_MISMATCH"
        }, source_run_id=source_run_id, source_run_binding_sha256=source_run_binding_sha256,
    )
    for code, failed in predicates.items():
        _require_recorded_predicate(reasons, code, failed)
    if baseline is None and "BASELINE_CONFIGURATION_DIGEST_MISMATCH" in reasons:
        raise ContractViolation("unreconstructed baseline cannot have a digest mismatch")
    return LkgWindowRollbackReadiness(json.loads(raw), result.digest, result.ready, tuple(reasons)), raw


# -- ADR-020 section 7 stable environment normalization ----------------


def _mapping(value: object, *, code: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _error(code)
    return value


def _normalized_endpoint(uri: str) -> dict[str, object]:
    from urllib.parse import urlsplit

    parts = urlsplit(_text(uri, field="milvus_uri"))
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise _error("LKG_READINESS_ENDPOINT_INVALID")
    port = parts.port if parts.port is not None else (443 if parts.scheme == "https" else 80)
    return {
        "scheme": parts.scheme,
        "host": parts.hostname,
        "port": int(port),
        "transport_security": "TLS" if parts.scheme == "https" else "PLAINTEXT",
    }


def _normalize_container(
    role: str, name: str, container: Mapping[str, object], image: Mapping[str, object]
) -> tuple[dict[str, object], bool]:
    """ADR-020 section 7 stable container facts plus its readiness bit."""

    code = "LKG_READINESS_CONTAINER_INVALID"
    state = _mapping(container.get("State"), code=code)
    health = state.get("Health")
    health_status = None
    if health is not None:
        health_status = _mapping(health, code=code).get("Status")
    image_id = container.get("Image")
    restart_count = container.get("RestartCount")
    oom_killed = state.get("OOMKilled")
    started_at = state.get("StartedAt")
    container_id = container.get("Id")
    if (
        type(image_id) is not str
        or type(restart_count) is not int
        or type(oom_killed) is not bool
        or type(started_at) is not str
        or type(container_id) is not str
    ):
        raise _error(code)
    repo_digests = image.get("RepoDigests")
    if repo_digests is None:
        repo_digests = []
    if type(repo_digests) is not list or any(
        type(item) is not str for item in repo_digests
    ):
        raise _error(code)
    ready = (
        state.get("Status") == "running"
        and oom_killed is False
        and health_status in {None, "healthy"}
    )
    return (
        {
            "role": role,
            "container_name": name,
            "container_id": container_id,
            "image_id": image_id,
            "repository_digests": sorted(set(repo_digests)),
            "restart_count": restart_count,
            "oom_killed": oom_killed,
            "started_at": started_at,
        },
        bool(ready),
    )


def _normalize_fields(description: Mapping[str, object]) -> list[dict[str, object]]:
    code = "LKG_READINESS_COLLECTION_METADATA_INVALID"
    raw = description.get("fields")
    if not isinstance(raw, (list, tuple)):
        raise _error(code)
    fields: list[dict[str, object]] = []
    for item in raw:
        entry = _mapping(item, code=code)
        params = entry.get("params")
        dimension = None
        if isinstance(params, Mapping) and type(params.get("dim")) is int:
            dimension = params["dim"]
        fields.append(
            {
                "name": str(entry.get("name", "")),
                "data_type": str(entry.get("data_type", entry.get("type", ""))),
                "is_primary": bool(entry.get("is_primary", False)),
                "dimension": dimension,
            }
        )
    fields.sort(key=lambda item: item["name"])
    return fields


def _normalize_index_parameters(index: Mapping[str, object]) -> list[dict[str, object]]:
    reserved = {
        "index_name", "index_type", "metric_type", "state",
        "pending_index_rows", "indexed_rows", "total_rows", "field_name",
    }
    parameters = [
        {"name": str(key), "value": str(value)}
        for key, value in index.items()
        if str(key) not in reserved
    ]
    parameters.sort(key=lambda item: item["name"])
    return parameters


def lkg_collection_schema_document(
    *, collection_name: str, database_name: str, fields: Sequence[Mapping[str, object]]
) -> dict[str, object]:
    """Amendment (ADR-020a) canonical LKG collection-schema payload.

    Exactly four fields. Carries no entity count, no load or index
    readiness, no health state, and no timestamp -- those are transient
    and are bound elsewhere.
    """

    if not isinstance(fields, (list, tuple)):
        raise ContractViolation("fields must be a list or tuple")
    normalized: list[dict[str, object]] = []
    for entry in fields:
        if not isinstance(entry, Mapping):
            raise ContractViolation("each field must be a mapping")
        normalized.append(dict(entry))
    return {
        "schema_version": LKG_COLLECTION_SCHEMA_SCHEMA_VERSION,
        "collection_name": _text(collection_name, field="collection_name"),
        "database_name": _text(database_name, field="database_name"),
        "fields": normalized,
    }


def lkg_collection_schema_sha256(document: Mapping[str, object]) -> str:
    """``sha256(b"vdbench.lkg-collection-schema.v1\\0" + canonical(doc))``."""

    if not isinstance(document, Mapping) or set(document) != {
        "schema_version", "collection_name", "database_name", "fields"
    }:
        raise ContractViolation(
            "collection-schema document must carry exactly the four ADR-020a fields"
        )
    if document["schema_version"] != LKG_COLLECTION_SCHEMA_SCHEMA_VERSION:
        raise ContractViolation(
            f"schema_version must equal {LKG_COLLECTION_SCHEMA_SCHEMA_VERSION!r}"
        )
    return hashlib.sha256(
        LKG_COLLECTION_SCHEMA_DOMAIN + canonical_json_bytes(dict(document))
    ).hexdigest()


def lkg_index_identity_document(
    *,
    collection_name: str,
    database_name: str,
    collection_schema_sha256: str,
    index_name: str,
    index_type: str,
    index_metric: str,
    index_parameters: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Amendment (ADR-020a) canonical LKG index-identity payload.

    ``collection_schema_sha256`` MUST be the LKG collection-schema digest
    defined above -- never an EXP-012 Gate-C digest, never an unrelated
    caller-supplied hash. Carries no transient readiness state.
    """

    if not isinstance(index_parameters, (list, tuple)):
        raise ContractViolation("index_parameters must be a list or tuple")
    parameters: list[dict[str, object]] = []
    for entry in index_parameters:
        if not isinstance(entry, Mapping):
            raise ContractViolation("each index parameter must be a mapping")
        parameters.append(dict(entry))
    return {
        "schema_version": LKG_INDEX_IDENTITY_SCHEMA_VERSION,
        "collection_name": _text(collection_name, field="collection_name"),
        "database_name": _text(database_name, field="database_name"),
        "collection_schema_sha256": _sha256_hex(
            collection_schema_sha256, field="collection_schema_sha256"
        ),
        "index_name": _text(index_name, field="index_name"),
        "index_type": _text(index_type, field="index_type"),
        "index_metric": _text(index_metric, field="index_metric"),
        "index_parameters": parameters,
    }


def lkg_index_identity_sha256(document: Mapping[str, object]) -> str:
    """``sha256(b"vdbench.lkg-index-identity.v1\\0" + canonical(doc))``."""

    expected = {
        "schema_version", "collection_name", "database_name",
        "collection_schema_sha256", "index_name", "index_type",
        "index_metric", "index_parameters",
    }
    if not isinstance(document, Mapping) or set(document) != expected:
        raise ContractViolation(
            "index-identity document must carry exactly the eight ADR-020a fields"
        )
    if document["schema_version"] != LKG_INDEX_IDENTITY_SCHEMA_VERSION:
        raise ContractViolation(
            f"schema_version must equal {LKG_INDEX_IDENTITY_SCHEMA_VERSION!r}"
        )
    _sha256_hex(document["collection_schema_sha256"], field="collection_schema_sha256")
    return hashlib.sha256(
        LKG_INDEX_IDENTITY_DOMAIN + canonical_json_bytes(dict(document))
    ).hexdigest()


def _normalize_data_plane(
    collection_name: str,
    spec: LkgEnvironmentObservationSpec,
    reader: LkgMetadataReader,
) -> tuple[dict[str, object], bool, bool, list[str]]:
    """ADR-020 section 7 stable data-plane facts plus readiness bits."""

    code = "LKG_READINESS_COLLECTION_METADATA_UNAVAILABLE"
    try:
        description = reader.describe_collection(collection_name=collection_name)
        index = reader.describe_index(
            collection_name=collection_name, index_name=spec.index_name
        )
        stats = reader.get_collection_stats(collection_name=collection_name)
        load = reader.get_load_state(collection_name=collection_name)
    except Exception as exc:  # noqa: BLE001 - external boundary, fail closed
        raise _error(code) from exc

    invalid = "LKG_READINESS_COLLECTION_METADATA_INVALID"
    description = _mapping(description, code=invalid)
    index = _mapping(index, code=invalid)
    stats = _mapping(stats, code=invalid)
    load = _mapping(load, code=invalid)

    fields = _normalize_fields(description)
    dimension = next(
        (item["dimension"] for item in fields if item["dimension"] is not None), None
    )
    entity_count = stats.get("row_count")
    if type(entity_count) is not int or isinstance(entity_count, bool):
        raise _error(invalid)

    reasons: list[str] = []
    load_ready = str(load.get("state")) == "Loaded"
    if not load_ready:
        reasons.append("COLLECTION_NOT_LOADED")
    index_ready = (
        index.get("state") == "Finished"
        and index.get("pending_index_rows") == 0
        and index.get("indexed_rows") == spec.expected_entity_count
    )
    if not index_ready:
        reasons.append("INDEX_NOT_READY")
    if entity_count != spec.expected_entity_count:
        reasons.append("ENTITY_COUNT_MISMATCH")

    # Amendment (ADR-020a): the two internal facts sets -- database name,
    # normalized fields, index name and index metric -- are bound
    # TRANSITIVELY through the LKG sub-digests and are deliberately NOT
    # elevated into the top-level section 7 entry.
    index_parameters = _normalize_index_parameters(index)
    schema_document = lkg_collection_schema_document(
        collection_name=collection_name,
        database_name=spec.database_name,
        fields=fields,
    )
    collection_schema_sha256 = lkg_collection_schema_sha256(schema_document)
    index_document = lkg_index_identity_document(
        collection_name=collection_name,
        database_name=spec.database_name,
        collection_schema_sha256=collection_schema_sha256,
        index_name=str(index.get("index_name", spec.index_name)),
        index_type=str(index.get("index_type", "")),
        index_metric=str(index.get("metric_type", "")),
        index_parameters=index_parameters,
    )
    index_identity_sha256 = lkg_index_identity_sha256(index_document)

    # Exactly the eight governed fields ADR-020 section 7 requires. The
    # top-level redundancy (index_type, index_parameters, metric,
    # dimensions, entity_count) is deliberate and is not optimised away
    # merely because a sub-digest already binds some of it.
    document = {
        "collection_name": collection_name,
        "collection_schema_sha256": collection_schema_sha256,
        "index_identity_sha256": index_identity_sha256,
        "index_type": str(index.get("index_type", "")),
        "index_parameters": index_parameters,
        "metric": spec.metric,
        "dimensions": dimension,
        "entity_count": entity_count,
    }
    return document, bool(load_ready), bool(index_ready), reasons


def build_lkg_stable_environment_document(
    *,
    spec: LkgEnvironmentObservationSpec,
    endpoint: Mapping[str, object],
    containers: Sequence[Mapping[str, object]],
    data_plane: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """ADR-020 section 7's stable, identity-bearing environment document.

    Deliberately excludes every transient readiness predicate and the
    observation timestamp.
    """

    return {
        "schema_version": LKG_ENVIRONMENT_IDENTITY_SCHEMA_VERSION,
        "endpoint": dict(endpoint),
        "containers": [dict(item) for item in containers],
        "data_plane": [dict(item) for item in data_plane],
        "expected_entity_count": spec.expected_entity_count,
        "metric": spec.metric,
        "dimensions": spec.dimensions,
    }


def lkg_environment_identity(stable_environment_document: Mapping[str, object]) -> str:
    """ADR-020 section 6: ``lkg-env-identity-v1:sha256:<64 hex>``."""

    digest = hashlib.sha256(
        LKG_ENVIRONMENT_IDENTITY_DOMAIN
        + canonical_json_bytes(dict(stable_environment_document))
    ).hexdigest()
    return f"{LKG_ENVIRONMENT_IDENTITY_PREFIX}:sha256:{digest}"


# -- health observation (ADR-020 sections 10-12) -----------------------


def observe_lkg_window_health(
    *,
    spec: LkgEnvironmentObservationSpec,
    run_bound_environment_identity: str,
    source_run_id: str,
    source_run_binding_sha256: str,
    metadata_reader: LkgMetadataReader,
    container_inspector: Callable[[str], object],
    image_inspector: Callable[[str], object],
    healthz_probe: Callable[[], bool],
    observed_at_utc: str,
) -> LkgWindowHealthObservation:
    """Perform ONE metadata-only health observation for one window.

    Returns an observed FAIL as real evidence. Raises
    ``LkgWindowReadinessObservationError`` only when no trustworthy
    observation could be made at all (ADR-020 section 26).
    """

    if not isinstance(spec, LkgEnvironmentObservationSpec):
        raise ContractViolation("spec must be an LkgEnvironmentObservationSpec")
    if _IDENTITY_RE.fullmatch(run_bound_environment_identity) is None:
        raise ContractViolation(
            "run_bound_environment_identity must be lkg-env-identity-v1:sha256:<64 hex>"
        )
    _text(source_run_id, field="source_run_id")
    _sha256_hex(source_run_binding_sha256, field="source_run_binding_sha256")
    _rfc3339_utc(observed_at_utc, field="observed_at_utc")

    reasons: list[str] = []

    containers: list[dict[str, object]] = []
    container_health: dict[str, bool] = {}
    for role, name in zip(
        _CONTAINER_ROLES,
        (spec.etcd_container, spec.minio_container, spec.milvus_container),
        strict=True,
    ):
        try:
            raw = container_inspector(name)
        except Exception as exc:  # noqa: BLE001 - external boundary, fail closed
            raise _error("LKG_READINESS_CONTAINER_UNAVAILABLE") from exc
        container = _mapping(raw, code="LKG_READINESS_CONTAINER_INVALID")
        image_id = container.get("Image")
        if type(image_id) is not str or not image_id:
            raise _error("LKG_READINESS_CONTAINER_INVALID")
        try:
            raw_image = image_inspector(image_id)
        except Exception as exc:  # noqa: BLE001 - external boundary, fail closed
            raise _error("LKG_READINESS_IMAGE_UNAVAILABLE") from exc
        image = _mapping(raw_image, code="LKG_READINESS_IMAGE_INVALID")
        normalized, ready = _normalize_container(role, name, container, image)
        containers.append(normalized)
        container_health[role] = ready
        if not ready:
            state = _mapping(container.get("State"), code="LKG_READINESS_CONTAINER_INVALID")
            if state.get("Status") != "running":
                reasons.append("CONTAINER_NOT_RUNNING")
            if state.get("OOMKilled") is True:
                reasons.append("CONTAINER_OOM_KILLED")
            health = state.get("Health")
            if isinstance(health, Mapping) and health.get("Status") not in {None, "healthy"}:
                reasons.append("CONTAINER_UNHEALTHY")

    try:
        healthz = healthz_probe()
    except Exception as exc:  # noqa: BLE001 - external boundary, fail closed
        raise _error("LKG_READINESS_HEALTHZ_UNAVAILABLE") from exc
    if type(healthz) is not bool:
        raise _error("LKG_READINESS_HEALTHZ_INVALID")
    if not healthz:
        reasons.append("MILVUS_HEALTHZ_FAILED")

    data_plane: list[dict[str, object]] = []
    collection_readiness: dict[str, bool] = {}
    index_readiness: dict[str, bool] = {}
    for role, collection_name in zip(
        _DATA_PLANE_ROLES,
        (spec.flat_collection_name, spec.hnsw_collection_name),
        strict=True,
    ):
        document, load_ready, index_ready, role_reasons = _normalize_data_plane(
            collection_name, spec, metadata_reader
        )
        data_plane.append(document)
        collection_readiness[role] = load_ready
        index_readiness[role] = index_ready
        reasons.extend(role_reasons)

    stable_document = build_lkg_stable_environment_document(
        spec=spec,
        endpoint=_normalized_endpoint(spec.milvus_uri),
        containers=containers,
        data_plane=data_plane,
    )
    observed_identity = lkg_environment_identity(stable_document)
    identity_matches = observed_identity == run_bound_environment_identity
    if not identity_matches:
        reasons.append("ENVIRONMENT_IDENTITY_MISMATCH")

    reason_codes = _sorted_reason_codes(reasons)
    document = {
        "observation_schema_version": LKG_HEALTH_OBSERVATION_SCHEMA_VERSION,
        "source_run_id": source_run_id,
        "source_run_binding_sha256": source_run_binding_sha256,
        "run_bound_environment_identity": run_bound_environment_identity,
        "observed_environment_identity": observed_identity,
        "environment_identity_matches": identity_matches,
        "observed_stable_environment_document": stable_document,
        "expected_entity_count": spec.expected_entity_count,
        "container_health": dict(sorted(container_health.items())),
        "milvus_healthz": healthz,
        "collection_readiness": dict(sorted(collection_readiness.items())),
        "index_readiness": dict(sorted(index_readiness.items())),
        "observed_at_utc": observed_at_utc,
        "reason_codes": list(reason_codes),
    }
    digest = hashlib.sha256(
        LKG_HEALTH_OBSERVATION_DOMAIN + canonical_json_bytes(document)
    ).hexdigest()
    return LkgWindowHealthObservation(
        document=document,
        digest=digest,
        passed=not reason_codes,
        reason_codes=reason_codes,
    )


# -- bootstrap rollback readiness (ADR-020 sections 27-36) -------------


def verify_lkg_window_rollback_readiness(
    *,
    source_run_id: str,
    source_run_binding_sha256: str,
    baseline_search_configuration: SearchConfiguration,
    expected_baseline_search_configuration_sha256: str,
    serving_configuration_identity: str,
    expected_serving_configuration_identity: str,
    verified_latest_lkg_present: bool,
    route_state_record: object | None,
    verified_at_utc: str,
) -> LkgWindowRollbackReadiness:
    """First-LKG bootstrap baseline-restorability verification.

    Zero actuation: this reconstructs a configuration, compares canonical
    digests, and reads already-loaded durable state. It performs no
    rollback, failback, route change, grant operation, canary, or index
    mutation, and ``ready`` is never evidence that any of those occurred
    (ADR-020 sections 27 and 45).

    ``route_state_record`` is the value the caller already obtained from
    the canonical route-state store's no-argument read: ``None`` for
    absence, or a validated ``RouteStateRecord``. This function never
    reads it itself, so an unreadable marker is the caller's provider
    inability, never an observed failure.
    """

    from .canary_route_state import RouteState, RouteStateRecord

    _text(source_run_id, field="source_run_id")
    _sha256_hex(source_run_binding_sha256, field="source_run_binding_sha256")
    _sha256_hex(
        expected_baseline_search_configuration_sha256,
        field="expected_baseline_search_configuration_sha256",
    )
    _text(serving_configuration_identity, field="serving_configuration_identity")
    _text(
        expected_serving_configuration_identity,
        field="expected_serving_configuration_identity",
    )
    if type(verified_latest_lkg_present) is not bool:
        raise ContractViolation("verified_latest_lkg_present must be a bool")
    _rfc3339_utc(verified_at_utc, field="verified_at_utc")

    if verified_latest_lkg_present:
        # ADR-020 section 4: this provider path is first-LKG only.
        raise _error("STEADY_STATE_SEMANTICS_NOT_AUTHORIZED")

    reasons: list[str] = []

    try:
        baseline_document = search_configuration_document(baseline_search_configuration)
        baseline_digest = search_configuration_sha256(baseline_search_configuration)
    except Exception:  # noqa: BLE001 - reconstruction failure is an observed FAIL
        baseline_document = None
        baseline_digest = None
        reasons.append("BASELINE_CONFIGURATION_UNRECONSTRUCTABLE")

    if baseline_digest is not None and (
        baseline_digest != expected_baseline_search_configuration_sha256
    ):
        reasons.append("BASELINE_CONFIGURATION_DIGEST_MISMATCH")

    if serving_configuration_identity != expected_serving_configuration_identity:
        reasons.append("SERVING_CONFIGURATION_IDENTITY_MISMATCH")

    route_present = route_state_record is not None
    route_fields: dict[str, object] = {
        "route_state_state": None,
        "route_state_metric": None,
        "route_state_threshold_stratum": None,
        "route_state_last_known_good_ef": None,
        "route_state_configuration_identity": None,
        "route_state_data_identity": None,
        "route_state_flat_binding_id": None,
        "route_state_hnsw_binding_id": None,
        "route_state_grant_id": None,
        "route_state_plan_sha256": None,
        "route_state_changed_at_utc": None,
        "route_state_reason_code": None,
    }
    if route_present:
        if not isinstance(route_state_record, RouteStateRecord):
            raise ContractViolation(
                "route_state_record must be a RouteStateRecord or None"
            )
        binding = route_state_record.binding
        route_fields = {
            "route_state_state": str(route_state_record.state.value),
            "route_state_metric": str(binding.metric.value),
            "route_state_threshold_stratum": binding.threshold_stratum,
            "route_state_last_known_good_ef": binding.last_known_good_ef,
            "route_state_configuration_identity": binding.configuration_identity,
            "route_state_data_identity": binding.data_identity,
            "route_state_flat_binding_id": binding.flat_binding_id,
            "route_state_hnsw_binding_id": binding.hnsw_binding_id,
            "route_state_grant_id": route_state_record.grant_id,
            "route_state_plan_sha256": route_state_record.plan_sha256,
            "route_state_changed_at_utc": route_state_record.changed_at_utc,
            "route_state_reason_code": route_state_record.reason_code,
        }
        # ADR-020 sections 29-30. Any marker fails first-LKG bootstrap:
        # matching context fields cannot substitute for verified Phase-3
        # authority, so they are recorded as provenance, never as a pass.
        if route_state_record.state is RouteState.ACTIVATING:
            reasons.append("CANDIDATE_ROUTE_ACTIVE")
        else:
            reasons.append("BOOTSTRAP_LKG_ROUTE_MARKER_PRESENT")

    restoration_target_digest = baseline_digest
    if restoration_target_digest is None:
        reasons.append("RESTORATION_TARGET_UNRESOLVED")

    reason_codes = _sorted_reason_codes(reasons)
    document = {
        "rollback_schema_version": LKG_ROLLBACK_READINESS_SCHEMA_VERSION,
        "verification_mode": LKG_ROLLBACK_VERIFICATION_MODE,
        "source_run_id": source_run_id,
        "source_run_binding_sha256": source_run_binding_sha256,
        "baseline_search_configuration_document": baseline_document,
        "baseline_search_configuration_sha256": baseline_digest,
        "serving_configuration_identity": serving_configuration_identity,
        "verified_latest_lkg_present": False,
        "route_state_present": route_present,
        **route_fields,
        "restoration_target_digest": restoration_target_digest,
        "verified_at_utc": verified_at_utc,
        "reason_codes": list(reason_codes),
    }
    digest = hashlib.sha256(
        LKG_ROLLBACK_READINESS_DOMAIN + canonical_json_bytes(document)
    ).hexdigest()
    return LkgWindowRollbackReadiness(
        document=document,
        digest=digest,
        ready=not reason_codes,
        reason_codes=reason_codes,
    )
