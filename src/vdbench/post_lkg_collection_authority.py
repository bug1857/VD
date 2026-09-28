"""Immutable post-LKG collection authority descended from verified D2.

Preparation performs read-only D2 and DATASET-001 verification and returns
canonical bytes.  Materialization is a separate, inert, exclusive-create
operation; neither operation contacts Milvus, Docker, policy, route state, or
actuation.
"""

from __future__ import annotations

import math
import os
import re
import stat
import unicodedata
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

from .canonical_serialization import (
    CANONICAL_JSON_SCHEMA_VERSION,
    CanonicalSerializationError,
    decode_strict_canonical_json,
    strict_canonical_digest,
    strict_canonical_json_bytes,
)
from .config import HNSW_EF_CONSTRUCTION, HNSW_M, INDEX_NAME, Metric
from .deployment_governance import (
    CANONICAL_ENV001_DEPLOYMENT_IDENTITY,
    DeploymentGovernanceScope,
    ensure_deployment_scope_directory,
)
from .drift import SENTINEL_EF
from .exp010_serving_configuration import (
    Exp010ServingConfiguration,
    derive_serving_configuration_identity,
)
from .exp010_v2_host import pin_dataset001_identity
from .lkg_phase3_persistence import (
    LkgPhase3AuthorityReferenceStore,
    LkgPhase3PersistenceError,
    VerifiedLatestLkgPhase3AuthorityReference,
)
from .shadow_window import TRACE_COUNT, TRACE_QUERY_COUNT, WINDOW_QUERY_COUNT

__all__ = [
    "POST_LKG_COLLECTION_AUTHORITY_DIGEST_DOMAIN",
    "POST_LKG_COLLECTION_AUTHORITY_SCHEMA_VERSION",
    "POST_LKG_COLLECTION_PREPARE_INPUT_SCHEMA_VERSION",
    "PostLkgCollectionAuthority",
    "PostLkgCollectionAuthorityError",
    "PostLkgCollectionPrepareInput",
    "VerifiedPostLkgCollectionAuthority",
    "derive_post_lkg_campaign_root",
    "derive_post_lkg_stream_id",
    "load_post_lkg_collection_prepare_input",
    "load_verified_post_lkg_collection_authority",
    "materialize_post_lkg_collection_authority",
    "prepare_post_lkg_collection_authority",
    "verify_post_lkg_collection_authority_d2",
]


POST_LKG_COLLECTION_AUTHORITY_SCHEMA_VERSION = (
    "vd-post-lkg-collection-authority-v1"
)
POST_LKG_COLLECTION_PREPARE_INPUT_SCHEMA_VERSION = (
    "vd-post-lkg-collection-prepare-input-v1"
)
POST_LKG_COLLECTION_AUTHORITY_DIGEST_DOMAIN = (
    b"VD::POST_LKG_COLLECTION_AUTHORITY::V1\x00"
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_REVISION_RE = re.compile(r"[0-9a-f]{40}\Z")
_CAMPAIGN_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}\Z")
_LOOPBACK_ADDRESSES = frozenset({"127.0.0.1", "::1", "localhost"})
_ISSUE_AUTHORITY = object()
_ISSUE_VERIFIED = object()

_AUTHORITY_FIELDS = frozenset(
    {
        "campaign",
        "canonical_lkg",
        "collection_execution_source_revision",
        "containers",
        "dataset",
        "detector",
        "endpoint",
        "milvus",
        "search_budget",
        "schema_version",
        "serialization_contract",
        "serving",
        "target",
        "window_plan",
        "workload_producer",
    }
)
_SECTION_FIELDS = {
    "campaign": frozenset({"campaign_id", "campaign_root", "stream_id"}),
    "canonical_lkg": frozenset(
        {
            "checkpoint_c_sha256",
            "d2_record_sha256",
            "d2_sequence",
            "deployment_identity",
            "deployment_namespace_sha256",
            "environment_identity",
            "evaluated_ef",
            "lkg_execution_source_revision",
            "source_run_binding_sha256",
            "source_run_id",
        }
    ),
    "containers": frozenset(
        {"etcd_container", "milvus_container", "minio_container"}
    ),
    "dataset": frozenset(
        {
            "base_vectors_sha256",
            "data_identity",
            "dataset001_dir",
            "generation_manifest_sha256",
            "version",
        }
    ),
    "detector": frozenset({"seed"}),
    "endpoint": frozenset({"host_address", "host_port"}),
    "milvus": frozenset(
        {
            "database_name",
            "flat_collection_name",
            "hnsw_collection_name",
            "index_name",
            "uri",
        }
    ),
    "search_budget": frozenset(
        {
            "flat_shadow_positions",
            "foreground_positions",
            "hnsw_sentinel_positions",
            "milvus_write_operations",
        }
    ),
    "serving": frozenset(
        {
            "configuration_identity",
            "consistency_level",
            "dimensions",
            "flat_binding_id",
            "hnsw_binding_id",
            "limit",
            "metric",
            "range_filter",
            "served_ef",
            "threshold_radius",
            "threshold_stratum",
        }
    ),
    "target": frozenset(
        {"expected_row_count", "hnsw_ef_construction", "hnsw_m"}
    ),
    "window_plan": frozenset(
        {
            "complete_windows",
            "current_window_sequences",
            "first_eligible_decision_sequence",
            "queries_per_window",
            "reference_window_sequence",
            "sentinel_ef",
            "served_ef",
            "target_source_records",
            "trace_count",
            "trace_size",
        }
    ),
    "workload_producer": frozenset({"external_workload_producer_identity"}),
}


class PostLkgCollectionAuthorityError(RuntimeError):
    """Fail-closed contract error carrying a stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(code if message is None else f"{code}: {message}")
        self.code = code


def _error(
    code: str, message: str | None = None
) -> PostLkgCollectionAuthorityError:
    return PostLkgCollectionAuthorityError(code, message)


def _text(
    value: object,
    *,
    field: str,
    maximum: int = 512,
    ascii_only: bool = False,
) -> str:
    if type(value) is not str or not 1 <= len(value) <= maximum:
        raise _error("POST_LKG_AUTHORITY_VALUE_INVALID", field)
    if (
        value != value.strip()
        or value != unicodedata.normalize("NFC", value)
        or any(not character.isprintable() for character in value)
        or (ascii_only and not value.isascii())
    ):
        raise _error("POST_LKG_AUTHORITY_VALUE_INVALID", field)
    return value


def _sha256(value: object, *, field: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise _error("POST_LKG_AUTHORITY_SHA256_INVALID", field)
    return value


def _revision(value: object, *, field: str) -> str:
    if type(value) is not str or _REVISION_RE.fullmatch(value) is None:
        raise _error("POST_LKG_AUTHORITY_REVISION_INVALID", field)
    return value


def _integer(
    value: object, *, field: str, minimum: int = 0, maximum: int | None = None
) -> int:
    if type(value) is not int or value < minimum or (
        maximum is not None and value > maximum
    ):
        raise _error("POST_LKG_AUTHORITY_INTEGER_INVALID", field)
    return value


def _real(value: object, *, field: str) -> float:
    if type(value) is not float or not math.isfinite(value):
        raise _error("POST_LKG_AUTHORITY_FLOAT_INVALID", field)
    return value


def _campaign_id(value: object) -> str:
    if type(value) is not str or _CAMPAIGN_RE.fullmatch(value) is None:
        raise _error("POST_LKG_CAMPAIGN_ID_INVALID")
    return value


def _require_exact_json_types(value: object) -> None:
    value_type = type(value)
    if value_type is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise _error("POST_LKG_AUTHORITY_TYPE_INVALID")
            _require_exact_json_types(item)
        return
    if value_type is list:
        for item in value:
            _require_exact_json_types(item)
        return
    if value_type is str or value_type is int:
        return
    if value_type is float and math.isfinite(value):
        return
    raise _error("POST_LKG_AUTHORITY_TYPE_INVALID")


def _section(document: dict[str, Any], name: str) -> dict[str, Any]:
    value = document.get(name)
    if type(value) is not dict or set(value) != _SECTION_FIELDS[name]:
        raise _error("POST_LKG_AUTHORITY_SCHEMA_INVALID", name)
    return value


def _absolute_canonical_path(value: object, *, field: str) -> str:
    text = _text(value, field=field, maximum=4096)
    if not os.path.isabs(text) or os.path.realpath(text) != text:
        raise _error("POST_LKG_AUTHORITY_PATH_INVALID", field)
    return text


@dataclass(frozen=True, slots=True)
class PostLkgCollectionPrepareInput:
    campaign_id: str
    collection_execution_source_revision: str
    consistency_level: str
    dataset001_dir: str
    detector_seed: int
    etcd_container: str
    expected_d2_record_sha256: str
    expected_d2_sequence: int
    expected_row_count: int
    external_workload_producer_identity: str
    flat_binding_id: str
    flat_collection_name: str
    hnsw_binding_id: str
    hnsw_collection_name: str
    host_address: str
    host_port: int
    limit: int
    milvus_container: str
    milvus_uri: str
    minio_container: str
    range_filter: float
    schema_version: str
    threshold_radius: float

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not str
            or self.schema_version
            != POST_LKG_COLLECTION_PREPARE_INPUT_SCHEMA_VERSION
        ):
            raise _error("POST_LKG_PREPARE_SCHEMA_INVALID")
        _campaign_id(self.campaign_id)
        _revision(
            self.collection_execution_source_revision,
            field="collection_execution_source_revision",
        )
        _text(self.consistency_level, field="consistency_level")
        _absolute_canonical_path(self.dataset001_dir, field="dataset001_dir")
        _integer(
            self.detector_seed,
            field="detector_seed",
            maximum=2**32 - 1,
        )
        for name in ("etcd_container", "milvus_container", "minio_container"):
            value = _text(getattr(self, name), field=name, maximum=255)
            if "/" in value:
                raise _error("POST_LKG_AUTHORITY_VALUE_INVALID", name)
        _sha256(self.expected_d2_record_sha256, field="expected_d2_record_sha256")
        _integer(self.expected_d2_sequence, field="expected_d2_sequence")
        _integer(self.expected_row_count, field="expected_row_count", minimum=1)
        _text(
            self.external_workload_producer_identity,
            field="external_workload_producer_identity",
            maximum=128,
        )
        for name in (
            "flat_binding_id",
            "flat_collection_name",
            "hnsw_binding_id",
            "hnsw_collection_name",
        ):
            _text(getattr(self, name), field=name, maximum=255)
        if self.flat_binding_id == self.hnsw_binding_id:
            raise _error("POST_LKG_BINDING_COLLISION")
        if self.flat_collection_name == self.hnsw_collection_name:
            raise _error("POST_LKG_COLLECTION_COLLISION")
        if (
            type(self.host_address) is not str
            or self.host_address not in _LOOPBACK_ADDRESSES
        ):
            raise _error("POST_LKG_HOST_ADDRESS_INVALID")
        _integer(self.host_port, field="host_port", minimum=1, maximum=65535)
        _integer(self.limit, field="limit", minimum=1)
        _text(self.milvus_uri, field="milvus_uri", maximum=2048)
        _real(self.range_filter, field="range_filter")
        _real(self.threshold_radius, field="threshold_radius")


@dataclass(frozen=True, slots=True, init=False)
class PostLkgCollectionAuthority:
    """Immutable prepared canonical authority; construction is module-owned."""

    _canonical_bytes: bytes
    authority_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("post-LKG authorities are issued by prepare or verified load")

    @classmethod
    def _from_canonical(
        cls, *, canonical_bytes: bytes, authority_sha256: str, token: object
    ) -> PostLkgCollectionAuthority:
        if token is not _ISSUE_AUTHORITY:
            raise TypeError("post-LKG authority construction token is invalid")
        value = object.__new__(cls)
        object.__setattr__(value, "_canonical_bytes", canonical_bytes)
        object.__setattr__(value, "authority_sha256", authority_sha256)
        return value

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    @property
    def document(self) -> dict[str, Any]:
        document = decode_strict_canonical_json(self._canonical_bytes)
        if type(document) is not dict:
            raise _error("POST_LKG_AUTHORITY_SCHEMA_INVALID")
        return document

    @property
    def campaign_id(self) -> str:
        return str(self.document["campaign"]["campaign_id"])


@dataclass(frozen=True, slots=True, init=False)
class VerifiedPostLkgCollectionAuthority:
    """Authority whose immutable file bytes and address were reverified."""

    _authority: PostLkgCollectionAuthority
    path: Path

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("verified post-LKG authorities are loader-issued")

    @classmethod
    def _from_verified(
        cls,
        *,
        authority: PostLkgCollectionAuthority,
        path: Path,
        token: object,
    ) -> VerifiedPostLkgCollectionAuthority:
        if token is not _ISSUE_VERIFIED:
            raise TypeError("verified post-LKG authority construction token is invalid")
        value = object.__new__(cls)
        object.__setattr__(value, "_authority", authority)
        object.__setattr__(value, "path", path)
        return value

    @property
    def authority(self) -> PostLkgCollectionAuthority:
        return self._authority

    @property
    def authority_sha256(self) -> str:
        return self._authority.authority_sha256

    @property
    def canonical_bytes(self) -> bytes:
        return self._authority.canonical_bytes

    @property
    def document(self) -> dict[str, Any]:
        return self._authority.document

    @property
    def campaign_id(self) -> str:
        return self._authority.campaign_id


def load_post_lkg_collection_prepare_input(
    path: str | os.PathLike[str],
) -> PostLkgCollectionPrepareInput:
    try:
        raw = Path(path).read_bytes()
        document = decode_strict_canonical_json(raw)
    except (OSError, CanonicalSerializationError) as exc:
        raise _error("POST_LKG_PREPARE_INPUT_UNREADABLE", str(exc)) from exc
    expected = {field.name for field in fields(PostLkgCollectionPrepareInput)}
    if type(document) is not dict or set(document) != expected:
        raise _error("POST_LKG_PREPARE_INPUT_SCHEMA_INVALID")
    try:
        return PostLkgCollectionPrepareInput(**document)
    except TypeError as exc:
        raise _error("POST_LKG_PREPARE_INPUT_SCHEMA_INVALID", str(exc)) from exc


def derive_post_lkg_campaign_root(
    *, governance_scope: DeploymentGovernanceScope, campaign_id: str
) -> Path:
    _require_scope(governance_scope)
    campaign = _campaign_id(campaign_id)
    return Path(governance_scope.campaign_root(campaign))


def derive_post_lkg_stream_id(*, d2_record_sha256: str, campaign_id: str) -> str:
    digest = _sha256(d2_record_sha256, field="d2_record_sha256")
    campaign = _campaign_id(campaign_id)
    return f"post-lkg-v1/{digest}/{campaign}"


def _require_scope(scope: object) -> DeploymentGovernanceScope:
    if type(scope) is not DeploymentGovernanceScope:
        raise _error("POST_LKG_GOVERNANCE_SCOPE_INVALID")
    if scope.deployment_identity != CANONICAL_ENV001_DEPLOYMENT_IDENTITY:
        raise _error("POST_LKG_DEPLOYMENT_IDENTITY_MISMATCH")
    return scope


def _load_verified_latest_d2(
    scope: DeploymentGovernanceScope,
) -> VerifiedLatestLkgPhase3AuthorityReference:
    path = Path(scope.lkg_authority_store_path)
    if not path.is_file() or path.is_symlink():
        raise _error("POST_LKG_D2_MISSING")
    try:
        with LkgPhase3AuthorityReferenceStore(str(path)) as store:
            latest = store.load_verified_latest()
    except (LkgPhase3PersistenceError, OSError) as exc:
        raise _error("POST_LKG_D2_UNVERIFIED", str(exc)) from exc
    if latest is None:
        raise _error("POST_LKG_D2_MISSING")
    return latest


def _validated_authority_document(
    value: object, *, scope: DeploymentGovernanceScope
) -> dict[str, Any]:
    _require_scope(scope)
    _require_exact_json_types(value)
    if type(value) is not dict or set(value) != _AUTHORITY_FIELDS:
        raise _error("POST_LKG_AUTHORITY_SCHEMA_INVALID")
    document = value
    sections = {name: _section(document, name) for name in _SECTION_FIELDS}
    campaign = sections["campaign"]
    canonical_lkg = sections["canonical_lkg"]
    containers = sections["containers"]
    dataset = sections["dataset"]
    detector = sections["detector"]
    endpoint = sections["endpoint"]
    milvus = sections["milvus"]
    search_budget = sections["search_budget"]
    serving = sections["serving"]
    target = sections["target"]
    window_plan = sections["window_plan"]
    producer = sections["workload_producer"]

    if document["schema_version"] != POST_LKG_COLLECTION_AUTHORITY_SCHEMA_VERSION:
        raise _error("POST_LKG_AUTHORITY_SCHEMA_INVALID", "schema_version")
    if document["serialization_contract"] != CANONICAL_JSON_SCHEMA_VERSION:
        raise _error("POST_LKG_AUTHORITY_SCHEMA_INVALID", "serialization_contract")
    campaign_id = _campaign_id(campaign["campaign_id"])
    expected_root = str(
        derive_post_lkg_campaign_root(
            governance_scope=scope, campaign_id=campaign_id
        )
    )
    if _absolute_canonical_path(campaign["campaign_root"], field="campaign_root") != expected_root:
        raise _error("POST_LKG_CAMPAIGN_ROOT_MISMATCH")
    d2_digest = _sha256(canonical_lkg["d2_record_sha256"], field="d2_record_sha256")
    if campaign["stream_id"] != derive_post_lkg_stream_id(
        d2_record_sha256=d2_digest, campaign_id=campaign_id
    ):
        raise _error("POST_LKG_STREAM_ID_MISMATCH")

    for name in (
        "checkpoint_c_sha256",
        "deployment_namespace_sha256",
        "source_run_binding_sha256",
    ):
        _sha256(canonical_lkg[name], field=name)
    _integer(canonical_lkg["d2_sequence"], field="d2_sequence")
    if canonical_lkg["deployment_identity"] != scope.deployment_identity:
        raise _error("POST_LKG_DEPLOYMENT_IDENTITY_MISMATCH")
    if canonical_lkg["deployment_namespace_sha256"] != scope.namespace_digest:
        raise _error("POST_LKG_NAMESPACE_DIGEST_MISMATCH")
    _text(canonical_lkg["environment_identity"], field="environment_identity")
    if _integer(canonical_lkg["evaluated_ef"], field="evaluated_ef", minimum=1) != 400:
        raise _error("POST_LKG_EVALUATED_EF_MISMATCH")
    _revision(
        canonical_lkg["lkg_execution_source_revision"],
        field="lkg_execution_source_revision",
    )
    _text(canonical_lkg["source_run_id"], field="source_run_id")
    _revision(
        document["collection_execution_source_revision"],
        field="collection_execution_source_revision",
    )

    for name in ("etcd_container", "milvus_container", "minio_container"):
        text = _text(containers[name], field=name, maximum=255)
        if "/" in text:
            raise _error("POST_LKG_AUTHORITY_VALUE_INVALID", name)
    _sha256(dataset["base_vectors_sha256"], field="base_vectors_sha256")
    _text(dataset["data_identity"], field="data_identity")
    _absolute_canonical_path(dataset["dataset001_dir"], field="dataset001_dir")
    _sha256(
        dataset["generation_manifest_sha256"], field="generation_manifest_sha256"
    )
    _text(dataset["version"], field="dataset.version")
    _integer(detector["seed"], field="seed", maximum=2**32 - 1)
    if endpoint["host_address"] not in _LOOPBACK_ADDRESSES:
        raise _error("POST_LKG_HOST_ADDRESS_INVALID")
    _integer(endpoint["host_port"], field="host_port", minimum=1, maximum=65535)
    if milvus["database_name"] != "default" or milvus["index_name"] != INDEX_NAME:
        raise _error("POST_LKG_DATABASE_OR_INDEX_INVALID")
    for name in ("flat_collection_name", "hnsw_collection_name"):
        _text(milvus[name], field=name, maximum=255)
    if milvus["flat_collection_name"] == milvus["hnsw_collection_name"]:
        raise _error("POST_LKG_COLLECTION_COLLISION")
    _text(milvus["uri"], field="milvus.uri", maximum=2048)

    expected_budget = {
        "flat_shadow_positions": 600,
        "foreground_positions": 600,
        "hnsw_sentinel_positions": 600,
        "milvus_write_operations": 0,
    }
    if search_budget != expected_budget:
        raise _error("POST_LKG_SEARCH_BUDGET_INVALID")
    for name in ("flat_binding_id", "hnsw_binding_id"):
        _text(serving[name], field=name, maximum=255)
    if serving["flat_binding_id"] == serving["hnsw_binding_id"]:
        raise _error("POST_LKG_BINDING_COLLISION")
    try:
        metric = Metric(_text(serving["metric"], field="metric"))
        configuration = Exp010ServingConfiguration(
            metric=metric,
            threshold_stratum=_text(
                serving["threshold_stratum"], field="threshold_stratum"
            ),
            threshold_radius=_real(
                serving["threshold_radius"], field="threshold_radius"
            ),
            range_filter=_real(serving["range_filter"], field="range_filter"),
            limit=_integer(serving["limit"], field="limit", minimum=1),
            served_ef=_integer(
                serving["served_ef"], field="served_ef", minimum=1
            ),
            dimensions=_integer(
                serving["dimensions"], field="dimensions", minimum=1
            ),
            consistency_level=_text(
                serving["consistency_level"], field="consistency_level"
            ),
        )
    except ValueError as exc:
        raise _error("POST_LKG_SERVING_CONFIGURATION_INVALID", str(exc)) from exc
    if serving["configuration_identity"] != derive_serving_configuration_identity(
        configuration
    ):
        raise _error("POST_LKG_CONFIGURATION_IDENTITY_MISMATCH")
    if target != {
        "expected_row_count": target["expected_row_count"],
        "hnsw_ef_construction": HNSW_EF_CONSTRUCTION,
        "hnsw_m": HNSW_M,
    }:
        raise _error("POST_LKG_TARGET_INVALID")
    _integer(target["expected_row_count"], field="expected_row_count", minimum=1)
    expected_window_plan = {
        "complete_windows": 3,
        "current_window_sequences": [1, 2],
        "first_eligible_decision_sequence": 2,
        "queries_per_window": WINDOW_QUERY_COUNT,
        "reference_window_sequence": 0,
        "sentinel_ef": SENTINEL_EF,
        "served_ef": 400,
        "target_source_records": 3 * WINDOW_QUERY_COUNT,
        "trace_count": TRACE_COUNT,
        "trace_size": TRACE_QUERY_COUNT,
    }
    if window_plan != expected_window_plan:
        raise _error("POST_LKG_WINDOW_PLAN_INVALID")
    _text(
        producer["external_workload_producer_identity"],
        field="external_workload_producer_identity",
        maximum=128,
    )
    try:
        strict_canonical_json_bytes(document)
    except CanonicalSerializationError as exc:
        raise _error("POST_LKG_AUTHORITY_NOT_CANONICAL", str(exc)) from exc
    return document


def _require_document_matches_d2(
    document: dict[str, Any],
    latest: VerifiedLatestLkgPhase3AuthorityReference,
    *,
    scope: DeploymentGovernanceScope,
) -> None:
    reference = latest.reference
    lkg = document["canonical_lkg"]
    expected = {
        "checkpoint_c_sha256": reference.canonical_evaluation_digest,
        "d2_record_sha256": reference.canonical_record_digest,
        "d2_sequence": reference.sequence_number,
        "deployment_identity": scope.deployment_identity,
        "deployment_namespace_sha256": scope.namespace_digest,
        "environment_identity": reference.environment_identity,
        "evaluated_ef": reference.evaluated_ef,
        "lkg_execution_source_revision": reference.source_revision,
        "source_run_binding_sha256": reference.source_run_binding_sha256,
        "source_run_id": reference.source_run_id,
    }
    if lkg != expected:
        raise _error("POST_LKG_D2_PROJECTION_MISMATCH")
    if document["milvus"]["hnsw_collection_name"] != reference.collection_name:
        raise _error("POST_LKG_D2_COLLECTION_MISMATCH")
    if document["serving"]["hnsw_binding_id"] != reference.index_identity:
        raise _error("POST_LKG_D2_INDEX_MISMATCH")
    if document["dataset"]["data_identity"] != reference.data_identity:
        raise _error("POST_LKG_D2_DATA_MISMATCH")
    if document["serving"]["metric"] != reference.metric:
        raise _error("POST_LKG_D2_METRIC_MISMATCH")
    if document["serving"]["threshold_stratum"] != reference.threshold_stratum:
        raise _error("POST_LKG_D2_STRATUM_MISMATCH")


def _issue_authority(
    document: dict[str, Any], *, scope: DeploymentGovernanceScope
) -> PostLkgCollectionAuthority:
    validated = _validated_authority_document(document, scope=scope)
    canonical_bytes = strict_canonical_json_bytes(validated)
    digest = strict_canonical_digest(
        POST_LKG_COLLECTION_AUTHORITY_DIGEST_DOMAIN, validated
    )
    return PostLkgCollectionAuthority._from_canonical(
        canonical_bytes=canonical_bytes,
        authority_sha256=digest,
        token=_ISSUE_AUTHORITY,
    )


def prepare_post_lkg_collection_authority(
    prepare_input: PostLkgCollectionPrepareInput,
    *,
    governance_scope: DeploymentGovernanceScope,
) -> PostLkgCollectionAuthority:
    if type(prepare_input) is not PostLkgCollectionPrepareInput:
        raise _error("POST_LKG_PREPARE_INPUT_INVALID")
    scope = _require_scope(governance_scope)
    latest = _load_verified_latest_d2(scope)
    if latest.sequence_number != prepare_input.expected_d2_sequence:
        raise _error("POST_LKG_D2_SEQUENCE_MISMATCH")
    if latest.canonical_record_digest != prepare_input.expected_d2_record_sha256:
        raise _error("POST_LKG_D2_RECORD_DIGEST_MISMATCH")
    dataset_path = Path(prepare_input.dataset001_dir)
    try:
        dataset = pin_dataset001_identity(dataset_path)
    except Exception as exc:
        raise _error("POST_LKG_DATASET_UNVERIFIED", str(exc)) from exc
    reference = latest.reference
    configuration = Exp010ServingConfiguration(
        metric=Metric(reference.metric),
        threshold_stratum=reference.threshold_stratum,
        threshold_radius=prepare_input.threshold_radius,
        range_filter=prepare_input.range_filter,
        limit=prepare_input.limit,
        served_ef=reference.evaluated_ef,
        dimensions=dataset.dimensions,
        consistency_level=prepare_input.consistency_level,
    )
    document: dict[str, Any] = {
        "campaign": {
            "campaign_id": prepare_input.campaign_id,
            "campaign_root": str(
                derive_post_lkg_campaign_root(
                    governance_scope=scope, campaign_id=prepare_input.campaign_id
                )
            ),
            "stream_id": derive_post_lkg_stream_id(
                d2_record_sha256=reference.canonical_record_digest,
                campaign_id=prepare_input.campaign_id,
            ),
        },
        "canonical_lkg": {
            "checkpoint_c_sha256": reference.canonical_evaluation_digest,
            "d2_record_sha256": reference.canonical_record_digest,
            "d2_sequence": reference.sequence_number,
            "deployment_identity": scope.deployment_identity,
            "deployment_namespace_sha256": scope.namespace_digest,
            "environment_identity": reference.environment_identity,
            "evaluated_ef": reference.evaluated_ef,
            "lkg_execution_source_revision": reference.source_revision,
            "source_run_binding_sha256": reference.source_run_binding_sha256,
            "source_run_id": reference.source_run_id,
        },
        "collection_execution_source_revision": (
            prepare_input.collection_execution_source_revision
        ),
        "containers": {
            "etcd_container": prepare_input.etcd_container,
            "milvus_container": prepare_input.milvus_container,
            "minio_container": prepare_input.minio_container,
        },
        "dataset": {
            "base_vectors_sha256": dataset.base_vectors_sha256,
            "data_identity": dataset.data_identity,
            "dataset001_dir": prepare_input.dataset001_dir,
            "generation_manifest_sha256": dataset.generation_manifest_sha256,
            "version": dataset.version,
        },
        "detector": {"seed": prepare_input.detector_seed},
        "endpoint": {
            "host_address": prepare_input.host_address,
            "host_port": prepare_input.host_port,
        },
        "milvus": {
            "database_name": "default",
            "flat_collection_name": prepare_input.flat_collection_name,
            "hnsw_collection_name": prepare_input.hnsw_collection_name,
            "index_name": INDEX_NAME,
            "uri": prepare_input.milvus_uri,
        },
        "search_budget": {
            "flat_shadow_positions": 600,
            "foreground_positions": 600,
            "hnsw_sentinel_positions": 600,
            "milvus_write_operations": 0,
        },
        "schema_version": POST_LKG_COLLECTION_AUTHORITY_SCHEMA_VERSION,
        "serialization_contract": CANONICAL_JSON_SCHEMA_VERSION,
        "serving": {
            "configuration_identity": derive_serving_configuration_identity(
                configuration
            ),
            "consistency_level": configuration.consistency_level,
            "dimensions": configuration.dimensions,
            "flat_binding_id": prepare_input.flat_binding_id,
            "hnsw_binding_id": prepare_input.hnsw_binding_id,
            "limit": configuration.limit,
            "metric": configuration.metric.value,
            "range_filter": configuration.range_filter,
            "served_ef": configuration.served_ef,
            "threshold_radius": configuration.threshold_radius,
            "threshold_stratum": configuration.threshold_stratum,
        },
        "target": {
            "expected_row_count": prepare_input.expected_row_count,
            "hnsw_ef_construction": HNSW_EF_CONSTRUCTION,
            "hnsw_m": HNSW_M,
        },
        "window_plan": {
            "complete_windows": 3,
            "current_window_sequences": [1, 2],
            "first_eligible_decision_sequence": 2,
            "queries_per_window": WINDOW_QUERY_COUNT,
            "reference_window_sequence": 0,
            "sentinel_ef": SENTINEL_EF,
            "served_ef": 400,
            "target_source_records": 3 * WINDOW_QUERY_COUNT,
            "trace_count": TRACE_COUNT,
            "trace_size": TRACE_QUERY_COUNT,
        },
        "workload_producer": {
            "external_workload_producer_identity": (
                prepare_input.external_workload_producer_identity
            )
        },
    }
    _require_document_matches_d2(document, latest, scope=scope)
    return _issue_authority(document, scope=scope)


def _validate_private_directory(path: Path, *, code: str) -> None:
    try:
        details = os.lstat(path)
    except OSError as exc:
        raise _error(code, str(exc)) from exc
    if (
        not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.getuid()
        or stat.S_IMODE(details.st_mode) != 0o700
    ):
        raise _error(code, str(path))


def _validate_authority_file(path: Path) -> os.stat_result:
    try:
        details = os.lstat(path)
    except OSError as exc:
        raise _error("POST_LKG_AUTHORITY_FILE_UNREADABLE", str(exc)) from exc
    if (
        not stat.S_ISREG(details.st_mode)
        or details.st_uid != os.getuid()
        or stat.S_IMODE(details.st_mode) != 0o600
        or details.st_nlink != 1
    ):
        raise _error("POST_LKG_AUTHORITY_FILE_UNSAFE", str(path))
    return details


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_authority_file(path: Path) -> bytes:
    _validate_authority_file(path)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or stat.S_IMODE(opened.st_mode) != 0o600
                or opened.st_nlink != 1
            ):
                raise _error("POST_LKG_AUTHORITY_FILE_UNSAFE", str(path))
            return b"".join(iter(lambda: os.read(descriptor, 64 * 1024), b""))
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise _error("POST_LKG_AUTHORITY_FILE_UNREADABLE", str(exc)) from exc


def materialize_post_lkg_collection_authority(
    authority: PostLkgCollectionAuthority,
    *,
    governance_scope: DeploymentGovernanceScope,
) -> VerifiedPostLkgCollectionAuthority:
    if type(authority) is not PostLkgCollectionAuthority:
        raise _error("POST_LKG_AUTHORITY_VALUE_INVALID")
    scope = _require_scope(governance_scope)
    document = _validated_authority_document(authority.document, scope=scope)
    _require_document_matches_d2(document, _load_verified_latest_d2(scope), scope=scope)
    if strict_canonical_json_bytes(document) != authority.canonical_bytes:
        raise _error("POST_LKG_AUTHORITY_BYTES_MISMATCH")
    if strict_canonical_digest(
        POST_LKG_COLLECTION_AUTHORITY_DIGEST_DOMAIN, document
    ) != authority.authority_sha256:
        raise _error("POST_LKG_AUTHORITY_DIGEST_MISMATCH")

    ensure_deployment_scope_directory(scope)
    authority_root = Path(scope.collection_authorities_root)
    try:
        os.mkdir(authority_root, 0o700)
        _fsync_directory(Path(scope.scope_root))
    except FileExistsError:
        pass
    except OSError as exc:
        raise _error("POST_LKG_AUTHORITY_ROOT_CREATE_FAILED", str(exc)) from exc
    _validate_private_directory(authority_root, code="POST_LKG_AUTHORITY_ROOT_UNSAFE")

    campaign_directory = Path(
        scope.collection_authority_campaign_root(authority.campaign_id)
    )
    target = Path(
        scope.collection_authority_path(
            authority.campaign_id, authority.authority_sha256
        )
    )
    created_campaign_directory = False
    try:
        os.mkdir(campaign_directory, 0o700)
        created_campaign_directory = True
        _fsync_directory(authority_root)
    except FileExistsError:
        _validate_private_directory(
            campaign_directory, code="POST_LKG_AUTHORITY_CAMPAIGN_DIRECTORY_UNSAFE"
        )
    except OSError as exc:
        raise _error("POST_LKG_AUTHORITY_CAMPAIGN_CREATE_FAILED", str(exc)) from exc
    _validate_private_directory(
        campaign_directory, code="POST_LKG_AUTHORITY_CAMPAIGN_DIRECTORY_UNSAFE"
    )

    entries = tuple(campaign_directory.iterdir())
    if entries:
        if len(entries) != 1 or entries[0] != target:
            raise _error("POST_LKG_AUTHORITY_CAMPAIGN_CONFLICT")
        if _read_authority_file(target) != authority.canonical_bytes:
            raise _error("POST_LKG_AUTHORITY_BYTES_MISMATCH")
        return load_verified_post_lkg_collection_authority(
            authority_sha256=authority.authority_sha256,
            governance_scope=scope,
        )
    if not created_campaign_directory:
        raise _error("POST_LKG_AUTHORITY_CAMPAIGN_INCOMPLETE")

    campaigns_root = Path(scope.campaigns_root)
    try:
        os.mkdir(campaigns_root, 0o700)
        _fsync_directory(Path(scope.scope_root))
    except FileExistsError:
        pass
    except OSError as exc:
        raise _error("POST_LKG_CAMPAIGNS_ROOT_CREATE_FAILED", str(exc)) from exc
    _validate_private_directory(
        campaigns_root, code="POST_LKG_CAMPAIGNS_ROOT_UNSAFE"
    )

    descriptor: int | None = None
    try:
        descriptor = os.open(
            target,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        remaining = memoryview(authority.canonical_bytes)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("zero-byte authority write")
            remaining = remaining[written:]
        os.fsync(descriptor)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_nlink != 1
        ):
            raise _error("POST_LKG_AUTHORITY_FILE_UNSAFE", str(target))
    except FileExistsError as exc:
        raise _error("POST_LKG_AUTHORITY_FILE_CONFLICT", str(target)) from exc
    except OSError as exc:
        raise _error("POST_LKG_AUTHORITY_WRITE_FAILED", str(exc)) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    _fsync_directory(campaign_directory)
    return load_verified_post_lkg_collection_authority(
        authority_sha256=authority.authority_sha256,
        governance_scope=scope,
    )


def _find_authority_path(
    *, authority_sha256: str, scope: DeploymentGovernanceScope
) -> Path:
    digest = _sha256(authority_sha256, field="authority_sha256")
    root = Path(scope.collection_authorities_root)
    _validate_private_directory(root, code="POST_LKG_AUTHORITY_ROOT_UNSAFE")
    matches: list[Path] = []
    try:
        children = sorted(root.iterdir(), key=lambda item: os.fsencode(item.name))
    except OSError as exc:
        raise _error("POST_LKG_AUTHORITY_ROOT_UNREADABLE", str(exc)) from exc
    for child in children:
        _validate_private_directory(
            child, code="POST_LKG_AUTHORITY_CAMPAIGN_DIRECTORY_UNSAFE"
        )
        candidate = child / f"{digest}.json"
        if candidate.exists() or candidate.is_symlink():
            matches.append(candidate)
    if not matches:
        raise _error("POST_LKG_AUTHORITY_MISSING", digest)
    if len(matches) != 1:
        raise _error("POST_LKG_AUTHORITY_DIGEST_COLLISION", digest)
    return matches[0]


def load_verified_post_lkg_collection_authority(
    *,
    authority_sha256: str,
    governance_scope: DeploymentGovernanceScope,
) -> VerifiedPostLkgCollectionAuthority:
    scope = _require_scope(governance_scope)
    path = _find_authority_path(authority_sha256=authority_sha256, scope=scope)
    raw = _read_authority_file(path)
    try:
        document = decode_strict_canonical_json(raw)
    except CanonicalSerializationError as exc:
        raise _error("POST_LKG_AUTHORITY_NOT_CANONICAL", str(exc)) from exc
    validated = _validated_authority_document(document, scope=scope)
    digest = strict_canonical_digest(
        POST_LKG_COLLECTION_AUTHORITY_DIGEST_DOMAIN, validated
    )
    if digest != authority_sha256:
        raise _error("POST_LKG_AUTHORITY_DIGEST_MISMATCH")
    campaign_id = validated["campaign"]["campaign_id"]
    expected = Path(scope.collection_authority_path(campaign_id, digest))
    if path != expected:
        raise _error("POST_LKG_AUTHORITY_PATH_MISMATCH")
    authority = PostLkgCollectionAuthority._from_canonical(
        canonical_bytes=raw,
        authority_sha256=digest,
        token=_ISSUE_AUTHORITY,
    )
    return VerifiedPostLkgCollectionAuthority._from_verified(
        authority=authority, path=path, token=_ISSUE_VERIFIED
    )


def verify_post_lkg_collection_authority_d2(
    authority: VerifiedPostLkgCollectionAuthority,
    *,
    governance_scope: DeploymentGovernanceScope,
) -> VerifiedLatestLkgPhase3AuthorityReference:
    if type(authority) is not VerifiedPostLkgCollectionAuthority:
        raise _error("POST_LKG_VERIFIED_AUTHORITY_REQUIRED")
    scope = _require_scope(governance_scope)
    latest = _load_verified_latest_d2(scope)
    _require_document_matches_d2(authority.document, latest, scope=scope)
    return latest
