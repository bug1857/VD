"""Pure immutable R2 response-profile population and schedule contracts.

This module implements only the pre-result commitments accepted by ADR-009
R2-G/R2-G.1.  It canonicalizes query/vector/payload/source identities, freezes
closed-role membership, builds the exact 1,200-query calibration population,
and deterministically constructs its 4,800-position PCG64 replay schedule.

It deliberately contains no observed result, latency, runtime epoch, retry,
persistence, root-pin capability, profile estimator, freshness, policy,
admission, routing, execution, Milvus, or actuation semantics.  Digests and
private constructors provide deterministic integrity/API discipline only; they
are not signatures and do not authenticate an external raw artifact.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from enum import StrEnum
import hashlib
import hmac
import math
import re
import unicodedata

import numpy as np

from .artifacts import canonical_json_bytes
from .config import IndexTrack, Metric, SearchConfiguration, THRESHOLD_LABELS
from .drift import CanonicalValue, canonical_serialize_tuple
from .response_profile import OBSERVATION_COUNT, SUPPORTED_EFS
from .search_configuration_digest import search_configuration_document


__all__ = [
    "CALIBRATION_QUERY_COUNT",
    "MEASURED_POSITION_COUNT",
    "WARMUP_QUERY_COUNT",
    "PROSPECTIVE_SEGMENT_COUNT",
    "REPLAY_MASTER_SEED",
    "SCHEDULE_NUMPY_VERSION",
    "SUPPORTED_RESPONSE_PROFILE_CELLS",
    "QUERY_PAYLOAD_SCHEMA_VERSION",
    "SOURCE_NAMESPACE_SCHEMA_VERSION",
    "OBSERVATION_IDENTITY_SCHEMA_VERSION",
    "CELL_SCHEMA_VERSION",
    "ROLE_SCHEMA_VERSION",
    "ROLE_MANIFEST_SCHEMA_VERSION",
    "CALIBRATION_POPULATION_SCHEMA_VERSION",
    "ORDERED_QUERY_PAYLOADS_SCHEMA_VERSION",
    "REPLAY_SCHEDULE_SCHEMA_VERSION",
    "REPLAY_SCHEDULE_ALGORITHM_VERSION",
    "ResponseProfileEvidenceContractError",
    "ResponseProfileSourceKind",
    "ResponseProfileRoleKind",
    "CanonicalQueryIdentity",
    "QueryVectorIdentity",
    "ResponseProfileQueryPayload",
    "ArtifactSourceNamespace",
    "LiveStreamSourceNamespace",
    "ObservationIdentity",
    "ResponseProfileCell",
    "ResponseProfileRole",
    "ResponseProfileRoleMember",
    "ResponseProfileRoleManifest",
    "CalibrationPopulationManifest",
    "ScheduleSeedEvidence",
    "ReplayPosition",
    "ReplayBlock",
    "ResponseProfileReplaySchedule",
    "canonical_response_profile_query_id",
    "canonical_response_profile_query_id_bytes",
    "build_canonical_query_identity",
    "build_query_vector_identity",
    "build_response_profile_query_payload",
    "query_payload",
    "build_artifact_source_namespace",
    "build_live_stream_source_namespace",
    "source_namespace_payload",
    "source_namespace_document",
    "build_observation_identity",
    "observation_identity_payload",
    "build_response_profile_cell",
    "cell_payload",
    "build_response_profile_role",
    "role_payload",
    "build_response_profile_role_member",
    "build_response_profile_role_manifest",
    "verify_response_profile_role_manifest",
    "role_manifest_payload",
    "role_manifest_document",
    "validate_role_manifest_disjointness",
    "build_calibration_population_manifest",
    "verify_calibration_population_manifest",
    "calibration_population_payload",
    "calibration_population_document",
    "ordered_query_payloads_payload",
    "build_response_profile_replay_schedule",
    "verify_response_profile_replay_schedule",
    "replay_schedule_payload",
    "replay_schedule_document",
]


CALIBRATION_QUERY_COUNT = OBSERVATION_COUNT
MEASURED_POSITION_COUNT = CALIBRATION_QUERY_COUNT * len(SUPPORTED_EFS)
WARMUP_QUERY_COUNT = 200
PROSPECTIVE_SEGMENT_COUNT = 20
REPLAY_MASTER_SEED = 20260810
SCHEDULE_NUMPY_VERSION = "2.5.1"
SUPPORTED_RESPONSE_PROFILE_CELLS = (
    (Metric.L2, "target-075"),
    (Metric.COSINE, "target-025"),
)

QUERY_PAYLOAD_SCHEMA_VERSION = "response-profile-query-payload-v1"
SOURCE_NAMESPACE_SCHEMA_VERSION = "response-profile-source-namespace-v1"
OBSERVATION_IDENTITY_SCHEMA_VERSION = "response-profile-observation-identity-v1"
CELL_SCHEMA_VERSION = "response-profile-cell-v1"
ROLE_SCHEMA_VERSION = "response-profile-role-v1"
ROLE_MANIFEST_SCHEMA_VERSION = "response-profile-role-manifest-v1"
CALIBRATION_POPULATION_SCHEMA_VERSION = "response-profile-calibration-population-v1"
ORDERED_QUERY_PAYLOADS_SCHEMA_VERSION = "response-profile-ordered-query-payloads-v1"
REPLAY_SCHEDULE_SCHEMA_VERSION = "response-profile-replay-schedule-v1"
REPLAY_SCHEDULE_ALGORITHM_VERSION = "response-profile-pcg64-query-block-v1"

QUERY_ID_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_QUERY_ID::V1\x00"
QUERY_VECTOR_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_QUERY_VECTOR::V1\x00"
QUERY_PAYLOAD_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_QUERY_PAYLOAD::V1\x00"
SOURCE_NAMESPACE_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_SOURCE_NAMESPACE::V1\x00"
OBSERVATION_IDENTITY_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_OBSERVATION_IDENTITY::V1\x00"
CELL_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_CELL::V1\x00"
ROLE_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_ROLE::V1\x00"
ROLE_MANIFEST_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_ROLE_MANIFEST::V1\x00"
CALIBRATION_POPULATION_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_CALIBRATION_POPULATION::V1\x00"
ORDERED_QUERY_PAYLOADS_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_ORDERED_QUERY_PAYLOADS::V1\x00"
SCHEDULE_SEED_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_SCHEDULE_SEED::V1\x00"
REPLAY_SCHEDULE_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_REPLAY_SCHEDULE::V1\x00"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_CONSTRUCTION_TOKEN = object()


class ResponseProfileEvidenceContractError(ValueError):
    """Stable fail-closed R2-A contract error."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileEvidenceContractError:
    return ResponseProfileEvidenceContractError(message, code=code)


class ResponseProfileSourceKind(StrEnum):
    """The two governed source namespace forms."""

    ARTIFACT = "ARTIFACT"
    LIVE_STREAM = "LIVE_STREAM"


class ResponseProfileRoleKind(StrEnum):
    """Closed ADR-009 R2 role catalog."""

    DETECTOR_EVIDENCE = "DETECTOR_EVIDENCE"
    RESPONSE_PROFILE_WARMUP = "RESPONSE_PROFILE_WARMUP"
    RESPONSE_PROFILE_CALIBRATION = "RESPONSE_PROFILE_CALIBRATION"
    RESPONSE_PROFILE_PROSPECTIVE_VALIDATION = "RESPONSE_PROFILE_PROSPECTIVE_VALIDATION"
    PHASE3_QUALIFICATION = "PHASE3_QUALIFICATION"
    STAGE4_ROUTING = "STAGE4_ROUTING"
    STAGE4_RECALL_AUDIT = "STAGE4_RECALL_AUDIT"
    STAGE4_SCHEDULE_CONTROL = "STAGE4_SCHEDULE_CONTROL"
    HISTORICAL_EXP001_CALIBRATION = "HISTORICAL_EXP001_CALIBRATION"
    HISTORICAL_EXP001_MEASURED = "HISTORICAL_EXP001_MEASURED"
    PROHIBITED_DATASET001_QUERY_VECTOR_INVENTORY = (
        "PROHIBITED_DATASET001_QUERY_VECTOR_INVENTORY"
    )
    PROHIBITED_DATASET002_QUERY_VECTOR_INVENTORY = (
        "PROHIBITED_DATASET002_QUERY_VECTOR_INVENTORY"
    )
    PROHIBITED_DATASET003_QUERY_VECTOR_INVENTORY = (
        "PROHIBITED_DATASET003_QUERY_VECTOR_INVENTORY"
    )


@dataclass(frozen=True, slots=True, init=False)
class CanonicalQueryIdentity:
    query_id: CanonicalValue
    query_id_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("CanonicalQueryIdentity must be built by its contract factory")


@dataclass(frozen=True, slots=True, init=False)
class QueryVectorIdentity:
    dimensions: int
    canonical_vector_bytes: bytes
    vector_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("QueryVectorIdentity must be built from canonical vector bytes")


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileQueryPayload:
    schema_version: str
    vector_sha256: str
    metric: Metric
    threshold_stratum: str
    radius: float
    range_filter: float
    limit: int
    consistency_level: str
    query_payload_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ResponseProfileQueryPayload must be built by its contract factory")


@dataclass(frozen=True, slots=True, init=False)
class ArtifactSourceNamespace:
    schema_version: str
    source_kind: ResponseProfileSourceKind
    dataset_id: str
    dataset_version: str
    generation_manifest_sha256: str
    source_namespace_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ArtifactSourceNamespace must be built by its contract factory")


@dataclass(frozen=True, slots=True, init=False)
class LiveStreamSourceNamespace:
    schema_version: str
    source_kind: ResponseProfileSourceKind
    stream_id: str
    data_identity: str
    source_workload_manifest_sha256: str
    source_namespace_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("LiveStreamSourceNamespace must be built by its contract factory")


SourceNamespace = ArtifactSourceNamespace | LiveStreamSourceNamespace


@dataclass(frozen=True, slots=True, init=False)
class ObservationIdentity:
    source_namespace_sha256: str
    query_id_sha256: str
    observation_identity_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ObservationIdentity must be built by its contract factory")


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileCell:
    schema_version: str
    metric: Metric
    threshold_stratum: str
    cell_id: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ResponseProfileCell must be built by its contract factory")


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileRole:
    schema_version: str
    kind: ResponseProfileRoleKind
    prospective_segment_index: int | None
    role_or_segment_id: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ResponseProfileRole must be built by its contract factory")


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileRoleMember:
    source_namespace: SourceNamespace
    query_identity: CanonicalQueryIdentity
    vector_identity: QueryVectorIdentity
    query_payload_identity: ResponseProfileQueryPayload
    observation_identity: ObservationIdentity

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ResponseProfileRoleMember must be built by its contract factory")


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileRoleManifest:
    schema_version: str
    role: ResponseProfileRole
    members: tuple[ResponseProfileRoleMember, ...]
    role_manifest_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ResponseProfileRoleManifest must be built by its contract factory")


@dataclass(frozen=True, slots=True, init=False)
class CalibrationPopulationManifest:
    schema_version: str
    cell: ResponseProfileCell
    calibration_role_manifest: ResponseProfileRoleManifest
    ordered_query_payload_sha256: str
    workload_manifest_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("CalibrationPopulationManifest must be built by its contract factory")


@dataclass(frozen=True, slots=True, init=False)
class ScheduleSeedEvidence:
    seed_tuple: tuple[CanonicalValue, ...]
    seed_sha256: str
    seed_u64: int

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ScheduleSeedEvidence is formula-derived")


@dataclass(frozen=True, slots=True, init=False)
class ReplayPosition:
    position_index: int
    block_index: int
    within_block_index: int
    canonical_query_index: int
    query_id: CanonicalValue
    query_id_sha256: str
    observation_identity_sha256: str
    ef: int

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ReplayPosition is schedule-derived")


@dataclass(frozen=True, slots=True, init=False)
class ReplayBlock:
    block_index: int
    canonical_query_index: int
    query_id: CanonicalValue
    query_id_sha256: str
    observation_identity_sha256: str
    ef_order_seed: ScheduleSeedEvidence
    positions: tuple[ReplayPosition, ...]

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ReplayBlock is schedule-derived")


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileReplaySchedule:
    schema_version: str
    algorithm_version: str
    numpy_version: str
    master_seed: int
    supported_efs: tuple[int, ...]
    population: CalibrationPopulationManifest
    source_revision: str
    query_order_seed: ScheduleSeedEvidence
    blocks: tuple[ReplayBlock, ...]
    replay_schedule_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ResponseProfileReplaySchedule must be formula-derived")


def _new(cls: type[object], /, **values: object) -> object:
    value = object.__new__(cls)
    for name, field_value in values.items():
        object.__setattr__(value, name, field_value)
    return value


def _digest(domain: bytes, payload: Mapping[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(payload)).hexdigest()


def _sha256(value: object, *, field: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise _error("SHA256_INVALID", f"{field} must be lower-case SHA-256")
    return value


def _canonical_text(value: object, *, field: str) -> str:
    if type(value) is not str or not value:
        raise _error("TEXT_INVALID", f"{field} must be non-empty text")
    if unicodedata.normalize("NFC", value) != value:
        raise _error("TEXT_INVALID", f"{field} must be NFC-normalized")
    return value


def _metric(value: object) -> Metric:
    if type(value) is not Metric:
        raise _error("METRIC_INVALID", "metric must be a concrete Metric")
    return value


def _threshold_stratum(value: object) -> str:
    if type(value) is not str or value not in THRESHOLD_LABELS:
        raise _error("THRESHOLD_STRATUM_INVALID", "threshold stratum is unsupported")
    return value


def _exact_int(value: object, *, field: str, minimum: int | None = None) -> int:
    if type(value) is not int:
        raise _error("INTEGER_INVALID", f"{field} must be an exact integer")
    if minimum is not None and value < minimum:
        raise _error("INTEGER_INVALID", f"{field} is below its minimum")
    return value


def _make(cls: type[object], *, construction_token: object, **values: object) -> object:
    if construction_token is not _CONSTRUCTION_TOKEN:
        raise TypeError("response-profile evidence construction token is invalid")
    return _new(cls, **values)


def _type_exact_equal(actual: object, expected: object) -> bool:
    if type(actual) is not type(expected):
        return False
    if type(expected) is tuple:
        return len(actual) == len(expected) and all(  # type: ignore[arg-type]
            _type_exact_equal(actual_item, expected_item)
            for actual_item, expected_item in zip(actual, expected, strict=True)  # type: ignore[arg-type]
        )
    try:
        expected_fields = fields(expected)
    except TypeError:
        return actual == expected
    try:
        return all(
            _type_exact_equal(
                getattr(actual, field.name), getattr(expected, field.name)
            )
            for field in expected_fields
        )
    except AttributeError:
        return False


def _same_fields(actual: object, expected: object) -> bool:
    return _type_exact_equal(actual, expected)


def canonical_response_profile_query_id(value: object) -> CanonicalValue:
    """Return one unchanged R1-compatible canonical query ID."""

    if type(value) is int:
        return value
    if type(value) is str:
        if not value:
            raise _error("QUERY_ID_INVALID", "string query IDs must be non-empty")
        if unicodedata.normalize("NFC", value) != value:
            raise _error("QUERY_ID_INVALID", "string query IDs must be NFC-normalized")
        return value
    raise _error("QUERY_ID_INVALID", "query ID must be exact int or str; bool is forbidden")


def canonical_response_profile_query_id_bytes(value: object) -> bytes:
    """Return the accepted length-prefixed R1 query identity bytes."""

    return canonical_serialize_tuple((canonical_response_profile_query_id(value),))


def build_canonical_query_identity(value: object) -> CanonicalQueryIdentity:
    query_id = canonical_response_profile_query_id(value)
    digest = hashlib.sha256(
        QUERY_ID_HASH_DOMAIN + canonical_serialize_tuple((query_id,))
    ).hexdigest()
    return _make(
        CanonicalQueryIdentity,
        construction_token=_CONSTRUCTION_TOKEN,
        query_id=query_id,
        query_id_sha256=digest,
    )  # type: ignore[return-value]


def _verify_query_identity(value: object) -> CanonicalQueryIdentity:
    if type(value) is not CanonicalQueryIdentity:
        raise _error("QUERY_IDENTITY_INVALID", "query identity must be concrete")
    try:
        rebuilt = build_canonical_query_identity(value.query_id)
    except (AttributeError, TypeError, ValueError) as exc:
        if isinstance(exc, ResponseProfileEvidenceContractError):
            raise
        raise _error("QUERY_IDENTITY_INVALID", "query identity is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("QUERY_IDENTITY_INVALID", "query identity failed recomputation")
    return rebuilt


def build_query_vector_identity(vector: np.ndarray) -> QueryVectorIdentity:
    """Hash one exact finite contiguous one-dimensional little-endian <f4 vector."""

    if type(vector) is not np.ndarray:
        raise _error("VECTOR_INVALID", "query vector must be a concrete numpy.ndarray")
    if vector.ndim != 1 or vector.size == 0:
        raise _error("VECTOR_INVALID", "query vector must be non-empty and one-dimensional")
    if vector.dtype.str != "<f4":
        raise _error("VECTOR_INVALID", "query vector dtype must be exact little-endian <f4")
    if not vector.flags.c_contiguous:
        raise _error("VECTOR_INVALID", "query vector must be C-contiguous")
    if not bool(np.isfinite(vector).all()):
        raise _error("VECTOR_INVALID", "query vector values must be finite")
    dimensions = int(vector.size)
    vector_bytes = vector.tobytes(order="C")
    return _vector_identity_from_canonical_bytes(
        dimensions=dimensions, vector_bytes=vector_bytes
    )


def _vector_identity_from_canonical_bytes(
    *, dimensions: int, vector_bytes: bytes
) -> QueryVectorIdentity:
    framed = canonical_serialize_tuple(("dtype", "<f4", "dimensions", dimensions))
    digest = hashlib.sha256(
        QUERY_VECTOR_HASH_DOMAIN + framed + vector_bytes
    ).hexdigest()
    return _make(
        QueryVectorIdentity,
        construction_token=_CONSTRUCTION_TOKEN,
        dimensions=dimensions,
        canonical_vector_bytes=vector_bytes,
        vector_sha256=digest,
    )  # type: ignore[return-value]


def _verify_vector_identity(value: object) -> QueryVectorIdentity:
    if type(value) is not QueryVectorIdentity:
        raise _error("VECTOR_IDENTITY_INVALID", "vector identity must be concrete")
    try:
        dimensions = _exact_int(value.dimensions, field="dimensions", minimum=1)
        if type(value.canonical_vector_bytes) is not bytes:
            raise _error(
                "VECTOR_IDENTITY_INVALID",
                "canonical vector bytes must be immutable bytes",
            )
        if len(value.canonical_vector_bytes) != dimensions * 4:
            raise _error(
                "VECTOR_IDENTITY_INVALID",
                "canonical vector byte length does not match dimensions",
            )
        vector = np.frombuffer(value.canonical_vector_bytes, dtype="<f4")
        if not bool(np.isfinite(vector).all()):
            raise _error(
                "VECTOR_IDENTITY_INVALID", "canonical vector bytes are non-finite"
            )
        rebuilt = _vector_identity_from_canonical_bytes(
            dimensions=dimensions,
            vector_bytes=value.canonical_vector_bytes,
        )
        _sha256(value.vector_sha256, field="vector_sha256")
    except AttributeError as exc:
        raise _error("VECTOR_IDENTITY_INVALID", "vector identity is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("VECTOR_IDENTITY_INVALID", "vector identity failed recomputation")
    return rebuilt


def _validated_common_search_configuration(value: object) -> SearchConfiguration:
    if type(value) is not SearchConfiguration:
        raise _error("QUERY_PAYLOAD_INVALID", "search configuration must be concrete")
    try:
        rebuilt = SearchConfiguration(
            metric=value.metric,
            threshold_label=value.threshold_label,
            radius=value.radius,
            index_track=value.index_track,
            ef=value.ef,
            limit=value.limit,
            consistency_level=value.consistency_level,
        )
        rebuilt.validate()
        document = search_configuration_document(rebuilt)
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("QUERY_PAYLOAD_INVALID", "search configuration is malformed") from exc
    if rebuilt != value:
        raise _error("QUERY_PAYLOAD_INVALID", "search configuration reconstruction changed")
    # The payload binds common range-query semantics only.  Index track and ef
    # are intentionally absent under R2-G and therefore are not constrained here.
    return SearchConfiguration(
        metric=Metric(document["metric"]),
        threshold_label=str(document["threshold_label"]),
        radius=float(document["radius"]),
        index_track=IndexTrack.FLAT,
        ef=None,
        limit=int(document["limit"]),
        consistency_level=str(document["consistency_level"]),
    )


def _query_payload_from_parts(
    *,
    vector_sha256: str,
    metric: Metric,
    threshold_stratum: str,
    radius: float,
    range_filter: float,
    limit: int,
    consistency_level: str,
) -> ResponseProfileQueryPayload:
    surrogate = SearchConfiguration(
        metric=metric,
        threshold_label=threshold_stratum,
        radius=radius,
        index_track=IndexTrack.FLAT,
        ef=None,
        limit=limit,
        consistency_level=consistency_level,
    )
    try:
        surrogate.validate()
        configuration_document = search_configuration_document(surrogate)
    except (TypeError, ValueError) as exc:
        raise _error("QUERY_PAYLOAD_INVALID", "common search semantics are invalid") from exc
    expected_range = configuration_document["range_filter"]
    if type(range_filter) is not float or not math.isfinite(range_filter):
        raise _error("QUERY_PAYLOAD_INVALID", "range_filter must be a finite exact float")
    normalized_range = 0.0 if range_filter == 0.0 else range_filter
    if normalized_range != expected_range:
        raise _error("QUERY_PAYLOAD_INVALID", "range_filter does not match metric")
    payload = {
        "schema_version": QUERY_PAYLOAD_SCHEMA_VERSION,
        "vector_sha256": _sha256(vector_sha256, field="vector_sha256"),
        "metric": metric.value,
        "threshold_stratum": threshold_stratum,
        "radius": float(configuration_document["radius"]),
        "range_filter": normalized_range,
        "limit": limit,
        "consistency_level": consistency_level,
    }
    digest = _digest(QUERY_PAYLOAD_HASH_DOMAIN, payload)
    return _make(
        ResponseProfileQueryPayload,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=QUERY_PAYLOAD_SCHEMA_VERSION,
        vector_sha256=payload["vector_sha256"],
        metric=metric,
        threshold_stratum=threshold_stratum,
        radius=payload["radius"],
        range_filter=payload["range_filter"],
        limit=limit,
        consistency_level=consistency_level,
        query_payload_sha256=digest,
    )  # type: ignore[return-value]


def build_response_profile_query_payload(
    *,
    vector_identity: QueryVectorIdentity,
    search_configuration: SearchConfiguration,
) -> ResponseProfileQueryPayload:
    vector = _verify_vector_identity(vector_identity)
    common = _validated_common_search_configuration(search_configuration)
    document = search_configuration_document(common)
    return _query_payload_from_parts(
        vector_sha256=vector.vector_sha256,
        metric=common.metric,
        threshold_stratum=common.threshold_label,
        radius=float(document["radius"]),
        range_filter=float(document["range_filter"]),
        limit=common.limit,
        consistency_level=common.consistency_level,
    )


def _verify_query_payload(value: object) -> ResponseProfileQueryPayload:
    if type(value) is not ResponseProfileQueryPayload:
        raise _error("QUERY_PAYLOAD_INVALID", "query payload identity must be concrete")
    try:
        if value.schema_version != QUERY_PAYLOAD_SCHEMA_VERSION:
            raise _error("QUERY_PAYLOAD_INVALID", "query payload schema is unsupported")
        rebuilt = _query_payload_from_parts(
            vector_sha256=value.vector_sha256,
            metric=_metric(value.metric),
            threshold_stratum=_threshold_stratum(value.threshold_stratum),
            radius=value.radius,
            range_filter=value.range_filter,
            limit=_exact_int(value.limit, field="limit", minimum=1),
            consistency_level=_canonical_text(
                value.consistency_level, field="consistency_level"
            ),
        )
    except AttributeError as exc:
        raise _error("QUERY_PAYLOAD_INVALID", "query payload is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("QUERY_PAYLOAD_INVALID", "query payload failed recomputation")
    return rebuilt


def query_payload(value: ResponseProfileQueryPayload) -> dict[str, object]:
    validated = _verify_query_payload(value)
    return {
        "schema_version": validated.schema_version,
        "vector_sha256": validated.vector_sha256,
        "metric": validated.metric.value,
        "threshold_stratum": validated.threshold_stratum,
        "radius": validated.radius,
        "range_filter": validated.range_filter,
        "limit": validated.limit,
        "consistency_level": validated.consistency_level,
    }


def _source_digest(payload: Mapping[str, object]) -> str:
    return _digest(SOURCE_NAMESPACE_HASH_DOMAIN, payload)


def build_artifact_source_namespace(
    *, dataset_id: str, dataset_version: str, generation_manifest_sha256: str
) -> ArtifactSourceNamespace:
    payload = {
        "schema_version": SOURCE_NAMESPACE_SCHEMA_VERSION,
        "source_kind": ResponseProfileSourceKind.ARTIFACT.value,
        "dataset_id": _canonical_text(dataset_id, field="dataset_id"),
        "dataset_version": _canonical_text(dataset_version, field="dataset_version"),
        "generation_manifest_sha256": _sha256(
            generation_manifest_sha256, field="generation_manifest_sha256"
        ),
    }
    return _make(
        ArtifactSourceNamespace,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=SOURCE_NAMESPACE_SCHEMA_VERSION,
        source_kind=ResponseProfileSourceKind.ARTIFACT,
        dataset_id=payload["dataset_id"],
        dataset_version=payload["dataset_version"],
        generation_manifest_sha256=payload["generation_manifest_sha256"],
        source_namespace_sha256=_source_digest(payload),
    )  # type: ignore[return-value]


def build_live_stream_source_namespace(
    *, stream_id: str, data_identity: str, source_workload_manifest_sha256: str
) -> LiveStreamSourceNamespace:
    payload = {
        "schema_version": SOURCE_NAMESPACE_SCHEMA_VERSION,
        "source_kind": ResponseProfileSourceKind.LIVE_STREAM.value,
        "stream_id": _canonical_text(stream_id, field="stream_id"),
        "data_identity": _canonical_text(data_identity, field="data_identity"),
        "source_workload_manifest_sha256": _sha256(
            source_workload_manifest_sha256,
            field="source_workload_manifest_sha256",
        ),
    }
    return _make(
        LiveStreamSourceNamespace,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=SOURCE_NAMESPACE_SCHEMA_VERSION,
        source_kind=ResponseProfileSourceKind.LIVE_STREAM,
        stream_id=payload["stream_id"],
        data_identity=payload["data_identity"],
        source_workload_manifest_sha256=payload["source_workload_manifest_sha256"],
        source_namespace_sha256=_source_digest(payload),
    )  # type: ignore[return-value]


def _verify_source_namespace(value: object) -> SourceNamespace:
    try:
        if type(value) is ArtifactSourceNamespace:
            rebuilt: SourceNamespace = build_artifact_source_namespace(
                dataset_id=value.dataset_id,
                dataset_version=value.dataset_version,
                generation_manifest_sha256=value.generation_manifest_sha256,
            )
        elif type(value) is LiveStreamSourceNamespace:
            rebuilt = build_live_stream_source_namespace(
                stream_id=value.stream_id,
                data_identity=value.data_identity,
                source_workload_manifest_sha256=value.source_workload_manifest_sha256,
            )
        else:
            raise _error("SOURCE_NAMESPACE_INVALID", "source namespace type is invalid")
    except AttributeError as exc:
        raise _error("SOURCE_NAMESPACE_INVALID", "source namespace is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("SOURCE_NAMESPACE_INVALID", "source namespace failed recomputation")
    return rebuilt


def source_namespace_payload(value: SourceNamespace) -> dict[str, object]:
    validated = _verify_source_namespace(value)
    if type(validated) is ArtifactSourceNamespace:
        return {
            "schema_version": validated.schema_version,
            "source_kind": validated.source_kind.value,
            "dataset_id": validated.dataset_id,
            "dataset_version": validated.dataset_version,
            "generation_manifest_sha256": validated.generation_manifest_sha256,
        }
    return {
        "schema_version": validated.schema_version,
        "source_kind": validated.source_kind.value,
        "stream_id": validated.stream_id,
        "data_identity": validated.data_identity,
        "source_workload_manifest_sha256": validated.source_workload_manifest_sha256,
    }


def source_namespace_document(value: SourceNamespace) -> dict[str, object]:
    validated = _verify_source_namespace(value)
    return {
        "source_namespace_payload": source_namespace_payload(validated),
        "source_namespace_sha256": validated.source_namespace_sha256,
    }


def build_observation_identity(
    *, source_namespace: SourceNamespace, query_identity: CanonicalQueryIdentity
) -> ObservationIdentity:
    source = _verify_source_namespace(source_namespace)
    query = _verify_query_identity(query_identity)
    payload = {
        "schema_version": OBSERVATION_IDENTITY_SCHEMA_VERSION,
        "source_namespace_sha256": source.source_namespace_sha256,
        "query_id_sha256": query.query_id_sha256,
    }
    return _make(
        ObservationIdentity,
        construction_token=_CONSTRUCTION_TOKEN,
        source_namespace_sha256=source.source_namespace_sha256,
        query_id_sha256=query.query_id_sha256,
        observation_identity_sha256=_digest(OBSERVATION_IDENTITY_HASH_DOMAIN, payload),
    )  # type: ignore[return-value]


def _verify_observation_identity(
    value: object, *, source: SourceNamespace, query: CanonicalQueryIdentity
) -> ObservationIdentity:
    if type(value) is not ObservationIdentity:
        raise _error("OBSERVATION_IDENTITY_INVALID", "observation identity must be concrete")
    rebuilt = build_observation_identity(source_namespace=source, query_identity=query)
    if not _same_fields(value, rebuilt):
        raise _error("OBSERVATION_IDENTITY_INVALID", "observation identity failed recomputation")
    return rebuilt


def observation_identity_payload(value: ObservationIdentity) -> dict[str, object]:
    if type(value) is not ObservationIdentity:
        raise _error("OBSERVATION_IDENTITY_INVALID", "observation identity must be concrete")
    payload = {
        "schema_version": OBSERVATION_IDENTITY_SCHEMA_VERSION,
        "source_namespace_sha256": _sha256(
            value.source_namespace_sha256, field="source_namespace_sha256"
        ),
        "query_id_sha256": _sha256(value.query_id_sha256, field="query_id_sha256"),
    }
    expected = _digest(OBSERVATION_IDENTITY_HASH_DOMAIN, payload)
    actual = _sha256(
        value.observation_identity_sha256,
        field="observation_identity_sha256",
    )
    if not hmac.compare_digest(actual, expected):
        raise _error(
            "OBSERVATION_IDENTITY_INVALID",
            "observation identity digest does not match its payload",
        )
    return payload


def build_response_profile_cell(
    *, metric: Metric, threshold_stratum: str
) -> ResponseProfileCell:
    normalized_metric = _metric(metric)
    normalized_stratum = _threshold_stratum(threshold_stratum)
    if (normalized_metric, normalized_stratum) not in SUPPORTED_RESPONSE_PROFILE_CELLS:
        raise _error(
            "CELL_UNSUPPORTED",
            "metric and threshold stratum do not identify an EXP-010 v1 cell",
        )
    payload = {
        "schema_version": CELL_SCHEMA_VERSION,
        "metric": normalized_metric.value,
        "threshold_stratum": normalized_stratum,
    }
    return _make(
        ResponseProfileCell,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=CELL_SCHEMA_VERSION,
        metric=normalized_metric,
        threshold_stratum=normalized_stratum,
        cell_id=_digest(CELL_HASH_DOMAIN, payload),
    )  # type: ignore[return-value]


def _verify_cell(value: object) -> ResponseProfileCell:
    if type(value) is not ResponseProfileCell:
        raise _error("CELL_INVALID", "cell must be concrete")
    try:
        rebuilt = build_response_profile_cell(
            metric=value.metric, threshold_stratum=value.threshold_stratum
        )
    except AttributeError as exc:
        raise _error("CELL_INVALID", "cell is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("CELL_INVALID", "cell failed recomputation")
    return rebuilt


def cell_payload(value: ResponseProfileCell) -> dict[str, object]:
    validated = _verify_cell(value)
    return {
        "schema_version": validated.schema_version,
        "metric": validated.metric.value,
        "threshold_stratum": validated.threshold_stratum,
    }


def build_response_profile_role(
    *,
    kind: ResponseProfileRoleKind,
    prospective_segment_index: int | None = None,
) -> ResponseProfileRole:
    if type(kind) is not ResponseProfileRoleKind:
        raise _error("ROLE_INVALID", "role kind must be a concrete closed enum")
    if kind is ResponseProfileRoleKind.RESPONSE_PROFILE_PROSPECTIVE_VALIDATION:
        if (
            type(prospective_segment_index) is not int
            or not 0 <= prospective_segment_index < PROSPECTIVE_SEGMENT_COUNT
        ):
            raise _error("ROLE_INVALID", "prospective segment index must be in 0..19")
        index = prospective_segment_index
    else:
        if prospective_segment_index is not None:
            raise _error("ROLE_INVALID", "non-prospective roles require a null segment index")
        index = None
    payload = {
        "schema_version": ROLE_SCHEMA_VERSION,
        "kind": kind.value,
        "prospective_segment_index": index,
    }
    return _make(
        ResponseProfileRole,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=ROLE_SCHEMA_VERSION,
        kind=kind,
        prospective_segment_index=index,
        role_or_segment_id=_digest(ROLE_HASH_DOMAIN, payload),
    )  # type: ignore[return-value]


def _verify_role(value: object) -> ResponseProfileRole:
    if type(value) is not ResponseProfileRole:
        raise _error("ROLE_INVALID", "role must be concrete")
    try:
        rebuilt = build_response_profile_role(
            kind=value.kind,
            prospective_segment_index=value.prospective_segment_index,
        )
    except AttributeError as exc:
        raise _error("ROLE_INVALID", "role is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("ROLE_INVALID", "role failed recomputation")
    return rebuilt


def role_payload(value: ResponseProfileRole) -> dict[str, object]:
    validated = _verify_role(value)
    return {
        "schema_version": validated.schema_version,
        "kind": validated.kind.value,
        "prospective_segment_index": validated.prospective_segment_index,
    }


def build_response_profile_role_member(
    *,
    source_namespace: SourceNamespace,
    query_identity: CanonicalQueryIdentity,
    vector_identity: QueryVectorIdentity,
    query_payload_identity: ResponseProfileQueryPayload,
) -> ResponseProfileRoleMember:
    source = _verify_source_namespace(source_namespace)
    query = _verify_query_identity(query_identity)
    vector = _verify_vector_identity(vector_identity)
    payload_identity = _verify_query_payload(query_payload_identity)
    if payload_identity.vector_sha256 != vector.vector_sha256:
        raise _error("ROLE_MEMBER_INVALID", "query payload does not bind member vector")
    observation = build_observation_identity(
        source_namespace=source, query_identity=query
    )
    return _make(
        ResponseProfileRoleMember,
        construction_token=_CONSTRUCTION_TOKEN,
        source_namespace=source,
        query_identity=query,
        vector_identity=vector,
        query_payload_identity=payload_identity,
        observation_identity=observation,
    )  # type: ignore[return-value]


def _verify_role_member(value: object) -> ResponseProfileRoleMember:
    if type(value) is not ResponseProfileRoleMember:
        raise _error("ROLE_MEMBER_INVALID", "role member must be concrete")
    try:
        rebuilt = build_response_profile_role_member(
            source_namespace=value.source_namespace,
            query_identity=value.query_identity,
            vector_identity=value.vector_identity,
            query_payload_identity=value.query_payload_identity,
        )
        _verify_observation_identity(
            value.observation_identity,
            source=rebuilt.source_namespace,
            query=rebuilt.query_identity,
        )
    except AttributeError as exc:
        raise _error("ROLE_MEMBER_INVALID", "role member is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("ROLE_MEMBER_INVALID", "role member failed recomputation")
    return rebuilt


def _role_member_payload(index: int, member: ResponseProfileRoleMember) -> dict[str, object]:
    value = _verify_role_member(member)
    return {
        "canonical_order_index": index,
        "source_namespace_sha256": value.source_namespace.source_namespace_sha256,
        "query_id": value.query_identity.query_id,
        "query_id_sha256": value.query_identity.query_id_sha256,
        "observation_identity_sha256": value.observation_identity.observation_identity_sha256,
        "vector_sha256": value.vector_identity.vector_sha256,
        "query_payload": query_payload(value.query_payload_identity),
        "query_payload_sha256": value.query_payload_identity.query_payload_sha256,
    }


def _required_role_count(role: ResponseProfileRole) -> int | None:
    if role.kind is ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP:
        return WARMUP_QUERY_COUNT
    if role.kind in (
        ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION,
        ResponseProfileRoleKind.RESPONSE_PROFILE_PROSPECTIVE_VALIDATION,
    ):
        return CALIBRATION_QUERY_COUNT
    return None


def _role_manifest_payload_from_parts(
    role: ResponseProfileRole, members: tuple[ResponseProfileRoleMember, ...]
) -> dict[str, object]:
    sources: dict[str, SourceNamespace] = {}
    for member in members:
        source = member.source_namespace
        existing = sources.get(source.source_namespace_sha256)
        if existing is not None and existing != source:
            raise _error("SOURCE_NAMESPACE_COLLISION", "source digest binds two payloads")
        sources[source.source_namespace_sha256] = source
    return {
        "schema_version": ROLE_MANIFEST_SCHEMA_VERSION,
        "role": role_payload(role),
        "source_namespaces": [
            source_namespace_document(sources[digest]) for digest in sorted(sources)
        ],
        "member_count": len(members),
        "members": [
            _role_member_payload(index, member) for index, member in enumerate(members)
        ],
    }


def build_response_profile_role_manifest(
    *, role: ResponseProfileRole, members: tuple[ResponseProfileRoleMember, ...]
) -> ResponseProfileRoleManifest:
    validated_role = _verify_role(role)
    if type(members) is not tuple or not members:
        raise _error("ROLE_MANIFEST_INVALID", "role manifest members must be a non-empty tuple")
    validated_members = tuple(_verify_role_member(member) for member in members)
    required_count = _required_role_count(validated_role)
    if required_count is not None and len(validated_members) != required_count:
        raise _error(
            "ROLE_MEMBER_COUNT_INVALID",
            f"role requires exactly {required_count} members",
        )
    seen_observations: set[str] = set()
    seen_vectors: set[str] = set()
    seen_payloads: set[str] = set()
    for member in validated_members:
        observation = member.observation_identity.observation_identity_sha256
        vector = member.vector_identity.vector_sha256
        payload_digest = member.query_payload_identity.query_payload_sha256
        if observation in seen_observations:
            raise _error("ROLE_MEMBER_DUPLICATE", "role contains a duplicate observation")
        if vector in seen_vectors:
            raise _error("ROLE_VECTOR_DUPLICATE", "role contains a duplicate vector")
        if payload_digest in seen_payloads:
            raise _error("ROLE_PAYLOAD_DUPLICATE", "role contains a duplicate query payload")
        seen_observations.add(observation)
        seen_vectors.add(vector)
        seen_payloads.add(payload_digest)
    payload = _role_manifest_payload_from_parts(validated_role, validated_members)
    return _make(
        ResponseProfileRoleManifest,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=ROLE_MANIFEST_SCHEMA_VERSION,
        role=validated_role,
        members=validated_members,
        role_manifest_sha256=_digest(ROLE_MANIFEST_HASH_DOMAIN, payload),
    )  # type: ignore[return-value]


def verify_response_profile_role_manifest(
    value: object,
) -> ResponseProfileRoleManifest:
    if type(value) is not ResponseProfileRoleManifest:
        raise _error("ROLE_MANIFEST_INVALID", "role manifest must be concrete")
    try:
        if value.schema_version != ROLE_MANIFEST_SCHEMA_VERSION:
            raise _error("ROLE_MANIFEST_INVALID", "role manifest schema is unsupported")
        rebuilt = build_response_profile_role_manifest(
            role=value.role, members=value.members
        )
    except AttributeError as exc:
        raise _error("ROLE_MANIFEST_INVALID", "role manifest is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("ROLE_MANIFEST_INVALID", "role manifest failed recomputation")
    return rebuilt


def role_manifest_payload(value: ResponseProfileRoleManifest) -> dict[str, object]:
    validated = verify_response_profile_role_manifest(value)
    return _role_manifest_payload_from_parts(validated.role, validated.members)


def role_manifest_document(value: ResponseProfileRoleManifest) -> dict[str, object]:
    validated = verify_response_profile_role_manifest(value)
    return {
        "role_manifest_payload": role_manifest_payload(validated),
        "role_manifest_sha256": validated.role_manifest_sha256,
    }


def validate_role_manifest_disjointness(
    manifests: tuple[ResponseProfileRoleManifest, ...],
) -> None:
    """Prove disjointness among exactly the supplied closed-role manifests.

    This cannot prove that an omitted governed role was supplied.  Closed-catalog
    completeness belongs to the later raw-evidence root verifier.
    """

    if type(manifests) is not tuple or not manifests:
        raise _error("ROLE_CATALOG_INVALID", "manifests must be a non-empty tuple")
    validated = tuple(verify_response_profile_role_manifest(item) for item in manifests)
    seen_roles: set[str] = set()
    seen_local_ids: set[tuple[str, str]] = set()
    seen_observations: set[str] = set()
    seen_vectors: set[str] = set()
    seen_payloads: set[str] = set()
    for manifest in validated:
        role_id = manifest.role.role_or_segment_id
        if role_id in seen_roles:
            raise _error("ROLE_CATALOG_DUPLICATE", "role manifest is duplicated")
        seen_roles.add(role_id)
        for member in manifest.members:
            local_id = (
                member.source_namespace.source_namespace_sha256,
                member.query_identity.query_id_sha256,
            )
            observation = member.observation_identity.observation_identity_sha256
            vector = member.vector_identity.vector_sha256
            payload_digest = member.query_payload_identity.query_payload_sha256
            if local_id in seen_local_ids or observation in seen_observations:
                raise _error("ROLE_QUERY_OVERLAP", "role manifests overlap by query identity")
            if vector in seen_vectors:
                raise _error("ROLE_VECTOR_OVERLAP", "role manifests overlap by vector")
            if payload_digest in seen_payloads:
                raise _error("ROLE_PAYLOAD_OVERLAP", "role manifests overlap by query payload")
            seen_local_ids.add(local_id)
            seen_observations.add(observation)
            seen_vectors.add(vector)
            seen_payloads.add(payload_digest)


def ordered_query_payloads_payload(
    value: ResponseProfileRoleManifest,
) -> dict[str, object]:
    manifest = verify_response_profile_role_manifest(value)
    if manifest.role.kind is not ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION:
        raise _error("CALIBRATION_POPULATION_INVALID", "ordered payloads require calibration role")
    return {
        "schema_version": ORDERED_QUERY_PAYLOADS_SCHEMA_VERSION,
        "query_payload_sha256": [
            member.query_payload_identity.query_payload_sha256
            for member in manifest.members
        ],
    }


def _calibration_population_payload_from_parts(
    cell: ResponseProfileCell,
    manifest: ResponseProfileRoleManifest,
    ordered_payload_digest: str,
) -> dict[str, object]:
    return {
        "schema_version": CALIBRATION_POPULATION_SCHEMA_VERSION,
        "cell": cell_payload(cell),
        "cell_id": cell.cell_id,
        "role": role_payload(manifest.role),
        "role_or_segment_id": manifest.role.role_or_segment_id,
        "calibration_role_manifest_sha256": manifest.role_manifest_sha256,
        "observation_count": len(manifest.members),
        "ordered_query_id_sha256": [
            member.query_identity.query_id_sha256 for member in manifest.members
        ],
        "ordered_observation_identity_sha256": [
            member.observation_identity.observation_identity_sha256
            for member in manifest.members
        ],
        "ordered_vector_sha256": [
            member.vector_identity.vector_sha256 for member in manifest.members
        ],
        "ordered_query_payload_sha256": ordered_payload_digest,
    }


def build_calibration_population_manifest(
    *, cell: ResponseProfileCell, calibration_role_manifest: ResponseProfileRoleManifest
) -> CalibrationPopulationManifest:
    validated_cell = _verify_cell(cell)
    manifest = verify_response_profile_role_manifest(calibration_role_manifest)
    if manifest.role.kind is not ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION:
        raise _error("CALIBRATION_POPULATION_INVALID", "manifest role is not calibration")
    if len(manifest.members) != CALIBRATION_QUERY_COUNT:
        raise _error("CALIBRATION_POPULATION_INVALID", "calibration requires 1200 members")
    seen_query_ids: set[str] = set()
    for member in manifest.members:
        if member.query_identity.query_id_sha256 in seen_query_ids:
            raise _error(
                "CALIBRATION_QUERY_ID_DUPLICATE",
                "calibration query IDs collide under unchanged R1 canonicalization",
            )
        seen_query_ids.add(member.query_identity.query_id_sha256)
        payload_identity = member.query_payload_identity
        if (
            payload_identity.metric is not validated_cell.metric
            or payload_identity.threshold_stratum != validated_cell.threshold_stratum
        ):
            raise _error("CALIBRATION_CELL_MISMATCH", "query payload does not match cell")
    ordered_payload = ordered_query_payloads_payload(manifest)
    ordered_digest = _digest(ORDERED_QUERY_PAYLOADS_HASH_DOMAIN, ordered_payload)
    payload = _calibration_population_payload_from_parts(
        validated_cell, manifest, ordered_digest
    )
    return _make(
        CalibrationPopulationManifest,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=CALIBRATION_POPULATION_SCHEMA_VERSION,
        cell=validated_cell,
        calibration_role_manifest=manifest,
        ordered_query_payload_sha256=ordered_digest,
        workload_manifest_sha256=_digest(CALIBRATION_POPULATION_HASH_DOMAIN, payload),
    )  # type: ignore[return-value]


def verify_calibration_population_manifest(
    value: object,
) -> CalibrationPopulationManifest:
    if type(value) is not CalibrationPopulationManifest:
        raise _error("CALIBRATION_POPULATION_INVALID", "population must be concrete")
    try:
        if value.schema_version != CALIBRATION_POPULATION_SCHEMA_VERSION:
            raise _error("CALIBRATION_POPULATION_INVALID", "population schema is unsupported")
        rebuilt = build_calibration_population_manifest(
            cell=value.cell,
            calibration_role_manifest=value.calibration_role_manifest,
        )
    except AttributeError as exc:
        raise _error("CALIBRATION_POPULATION_INVALID", "population is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("CALIBRATION_POPULATION_INVALID", "population failed recomputation")
    return rebuilt


def calibration_population_payload(
    value: CalibrationPopulationManifest,
) -> dict[str, object]:
    validated = verify_calibration_population_manifest(value)
    return _calibration_population_payload_from_parts(
        validated.cell,
        validated.calibration_role_manifest,
        validated.ordered_query_payload_sha256,
    )


def calibration_population_document(
    value: CalibrationPopulationManifest,
) -> dict[str, object]:
    validated = verify_calibration_population_manifest(value)
    return {
        "calibration_population_payload": calibration_population_payload(validated),
        "workload_manifest_sha256": validated.workload_manifest_sha256,
    }


def _schedule_seed(values: tuple[CanonicalValue, ...]) -> ScheduleSeedEvidence:
    serialized = canonical_serialize_tuple(values)
    digest_bytes = hashlib.sha256(SCHEDULE_SEED_HASH_DOMAIN + serialized).digest()
    return _make(
        ScheduleSeedEvidence,
        construction_token=_CONSTRUCTION_TOKEN,
        seed_tuple=values,
        seed_sha256=digest_bytes.hex(),
        seed_u64=int.from_bytes(digest_bytes[:8], byteorder="big", signed=False),
    )  # type: ignore[return-value]


def _seed_payload(value: ScheduleSeedEvidence) -> dict[str, object]:
    return {
        "seed_tuple": list(value.seed_tuple),
        "seed_sha256": value.seed_sha256,
        "seed_u64": value.seed_u64,
    }


def _position_payload(value: ReplayPosition) -> dict[str, object]:
    return {
        "position_index": value.position_index,
        "block_index": value.block_index,
        "within_block_index": value.within_block_index,
        "canonical_query_index": value.canonical_query_index,
        "query_id": value.query_id,
        "query_id_sha256": value.query_id_sha256,
        "observation_identity_sha256": value.observation_identity_sha256,
        "ef": value.ef,
    }


def _block_payload(value: ReplayBlock) -> dict[str, object]:
    return {
        "block_index": value.block_index,
        "canonical_query_index": value.canonical_query_index,
        "query_id": value.query_id,
        "query_id_sha256": value.query_id_sha256,
        "observation_identity_sha256": value.observation_identity_sha256,
        "ef_order_seed": _seed_payload(value.ef_order_seed),
        "positions": [_position_payload(position) for position in value.positions],
    }


def _replay_schedule_payload_from_parts(
    *,
    population: CalibrationPopulationManifest,
    source_revision: str,
    query_order_seed: ScheduleSeedEvidence,
    blocks: tuple[ReplayBlock, ...],
) -> dict[str, object]:
    return {
        "schema_version": REPLAY_SCHEDULE_SCHEMA_VERSION,
        "algorithm_version": REPLAY_SCHEDULE_ALGORITHM_VERSION,
        "numpy_version": SCHEDULE_NUMPY_VERSION,
        "master_seed": REPLAY_MASTER_SEED,
        "supported_efs": list(SUPPORTED_EFS),
        "cell_id": population.cell.cell_id,
        "role_or_segment_id": population.calibration_role_manifest.role.role_or_segment_id,
        "workload_manifest_sha256": population.workload_manifest_sha256,
        "source_revision": source_revision,
        "query_order_seed": _seed_payload(query_order_seed),
        "block_count": len(blocks),
        "position_count": sum(len(block.positions) for block in blocks),
        "blocks": [_block_payload(block) for block in blocks],
    }


def build_response_profile_replay_schedule(
    *, population: CalibrationPopulationManifest, source_revision: str
) -> ResponseProfileReplaySchedule:
    """Build the exact NumPy-2.5.1 1,200-block/4,800-position schedule."""

    validated_population = verify_calibration_population_manifest(population)
    revision = _canonical_text(source_revision, field="source_revision")
    if np.__version__ != SCHEDULE_NUMPY_VERSION:
        raise _error(
            "NUMPY_VERSION_UNSUPPORTED",
            f"schedule requires NumPy {SCHEDULE_NUMPY_VERSION}",
        )
    cell_id = validated_population.cell.cell_id
    role_id = validated_population.calibration_role_manifest.role.role_or_segment_id
    workload_digest = validated_population.workload_manifest_sha256
    query_seed = _schedule_seed(
        (
            REPLAY_MASTER_SEED,
            cell_id,
            role_id,
            workload_digest,
            revision,
            "QUERY_ORDER",
        )
    )
    query_generator = np.random.Generator(np.random.PCG64(query_seed.seed_u64))
    query_order = tuple(
        int(index) for index in query_generator.permutation(CALIBRATION_QUERY_COUNT)
    )
    members = validated_population.calibration_role_manifest.members
    blocks: list[ReplayBlock] = []
    for block_index, canonical_query_index in enumerate(query_order):
        member = members[canonical_query_index]
        query_id = member.query_identity.query_id
        query_digest = member.query_identity.query_id_sha256
        ef_seed = _schedule_seed(
            (
                REPLAY_MASTER_SEED,
                cell_id,
                role_id,
                workload_digest,
                revision,
                "EF_ORDER",
                query_digest,
            )
        )
        ef_generator = np.random.Generator(np.random.PCG64(ef_seed.seed_u64))
        ef_indices = tuple(
            int(index) for index in ef_generator.permutation(len(SUPPORTED_EFS))
        )
        positions = tuple(
            _make(
                ReplayPosition,
                construction_token=_CONSTRUCTION_TOKEN,
                position_index=block_index * len(SUPPORTED_EFS) + within_block_index,
                block_index=block_index,
                within_block_index=within_block_index,
                canonical_query_index=canonical_query_index,
                query_id=query_id,
                query_id_sha256=query_digest,
                observation_identity_sha256=(
                    member.observation_identity.observation_identity_sha256
                ),
                ef=SUPPORTED_EFS[ef_index],
            )
            for within_block_index, ef_index in enumerate(ef_indices)
        )
        blocks.append(
            _make(
                ReplayBlock,
                construction_token=_CONSTRUCTION_TOKEN,
                block_index=block_index,
                canonical_query_index=canonical_query_index,
                query_id=query_id,
                query_id_sha256=query_digest,
                observation_identity_sha256=(
                    member.observation_identity.observation_identity_sha256
                ),
                ef_order_seed=ef_seed,
                positions=positions,
            )
        )
    block_tuple = tuple(blocks)
    payload = _replay_schedule_payload_from_parts(
        population=validated_population,
        source_revision=revision,
        query_order_seed=query_seed,
        blocks=block_tuple,
    )
    return _make(
        ResponseProfileReplaySchedule,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=REPLAY_SCHEDULE_SCHEMA_VERSION,
        algorithm_version=REPLAY_SCHEDULE_ALGORITHM_VERSION,
        numpy_version=SCHEDULE_NUMPY_VERSION,
        master_seed=REPLAY_MASTER_SEED,
        supported_efs=SUPPORTED_EFS,
        population=validated_population,
        source_revision=revision,
        query_order_seed=query_seed,
        blocks=block_tuple,
        replay_schedule_sha256=_digest(REPLAY_SCHEDULE_HASH_DOMAIN, payload),
    )  # type: ignore[return-value]


def verify_response_profile_replay_schedule(
    value: object,
    *,
    population: CalibrationPopulationManifest,
    source_revision: str,
) -> ResponseProfileReplaySchedule:
    if type(value) is not ResponseProfileReplaySchedule:
        raise _error("REPLAY_SCHEDULE_INVALID", "schedule must be concrete")
    try:
        rebuilt = build_response_profile_replay_schedule(
            population=population, source_revision=source_revision
        )
    except AttributeError as exc:
        raise _error("REPLAY_SCHEDULE_INVALID", "schedule is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("REPLAY_SCHEDULE_INVALID", "schedule failed complete recomputation")
    return rebuilt


def replay_schedule_payload(value: ResponseProfileReplaySchedule) -> dict[str, object]:
    if type(value) is not ResponseProfileReplaySchedule:
        raise _error("REPLAY_SCHEDULE_INVALID", "schedule must be concrete")
    validated = verify_response_profile_replay_schedule(
        value,
        population=value.population,
        source_revision=value.source_revision,
    )
    return _replay_schedule_payload_from_parts(
        population=validated.population,
        source_revision=validated.source_revision,
        query_order_seed=validated.query_order_seed,
        blocks=validated.blocks,
    )


def replay_schedule_document(value: ResponseProfileReplaySchedule) -> dict[str, object]:
    payload = replay_schedule_payload(value)
    digest = _sha256(value.replay_schedule_sha256, field="replay_schedule_sha256")
    recomputed = _digest(REPLAY_SCHEDULE_HASH_DOMAIN, payload)
    if not hmac.compare_digest(digest, recomputed):
        raise _error("REPLAY_SCHEDULE_INVALID", "schedule digest does not match payload")
    return {"replay_schedule_payload": payload, "replay_schedule_sha256": digest}
