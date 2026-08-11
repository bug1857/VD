"""R2-C semantic verification for response-profile lifecycle evidence.

The module interprets the four opaque R2-B evidence roles under ADR-009
R2-G.3.  It performs complete R2-A/R2-B reconstruction, exact-oracle binding,
threshold/result validation, and query-level recall/latency derivation.  Its
output is an integrity report and computed raw-evidence root only; neither is
qualification, freshness, policy, admission, grant, route, or actuation
authority.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from enum import StrEnum
import hashlib
import hmac
import json
import math
import re
import unicodedata

from .artifacts import canonical_json_bytes
from .config import IndexTrack, Metric, SearchConfiguration
from .lkg_window_readiness import parse_rfc3339_utc_instant, validate_rfc3339_utc
from .oracle import capped_threshold_recall, threshold_violations
from .response_profile import (
    OBSERVATION_COUNT,
    SUPPORTED_EFS,
    ResponseProfileEfObservation,
    ResponseProfileCalibrationEvidence,
    ResponseProfileIdentity,
    ResponseProfileQueryObservation,
    compute_response_profile_estimates,
)
from .response_profile_control import (
    ResponseProfileControl,
    verify_response_profile_control,
)
from .response_profile_evidence import (
    CalibrationPopulationManifest,
    ResponseProfileReplaySchedule,
    ResponseProfileRoleKind,
    ResponseProfileRoleManifest,
    verify_calibration_population_manifest,
    verify_response_profile_replay_schedule,
    verify_response_profile_role_manifest,
)
from .response_profile_lifecycle import (
    LifecycleEventKind,
    OpaqueEvidenceBlob,
    OpaqueEvidenceRole,
    ResponseProfileLifecycleEvent,
    ResponseProfileRunBinding,
    reduce_response_profile_lifecycle,
    verify_opaque_evidence_blob,
    verify_response_profile_lifecycle_event,
    verify_response_profile_run_binding,
)
from .search_configuration_digest import (
    search_configuration_document,
    search_configuration_sha256,
)


__all__ = [
    "ORACLE_RECORD_SCHEMA_VERSION",
    "ORACLE_MANIFEST_SCHEMA_VERSION",
    "WARMUP_EXECUTION_SCHEMA_VERSION",
    "RUNTIME_SNAPSHOT_SCHEMA_VERSION",
    "MEASURED_RESULT_SCHEMA_VERSION",
    "SEMANTIC_REPORT_SCHEMA_VERSION",
    "RAW_EVIDENCE_ROOT_SCHEMA_VERSION",
    "ResponseProfileSemanticError",
    "MeasuredResultOutcome",
    "RuntimeSnapshotPhase",
    "ResponseProfileOracleRecord",
    "ResponseProfileOracleManifest",
    "ResponseProfileSemanticExpectation",
    "ResponseProfileSemanticBundle",
    "ResponseProfileStaticIdentity",
    "ResponseProfileSemanticEncoder",
    "ResponseProfileSemanticReport",
    "ResponseProfileSemanticVerification",
    "build_response_profile_oracle_record",
    "build_response_profile_oracle_manifest",
    "build_response_profile_static_identity",
    "build_response_profile_identity_from_static",
    "build_response_profile_semantic_encoder",
    "build_response_profile_semantic_encoder_from_static",
    "oracle_record_document",
    "oracle_manifest_document",
    "encode_warmup_execution",
    "encode_runtime_snapshot",
    "encode_measured_result",
    "verify_response_profile_semantic_bundle",
    "semantic_report_payload",
    "response_profile_semantic_identity_payload",
    "raw_evidence_root_payload",
]


ORACLE_RECORD_SCHEMA_VERSION = "response-profile-oracle-record-v1"
ORACLE_MANIFEST_SCHEMA_VERSION = "response-profile-oracle-manifest-v1"
WARMUP_EXECUTION_SCHEMA_VERSION = "response-profile-warmup-execution-v1"
RUNTIME_SNAPSHOT_SCHEMA_VERSION = "response-profile-runtime-snapshot-v1"
MEASURED_RESULT_SCHEMA_VERSION = "response-profile-measured-result-v1"
SEMANTIC_REPORT_SCHEMA_VERSION = "response-profile-semantic-verification-v1"
RAW_EVIDENCE_ROOT_SCHEMA_VERSION = "response-profile-raw-evidence-root-v1"

ORACLE_RECORD_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_ORACLE_RECORD::V1\x00"
ORACLE_MANIFEST_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_ORACLE_MANIFEST::V1\x00"
WARMUP_EXECUTION_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_WARMUP_EXECUTION::V1\x00"
RUNTIME_SNAPSHOT_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_RUNTIME_SNAPSHOT::V1\x00"
MEASURED_RESULT_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_MEASURED_RESULT::V1\x00"
SEMANTIC_REPORT_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_SEMANTIC_VERIFICATION::V1\x00"
RAW_EVIDENCE_ROOT_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_RAW_EVIDENCE_ROOT::V1\x00"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_CONSTRUCTION_TOKEN = object()


class ResponseProfileSemanticError(ValueError):
    """Stable fail-closed R2-C error."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileSemanticError:
    return ResponseProfileSemanticError(message, code=code)


class MeasuredResultOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"


class RuntimeSnapshotPhase(StrEnum):
    PRE_BLOCK = "PRE_BLOCK"
    POST_BLOCK = "POST_BLOCK"


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileOracleRecord:
    observation_identity_sha256: str
    query_id_sha256: str
    query_payload_sha256: str
    limit: int
    full_count: int
    capped_ids: tuple[int, ...]
    capped_distances: tuple[float, ...]
    oracle_record_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("oracle records must be built by the contract factory")


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileOracleManifest:
    workload_manifest_sha256: str
    records: tuple[ResponseProfileOracleRecord, ...]
    oracle_manifest_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("oracle manifests must be built by the contract factory")


@dataclass(frozen=True, slots=True)
class ResponseProfileSemanticExpectation:
    profile_identity: ResponseProfileIdentity
    expected_oracle_manifest_sha256: str


@dataclass(frozen=True, slots=True)
class ResponseProfileSemanticBundle:
    calibration_population: CalibrationPopulationManifest
    warmup_role_manifest: ResponseProfileRoleManifest
    replay_schedule: ResponseProfileReplaySchedule
    run_binding: ResponseProfileRunBinding
    events: tuple[ResponseProfileLifecycleEvent, ...]
    opaque_evidence: tuple[OpaqueEvidenceBlob, ...]
    oracle_manifest: ResponseProfileOracleManifest
    control: ResponseProfileControl


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileStaticIdentity:
    """Pre-result identity projection; final calibration times are excluded."""

    metric: Metric
    threshold_stratum: str
    search_configurations: tuple[SearchConfiguration, ...]
    hnsw_index_identity: str
    data_identity: str
    workload_manifest_sha256: str
    ordered_query_payload_sha256: str
    replay_schedule_sha256: str
    control_profile_sha256: str
    environment_manifest_sha256: str
    source_revision: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("static identities must be built by the contract factory")


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileSemanticEncoder:
    """Producer helper that validates one immutable run context exactly once.

    The encoder is not verification authority. R2-C independently reconstructs
    the complete bundle even when its bytes were produced by this helper.
    """

    run_binding: ResponseProfileRunBinding
    identity: ResponseProfileStaticIdentity
    configurations: tuple[SearchConfiguration, ...]

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("semantic encoders must be built by the contract factory")

    def warmup_execution(self, *, epoch_index: int) -> bytes:
        return _encode_warmup_execution(
            binding=self.run_binding,
            identity=self.identity,
            configurations={item.ef: item for item in self.configurations},
            epoch_index=epoch_index,
        )

    def runtime_snapshot(
        self,
        *,
        epoch_index: int,
        block_index: int,
        phase: RuntimeSnapshotPhase,
        observed_at_utc: str,
        collection_loaded: bool = True,
        milvus_healthy: bool = True,
        etcd_healthy: bool = True,
        minio_healthy: bool = True,
    ) -> bytes:
        return _encode_runtime_snapshot(
            binding=self.run_binding,
            identity=self.identity,
            configurations={item.ef: item for item in self.configurations},
            epoch_index=epoch_index,
            block_index=block_index,
            phase=phase,
            observed_at_utc=observed_at_utc,
            collection_loaded=collection_loaded,
            milvus_healthy=milvus_healthy,
            etcd_healthy=etcd_healthy,
            minio_healthy=minio_healthy,
        )

    def measured_result(self, **values: object) -> bytes:
        return _encode_measured_result(
            binding=self.run_binding,
            identity=self.identity,
            configurations={item.ef: item for item in self.configurations},
            **values,
        )


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileSemanticReport:
    schema_version: str
    complete: bool
    reason_codes: tuple[str, ...]
    run_binding_sha256: str
    workload_manifest_sha256: str
    warmup_role_manifest_sha256: str
    replay_schedule_sha256: str
    oracle_manifest_sha256: str
    profile_identity_sha256: str
    event_count: int
    blob_count: int
    closed_block_count: int
    completed_position_count: int
    calibration_started_at_utc: str
    calibration_completed_at_utc: str
    observations: tuple[ResponseProfileQueryObservation, ...]
    semantic_report_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("semantic reports are verifier-issued")


@dataclass(frozen=True, slots=True)
class ResponseProfileSemanticVerification:
    report: ResponseProfileSemanticReport
    raw_evidence_sha256: str


def _new(cls: type[object], /, **values: object) -> object:
    value = object.__new__(cls)
    for name, item in values.items():
        object.__setattr__(value, name, item)
    return value


def _same_fields(actual: object, expected: object) -> bool:
    if type(actual) is not type(expected):
        return False
    if hasattr(type(actual), "__dataclass_fields__"):
        return all(
            _same_fields(getattr(actual, item.name), getattr(expected, item.name))
            for item in fields(actual)
        )
    if type(actual) is tuple:
        return len(actual) == len(expected) and all(  # type: ignore[arg-type]
            _same_fields(left, right)
            for left, right in zip(actual, expected, strict=True)  # type: ignore[arg-type]
        )
    return actual == expected


def _digest(domain: bytes, payload: Mapping[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(payload)).hexdigest()


def _sha256(value: object, *, field: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise _error("SHA256_INVALID", f"{field} must be lowercase SHA-256")
    return value


def _text(value: object, *, field: str) -> str:
    if type(value) is not str or not value or unicodedata.normalize("NFC", value) != value:
        raise _error("TEXT_INVALID", f"{field} must be non-empty NFC text")
    return value


def _integer(value: object, *, field: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise _error("INTEGER_INVALID", f"{field} must be an exact integer >= {minimum}")
    return value


def _finite_float(value: object, *, field: str) -> float:
    if type(value) is not float or not math.isfinite(value):
        raise _error("FLOAT_INVALID", f"{field} must be a finite exact float")
    return 0.0 if value == 0.0 else value


def _exact_mapping(value: object, *, expected: frozenset[str], field: str) -> Mapping[str, object]:
    if type(value) is not dict or frozenset(value) != expected:
        raise _error("DOCUMENT_INVALID", f"{field} fields are invalid")
    return value


class _DuplicateField(ValueError):
    pass


def _reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateField(key)
        result[key] = value
    return result


def _parse_document(value: object, *, field: str) -> dict[str, object]:
    if type(value) is not bytes or not value:
        raise _error("DOCUMENT_INVALID", f"{field} must be non-empty bytes")
    try:
        decoded = value.decode("utf-8")
        document = json.loads(decoded, object_pairs_hook=_reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateField) as exc:
        raise _error("DOCUMENT_INVALID", f"{field} is malformed JSON") from exc
    if type(document) is not dict or canonical_json_bytes(document) != value:
        raise _error("DOCUMENT_NONCANONICAL", f"{field} is not canonical JSON")
    return document


def _envelope_bytes(
    *, payload_name: str, digest_name: str, domain: bytes, payload: Mapping[str, object]
) -> bytes:
    digest = _digest(domain, payload)
    return canonical_json_bytes({payload_name: dict(payload), digest_name: digest})


def _parse_envelope(
    value: bytes,
    *,
    field: str,
    payload_name: str,
    digest_name: str,
    domain: bytes,
) -> dict[str, object]:
    document = _exact_mapping(
        _parse_document(value, field=field),
        expected=frozenset({payload_name, digest_name}),
        field=field,
    )
    payload = document[payload_name]
    if type(payload) is not dict:
        raise _error("DOCUMENT_INVALID", f"{field} payload must be an object")
    supplied = _sha256(document[digest_name], field=digest_name)
    expected = _digest(domain, payload)
    if not hmac.compare_digest(supplied, expected):
        raise _error("DIGEST_MISMATCH", f"{field} digest mismatch")
    return payload


def _ordered_result_valid(
    ids: tuple[int, ...], distances: tuple[float, ...], metric: Metric
) -> bool:
    if len(ids) != len(distances) or len(set(ids)) != len(ids):
        return False
    pairs = tuple(zip(ids, distances, strict=True))
    expected = tuple(
        sorted(pairs, key=(lambda item: (item[1], item[0])) if metric is Metric.L2 else (lambda item: (-item[1], item[0])))
    )
    return pairs == expected


def _oracle_payload(record: ResponseProfileOracleRecord) -> dict[str, object]:
    return {
        "schema_version": ORACLE_RECORD_SCHEMA_VERSION,
        "observation_identity_sha256": record.observation_identity_sha256,
        "query_id_sha256": record.query_id_sha256,
        "query_payload_sha256": record.query_payload_sha256,
        "limit": record.limit,
        "full_count": record.full_count,
        "capped_ids": list(record.capped_ids),
        "capped_distances": list(record.capped_distances),
    }


def build_response_profile_oracle_record(
    *,
    observation_identity_sha256: str,
    query_id_sha256: str,
    query_payload_sha256: str,
    limit: int,
    full_count: int,
    capped_ids: tuple[int, ...],
    capped_distances: tuple[float, ...],
    metric: Metric,
    radius: float,
    range_filter: float,
) -> ResponseProfileOracleRecord:
    observation = _sha256(observation_identity_sha256, field="observation_identity_sha256")
    query_id = _sha256(query_id_sha256, field="query_id_sha256")
    payload_digest = _sha256(query_payload_sha256, field="query_payload_sha256")
    limit_value = _integer(limit, field="limit", minimum=1)
    full = _integer(full_count, field="full_count")
    if type(metric) is not Metric:
        raise _error("ORACLE_INVALID", "metric must be concrete")
    radius_value = _finite_float(radius, field="radius")
    range_value = _finite_float(range_filter, field="range_filter")
    if type(capped_ids) is not tuple or type(capped_distances) is not tuple:
        raise _error("ORACLE_INVALID", "oracle results must be tuples")
    ids = tuple(_integer(item, field="capped_id") for item in capped_ids)
    distances = tuple(_finite_float(item, field="capped_distance") for item in capped_distances)
    if len(ids) != min(full, limit_value) or not _ordered_result_valid(ids, distances, metric):
        raise _error("ORACLE_INVALID", "oracle result count/order is invalid")
    if threshold_violations(distances, metric, radius=radius_value, range_filter=range_value):
        raise _error("ORACLE_INVALID", "oracle contains a threshold violation")
    provisional = _new(
        ResponseProfileOracleRecord,
        observation_identity_sha256=observation,
        query_id_sha256=query_id,
        query_payload_sha256=payload_digest,
        limit=limit_value,
        full_count=full,
        capped_ids=ids,
        capped_distances=distances,
        oracle_record_sha256="0" * 64,
    )
    digest = _digest(ORACLE_RECORD_HASH_DOMAIN, _oracle_payload(provisional))  # type: ignore[arg-type]
    return _new(
        ResponseProfileOracleRecord,
        observation_identity_sha256=observation,
        query_id_sha256=query_id,
        query_payload_sha256=payload_digest,
        limit=limit_value,
        full_count=full,
        capped_ids=ids,
        capped_distances=distances,
        oracle_record_sha256=digest,
    )  # type: ignore[return-value]


def _verify_oracle_record(
    value: object, *, member: object, metric: Metric
) -> ResponseProfileOracleRecord:
    if type(value) is not ResponseProfileOracleRecord:
        raise _error("ORACLE_INVALID", "oracle record must be concrete")
    try:
        payload_identity = member.query_payload_identity
        rebuilt = build_response_profile_oracle_record(
            observation_identity_sha256=member.observation_identity.observation_identity_sha256,
            query_id_sha256=member.query_identity.query_id_sha256,
            query_payload_sha256=payload_identity.query_payload_sha256,
            limit=value.limit,
            full_count=value.full_count,
            capped_ids=value.capped_ids,
            capped_distances=value.capped_distances,
            metric=metric,
            radius=payload_identity.radius,
            range_filter=payload_identity.range_filter,
        )
    except AttributeError as exc:
        raise _error("ORACLE_INVALID", "oracle record is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("ORACLE_INVALID", "oracle record failed reconstruction")
    return rebuilt


def oracle_record_document(value: ResponseProfileOracleRecord) -> dict[str, object]:
    if type(value) is not ResponseProfileOracleRecord:
        raise _error("ORACLE_INVALID", "oracle record must be concrete")
    payload = _oracle_payload(value)
    if not hmac.compare_digest(value.oracle_record_sha256, _digest(ORACLE_RECORD_HASH_DOMAIN, payload)):
        raise _error("ORACLE_INVALID", "oracle record digest mismatch")
    return {"oracle_record_payload": payload, "oracle_record_sha256": value.oracle_record_sha256}


def build_response_profile_oracle_manifest(
    *,
    population: CalibrationPopulationManifest,
    records: tuple[ResponseProfileOracleRecord, ...],
) -> ResponseProfileOracleManifest:
    verified_population = verify_calibration_population_manifest(population)
    if type(records) is not tuple or len(records) != OBSERVATION_COUNT:
        raise _error("ORACLE_MANIFEST_INVALID", "oracle manifest requires 1200 records")
    metric = verified_population.cell.metric
    verified = tuple(
        _verify_oracle_record(record, member=member, metric=metric)
        for record, member in zip(
            records,
            verified_population.calibration_role_manifest.members,
            strict=True,
        )
    )
    payload = {
        "schema_version": ORACLE_MANIFEST_SCHEMA_VERSION,
        "workload_manifest_sha256": verified_population.workload_manifest_sha256,
        "record_count": len(verified),
        "records": [oracle_record_document(item) for item in verified],
    }
    return _new(
        ResponseProfileOracleManifest,
        workload_manifest_sha256=verified_population.workload_manifest_sha256,
        records=verified,
        oracle_manifest_sha256=_digest(ORACLE_MANIFEST_HASH_DOMAIN, payload),
    )  # type: ignore[return-value]


def _verify_oracle_manifest(
    value: object, *, population: CalibrationPopulationManifest
) -> ResponseProfileOracleManifest:
    if type(value) is not ResponseProfileOracleManifest:
        raise _error("ORACLE_MANIFEST_INVALID", "oracle manifest must be concrete")
    try:
        rebuilt = build_response_profile_oracle_manifest(
            population=population, records=value.records
        )
    except AttributeError as exc:
        raise _error("ORACLE_MANIFEST_INVALID", "oracle manifest is malformed") from exc
    if not _same_fields(value, rebuilt):
        raise _error("ORACLE_MANIFEST_INVALID", "oracle manifest failed reconstruction")
    return rebuilt


def oracle_manifest_document(value: ResponseProfileOracleManifest) -> dict[str, object]:
    if type(value) is not ResponseProfileOracleManifest:
        raise _error("ORACLE_MANIFEST_INVALID", "oracle manifest must be concrete")
    payload = {
        "schema_version": ORACLE_MANIFEST_SCHEMA_VERSION,
        "workload_manifest_sha256": _sha256(
            value.workload_manifest_sha256, field="workload_manifest_sha256"
        ),
        "record_count": len(value.records),
        "records": [oracle_record_document(item) for item in value.records],
    }
    if not hmac.compare_digest(
        value.oracle_manifest_sha256,
        _digest(ORACLE_MANIFEST_HASH_DOMAIN, payload),
    ):
        raise _error("ORACLE_MANIFEST_INVALID", "oracle manifest digest mismatch")
    return {
        "oracle_manifest_payload": payload,
        "oracle_manifest_sha256": value.oracle_manifest_sha256,
    }


def _configuration_map(
    identity: ResponseProfileIdentity | ResponseProfileStaticIdentity,
) -> dict[int, SearchConfiguration]:
    if type(identity) not in {ResponseProfileIdentity, ResponseProfileStaticIdentity} or type(identity.search_configurations) is not tuple:
        raise _error("PROFILE_IDENTITY_INVALID", "profile identity must be concrete")
    configurations: dict[int, SearchConfiguration] = {}
    for configuration in identity.search_configurations:
        if type(configuration) is not SearchConfiguration:
            raise _error("PROFILE_IDENTITY_INVALID", "search configuration must be concrete")
        configuration.validate()
        if configuration.index_track is not IndexTrack.HNSW or configuration.ef not in SUPPORTED_EFS:
            raise _error("PROFILE_IDENTITY_INVALID", "profile requires supported HNSW configurations")
        configurations[configuration.ef] = configuration
    if tuple(configurations) != SUPPORTED_EFS:
        raise _error("PROFILE_IDENTITY_INVALID", "profile configuration order/family is invalid")
    documents = [search_configuration_document(configurations[ef]) for ef in SUPPORTED_EFS]
    first_common = {key: value for key, value in documents[0].items() if key != "ef"}
    if any(
        {key: value for key, value in document.items() if key != "ef"} != first_common
        for document in documents[1:]
    ):
        raise _error("PROFILE_IDENTITY_INVALID", "search configurations differ beyond ef")
    if (
        configurations[SUPPORTED_EFS[0]].metric is not identity.metric
        or configurations[SUPPORTED_EFS[0]].threshold_label != identity.threshold_stratum
    ):
        raise _error("PROFILE_IDENTITY_INVALID", "search configuration cell mismatch")
    return configurations


def _validate_member_search_semantics(
    *, member: object, configurations: Mapping[int, SearchConfiguration]
) -> None:
    try:
        payload = member.query_payload_identity
        reference = configurations[SUPPORTED_EFS[0]]
        if (
            payload.metric is not reference.metric
            or payload.threshold_stratum != reference.threshold_label
            or payload.radius != reference.radius
            or payload.range_filter != reference.range_filter
            or payload.limit != reference.limit
            or payload.consistency_level != reference.consistency_level
        ):
            raise _error("QUERY_PAYLOAD_MISMATCH", "query payload search semantics mismatch")
    except AttributeError as exc:
        raise _error("QUERY_PAYLOAD_MISMATCH", "query payload is malformed") from exc


def build_response_profile_static_identity(
    *,
    metric: Metric,
    threshold_stratum: str,
    search_configurations: tuple[SearchConfiguration, ...],
    hnsw_index_identity: str,
    data_identity: str,
    workload_manifest_sha256: str,
    ordered_query_payload_sha256: str,
    replay_schedule_sha256: str,
    control_profile_sha256: str,
    environment_manifest_sha256: str,
    source_revision: str,
) -> ResponseProfileStaticIdentity:
    value = _new(
        ResponseProfileStaticIdentity,
        metric=metric,
        threshold_stratum=threshold_stratum,
        search_configurations=search_configurations,
        hnsw_index_identity=hnsw_index_identity,
        data_identity=data_identity,
        workload_manifest_sha256=workload_manifest_sha256,
        ordered_query_payload_sha256=ordered_query_payload_sha256,
        replay_schedule_sha256=replay_schedule_sha256,
        control_profile_sha256=control_profile_sha256,
        environment_manifest_sha256=environment_manifest_sha256,
        source_revision=source_revision,
    )
    _static_identity_payload(value)  # type: ignore[arg-type]
    return value  # type: ignore[return-value]


def _static_identity_from_profile(
    identity: ResponseProfileIdentity,
) -> ResponseProfileStaticIdentity:
    _identity_payload(identity)
    return build_response_profile_static_identity(
        metric=identity.metric,
        threshold_stratum=identity.threshold_stratum,
        search_configurations=identity.search_configurations,
        hnsw_index_identity=identity.hnsw_index_identity,
        data_identity=identity.data_identity,
        workload_manifest_sha256=identity.workload_manifest_sha256,
        ordered_query_payload_sha256=identity.ordered_query_payload_sha256,
        replay_schedule_sha256=identity.replay_schedule_sha256,
        control_profile_sha256=identity.control_profile_sha256,
        environment_manifest_sha256=identity.environment_manifest_sha256,
        source_revision=identity.source_revision,
    )


def build_response_profile_identity_from_static(
    *,
    static_identity: ResponseProfileStaticIdentity,
    calibration_started_at_utc: str,
    calibration_completed_at_utc: str,
    generated_at_utc: str,
) -> ResponseProfileIdentity:
    static = _verify_static_identity(static_identity)
    identity = ResponseProfileIdentity(
        metric=static.metric,
        threshold_stratum=static.threshold_stratum,
        search_configurations=static.search_configurations,
        hnsw_index_identity=static.hnsw_index_identity,
        data_identity=static.data_identity,
        workload_manifest_sha256=static.workload_manifest_sha256,
        ordered_query_payload_sha256=static.ordered_query_payload_sha256,
        replay_schedule_sha256=static.replay_schedule_sha256,
        control_profile_sha256=static.control_profile_sha256,
        environment_manifest_sha256=static.environment_manifest_sha256,
        source_revision=static.source_revision,
        calibration_started_at_utc=calibration_started_at_utc,
        calibration_completed_at_utc=calibration_completed_at_utc,
        generated_at_utc=generated_at_utc,
    )
    _identity_payload(identity)
    return identity


def build_response_profile_semantic_encoder_from_static(
    *,
    run_binding: ResponseProfileRunBinding,
    static_identity: ResponseProfileStaticIdentity,
) -> ResponseProfileSemanticEncoder:
    binding = verify_response_profile_run_binding(run_binding)
    identity = _verify_static_identity(static_identity)
    configurations = _configuration_map(identity)
    for member in binding.population.calibration_role_manifest.members:
        _validate_member_search_semantics(
            member=member, configurations=configurations
        )
    for member in binding.warmup_role_manifest.members:
        _validate_member_search_semantics(
            member=member, configurations=configurations
        )
    return _new(
        ResponseProfileSemanticEncoder,
        run_binding=binding,
        identity=identity,
        configurations=tuple(configurations[ef] for ef in SUPPORTED_EFS),
    )  # type: ignore[return-value]


def build_response_profile_semantic_encoder(
    *, run_binding: ResponseProfileRunBinding, identity: ResponseProfileIdentity
) -> ResponseProfileSemanticEncoder:
    if type(identity) is not ResponseProfileIdentity:
        raise _error("PROFILE_IDENTITY_INVALID", "profile identity must be concrete")
    return build_response_profile_semantic_encoder_from_static(
        run_binding=run_binding,
        static_identity=_static_identity_from_profile(identity),
    )


def _static_identity_payload(
    identity: ResponseProfileStaticIdentity,
) -> dict[str, object]:
    configurations = _configuration_map(identity)
    if type(identity.metric) is not Metric:
        raise _error("PROFILE_IDENTITY_INVALID", "metric must be concrete")
    for value, name in (
        (identity.threshold_stratum, "threshold_stratum"),
        (identity.hnsw_index_identity, "hnsw_index_identity"),
        (identity.data_identity, "data_identity"),
        (identity.source_revision, "source_revision"),
    ):
        _text(value, field=name)
    for value, name in (
        (identity.workload_manifest_sha256, "workload_manifest_sha256"),
        (identity.ordered_query_payload_sha256, "ordered_query_payload_sha256"),
        (identity.replay_schedule_sha256, "replay_schedule_sha256"),
        (identity.control_profile_sha256, "control_profile_sha256"),
        (identity.environment_manifest_sha256, "environment_manifest_sha256"),
    ):
        _sha256(value, field=name)
    if (
        configurations[SUPPORTED_EFS[0]].metric is not identity.metric
        or configurations[SUPPORTED_EFS[0]].threshold_label
        != identity.threshold_stratum
    ):
        raise _error("PROFILE_IDENTITY_INVALID", "search configuration cell mismatch")
    return {
        "metric": identity.metric.value,
        "threshold_stratum": identity.threshold_stratum,
        "search_configurations": [
            search_configuration_document(configurations[ef])
            for ef in SUPPORTED_EFS
        ],
        "hnsw_index_identity": identity.hnsw_index_identity,
        "data_identity": identity.data_identity,
        "workload_manifest_sha256": identity.workload_manifest_sha256,
        "ordered_query_payload_sha256": identity.ordered_query_payload_sha256,
        "replay_schedule_sha256": identity.replay_schedule_sha256,
        "control_profile_sha256": identity.control_profile_sha256,
        "environment_manifest_sha256": identity.environment_manifest_sha256,
        "source_revision": identity.source_revision,
    }


def _verify_static_identity(
    identity: ResponseProfileStaticIdentity,
) -> ResponseProfileStaticIdentity:
    if type(identity) is not ResponseProfileStaticIdentity:
        raise _error("PROFILE_IDENTITY_INVALID", "static identity must be concrete")
    _static_identity_payload(identity)
    # Reconstruct with exact declared fields so object.__new__ forgeries cannot
    # bypass factory validation.  The local constructor call intentionally uses
    # canonical field values, not the serialized configuration documents.
    expected = _new(
        ResponseProfileStaticIdentity,
        metric=identity.metric,
        threshold_stratum=identity.threshold_stratum,
        search_configurations=tuple(identity.search_configurations),
        hnsw_index_identity=identity.hnsw_index_identity,
        data_identity=identity.data_identity,
        workload_manifest_sha256=identity.workload_manifest_sha256,
        ordered_query_payload_sha256=identity.ordered_query_payload_sha256,
        replay_schedule_sha256=identity.replay_schedule_sha256,
        control_profile_sha256=identity.control_profile_sha256,
        environment_manifest_sha256=identity.environment_manifest_sha256,
        source_revision=identity.source_revision,
    )
    if not _same_fields(identity, expected):
        raise _error("PROFILE_IDENTITY_INVALID", "static identity is noncanonical")
    return identity


def _identity_payload(identity: ResponseProfileIdentity) -> dict[str, object]:
    configurations = _configuration_map(identity)
    if type(identity.metric) is not Metric:
        raise _error("PROFILE_IDENTITY_INVALID", "metric must be concrete")
    for value, name in (
        (identity.hnsw_index_identity, "hnsw_index_identity"),
        (identity.data_identity, "data_identity"),
        (identity.source_revision, "source_revision"),
    ):
        _text(value, field=name)
    for value, name in (
        (identity.workload_manifest_sha256, "workload_manifest_sha256"),
        (identity.ordered_query_payload_sha256, "ordered_query_payload_sha256"),
        (identity.replay_schedule_sha256, "replay_schedule_sha256"),
        (identity.control_profile_sha256, "control_profile_sha256"),
        (identity.environment_manifest_sha256, "environment_manifest_sha256"),
    ):
        _sha256(value, field=name)
    for value, name in (
        (identity.calibration_started_at_utc, "calibration_started_at_utc"),
        (identity.calibration_completed_at_utc, "calibration_completed_at_utc"),
        (identity.generated_at_utc, "generated_at_utc"),
    ):
        validate_rfc3339_utc(value, field=name)
    if not parse_rfc3339_utc_instant(identity.calibration_started_at_utc) < parse_rfc3339_utc_instant(identity.calibration_completed_at_utc):
        raise _error("PROFILE_IDENTITY_INVALID", "calibration timestamps are not increasing")
    if parse_rfc3339_utc_instant(identity.generated_at_utc) < parse_rfc3339_utc_instant(identity.calibration_completed_at_utc):
        raise _error("PROFILE_IDENTITY_INVALID", "generated timestamp predates calibration")
    return {
        "metric": identity.metric.value,
        "threshold_stratum": identity.threshold_stratum,
        "search_configurations": [search_configuration_document(configurations[ef]) for ef in SUPPORTED_EFS],
        "hnsw_index_identity": identity.hnsw_index_identity,
        "data_identity": identity.data_identity,
        "workload_manifest_sha256": identity.workload_manifest_sha256,
        "ordered_query_payload_sha256": identity.ordered_query_payload_sha256,
        "replay_schedule_sha256": identity.replay_schedule_sha256,
        "control_profile_sha256": identity.control_profile_sha256,
        "environment_manifest_sha256": identity.environment_manifest_sha256,
        "source_revision": identity.source_revision,
        "calibration_started_at_utc": identity.calibration_started_at_utc,
        "calibration_completed_at_utc": identity.calibration_completed_at_utc,
        "generated_at_utc": identity.generated_at_utc,
    }


def response_profile_semantic_identity_payload(
    identity: ResponseProfileIdentity,
) -> dict[str, object]:
    """Return the strict R2 identity projection after complete validation."""

    return _identity_payload(identity)


def _common_runtime_payload(
    *,
    run_binding: ResponseProfileRunBinding,
    identity: ResponseProfileIdentity | ResponseProfileStaticIdentity,
    configurations: Mapping[int, SearchConfiguration],
) -> dict[str, object]:
    return {
        "run_binding_sha256": run_binding.run_binding_sha256,
        "metric": identity.metric.value,
        "threshold_stratum": identity.threshold_stratum,
        "control_profile_sha256": identity.control_profile_sha256,
        "environment_manifest_sha256": identity.environment_manifest_sha256,
        "hnsw_index_identity": identity.hnsw_index_identity,
        "data_identity": identity.data_identity,
        "source_revision": identity.source_revision,
        "search_configuration_sha256": [search_configuration_sha256(configurations[ef]) for ef in SUPPORTED_EFS],
    }


def _encode_warmup_execution(
    *,
    binding: ResponseProfileRunBinding,
    epoch_index: int,
    identity: ResponseProfileIdentity | ResponseProfileStaticIdentity,
    configurations: Mapping[int, SearchConfiguration],
) -> bytes:
    epoch = _integer(epoch_index, field="epoch_index")
    common = _common_runtime_payload(
        run_binding=binding, identity=identity, configurations=configurations
    )
    records: list[dict[str, object]] = []
    for member in binding.warmup_role_manifest.members:
        _validate_member_search_semantics(member=member, configurations=configurations)
        for ef in SUPPORTED_EFS:
            records.append(
                {
                    "observation_identity_sha256": member.observation_identity.observation_identity_sha256,
                    "query_id_sha256": member.query_identity.query_id_sha256,
                    "query_payload_sha256": member.query_payload_identity.query_payload_sha256,
                    "ef": ef,
                    "search_configuration_sha256": search_configuration_sha256(configurations[ef]),
                    "outcome": MeasuredResultOutcome.SUCCESS.value,
                }
            )
    payload = {
        "schema_version": WARMUP_EXECUTION_SCHEMA_VERSION,
        **common,
        "epoch_index": epoch,
        "warmup_role_manifest_sha256": binding.warmup_role_manifest_sha256,
        "execution_count": len(records),
        "executions": records,
    }
    return _envelope_bytes(
        payload_name="warmup_execution_payload",
        digest_name="warmup_execution_sha256",
        domain=WARMUP_EXECUTION_HASH_DOMAIN,
        payload=payload,
    )


def encode_warmup_execution(
    *, run_binding: ResponseProfileRunBinding, epoch_index: int, identity: ResponseProfileIdentity
) -> bytes:
    return build_response_profile_semantic_encoder(
        run_binding=run_binding, identity=identity
    ).warmup_execution(epoch_index=epoch_index)


def _encode_runtime_snapshot(
    *,
    binding: ResponseProfileRunBinding,
    identity: ResponseProfileIdentity | ResponseProfileStaticIdentity,
    configurations: Mapping[int, SearchConfiguration],
    epoch_index: int,
    block_index: int,
    phase: RuntimeSnapshotPhase,
    observed_at_utc: str,
    collection_loaded: bool = True,
    milvus_healthy: bool = True,
    etcd_healthy: bool = True,
    minio_healthy: bool = True,
) -> bytes:
    if type(phase) is not RuntimeSnapshotPhase:
        raise _error("RUNTIME_SNAPSHOT_INVALID", "snapshot phase must be concrete")
    validate_rfc3339_utc(observed_at_utc, field="observed_at_utc")
    for value, field in (
        (collection_loaded, "collection_loaded"),
        (milvus_healthy, "milvus_healthy"),
        (etcd_healthy, "etcd_healthy"),
        (minio_healthy, "minio_healthy"),
    ):
        if type(value) is not bool:
            raise _error("RUNTIME_SNAPSHOT_INVALID", f"{field} must be bool")
    payload = {
        "schema_version": RUNTIME_SNAPSHOT_SCHEMA_VERSION,
        **_common_runtime_payload(
            run_binding=binding, identity=identity, configurations=configurations
        ),
        "epoch_index": _integer(epoch_index, field="epoch_index"),
        "block_index": _integer(block_index, field="block_index"),
        "phase": phase.value,
        "observed_at_utc": observed_at_utc,
        "collection_loaded": collection_loaded,
        "milvus_healthy": milvus_healthy,
        "etcd_healthy": etcd_healthy,
        "minio_healthy": minio_healthy,
    }
    return _envelope_bytes(
        payload_name="runtime_snapshot_payload",
        digest_name="runtime_snapshot_sha256",
        domain=RUNTIME_SNAPSHOT_HASH_DOMAIN,
        payload=payload,
    )


def encode_runtime_snapshot(
    *,
    run_binding: ResponseProfileRunBinding,
    identity: ResponseProfileIdentity,
    epoch_index: int,
    block_index: int,
    phase: RuntimeSnapshotPhase,
    observed_at_utc: str,
    collection_loaded: bool = True,
    milvus_healthy: bool = True,
    etcd_healthy: bool = True,
    minio_healthy: bool = True,
) -> bytes:
    return build_response_profile_semantic_encoder(
        run_binding=run_binding, identity=identity
    ).runtime_snapshot(
        epoch_index=epoch_index,
        block_index=block_index,
        phase=phase,
        observed_at_utc=observed_at_utc,
        collection_loaded=collection_loaded,
        milvus_healthy=milvus_healthy,
        etcd_healthy=etcd_healthy,
        minio_healthy=minio_healthy,
    )


def _encode_measured_result(
    *,
    binding: ResponseProfileRunBinding,
    identity: ResponseProfileIdentity | ResponseProfileStaticIdentity,
    configurations: Mapping[int, SearchConfiguration],
    epoch_index: int,
    block_index: int,
    position_index: int,
    measurement_started_event_sha256: str,
    observation_identity_sha256: str,
    query_id_sha256: str,
    query_payload_sha256: str,
    ef: int,
    oracle_record_sha256: str,
    outcome: MeasuredResultOutcome,
    candidate_ids: tuple[int, ...],
    candidate_distances: tuple[float, ...],
    failure_code: str | None,
) -> bytes:
    if type(outcome) is not MeasuredResultOutcome:
        raise _error("MEASURED_RESULT_INVALID", "outcome must be concrete")
    if ef not in configurations:
        raise _error("MEASURED_RESULT_INVALID", "ef is unsupported")
    if type(candidate_ids) is not tuple or type(candidate_distances) is not tuple:
        raise _error("MEASURED_RESULT_INVALID", "candidate results must be tuples")
    ids = tuple(_integer(item, field="candidate_id") for item in candidate_ids)
    distances = tuple(_finite_float(item, field="candidate_distance") for item in candidate_distances)
    if outcome is MeasuredResultOutcome.SUCCESS:
        if failure_code is not None:
            raise _error("MEASURED_RESULT_INVALID", "successful result requires null failure code")
    else:
        if ids or distances or failure_code is None:
            raise _error("MEASURED_RESULT_INVALID", "failed result evidence is malformed")
        _text(failure_code, field="failure_code")
    payload = {
        "schema_version": MEASURED_RESULT_SCHEMA_VERSION,
        "run_binding_sha256": binding.run_binding_sha256,
        "epoch_index": _integer(epoch_index, field="epoch_index"),
        "block_index": _integer(block_index, field="block_index"),
        "position_index": _integer(position_index, field="position_index"),
        "measurement_started_event_sha256": _sha256(measurement_started_event_sha256, field="measurement_started_event_sha256"),
        "observation_identity_sha256": _sha256(observation_identity_sha256, field="observation_identity_sha256"),
        "query_id_sha256": _sha256(query_id_sha256, field="query_id_sha256"),
        "query_payload_sha256": _sha256(query_payload_sha256, field="query_payload_sha256"),
        "ef": ef,
        "search_configuration_sha256": search_configuration_sha256(configurations[ef]),
        "oracle_record_sha256": _sha256(oracle_record_sha256, field="oracle_record_sha256"),
        "outcome": outcome.value,
        "candidate_ids": list(ids),
        "candidate_distances": list(distances),
        "failure_code": failure_code,
    }
    return _envelope_bytes(
        payload_name="measured_result_payload",
        digest_name="measured_result_sha256",
        domain=MEASURED_RESULT_HASH_DOMAIN,
        payload=payload,
    )


def encode_measured_result(
    *,
    run_binding: ResponseProfileRunBinding,
    identity: ResponseProfileIdentity,
    epoch_index: int,
    block_index: int,
    position_index: int,
    measurement_started_event_sha256: str,
    observation_identity_sha256: str,
    query_id_sha256: str,
    query_payload_sha256: str,
    ef: int,
    oracle_record_sha256: str,
    outcome: MeasuredResultOutcome,
    candidate_ids: tuple[int, ...],
    candidate_distances: tuple[float, ...],
    failure_code: str | None,
) -> bytes:
    return build_response_profile_semantic_encoder(
        run_binding=run_binding, identity=identity
    ).measured_result(
        epoch_index=epoch_index,
        block_index=block_index,
        position_index=position_index,
        measurement_started_event_sha256=measurement_started_event_sha256,
        observation_identity_sha256=observation_identity_sha256,
        query_id_sha256=query_id_sha256,
        query_payload_sha256=query_payload_sha256,
        ef=ef,
        oracle_record_sha256=oracle_record_sha256,
        outcome=outcome,
        candidate_ids=candidate_ids,
        candidate_distances=candidate_distances,
        failure_code=failure_code,
    )


def _blob_map(blobs: tuple[OpaqueEvidenceBlob, ...]) -> dict[str, OpaqueEvidenceBlob]:
    result: dict[str, OpaqueEvidenceBlob] = {}
    for blob in blobs:
        verified = verify_opaque_evidence_blob(blob)
        if verified.opaque_evidence_sha256 in result:
            raise _error("EVIDENCE_REUSED", "opaque evidence digest is duplicated")
        result[verified.opaque_evidence_sha256] = verified
    return result


def _event_blob_digest(event: ResponseProfileLifecycleEvent) -> str | None:
    data = event.event_data
    if event.event_kind is LifecycleEventKind.WARMUP_COMPLETED:
        return data.warmup_execution_blob_sha256
    if event.event_kind is LifecycleEventKind.BLOCK_STARTED:
        return data.pre_block_runtime_snapshot_blob_sha256
    if event.event_kind is LifecycleEventKind.MEASUREMENT_COMPLETED:
        return data.measured_result_blob_sha256
    if event.event_kind is LifecycleEventKind.BLOCK_CLOSED:
        return data.post_block_runtime_snapshot_blob_sha256
    return None


def _runtime_payload_without_time(payload: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in payload.items() if key not in {"phase", "observed_at_utc"}}


def _semantic_report_payload_from_parts(
    *,
    run_binding_sha256: str,
    workload_manifest_sha256: str,
    warmup_role_manifest_sha256: str,
    replay_schedule_sha256: str,
    oracle_manifest_sha256: str,
    identity_sha256: str,
    event_count: int,
    blob_count: int,
    started_at: str,
    completed_at: str,
    observations: tuple[ResponseProfileQueryObservation, ...],
) -> dict[str, object]:
    return {
        "schema_version": SEMANTIC_REPORT_SCHEMA_VERSION,
        "complete": True,
        "reason_codes": [],
        "run_binding_sha256": run_binding_sha256,
        "workload_manifest_sha256": workload_manifest_sha256,
        "warmup_role_manifest_sha256": warmup_role_manifest_sha256,
        "replay_schedule_sha256": replay_schedule_sha256,
        "oracle_manifest_sha256": oracle_manifest_sha256,
        "profile_identity_sha256": identity_sha256,
        "event_count": event_count,
        "blob_count": blob_count,
        "closed_block_count": OBSERVATION_COUNT,
        "completed_position_count": OBSERVATION_COUNT * len(SUPPORTED_EFS),
        "calibration_started_at_utc": started_at,
        "calibration_completed_at_utc": completed_at,
        "observations": [
            {
                "query_id": item.query_id,
                "responses": [
                    {
                        "ef": response.ef,
                        "capped_recall": response.capped_recall,
                        "latency_ms": response.latency_ms,
                    }
                    for response in item.responses
                ],
            }
            for item in observations
        ],
    }


def semantic_report_payload(value: ResponseProfileSemanticReport) -> dict[str, object]:
    if type(value) is not ResponseProfileSemanticReport:
        raise _error("SEMANTIC_REPORT_INVALID", "semantic report must be concrete")
    if (
        value.schema_version != SEMANTIC_REPORT_SCHEMA_VERSION
        or value.complete is not True
        or value.reason_codes != ()
        or value.closed_block_count != OBSERVATION_COUNT
        or value.completed_position_count != OBSERVATION_COUNT * len(SUPPORTED_EFS)
        or len(value.observations) != OBSERVATION_COUNT
    ):
        raise _error("SEMANTIC_REPORT_INVALID", "semantic report constants are invalid")
    try:
        compute_response_profile_estimates(
            ResponseProfileCalibrationEvidence(
                raw_evidence_sha256="0" * 64,
                observations=value.observations,
            )
        )
    except (AttributeError, ResponseProfileContractError, TypeError, ValueError) as exc:
        raise _error("SEMANTIC_REPORT_INVALID", "semantic observations are invalid") from exc
    payload = _semantic_report_payload_from_parts(
        run_binding_sha256=_sha256(value.run_binding_sha256, field="run_binding_sha256"),
        workload_manifest_sha256=_sha256(value.workload_manifest_sha256, field="workload_manifest_sha256"),
        warmup_role_manifest_sha256=_sha256(value.warmup_role_manifest_sha256, field="warmup_role_manifest_sha256"),
        replay_schedule_sha256=_sha256(value.replay_schedule_sha256, field="replay_schedule_sha256"),
        oracle_manifest_sha256=_sha256(value.oracle_manifest_sha256, field="oracle_manifest_sha256"),
        identity_sha256=value.profile_identity_sha256,
        event_count=value.event_count,
        blob_count=value.blob_count,
        started_at=value.calibration_started_at_utc,
        completed_at=value.calibration_completed_at_utc,
        observations=value.observations,
    )
    if not hmac.compare_digest(value.semantic_report_sha256, _digest(SEMANTIC_REPORT_HASH_DOMAIN, payload)):
        raise _error("SEMANTIC_REPORT_INVALID", "semantic report digest mismatch")
    return payload


def raw_evidence_root_payload(
    *,
    report: ResponseProfileSemanticReport,
    identity: ResponseProfileIdentity,
    events: tuple[ResponseProfileLifecycleEvent, ...],
    blobs: tuple[OpaqueEvidenceBlob, ...],
) -> dict[str, object]:
    semantic_report_payload(report)
    if type(events) is not tuple or type(blobs) is not tuple:
        raise _error("RAW_EVIDENCE_ROOT_INVALID", "events and blobs must be tuples")
    verified_events = tuple(verify_response_profile_lifecycle_event(item) for item in events)
    verified_blobs = tuple(verify_opaque_evidence_blob(item) for item in blobs)
    if report.event_count != len(verified_events) or report.blob_count != len(verified_blobs):
        raise _error("RAW_EVIDENCE_ROOT_INVALID", "root counts do not match evidence")
    return {
        "schema_version": RAW_EVIDENCE_ROOT_SCHEMA_VERSION,
        "profile_identity": _identity_payload(identity),
        "semantic_report_sha256": report.semantic_report_sha256,
        "run_binding_sha256": report.run_binding_sha256,
        "workload_manifest_sha256": report.workload_manifest_sha256,
        "warmup_role_manifest_sha256": report.warmup_role_manifest_sha256,
        "replay_schedule_sha256": report.replay_schedule_sha256,
        "oracle_manifest_sha256": report.oracle_manifest_sha256,
        "lifecycle_event_sha256": [item.lifecycle_event_sha256 for item in verified_events],
        "opaque_evidence": [
            {
                "opaque_evidence_sha256": item.opaque_evidence_sha256,
                "evidence_bytes_sha256": item.evidence_bytes_sha256,
            }
            for item in verified_blobs
        ],
        "event_count": report.event_count,
        "blob_count": report.blob_count,
        "closed_block_count": report.closed_block_count,
        "completed_position_count": report.completed_position_count,
        "calibration_started_at_utc": report.calibration_started_at_utc,
        "calibration_completed_at_utc": report.calibration_completed_at_utc,
    }


def verify_response_profile_semantic_bundle(
    *, bundle: ResponseProfileSemanticBundle, expectation: ResponseProfileSemanticExpectation
) -> ResponseProfileSemanticVerification:
    if type(bundle) is not ResponseProfileSemanticBundle or type(expectation) is not ResponseProfileSemanticExpectation:
        raise _error("SEMANTIC_BUNDLE_INVALID", "bundle and expectation must be concrete")
    population = verify_calibration_population_manifest(bundle.calibration_population)
    warmup = verify_response_profile_role_manifest(bundle.warmup_role_manifest)
    schedule = verify_response_profile_replay_schedule(
        bundle.replay_schedule,
        population=population,
        source_revision=bundle.run_binding.source_revision,
    )
    binding = verify_response_profile_run_binding(bundle.run_binding)
    if warmup.role.kind is not ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP:
        raise _error("SEMANTIC_BUNDLE_INVALID", "warm-up manifest role is invalid")
    if (
        binding.population.workload_manifest_sha256 != population.workload_manifest_sha256
        or binding.replay_schedule.replay_schedule_sha256 != schedule.replay_schedule_sha256
        or binding.warmup_role_manifest.role_manifest_sha256 != warmup.role_manifest_sha256
    ):
        raise _error("SEMANTIC_BUNDLE_INVALID", "run binding components mismatch")
    identity = expectation.profile_identity
    identity_payload = _identity_payload(identity)
    configurations = _configuration_map(identity)
    identity_sha256 = hashlib.sha256(canonical_json_bytes(identity_payload)).hexdigest()
    if (
        identity.metric is not population.cell.metric
        or identity.threshold_stratum != population.cell.threshold_stratum
        or identity.workload_manifest_sha256 != population.workload_manifest_sha256
        or identity.ordered_query_payload_sha256 != population.ordered_query_payload_sha256
        or identity.replay_schedule_sha256 != schedule.replay_schedule_sha256
        or identity.source_revision != schedule.source_revision
        or binding.source_revision != identity.source_revision
    ):
        raise _error("PROFILE_IDENTITY_MISMATCH", "profile identity does not match R2-A/R2-B")
    try:
        control = verify_response_profile_control(bundle.control)
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("CONTROL_PROFILE_INVALID", "response-profile control is invalid") from exc
    if (
        control.control_profile_sha256 != identity.control_profile_sha256
        or control.calibration_population_sha256 != population.workload_manifest_sha256
        or control.warmup_role_manifest_sha256 != warmup.role_manifest_sha256
        or control.ordered_query_payload_sha256 != population.ordered_query_payload_sha256
        or control.replay_schedule_sha256 != schedule.replay_schedule_sha256
        or control.environment_manifest_sha256 != identity.environment_manifest_sha256
        or control.source_revision != identity.source_revision
        or control.stream_key.metric is not identity.metric
        or control.stream_key.threshold_stratum != identity.threshold_stratum
        or control.stream_key.data_identity != identity.data_identity
        or control.stream_key.hnsw_binding_id != identity.hnsw_index_identity
    ):
        raise _error("CONTROL_PROFILE_MISMATCH", "control does not match profile evidence")
    if parse_rfc3339_utc_instant(control.frozen_at_utc) >= parse_rfc3339_utc_instant(
        identity.calibration_started_at_utc
    ):
        raise _error(
            "CONTROL_PROFILE_LATE",
            "control was not frozen strictly before calibration began",
        )
    for member in population.calibration_role_manifest.members:
        _validate_member_search_semantics(member=member, configurations=configurations)
    for member in warmup.members:
        _validate_member_search_semantics(member=member, configurations=configurations)
    encoder = build_response_profile_semantic_encoder_from_static(
        run_binding=binding,
        static_identity=_static_identity_from_profile(identity),
    )
    oracle_manifest = _verify_oracle_manifest(bundle.oracle_manifest, population=population)
    expected_oracle = _sha256(expectation.expected_oracle_manifest_sha256, field="expected_oracle_manifest_sha256")
    if not hmac.compare_digest(expected_oracle, oracle_manifest.oracle_manifest_sha256):
        raise _error("ORACLE_ROOT_MISMATCH", "oracle manifest does not match independent expectation")
    if type(bundle.events) is not tuple or type(bundle.opaque_evidence) is not tuple:
        raise _error("SEMANTIC_BUNDLE_INVALID", "events and blobs must be tuples")
    events = tuple(verify_response_profile_lifecycle_event(item) for item in bundle.events)
    blobs = tuple(verify_opaque_evidence_blob(item) for item in bundle.opaque_evidence)
    lifecycle = reduce_response_profile_lifecycle(
        run_binding=binding,
        events=events,
        opaque_evidence=blobs,
        recovery_boundary=False,
    )
    if lifecycle.mechanically_invalid or not lifecycle.structurally_complete:
        raise _error("LIFECYCLE_INCOMPLETE", "R2-B lifecycle is not complete and valid")
    blob_map = _blob_map(blobs)
    referenced = [digest for event in events if (digest := _event_blob_digest(event)) is not None]
    if len(referenced) != len(set(referenced)) or set(referenced) != set(blob_map):
        raise _error("EVIDENCE_REFERENCE_INVALID", "opaque evidence references are not one-to-one")

    oracle_by_observation = {
        item.observation_identity_sha256: item for item in oracle_manifest.records
    }
    member_by_observation = {
        item.observation_identity.observation_identity_sha256: item
        for item in population.calibration_role_manifest.members
    }
    derived: dict[int, dict[int, ResponseProfileEfObservation]] = {
        index: {} for index in range(OBSERVATION_COUNT)
    }
    started_by_digest: dict[str, ResponseProfileLifecycleEvent] = {}
    pre_by_block: dict[int, Mapping[str, object]] = {}
    first_started_at: str | None = None
    last_completed_at: str | None = None

    for event in events:
        digest = _event_blob_digest(event)
        blob = blob_map.get(digest) if digest is not None else None
        if event.event_kind is LifecycleEventKind.WARMUP_COMPLETED:
            if blob is None or blob.evidence_role is not OpaqueEvidenceRole.WARMUP_EXECUTION:
                raise _error("WARMUP_INVALID", "warm-up blob is missing or wrong-role")
            expected_bytes = encoder.warmup_execution(epoch_index=event.epoch_index)
            if not hmac.compare_digest(blob.evidence_bytes, expected_bytes):
                raise _error("WARMUP_INVALID", "warm-up execution evidence mismatch")
        elif event.event_kind is LifecycleEventKind.BLOCK_STARTED:
            if blob is None or blob.evidence_role is not OpaqueEvidenceRole.PRE_BLOCK_RUNTIME_SNAPSHOT:
                raise _error("RUNTIME_SNAPSHOT_INVALID", "pre-block snapshot is missing or wrong-role")
            payload = _parse_envelope(
                blob.evidence_bytes,
                field="pre_block_runtime_snapshot",
                payload_name="runtime_snapshot_payload",
                digest_name="runtime_snapshot_sha256",
                domain=RUNTIME_SNAPSHOT_HASH_DOMAIN,
            )
            observed_at = validate_rfc3339_utc(payload.get("observed_at_utc"), field="observed_at_utc")
            expected_bytes = encoder.runtime_snapshot(
                epoch_index=event.epoch_index,
                block_index=event.block_index,
                phase=RuntimeSnapshotPhase.PRE_BLOCK,
                observed_at_utc=observed_at,
            )
            if not hmac.compare_digest(blob.evidence_bytes, expected_bytes):
                raise _error("RUNTIME_SNAPSHOT_INVALID", "pre-block runtime snapshot mismatch")
            pre_by_block[event.block_index] = payload
        elif event.event_kind is LifecycleEventKind.MEASUREMENT_STARTED:
            started_by_digest[event.lifecycle_event_sha256] = event
            if first_started_at is None:
                first_started_at = event.recorded_at_utc
        elif event.event_kind is LifecycleEventKind.MEASUREMENT_COMPLETED:
            if blob is None or blob.evidence_role is not OpaqueEvidenceRole.MEASURED_RESULT:
                raise _error("MEASURED_RESULT_INVALID", "measured result is missing or wrong-role")
            started = started_by_digest.get(event.event_data.measurement_started_event_sha256)
            if started is None:
                raise _error("MEASURED_RESULT_INVALID", "matching STARTED event is unavailable")
            payload = _parse_envelope(
                blob.evidence_bytes,
                field="measured_result",
                payload_name="measured_result_payload",
                digest_name="measured_result_sha256",
                domain=MEASURED_RESULT_HASH_DOMAIN,
            )
            try:
                outcome = MeasuredResultOutcome(payload["outcome"])
            except (KeyError, ValueError, TypeError) as exc:
                raise _error("MEASURED_RESULT_INVALID", "measured outcome is invalid") from exc
            if outcome is not MeasuredResultOutcome.SUCCESS:
                raise _error("MEASURED_SEARCH_FAILED", "measured search did not succeed")
            member = member_by_observation.get(started.event_data.observation_identity_sha256)
            oracle = oracle_by_observation.get(started.event_data.observation_identity_sha256)
            if member is None or oracle is None:
                raise _error("MEASURED_RESULT_INVALID", "measured identity is not in population/oracle")
            ids_raw = payload.get("candidate_ids")
            distances_raw = payload.get("candidate_distances")
            if type(ids_raw) is not list or type(distances_raw) is not list:
                raise _error("MEASURED_RESULT_INVALID", "candidate result arrays are invalid")
            ids = tuple(_integer(item, field="candidate_id") for item in ids_raw)
            distances = tuple(_finite_float(item, field="candidate_distance") for item in distances_raw)
            query_payload_identity = member.query_payload_identity
            if (
                len(ids) > query_payload_identity.limit
                or not _ordered_result_valid(ids, distances, identity.metric)
                or threshold_violations(
                    distances,
                    identity.metric,
                    radius=query_payload_identity.radius,
                    range_filter=query_payload_identity.range_filter,
                )
            ):
                raise _error("MEASURED_RESULT_INVALID", "candidate result semantics are invalid")
            expected_bytes = encoder.measured_result(
                epoch_index=event.epoch_index,
                block_index=event.block_index,
                position_index=event.position_index,
                measurement_started_event_sha256=started.lifecycle_event_sha256,
                observation_identity_sha256=started.event_data.observation_identity_sha256,
                query_id_sha256=started.event_data.query_id_sha256,
                query_payload_sha256=query_payload_identity.query_payload_sha256,
                ef=started.event_data.ef,
                oracle_record_sha256=oracle.oracle_record_sha256,
                outcome=outcome,
                candidate_ids=ids,
                candidate_distances=distances,
                failure_code=None,
            )
            if not hmac.compare_digest(blob.evidence_bytes, expected_bytes):
                raise _error("MEASURED_RESULT_INVALID", "measured result binding mismatch")
            latency_ms = (event.event_data.completed_monotonic_ns - started.event_data.started_monotonic_ns) / 1_000_000.0
            if not math.isfinite(latency_ms) or latency_ms <= 0.0:
                raise _error("LATENCY_INVALID", "derived latency is invalid")
            recall = capped_threshold_recall(ids, oracle.capped_ids)
            query_index = started.event_data.canonical_query_index
            ef = started.event_data.ef
            if ef in derived[query_index]:
                raise _error("MEASURED_RESULT_DUPLICATE", "query/ef observation is duplicated")
            derived[query_index][ef] = ResponseProfileEfObservation(
                ef=ef, capped_recall=float(recall), latency_ms=float(latency_ms)
            )
            last_completed_at = event.recorded_at_utc
        elif event.event_kind is LifecycleEventKind.BLOCK_CLOSED:
            if blob is None or blob.evidence_role is not OpaqueEvidenceRole.POST_BLOCK_RUNTIME_SNAPSHOT:
                raise _error("RUNTIME_SNAPSHOT_INVALID", "post-block snapshot is missing or wrong-role")
            payload = _parse_envelope(
                blob.evidence_bytes,
                field="post_block_runtime_snapshot",
                payload_name="runtime_snapshot_payload",
                digest_name="runtime_snapshot_sha256",
                domain=RUNTIME_SNAPSHOT_HASH_DOMAIN,
            )
            observed_at = validate_rfc3339_utc(payload.get("observed_at_utc"), field="observed_at_utc")
            expected_bytes = encoder.runtime_snapshot(
                epoch_index=event.epoch_index,
                block_index=event.block_index,
                phase=RuntimeSnapshotPhase.POST_BLOCK,
                observed_at_utc=observed_at,
            )
            if not hmac.compare_digest(blob.evidence_bytes, expected_bytes):
                raise _error("RUNTIME_SNAPSHOT_INVALID", "post-block runtime snapshot mismatch")
            pre = pre_by_block.get(event.block_index)
            if pre is None or _runtime_payload_without_time(pre) != _runtime_payload_without_time(payload):
                raise _error("RUNTIME_SNAPSHOT_INVALID", "PRE/POST runtime snapshots disagree")

    if first_started_at is None or last_completed_at is None:
        raise _error("SEMANTIC_BUNDLE_INCOMPLETE", "measurement timestamps are missing")
    if (
        first_started_at != identity.calibration_started_at_utc
        or last_completed_at != identity.calibration_completed_at_utc
    ):
        raise _error("CALIBRATION_TIMESTAMP_MISMATCH", "R1 calibration timestamps do not match lifecycle")
    observations: list[ResponseProfileQueryObservation] = []
    for index, member in enumerate(population.calibration_role_manifest.members):
        by_ef = derived[index]
        if tuple(sorted(by_ef)) != SUPPORTED_EFS:
            raise _error("SEMANTIC_BUNDLE_INCOMPLETE", "query is missing supported ef observations")
        observations.append(
            ResponseProfileQueryObservation(
                query_id=member.query_identity.query_id,
                responses=tuple(by_ef[ef] for ef in SUPPORTED_EFS),
            )
        )
    observation_tuple = tuple(observations)
    report_payload = _semantic_report_payload_from_parts(
        run_binding_sha256=binding.run_binding_sha256,
        workload_manifest_sha256=binding.workload_manifest_sha256,
        warmup_role_manifest_sha256=binding.warmup_role_manifest_sha256,
        replay_schedule_sha256=binding.replay_schedule_sha256,
        oracle_manifest_sha256=oracle_manifest.oracle_manifest_sha256,
        identity_sha256=identity_sha256,
        event_count=len(events),
        blob_count=len(blobs),
        started_at=first_started_at,
        completed_at=last_completed_at,
        observations=observation_tuple,
    )
    report = _new(
        ResponseProfileSemanticReport,
        schema_version=SEMANTIC_REPORT_SCHEMA_VERSION,
        complete=True,
        reason_codes=(),
        run_binding_sha256=binding.run_binding_sha256,
        workload_manifest_sha256=binding.workload_manifest_sha256,
        warmup_role_manifest_sha256=binding.warmup_role_manifest_sha256,
        replay_schedule_sha256=binding.replay_schedule_sha256,
        oracle_manifest_sha256=oracle_manifest.oracle_manifest_sha256,
        profile_identity_sha256=identity_sha256,
        event_count=len(events),
        blob_count=len(blobs),
        closed_block_count=OBSERVATION_COUNT,
        completed_position_count=OBSERVATION_COUNT * len(SUPPORTED_EFS),
        calibration_started_at_utc=first_started_at,
        calibration_completed_at_utc=last_completed_at,
        observations=observation_tuple,
        semantic_report_sha256=_digest(SEMANTIC_REPORT_HASH_DOMAIN, report_payload),
    )
    root_payload = raw_evidence_root_payload(
        report=report, identity=identity, events=events, blobs=blobs  # type: ignore[arg-type]
    )
    return ResponseProfileSemanticVerification(
        report=report,  # type: ignore[arg-type]
        raw_evidence_sha256=_digest(RAW_EVIDENCE_ROOT_HASH_DOMAIN, root_payload),
    )
