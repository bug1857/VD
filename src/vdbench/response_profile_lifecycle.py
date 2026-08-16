"""Pure immutable R2-B1 response-profile lifecycle contracts.

Purpose:
    Bind an R2-A calibration population, replay schedule, and warm-up role to
    one run; build canonical lifecycle events and opaque evidence descriptors;
    and mechanically replay their immutable sequence.  The reducer derives all
    progress and validity from the supplied events.  No mutable counter or
    caller assertion is authority.

Boundary:
    Opaque bytes are checked only for exact type, length, role, SHA-256, and
    event binding.  Their meaning belongs to R2-C.  This module performs no
    persistence, locking, statistics, result interpretation, policy,
    qualification, authorization, routing, Milvus, or actuation work.

Crash semantics:
    A durable STARTED record necessarily exists briefly while its search is in
    flight.  Therefore callers must state whether reduction occurs at a
    recovery boundary.  An active prefix may end inside a block; the identical
    persisted prefix is terminal when ``recovery_boundary=True``.  This flag is
    evaluation context only and is never part of a canonical event.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from enum import StrEnum

from .artifacts import canonical_json_bytes
from .response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    MEASURED_POSITION_COUNT,
    WARMUP_QUERY_COUNT,
    CalibrationPopulationManifest,
    ResponseProfileReplaySchedule,
    ResponseProfileRoleKind,
    ResponseProfileRoleManifest,
    canonical_response_profile_query_id,
    validate_role_manifest_disjointness,
    verify_calibration_population_manifest,
    verify_response_profile_replay_schedule,
    verify_response_profile_role_manifest,
)

__all__ = [
    "LIFECYCLE_EVENT_HASH_DOMAIN",
    "LIFECYCLE_EVENT_SCHEMA_VERSION",
    "OPAQUE_EVIDENCE_HASH_DOMAIN",
    "OPAQUE_EVIDENCE_SCHEMA_VERSION",
    "RUN_BINDING_HASH_DOMAIN",
    "RUN_BINDING_SCHEMA_VERSION",
    "LifecycleEventKind",
    "OpaqueEvidenceBlob",
    "OpaqueEvidenceRole",
    "ResponseProfileLifecycleContractError",
    "ResponseProfileLifecycleEvent",
    "ResponseProfileLifecycleSnapshot",
    "ResponseProfileRunBinding",
    "apply_next_lifecycle_event",
    "build_opaque_evidence_blob",
    "build_response_profile_lifecycle_event",
    "build_response_profile_run_binding",
    "initial_lifecycle_reducer_state",
    "opaque_evidence_descriptor_payload",
    "opaque_evidence_document",
    "reduce_response_profile_lifecycle",
    "response_profile_lifecycle_event_document",
    "response_profile_lifecycle_event_payload",
    "response_profile_run_binding_document",
    "response_profile_run_binding_payload",
    "verify_opaque_evidence_blob",
    "verify_response_profile_lifecycle_event",
    "verify_response_profile_run_binding",
]


RUN_BINDING_SCHEMA_VERSION = "response-profile-lifecycle-run-binding-v1"
OPAQUE_EVIDENCE_SCHEMA_VERSION = "response-profile-opaque-evidence-blob-v1"
LIFECYCLE_EVENT_SCHEMA_VERSION = "response-profile-lifecycle-event-v1"

RUN_BINDING_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_LIFECYCLE_RUN_BINDING::V1\x00"
OPAQUE_EVIDENCE_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_OPAQUE_EVIDENCE_BLOB::V1\x00"
LIFECYCLE_EVENT_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_LIFECYCLE_EVENT::V1\x00"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?Z\Z"
)
_CONSTRUCTION_TOKEN = object()


class ResponseProfileLifecycleContractError(ValueError):
    """Stable fail-closed R2-B1 contract error."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileLifecycleContractError:
    return ResponseProfileLifecycleContractError(message, code=code)


class OpaqueEvidenceRole(StrEnum):
    """The exact closed R2-G.2 opaque evidence role catalog."""

    WARMUP_EXECUTION = "WARMUP_EXECUTION"
    MEASURED_RESULT = "MEASURED_RESULT"
    PRE_BLOCK_RUNTIME_SNAPSHOT = "PRE_BLOCK_RUNTIME_SNAPSHOT"
    POST_BLOCK_RUNTIME_SNAPSHOT = "POST_BLOCK_RUNTIME_SNAPSHOT"


class LifecycleEventKind(StrEnum):
    """The exact closed R2-G.2 lifecycle event catalog."""

    EPOCH_STARTED = "EPOCH_STARTED"
    WARMUP_COMPLETED = "WARMUP_COMPLETED"
    BLOCK_STARTED = "BLOCK_STARTED"
    MEASUREMENT_STARTED = "MEASUREMENT_STARTED"
    MEASUREMENT_COMPLETED = "MEASUREMENT_COMPLETED"
    BLOCK_CLOSED = "BLOCK_CLOSED"
    RUN_SEALED = "RUN_SEALED"
    RUN_INVALIDATED = "RUN_INVALIDATED"


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileRunBinding:
    """One fully reconstructable R2-A population/schedule/warm-up binding."""

    schema_version: str
    run_id: str
    created_at_utc: str
    cell_id: str
    workload_manifest_sha256: str
    replay_schedule_sha256: str
    warmup_role_manifest_sha256: str
    source_revision: str
    population: CalibrationPopulationManifest
    replay_schedule: ResponseProfileReplaySchedule
    warmup_role_manifest: ResponseProfileRoleManifest
    run_binding_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ResponseProfileRunBinding must be built by its contract factory")


@dataclass(frozen=True, slots=True, init=False)
class OpaqueEvidenceBlob:
    """One immutable byte value plus its governed detached descriptor.

    ``evidence_bytes`` is storage material, not a field in the canonical
    descriptor.  The descriptor binds it through exact length and SHA-256.
    """

    schema_version: str
    run_binding_sha256: str
    event_seq: int
    evidence_role: OpaqueEvidenceRole
    byte_length: int
    evidence_bytes_sha256: str
    opaque_evidence_sha256: str
    evidence_bytes: bytes

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("OpaqueEvidenceBlob must be built from exact evidence bytes")


@dataclass(frozen=True, slots=True)
class _EpochStartedData:
    pass


@dataclass(frozen=True, slots=True)
class _WarmupCompletedData:
    warmup_role_manifest_sha256: str
    warmup_execution_blob_sha256: str


@dataclass(frozen=True, slots=True)
class _BlockStartedData:
    pre_block_runtime_snapshot_blob_sha256: str


@dataclass(frozen=True, slots=True)
class _MeasurementStartedData:
    within_block_index: int
    canonical_query_index: int
    query_id: int | str
    query_id_sha256: str
    observation_identity_sha256: str
    ef: int
    started_monotonic_ns: int


@dataclass(frozen=True, slots=True)
class _MeasurementCompletedData:
    measurement_started_event_sha256: str
    measured_result_blob_sha256: str
    completed_monotonic_ns: int


@dataclass(frozen=True, slots=True)
class _BlockClosedData:
    block_started_event_sha256: str
    measurement_completed_event_sha256: tuple[str, ...]
    post_block_runtime_snapshot_blob_sha256: str


@dataclass(frozen=True, slots=True)
class _RunSealedData:
    pass


@dataclass(frozen=True, slots=True)
class _RunInvalidatedData:
    reason_code: str


_LifecycleEventData = (
    _EpochStartedData
    | _WarmupCompletedData
    | _BlockStartedData
    | _MeasurementStartedData
    | _MeasurementCompletedData
    | _BlockClosedData
    | _RunSealedData
    | _RunInvalidatedData
)


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileLifecycleEvent:
    """One immutable canonical event in the run-wide hash chain."""

    schema_version: str
    run_binding_sha256: str
    event_seq: int
    event_kind: LifecycleEventKind
    epoch_index: int | None
    block_index: int | None
    position_index: int | None
    recorded_at_utc: str
    event_data: _LifecycleEventData
    previous_event_sha256: str
    lifecycle_event_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "ResponseProfileLifecycleEvent must be built by its contract factory"
        )


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileLifecycleSnapshot:
    """Mechanically derived lifecycle state; never a persisted authority."""

    run_binding_sha256: str
    event_count: int
    last_event_sha256: str
    current_epoch_index: int | None
    warmup_completed_in_current_epoch: bool
    open_block_index: int | None
    open_measurement_position_index: int | None
    closed_block_count: int
    completed_position_count: int
    seen_epoch_indexes: tuple[int, ...]
    run_sealed_event_count: int
    run_invalidated_event_count: int
    evaluated_at_recovery_boundary: bool
    requires_fresh_epoch_after_recovery: bool
    structurally_complete: bool
    mechanically_invalid: bool
    reason_codes: tuple[str, ...]

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ResponseProfileLifecycleSnapshot is reducer-derived")


def _new(cls: type[object], /, **values: object) -> object:
    instance = object.__new__(cls)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


def _make(cls: type[object], *, construction_token: object, **values: object) -> object:
    if construction_token is not _CONSTRUCTION_TOKEN:
        raise TypeError("response-profile lifecycle construction token is invalid")
    return _new(cls, **values)


def _type_exact_equal(actual: object, expected: object) -> bool:
    if type(actual) is not type(expected):
        return False
    if type(expected) is tuple:
        return len(actual) == len(expected) and all(  # type: ignore[arg-type]
            _type_exact_equal(left, right)
            for left, right in zip(actual, expected, strict=True)  # type: ignore[arg-type]
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


def _exact_int(
    value: object,
    *,
    field: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if type(value) is not int:
        raise _error("INTEGER_INVALID", f"{field} must be an exact integer")
    if minimum is not None and value < minimum:
        raise _error("INTEGER_INVALID", f"{field} is below its minimum")
    if maximum is not None and value > maximum:
        raise _error("INTEGER_INVALID", f"{field} exceeds its maximum")
    return value


def _optional_int(
    value: object,
    *,
    field: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    if value is None:
        return None
    return _exact_int(value, field=field, minimum=minimum, maximum=maximum)


def _rfc3339_utc(value: object, *, field: str) -> str:
    if type(value) is not str or _RFC3339_UTC_RE.fullmatch(value) is None:
        raise _error("TIMESTAMP_INVALID", f"{field} must be strict RFC3339 UTC")
    try:
        instant = datetime.fromisoformat(value)
    except ValueError as exc:
        raise _error("TIMESTAMP_INVALID", f"{field} has invalid calendar values") from exc
    if instant.tzinfo is None or instant.utcoffset() != UTC.utcoffset(instant):
        raise _error("TIMESTAMP_INVALID", f"{field} must use UTC Z")
    return value


def _exact_mapping(
    value: object, *, fields_: frozenset[str], field: str
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or frozenset(value) != fields_:
        raise _error("EVENT_DATA_INVALID", f"{field} has an invalid exact schema")
    return value


def _run_binding_payload_from_parts(
    *,
    run_id: str,
    created_at_utc: str,
    cell_id: str,
    workload_manifest_sha256: str,
    replay_schedule_sha256: str,
    warmup_role_manifest_sha256: str,
    source_revision: str,
) -> dict[str, object]:
    return {
        "schema_version": RUN_BINDING_SCHEMA_VERSION,
        "run_id": run_id,
        "created_at_utc": created_at_utc,
        "cell_id": cell_id,
        "workload_manifest_sha256": workload_manifest_sha256,
        "replay_schedule_sha256": replay_schedule_sha256,
        "warmup_role_manifest_sha256": warmup_role_manifest_sha256,
        "source_revision": source_revision,
    }


def build_response_profile_run_binding(
    *,
    run_id: str,
    created_at_utc: str,
    population: CalibrationPopulationManifest,
    replay_schedule: ResponseProfileReplaySchedule,
    warmup_role_manifest: ResponseProfileRoleManifest,
    source_revision: str,
) -> ResponseProfileRunBinding:
    """Bind fully reconstructed R2-A inputs without interpreting results."""

    normalized_run_id = _canonical_text(run_id, field="run_id")
    timestamp = _rfc3339_utc(created_at_utc, field="created_at_utc")
    revision = _canonical_text(source_revision, field="source_revision")
    try:
        verified_population = verify_calibration_population_manifest(population)
        verified_schedule = verify_response_profile_replay_schedule(
            replay_schedule,
            population=verified_population,
            source_revision=revision,
        )
        verified_warmup = verify_response_profile_role_manifest(
            warmup_role_manifest
        )
        if (
            verified_warmup.role.kind
            is not ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP
            or len(verified_warmup.members) != WARMUP_QUERY_COUNT
        ):
            raise _error(
                "WARMUP_MANIFEST_INVALID",
                "run binding requires the exact 200-member warm-up role",
            )
        validate_role_manifest_disjointness(
            (
                verified_population.calibration_role_manifest,
                verified_warmup,
            )
        )
    except ResponseProfileLifecycleContractError:
        raise
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error(
            "RUN_BINDING_INPUT_INVALID",
            "R2-A population, schedule, or warm-up manifest is invalid",
        ) from exc

    payload = _run_binding_payload_from_parts(
        run_id=normalized_run_id,
        created_at_utc=timestamp,
        cell_id=verified_population.cell.cell_id,
        workload_manifest_sha256=verified_population.workload_manifest_sha256,
        replay_schedule_sha256=verified_schedule.replay_schedule_sha256,
        warmup_role_manifest_sha256=verified_warmup.role_manifest_sha256,
        source_revision=revision,
    )
    return _make(
        ResponseProfileRunBinding,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=RUN_BINDING_SCHEMA_VERSION,
        run_id=normalized_run_id,
        created_at_utc=timestamp,
        cell_id=verified_population.cell.cell_id,
        workload_manifest_sha256=verified_population.workload_manifest_sha256,
        replay_schedule_sha256=verified_schedule.replay_schedule_sha256,
        warmup_role_manifest_sha256=verified_warmup.role_manifest_sha256,
        source_revision=revision,
        population=verified_population,
        replay_schedule=verified_schedule,
        warmup_role_manifest=verified_warmup,
        run_binding_sha256=_digest(RUN_BINDING_HASH_DOMAIN, payload),
    )  # type: ignore[return-value]


def verify_response_profile_run_binding(value: object) -> ResponseProfileRunBinding:
    if type(value) is not ResponseProfileRunBinding:
        raise _error("RUN_BINDING_INVALID", "run binding must be concrete")
    try:
        if value.schema_version != RUN_BINDING_SCHEMA_VERSION:
            raise _error("RUN_BINDING_INVALID", "run binding schema is unsupported")
        rebuilt = build_response_profile_run_binding(
            run_id=value.run_id,
            created_at_utc=value.created_at_utc,
            population=value.population,
            replay_schedule=value.replay_schedule,
            warmup_role_manifest=value.warmup_role_manifest,
            source_revision=value.source_revision,
        )
    except AttributeError as exc:
        raise _error("RUN_BINDING_INVALID", "run binding is malformed") from exc
    if not _type_exact_equal(value, rebuilt):
        raise _error("RUN_BINDING_INVALID", "run binding failed full reconstruction")
    return rebuilt


def response_profile_run_binding_payload(
    value: ResponseProfileRunBinding,
) -> dict[str, object]:
    verified = verify_response_profile_run_binding(value)
    return _run_binding_payload_from_parts(
        run_id=verified.run_id,
        created_at_utc=verified.created_at_utc,
        cell_id=verified.cell_id,
        workload_manifest_sha256=verified.workload_manifest_sha256,
        replay_schedule_sha256=verified.replay_schedule_sha256,
        warmup_role_manifest_sha256=verified.warmup_role_manifest_sha256,
        source_revision=verified.source_revision,
    )


def response_profile_run_binding_document(
    value: ResponseProfileRunBinding,
) -> dict[str, object]:
    verified = verify_response_profile_run_binding(value)
    return {
        "run_binding_payload": response_profile_run_binding_payload(verified),
        "run_binding_sha256": verified.run_binding_sha256,
    }


def _opaque_descriptor_payload_from_parts(
    *,
    run_binding_sha256: str,
    event_seq: int,
    evidence_role: OpaqueEvidenceRole,
    byte_length: int,
    evidence_bytes_sha256: str,
) -> dict[str, object]:
    return {
        "schema_version": OPAQUE_EVIDENCE_SCHEMA_VERSION,
        "run_binding_sha256": run_binding_sha256,
        "event_seq": event_seq,
        "evidence_role": evidence_role.value,
        "byte_length": byte_length,
        "evidence_bytes_sha256": evidence_bytes_sha256,
    }


def build_opaque_evidence_blob(
    *,
    run_binding_sha256: str,
    event_seq: int,
    evidence_role: OpaqueEvidenceRole,
    evidence_bytes: bytes,
) -> OpaqueEvidenceBlob:
    run_digest = _sha256(run_binding_sha256, field="run_binding_sha256")
    sequence = _exact_int(event_seq, field="event_seq", minimum=0)
    if type(evidence_role) is not OpaqueEvidenceRole:
        raise _error("EVIDENCE_ROLE_INVALID", "evidence role must be a concrete enum")
    if type(evidence_bytes) is not bytes or not evidence_bytes:
        raise _error("EVIDENCE_BYTES_INVALID", "evidence bytes must be non-empty bytes")
    byte_digest = hashlib.sha256(evidence_bytes).hexdigest()
    payload = _opaque_descriptor_payload_from_parts(
        run_binding_sha256=run_digest,
        event_seq=sequence,
        evidence_role=evidence_role,
        byte_length=len(evidence_bytes),
        evidence_bytes_sha256=byte_digest,
    )
    return _make(
        OpaqueEvidenceBlob,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=OPAQUE_EVIDENCE_SCHEMA_VERSION,
        run_binding_sha256=run_digest,
        event_seq=sequence,
        evidence_role=evidence_role,
        byte_length=len(evidence_bytes),
        evidence_bytes_sha256=byte_digest,
        opaque_evidence_sha256=_digest(OPAQUE_EVIDENCE_HASH_DOMAIN, payload),
        evidence_bytes=evidence_bytes,
    )  # type: ignore[return-value]


def verify_opaque_evidence_blob(value: object) -> OpaqueEvidenceBlob:
    if type(value) is not OpaqueEvidenceBlob:
        raise _error("OPAQUE_EVIDENCE_INVALID", "opaque evidence must be concrete")
    try:
        if value.schema_version != OPAQUE_EVIDENCE_SCHEMA_VERSION:
            raise _error("OPAQUE_EVIDENCE_INVALID", "opaque evidence schema is unsupported")
        rebuilt = build_opaque_evidence_blob(
            run_binding_sha256=value.run_binding_sha256,
            event_seq=value.event_seq,
            evidence_role=value.evidence_role,
            evidence_bytes=value.evidence_bytes,
        )
    except AttributeError as exc:
        raise _error("OPAQUE_EVIDENCE_INVALID", "opaque evidence is malformed") from exc
    if not _type_exact_equal(value, rebuilt):
        raise _error("OPAQUE_EVIDENCE_INVALID", "opaque evidence failed reconstruction")
    return rebuilt


def opaque_evidence_descriptor_payload(value: OpaqueEvidenceBlob) -> dict[str, object]:
    verified = verify_opaque_evidence_blob(value)
    return _opaque_descriptor_payload_from_parts(
        run_binding_sha256=verified.run_binding_sha256,
        event_seq=verified.event_seq,
        evidence_role=verified.evidence_role,
        byte_length=verified.byte_length,
        evidence_bytes_sha256=verified.evidence_bytes_sha256,
    )


def opaque_evidence_document(value: OpaqueEvidenceBlob) -> dict[str, object]:
    verified = verify_opaque_evidence_blob(value)
    return {
        "opaque_evidence_descriptor": opaque_evidence_descriptor_payload(verified),
        "opaque_evidence_sha256": verified.opaque_evidence_sha256,
    }


def _event_data_payload(data: _LifecycleEventData) -> dict[str, object]:
    if type(data) is _EpochStartedData or type(data) is _RunSealedData:
        return {}
    if type(data) is _WarmupCompletedData:
        return {
            "warmup_role_manifest_sha256": data.warmup_role_manifest_sha256,
            "warmup_execution_blob_sha256": data.warmup_execution_blob_sha256,
        }
    if type(data) is _BlockStartedData:
        return {
            "pre_block_runtime_snapshot_blob_sha256": (
                data.pre_block_runtime_snapshot_blob_sha256
            )
        }
    if type(data) is _MeasurementStartedData:
        return {
            "within_block_index": data.within_block_index,
            "canonical_query_index": data.canonical_query_index,
            "query_id": data.query_id,
            "query_id_sha256": data.query_id_sha256,
            "observation_identity_sha256": data.observation_identity_sha256,
            "ef": data.ef,
            "started_monotonic_ns": data.started_monotonic_ns,
        }
    if type(data) is _MeasurementCompletedData:
        return {
            "measurement_started_event_sha256": (
                data.measurement_started_event_sha256
            ),
            "measured_result_blob_sha256": data.measured_result_blob_sha256,
            "completed_monotonic_ns": data.completed_monotonic_ns,
        }
    if type(data) is _BlockClosedData:
        return {
            "block_started_event_sha256": data.block_started_event_sha256,
            "measurement_completed_event_sha256": list(
                data.measurement_completed_event_sha256
            ),
            "post_block_runtime_snapshot_blob_sha256": (
                data.post_block_runtime_snapshot_blob_sha256
            ),
        }
    if type(data) is _RunInvalidatedData:
        return {"reason_code": data.reason_code}
    raise _error("EVENT_DATA_INVALID", "event data type is unsupported")


def _normalize_event_data(
    event_kind: LifecycleEventKind, value: object
) -> _LifecycleEventData:
    try:
        if event_kind is LifecycleEventKind.EPOCH_STARTED:
            _exact_mapping(value, fields_=frozenset(), field="event_data")
            return _EpochStartedData()
        if event_kind is LifecycleEventKind.WARMUP_COMPLETED:
            mapping = _exact_mapping(
                value,
                fields_=frozenset(
                    {"warmup_role_manifest_sha256", "warmup_execution_blob_sha256"}
                ),
                field="event_data",
            )
            return _WarmupCompletedData(
                warmup_role_manifest_sha256=_sha256(
                    mapping["warmup_role_manifest_sha256"],
                    field="warmup_role_manifest_sha256",
                ),
                warmup_execution_blob_sha256=_sha256(
                    mapping["warmup_execution_blob_sha256"],
                    field="warmup_execution_blob_sha256",
                ),
            )
        if event_kind is LifecycleEventKind.BLOCK_STARTED:
            mapping = _exact_mapping(
                value,
                fields_=frozenset({"pre_block_runtime_snapshot_blob_sha256"}),
                field="event_data",
            )
            return _BlockStartedData(
                pre_block_runtime_snapshot_blob_sha256=_sha256(
                    mapping["pre_block_runtime_snapshot_blob_sha256"],
                    field="pre_block_runtime_snapshot_blob_sha256",
                )
            )
        if event_kind is LifecycleEventKind.MEASUREMENT_STARTED:
            mapping = _exact_mapping(
                value,
                fields_=frozenset(
                    {
                        "within_block_index",
                        "canonical_query_index",
                        "query_id",
                        "query_id_sha256",
                        "observation_identity_sha256",
                        "ef",
                        "started_monotonic_ns",
                    }
                ),
                field="event_data",
            )
            query_id = canonical_response_profile_query_id(mapping["query_id"])
            return _MeasurementStartedData(
                within_block_index=_exact_int(
                    mapping["within_block_index"],
                    field="within_block_index",
                    minimum=0,
                    maximum=3,
                ),
                canonical_query_index=_exact_int(
                    mapping["canonical_query_index"],
                    field="canonical_query_index",
                    minimum=0,
                    maximum=CALIBRATION_QUERY_COUNT - 1,
                ),
                query_id=query_id,
                query_id_sha256=_sha256(
                    mapping["query_id_sha256"], field="query_id_sha256"
                ),
                observation_identity_sha256=_sha256(
                    mapping["observation_identity_sha256"],
                    field="observation_identity_sha256",
                ),
                ef=_exact_int(mapping["ef"], field="ef", minimum=1),
                started_monotonic_ns=_exact_int(
                    mapping["started_monotonic_ns"],
                    field="started_monotonic_ns",
                    minimum=0,
                ),
            )
        if event_kind is LifecycleEventKind.MEASUREMENT_COMPLETED:
            mapping = _exact_mapping(
                value,
                fields_=frozenset(
                    {
                        "measurement_started_event_sha256",
                        "measured_result_blob_sha256",
                        "completed_monotonic_ns",
                    }
                ),
                field="event_data",
            )
            return _MeasurementCompletedData(
                measurement_started_event_sha256=_sha256(
                    mapping["measurement_started_event_sha256"],
                    field="measurement_started_event_sha256",
                ),
                measured_result_blob_sha256=_sha256(
                    mapping["measured_result_blob_sha256"],
                    field="measured_result_blob_sha256",
                ),
                completed_monotonic_ns=_exact_int(
                    mapping["completed_monotonic_ns"],
                    field="completed_monotonic_ns",
                    minimum=0,
                ),
            )
        if event_kind is LifecycleEventKind.BLOCK_CLOSED:
            mapping = _exact_mapping(
                value,
                fields_=frozenset(
                    {
                        "block_started_event_sha256",
                        "measurement_completed_event_sha256",
                        "post_block_runtime_snapshot_blob_sha256",
                    }
                ),
                field="event_data",
            )
            completed = mapping["measurement_completed_event_sha256"]
            if type(completed) is not list or len(completed) != 4:
                raise _error(
                    "EVENT_DATA_INVALID",
                    "block close requires exactly four completion digests",
                )
            return _BlockClosedData(
                block_started_event_sha256=_sha256(
                    mapping["block_started_event_sha256"],
                    field="block_started_event_sha256",
                ),
                measurement_completed_event_sha256=tuple(
                    _sha256(item, field="measurement_completed_event_sha256")
                    for item in completed
                ),
                post_block_runtime_snapshot_blob_sha256=_sha256(
                    mapping["post_block_runtime_snapshot_blob_sha256"],
                    field="post_block_runtime_snapshot_blob_sha256",
                ),
            )
        if event_kind is LifecycleEventKind.RUN_SEALED:
            _exact_mapping(value, fields_=frozenset(), field="event_data")
            return _RunSealedData()
        if event_kind is LifecycleEventKind.RUN_INVALIDATED:
            mapping = _exact_mapping(
                value,
                fields_=frozenset({"reason_code"}),
                field="event_data",
            )
            return _RunInvalidatedData(
                reason_code=_canonical_text(mapping["reason_code"], field="reason_code")
            )
    except ResponseProfileLifecycleContractError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("EVENT_DATA_INVALID", "event data is malformed") from exc
    raise _error("EVENT_KIND_INVALID", "event kind is unsupported")


def _validate_event_indexes(
    *,
    event_kind: LifecycleEventKind,
    epoch_index: object,
    block_index: object,
    position_index: object,
) -> tuple[int | None, int | None, int | None]:
    if event_kind in (
        LifecycleEventKind.RUN_SEALED,
        LifecycleEventKind.RUN_INVALIDATED,
    ):
        if epoch_index is not None or block_index is not None or position_index is not None:
            raise _error("EVENT_INDEX_INVALID", "run-level event indexes must be null")
        return None, None, None

    epoch = _optional_int(epoch_index, field="epoch_index", minimum=0)
    if epoch is None:
        raise _error("EVENT_INDEX_INVALID", "epoch-level events require epoch_index")
    if event_kind in (
        LifecycleEventKind.EPOCH_STARTED,
        LifecycleEventKind.WARMUP_COMPLETED,
    ):
        if block_index is not None or position_index is not None:
            raise _error("EVENT_INDEX_INVALID", "epoch event block/position must be null")
        return epoch, None, None

    block = _optional_int(
        block_index,
        field="block_index",
        minimum=0,
        maximum=CALIBRATION_QUERY_COUNT - 1,
    )
    if block is None:
        raise _error("EVENT_INDEX_INVALID", "block events require block_index")
    if event_kind in (
        LifecycleEventKind.BLOCK_STARTED,
        LifecycleEventKind.BLOCK_CLOSED,
    ):
        if position_index is not None:
            raise _error("EVENT_INDEX_INVALID", "block event position must be null")
        return epoch, block, None

    position = _optional_int(
        position_index,
        field="position_index",
        minimum=0,
        maximum=MEASURED_POSITION_COUNT - 1,
    )
    if position is None:
        raise _error("EVENT_INDEX_INVALID", "measurement events require position_index")
    return epoch, block, position


def _event_payload_from_parts(
    *,
    run_binding_sha256: str,
    event_seq: int,
    event_kind: LifecycleEventKind,
    epoch_index: int | None,
    block_index: int | None,
    position_index: int | None,
    recorded_at_utc: str,
    event_data: _LifecycleEventData,
    previous_event_sha256: str,
) -> dict[str, object]:
    return {
        "schema_version": LIFECYCLE_EVENT_SCHEMA_VERSION,
        "run_binding_sha256": run_binding_sha256,
        "event_seq": event_seq,
        "event_kind": event_kind.value,
        "epoch_index": epoch_index,
        "block_index": block_index,
        "position_index": position_index,
        "recorded_at_utc": recorded_at_utc,
        "event_data": _event_data_payload(event_data),
        "previous_event_sha256": previous_event_sha256,
    }


def build_response_profile_lifecycle_event(
    *,
    run_binding_sha256: str,
    event_seq: int,
    event_kind: LifecycleEventKind,
    epoch_index: int | None,
    block_index: int | None,
    position_index: int | None,
    recorded_at_utc: str,
    event_data: Mapping[str, object],
    previous_event_sha256: str,
) -> ResponseProfileLifecycleEvent:
    run_digest = _sha256(run_binding_sha256, field="run_binding_sha256")
    sequence = _exact_int(event_seq, field="event_seq", minimum=0)
    if type(event_kind) is not LifecycleEventKind:
        raise _error("EVENT_KIND_INVALID", "event kind must be a concrete enum")
    epoch, block, position = _validate_event_indexes(
        event_kind=event_kind,
        epoch_index=epoch_index,
        block_index=block_index,
        position_index=position_index,
    )
    timestamp = _rfc3339_utc(recorded_at_utc, field="recorded_at_utc")
    data = _normalize_event_data(event_kind, event_data)
    previous = _sha256(previous_event_sha256, field="previous_event_sha256")
    payload = _event_payload_from_parts(
        run_binding_sha256=run_digest,
        event_seq=sequence,
        event_kind=event_kind,
        epoch_index=epoch,
        block_index=block,
        position_index=position,
        recorded_at_utc=timestamp,
        event_data=data,
        previous_event_sha256=previous,
    )
    return _make(
        ResponseProfileLifecycleEvent,
        construction_token=_CONSTRUCTION_TOKEN,
        schema_version=LIFECYCLE_EVENT_SCHEMA_VERSION,
        run_binding_sha256=run_digest,
        event_seq=sequence,
        event_kind=event_kind,
        epoch_index=epoch,
        block_index=block,
        position_index=position,
        recorded_at_utc=timestamp,
        event_data=data,
        previous_event_sha256=previous,
        lifecycle_event_sha256=_digest(LIFECYCLE_EVENT_HASH_DOMAIN, payload),
    )  # type: ignore[return-value]


def verify_response_profile_lifecycle_event(
    value: object,
) -> ResponseProfileLifecycleEvent:
    if type(value) is not ResponseProfileLifecycleEvent:
        raise _error("LIFECYCLE_EVENT_INVALID", "lifecycle event must be concrete")
    try:
        if value.schema_version != LIFECYCLE_EVENT_SCHEMA_VERSION:
            raise _error("LIFECYCLE_EVENT_INVALID", "event schema is unsupported")
        rebuilt = build_response_profile_lifecycle_event(
            run_binding_sha256=value.run_binding_sha256,
            event_seq=value.event_seq,
            event_kind=value.event_kind,
            epoch_index=value.epoch_index,
            block_index=value.block_index,
            position_index=value.position_index,
            recorded_at_utc=value.recorded_at_utc,
            event_data=_event_data_payload(value.event_data),
            previous_event_sha256=value.previous_event_sha256,
        )
    except AttributeError as exc:
        raise _error("LIFECYCLE_EVENT_INVALID", "lifecycle event is malformed") from exc
    if not _type_exact_equal(value, rebuilt):
        raise _error("LIFECYCLE_EVENT_INVALID", "event failed full reconstruction")
    return rebuilt


def response_profile_lifecycle_event_payload(
    value: ResponseProfileLifecycleEvent,
) -> dict[str, object]:
    verified = verify_response_profile_lifecycle_event(value)
    return _event_payload_from_parts(
        run_binding_sha256=verified.run_binding_sha256,
        event_seq=verified.event_seq,
        event_kind=verified.event_kind,
        epoch_index=verified.epoch_index,
        block_index=verified.block_index,
        position_index=verified.position_index,
        recorded_at_utc=verified.recorded_at_utc,
        event_data=verified.event_data,
        previous_event_sha256=verified.previous_event_sha256,
    )


def response_profile_lifecycle_event_document(
    value: ResponseProfileLifecycleEvent,
) -> dict[str, object]:
    verified = verify_response_profile_lifecycle_event(value)
    return {
        "lifecycle_event_payload": response_profile_lifecycle_event_payload(verified),
        "lifecycle_event_sha256": verified.lifecycle_event_sha256,
    }


@dataclass(slots=True)
class _ReducerState:
    current_epoch_index: int | None
    warmup_completed: bool
    previous_completed_monotonic_ns: int | None
    open_block_index: int | None
    block_started_event_sha256: str | None
    active_started_event: ResponseProfileLifecycleEvent | None
    block_completion_digests: list[str]
    closed_block_count: int
    completed_position_count: int
    seen_epoch_indexes: set[int]
    seen_position_indexes: set[int]
    referenced_blob_digests: set[str]
    last_event_sha256: str
    event_count: int
    run_sealed_event_count: int
    run_invalidated_event_count: int
    # Cached result of verifying the run binding exactly once. Verification
    # fully rebuilds the replay schedule (all CALIBRATION_QUERY_COUNT blocks),
    # which is expensive; `apply_next_lifecycle_event` must never redo it on
    # every call, or the incremental writer path stops being O(1) per event.
    verified_run_binding: ResponseProfileRunBinding


def _snapshot(
    *,
    run_binding: ResponseProfileRunBinding,
    state: _ReducerState,
    recovery_boundary: bool,
    reasons: tuple[str, ...],
) -> ResponseProfileLifecycleSnapshot:
    reason_list = list(dict.fromkeys(reasons))
    if recovery_boundary:
        if state.active_started_event is not None:
            reason_list.append("ORPHAN_MEASUREMENT_STARTED")
        elif state.open_block_index is not None:
            reason_list.append("PARTIAL_MEASURED_BLOCK")
    deduplicated = tuple(dict.fromkeys(reason_list))
    invalid = bool(deduplicated)
    complete = (
        not invalid
        and state.closed_block_count == CALIBRATION_QUERY_COUNT
        and state.completed_position_count == MEASURED_POSITION_COUNT
        and state.open_block_index is None
        and state.active_started_event is None
    )
    requires_fresh = (
        recovery_boundary
        and not invalid
        and state.current_epoch_index is not None
        and not complete
    )
    return _make(
        ResponseProfileLifecycleSnapshot,
        construction_token=_CONSTRUCTION_TOKEN,
        run_binding_sha256=run_binding.run_binding_sha256,
        event_count=state.event_count,
        last_event_sha256=state.last_event_sha256,
        current_epoch_index=state.current_epoch_index,
        warmup_completed_in_current_epoch=state.warmup_completed,
        open_block_index=state.open_block_index,
        open_measurement_position_index=(
            None
            if state.active_started_event is None
            else state.active_started_event.position_index
        ),
        closed_block_count=state.closed_block_count,
        completed_position_count=state.completed_position_count,
        seen_epoch_indexes=tuple(sorted(state.seen_epoch_indexes)),
        run_sealed_event_count=state.run_sealed_event_count,
        run_invalidated_event_count=state.run_invalidated_event_count,
        evaluated_at_recovery_boundary=recovery_boundary,
        requires_fresh_epoch_after_recovery=requires_fresh,
        structurally_complete=complete,
        mechanically_invalid=invalid,
        reason_codes=deduplicated,
    )  # type: ignore[return-value]


def _fail(
    code: str,
    *,
    run_binding: ResponseProfileRunBinding,
    state: _ReducerState,
    recovery_boundary: bool,
) -> ResponseProfileLifecycleSnapshot:
    return _snapshot(
        run_binding=run_binding,
        state=state,
        recovery_boundary=recovery_boundary,
        reasons=(code,),
    )


def _same_query_id(left: object, right: object) -> bool:
    return type(left) is type(right) and left == right


_BLOB_CONSUMING_EVENT_KINDS = frozenset(
    {
        LifecycleEventKind.WARMUP_COMPLETED,
        LifecycleEventKind.BLOCK_STARTED,
        LifecycleEventKind.MEASUREMENT_COMPLETED,
        LifecycleEventKind.BLOCK_CLOSED,
    }
)


def _apply_lifecycle_event(
    state: _ReducerState,
    *,
    verified_binding: ResponseProfileRunBinding,
    candidate: ResponseProfileLifecycleEvent,
    consume_blob: Callable[..., str | None],
) -> str | None:
    """Apply exactly one candidate lifecycle event to ``state`` in place.

    Returns a failure reason code, or ``None`` on success (in which case
    ``state.last_event_sha256``/``state.event_count`` have been advanced).

    This is the SOLE per-event transition logic, mechanically extracted
    verbatim from ``reduce_response_profile_lifecycle``'s own loop body with
    no behavior change. Both the full reference reducer (replaying from
    genesis) and the writer-side incremental step
    (``apply_next_lifecycle_event``, B1.1) call this exact function, so their
    per-event outcomes can never diverge -- there is only one implementation
    of "what one event does to state," not two independently maintained
    ones.
    """

    try:
        event = verify_response_profile_lifecycle_event(candidate)
    except (AttributeError, TypeError, ValueError):
        return "LIFECYCLE_EVENT_INVALID"
    if event.run_binding_sha256 != verified_binding.run_binding_sha256:
        return "EVENT_RUN_BINDING_MISMATCH"
    if event.event_seq != state.event_count:
        return "EVENT_SEQUENCE_INVALID"
    if not hmac.compare_digest(
        event.previous_event_sha256, state.last_event_sha256
    ):
        return "EVENT_HASH_CHAIN_INVALID"

    kind = event.event_kind
    data = event.event_data
    reason: str | None = None

    if kind is LifecycleEventKind.EPOCH_STARTED:
        if state.open_block_index is not None or state.active_started_event is not None:
            reason = "EPOCH_STARTED_DURING_OPEN_BLOCK"
        elif event.epoch_index in state.seen_epoch_indexes:
            reason = "EPOCH_INDEX_REUSED"
        else:
            assert event.epoch_index is not None
            state.current_epoch_index = event.epoch_index
            state.seen_epoch_indexes.add(event.epoch_index)
            state.warmup_completed = False
            state.previous_completed_monotonic_ns = None

    elif kind is LifecycleEventKind.WARMUP_COMPLETED:
        assert type(data) is _WarmupCompletedData
        if state.current_epoch_index is None:
            reason = "EPOCH_REQUIRED"
        elif event.epoch_index != state.current_epoch_index:
            reason = "EPOCH_INDEX_MISMATCH"
        elif state.warmup_completed:
            reason = "WARMUP_DUPLICATE"
        elif data.warmup_role_manifest_sha256 != (
            verified_binding.warmup_role_manifest_sha256
        ):
            reason = "WARMUP_MANIFEST_MISMATCH"
        else:
            reason = consume_blob(
                data.warmup_execution_blob_sha256,
                role=OpaqueEvidenceRole.WARMUP_EXECUTION,
                event=event,
            )
            if reason is None:
                state.warmup_completed = True

    elif kind is LifecycleEventKind.BLOCK_STARTED:
        assert type(data) is _BlockStartedData
        if state.current_epoch_index is None:
            reason = "EPOCH_REQUIRED"
        elif event.epoch_index != state.current_epoch_index:
            reason = "EPOCH_INDEX_MISMATCH"
        elif not state.warmup_completed:
            reason = "WARMUP_REQUIRED"
        elif state.open_block_index is not None:
            reason = "BLOCK_ALREADY_OPEN"
        elif state.closed_block_count >= CALIBRATION_QUERY_COUNT:
            reason = "SCHEDULE_EXHAUSTED"
        elif event.block_index != state.closed_block_count:
            reason = "BLOCK_ORDER_MISMATCH"
        else:
            reason = consume_blob(
                data.pre_block_runtime_snapshot_blob_sha256,
                role=OpaqueEvidenceRole.PRE_BLOCK_RUNTIME_SNAPSHOT,
                event=event,
            )
            if reason is None:
                state.open_block_index = event.block_index
                state.block_started_event_sha256 = event.lifecycle_event_sha256
                state.block_completion_digests = []

    elif kind is LifecycleEventKind.MEASUREMENT_STARTED:
        assert type(data) is _MeasurementStartedData
        if state.open_block_index is None:
            reason = "BLOCK_REQUIRED"
        elif state.active_started_event is not None:
            reason = "MEASUREMENT_ALREADY_STARTED"
        elif event.epoch_index != state.current_epoch_index:
            reason = "EPOCH_INDEX_MISMATCH"
        elif event.block_index != state.open_block_index:
            reason = "BLOCK_INDEX_MISMATCH"
        else:
            expected_block = verified_binding.replay_schedule.blocks[
                state.open_block_index
            ]
            within_index = len(state.block_completion_digests)
            expected = expected_block.positions[within_index]
            if data.within_block_index != within_index or event.position_index != expected.position_index:
                reason = "POSITION_ORDER_MISMATCH"
            elif expected.position_index in state.seen_position_indexes:
                reason = "POSITION_DUPLICATE"
            elif (
                state.previous_completed_monotonic_ns is not None
                and data.started_monotonic_ns
                < state.previous_completed_monotonic_ns
            ):
                reason = "MONOTONIC_CHRONOLOGY_INVALID"
            elif (
                data.canonical_query_index != expected.canonical_query_index
                or not _same_query_id(data.query_id, expected.query_id)
                or data.query_id_sha256 != expected.query_id_sha256
                or data.observation_identity_sha256
                != expected.observation_identity_sha256
                or data.ef != expected.ef
            ):
                reason = "SCHEDULE_POSITION_MISMATCH"
            else:
                state.active_started_event = event
                state.seen_position_indexes.add(expected.position_index)

    elif kind is LifecycleEventKind.MEASUREMENT_COMPLETED:
        assert type(data) is _MeasurementCompletedData
        started = state.active_started_event
        if started is None:
            reason = "MEASUREMENT_REQUIRED"
        elif (
            event.epoch_index != started.epoch_index
            or event.block_index != started.block_index
            or event.position_index != started.position_index
        ):
            reason = "MEASUREMENT_INDEX_MISMATCH"
        elif data.measurement_started_event_sha256 != (
            started.lifecycle_event_sha256
        ):
            reason = "STARTED_EVENT_DIGEST_MISMATCH"
        else:
            started_data = started.event_data
            assert type(started_data) is _MeasurementStartedData
            if data.completed_monotonic_ns <= started_data.started_monotonic_ns:
                reason = "MONOTONIC_ORDER_INVALID"
            else:
                reason = consume_blob(
                    data.measured_result_blob_sha256,
                    role=OpaqueEvidenceRole.MEASURED_RESULT,
                    event=event,
                )
                if reason is None:
                    state.block_completion_digests.append(
                        event.lifecycle_event_sha256
                    )
                    state.completed_position_count += 1
                    state.previous_completed_monotonic_ns = (
                        data.completed_monotonic_ns
                    )
                    state.active_started_event = None

    elif kind is LifecycleEventKind.BLOCK_CLOSED:
        assert type(data) is _BlockClosedData
        if state.open_block_index is None:
            reason = "BLOCK_REQUIRED"
        elif state.active_started_event is not None:
            reason = "MEASUREMENT_INCOMPLETE"
        elif event.epoch_index != state.current_epoch_index:
            reason = "EPOCH_INDEX_MISMATCH"
        elif event.block_index != state.open_block_index:
            reason = "BLOCK_INDEX_MISMATCH"
        elif len(state.block_completion_digests) != 4:
            reason = "BLOCK_INCOMPLETE"
        elif data.block_started_event_sha256 != state.block_started_event_sha256:
            reason = "BLOCK_STARTED_DIGEST_MISMATCH"
        elif data.measurement_completed_event_sha256 != tuple(
            state.block_completion_digests
        ):
            reason = "BLOCK_COMPLETION_ORDER_MISMATCH"
        else:
            reason = consume_blob(
                data.post_block_runtime_snapshot_blob_sha256,
                role=OpaqueEvidenceRole.POST_BLOCK_RUNTIME_SNAPSHOT,
                event=event,
            )
            if reason is None:
                state.closed_block_count += 1
                state.open_block_index = None
                state.block_started_event_sha256 = None
                state.block_completion_digests = []

    elif kind is LifecycleEventKind.RUN_SEALED:
        state.run_sealed_event_count += 1

    elif kind is LifecycleEventKind.RUN_INVALIDATED:
        state.run_invalidated_event_count += 1

    if reason is not None:
        return reason

    state.last_event_sha256 = event.lifecycle_event_sha256
    state.event_count += 1
    return None


def reduce_response_profile_lifecycle(
    *,
    run_binding: ResponseProfileRunBinding,
    events: tuple[ResponseProfileLifecycleEvent, ...],
    opaque_evidence: tuple[OpaqueEvidenceBlob, ...],
    recovery_boundary: bool,
) -> ResponseProfileLifecycleSnapshot:
    """Replay one immutable lifecycle and derive its structural state.

    ``recovery_boundary`` must be explicit.  ``False`` permits a currently
    in-flight block prefix; ``True`` makes an orphan STARTED or any unclosed
    measured block terminal, exactly as required for restart replay.
    """

    verified_binding = verify_response_profile_run_binding(run_binding)
    if type(events) is not tuple:
        raise _error("EVENT_SEQUENCE_INVALID", "events must be an immutable tuple")
    if type(opaque_evidence) is not tuple:
        raise _error(
            "OPAQUE_EVIDENCE_INVALID", "opaque evidence must be an immutable tuple"
        )
    if type(recovery_boundary) is not bool:
        raise _error(
            "RECOVERY_BOUNDARY_INVALID", "recovery_boundary must be an exact bool"
        )

    state = _ReducerState(
        current_epoch_index=None,
        warmup_completed=False,
        previous_completed_monotonic_ns=None,
        open_block_index=None,
        block_started_event_sha256=None,
        active_started_event=None,
        block_completion_digests=[],
        closed_block_count=0,
        completed_position_count=0,
        seen_epoch_indexes=set(),
        seen_position_indexes=set(),
        referenced_blob_digests=set(),
        last_event_sha256=verified_binding.run_binding_sha256,
        event_count=0,
        run_sealed_event_count=0,
        run_invalidated_event_count=0,
        verified_run_binding=verified_binding,
    )

    blobs: dict[str, OpaqueEvidenceBlob] = {}
    try:
        for candidate in opaque_evidence:
            blob = verify_opaque_evidence_blob(candidate)
            if blob.run_binding_sha256 != verified_binding.run_binding_sha256:
                return _fail(
                    "OPAQUE_EVIDENCE_RUN_BINDING_MISMATCH",
                    run_binding=verified_binding,
                    state=state,
                    recovery_boundary=recovery_boundary,
                )
            if blob.opaque_evidence_sha256 in blobs:
                return _fail(
                    "OPAQUE_EVIDENCE_DUPLICATE",
                    run_binding=verified_binding,
                    state=state,
                    recovery_boundary=recovery_boundary,
                )
            blobs[blob.opaque_evidence_sha256] = blob
    except (AttributeError, TypeError, ValueError):
        return _fail(
            "OPAQUE_EVIDENCE_INVALID",
            run_binding=verified_binding,
            state=state,
            recovery_boundary=recovery_boundary,
        )

    def consume_blob(
        digest: str,
        *,
        role: OpaqueEvidenceRole,
        event: ResponseProfileLifecycleEvent,
    ) -> str | None:
        blob = blobs.get(digest)
        if blob is None:
            return "OPAQUE_EVIDENCE_MISSING"
        if digest in state.referenced_blob_digests:
            return "OPAQUE_EVIDENCE_REUSED"
        if blob.evidence_role is not role:
            return "OPAQUE_EVIDENCE_ROLE_MISMATCH"
        if (
            blob.run_binding_sha256 != event.run_binding_sha256
            or blob.event_seq != event.event_seq
        ):
            return "OPAQUE_EVIDENCE_EVENT_BINDING_MISMATCH"
        state.referenced_blob_digests.add(digest)
        return None

    for candidate in events:
        reason = _apply_lifecycle_event(
            state,
            verified_binding=verified_binding,
            candidate=candidate,
            consume_blob=consume_blob,
        )
        if reason is not None:
            return _fail(
                reason,
                run_binding=verified_binding,
                state=state,
                recovery_boundary=recovery_boundary,
            )

    if len(state.referenced_blob_digests) != len(blobs):
        return _fail(
            "OPAQUE_EVIDENCE_UNREFERENCED",
            run_binding=verified_binding,
            state=state,
            recovery_boundary=recovery_boundary,
        )

    return _snapshot(
        run_binding=verified_binding,
        state=state,
        recovery_boundary=recovery_boundary,
        reasons=(),
    )


# -----------------------------------------------------------------------------
# B1.1 writer-side incremental reduction
#
# `reduce_response_profile_lifecycle` above is, and remains, the sole
# reference truth: full replay from genesis, unchanged in behavior. Reopen
# and recovery MUST keep using it exactly as before.
#
# The two functions below exist only to avoid re-verifying and re-scanning
# the COMPLETE prior event/blob history on every single append (previously
# O(N) work per append, O(N^2) total across N appends -- see B1.1 scale
# measurement). Both are thin wrappers around `_apply_lifecycle_event`, the
# exact same per-event step function the full reducer's own loop calls, so
# there is only one implementation of "what one event does to state" for
# both call paths to ever diverge from.
# -----------------------------------------------------------------------------


def initial_lifecycle_reducer_state(run_binding: ResponseProfileRunBinding) -> object:
    """Return an opaque, mutable, writer-only reducer-state handle bound to
    ``run_binding``, equivalent to the internal state
    ``reduce_response_profile_lifecycle`` holds before processing its first
    event.

    The return value is deliberately untyped from the caller's perspective:
    treat it as an opaque token. Never construct, inspect, copy, serialize,
    or persist it directly -- it is valid only for the lifetime of one
    process's in-memory writer path and must be rebuilt (via this function,
    then replayed forward with ``apply_next_lifecycle_event``) on every
    reopen, exactly mirroring what a fresh full reduction would derive.
    """

    verified_binding = verify_response_profile_run_binding(run_binding)
    return _ReducerState(
        current_epoch_index=None,
        warmup_completed=False,
        previous_completed_monotonic_ns=None,
        open_block_index=None,
        block_started_event_sha256=None,
        active_started_event=None,
        block_completion_digests=[],
        closed_block_count=0,
        completed_position_count=0,
        seen_epoch_indexes=set(),
        seen_position_indexes=set(),
        referenced_blob_digests=set(),
        last_event_sha256=verified_binding.run_binding_sha256,
        event_count=0,
        run_sealed_event_count=0,
        run_invalidated_event_count=0,
        verified_run_binding=verified_binding,
    )


def apply_next_lifecycle_event(
    *,
    run_binding: ResponseProfileRunBinding,
    reducer_state: object,
    event: ResponseProfileLifecycleEvent,
    blob: OpaqueEvidenceBlob | None,
    recovery_boundary: bool,
) -> ResponseProfileLifecycleSnapshot:
    """Writer-side incremental B1.1 step.

    Applies exactly one new canonical ``event`` (plus at most one new opaque
    evidence ``blob``, which must be referenced by that same event's own
    data fields) to ``reducer_state`` in place, without re-verifying or
    re-scanning any prior event or blob, and returns the resulting
    ``ResponseProfileLifecycleSnapshot``.

    ``run_binding`` is checked cheaply (type and digest only) against the
    binding already verified once by ``initial_lifecycle_reducer_state`` --
    it is deliberately never re-verified from scratch here. Full binding
    verification rebuilds the entire replay schedule and is expensive
    (seconds, for a full-size schedule); paying that cost again on every
    single event would defeat the point of an O(1)-per-event writer step.

    ``reducer_state`` MUST be the exact object most recently returned by
    ``initial_lifecycle_reducer_state`` (for the first call) or by this
    function (for every subsequent call) for this exact ``run_binding`` --
    it is mutated in place; there is no separate value to thread through by
    hand.

    Equivalence requirement (proved exhaustively by this module's own test
    suite, not merely asserted here): calling this function once per event,
    in canonical order, starting from ``initial_lifecycle_reducer_state``,
    must produce -- after the Nth call -- a snapshot byte-identical to
    ``reduce_response_profile_lifecycle`` called fresh with the same first N
    events/blobs and the same ``recovery_boundary``. On a governed
    transition failure, ``reducer_state`` is left completely unchanged
    (matching ``_apply_lifecycle_event``'s own atomic-per-event contract),
    so a failed step never corrupts the running state for a caller that
    chooses to inspect it afterward.

    This function must never substitute for full replay at reopen/recovery
    time -- it is valid only for advancing an already-verified running state
    one step at a time on the normal (non-recovery) append path.
    """

    if not isinstance(reducer_state, _ReducerState):
        raise _error(
            "REDUCER_STATE_INVALID",
            "reducer_state must be the opaque handle returned by "
            "initial_lifecycle_reducer_state or apply_next_lifecycle_event",
        )
    if (
        type(run_binding) is not ResponseProfileRunBinding
        or run_binding.run_binding_sha256
        != reducer_state.verified_run_binding.run_binding_sha256
    ):
        raise _error(
            "RUN_BINDING_INVALID",
            "run_binding must be the exact binding reducer_state was built from",
        )
    verified_binding = reducer_state.verified_run_binding
    if type(recovery_boundary) is not bool:
        raise _error(
            "RECOVERY_BOUNDARY_INVALID", "recovery_boundary must be an exact bool"
        )

    blobs: dict[str, OpaqueEvidenceBlob] = {}
    if blob is not None:
        try:
            verified_blob = verify_opaque_evidence_blob(blob)
        except (AttributeError, TypeError, ValueError):
            return _fail(
                "OPAQUE_EVIDENCE_INVALID",
                run_binding=verified_binding,
                state=reducer_state,
                recovery_boundary=recovery_boundary,
            )
        if verified_blob.run_binding_sha256 != verified_binding.run_binding_sha256:
            return _fail(
                "OPAQUE_EVIDENCE_RUN_BINDING_MISMATCH",
                run_binding=verified_binding,
                state=reducer_state,
                recovery_boundary=recovery_boundary,
            )
        # A blob can only ever be referenced by an event kind whose branch in
        # `_apply_lifecycle_event` calls `consume_blob`. Reject a blob paired
        # with any other kind here, before `_apply_lifecycle_event` runs --
        # otherwise the event could apply successfully and mutate
        # `reducer_state` in place (advancing event_count/last_event_sha256)
        # while this function still reports failure, silently poisoning the
        # writer's running state out from under a caller who trusts this
        # function's own atomicity contract.
        try:
            verified_event = verify_response_profile_lifecycle_event(event)
        except (AttributeError, TypeError, ValueError):
            return _fail(
                "LIFECYCLE_EVENT_INVALID",
                run_binding=verified_binding,
                state=reducer_state,
                recovery_boundary=recovery_boundary,
            )
        if verified_event.event_kind not in _BLOB_CONSUMING_EVENT_KINDS:
            return _fail(
                "OPAQUE_EVIDENCE_UNREFERENCED",
                run_binding=verified_binding,
                state=reducer_state,
                recovery_boundary=recovery_boundary,
            )
        blobs[verified_blob.opaque_evidence_sha256] = verified_blob

    def consume_blob(
        digest: str,
        *,
        role: OpaqueEvidenceRole,
        event: ResponseProfileLifecycleEvent,
    ) -> str | None:
        candidate_blob = blobs.get(digest)
        if candidate_blob is None:
            return "OPAQUE_EVIDENCE_MISSING"
        if digest in reducer_state.referenced_blob_digests:
            return "OPAQUE_EVIDENCE_REUSED"
        if candidate_blob.evidence_role is not role:
            return "OPAQUE_EVIDENCE_ROLE_MISMATCH"
        if (
            candidate_blob.run_binding_sha256 != event.run_binding_sha256
            or candidate_blob.event_seq != event.event_seq
        ):
            return "OPAQUE_EVIDENCE_EVENT_BINDING_MISMATCH"
        reducer_state.referenced_blob_digests.add(digest)
        return None

    reason = _apply_lifecycle_event(
        reducer_state,
        verified_binding=verified_binding,
        candidate=event,
        consume_blob=consume_blob,
    )
    if reason is not None:
        return _fail(
            reason,
            run_binding=verified_binding,
            state=reducer_state,
            recovery_boundary=recovery_boundary,
        )

    if blobs and not (set(blobs) <= reducer_state.referenced_blob_digests):
        return _fail(
            "OPAQUE_EVIDENCE_UNREFERENCED",
            run_binding=verified_binding,
            state=reducer_state,
            recovery_boundary=recovery_boundary,
        )

    return _snapshot(
        run_binding=verified_binding,
        state=reducer_state,
        recovery_boundary=recovery_boundary,
        reasons=(),
    )
