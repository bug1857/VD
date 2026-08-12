"""Pre-result detector-trigger control binding for ADR-010."""

from __future__ import annotations

from dataclasses import dataclass, fields
import hashlib
import hmac
import re
import unicodedata

from .artifacts import canonical_json_bytes
from .config import Metric
from .drift import EvidenceProvenance, build_evidence_provenance, evidence_provenance_valid
from .lkg_window_readiness import parse_rfc3339_utc_instant, validate_rfc3339_utc
from .shadow_event_types import MonitorStreamKey


CONTROL_SCHEMA_VERSION = "response-profile-control-v1"
CONTROL_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_CONTROL::V1\x00"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

__all__ = [
    "CONTROL_SCHEMA_VERSION",
    "CONTROL_HASH_DOMAIN",
    "ResponseProfileControlError",
    "ResponseProfileControl",
    "build_response_profile_control",
    "verify_response_profile_control",
    "response_profile_control_payload",
    "response_profile_control_document",
    "response_profile_control_from_document",
]


class ResponseProfileControlError(ValueError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileControlError:
    return ResponseProfileControlError(message, code=code)


def _text(value: object, *, field: str) -> str:
    if type(value) is not str or not value:
        raise _error("CONTROL_FIELD_INVALID", f"{field} must be non-empty text")
    normalized = unicodedata.normalize("NFC", value)
    if normalized != value:
        raise _error("CONTROL_FIELD_INVALID", f"{field} must already be NFC")
    return value


def _sha(value: object, *, field: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _error("CONTROL_FIELD_INVALID", f"{field} must be lowercase SHA-256")
    return value


def _provenance_payload(value: EvidenceProvenance) -> dict[str, object]:
    if type(value) is not EvidenceProvenance or not evidence_provenance_valid(value):
        raise _error("CONTROL_PROVENANCE_INVALID", "detector provenance is invalid")
    return {
        "schema_version": value.schema_version,
        "metric": value.metric.value,
        "threshold_stratum": value.threshold_stratum,
        "reference_window_id": value.reference_window_id,
        "current_window_id": value.current_window_id,
        "reference_manifest_sha256": value.reference_manifest_sha256,
        "current_manifest_sha256": value.current_manifest_sha256,
        "configuration_identity": value.configuration_identity,
        "data_identity": value.data_identity,
        "flat_binding_id": value.flat_binding_id,
        "hnsw_binding_id": value.hnsw_binding_id,
        "reference_audit_ids": list(value.reference_audit_ids),
        "reference_audit_rank_digests": list(value.reference_audit_rank_digests),
        "current_audit_ids": list(value.current_audit_ids),
        "current_audit_rank_digests": list(value.current_audit_rank_digests),
        "sha256": value.sha256,
    }


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileControl:
    schema_version: str
    stream_key: MonitorStreamKey
    detector_provenance: EvidenceProvenance
    trigger_window_sequence: int
    detector_head_sha256: str
    detector_head_record_sequence: int
    detector_head_record_sha256: str
    detector_head_persisted_at_utc: str
    calibration_population_sha256: str
    warmup_role_manifest_sha256: str
    ordered_query_payload_sha256: str
    replay_schedule_sha256: str
    environment_manifest_sha256: str
    source_revision: str
    frozen_at_utc: str
    control_profile_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("response-profile controls must be built by the contract factory")


def _new(**values: object) -> ResponseProfileControl:
    result = object.__new__(ResponseProfileControl)
    for name, value in values.items():
        object.__setattr__(result, name, value)
    return result


def _stream_payload(value: MonitorStreamKey) -> dict[str, object]:
    if type(value) is not MonitorStreamKey:
        raise _error("CONTROL_STREAM_INVALID", "stream key must be concrete")
    rebuilt = MonitorStreamKey(
        stream_id=value.stream_id,
        metric=value.metric,
        threshold_stratum=value.threshold_stratum,
        configuration_identity=value.configuration_identity,
        data_identity=value.data_identity,
        flat_binding_id=value.flat_binding_id,
        hnsw_binding_id=value.hnsw_binding_id,
    )
    if any(type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name)) for item in fields(value)):
        raise _error("CONTROL_STREAM_INVALID", "stream key is noncanonical")
    return {
        "stream_id": _text(value.stream_id, field="stream_id"),
        "metric": value.metric.value,
        "threshold_stratum": _text(value.threshold_stratum, field="threshold_stratum"),
        "configuration_identity": _text(value.configuration_identity, field="configuration_identity"),
        "data_identity": _text(value.data_identity, field="data_identity"),
        "flat_binding_id": _text(value.flat_binding_id, field="flat_binding_id"),
        "hnsw_binding_id": _text(value.hnsw_binding_id, field="hnsw_binding_id"),
    }


def _payload(
    *,
    stream_key: MonitorStreamKey,
    detector_provenance: EvidenceProvenance,
    trigger_window_sequence: int,
    detector_head_sha256: str,
    detector_head_record_sequence: int,
    detector_head_record_sha256: str,
    detector_head_persisted_at_utc: str,
    calibration_population_sha256: str,
    warmup_role_manifest_sha256: str,
    ordered_query_payload_sha256: str,
    replay_schedule_sha256: str,
    environment_manifest_sha256: str,
    source_revision: str,
    frozen_at_utc: str,
) -> dict[str, object]:
    if type(trigger_window_sequence) is not int or trigger_window_sequence < 0:
        raise _error("CONTROL_SEQUENCE_INVALID", "trigger window sequence is invalid")
    if (
        type(detector_head_record_sequence) is not int
        or detector_head_record_sequence < 0
    ):
        raise _error(
            "CONTROL_HEAD_RECORD_INVALID",
            "detector-head record sequence is invalid",
        )
    provenance = _provenance_payload(detector_provenance)
    stream = _stream_payload(stream_key)
    if (
        detector_provenance.metric is not stream_key.metric
        or detector_provenance.threshold_stratum != stream_key.threshold_stratum
        or detector_provenance.configuration_identity != stream_key.configuration_identity
        or detector_provenance.data_identity != stream_key.data_identity
        or detector_provenance.flat_binding_id != stream_key.flat_binding_id
        or detector_provenance.hnsw_binding_id != stream_key.hnsw_binding_id
    ):
        raise _error("CONTROL_PROVENANCE_MISMATCH", "provenance does not match stream")
    validate_rfc3339_utc(frozen_at_utc, field="frozen_at_utc")
    validate_rfc3339_utc(
        detector_head_persisted_at_utc,
        field="detector_head_persisted_at_utc",
    )
    if parse_rfc3339_utc_instant(
        detector_head_persisted_at_utc
    ) >= parse_rfc3339_utc_instant(frozen_at_utc):
        raise _error(
            "CONTROL_FROZEN_BEFORE_HEAD_COMMIT",
            "control must be frozen after the bound detector-head record",
        )
    return {
        "schema_version": CONTROL_SCHEMA_VERSION,
        "stream": stream,
        "detector_provenance": provenance,
        "trigger_window_sequence": trigger_window_sequence,
        "detector_head_sha256": _sha(
            detector_head_sha256, field="detector_head_sha256"
        ),
        "detector_head_record_sequence": detector_head_record_sequence,
        "detector_head_record_sha256": _sha(
            detector_head_record_sha256, field="detector_head_record_sha256"
        ),
        "detector_head_persisted_at_utc": detector_head_persisted_at_utc,
        "calibration_population_sha256": _sha(calibration_population_sha256, field="calibration_population_sha256"),
        "warmup_role_manifest_sha256": _sha(warmup_role_manifest_sha256, field="warmup_role_manifest_sha256"),
        "ordered_query_payload_sha256": _sha(ordered_query_payload_sha256, field="ordered_query_payload_sha256"),
        "replay_schedule_sha256": _sha(replay_schedule_sha256, field="replay_schedule_sha256"),
        "environment_manifest_sha256": _sha(environment_manifest_sha256, field="environment_manifest_sha256"),
        "source_revision": _text(source_revision, field="source_revision"),
        "frozen_at_utc": frozen_at_utc,
    }


def build_response_profile_control(
    *,
    stream_key: MonitorStreamKey,
    detector_provenance: EvidenceProvenance,
    trigger_window_sequence: int,
    detector_head_sha256: str,
    detector_head_record_sequence: int,
    detector_head_record_sha256: str,
    detector_head_persisted_at_utc: str,
    calibration_population_sha256: str,
    warmup_role_manifest_sha256: str,
    ordered_query_payload_sha256: str,
    replay_schedule_sha256: str,
    environment_manifest_sha256: str,
    source_revision: str,
    frozen_at_utc: str,
) -> ResponseProfileControl:
    values = {
        "stream_key": stream_key,
        "detector_provenance": detector_provenance,
        "trigger_window_sequence": trigger_window_sequence,
        "detector_head_sha256": detector_head_sha256,
        "detector_head_record_sequence": detector_head_record_sequence,
        "detector_head_record_sha256": detector_head_record_sha256,
        "detector_head_persisted_at_utc": detector_head_persisted_at_utc,
        "calibration_population_sha256": calibration_population_sha256,
        "warmup_role_manifest_sha256": warmup_role_manifest_sha256,
        "ordered_query_payload_sha256": ordered_query_payload_sha256,
        "replay_schedule_sha256": replay_schedule_sha256,
        "environment_manifest_sha256": environment_manifest_sha256,
        "source_revision": source_revision,
        "frozen_at_utc": frozen_at_utc,
    }
    try:
        payload = _payload(**values)
    except ResponseProfileControlError:
        raise
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("CONTROL_INVALID", "control construction failed") from exc
    return _new(
        schema_version=CONTROL_SCHEMA_VERSION,
        **values,
        control_profile_sha256=hashlib.sha256(
            CONTROL_HASH_DOMAIN + canonical_json_bytes(payload)
        ).hexdigest(),
    )


def verify_response_profile_control(value: object) -> ResponseProfileControl:
    if type(value) is not ResponseProfileControl:
        raise _error("CONTROL_INVALID", "control must be concrete")
    try:
        rebuilt = build_response_profile_control(
            stream_key=value.stream_key,
            detector_provenance=value.detector_provenance,
            trigger_window_sequence=value.trigger_window_sequence,
            detector_head_sha256=value.detector_head_sha256,
            detector_head_record_sequence=value.detector_head_record_sequence,
            detector_head_record_sha256=value.detector_head_record_sha256,
            detector_head_persisted_at_utc=value.detector_head_persisted_at_utc,
            calibration_population_sha256=value.calibration_population_sha256,
            warmup_role_manifest_sha256=value.warmup_role_manifest_sha256,
            ordered_query_payload_sha256=value.ordered_query_payload_sha256,
            replay_schedule_sha256=value.replay_schedule_sha256,
            environment_manifest_sha256=value.environment_manifest_sha256,
            source_revision=value.source_revision,
            frozen_at_utc=value.frozen_at_utc,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("CONTROL_INVALID", "control reconstruction failed") from exc
    if value.schema_version != rebuilt.schema_version or not hmac.compare_digest(
        value.control_profile_sha256, rebuilt.control_profile_sha256
    ):
        raise _error("CONTROL_INVALID", "control digest mismatch")
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("CONTROL_INVALID", "control is noncanonical")
    return rebuilt


def response_profile_control_payload(value: ResponseProfileControl) -> dict[str, object]:
    verified = verify_response_profile_control(value)
    return _payload(
        stream_key=verified.stream_key,
        detector_provenance=verified.detector_provenance,
        trigger_window_sequence=verified.trigger_window_sequence,
        detector_head_sha256=verified.detector_head_sha256,
        detector_head_record_sequence=verified.detector_head_record_sequence,
        detector_head_record_sha256=verified.detector_head_record_sha256,
        detector_head_persisted_at_utc=verified.detector_head_persisted_at_utc,
        calibration_population_sha256=verified.calibration_population_sha256,
        warmup_role_manifest_sha256=verified.warmup_role_manifest_sha256,
        ordered_query_payload_sha256=verified.ordered_query_payload_sha256,
        replay_schedule_sha256=verified.replay_schedule_sha256,
        environment_manifest_sha256=verified.environment_manifest_sha256,
        source_revision=verified.source_revision,
        frozen_at_utc=verified.frozen_at_utc,
    )


def response_profile_control_document(value: ResponseProfileControl) -> dict[str, object]:
    """Self-verifying document: payload plus the control's own outer digest.

    Mirrors ``response_profile_detector_head_document``'s exact two-level
    shape (payload + digest) even though this module previously only
    exposed the inner ``response_profile_control_payload``.
    """

    verified = verify_response_profile_control(value)
    return {
        "control_payload": response_profile_control_payload(verified),
        "control_profile_sha256": verified.control_profile_sha256,
    }


def response_profile_control_from_document(value: object) -> ResponseProfileControl:
    """Strictly reconstruct one ``ResponseProfileControl`` from its canonical
    document -- the exact inverse of ``response_profile_control_document``.

    Every nested identity (``MonitorStreamKey``, ``EvidenceProvenance``) is
    rebuilt through its own real contract factory, never through
    ``object.__new__``; the final digest comparison against
    ``control_profile_sha256`` is the canonical round-trip proof. No field is
    defaulted, inferred, or repaired -- an unexpected or missing field, a
    malformed enum, or a mismatched digest at any nesting level fails closed.
    """

    try:
        if type(value) is not dict or frozenset(value) != {
            "control_payload",
            "control_profile_sha256",
        }:
            raise ValueError("document fields differ")
        payload = value["control_payload"]
        if type(payload) is not dict or frozenset(payload) != {
            "schema_version",
            "stream",
            "detector_provenance",
            "trigger_window_sequence",
            "detector_head_sha256",
            "detector_head_record_sequence",
            "detector_head_record_sha256",
            "detector_head_persisted_at_utc",
            "calibration_population_sha256",
            "warmup_role_manifest_sha256",
            "ordered_query_payload_sha256",
            "replay_schedule_sha256",
            "environment_manifest_sha256",
            "source_revision",
            "frozen_at_utc",
        }:
            raise ValueError("payload fields differ")
        if payload["schema_version"] != CONTROL_SCHEMA_VERSION:
            raise ValueError("schema differs")
        stream = payload["stream"]
        if type(stream) is not dict or frozenset(stream) != {
            "stream_id",
            "metric",
            "threshold_stratum",
            "configuration_identity",
            "data_identity",
            "flat_binding_id",
            "hnsw_binding_id",
        }:
            raise ValueError("stream fields differ")
        provenance = payload["detector_provenance"]
        if type(provenance) is not dict or frozenset(provenance) != {
            "schema_version",
            "metric",
            "threshold_stratum",
            "reference_window_id",
            "current_window_id",
            "reference_manifest_sha256",
            "current_manifest_sha256",
            "configuration_identity",
            "data_identity",
            "flat_binding_id",
            "hnsw_binding_id",
            "reference_audit_ids",
            "reference_audit_rank_digests",
            "current_audit_ids",
            "current_audit_rank_digests",
            "sha256",
        }:
            raise ValueError("provenance fields differ")
        trigger_window_sequence = payload["trigger_window_sequence"]
        if isinstance(trigger_window_sequence, bool) or not isinstance(trigger_window_sequence, int):
            raise ValueError("trigger_window_sequence must be an integer")
        detector_head_record_sequence = payload["detector_head_record_sequence"]
        if isinstance(detector_head_record_sequence, bool) or not isinstance(
            detector_head_record_sequence, int
        ):
            raise ValueError("detector_head_record_sequence must be an integer")

        stream_key = MonitorStreamKey(
            stream_id=stream["stream_id"],
            metric=Metric(stream["metric"]),
            threshold_stratum=stream["threshold_stratum"],
            configuration_identity=stream["configuration_identity"],
            data_identity=stream["data_identity"],
            flat_binding_id=stream["flat_binding_id"],
            hnsw_binding_id=stream["hnsw_binding_id"],
        )
        rebuilt_provenance = build_evidence_provenance(
            metric=Metric(provenance["metric"]),
            threshold_stratum=provenance["threshold_stratum"],
            reference_window_id=provenance["reference_window_id"],
            current_window_id=provenance["current_window_id"],
            reference_manifest_sha256=provenance["reference_manifest_sha256"],
            current_manifest_sha256=provenance["current_manifest_sha256"],
            configuration_identity=provenance["configuration_identity"],
            data_identity=provenance["data_identity"],
            flat_binding_id=provenance["flat_binding_id"],
            hnsw_binding_id=provenance["hnsw_binding_id"],
            reference_audit_ids=tuple(provenance["reference_audit_ids"]),
            reference_audit_rank_digests=tuple(provenance["reference_audit_rank_digests"]),
            current_audit_ids=tuple(provenance["current_audit_ids"]),
            current_audit_rank_digests=tuple(provenance["current_audit_rank_digests"]),
        )
        if (
            provenance["schema_version"] != rebuilt_provenance.schema_version
            or provenance["sha256"] != rebuilt_provenance.sha256
        ):
            raise ValueError("provenance digest differs")

        result = build_response_profile_control(
            stream_key=stream_key,
            detector_provenance=rebuilt_provenance,
            trigger_window_sequence=trigger_window_sequence,
            detector_head_sha256=payload["detector_head_sha256"],
            detector_head_record_sequence=detector_head_record_sequence,
            detector_head_record_sha256=payload["detector_head_record_sha256"],
            detector_head_persisted_at_utc=payload["detector_head_persisted_at_utc"],
            calibration_population_sha256=payload["calibration_population_sha256"],
            warmup_role_manifest_sha256=payload["warmup_role_manifest_sha256"],
            ordered_query_payload_sha256=payload["ordered_query_payload_sha256"],
            replay_schedule_sha256=payload["replay_schedule_sha256"],
            environment_manifest_sha256=payload["environment_manifest_sha256"],
            source_revision=payload["source_revision"],
            frozen_at_utc=payload["frozen_at_utc"],
        )
        if value["control_profile_sha256"] != result.control_profile_sha256:
            raise ValueError("control digest differs")
        return result
    except ResponseProfileControlError:
        raise
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise _error("CONTROL_DOCUMENT_INVALID", "control document is invalid") from exc
