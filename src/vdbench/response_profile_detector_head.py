"""Canonical latest-detector-head identity for ADR-010.

The value records one completed detector evaluation lineage.  It is immutable
identity evidence only: it does not assert that it is still the latest head.
Only the hardened monitor store may issue a verified-latest snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
import hashlib
import hmac

from .artifacts import canonical_json_bytes
from .config import Metric
from .drift import (
    DetectorState,
    DriftClassification,
    EvidenceProvenance,
    build_evidence_provenance,
    evidence_provenance_valid,
)
from .shadow_event_types import MonitorStreamKey


DETECTOR_HEAD_SCHEMA_VERSION = "response-profile-detector-head-v1"
DETECTOR_HEAD_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_DETECTOR_HEAD::V1\x00"

__all__ = [
    "DETECTOR_HEAD_SCHEMA_VERSION",
    "DETECTOR_HEAD_HASH_DOMAIN",
    "ResponseProfileDetectorHeadError",
    "ResponseProfileDetectorHead",
    "build_response_profile_detector_head",
    "response_profile_detector_head_document",
    "response_profile_detector_head_from_document",
    "response_profile_detector_head_payload",
    "verify_response_profile_detector_head",
]


class ResponseProfileDetectorHeadError(ValueError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileDetectorHeadError:
    return ResponseProfileDetectorHeadError(message, code=code)


def _provenance_document(value: EvidenceProvenance) -> dict[str, object]:
    if type(value) is not EvidenceProvenance or not evidence_provenance_valid(value):
        raise _error("DETECTOR_HEAD_PROVENANCE_INVALID", "provenance is invalid")
    rebuilt = build_evidence_provenance(
        metric=value.metric,
        threshold_stratum=value.threshold_stratum,
        reference_window_id=value.reference_window_id,
        current_window_id=value.current_window_id,
        reference_manifest_sha256=value.reference_manifest_sha256,
        current_manifest_sha256=value.current_manifest_sha256,
        configuration_identity=value.configuration_identity,
        data_identity=value.data_identity,
        flat_binding_id=value.flat_binding_id,
        hnsw_binding_id=value.hnsw_binding_id,
        reference_audit_ids=value.reference_audit_ids,
        reference_audit_rank_digests=value.reference_audit_rank_digests,
        current_audit_ids=value.current_audit_ids,
        current_audit_rank_digests=value.current_audit_rank_digests,
    )
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("DETECTOR_HEAD_PROVENANCE_INVALID", "provenance is noncanonical")
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


def _stream_document(value: MonitorStreamKey) -> dict[str, object]:
    if type(value) is not MonitorStreamKey:
        raise _error("DETECTOR_HEAD_STREAM_INVALID", "stream key must be concrete")
    try:
        rebuilt = MonitorStreamKey(
            stream_id=value.stream_id,
            metric=value.metric,
            threshold_stratum=value.threshold_stratum,
            configuration_identity=value.configuration_identity,
            data_identity=value.data_identity,
            flat_binding_id=value.flat_binding_id,
            hnsw_binding_id=value.hnsw_binding_id,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("DETECTOR_HEAD_STREAM_INVALID", "stream key is invalid") from exc
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("DETECTOR_HEAD_STREAM_INVALID", "stream key is noncanonical")
    return {
        "stream_id": value.stream_id,
        "metric": value.metric.value,
        "threshold_stratum": value.threshold_stratum,
        "configuration_identity": value.configuration_identity,
        "data_identity": value.data_identity,
        "flat_binding_id": value.flat_binding_id,
        "hnsw_binding_id": value.hnsw_binding_id,
    }


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileDetectorHead:
    schema_version: str
    stream_key: MonitorStreamKey
    window_sequence: int
    detector_state: DetectorState
    detector_classification: DriftClassification
    detector_provenance: EvidenceProvenance
    detector_head_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("detector heads must be built by the contract factory")


def _new(**values: object) -> ResponseProfileDetectorHead:
    result = object.__new__(ResponseProfileDetectorHead)
    for name, value in values.items():
        object.__setattr__(result, name, value)
    return result


def _payload(
    *,
    stream_key: MonitorStreamKey,
    window_sequence: int,
    detector_state: DetectorState,
    detector_classification: DriftClassification,
    detector_provenance: EvidenceProvenance,
) -> dict[str, object]:
    if type(window_sequence) is not int or window_sequence < 2:
        raise _error("DETECTOR_HEAD_SEQUENCE_INVALID", "window sequence must be at least 2")
    stream = _stream_document(stream_key)
    provenance = _provenance_document(detector_provenance)
    if type(detector_state) is not DetectorState:
        raise _error("DETECTOR_HEAD_STATE_INVALID", "detector state must be concrete")
    if type(detector_classification) is not DriftClassification:
        raise _error(
            "DETECTOR_HEAD_CLASSIFICATION_INVALID",
            "detector classification must be concrete",
        )
    if (detector_state is DetectorState.DRIFT) != (
        detector_classification is not DriftClassification.NONE
    ):
        raise _error(
            "DETECTOR_HEAD_OUTCOME_INVALID",
            "detector state and classification are inconsistent",
        )
    if (
        detector_provenance.metric is not stream_key.metric
        or detector_provenance.threshold_stratum != stream_key.threshold_stratum
        or detector_provenance.configuration_identity != stream_key.configuration_identity
        or detector_provenance.data_identity != stream_key.data_identity
        or detector_provenance.flat_binding_id != stream_key.flat_binding_id
        or detector_provenance.hnsw_binding_id != stream_key.hnsw_binding_id
    ):
        raise _error("DETECTOR_HEAD_STREAM_MISMATCH", "provenance differs from stream")
    return {
        "schema_version": DETECTOR_HEAD_SCHEMA_VERSION,
        "stream": stream,
        "window_sequence": window_sequence,
        "detector_state": detector_state.value,
        "detector_classification": detector_classification.value,
        "detector_provenance": provenance,
    }


def build_response_profile_detector_head(
    *,
    stream_key: MonitorStreamKey,
    window_sequence: int,
    detector_state: DetectorState,
    detector_classification: DriftClassification,
    detector_provenance: EvidenceProvenance,
) -> ResponseProfileDetectorHead:
    payload = _payload(
        stream_key=stream_key,
        window_sequence=window_sequence,
        detector_state=detector_state,
        detector_classification=detector_classification,
        detector_provenance=detector_provenance,
    )
    return _new(
        schema_version=DETECTOR_HEAD_SCHEMA_VERSION,
        stream_key=stream_key,
        window_sequence=window_sequence,
        detector_state=detector_state,
        detector_classification=detector_classification,
        detector_provenance=detector_provenance,
        detector_head_sha256=hashlib.sha256(
            DETECTOR_HEAD_HASH_DOMAIN + canonical_json_bytes(payload)
        ).hexdigest(),
    )


def verify_response_profile_detector_head(value: object) -> ResponseProfileDetectorHead:
    if type(value) is not ResponseProfileDetectorHead:
        raise _error("DETECTOR_HEAD_INVALID", "detector head must be concrete")
    try:
        rebuilt = build_response_profile_detector_head(
            stream_key=value.stream_key,
            window_sequence=value.window_sequence,
            detector_state=value.detector_state,
            detector_classification=value.detector_classification,
            detector_provenance=value.detector_provenance,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("DETECTOR_HEAD_INVALID", "detector head reconstruction failed") from exc
    if value.schema_version != rebuilt.schema_version or not hmac.compare_digest(
        value.detector_head_sha256, rebuilt.detector_head_sha256
    ):
        raise _error("DETECTOR_HEAD_INVALID", "detector head digest mismatch")
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("DETECTOR_HEAD_INVALID", "detector head is noncanonical")
    return rebuilt


def response_profile_detector_head_payload(
    value: ResponseProfileDetectorHead,
) -> dict[str, object]:
    verified = verify_response_profile_detector_head(value)
    return _payload(
        stream_key=verified.stream_key,
        window_sequence=verified.window_sequence,
        detector_state=verified.detector_state,
        detector_classification=verified.detector_classification,
        detector_provenance=verified.detector_provenance,
    )


def response_profile_detector_head_document(
    value: ResponseProfileDetectorHead,
) -> dict[str, object]:
    verified = verify_response_profile_detector_head(value)
    return {
        "detector_head_payload": response_profile_detector_head_payload(verified),
        "detector_head_sha256": verified.detector_head_sha256,
    }


def response_profile_detector_head_from_document(
    value: object,
) -> ResponseProfileDetectorHead:
    try:
        if type(value) is not dict or frozenset(value) != {
            "detector_head_payload",
            "detector_head_sha256",
        }:
            raise ValueError("document fields differ")
        payload = value["detector_head_payload"]
        if type(payload) is not dict or frozenset(payload) != {
            "schema_version",
            "stream",
            "window_sequence",
            "detector_state",
            "detector_classification",
            "detector_provenance",
        }:
            raise ValueError("payload fields differ")
        if payload["schema_version"] != DETECTOR_HEAD_SCHEMA_VERSION:
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
            reference_audit_ids=provenance["reference_audit_ids"],
            reference_audit_rank_digests=provenance["reference_audit_rank_digests"],
            current_audit_ids=provenance["current_audit_ids"],
            current_audit_rank_digests=provenance["current_audit_rank_digests"],
        )
        if (
            provenance["schema_version"] != rebuilt_provenance.schema_version
            or provenance["sha256"] != rebuilt_provenance.sha256
        ):
            raise ValueError("provenance digest differs")
        result = build_response_profile_detector_head(
            stream_key=stream_key,
            window_sequence=payload["window_sequence"],
            detector_state=DetectorState(payload["detector_state"]),
            detector_classification=DriftClassification(
                payload["detector_classification"]
            ),
            detector_provenance=rebuilt_provenance,
        )
        if value["detector_head_sha256"] != result.detector_head_sha256:
            raise ValueError("head digest differs")
        return result
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise _error("DETECTOR_HEAD_DOCUMENT_INVALID", "detector-head document is invalid") from exc
