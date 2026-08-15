"""ADR-012 source-bound shadow windows and durable detector-v2 progression.

This future-v2 path is schema-distinct from historical detector evidence.  It
never chooses source membership: every shadow position binds one immutable
ADR-013 source record.  The SQLite store persists complete canonical window
and provenance documents, then reconstructs reference/gap/head state on every
open.  It performs no search, policy, grant, routing, or actuation operation.

Evaluator trust boundary (read before consuming any head):
    ``SQLiteHostWindowDetectorV2Store.process_window`` takes a *caller-supplied*
    ``evaluator`` callable and persists whatever ``DriftDecision`` it returns.
    This module never invokes ``vdbench.drift.evaluate_drift`` and performs no
    statistical computation of its own, so it cannot and does not verify that a
    real governed detector produced the decision.

    Consequently a ``V2DetectorHead`` proves exactly three things: that its
    reference/current source windows are the durably committed ADR-013 windows
    it names, that its provenance and shadow-window digests bind those exact
    windows, and that the reference/gap/rebaseline progression recorded around
    it is internally consistent and reconstructs identically after restart.  It
    is lineage, binding, and durable-progression evidence only.

    A head does NOT prove that a real governed statistical detector executed.
    A head minted from a structural or deterministic-fake evaluator is
    indistinguishable by type or field from one minted by a real detector, and
    is therefore authorized for OFFLINE STRUCTURAL use only.  A real EXP-010
    trigger will additionally require a separately governed real-detector
    attestation that does not exist in this module.

    No qualification, policy, grant, routing, admission, activation, actuation,
    or candidate authority is created by this module or by any head it issues.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields
from enum import StrEnum
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading

from .artifacts import canonical_json_bytes
from .config import Metric
from .drift import (
    DetectorState,
    DriftClassification,
    DriftDecision,
    EvidenceProvenance,
    build_evidence_provenance,
    evidence_provenance_valid,
)
from .host_window_lineage import (
    CommittedHostObservation,
    verify_committed_host_observation,
)
from .shadow_event_types import MonitorStreamKey

__all__ = [
    "DETECTOR_V2_HEAD_SCHEMA_VERSION",
    "HostWindowDetectorV2Error",
    "HostWindowV2Status",
    "SQLiteHostWindowDetectorV2Store",
    "V2DetectorHead",
    "PersistedV2DetectorWindow",
    "V2DetectorProgression",
    "V2DetectorProcessResult",
    "V2ShadowPositionEvidence",
    "V2ShadowWindow",
    "VerifiedLatestV2DetectorHead",
    "build_v2_shadow_position",
    "build_v2_shadow_window",
    "source_window_sha256",
    "verify_v2_detector_head",
]

DETECTOR_V2_HEAD_SCHEMA_VERSION = "response-profile-detector-head-v2"
_POSITION_SCHEMA = "response-profile-shadow-position-v2"
_WINDOW_SCHEMA = "response-profile-shadow-window-v2"
_EVENT_SCHEMA = "response-profile-detector-window-event-v2"
_STORE_BINDING_SCHEMA = "response-profile-detector-store-binding-v2"
_DB_VERSION = 2
_SOURCE_WINDOW_DOMAIN = b"VD::HOST_RESPONSE_WINDOW::V2\x00"
_POSITION_DOMAIN = b"VD::SHADOW_POSITION::V2\x00"
_WINDOW_DOMAIN = b"VD::SHADOW_WINDOW::V2\x00"
_HEAD_DOMAIN = b"VD::RESPONSE_PROFILE_DETECTOR_HEAD::V2\x00"
_EVENT_DOMAIN = b"VD::RESPONSE_PROFILE_DETECTOR_EVENT::V2\x00"
_BINDING_DOMAIN = b"VD::RESPONSE_PROFILE_DETECTOR_STORE::V2\x00"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z\Z")
_OWNERSHIP_LOCK = threading.Lock()
_OWNED_LOCK_INODES: set[tuple[int, int]] = set()


class HostWindowDetectorV2Error(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> HostWindowDetectorV2Error:
    return HostWindowDetectorV2Error(code)


def _digest(domain: bytes, payload: Mapping[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(dict(payload))).hexdigest()


def _sha(value: object) -> str:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise _error("DETECTOR_V2_DIGEST_INVALID")
    return value


def _timestamp(value: object) -> str:
    if type(value) is not str or _RFC3339.fullmatch(value) is None:
        raise _error("DETECTOR_V2_TIMESTAMP_INVALID")
    return value


def _stream_document(value: MonitorStreamKey) -> dict[str, object]:
    if type(value) is not MonitorStreamKey:
        raise _error("DETECTOR_V2_STREAM_INVALID")
    rebuilt = MonitorStreamKey(
        value.stream_id, value.metric, value.threshold_stratum,
        value.configuration_identity, value.data_identity,
        value.flat_binding_id, value.hnsw_binding_id,
    )
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("DETECTOR_V2_STREAM_INVALID")
    return {
        "stream_id": value.stream_id,
        "metric": value.metric.value,
        "threshold_stratum": value.threshold_stratum,
        "configuration_identity": value.configuration_identity,
        "data_identity": value.data_identity,
        "flat_binding_id": value.flat_binding_id,
        "hnsw_binding_id": value.hnsw_binding_id,
    }


def _stream_from_document(value: object) -> MonitorStreamKey:
    required = {
        "stream_id", "metric", "threshold_stratum", "configuration_identity",
        "data_identity", "flat_binding_id", "hnsw_binding_id",
    }
    if type(value) is not dict or set(value) != required:
        raise _error("DETECTOR_V2_STREAM_INVALID")
    try:
        return MonitorStreamKey(
            value["stream_id"], Metric(value["metric"]), value["threshold_stratum"],
            value["configuration_identity"], value["data_identity"],
            value["flat_binding_id"], value["hnsw_binding_id"],
        )
    except (TypeError, ValueError) as exc:
        raise _error("DETECTOR_V2_STREAM_INVALID") from exc


class HostWindowV2Status(StrEnum):
    WINDOW_INCOMPLETE = "WINDOW_INCOMPLETE"
    WINDOW_UNEVALUABLE = "WINDOW_UNEVALUABLE"
    READY = "READY"
    REBASELINE = "REBASELINE"
    EVALUATED = "EVALUATED"


@dataclass(frozen=True, slots=True)
class V2ShadowPositionEvidence:
    schema_version: str
    source_sequence: int
    window_sequence: int
    within_window_index: int
    source_sha256: str
    stream_key: MonitorStreamKey
    evaluation_eligible: bool
    reason_codes: tuple[str, ...]
    evaluation_evidence_sha256: str | None
    shadow_position_sha256: str


def _position_payload(
    *, source_sequence: int, window_sequence: int, within_window_index: int,
    source_sha256: str, stream_key: MonitorStreamKey,
    evaluation_eligible: bool, reason_codes: tuple[str, ...],
    evaluation_evidence_sha256: str | None,
) -> dict[str, object]:
    if (
        type(source_sequence) is not int or source_sequence < 0
        or type(window_sequence) is not int or window_sequence < 0
        or type(within_window_index) is not int
        or not 0 <= within_window_index < 200
        or source_sequence != window_sequence * 200 + within_window_index
    ):
        raise _error("SHADOW_V2_POSITION_INVALID")
    _sha(source_sha256)
    if type(evaluation_eligible) is not bool:
        raise _error("SHADOW_V2_ELIGIBILITY_INVALID")
    if (
        type(reason_codes) is not tuple
        or any(type(item) is not str or not item for item in reason_codes)
        or len(set(reason_codes)) != len(reason_codes)
    ):
        raise _error("SHADOW_V2_REASON_INVALID")
    if evaluation_eligible:
        if reason_codes or evaluation_evidence_sha256 is None:
            raise _error("SHADOW_V2_ELIGIBILITY_INVALID")
        _sha(evaluation_evidence_sha256)
    elif not reason_codes or evaluation_evidence_sha256 is not None:
        raise _error("SHADOW_V2_ELIGIBILITY_INVALID")
    return {
        "schema_version": _POSITION_SCHEMA,
        "source_sequence": source_sequence,
        "window_sequence": window_sequence,
        "within_window_index": within_window_index,
        "source_sha256": source_sha256,
        "stream": _stream_document(stream_key),
        "evaluation_eligible": evaluation_eligible,
        "reason_codes": list(reason_codes),
        "evaluation_evidence_sha256": evaluation_evidence_sha256,
    }


def _position_document(value: V2ShadowPositionEvidence) -> dict[str, object]:
    if type(value) is not V2ShadowPositionEvidence:
        raise _error("SHADOW_V2_POSITION_INVALID")
    payload = _position_payload(
        source_sequence=value.source_sequence,
        window_sequence=value.window_sequence,
        within_window_index=value.within_window_index,
        source_sha256=value.source_sha256,
        stream_key=value.stream_key,
        evaluation_eligible=value.evaluation_eligible,
        reason_codes=value.reason_codes,
        evaluation_evidence_sha256=value.evaluation_evidence_sha256,
    )
    digest = _digest(_POSITION_DOMAIN, payload)
    if value.schema_version != _POSITION_SCHEMA or not hmac.compare_digest(
        value.shadow_position_sha256, digest
    ):
        raise _error("SHADOW_V2_POSITION_INVALID")
    return {"position_payload": payload, "shadow_position_sha256": digest}


def _position_from_document(value: object) -> V2ShadowPositionEvidence:
    if type(value) is not dict or set(value) != {"position_payload", "shadow_position_sha256"}:
        raise _error("SHADOW_V2_POSITION_INVALID")
    payload = value["position_payload"]
    required = {
        "schema_version", "source_sequence", "window_sequence",
        "within_window_index", "source_sha256", "stream",
        "evaluation_eligible", "reason_codes", "evaluation_evidence_sha256",
    }
    if type(payload) is not dict or set(payload) != required or payload["schema_version"] != _POSITION_SCHEMA:
        raise _error("SHADOW_V2_POSITION_INVALID")
    if type(payload["reason_codes"]) is not list:
        raise _error("SHADOW_V2_POSITION_INVALID")
    reasons = tuple(payload["reason_codes"])
    stream = _stream_from_document(payload["stream"])
    canonical = _position_payload(
        source_sequence=payload["source_sequence"],
        window_sequence=payload["window_sequence"],
        within_window_index=payload["within_window_index"],
        source_sha256=payload["source_sha256"], stream_key=stream,
        evaluation_eligible=payload["evaluation_eligible"], reason_codes=reasons,
        evaluation_evidence_sha256=payload["evaluation_evidence_sha256"],
    )
    digest = _digest(_POSITION_DOMAIN, canonical)
    if canonical_json_bytes(canonical) != canonical_json_bytes(payload) or value["shadow_position_sha256"] != digest:
        raise _error("SHADOW_V2_POSITION_INVALID")
    return V2ShadowPositionEvidence(
        _POSITION_SCHEMA, payload["source_sequence"], payload["window_sequence"],
        payload["within_window_index"], payload["source_sha256"], stream,
        payload["evaluation_eligible"], reasons,
        payload["evaluation_evidence_sha256"], digest,
    )


def build_v2_shadow_position(
    *, source: CommittedHostObservation, evaluation_eligible: bool,
    reason_codes: tuple[str, ...] = (),
    evaluation_evidence_sha256: str | None = None,
) -> V2ShadowPositionEvidence:
    if type(source) is not CommittedHostObservation:
        raise _error("SHADOW_V2_SOURCE_INVALID")
    payload = _position_payload(
        source_sequence=source.source_sequence,
        window_sequence=source.window_sequence,
        within_window_index=source.within_window_index,
        source_sha256=source.source_sha256, stream_key=source.stream_key,
        evaluation_eligible=evaluation_eligible, reason_codes=reason_codes,
        evaluation_evidence_sha256=evaluation_evidence_sha256,
    )
    return V2ShadowPositionEvidence(
        _POSITION_SCHEMA, source.source_sequence, source.window_sequence,
        source.within_window_index, source.source_sha256, source.stream_key,
        evaluation_eligible, reason_codes, evaluation_evidence_sha256,
        _digest(_POSITION_DOMAIN, payload),
    )


def _source_window_digest(
    *, stream_key: MonitorStreamKey, window_sequence: int,
    source_digests: tuple[str, ...],
) -> str:
    if type(window_sequence) is not int or window_sequence < 0 or len(source_digests) != 200:
        raise _error("SOURCE_WINDOW_INCOMPLETE")
    for value in source_digests:
        _sha(value)
    return _digest(
        _SOURCE_WINDOW_DOMAIN,
        {
            "schema_version": "response-profile-host-source-window-v2",
            "stream": _stream_document(stream_key),
            "window_sequence": window_sequence,
            "source_sha256": list(source_digests),
        },
    )


def source_window_sha256(sources: tuple[CommittedHostObservation, ...]) -> str:
    if type(sources) is not tuple or len(sources) != 200:
        raise _error("SOURCE_WINDOW_INCOMPLETE")
    try:
        verified = tuple(verify_committed_host_observation(item) for item in sources)
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        raise _error("SOURCE_WINDOW_INVALID") from exc
    first = verified[0]
    for offset, item in enumerate(verified):
        if (
            type(item) is not CommittedHostObservation
            or item.stream_key != first.stream_key
            or item.window_sequence != first.window_sequence
            or item.source_sequence != first.window_sequence * 200 + offset
            or item.within_window_index != offset
        ):
            raise _error("SOURCE_WINDOW_LINEAGE_INVALID")
    return _source_window_digest(
        stream_key=first.stream_key, window_sequence=first.window_sequence,
        source_digests=tuple(item.source_sha256 for item in verified),
    )


@dataclass(frozen=True, slots=True)
class V2ShadowWindow:
    schema_version: str
    stream_key: MonitorStreamKey
    window_sequence: int
    source_window_sha256: str | None
    positions: tuple[V2ShadowPositionEvidence, ...]
    status: HostWindowV2Status
    reason_codes: tuple[str, ...]
    shadow_window_sha256: str | None


def _window_payload(
    *, stream_key: MonitorStreamKey, window_sequence: int,
    source_window_digest: str, positions: tuple[V2ShadowPositionEvidence, ...],
    status: HostWindowV2Status, reason_codes: tuple[str, ...],
) -> dict[str, object]:
    if status not in {HostWindowV2Status.READY, HostWindowV2Status.WINDOW_UNEVALUABLE}:
        raise _error("SHADOW_WINDOW_STATUS_INVALID")
    _sha(source_window_digest)
    if type(positions) is not tuple or len(positions) != 200:
        raise _error("SHADOW_WINDOW_POSITION_COUNT_INVALID")
    documents = tuple(_position_document(item) for item in positions)
    for offset, position in enumerate(positions):
        if (
            position.stream_key != stream_key
            or position.window_sequence != window_sequence
            or position.source_sequence != window_sequence * 200 + offset
            or position.within_window_index != offset
        ):
            raise _error("SHADOW_POSITION_SUBSTITUTED")
    if _source_window_digest(
        stream_key=stream_key, window_sequence=window_sequence,
        source_digests=tuple(item.source_sha256 for item in positions),
    ) != source_window_digest:
        raise _error("SHADOW_SOURCE_WINDOW_SUBSTITUTED")
    derived_status = (
        HostWindowV2Status.READY
        if all(item.evaluation_eligible for item in positions)
        else HostWindowV2Status.WINDOW_UNEVALUABLE
    )
    derived_reasons = tuple(dict.fromkeys(
        (() if derived_status is HostWindowV2Status.READY else ("WINDOW_UNEVALUABLE",))
        + tuple(reason for item in positions for reason in item.reason_codes)
    ))
    if status is not derived_status or reason_codes != derived_reasons:
        raise _error("SHADOW_WINDOW_STATUS_INVALID")
    return {
        "schema_version": _WINDOW_SCHEMA,
        "stream": _stream_document(stream_key),
        "window_sequence": window_sequence,
        "source_window_sha256": source_window_digest,
        "positions": list(documents),
        "status": status.value,
        "reason_codes": list(reason_codes),
    }


def _window_document(value: V2ShadowWindow) -> dict[str, object]:
    if type(value) is not V2ShadowWindow or value.source_window_sha256 is None or value.shadow_window_sha256 is None:
        raise _error("SHADOW_WINDOW_INVALID")
    payload = _window_payload(
        stream_key=value.stream_key, window_sequence=value.window_sequence,
        source_window_digest=value.source_window_sha256, positions=value.positions,
        status=value.status, reason_codes=value.reason_codes,
    )
    digest = _digest(_WINDOW_DOMAIN, payload)
    if value.schema_version != _WINDOW_SCHEMA or value.shadow_window_sha256 != digest:
        raise _error("SHADOW_WINDOW_INVALID")
    return {"window_payload": payload, "shadow_window_sha256": digest}


def _window_from_document(value: object) -> V2ShadowWindow:
    if type(value) is not dict or set(value) != {"window_payload", "shadow_window_sha256"}:
        raise _error("SHADOW_WINDOW_INVALID")
    payload = value["window_payload"]
    required = {
        "schema_version", "stream", "window_sequence", "source_window_sha256",
        "positions", "status", "reason_codes",
    }
    if type(payload) is not dict or set(payload) != required or payload["schema_version"] != _WINDOW_SCHEMA:
        raise _error("SHADOW_WINDOW_INVALID")
    if type(payload["positions"]) is not list or type(payload["reason_codes"]) is not list:
        raise _error("SHADOW_WINDOW_INVALID")
    try:
        status = HostWindowV2Status(payload["status"])
    except (TypeError, ValueError) as exc:
        raise _error("SHADOW_WINDOW_INVALID") from exc
    stream = _stream_from_document(payload["stream"])
    positions = tuple(_position_from_document(item) for item in payload["positions"])
    reasons = tuple(payload["reason_codes"])
    canonical = _window_payload(
        stream_key=stream, window_sequence=payload["window_sequence"],
        source_window_digest=payload["source_window_sha256"], positions=positions,
        status=status, reason_codes=reasons,
    )
    digest = _digest(_WINDOW_DOMAIN, canonical)
    if canonical_json_bytes(canonical) != canonical_json_bytes(payload) or value["shadow_window_sha256"] != digest:
        raise _error("SHADOW_WINDOW_INVALID")
    return V2ShadowWindow(
        _WINDOW_SCHEMA, stream, payload["window_sequence"],
        payload["source_window_sha256"], positions, status, reasons, digest,
    )


def build_v2_shadow_window(
    *, sources: tuple[CommittedHostObservation, ...],
    positions: tuple[V2ShadowPositionEvidence, ...],
) -> V2ShadowWindow:
    if type(sources) is not tuple or not sources or len(sources) > 200:
        raise _error("SOURCE_WINDOW_INVALID")
    try:
        verified_sources = tuple(
            verify_committed_host_observation(item) for item in sources
        )
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        raise _error("SOURCE_WINDOW_INVALID") from exc
    first = verified_sources[0]
    for offset, item in enumerate(verified_sources):
        if (
            type(item) is not CommittedHostObservation
            or item.stream_key != first.stream_key
            or item.window_sequence != first.window_sequence
            or item.source_sequence != first.window_sequence * 200 + offset
            or item.within_window_index != offset
        ):
            raise _error("SOURCE_WINDOW_LINEAGE_INVALID")
    if len(verified_sources) < 200:
        if positions:
            raise _error("INCOMPLETE_WINDOW_HAS_SHADOW_EVIDENCE")
        return V2ShadowWindow(
            _WINDOW_SCHEMA, first.stream_key, first.window_sequence, None, (),
            HostWindowV2Status.WINDOW_INCOMPLETE, ("WINDOW_INCOMPLETE",), None,
        )
    if type(positions) is not tuple or len(positions) != 200:
        raise _error("SHADOW_WINDOW_POSITION_COUNT_INVALID")
    for source, position in zip(verified_sources, positions, strict=True):
        if (
            type(position) is not V2ShadowPositionEvidence
            or position.source_sequence != source.source_sequence
            or position.source_sha256 != source.source_sha256
            or position.stream_key != source.stream_key
        ):
            raise _error("SHADOW_POSITION_SUBSTITUTED")
    status = (
        HostWindowV2Status.READY
        if all(item.evaluation_eligible for item in positions)
        else HostWindowV2Status.WINDOW_UNEVALUABLE
    )
    reasons = tuple(dict.fromkeys(
        (() if status is HostWindowV2Status.READY else ("WINDOW_UNEVALUABLE",))
        + tuple(reason for item in positions for reason in item.reason_codes)
    ))
    source_digest = source_window_sha256(verified_sources)
    payload = _window_payload(
        stream_key=first.stream_key, window_sequence=first.window_sequence,
        source_window_digest=source_digest, positions=positions,
        status=status, reason_codes=reasons,
    )
    return V2ShadowWindow(
        _WINDOW_SCHEMA, first.stream_key, first.window_sequence, source_digest,
        positions, status, reasons, _digest(_WINDOW_DOMAIN, payload),
    )


def _provenance_document(value: EvidenceProvenance) -> dict[str, object]:
    if type(value) is not EvidenceProvenance or not evidence_provenance_valid(value):
        raise _error("DETECTOR_V2_PROVENANCE_INVALID")
    return {
        "schema_version": value.schema_version, "metric": value.metric.value,
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


def _provenance_from_document(value: object) -> EvidenceProvenance:
    required = {
        "schema_version", "metric", "threshold_stratum", "reference_window_id",
        "current_window_id", "reference_manifest_sha256", "current_manifest_sha256",
        "configuration_identity", "data_identity", "flat_binding_id",
        "hnsw_binding_id", "reference_audit_ids", "reference_audit_rank_digests",
        "current_audit_ids", "current_audit_rank_digests", "sha256",
    }
    if type(value) is not dict or set(value) != required:
        raise _error("DETECTOR_V2_PROVENANCE_INVALID")
    try:
        rebuilt = build_evidence_provenance(
            metric=Metric(value["metric"]), threshold_stratum=value["threshold_stratum"],
            reference_window_id=value["reference_window_id"],
            current_window_id=value["current_window_id"],
            reference_manifest_sha256=value["reference_manifest_sha256"],
            current_manifest_sha256=value["current_manifest_sha256"],
            configuration_identity=value["configuration_identity"],
            data_identity=value["data_identity"], flat_binding_id=value["flat_binding_id"],
            hnsw_binding_id=value["hnsw_binding_id"],
            reference_audit_ids=tuple(value["reference_audit_ids"]),
            reference_audit_rank_digests=tuple(value["reference_audit_rank_digests"]),
            current_audit_ids=tuple(value["current_audit_ids"]),
            current_audit_rank_digests=tuple(value["current_audit_rank_digests"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("DETECTOR_V2_PROVENANCE_INVALID") from exc
    if value["schema_version"] != rebuilt.schema_version or value["sha256"] != rebuilt.sha256:
        raise _error("DETECTOR_V2_PROVENANCE_INVALID")
    return rebuilt


@dataclass(frozen=True, slots=True, init=False)
class V2DetectorHead:
    schema_version: str
    stream_key: MonitorStreamKey
    reference_window_sequence: int
    reference_source_window_sha256: str
    current_window_sequence: int
    current_source_window_sha256: str
    current_shadow_window_sha256: str
    detector_state: DetectorState
    detector_classification: DriftClassification
    detector_provenance: EvidenceProvenance
    detector_head_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("v2 detector heads are store-issued")

    @property
    def window_sequence(self) -> int:
        return self.current_window_sequence


_HEAD_DETECTOR_STATES = frozenset(
    {
        DetectorState.DRIFT,
        DetectorState.NO_DRIFT,
        # ADR-014 item 7: evidence-bearing, never trigger-bearing. The first
        # evaluated comparison under a new reference is necessarily
        # INSUFFICIENT_EVIDENCE (ADR-002 has no previous WindowEvidence yet),
        # and must be durable so it can become that reference epoch's attested
        # previous evidence. `latest_drift_head` and the EXP-010 capture gate
        # both continue to require DRIFT.
        DetectorState.INSUFFICIENT_EVIDENCE,
    }
)


def _head_payload(
    *, reference: V2ShadowWindow, current: V2ShadowWindow,
    decision: DriftDecision,
) -> dict[str, object]:
    provenance = decision.evidence_provenance
    if (
        reference.status is not HostWindowV2Status.READY
        or current.status is not HostWindowV2Status.READY
        or reference.stream_key != current.stream_key
        or type(decision) is not DriftDecision
        or decision.state not in _HEAD_DETECTOR_STATES
        or type(decision.classification) is not DriftClassification
        # ADR-014 item 7: DRIFT iff classified. NO_DRIFT and
        # INSUFFICIENT_EVIDENCE must both carry NONE. Enforced here, at the
        # earliest boundary, so an inconsistent pair can never be digested.
        or (decision.state is DetectorState.DRIFT)
        != (decision.classification is not DriftClassification.NONE)
        or provenance is None or not evidence_provenance_valid(provenance)
        or provenance.reference_window_id != reference.window_sequence
        or provenance.current_window_id != current.window_sequence
        # ADR-014 clarification: `EvidenceProvenance.*_manifest_sha256` is the
        # AssembledShadowWindow manifest digest, a *different canonical domain*
        # from `source_window_sha256` (committed-source membership). The head
        # must not assert equality across those domains. Source and shadow
        # binding stay below and in the head's own fields; equality of the
        # provenance manifests against the exact assembled windows is proven by
        # ADR-014's RealDetectorAttestation instead.
        or provenance.metric is not current.stream_key.metric
        or provenance.threshold_stratum != current.stream_key.threshold_stratum
        or provenance.configuration_identity != current.stream_key.configuration_identity
        or provenance.data_identity != current.stream_key.data_identity
        or provenance.flat_binding_id != current.stream_key.flat_binding_id
        or provenance.hnsw_binding_id != current.stream_key.hnsw_binding_id
    ):
        raise _error("DETECTOR_V2_DECISION_INVALID")
    return {
        "schema_version": DETECTOR_V2_HEAD_SCHEMA_VERSION,
        "stream": _stream_document(current.stream_key),
        "reference_window_sequence": reference.window_sequence,
        "reference_source_window_sha256": reference.source_window_sha256,
        "current_window_sequence": current.window_sequence,
        "current_source_window_sha256": current.source_window_sha256,
        "current_shadow_window_sha256": current.shadow_window_sha256,
        "detector_state": decision.state.value,
        "detector_classification": decision.classification.value,
        "detector_provenance_sha256": provenance.sha256,
    }


def _make_head(
    *, reference: V2ShadowWindow, current: V2ShadowWindow,
    decision: DriftDecision,
) -> V2DetectorHead:
    payload = _head_payload(reference=reference, current=current, decision=decision)
    result = object.__new__(V2DetectorHead)
    values = {
        "schema_version": DETECTOR_V2_HEAD_SCHEMA_VERSION,
        "stream_key": current.stream_key,
        "reference_window_sequence": reference.window_sequence,
        "reference_source_window_sha256": reference.source_window_sha256,
        "current_window_sequence": current.window_sequence,
        "current_source_window_sha256": current.source_window_sha256,
        "current_shadow_window_sha256": current.shadow_window_sha256,
        "detector_state": decision.state,
        "detector_classification": decision.classification,
        "detector_provenance": decision.evidence_provenance,
        "detector_head_sha256": _digest(_HEAD_DOMAIN, payload),
    }
    for name, value in values.items():
        object.__setattr__(result, name, value)
    return result


def _head_document(value: V2DetectorHead) -> dict[str, object]:
    if type(value) is not V2DetectorHead:
        raise _error("DETECTOR_V2_HEAD_INVALID")
    provenance = value.detector_provenance
    if (
        type(value.reference_window_sequence) is not int
        or type(value.current_window_sequence) is not int
        or value.reference_window_sequence < 0
        or value.current_window_sequence <= value.reference_window_sequence
        or type(value.detector_state) is not DetectorState
        or type(value.detector_classification) is not DriftClassification
        or (value.detector_state is DetectorState.DRIFT)
        != (value.detector_classification is not DriftClassification.NONE)
        or type(provenance) is not EvidenceProvenance
        or not evidence_provenance_valid(provenance)
        or provenance.reference_window_id != value.reference_window_sequence
        or provenance.current_window_id != value.current_window_sequence
        # ADR-014 clarification: distinct digest domains -- see `_head_payload`.
        # The provenance value itself remains inside the head document and the
        # head digest, so it is still tamper-evident here; only the invalid
        # cross-domain equality is gone.
        or provenance.metric is not value.stream_key.metric
        or provenance.threshold_stratum != value.stream_key.threshold_stratum
        or provenance.configuration_identity != value.stream_key.configuration_identity
        or provenance.data_identity != value.stream_key.data_identity
        or provenance.flat_binding_id != value.stream_key.flat_binding_id
        or provenance.hnsw_binding_id != value.stream_key.hnsw_binding_id
    ):
        raise _error("DETECTOR_V2_HEAD_INVALID")
    _sha(value.reference_source_window_sha256)
    _sha(value.current_source_window_sha256)
    _sha(value.current_shadow_window_sha256)
    payload = {
        "schema_version": DETECTOR_V2_HEAD_SCHEMA_VERSION,
        "stream": _stream_document(value.stream_key),
        "reference_window_sequence": value.reference_window_sequence,
        "reference_source_window_sha256": value.reference_source_window_sha256,
        "current_window_sequence": value.current_window_sequence,
        "current_source_window_sha256": value.current_source_window_sha256,
        "current_shadow_window_sha256": value.current_shadow_window_sha256,
        "detector_state": value.detector_state.value,
        "detector_classification": value.detector_classification.value,
        "detector_provenance_sha256": provenance.sha256,
    }
    digest = _digest(_HEAD_DOMAIN, payload)
    if value.schema_version != DETECTOR_V2_HEAD_SCHEMA_VERSION or value.detector_head_sha256 != digest:
        raise _error("DETECTOR_V2_HEAD_INVALID")
    return {
        "head_payload": payload,
        "detector_provenance": _provenance_document(provenance),
        "detector_head_sha256": digest,
    }


def verify_v2_detector_head(value: object) -> V2DetectorHead:
    """Recompute every self-contained v2 head field and detached digest."""

    if type(value) is not V2DetectorHead:
        raise _error("DETECTOR_V2_HEAD_INVALID")
    _head_document(value)
    return value


def _head_from_document(
    value: object, *, reference: V2ShadowWindow, current: V2ShadowWindow,
) -> V2DetectorHead:
    if type(value) is not dict or set(value) != {"head_payload", "detector_provenance", "detector_head_sha256"}:
        raise _error("DETECTOR_V2_HEAD_INVALID")
    provenance = _provenance_from_document(value["detector_provenance"])
    payload = value["head_payload"]
    try:
        decision = DriftDecision(
            state=DetectorState(payload["detector_state"]),
            classification=DriftClassification(payload["detector_classification"]),
            evidence_provenance=provenance,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("DETECTOR_V2_HEAD_INVALID") from exc
    canonical = _head_payload(reference=reference, current=current, decision=decision)
    digest = _digest(_HEAD_DOMAIN, canonical)
    if canonical_json_bytes(canonical) != canonical_json_bytes(payload) or value["detector_head_sha256"] != digest:
        raise _error("DETECTOR_V2_HEAD_INVALID")
    return _make_head(reference=reference, current=current, decision=decision)


@dataclass(frozen=True, slots=True)
class V2DetectorProcessResult:
    status: HostWindowV2Status
    window_sequence: int
    reason_codes: tuple[str, ...]
    detector_head: V2DetectorHead | None


@dataclass(frozen=True, slots=True)
class PersistedV2DetectorWindow:
    """Fully reconstructed durable detector event for one canonical window."""

    result: V2DetectorProcessResult
    source_window_sha256: str
    shadow_window_sha256: str
    event_sha256: str


@dataclass(frozen=True, slots=True)
class V2DetectorProgression:
    """Verified current progression needed by the reconciliation coordinator."""

    next_window_sequence: int
    reference_window_sequence: int | None
    reference_source_window_sha256: str | None
    reference_shadow_window_sha256: str | None
    requires_rebaseline: bool


_ISSUE_TOKEN = object()


@dataclass(frozen=True, slots=True, init=False)
class VerifiedLatestV2DetectorHead:
    head: V2DetectorHead
    head_record_sequence: int
    head_record_sha256: str
    head_record_persisted_at_utc: str

    def __init__(self, *, _token: object, **values: object) -> None:
        if _token is not _ISSUE_TOKEN:
            raise TypeError("verified latest v2 heads are store-issued")
        for name, value in values.items():
            object.__setattr__(self, name, value)


_SCHEMA_SQL = (
    "CREATE TABLE detector_binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), binding_json BLOB NOT NULL, binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256)=64)) STRICT",
    "CREATE TABLE detector_events (window_sequence INTEGER PRIMARY KEY CHECK(window_sequence>=0), event_json BLOB NOT NULL, previous_event_sha256 TEXT, event_sha256 TEXT NOT NULL UNIQUE CHECK(length(event_sha256)=64)) STRICT",
    "CREATE TRIGGER detector_binding_no_update BEFORE UPDATE ON detector_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER detector_binding_no_delete BEFORE DELETE ON detector_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER detector_events_no_update BEFORE UPDATE ON detector_events BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER detector_events_no_delete BEFORE DELETE ON detector_events BEGIN SELECT RAISE(ABORT,'append-only'); END",
)


def _normalize_sql(value: str) -> str:
    return " ".join(value.strip().removesuffix(";").split())


class SQLiteHostWindowDetectorV2Store:
    """Exclusive-writer append-only detector-v2 state and latest-head store."""

    def __init__(self, path: str | os.PathLike[str], *, stream_key: MonitorStreamKey) -> None:
        self.path = Path(path)
        self.stream_key = stream_key
        self._binding = {"schema_version": _STORE_BINDING_SCHEMA, "stream": _stream_document(stream_key)}
        self._mutex = threading.RLock()
        self._closed = False
        self._pid = os.getpid()
        self._lock_handle = None
        self._lock_inode: tuple[int, int] | None = None
        self._open()

    def __enter__(self) -> "SQLiteHostWindowDetectorV2Store":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _open(self) -> None:
        parent = self.path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = parent.stat(follow_symlinks=False)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o022:
            raise _error("DETECTOR_V2_PATH_UNSAFE")
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        self._lock_handle = os.fdopen(fd, "a+b")
        lock_info = os.fstat(fd)
        if not stat.S_ISREG(lock_info.st_mode) or lock_info.st_nlink != 1 or lock_info.st_uid != os.geteuid() or stat.S_IMODE(lock_info.st_mode) != 0o600:
            self.close()
            raise _error("DETECTOR_V2_PATH_UNSAFE")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.close()
            raise _error("DETECTOR_V2_STORE_BUSY") from exc
        inode = (lock_info.st_dev, lock_info.st_ino)
        with _OWNERSHIP_LOCK:
            if inode in _OWNED_LOCK_INODES:
                self.close()
                raise _error("DETECTOR_V2_STORE_BUSY")
            _OWNED_LOCK_INODES.add(inode)
        self._lock_inode = inode
        created = not self.path.exists()
        if created:
            database_fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
            os.close(database_fd)
        self._verify_path()
        self._db = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        try:
            if created:
                self._db.execute("PRAGMA journal_mode=DELETE")
                self._db.execute("PRAGMA synchronous=FULL")
                self._db.execute("PRAGMA trusted_schema=OFF")
                self._db.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA_SQL:
                    self._db.execute(statement)
                self._db.execute(f"PRAGMA user_version={_DB_VERSION}")
                self._db.execute(
                    "INSERT INTO detector_binding VALUES(1,?,?)",
                    (canonical_json_bytes(self._binding), _digest(_BINDING_DOMAIN, self._binding)),
                )
                self._db.execute("COMMIT")
            else:
                if str(self._db.execute("PRAGMA journal_mode").fetchone()[0]).lower() != "delete":
                    raise _error("DETECTOR_V2_SCHEMA_INVALID")
                self._db.execute("PRAGMA synchronous=FULL")
                self._db.execute("PRAGMA trusted_schema=OFF")
            self._reconstruct()
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        database = getattr(self, "_db", None)
        if database is not None:
            database.close()
        if self._lock_handle is not None and not self._lock_handle.closed:
            try:
                fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._lock_handle.close()
        if self._lock_inode is not None:
            with _OWNERSHIP_LOCK:
                _OWNED_LOCK_INODES.discard(self._lock_inode)
            self._lock_inode = None

    def _verify_path(self) -> None:
        info = os.lstat(self.path)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise _error("DETECTOR_V2_PATH_UNSAFE")

    def _require_live(self) -> None:
        if self._closed:
            raise _error("DETECTOR_V2_STORE_CLOSED")
        if os.getpid() != self._pid:
            raise _error("DETECTOR_V2_STORE_FORKED")
        self._verify_path()

    def _verify_schema(self) -> None:
        if self._db.execute("PRAGMA user_version").fetchone()[0] != _DB_VERSION:
            raise _error("DETECTOR_V2_SCHEMA_INVALID")
        actual = {
            row[0]: _normalize_sql(row[1])
            for row in self._db.execute("SELECT name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'")
        }
        expected = {statement.split()[2]: _normalize_sql(statement) for statement in _SCHEMA_SQL}
        if actual != expected:
            raise _error("DETECTOR_V2_SCHEMA_INVALID")
        row = self._db.execute("SELECT binding_json,binding_sha256 FROM detector_binding WHERE singleton=1").fetchone()
        if row is None or bytes(row[0]) != canonical_json_bytes(self._binding) or row[1] != _digest(_BINDING_DOMAIN, self._binding):
            raise _error("DETECTOR_V2_BINDING_INVALID")

    def _documents(self) -> tuple[dict[str, object], ...]:
        documents = []
        previous = None
        for expected, row in enumerate(self._db.execute(
            "SELECT window_sequence,event_json,previous_event_sha256,event_sha256 FROM detector_events ORDER BY window_sequence"
        )):
            try:
                document = json.loads(bytes(row[1]).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _error("DETECTOR_V2_HISTORY_INVALID") from exc
            if (
                row[0] != expected or row[2] != previous
                or canonical_json_bytes(document) != bytes(row[1])
                or document.get("window_sequence") != expected
                or document.get("previous_event_sha256") != previous
                or row[3] != _digest(_EVENT_DOMAIN, document)
            ):
                raise _error("DETECTOR_V2_HISTORY_INVALID")
            previous = row[3]
            documents.append(document)
        return tuple(documents)

    def _reconstruct(self) -> tuple[V2ShadowWindow | None, bool, V2DetectorHead | None, int, tuple[dict[str, object], ...]]:
        self._require_live()
        self._verify_schema()
        documents = self._documents()
        reference: V2ShadowWindow | None = None
        gap = False
        latest: V2DetectorHead | None = None
        head_count = 0
        persisted_windows: list[PersistedV2DetectorWindow] = []
        for sequence, document in enumerate(documents):
            required = {
                "schema_version", "stream", "window_sequence", "window",
                "status", "reason_codes", "reference_window_sequence",
                "reference_source_window_sha256", "head_record_sequence",
                "detector_head", "persisted_at_utc", "previous_event_sha256",
            }
            if type(document) is not dict or set(document) != required or document["schema_version"] != _EVENT_SCHEMA:
                raise _error("DETECTOR_V2_HISTORY_INVALID")
            if (
                _stream_from_document(document["stream"]) != self.stream_key
                or type(document["reason_codes"]) is not list
                or any(type(item) is not str or not item for item in document["reason_codes"])
            ):
                raise _error("DETECTOR_V2_HISTORY_INVALID")
            _timestamp(document["persisted_at_utc"])
            window = _window_from_document(document["window"])
            if window.stream_key != self.stream_key or window.window_sequence != sequence:
                raise _error("DETECTOR_V2_HISTORY_INVALID")
            try:
                status = HostWindowV2Status(document["status"])
            except (TypeError, ValueError) as exc:
                raise _error("DETECTOR_V2_HISTORY_INVALID") from exc
            if window.status is HostWindowV2Status.WINDOW_UNEVALUABLE:
                if status is not HostWindowV2Status.WINDOW_UNEVALUABLE or document["detector_head"] is not None or document["head_record_sequence"] is not None:
                    raise _error("DETECTOR_V2_HISTORY_INVALID")
                gap = True
                latest = None
                persisted_windows.append(
                    PersistedV2DetectorWindow(
                        result=V2DetectorProcessResult(
                            status, sequence, tuple(document["reason_codes"]), None
                        ),
                        source_window_sha256=window.source_window_sha256,
                        shadow_window_sha256=window.shadow_window_sha256,
                        event_sha256=_digest(_EVENT_DOMAIN, document),
                    )
                )
                continue
            if reference is None or gap:
                if status is not HostWindowV2Status.REBASELINE or document["detector_head"] is not None or document["head_record_sequence"] is not None:
                    raise _error("DETECTOR_V2_HISTORY_INVALID")
                reference = window
                gap = False
                latest = None
                if (
                    document["reference_window_sequence"] != window.window_sequence
                    or document["reference_source_window_sha256"] != window.source_window_sha256
                ):
                    raise _error("DETECTOR_V2_HISTORY_INVALID")
                persisted_windows.append(
                    PersistedV2DetectorWindow(
                        result=V2DetectorProcessResult(
                            status, sequence, tuple(document["reason_codes"]), None
                        ),
                        source_window_sha256=window.source_window_sha256,
                        shadow_window_sha256=window.shadow_window_sha256,
                        event_sha256=_digest(_EVENT_DOMAIN, document),
                    )
                )
                continue
            if status is not HostWindowV2Status.EVALUATED or document["head_record_sequence"] != head_count:
                raise _error("DETECTOR_V2_HISTORY_INVALID")
            head = _head_from_document(document["detector_head"], reference=reference, current=window)
            if (
                document["reference_window_sequence"] != reference.window_sequence
                or document["reference_source_window_sha256"] != reference.source_window_sha256
            ):
                raise _error("DETECTOR_V2_HISTORY_INVALID")
            latest = head
            head_count += 1
            persisted_windows.append(
                PersistedV2DetectorWindow(
                    result=V2DetectorProcessResult(
                        status, sequence, tuple(document["reason_codes"]), head
                    ),
                    source_window_sha256=window.source_window_sha256,
                    shadow_window_sha256=window.shadow_window_sha256,
                    event_sha256=_digest(_EVENT_DOMAIN, document),
                )
            )
        self._reference = reference
        self._gap = gap
        self._latest_head = latest
        self._head_count = head_count
        self._persisted_windows = tuple(persisted_windows)
        return reference, gap, latest, head_count, documents

    def load_persisted_window(
        self, window_sequence: int
    ) -> PersistedV2DetectorWindow | None:
        """Load one exact canonical event without replaying detector processing."""

        with self._mutex:
            self._reconstruct()
            if type(window_sequence) is not int or window_sequence < 0:
                raise _error("DETECTOR_V2_WINDOW_SEQUENCE_INVALID")
            if window_sequence >= len(self._persisted_windows):
                return None
            return self._persisted_windows[window_sequence]

    def load_progression(self) -> V2DetectorProgression:
        """Return verified reference and next-window state for reconciliation."""

        with self._mutex:
            reference, gap, _latest, _count, documents = self._reconstruct()
            return V2DetectorProgression(
                next_window_sequence=len(documents),
                reference_window_sequence=(
                    None if reference is None else reference.window_sequence
                ),
                reference_source_window_sha256=(
                    None if reference is None else reference.source_window_sha256
                ),
                reference_shadow_window_sha256=(
                    None if reference is None else reference.shadow_window_sha256
                ),
                requires_rebaseline=reference is None or gap,
            )

    def process_window(
        self, *, window: V2ShadowWindow,
        evaluator: Callable[[V2ShadowWindow, V2ShadowWindow], DriftDecision],
        persisted_at_utc: str,
    ) -> V2DetectorProcessResult:
        """Advance the durable window progression by exactly one window.

        ``evaluator`` is caller-supplied and UNVERIFIED: this store persists
        whatever ``DriftDecision`` it returns after checking only that the
        decision's provenance binds these exact source windows.  It never calls
        ``vdbench.drift.evaluate_drift``.  A head issued here therefore proves
        lineage, binding, and durable progression -- never that a real governed
        statistical detector executed.  See this module's docstring
        ("Evaluator trust boundary"): fake/structural evaluator heads are
        offline-only, a real EXP-010 trigger needs a separately governed
        real-detector attestation, and no policy, grant, routing, or candidate
        authority is created here.
        """

        with self._mutex:
            if window.status is HostWindowV2Status.WINDOW_INCOMPLETE:
                return V2DetectorProcessResult(window.status, window.window_sequence, window.reason_codes, None)
            canonical_window = _window_from_document(_window_document(window))
            reference, gap, _latest, head_count, documents = self._reconstruct()
            if canonical_window.window_sequence != len(documents):
                raise _error("DETECTOR_V2_WINDOW_SEQUENCE_INVALID")
            persisted = _timestamp(persisted_at_utc)
            head = None
            head_record_sequence = None
            if canonical_window.status is HostWindowV2Status.WINDOW_UNEVALUABLE:
                status = HostWindowV2Status.WINDOW_UNEVALUABLE
                reasons = canonical_window.reason_codes
            elif reference is None or gap:
                status = HostWindowV2Status.REBASELINE
                reasons = ("REFERENCE_ESTABLISHED",)
                event_reference = canonical_window
            else:
                decision = evaluator(reference, canonical_window)
                head = _make_head(reference=reference, current=canonical_window, decision=decision)
                status = HostWindowV2Status.EVALUATED
                reasons = decision.reason_codes
                head_record_sequence = head_count
                event_reference = reference
            if canonical_window.status is HostWindowV2Status.WINDOW_UNEVALUABLE:
                event_reference = reference
            previous = None if not documents else _digest(_EVENT_DOMAIN, documents[-1])
            payload = {
                "schema_version": _EVENT_SCHEMA,
                "stream": _stream_document(self.stream_key),
                "window_sequence": canonical_window.window_sequence,
                "window": _window_document(canonical_window),
                "status": status.value,
                "reason_codes": list(reasons),
                "reference_window_sequence": None if event_reference is None else event_reference.window_sequence,
                "reference_source_window_sha256": None if event_reference is None else event_reference.source_window_sha256,
                "head_record_sequence": head_record_sequence,
                "detector_head": None if head is None else _head_document(head),
                "persisted_at_utc": persisted,
                "previous_event_sha256": previous,
            }
            digest = _digest(_EVENT_DOMAIN, payload)
            try:
                self._db.execute("BEGIN IMMEDIATE")
                if self._documents() != documents:
                    raise _error("DETECTOR_V2_HEAD_DRIFT")
                self._db.execute(
                    "INSERT INTO detector_events VALUES(?,?,?,?)",
                    (canonical_window.window_sequence, canonical_json_bytes(payload), previous, digest),
                )
                self._db.execute("COMMIT")
            except HostWindowDetectorV2Error:
                try:
                    self._db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            except sqlite3.Error as exc:
                try:
                    self._db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise _error("DETECTOR_V2_WRITE_FAILED") from exc
            self._reconstruct()
            return V2DetectorProcessResult(status, canonical_window.window_sequence, reasons, head)

    def load_verified_latest(self, stream_key: MonitorStreamKey) -> VerifiedLatestV2DetectorHead | None:
        with self._mutex:
            _reference, _gap, head, _head_count, documents = self._reconstruct()
            if stream_key != self.stream_key:
                raise _error("DETECTOR_V2_STREAM_INVALID")
            if head is None:
                return None
            document = next(
                item for item in reversed(documents)
                if item["detector_head"] is not None
            )
            return VerifiedLatestV2DetectorHead(
                _token=_ISSUE_TOKEN, head=head,
                head_record_sequence=document["head_record_sequence"],
                head_record_sha256=_digest(_EVENT_DOMAIN, document),
                head_record_persisted_at_utc=document["persisted_at_utc"],
            )

    def latest_drift_head(self) -> V2DetectorHead | None:
        latest = self.load_verified_latest(self.stream_key)
        if latest is None or latest.head.detector_state is not DetectorState.DRIFT:
            return None
        return latest.head
