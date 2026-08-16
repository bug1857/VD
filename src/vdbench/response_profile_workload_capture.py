"""Durable, non-generating capture of genuine post-trigger query populations.

The module deliberately provides ports, not a live source implementation.  A
production composition root must inject observations emitted by a genuine
workload and a read-only metadata snapshot.  Tests may inject structural
fakes.  No Milvus, policy, routing, grant, oracle, or actuation dependency is
present here.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import threading
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol, Self

import numpy as np

from .artifacts import canonical_json_bytes, write_immutable_json
from .config import IndexTrack, Metric, SearchConfiguration
from .drift import DetectorState
from .response_profile_detector_head import (
    response_profile_detector_head_document,
    response_profile_detector_head_from_document,
)
from .response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    WARMUP_QUERY_COUNT,
    CalibrationPopulationManifest,
    LiveStreamSourceNamespace,
    ResponseProfileEvidenceContractError,
    ResponseProfileRoleKind,
    ResponseProfileRoleManifest,
    build_calibration_population_manifest,
    build_canonical_query_identity,
    build_live_stream_source_namespace,
    build_query_vector_identity,
    build_response_profile_cell,
    build_response_profile_query_payload,
    build_response_profile_replay_schedule,
    build_response_profile_role,
    build_response_profile_role_manifest,
    build_response_profile_role_member,
    calibration_population_document,
    role_manifest_document,
)
from .response_profile_lifecycle import (
    ResponseProfileRunBinding,
    build_response_profile_run_binding,
    response_profile_run_binding_document,
)
from .response_profile_monitor_store import (
    ResponseProfileMonitorStateStore,
    VerifiedLatestResponseProfileDetectorHead,
)
from .response_profile_vector_material import response_profile_vector_material_document
from .shadow_event_types import MonitorStreamKey

__all__ = [
    "CAPTURE_EVIDENCE_STATUS",
    "CaptureEnvironmentIdentity",
    "CapturePhase",
    "CapturedPopulationArtifacts",
    "GenuineWorkloadObservation",
    "GenuineWorkloadObservationSource",
    "ReadOnlyCaptureMetadataProvider",
    "ResponseProfileWorkloadCapture",
    "ResponseProfileWorkloadCaptureError",
    "build_capture_environment_identity",
]


CAPTURE_EVIDENCE_STATUS = "CAPTURE_INPUT_NOT_EXP011_PROSPECTIVE_EVIDENCE"
_SCHEMA_VERSION = 1
_BINDING_SCHEMA = "response-profile-workload-capture-binding-v1"
_EVENT_SCHEMA = "response-profile-workload-capture-event-v1"
_METADATA_SCHEMA = "response-profile-capture-environment-v1"
_OBSERVATION_SCHEMA = "response-profile-genuine-query-observation-v1"
_MANIFEST_SCHEMA = "response-profile-workload-capture-manifest-v1"
_ORDER_SCHEMA = "response-profile-workload-capture-order-v1"
_BINDING_DOMAIN = b"VD::RESPONSE_PROFILE_WORKLOAD_CAPTURE_BINDING::V1\x00"
_EVENT_DOMAIN = b"VD::RESPONSE_PROFILE_WORKLOAD_CAPTURE_EVENT::V1\x00"
_METADATA_DOMAIN = b"VD::RESPONSE_PROFILE_CAPTURE_ENVIRONMENT::V1\x00"
_MANIFEST_DOMAIN = b"VD::RESPONSE_PROFILE_WORKLOAD_CAPTURE_MANIFEST::V1\x00"
_ORDER_DOMAIN = b"VD::RESPONSE_PROFILE_WORKLOAD_CAPTURE_ORDER::V1\x00"
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_RFC3339_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)
_EVENT_KINDS = (
    "TRIGGERED",
    "QUERY_CAPTURED",
    "WARMUP_FROZEN",
    "CALIBRATION_FROZEN",
    "CAPTURE_COMPLETE",
    "CAPTURE_INVALIDATED",
)
_OWNERSHIP_LOCK = threading.Lock()
_OWNED_LOCK_INODES: set[tuple[int, int]] = set()


class ResponseProfileWorkloadCaptureError(RuntimeError):
    """Fail-closed capture error carrying one stable reason code."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileWorkloadCaptureError:
    return ResponseProfileWorkloadCaptureError(message, code=code)


def _text(value: object, *, field: str) -> str:
    if type(value) is not str or not value or unicodedata.normalize("NFC", value) != value:
        raise _error("CAPTURE_INPUT_INVALID", f"{field} must be non-empty NFC text")
    return value


def _sha(value: object, *, field: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise _error("CAPTURE_INPUT_INVALID", f"{field} must be lower-case SHA-256")
    return value


def _timestamp(value: object, *, field: str) -> str:
    if type(value) is not str or _RFC3339_RE.fullmatch(value) is None:
        raise _error("CAPTURE_INPUT_INVALID", f"{field} must be canonical RFC3339 UTC")
    return value


def _digest(domain: bytes, payload: Mapping[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(dict(payload))).hexdigest()


def _stream_document(value: MonitorStreamKey) -> dict[str, object]:
    if type(value) is not MonitorStreamKey:
        raise _error("CAPTURE_STREAM_INVALID", "stream key must be concrete")
    return {
        "stream_id": value.stream_id,
        "metric": value.metric.value,
        "threshold_stratum": value.threshold_stratum,
        "configuration_identity": value.configuration_identity,
        "data_identity": value.data_identity,
        "flat_binding_id": value.flat_binding_id,
        "hnsw_binding_id": value.hnsw_binding_id,
    }


class CapturePhase(StrEnum):
    OBSERVING = "OBSERVING"
    TRIGGERED = "TRIGGERED"
    WARMUP_FROZEN = "WARMUP_FROZEN"
    CALIBRATION_FROZEN = "CALIBRATION_FROZEN"
    CAPTURE_COMPLETE = "CAPTURE_COMPLETE"
    INVALID = "INVALID"


@dataclass(frozen=True, slots=True)
class CaptureEnvironmentIdentity:
    schema_version: str
    milvus_uri: str
    deployment_identity: str
    collection_name: str
    dimensions: int
    metric: Metric
    hnsw_index_identity: str
    data_identity: str
    source_revision: str
    observed_at_utc: str
    environment_manifest_canonical_json: bytes
    environment_manifest_sha256: str


def _metadata_payload(value: CaptureEnvironmentIdentity) -> dict[str, object]:
    if type(value) is not CaptureEnvironmentIdentity:
        raise _error("CAPTURE_METADATA_INVALID", "metadata must be concrete")
    if value.schema_version != _METADATA_SCHEMA or type(value.metric) is not Metric:
        raise _error("CAPTURE_METADATA_INVALID", "metadata schema or metric is invalid")
    for name in (
        "milvus_uri",
        "deployment_identity",
        "collection_name",
        "hnsw_index_identity",
        "data_identity",
        "source_revision",
    ):
        _text(getattr(value, name), field=name)
    if type(value.dimensions) is not int or value.dimensions <= 0:
        raise _error("CAPTURE_METADATA_INVALID", "dimensions must be a positive integer")
    _timestamp(value.observed_at_utc, field="observed_at_utc")
    if type(value.environment_manifest_canonical_json) is not bytes:
        raise _error("CAPTURE_METADATA_INVALID", "environment manifest must be canonical bytes")
    try:
        environment_manifest = json.loads(
            value.environment_manifest_canonical_json.decode("utf-8")
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _error("CAPTURE_METADATA_INVALID", "environment manifest JSON is invalid") from exc
    if (
        type(environment_manifest) is not dict
        or canonical_json_bytes(environment_manifest)
        != value.environment_manifest_canonical_json
    ):
        raise _error("CAPTURE_METADATA_INVALID", "environment manifest is not canonical")
    payload = {
        "schema_version": _METADATA_SCHEMA,
        "milvus_uri": value.milvus_uri,
        "deployment_identity": value.deployment_identity,
        "collection_name": value.collection_name,
        "dimensions": value.dimensions,
        "metric": value.metric.value,
        "hnsw_index_identity": value.hnsw_index_identity,
        "data_identity": value.data_identity,
        "source_revision": value.source_revision,
        "observed_at_utc": value.observed_at_utc,
        "environment_manifest": environment_manifest,
    }
    if _digest(_METADATA_DOMAIN, payload) != value.environment_manifest_sha256:
        raise _error("CAPTURE_METADATA_INVALID", "environment digest mismatch")
    return payload


def build_capture_environment_identity(
    *,
    milvus_uri: str,
    deployment_identity: str,
    collection_name: str,
    dimensions: int,
    metric: Metric,
    hnsw_index_identity: str,
    data_identity: str,
    source_revision: str,
    observed_at_utc: str,
    environment_manifest: dict[str, object],
) -> CaptureEnvironmentIdentity:
    payload = {
        "schema_version": _METADATA_SCHEMA,
        "milvus_uri": _text(milvus_uri, field="milvus_uri"),
        "deployment_identity": _text(deployment_identity, field="deployment_identity"),
        "collection_name": _text(collection_name, field="collection_name"),
        "dimensions": dimensions,
        "metric": metric.value if type(metric) is Metric else metric,
        "hnsw_index_identity": _text(hnsw_index_identity, field="hnsw_index_identity"),
        "data_identity": _text(data_identity, field="data_identity"),
        "source_revision": _text(source_revision, field="source_revision"),
        "observed_at_utc": _timestamp(observed_at_utc, field="observed_at_utc"),
        "environment_manifest": environment_manifest,
    }
    if type(dimensions) is not int or dimensions <= 0 or type(metric) is not Metric:
        raise _error("CAPTURE_METADATA_INVALID", "dimensions or metric is invalid")
    if type(environment_manifest) is not dict:
        raise _error("CAPTURE_METADATA_INVALID", "environment manifest must be a dict")
    value = CaptureEnvironmentIdentity(
        schema_version=_METADATA_SCHEMA,
        milvus_uri=payload["milvus_uri"],  # type: ignore[arg-type]
        deployment_identity=payload["deployment_identity"],  # type: ignore[arg-type]
        collection_name=payload["collection_name"],  # type: ignore[arg-type]
        dimensions=dimensions,
        metric=metric,
        hnsw_index_identity=payload["hnsw_index_identity"],  # type: ignore[arg-type]
        data_identity=payload["data_identity"],  # type: ignore[arg-type]
        source_revision=payload["source_revision"],  # type: ignore[arg-type]
        observed_at_utc=payload["observed_at_utc"],  # type: ignore[arg-type]
        environment_manifest_canonical_json=canonical_json_bytes(environment_manifest),
        environment_manifest_sha256=_digest(_METADATA_DOMAIN, payload),
    )
    _metadata_payload(value)
    return value


class ReadOnlyCaptureMetadataProvider(Protocol):
    """Port for one read-only environment/index snapshot at trigger time."""

    def capture(self) -> CaptureEnvironmentIdentity: ...


@dataclass(frozen=True, slots=True)
class GenuineWorkloadObservation:
    """One genuine request observed after normal serving, never generated here."""

    event_id: str
    source_sequence: int
    window_sequence: int
    within_window_index: int
    query_id: int | str
    observed_at_utc: str
    stream_key: MonitorStreamKey
    source_revision: str
    environment_manifest_sha256: str
    query_vector: tuple[float, ...]
    threshold_radius: float
    range_filter: float
    limit: int
    consistency_level: str

    def __post_init__(self) -> None:
        _text(self.event_id, field="event_id")
        if type(self.source_sequence) is not int or self.source_sequence < 0:
            raise _error("OBSERVATION_INVALID", "source sequence must be non-negative")
        if type(self.window_sequence) is not int or self.window_sequence < 0:
            raise _error("OBSERVATION_INVALID", "window sequence must be non-negative")
        if type(self.within_window_index) is not int or not 0 <= self.within_window_index < 200:
            raise _error("OBSERVATION_INVALID", "within-window index must be 0..199")
        build_canonical_query_identity(self.query_id)
        _timestamp(self.observed_at_utc, field="observed_at_utc")
        if type(self.stream_key) is not MonitorStreamKey:
            raise _error("OBSERVATION_INVALID", "stream key must be concrete")
        _text(self.source_revision, field="source_revision")
        _sha(self.environment_manifest_sha256, field="environment_manifest_sha256")
        if type(self.query_vector) is not tuple or not self.query_vector:
            raise _error("OBSERVATION_INVALID", "query vector must be a non-empty tuple")
        if any(type(item) is not float or not math.isfinite(item) for item in self.query_vector):
            raise _error("OBSERVATION_INVALID", "query vector values must be finite floats")
        for name in ("threshold_radius", "range_filter"):
            item = getattr(self, name)
            if type(item) is not float or not math.isfinite(item):
                raise _error("OBSERVATION_INVALID", f"{name} must be a finite float")
        if type(self.limit) is not int or self.limit <= 0:
            raise _error("OBSERVATION_INVALID", "limit must be a positive integer")
        _text(self.consistency_level, field="consistency_level")


class GenuineWorkloadObservationSource(Protocol):
    """At-least-once, read-only source; implementations must not generate traffic."""

    def poll(self, *, limit: int) -> tuple[GenuineWorkloadObservation, ...]: ...

    def acknowledge(self, event_ids: tuple[str, ...]) -> None: ...


@dataclass(frozen=True, slots=True)
class CapturedPopulationArtifacts:
    output_dir: Path
    capture_manifest_path: Path
    warmup_manifest_path: Path
    calibration_population_path: Path
    run_binding_path: Path
    vector_material_path: Path
    warmup_role_manifest: ResponseProfileRoleManifest
    population: CalibrationPopulationManifest
    run_binding: ResponseProfileRunBinding


@dataclass(frozen=True, slots=True)
class _ReducedState:
    phase: CapturePhase
    trigger_payload: dict[str, object] | None
    observations: tuple[dict[str, object], ...]
    event_ids: frozenset[str]
    query_id_sha256: frozenset[str]
    vector_sha256: frozenset[str]
    query_payload_sha256: frozenset[str]
    last_source_sequence: int | None
    invalid_reason: str | None


def _binding_payload(
    *, run_id: str, created_at_utc: str, stream_key: MonitorStreamKey,
    source_namespace: LiveStreamSourceNamespace, source_revision: str,
) -> dict[str, object]:
    return {
        "schema_version": _BINDING_SCHEMA,
        "run_id": _text(run_id, field="run_id"),
        "created_at_utc": _timestamp(created_at_utc, field="created_at_utc"),
        "stream_key": _stream_document(stream_key),
        "source_namespace_sha256": source_namespace.source_namespace_sha256,
        "source_workload_manifest_sha256": source_namespace.source_workload_manifest_sha256,
        "source_revision": _text(source_revision, field="source_revision"),
        "evidence_status": CAPTURE_EVIDENCE_STATUS,
    }


def _event_document(
    *, event_seq: int, kind: str, payload: dict[str, object], previous: str | None
) -> dict[str, object]:
    body = {
        "schema_version": _EVENT_SCHEMA,
        "event_seq": event_seq,
        "kind": kind,
        "payload": payload,
        "previous_event_sha256": previous,
    }
    return {"event_payload": body, "event_sha256": _digest(_EVENT_DOMAIN, body)}


def _observation_payload(value: GenuineWorkloadObservation) -> dict[str, object]:
    try:
        vector = np.asarray(value.query_vector, dtype="<f4")
        vector_identity = build_query_vector_identity(vector)
        configuration = SearchConfiguration(
            metric=value.stream_key.metric,
            threshold_label=value.stream_key.threshold_stratum,
            radius=value.threshold_radius,
            index_track=IndexTrack.FLAT,
            ef=None,
            limit=value.limit,
            consistency_level=value.consistency_level,
        )
        payload = build_response_profile_query_payload(
            vector_identity=vector_identity, search_configuration=configuration
        )
        query = build_canonical_query_identity(value.query_id)
    except (TypeError, ValueError, ResponseProfileEvidenceContractError) as exc:
        raise _error("OBSERVATION_INVALID", "query evidence construction failed") from exc
    return {
        "schema_version": _OBSERVATION_SCHEMA,
        "event_id": value.event_id,
        "source_sequence": value.source_sequence,
        "window_sequence": value.window_sequence,
        "within_window_index": value.within_window_index,
        "query_id": query.query_id,
        "query_id_sha256": query.query_id_sha256,
        "observed_at_utc": value.observed_at_utc,
        "stream_key": _stream_document(value.stream_key),
        "source_revision": value.source_revision,
        "environment_manifest_sha256": value.environment_manifest_sha256,
        "dimensions": vector_identity.dimensions,
        "vector_sha256": vector_identity.vector_sha256,
        "canonical_vector_bytes_base64": base64.b64encode(
            vector_identity.canonical_vector_bytes
        ).decode("ascii"),
        "query_payload_sha256": payload.query_payload_sha256,
        "threshold_radius": payload.radius,
        "range_filter": payload.range_filter,
        "limit": payload.limit,
        "consistency_level": payload.consistency_level,
    }


_OBSERVATION_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "source_sequence",
        "window_sequence",
        "within_window_index",
        "query_id",
        "query_id_sha256",
        "observed_at_utc",
        "stream_key",
        "source_revision",
        "environment_manifest_sha256",
        "dimensions",
        "vector_sha256",
        "canonical_vector_bytes_base64",
        "query_payload_sha256",
        "threshold_radius",
        "range_filter",
        "limit",
        "consistency_level",
    }
)


def _validated_observation_document(value: object) -> dict[str, object]:
    """Reconstruct one persisted observation through the public contracts."""

    if type(value) is not dict or frozenset(value) != _OBSERVATION_FIELDS:
        raise _error("CAPTURE_LEDGER_INVALID", "captured query fields differ")
    if value.get("schema_version") != _OBSERVATION_SCHEMA:
        raise _error("CAPTURE_LEDGER_INVALID", "captured query schema differs")
    stream = value.get("stream_key")
    if type(stream) is not dict or frozenset(stream) != {
        "stream_id",
        "metric",
        "threshold_stratum",
        "configuration_identity",
        "data_identity",
        "flat_binding_id",
        "hnsw_binding_id",
    }:
        raise _error("CAPTURE_LEDGER_INVALID", "captured stream fields differ")
    encoded = value.get("canonical_vector_bytes_base64")
    if type(encoded) is not str:
        raise _error("CAPTURE_LEDGER_INVALID", "captured vector encoding is invalid")
    try:
        raw = base64.b64decode(encoded, validate=True)
        vector = np.frombuffer(raw, dtype="<f4")
        rebuilt = GenuineWorkloadObservation(
            event_id=value["event_id"],  # type: ignore[arg-type]
            source_sequence=value["source_sequence"],  # type: ignore[arg-type]
            window_sequence=value["window_sequence"],  # type: ignore[arg-type]
            within_window_index=value["within_window_index"],  # type: ignore[arg-type]
            query_id=value["query_id"],  # type: ignore[arg-type]
            observed_at_utc=value["observed_at_utc"],  # type: ignore[arg-type]
            stream_key=MonitorStreamKey(
                stream_id=stream["stream_id"],  # type: ignore[arg-type]
                metric=Metric(stream["metric"]),
                threshold_stratum=stream["threshold_stratum"],  # type: ignore[arg-type]
                configuration_identity=stream["configuration_identity"],  # type: ignore[arg-type]
                data_identity=stream["data_identity"],  # type: ignore[arg-type]
                flat_binding_id=stream["flat_binding_id"],  # type: ignore[arg-type]
                hnsw_binding_id=stream["hnsw_binding_id"],  # type: ignore[arg-type]
            ),
            source_revision=value["source_revision"],  # type: ignore[arg-type]
            environment_manifest_sha256=value["environment_manifest_sha256"],  # type: ignore[arg-type]
            query_vector=tuple(float(item) for item in vector),
            threshold_radius=value["threshold_radius"],  # type: ignore[arg-type]
            range_filter=value["range_filter"],  # type: ignore[arg-type]
            limit=value["limit"],  # type: ignore[arg-type]
            consistency_level=value["consistency_level"],  # type: ignore[arg-type]
        )
    except (KeyError, TypeError, ValueError, ResponseProfileWorkloadCaptureError) as exc:
        raise _error("CAPTURE_LEDGER_INVALID", "captured query reconstruction failed") from exc
    canonical = _observation_payload(rebuilt)
    if canonical_json_bytes(canonical) != canonical_json_bytes(value):
        raise _error("CAPTURE_LEDGER_INVALID", "captured query is noncanonical")
    return canonical


def _initial_state() -> _ReducedState:
    return _ReducedState(
        CapturePhase.OBSERVING,
        None,
        (),
        frozenset(),
        frozenset(),
        frozenset(),
        frozenset(),
        None,
        None,
    )


def _metadata_from_payload(value: object) -> CaptureEnvironmentIdentity:
    if type(value) is not dict or frozenset(value) != {
        "schema_version",
        "milvus_uri",
        "deployment_identity",
        "collection_name",
        "dimensions",
        "metric",
        "hnsw_index_identity",
        "data_identity",
        "source_revision",
        "observed_at_utc",
        "environment_manifest",
    }:
        raise _error("CAPTURE_LEDGER_INVALID", "trigger metadata fields differ")
    try:
        rebuilt = build_capture_environment_identity(
            milvus_uri=value["milvus_uri"],  # type: ignore[arg-type]
            deployment_identity=value["deployment_identity"],  # type: ignore[arg-type]
            collection_name=value["collection_name"],  # type: ignore[arg-type]
            dimensions=value["dimensions"],  # type: ignore[arg-type]
            metric=Metric(value["metric"]),
            hnsw_index_identity=value["hnsw_index_identity"],  # type: ignore[arg-type]
            data_identity=value["data_identity"],  # type: ignore[arg-type]
            source_revision=value["source_revision"],  # type: ignore[arg-type]
            observed_at_utc=value["observed_at_utc"],  # type: ignore[arg-type]
            environment_manifest=value["environment_manifest"],  # type: ignore[arg-type]
        )
    except (KeyError, TypeError, ValueError, ResponseProfileWorkloadCaptureError) as exc:
        raise _error("CAPTURE_LEDGER_INVALID", "trigger metadata reconstruction failed") from exc
    if canonical_json_bytes(_metadata_payload(rebuilt)) != canonical_json_bytes(value):
        raise _error("CAPTURE_LEDGER_INVALID", "trigger metadata is noncanonical")
    return rebuilt


def _validated_trigger_payload(value: object) -> dict[str, object]:
    if type(value) is not dict or frozenset(value) != {
        "detector_head",
        "head_record_sequence",
        "head_record_sha256",
        "head_record_persisted_at_utc",
        "metadata",
        "environment_manifest_sha256",
    }:
        raise _error("CAPTURE_LEDGER_INVALID", "trigger fields differ")
    try:
        head = response_profile_detector_head_from_document(value["detector_head"])
        metadata = _metadata_from_payload(value["metadata"])
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("CAPTURE_LEDGER_INVALID", "trigger reconstruction failed") from exc
    if head.detector_state is not DetectorState.DRIFT:
        raise _error("CAPTURE_LEDGER_INVALID", "trigger detector outcome is not DRIFT")
    if (
        metadata.metric is not head.stream_key.metric
        or metadata.data_identity != head.stream_key.data_identity
        or metadata.hnsw_index_identity != head.stream_key.hnsw_binding_id
    ):
        raise _error("CAPTURE_LEDGER_INVALID", "trigger metadata differs from stream")
    sequence = value["head_record_sequence"]
    if type(sequence) is not int or sequence < 0:
        raise _error("CAPTURE_LEDGER_INVALID", "trigger record sequence is invalid")
    try:
        _sha(value["head_record_sha256"], field="head_record_sha256")
        _timestamp(value["head_record_persisted_at_utc"], field="head_record_persisted_at_utc")
        _sha(value["environment_manifest_sha256"], field="environment_manifest_sha256")
    except ResponseProfileWorkloadCaptureError as exc:
        raise _error("CAPTURE_LEDGER_INVALID", "trigger identity is malformed") from exc
    if value["environment_manifest_sha256"] != metadata.environment_manifest_sha256:
        raise _error("CAPTURE_LEDGER_INVALID", "trigger environment digest differs")
    return dict(value)


def _reduce(
    events: tuple[dict[str, object], ...],
    *,
    initial_state: _ReducedState | None = None,
    initial_previous: str | None = None,
    start_sequence: int = 0,
) -> _ReducedState:
    """Apply canonical transitions; full replay and incremental writes share it."""

    state = initial_state if initial_state is not None else _initial_state()
    previous = initial_previous
    for offset, document in enumerate(events):
        expected_seq = start_sequence + offset
        if type(document) is not dict or frozenset(document) != {"event_payload", "event_sha256"}:
            raise _error("CAPTURE_LEDGER_INVALID", "event document fields differ")
        payload = document["event_payload"]
        if type(payload) is not dict or frozenset(payload) != {
            "schema_version", "event_seq", "kind", "payload", "previous_event_sha256"
        }:
            raise _error("CAPTURE_LEDGER_INVALID", "event payload fields differ")
        if (
            payload["schema_version"] != _EVENT_SCHEMA
            or type(payload["event_seq"]) is not int
            or payload["event_seq"] != expected_seq
        ):
            raise _error("CAPTURE_LEDGER_INVALID", "event sequence/schema differs")
        if (
            payload["previous_event_sha256"] != previous
            or (
                payload["previous_event_sha256"] is not None
                and type(payload["previous_event_sha256"]) is not str
            )
        ):
            raise _error("CAPTURE_LEDGER_INVALID", "event hash chain differs")
        if _digest(_EVENT_DOMAIN, payload) != document["event_sha256"]:
            raise _error("CAPTURE_LEDGER_INVALID", "event digest differs")
        kind = payload["kind"]
        body = payload["payload"]
        if type(kind) is not str or kind not in _EVENT_KINDS or type(body) is not dict:
            raise _error("CAPTURE_LEDGER_INVALID", "event kind/payload is invalid")
        if state.phase is CapturePhase.INVALID:
            raise _error("CAPTURE_LEDGER_INVALID", "events follow invalidation")
        if kind == "TRIGGERED":
            if state.phase is not CapturePhase.OBSERVING:
                raise _error("CAPTURE_LEDGER_INVALID", "trigger is duplicated")
            trigger = _validated_trigger_payload(body)
            state = _ReducedState(
                CapturePhase.TRIGGERED,
                trigger,
                (),
                frozenset(),
                frozenset(),
                frozenset(),
                frozenset(),
                None,
                None,
            )
        elif kind == "QUERY_CAPTURED":
            if state.phase not in {CapturePhase.TRIGGERED, CapturePhase.WARMUP_FROZEN}:
                raise _error("CAPTURE_LEDGER_INVALID", "query appears outside capture phase")
            observation = _validated_observation_document(body)
            event_id = observation.get("event_id")
            source_sequence = observation.get("source_sequence")
            if type(event_id) is not str or event_id in state.event_ids:
                raise _error("CAPTURE_LEDGER_INVALID", "query event is duplicated")
            if type(source_sequence) is not int:
                raise _error("CAPTURE_LEDGER_INVALID", "source sequence is invalid")
            if state.last_source_sequence is not None and source_sequence != state.last_source_sequence + 1:
                raise _error("CAPTURE_LEDGER_INVALID", "source sequence is non-consecutive")
            if state.trigger_payload is None:
                raise _error("CAPTURE_LEDGER_INVALID", "query has no trigger")
            trigger_head = response_profile_detector_head_from_document(
                state.trigger_payload["detector_head"]
            )
            captured_index = len(state.observations)
            expected_window = trigger_head.window_sequence + 1 + captured_index // 200
            expected_within = captured_index % 200
            trigger_metadata = state.trigger_payload["metadata"]
            if (
                observation["window_sequence"] != expected_window
                or observation["within_window_index"] != expected_within
                or observation["stream_key"] != _stream_document(trigger_head.stream_key)
                or observation["source_revision"] != trigger_metadata["source_revision"]
                or observation["environment_manifest_sha256"]
                != state.trigger_payload["environment_manifest_sha256"]
                or observation["dimensions"] != trigger_metadata["dimensions"]
            ):
                raise _error("CAPTURE_LEDGER_INVALID", "captured query lineage/order differs")
            if (
                observation["query_id_sha256"] in state.query_id_sha256
                or observation["vector_sha256"] in state.vector_sha256
                or observation["query_payload_sha256"] in state.query_payload_sha256
            ):
                raise _error("CAPTURE_LEDGER_INVALID", "captured query material is duplicated")
            state = _ReducedState(
                state.phase, state.trigger_payload, (*state.observations, observation),
                state.event_ids | {event_id},
                state.query_id_sha256 | {observation["query_id_sha256"]},
                state.vector_sha256 | {observation["vector_sha256"]},
                state.query_payload_sha256 | {observation["query_payload_sha256"]},
                source_sequence,
                None,
            )
        elif kind == "WARMUP_FROZEN":
            if body or state.phase is not CapturePhase.TRIGGERED or len(state.observations) != WARMUP_QUERY_COUNT:
                raise _error("CAPTURE_LEDGER_INVALID", "warm-up freeze is misplaced")
            state = _ReducedState(
                CapturePhase.WARMUP_FROZEN, state.trigger_payload, state.observations,
                state.event_ids, state.query_id_sha256, state.vector_sha256,
                state.query_payload_sha256, state.last_source_sequence, None,
            )
        elif kind == "CALIBRATION_FROZEN":
            if body or state.phase is not CapturePhase.WARMUP_FROZEN or len(state.observations) != WARMUP_QUERY_COUNT + CALIBRATION_QUERY_COUNT:
                raise _error("CAPTURE_LEDGER_INVALID", "calibration freeze is misplaced")
            state = _ReducedState(
                CapturePhase.CALIBRATION_FROZEN, state.trigger_payload, state.observations,
                state.event_ids, state.query_id_sha256, state.vector_sha256,
                state.query_payload_sha256, state.last_source_sequence, None,
            )
        elif kind == "CAPTURE_COMPLETE":
            if body or state.phase is not CapturePhase.CALIBRATION_FROZEN:
                raise _error("CAPTURE_LEDGER_INVALID", "completion is misplaced")
            state = _ReducedState(
                CapturePhase.CAPTURE_COMPLETE, state.trigger_payload, state.observations,
                state.event_ids, state.query_id_sha256, state.vector_sha256,
                state.query_payload_sha256, state.last_source_sequence, None,
            )
        else:
            reason = body.get("reason_code")
            if frozenset(body) != {"reason_code"} or type(reason) is not str or not reason:
                raise _error("CAPTURE_LEDGER_INVALID", "invalidation reason is missing")
            state = _ReducedState(
                CapturePhase.INVALID, state.trigger_payload, state.observations,
                state.event_ids, state.query_id_sha256, state.vector_sha256,
                state.query_payload_sha256, state.last_source_sequence, reason,
            )
        previous = document["event_sha256"]  # type: ignore[assignment]
    return state


_SCHEMA = (
    "CREATE TABLE capture_binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), canonical_json BLOB NOT NULL, binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256)=64)) STRICT",
    "CREATE TABLE capture_events (event_seq INTEGER PRIMARY KEY CHECK(event_seq>=0), kind TEXT NOT NULL, canonical_json BLOB NOT NULL, event_sha256 TEXT NOT NULL UNIQUE CHECK(length(event_sha256)=64), previous_event_sha256 TEXT) STRICT",
    "CREATE TRIGGER capture_binding_no_update BEFORE UPDATE ON capture_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER capture_binding_no_delete BEFORE DELETE ON capture_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER capture_events_no_update BEFORE UPDATE ON capture_events BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER capture_events_no_delete BEFORE DELETE ON capture_events BEGIN SELECT RAISE(ABORT,'append-only'); END",
)


def _normalized_sql(value: str) -> str:
    return " ".join(value.strip().removesuffix(";").split())


def _private_regular(path: Path, *, code: str) -> os.stat_result:
    try:
        value = os.lstat(path)
    except OSError as exc:
        raise _error(code, f"{path.name} is unavailable") from exc
    if (
        not stat.S_ISREG(value.st_mode)
        or value.st_nlink != 1
        or value.st_uid != os.geteuid()
        or stat.S_IMODE(value.st_mode) != 0o600
    ):
        raise _error(code, f"{path.name} must be private, regular, and singly linked")
    return value


class _CaptureLedger:
    def __init__(self, path: Path, *, binding: dict[str, object]) -> None:
        self.path = Path(path)
        self._mutex = threading.RLock()
        self._pid = os.getpid()
        self._closed = False
        self._lock_handle = None
        self._lock_inode: tuple[int, int] | None = None
        parent = self.path.parent
        try:
            parent_info = parent.stat()
        except OSError as exc:
            raise _error("CAPTURE_LEDGER_PARENT_UNSAFE", "ledger parent is unavailable") from exc
        if (
            not stat.S_ISDIR(parent_info.st_mode)
            or parent_info.st_uid != os.geteuid()
            or stat.S_IMODE(parent_info.st_mode) & 0o022
        ):
            raise _error("CAPTURE_LEDGER_PARENT_UNSAFE", "ledger parent is not owner-controlled")
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        lock_existed = lock_path.exists()
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        lock_fd = os.open(lock_path, flags, 0o600)
        self._lock_handle = os.fdopen(lock_fd, "a+b")
        if not lock_existed:
            os.fchmod(lock_fd, 0o600)
        lock_info = os.fstat(lock_fd)
        if (
            not stat.S_ISREG(lock_info.st_mode)
            or lock_info.st_nlink != 1
            or lock_info.st_uid != os.geteuid()
            or stat.S_IMODE(lock_info.st_mode) != 0o600
        ):
            self.close()
            raise _error("CAPTURE_LEDGER_PATH_UNSAFE", "capture lock path is unsafe")
        try:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._lock_handle.close()
            raise _error("CAPTURE_LEDGER_BUSY", "capture ledger is already owned") from exc
        lock_inode = (lock_info.st_dev, lock_info.st_ino)
        with _OWNERSHIP_LOCK:
            if lock_inode in _OWNED_LOCK_INODES:
                self.close()
                raise _error("CAPTURE_LEDGER_BUSY", "capture ledger is already owned")
            _OWNED_LOCK_INODES.add(lock_inode)
        self._lock_inode = lock_inode
        created = not self.path.exists()
        if created:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
            os.close(descriptor)
        _private_regular(self.path, code="CAPTURE_LEDGER_PATH_UNSAFE")
        self._connection = sqlite3.connect(self.path, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        try:
            if created:
                self._connection.execute("PRAGMA journal_mode=DELETE")
                self._connection.execute("PRAGMA synchronous=FULL")
                self._connection.execute("PRAGMA trusted_schema=OFF")
                self._connection.execute("BEGIN IMMEDIATE")
                for sql in _SCHEMA:
                    self._connection.execute(sql)
                self._connection.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")
                digest = _digest(_BINDING_DOMAIN, binding)
                self._connection.execute(
                    "INSERT INTO capture_binding VALUES(1,?,?)",
                    (canonical_json_bytes(binding), digest),
                )
                self._connection.execute("COMMIT")
            else:
                mode = self._connection.execute("PRAGMA journal_mode").fetchone()[0]
                if str(mode).lower() != "delete":
                    raise _error("CAPTURE_LEDGER_INVALID", "existing journal mode differs")
                self._connection.execute("PRAGMA synchronous=FULL")
                self._connection.execute("PRAGMA trusted_schema=OFF")
            self._documents = self._verify(binding)
            self._state = _reduce(self._documents)
        except Exception:
            self.close()
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        connection = getattr(self, "_connection", None)
        if connection is not None:
            connection.close()
        if self._lock_handle is not None:
            try:
                fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._lock_handle.close()
        if self._lock_inode is not None:
            with _OWNERSHIP_LOCK:
                _OWNED_LOCK_INODES.discard(self._lock_inode)
            self._lock_inode = None

    def _verify(self, binding: dict[str, object]) -> tuple[dict[str, object], ...]:
        if self._closed:
            raise _error("CAPTURE_LEDGER_CLOSED", "capture ledger is closed")
        if os.getpid() != self._pid:
            raise _error("CAPTURE_LEDGER_FORKED", "capture ledger cannot cross fork")
        _private_regular(self.path, code="CAPTURE_LEDGER_PATH_UNSAFE")
        if self._connection.execute("PRAGMA user_version").fetchone()[0] != _SCHEMA_VERSION:
            raise _error("CAPTURE_LEDGER_INVALID", "capture ledger version differs")
        names = {
            row[0] for row in self._connection.execute(
                "SELECT name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        expected = {"capture_binding", "capture_events", "capture_binding_no_update", "capture_binding_no_delete", "capture_events_no_update", "capture_events_no_delete"}
        if names != expected:
            raise _error("CAPTURE_LEDGER_INVALID", "capture ledger schema differs")
        actual_sql = {
            row["name"]: _normalized_sql(row["sql"])
            for row in self._connection.execute(
                "SELECT name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        expected_sql: dict[str, str] = {}
        for statement in _SCHEMA:
            tokens = statement.split()
            name = tokens[2]
            expected_sql[name] = _normalized_sql(statement)
        if actual_sql != expected_sql:
            raise _error("CAPTURE_LEDGER_INVALID", "capture ledger SQL differs")
        row = self._connection.execute("SELECT canonical_json,binding_sha256 FROM capture_binding WHERE singleton=1").fetchone()
        expected_bytes = canonical_json_bytes(binding)
        if row is None or bytes(row[0]) != expected_bytes or row[1] != _digest(_BINDING_DOMAIN, binding):
            raise _error("CAPTURE_BINDING_MISMATCH", "capture binding differs")
        documents: list[dict[str, object]] = []
        for expected_seq, item in enumerate(self._connection.execute("SELECT * FROM capture_events ORDER BY event_seq")):
            try:
                document = json.loads(bytes(item["canonical_json"]).decode("utf-8"))
            except Exception as exc:
                raise _error("CAPTURE_LEDGER_INVALID", "event JSON is invalid") from exc
            if canonical_json_bytes(document) != bytes(item["canonical_json"]):
                raise _error("CAPTURE_LEDGER_INVALID", "event JSON is noncanonical")
            if item["event_seq"] != expected_seq or item["kind"] != document["event_payload"]["kind"] or item["event_sha256"] != document["event_sha256"] or item["previous_event_sha256"] != document["event_payload"]["previous_event_sha256"]:
                raise _error("CAPTURE_LEDGER_INVALID", "event row projection differs")
            documents.append(document)
        state = _reduce(tuple(documents))
        if state.trigger_payload is not None:
            head = response_profile_detector_head_from_document(
                state.trigger_payload["detector_head"]
            )
            metadata = state.trigger_payload["metadata"]
            if (
                _stream_document(head.stream_key) != binding["stream_key"]
                or metadata["source_revision"] != binding["source_revision"]
            ):
                raise _error("CAPTURE_BINDING_MISMATCH", "trigger differs from run binding")
        return tuple(documents)

    def _verify_cached_head(self, binding: dict[str, object]) -> None:
        if self._closed or os.getpid() != self._pid:
            raise _error("CAPTURE_LEDGER_CLOSED", "capture ledger is unavailable")
        _private_regular(self.path, code="CAPTURE_LEDGER_PATH_UNSAFE")
        row = self._connection.execute(
            "SELECT COUNT(*) AS count,MAX(event_seq) AS maximum FROM capture_events"
        ).fetchone()
        expected_count = len(self._documents)
        if row["count"] != expected_count or row["maximum"] != (
            expected_count - 1 if expected_count else None
        ):
            raise _error("CAPTURE_LEDGER_HEAD_DRIFT", "capture event head changed")
        if expected_count:
            head = self._connection.execute(
                "SELECT event_sha256 FROM capture_events WHERE event_seq=?",
                (expected_count - 1,),
            ).fetchone()
            if head is None or head[0] != self._documents[-1]["event_sha256"]:
                raise _error("CAPTURE_LEDGER_HEAD_DRIFT", "capture event digest changed")
        binding_row = self._connection.execute(
            "SELECT canonical_json,binding_sha256 FROM capture_binding WHERE singleton=1"
        ).fetchone()
        if (
            binding_row is None
            or bytes(binding_row[0]) != canonical_json_bytes(binding)
            or binding_row[1] != _digest(_BINDING_DOMAIN, binding)
        ):
            raise _error("CAPTURE_BINDING_MISMATCH", "capture binding changed")

    def state(self, binding: dict[str, object]) -> _ReducedState:
        with self._mutex:
            self._verify_cached_head(binding)
            return self._state

    def append(self, binding: dict[str, object], kind: str, payload: dict[str, object]) -> _ReducedState:
        with self._mutex:
            self._verify_cached_head(binding)
            documents = self._documents
            seq = len(documents)
            previous = documents[-1]["event_sha256"] if documents else None
            document = _event_document(event_seq=seq, kind=kind, payload=payload, previous=previous)  # type: ignore[arg-type]
            candidate = (*documents, document)
            state = _reduce(
                (document,),
                initial_state=self._state,
                initial_previous=previous,  # type: ignore[arg-type]
                start_sequence=seq,
            )
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                self._verify_cached_head(binding)
                self._connection.execute(
                    "INSERT INTO capture_events VALUES(?,?,?,?,?)",
                    (seq, kind, canonical_json_bytes(document), document["event_sha256"], previous),
                )
                self._connection.execute("COMMIT")
            except sqlite3.Error as exc:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise _error("CAPTURE_LEDGER_WRITE_FAILED", "capture event append failed") from exc
            self._documents = candidate
            self._state = state
            return state


def _member_from_observation(
    observation: dict[str, object], *, source_namespace: LiveStreamSourceNamespace
):
    raw = base64.b64decode(observation["canonical_vector_bytes_base64"], validate=True)
    vector = build_query_vector_identity(np.frombuffer(raw, dtype="<f4").copy())
    configuration = SearchConfiguration(
        metric=Metric(observation["stream_key"]["metric"]),  # type: ignore[index]
        threshold_label=observation["stream_key"]["threshold_stratum"],  # type: ignore[index]
        radius=observation["threshold_radius"],
        index_track=IndexTrack.FLAT,
        ef=None,
        limit=observation["limit"],
        consistency_level=observation["consistency_level"],
    )
    member = build_response_profile_role_member(
        source_namespace=source_namespace,
        query_identity=build_canonical_query_identity(observation["query_id"]),
        vector_identity=vector,
        query_payload_identity=build_response_profile_query_payload(
            vector_identity=vector, search_configuration=configuration
        ),
    )
    if (
        member.query_identity.query_id_sha256 != observation["query_id_sha256"]
        or member.vector_identity.vector_sha256 != observation["vector_sha256"]
        or member.query_payload_identity.query_payload_sha256 != observation["query_payload_sha256"]
        or member.vector_identity.dimensions != observation["dimensions"]
    ):
        raise _error("CAPTURE_LEDGER_INVALID", "captured query failed reconstruction")
    return member


def _ordered_capture_payload(
    observations: tuple[dict[str, object], ...],
) -> dict[str, object]:
    return {
        "schema_version": _ORDER_SCHEMA,
        "observations": [
            {
                "event_id": item["event_id"],
                "source_sequence": item["source_sequence"],
                "window_sequence": item["window_sequence"],
                "within_window_index": item["within_window_index"],
                "observed_at_utc": item["observed_at_utc"],
                "query_id_sha256": item["query_id_sha256"],
                "vector_sha256": item["vector_sha256"],
                "query_payload_sha256": item["query_payload_sha256"],
            }
            for item in observations
        ],
    }


class ResponseProfileWorkloadCapture:
    """Offline-testable coordinator for one genuine post-trigger population."""

    def __init__(
        self,
        *,
        ledger_path: Path,
        run_id: str,
        created_at_utc: str,
        stream_key: MonitorStreamKey,
        source_workload_manifest_sha256: str,
        source_revision: str,
        monitor_store: ResponseProfileMonitorStateStore,
        source: GenuineWorkloadObservationSource,
        metadata_provider: ReadOnlyCaptureMetadataProvider,
    ) -> None:
        if type(monitor_store) is not ResponseProfileMonitorStateStore:
            raise _error("DETECTOR_STORE_INVALID", "monitor store must be concrete")
        if not callable(getattr(source, "poll", None)) or not callable(getattr(source, "acknowledge", None)):
            raise _error("WORKLOAD_SOURCE_UNAVAILABLE", "genuine workload source is unavailable")
        if not callable(getattr(metadata_provider, "capture", None)):
            raise _error("CAPTURE_METADATA_UNAVAILABLE", "metadata provider is unavailable")
        self._stream_key = stream_key
        self._source_revision = _text(source_revision, field="source_revision")
        self._monitor_store = monitor_store
        self._source = source
        self._metadata_provider = metadata_provider
        self._source_namespace = build_live_stream_source_namespace(
            stream_id=stream_key.stream_id,
            data_identity=stream_key.data_identity,
            source_workload_manifest_sha256=source_workload_manifest_sha256,
        )
        self._binding = _binding_payload(
            run_id=run_id, created_at_utc=created_at_utc, stream_key=stream_key,
            source_namespace=self._source_namespace, source_revision=self._source_revision,
        )
        self._ledger = _CaptureLedger(ledger_path, binding=self._binding)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        self._ledger.close()

    @property
    def phase(self) -> CapturePhase:
        return self._ledger.state(self._binding).phase

    def _append_invalidation(self, code: str) -> None:
        state = self._ledger.state(self._binding)
        if state.phase is not CapturePhase.INVALID:
            self._ledger.append(self._binding, "CAPTURE_INVALIDATED", {"reason_code": code})

    def _observe_trigger(self) -> bool:
        """Persist one store-verified latest detector head, if available."""

        state = self._ledger.state(self._binding)
        if state.phase is not CapturePhase.OBSERVING:
            return state.phase is not CapturePhase.INVALID
        latest = self._monitor_store.load_verified_latest(self._stream_key)
        if latest is None:
            return False
        if type(latest) is not VerifiedLatestResponseProfileDetectorHead:
            raise _error("DETECTOR_TRIGGER_INVALID", "detector trigger is not store-issued")
        if latest.head.detector_state is not DetectorState.DRIFT:
            return False
        metadata = self._metadata_provider.capture()
        metadata_payload = _metadata_payload(metadata)
        if (
            metadata.metric is not self._stream_key.metric
            or metadata.data_identity != self._stream_key.data_identity
            or metadata.hnsw_index_identity != self._stream_key.hnsw_binding_id
            or metadata.source_revision != self._source_revision
        ):
            raise _error("CAPTURE_METADATA_MISMATCH", "metadata differs from capture stream")
        self._ledger.append(
            self._binding,
            "TRIGGERED",
            {
                "detector_head": response_profile_detector_head_document(latest.head),
                "head_record_sequence": latest.head_record_sequence,
                "head_record_sha256": latest.head_record_sha256,
                "head_record_persisted_at_utc": latest.head_record_persisted_at_utc,
                "metadata": metadata_payload,
                "environment_manifest_sha256": metadata.environment_manifest_sha256,
            },
        )
        return True

    def _ensure_trigger_current(self, state: _ReducedState) -> None:
        latest = self._monitor_store.load_verified_latest(self._stream_key)
        if latest is None or state.trigger_payload is None:
            raise _error("DETECTOR_TRIGGER_INVALID", "persisted trigger is unavailable")
        trigger = state.trigger_payload
        if type(latest) is not VerifiedLatestResponseProfileDetectorHead:
            raise _error("DETECTOR_HEAD_SUBSTITUTED", "detector head changed during capture")
        trigger_sequence = trigger["head_record_sequence"]
        if type(trigger_sequence) is not int or latest.head_record_sequence < trigger_sequence:
            raise _error("DETECTOR_HEAD_SUBSTITUTED", "detector head history regressed")
        # The monitor store verifies its complete immutable hash chain.  A later
        # head is expected while the live monitor continues; equality is
        # required only while the captured trigger is still the current head.
        if latest.head_record_sequence == trigger_sequence and (
            latest.head_record_sha256 != trigger["head_record_sha256"]
            or response_profile_detector_head_document(latest.head)
            != trigger["detector_head"]
        ):
            raise _error("DETECTOR_HEAD_SUBSTITUTED", "detector head changed during capture")

    def run_once(self, *, max_observations: int) -> int:
        if type(max_observations) is not int or max_observations <= 0:
            raise _error("CAPTURE_INPUT_INVALID", "max_observations must be positive")
        try:
            state = self._ledger.state(self._binding)
            if state.phase is CapturePhase.OBSERVING:
                if not self._observe_trigger():
                    return 0
                state = self._ledger.state(self._binding)
            if state.phase is CapturePhase.INVALID:
                raise _error(state.invalid_reason or "CAPTURE_INVALID", "capture is invalid")
            if state.phase is CapturePhase.CAPTURE_COMPLETE:
                return 0
            self._ensure_trigger_current(state)
            trigger_metadata = state.trigger_payload["metadata"]  # type: ignore[index]
            try:
                observations = self._source.poll(limit=max_observations)
            except (OSError, TypeError, ValueError) as exc:
                raise _error("WORKLOAD_SOURCE_FAILED", "workload source poll failed") from exc
            if type(observations) is not tuple:
                raise _error("WORKLOAD_SOURCE_INVALID", "source poll must return a tuple")
            accepted: list[str] = []
            for observation in observations:
                if type(observation) is not GenuineWorkloadObservation:
                    raise _error("OBSERVATION_INVALID", "source returned malformed observation")
                state = self._ledger.state(self._binding)
                if observation.event_id in state.event_ids:
                    persisted = next(
                        item for item in state.observations
                        if item["event_id"] == observation.event_id
                    )
                    if canonical_json_bytes(_observation_payload(observation)) != canonical_json_bytes(persisted):
                        raise _error(
                            "CAPTURE_EVENT_REDELIVERY_MISMATCH",
                            "redelivered event content differs",
                        )
                    accepted.append(observation.event_id)
                    continue
                if len(state.observations) >= WARMUP_QUERY_COUNT + CALIBRATION_QUERY_COUNT:
                    break
                trigger_window = state.trigger_payload["detector_head"]["detector_head_payload"]["window_sequence"]  # type: ignore[index]
                captured_index = len(state.observations)
                expected_window = trigger_window + 1 + captured_index // 200
                expected_within = captured_index % 200
                if (
                    observation.window_sequence != expected_window
                    or observation.within_window_index != expected_within
                ):
                    raise _error("QUERY_SEQUENCE_NON_CONSECUTIVE", "query window/order is not consecutive")
                if (
                    state.last_source_sequence is not None
                    and observation.source_sequence != state.last_source_sequence + 1
                ):
                    raise _error(
                        "QUERY_SEQUENCE_NON_CONSECUTIVE",
                        "source sequence is repeated or skipped",
                    )
                if observation.stream_key != self._stream_key:
                    raise _error("CAPTURE_STREAM_CHANGED", "query stream/cell changed")
                if observation.source_revision != self._source_revision:
                    raise _error("CAPTURE_SOURCE_REVISION_CHANGED", "source revision changed")
                if observation.environment_manifest_sha256 != state.trigger_payload["environment_manifest_sha256"]:
                    raise _error("CAPTURE_ENVIRONMENT_CHANGED", "environment identity changed")
                payload = _observation_payload(observation)
                if payload["dimensions"] != trigger_metadata["dimensions"]:
                    raise _error("CAPTURE_DIMENSIONS_CHANGED", "query dimensions changed")
                if (
                    payload["query_id_sha256"] in state.query_id_sha256
                    or payload["vector_sha256"] in state.vector_sha256
                    or payload["query_payload_sha256"] in state.query_payload_sha256
                ):
                    raise _error("CAPTURE_QUERY_DUPLICATE", "query identity/vector/payload was reused")
                state = self._ledger.append(self._binding, "QUERY_CAPTURED", payload)
                accepted.append(observation.event_id)
                if len(state.observations) == WARMUP_QUERY_COUNT:
                    state = self._ledger.append(self._binding, "WARMUP_FROZEN", {})
                if len(state.observations) == WARMUP_QUERY_COUNT + CALIBRATION_QUERY_COUNT:
                    state = self._ledger.append(self._binding, "CALIBRATION_FROZEN", {})
                    self._ledger.append(self._binding, "CAPTURE_COMPLETE", {})
                    break
            if accepted:
                try:
                    self._source.acknowledge(tuple(accepted))
                except (OSError, TypeError, ValueError) as exc:
                    # Captured rows are already durable.  Never invalidate or
                    # retry them as new observations merely because source
                    # acknowledgement failed; exact redelivery is idempotent.
                    raise _error("WORKLOAD_ACK_FAILED", "source acknowledgement failed") from exc
            return len(accepted)
        except ResponseProfileWorkloadCaptureError as exc:
            if exc.code != "WORKLOAD_ACK_FAILED":
                self._append_invalidation(exc.code)
            raise

    def publish(self, output_dir: Path) -> CapturedPopulationArtifacts:
        state = self._ledger.state(self._binding)
        if state.phase is not CapturePhase.CAPTURE_COMPLETE or state.trigger_payload is None:
            raise _error("CAPTURE_NOT_COMPLETE", "capture is not complete")
        warmup_observations = state.observations[:WARMUP_QUERY_COUNT]
        calibration_observations = state.observations[WARMUP_QUERY_COUNT:]
        warmup = build_response_profile_role_manifest(
            role=build_response_profile_role(kind=ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP),
            members=tuple(_member_from_observation(item, source_namespace=self._source_namespace) for item in warmup_observations),
        )
        calibration = build_response_profile_role_manifest(
            role=build_response_profile_role(kind=ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION),
            members=tuple(_member_from_observation(item, source_namespace=self._source_namespace) for item in calibration_observations),
        )
        population = build_calibration_population_manifest(
            cell=build_response_profile_cell(
                metric=self._stream_key.metric,
                threshold_stratum=self._stream_key.threshold_stratum,
            ),
            calibration_role_manifest=calibration,
        )
        schedule = build_response_profile_replay_schedule(
            population=population, source_revision=self._source_revision
        )
        run_binding = build_response_profile_run_binding(
            run_id=self._binding["run_id"],  # type: ignore[arg-type]
            created_at_utc=self._binding["created_at_utc"],  # type: ignore[arg-type]
            population=population,
            replay_schedule=schedule,
            warmup_role_manifest=warmup,
            source_revision=self._source_revision,
        )
        documents = {
            "warmup_role_manifest.json": role_manifest_document(warmup),
            "calibration_population.json": calibration_population_document(population),
            "run_binding.json": response_profile_run_binding_document(run_binding),
            "vector_material.json": response_profile_vector_material_document(run_binding),
        }
        manifest_payload = {
            "schema_version": _MANIFEST_SCHEMA,
            "evidence_status": CAPTURE_EVIDENCE_STATUS,
            "capture_binding_sha256": _digest(_BINDING_DOMAIN, self._binding),
            "trigger": state.trigger_payload,
            "warmup_count": len(warmup_observations),
            "calibration_count": len(calibration_observations),
            "warmup_window_sequence": warmup_observations[0]["window_sequence"],
            "calibration_window_sequences": list(
                dict.fromkeys(item["window_sequence"] for item in calibration_observations)
            ),
            "first_source_sequence": state.observations[0]["source_sequence"],
            "last_source_sequence": state.observations[-1]["source_sequence"],
            "ordered_capture_observations_sha256": _digest(
                _ORDER_DOMAIN, _ordered_capture_payload(state.observations)
            ),
            "capture_event_count": len(self._ledger._documents),
            "capture_event_head_sha256": self._ledger._documents[-1]["event_sha256"],
            "warmup_role_manifest_sha256": warmup.role_manifest_sha256,
            "workload_manifest_sha256": population.workload_manifest_sha256,
            "run_binding_sha256": run_binding.run_binding_sha256,
            "artifact_sha256": {
                name: hashlib.sha256(canonical_json_bytes(document)).hexdigest()
                for name, document in documents.items()
            },
        }
        documents["capture_manifest.json"] = {
            "capture_manifest_payload": manifest_payload,
            "capture_manifest_sha256": _digest(_MANIFEST_DOMAIN, manifest_payload),
        }
        target = Path(output_dir)
        if target.exists():
            raise _error("CAPTURE_OUTPUT_EXISTS", "capture output already exists")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
        try:
            os.chmod(temporary, 0o700)
            for name, document in documents.items():
                write_immutable_json(temporary / name, document)
            descriptor = os.open(temporary, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            if target.exists():
                raise _error("CAPTURE_OUTPUT_EXISTS", "capture output appeared")
            os.replace(temporary, target)
        except Exception:
            if temporary.exists():
                shutil.rmtree(temporary)
            raise
        return CapturedPopulationArtifacts(
            output_dir=target,
            capture_manifest_path=target / "capture_manifest.json",
            warmup_manifest_path=target / "warmup_role_manifest.json",
            calibration_population_path=target / "calibration_population.json",
            run_binding_path=target / "run_binding.json",
            vector_material_path=target / "vector_material.json",
            warmup_role_manifest=warmup,
            population=population,
            run_binding=run_binding,
        )
