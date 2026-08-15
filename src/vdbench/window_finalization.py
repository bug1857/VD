"""Prospective ADR-015 cross-store window-finalization reconciliation journal.

The detector, attestation, source acknowledgement, and shadow-attempt stores
remain the authorities for their own artifacts.  This append-only journal only
records enough canonical intent and observed artifact identities to reconcile
those stores after a crash; it never pretends they share one transaction and
never creates detector, policy, grant, routing, or actuation authority.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import unicodedata

from .artifacts import canonical_json_bytes
from .config import Metric
from .drift import (
    DetectorState,
    DriftClassification,
    DriftDecision,
    Signal,
    evidence_provenance_valid,
)
from .host_window_detector_v2 import HostWindowV2Status, source_window_sha256
from .monitor_evidence import (
    decode_persisted_window_evidence,
    encode_persisted_window_evidence,
)
from .shadow_attempt_store import ShadowAttemptRecord, ShadowAttemptStatus
from .shadow_event_types import MonitorStreamKey
from .v2_shadow_worker import V2ShadowWindowBundle

__all__ = [
    "PreparedWindowFinalization",
    "WindowFinalizationError",
    "WindowFinalizationPhase",
    "WindowFinalizationState",
    "SQLiteWindowFinalizationStore",
    "build_prepared_window_finalization",
    "restore_prepared_evaluation",
]


_DB_VERSION = 1
_BINDING_SCHEMA = "response-profile-window-finalization-binding-v1"
_PREPARED_SCHEMA = "response-profile-window-finalization-prepared-v1"
_EVENT_SCHEMA = "response-profile-window-finalization-event-v1"
_EVALUATION_SCHEMA = "response-profile-window-finalization-evaluation-v1"
_BINDING_DOMAIN = b"VD::WINDOW_FINALIZATION_BINDING::V1\x00"
_PREPARED_DOMAIN = b"VD::WINDOW_FINALIZATION_PREPARED::V1\x00"
_EVENT_DOMAIN = b"VD::WINDOW_FINALIZATION_EVENT::V1\x00"
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339 = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z\Z"
)
_OWNERSHIP_LOCK = threading.Lock()
_OWNED_LOCK_INODES: set[tuple[int, int]] = set()


class WindowFinalizationError(RuntimeError):
    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> WindowFinalizationError:
    return WindowFinalizationError(code, message)


def _text(value: object, *, code: str) -> str:
    if type(value) is not str:
        raise _error(code)
    normalized = unicodedata.normalize("NFC", value)
    if not normalized or normalized != value:
        raise _error(code)
    return normalized


def _sha(value: object, *, code: str) -> str:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise _error(code)
    return value


def _timestamp(value: object) -> str:
    if type(value) is not str or _RFC3339.fullmatch(value) is None:
        raise _error("WINDOW_FINALIZATION_TIMESTAMP_INVALID")
    return value


def _digest(domain: bytes, payload: Mapping[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(dict(payload))).hexdigest()


def _stream_document(value: MonitorStreamKey) -> dict[str, object]:
    if type(value) is not MonitorStreamKey:
        raise _error("WINDOW_FINALIZATION_STREAM_INVALID")
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
    fields = {
        "stream_id",
        "metric",
        "threshold_stratum",
        "configuration_identity",
        "data_identity",
        "flat_binding_id",
        "hnsw_binding_id",
    }
    if type(value) is not dict or set(value) != fields:
        raise _error("WINDOW_FINALIZATION_STREAM_INVALID")
    try:
        return MonitorStreamKey(
            stream_id=value["stream_id"],
            metric=Metric(value["metric"]),
            threshold_stratum=value["threshold_stratum"],
            configuration_identity=value["configuration_identity"],
            data_identity=value["data_identity"],
            flat_binding_id=value["flat_binding_id"],
            hnsw_binding_id=value["hnsw_binding_id"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("WINDOW_FINALIZATION_STREAM_INVALID") from exc


def _optional_float(value: object, *, code: str) -> float | None:
    if value is None:
        return None
    if type(value) is not float or not math.isfinite(value):
        raise _error(code)
    return value


def _evaluation_document(
    decision: DriftDecision, pending: Mapping[str, object]
) -> dict[str, object]:
    required = {
        "detector_contract_identity",
        "stream_key",
        "reference_window_sequence",
        "reference_source_window_sha256",
        "reference_shadow_window_sha256",
        "reference_assembled_manifest_sha256",
        "current_window_sequence",
        "current_source_window_sha256",
        "current_shadow_window_sha256",
        "current_assembled_manifest_sha256",
        "detector_seed",
        "previous_window_evidence_sha256",
        "previous_attestation_sha256",
        "evidence_provenance_sha256",
        "detector_state",
        "detector_classification",
        "source_revision",
        "environment_manifest_sha256",
        "current_window_evidence",
    }
    if (
        type(decision) is not DriftDecision
        or type(decision.state) is not DetectorState
        or type(decision.classification) is not DriftClassification
        or type(decision.triggering_signals) is not tuple
        or any(type(item) is not Signal for item in decision.triggering_signals)
        or type(decision.reason_codes) is not tuple
        or any(type(item) is not str for item in decision.reason_codes)
        or set(pending) != required
    ):
        raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
    _optional_float(
        decision.significance_evidence_score,
        code="WINDOW_FINALIZATION_EVALUATION_INVALID",
    )
    _optional_float(
        decision.drift_magnitude,
        code="WINDOW_FINALIZATION_EVALUATION_INVALID",
    )
    for reason in decision.reason_codes:
        _text(reason, code="WINDOW_FINALIZATION_EVALUATION_INVALID")
    evidence = pending["current_window_evidence"]
    encoded = encode_persisted_window_evidence(evidence)
    provenance = evidence.provenance
    if (
        provenance is None
        or not evidence_provenance_valid(provenance)
        or decision.evidence_provenance != provenance
        or type(pending["stream_key"]) is not MonitorStreamKey
        or pending["detector_state"] is not decision.state
        or pending["detector_classification"] is not decision.classification
        or pending["evidence_provenance_sha256"] != provenance.sha256
    ):
        raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
    previous_evidence = pending["previous_window_evidence_sha256"]
    previous_attestation = pending["previous_attestation_sha256"]
    if previous_evidence is not None:
        _sha(previous_evidence, code="WINDOW_FINALIZATION_EVALUATION_INVALID")
    if previous_attestation is not None:
        _sha(previous_attestation, code="WINDOW_FINALIZATION_EVALUATION_INVALID")
    for name in (
        "reference_source_window_sha256",
        "reference_shadow_window_sha256",
        "reference_assembled_manifest_sha256",
        "current_source_window_sha256",
        "current_shadow_window_sha256",
        "current_assembled_manifest_sha256",
        "evidence_provenance_sha256",
        "environment_manifest_sha256",
    ):
        _sha(pending[name], code="WINDOW_FINALIZATION_EVALUATION_INVALID")
    for name in ("reference_window_sequence", "current_window_sequence", "detector_seed"):
        if type(pending[name]) is not int or pending[name] < 0:
            raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
    return {
        "schema_version": _EVALUATION_SCHEMA,
        "decision": {
            "state": decision.state.value,
            "classification": decision.classification.value,
            "triggering_signals": [item.value for item in decision.triggering_signals],
            "significance_evidence_score": decision.significance_evidence_score,
            "drift_magnitude": decision.drift_magnitude,
            "reason_codes": list(decision.reason_codes),
        },
        "pending": {
            "detector_contract_identity": _text(
                pending["detector_contract_identity"],
                code="WINDOW_FINALIZATION_EVALUATION_INVALID",
            ),
            "stream": _stream_document(pending["stream_key"]),
            "reference_window_sequence": pending["reference_window_sequence"],
            "reference_source_window_sha256": pending["reference_source_window_sha256"],
            "reference_shadow_window_sha256": pending["reference_shadow_window_sha256"],
            "reference_assembled_manifest_sha256": pending["reference_assembled_manifest_sha256"],
            "current_window_sequence": pending["current_window_sequence"],
            "current_source_window_sha256": pending["current_source_window_sha256"],
            "current_shadow_window_sha256": pending["current_shadow_window_sha256"],
            "current_assembled_manifest_sha256": pending["current_assembled_manifest_sha256"],
            "detector_seed": pending["detector_seed"],
            "previous_window_evidence_sha256": previous_evidence,
            "previous_attestation_sha256": previous_attestation,
            "evidence_provenance_sha256": pending["evidence_provenance_sha256"],
            "source_revision": _text(
                pending["source_revision"], code="WINDOW_FINALIZATION_EVALUATION_INVALID"
            ),
            "environment_manifest_sha256": pending["environment_manifest_sha256"],
            "current_window_evidence": encoded,
        },
    }


def _restore_evaluation(value: object) -> tuple[DriftDecision, dict[str, object]]:
    if type(value) is not dict or set(value) != {"schema_version", "decision", "pending"}:
        raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
    if value["schema_version"] != _EVALUATION_SCHEMA:
        raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
    decision_doc = value["decision"]
    pending_doc = value["pending"]
    if type(decision_doc) is not dict or set(decision_doc) != {
        "state",
        "classification",
        "triggering_signals",
        "significance_evidence_score",
        "drift_magnitude",
        "reason_codes",
    }:
        raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
    pending_fields = {
        "detector_contract_identity",
        "stream",
        "reference_window_sequence",
        "reference_source_window_sha256",
        "reference_shadow_window_sha256",
        "reference_assembled_manifest_sha256",
        "current_window_sequence",
        "current_source_window_sha256",
        "current_shadow_window_sha256",
        "current_assembled_manifest_sha256",
        "detector_seed",
        "previous_window_evidence_sha256",
        "previous_attestation_sha256",
        "evidence_provenance_sha256",
        "source_revision",
        "environment_manifest_sha256",
        "current_window_evidence",
    }
    if type(pending_doc) is not dict or set(pending_doc) != pending_fields:
        raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
    try:
        evidence = decode_persisted_window_evidence(
            pending_doc["current_window_evidence"]
        )
        provenance = evidence.provenance
        decision = DriftDecision(
            state=DetectorState(decision_doc["state"]),
            classification=DriftClassification(decision_doc["classification"]),
            triggering_signals=tuple(
                Signal(item) for item in decision_doc["triggering_signals"]
            ),
            significance_evidence_score=_optional_float(
                decision_doc["significance_evidence_score"],
                code="WINDOW_FINALIZATION_EVALUATION_INVALID",
            ),
            drift_magnitude=_optional_float(
                decision_doc["drift_magnitude"],
                code="WINDOW_FINALIZATION_EVALUATION_INVALID",
            ),
            reason_codes=tuple(decision_doc["reason_codes"]),
            evidence_provenance=provenance,
        )
        pending = {
            "detector_contract_identity": pending_doc["detector_contract_identity"],
            "stream_key": _stream_from_document(pending_doc["stream"]),
            "reference_window_sequence": pending_doc["reference_window_sequence"],
            "reference_source_window_sha256": pending_doc["reference_source_window_sha256"],
            "reference_shadow_window_sha256": pending_doc["reference_shadow_window_sha256"],
            "reference_assembled_manifest_sha256": pending_doc["reference_assembled_manifest_sha256"],
            "current_window_sequence": pending_doc["current_window_sequence"],
            "current_source_window_sha256": pending_doc["current_source_window_sha256"],
            "current_shadow_window_sha256": pending_doc["current_shadow_window_sha256"],
            "current_assembled_manifest_sha256": pending_doc["current_assembled_manifest_sha256"],
            "detector_seed": pending_doc["detector_seed"],
            "previous_window_evidence_sha256": pending_doc["previous_window_evidence_sha256"],
            "previous_attestation_sha256": pending_doc["previous_attestation_sha256"],
            "evidence_provenance_sha256": pending_doc["evidence_provenance_sha256"],
            "detector_state": decision.state,
            "detector_classification": decision.classification,
            "source_revision": pending_doc["source_revision"],
            "environment_manifest_sha256": pending_doc["environment_manifest_sha256"],
            "current_window_evidence": evidence,
        }
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, WindowFinalizationError):
            raise
        raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID") from exc
    canonical = _evaluation_document(decision, pending)
    if canonical_json_bytes(canonical) != canonical_json_bytes(value):
        raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
    return decision, pending


@dataclass(frozen=True, slots=True, init=False)
class PreparedWindowFinalization:
    stream_key: MonitorStreamKey
    source_revision: str
    environment_manifest_sha256: str
    window_sequence: int
    source_window_sha256: str
    shadow_window_sha256: str
    assembled_manifest_sha256: str
    source_sequences: tuple[int, ...]
    source_event_ids: tuple[str, ...]
    source_sha256s: tuple[str, ...]
    query_id_sha256s: tuple[str, ...]
    attempt_sha256s: tuple[str, ...]
    trace_sha256s: tuple[str, ...]
    expected_detector_status: HostWindowV2Status
    evaluation_json: bytes | None
    prepared_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("prepared window finalizations are builder-issued")


def _prepared_payload(value: PreparedWindowFinalization) -> dict[str, object]:
    if type(value) is not PreparedWindowFinalization:
        raise _error("WINDOW_FINALIZATION_PREPARED_INVALID")
    evaluation = None
    if value.evaluation_json is not None:
        try:
            evaluation = json.loads(value.evaluation_json.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID") from exc
        if canonical_json_bytes(evaluation) != value.evaluation_json:
            raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
        _restore_evaluation(evaluation)
    return {
        "schema_version": _PREPARED_SCHEMA,
        "stream": _stream_document(value.stream_key),
        "source_revision": value.source_revision,
        "environment_manifest_sha256": value.environment_manifest_sha256,
        "window_sequence": value.window_sequence,
        "source_window_sha256": value.source_window_sha256,
        "shadow_window_sha256": value.shadow_window_sha256,
        "assembled_manifest_sha256": value.assembled_manifest_sha256,
        "source_sequences": list(value.source_sequences),
        "source_event_ids": list(value.source_event_ids),
        "source_sha256s": list(value.source_sha256s),
        "query_id_sha256s": list(value.query_id_sha256s),
        "attempt_sha256s": list(value.attempt_sha256s),
        "trace_sha256s": list(value.trace_sha256s),
        "expected_detector_status": value.expected_detector_status.value,
        "evaluation": evaluation,
    }


def _prepared_from_payload(payload: object, digest: object) -> PreparedWindowFinalization:
    fields = {
        "schema_version",
        "stream",
        "source_revision",
        "environment_manifest_sha256",
        "window_sequence",
        "source_window_sha256",
        "shadow_window_sha256",
        "assembled_manifest_sha256",
        "source_sequences",
        "source_event_ids",
        "source_sha256s",
        "query_id_sha256s",
        "attempt_sha256s",
        "trace_sha256s",
        "expected_detector_status",
        "evaluation",
    }
    if type(payload) is not dict or set(payload) != fields or payload["schema_version"] != _PREPARED_SCHEMA:
        raise _error("WINDOW_FINALIZATION_PREPARED_INVALID")
    expected_digest = _digest(_PREPARED_DOMAIN, payload)
    if digest != expected_digest:
        raise _error("WINDOW_FINALIZATION_PREPARED_DIGEST_INVALID")
    if any(
        type(payload[name]) is not list
        for name in (
            "source_sequences",
            "source_event_ids",
            "source_sha256s",
            "query_id_sha256s",
            "attempt_sha256s",
            "trace_sha256s",
        )
    ):
        raise _error("WINDOW_FINALIZATION_PREPARED_INVALID")
    try:
        stream = _stream_from_document(payload["stream"])
        status = HostWindowV2Status(payload["expected_detector_status"])
        sequences = tuple(payload["source_sequences"])
        event_ids = tuple(payload["source_event_ids"])
        source_hashes = tuple(payload["source_sha256s"])
        query_hashes = tuple(payload["query_id_sha256s"])
        attempt_hashes = tuple(payload["attempt_sha256s"])
        trace_hashes = tuple(payload["trace_sha256s"])
    except (TypeError, ValueError) as exc:
        raise _error("WINDOW_FINALIZATION_PREPARED_INVALID") from exc
    sequence = payload["window_sequence"]
    if (
        type(sequence) is not int
        or sequence < 0
        or any(type(item) is not int for item in sequences)
        or sequences != tuple(range(sequence * 200, (sequence + 1) * 200))
        or len(event_ids) != 200
        or len(set(event_ids)) != 200
        or len(source_hashes) != 200
        or len(set(source_hashes)) != 200
        or len(query_hashes) != 200
        or len(set(query_hashes)) != 200
        or len(attempt_hashes) != 4
        or len(set(attempt_hashes)) != 4
        or len(trace_hashes) != 4
        or len(set(trace_hashes)) != 4
        or status not in {HostWindowV2Status.REBASELINE, HostWindowV2Status.EVALUATED}
        or (status is HostWindowV2Status.EVALUATED) != (payload["evaluation"] is not None)
    ):
        raise _error("WINDOW_FINALIZATION_PREPARED_INVALID")
    for value in event_ids:
        _text(value, code="WINDOW_FINALIZATION_PREPARED_INVALID")
    for value in (*source_hashes, *query_hashes, *attempt_hashes, *trace_hashes):
        _sha(value, code="WINDOW_FINALIZATION_PREPARED_INVALID")
    for name in (
        "environment_manifest_sha256",
        "source_window_sha256",
        "shadow_window_sha256",
        "assembled_manifest_sha256",
    ):
        _sha(payload[name], code="WINDOW_FINALIZATION_PREPARED_INVALID")
    _text(payload["source_revision"], code="WINDOW_FINALIZATION_PREPARED_INVALID")
    evaluation_json = None
    if payload["evaluation"] is not None:
        _restore_evaluation(payload["evaluation"])
        evaluation_json = canonical_json_bytes(payload["evaluation"])
    result = object.__new__(PreparedWindowFinalization)
    values = {
        "stream_key": stream,
        "source_revision": payload["source_revision"],
        "environment_manifest_sha256": payload["environment_manifest_sha256"],
        "window_sequence": sequence,
        "source_window_sha256": payload["source_window_sha256"],
        "shadow_window_sha256": payload["shadow_window_sha256"],
        "assembled_manifest_sha256": payload["assembled_manifest_sha256"],
        "source_sequences": sequences,
        "source_event_ids": event_ids,
        "source_sha256s": source_hashes,
        "query_id_sha256s": query_hashes,
        "attempt_sha256s": attempt_hashes,
        "trace_sha256s": trace_hashes,
        "expected_detector_status": status,
        "evaluation_json": evaluation_json,
        "prepared_sha256": expected_digest,
    }
    for name, item in values.items():
        object.__setattr__(result, name, item)
    if _prepared_payload(result) != payload:
        raise _error("WINDOW_FINALIZATION_PREPARED_INVALID")
    return result


def build_prepared_window_finalization(
    *,
    bundle: V2ShadowWindowBundle,
    attempts: tuple[ShadowAttemptRecord, ...],
    source_revision: str,
    environment_manifest_sha256: str,
    expected_detector_status: HostWindowV2Status,
    decision: DriftDecision | None = None,
    pending: Mapping[str, object] | None = None,
) -> PreparedWindowFinalization:
    if (
        type(bundle) is not V2ShadowWindowBundle
        or type(attempts) is not tuple
        or len(attempts) != 4
        or any(
            type(item) is not ShadowAttemptRecord
            or item.status is not ShadowAttemptStatus.COMPLETED
            or item.envelope is None
            for item in attempts
        )
    ):
        raise _error("WINDOW_FINALIZATION_PREPARED_INVALID")
    sources = bundle.sources
    if (
        len(sources) != 200
        or bundle.window_sequence != bundle.shadow_window.window_sequence
        or source_window_sha256(sources)
        != bundle.shadow_window.source_window_sha256
        or tuple(item.envelope for item in attempts) != bundle.envelopes
        or any(
            item.stream_key != bundle.shadow_window.stream_key
            or item.source_revision != source_revision
            or item.environment_manifest_sha256 != environment_manifest_sha256
            for item in sources
        )
    ):
        raise _error("WINDOW_FINALIZATION_PREPARED_INVALID")
    evaluation = None
    if expected_detector_status is HostWindowV2Status.EVALUATED:
        if decision is None or pending is None:
            raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
        if (
            pending.get("stream_key") != bundle.shadow_window.stream_key
            or pending.get("current_window_sequence") != bundle.window_sequence
            or pending.get("current_source_window_sha256")
            != bundle.shadow_window.source_window_sha256
            or pending.get("current_shadow_window_sha256")
            != bundle.shadow_window.shadow_window_sha256
            or pending.get("current_assembled_manifest_sha256")
            != bundle.assembled.manifest_sha256
            or pending.get("source_revision") != source_revision
            or pending.get("environment_manifest_sha256")
            != environment_manifest_sha256
        ):
            raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
        evaluation = _evaluation_document(decision, pending)
    elif decision is not None or pending is not None:
        raise _error("WINDOW_FINALIZATION_EVALUATION_INVALID")
    payload = {
        "schema_version": _PREPARED_SCHEMA,
        "stream": _stream_document(bundle.shadow_window.stream_key),
        "source_revision": _text(
            source_revision, code="WINDOW_FINALIZATION_PREPARED_INVALID"
        ),
        "environment_manifest_sha256": _sha(
            environment_manifest_sha256,
            code="WINDOW_FINALIZATION_PREPARED_INVALID",
        ),
        "window_sequence": bundle.window_sequence,
        "source_window_sha256": bundle.shadow_window.source_window_sha256,
        "shadow_window_sha256": bundle.shadow_window.shadow_window_sha256,
        "assembled_manifest_sha256": bundle.assembled.manifest_sha256,
        "source_sequences": [item.source_sequence for item in sources],
        "source_event_ids": [item.event_id for item in sources],
        "source_sha256s": [item.source_sha256 for item in sources],
        "query_id_sha256s": [item.query_id_sha256 for item in sources],
        "attempt_sha256s": [item.identity.attempt_sha256 for item in attempts],
        "trace_sha256s": [item.envelope.expected_trace_sha256 for item in attempts],
        "expected_detector_status": expected_detector_status.value,
        "evaluation": evaluation,
    }
    return _prepared_from_payload(payload, _digest(_PREPARED_DOMAIN, payload))


def restore_prepared_evaluation(
    prepared: PreparedWindowFinalization,
) -> tuple[DriftDecision, dict[str, object]] | None:
    canonical = _prepared_from_payload(
        _prepared_payload(prepared), prepared.prepared_sha256
    )
    if canonical.evaluation_json is None:
        return None
    return _restore_evaluation(json.loads(canonical.evaluation_json.decode("utf-8")))


class WindowFinalizationPhase(StrEnum):
    PREPARED = "PREPARED"
    DETECTOR_COMMITTED = "DETECTOR_COMMITTED"
    ATTESTATION_COMMITTED = "ATTESTATION_COMMITTED"
    ATTESTATION_NOT_REQUIRED = "ATTESTATION_NOT_REQUIRED"
    SOURCE_ACKNOWLEDGED = "SOURCE_ACKNOWLEDGED"
    FINALIZED = "FINALIZED"


@dataclass(frozen=True, slots=True)
class WindowFinalizationState:
    prepared: PreparedWindowFinalization
    phase: WindowFinalizationPhase
    detector_event_sha256: str | None = None
    detector_head_sha256: str | None = None
    attestation_record_sha256: str | None = None
    attestation_sha256: str | None = None
    acknowledgement_head_sha256: str | None = None


_SCHEMA_SQL = (
    "CREATE TABLE finalization_binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), binding_json BLOB NOT NULL, binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256)=64)) STRICT",
    "CREATE TABLE finalization_events (event_sequence INTEGER PRIMARY KEY CHECK(event_sequence>=0), window_sequence INTEGER NOT NULL CHECK(window_sequence>=0), phase TEXT NOT NULL CHECK(phase IN ('PREPARED','DETECTOR_COMMITTED','ATTESTATION_COMMITTED','ATTESTATION_NOT_REQUIRED','SOURCE_ACKNOWLEDGED','FINALIZED')), prepared_sha256 TEXT NOT NULL CHECK(length(prepared_sha256)=64), event_json BLOB NOT NULL, previous_event_sha256 TEXT, event_sha256 TEXT NOT NULL UNIQUE CHECK(length(event_sha256)=64)) STRICT",
    "CREATE INDEX finalization_events_window ON finalization_events(window_sequence,event_sequence)",
    "CREATE TRIGGER finalization_binding_no_update BEFORE UPDATE ON finalization_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER finalization_binding_no_delete BEFORE DELETE ON finalization_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER finalization_events_no_update BEFORE UPDATE ON finalization_events BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER finalization_events_no_delete BEFORE DELETE ON finalization_events BEGIN SELECT RAISE(ABORT,'append-only'); END",
)


def _normalize_sql(value: str) -> str:
    return " ".join(value.strip().removesuffix(";").split())


class SQLiteWindowFinalizationStore:
    """Exclusive-writer append-only cross-store reconciliation journal."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        stream_key: MonitorStreamKey,
        source_revision: str,
        environment_manifest_sha256: str,
    ) -> None:
        self.path = Path(path)
        self.stream_key = stream_key
        self.source_revision = _text(
            source_revision, code="WINDOW_FINALIZATION_BINDING_INVALID"
        )
        self.environment_manifest_sha256 = _sha(
            environment_manifest_sha256, code="WINDOW_FINALIZATION_BINDING_INVALID"
        )
        self._binding = {
            "schema_version": _BINDING_SCHEMA,
            "stream": _stream_document(stream_key),
            "source_revision": self.source_revision,
            "environment_manifest_sha256": self.environment_manifest_sha256,
        }
        self._mutex = threading.RLock()
        self._pid = os.getpid()
        self._closed = False
        self._lock_handle = None
        self._lock_inode = None
        self._lock_path: Path | None = None
        self._db_inode: tuple[int, int] | None = None
        self._open()

    def __enter__(self) -> "SQLiteWindowFinalizationStore":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _open(self) -> None:
        parent = self.path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = parent.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) & 0o022
        ):
            raise _error("WINDOW_FINALIZATION_PATH_UNSAFE")
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self._lock_path = lock_path
        fd = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        self._lock_handle = os.fdopen(fd, "a+b")
        lock_info = os.fstat(fd)
        if (
            not stat.S_ISREG(lock_info.st_mode)
            or lock_info.st_nlink != 1
            or lock_info.st_uid != os.geteuid()
            or stat.S_IMODE(lock_info.st_mode) != 0o600
        ):
            self.close()
            raise _error("WINDOW_FINALIZATION_PATH_UNSAFE")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.close()
            raise _error("WINDOW_FINALIZATION_STORE_BUSY") from exc
        inode = (lock_info.st_dev, lock_info.st_ino)
        with _OWNERSHIP_LOCK:
            if inode in _OWNED_LOCK_INODES:
                self.close()
                raise _error("WINDOW_FINALIZATION_STORE_BUSY")
            _OWNED_LOCK_INODES.add(inode)
        self._lock_inode = inode
        created = not self.path.exists()
        if created:
            created_fd = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            os.close(created_fd)
        path_info = os.lstat(self.path)
        if (
            not stat.S_ISREG(path_info.st_mode)
            or path_info.st_nlink != 1
            or path_info.st_uid != os.geteuid()
            or stat.S_IMODE(path_info.st_mode) != 0o600
        ):
            self.close()
            raise _error("WINDOW_FINALIZATION_PATH_UNSAFE")
        self._db_inode = (path_info.st_dev, path_info.st_ino)
        self._db = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
        try:
            self._db.execute("PRAGMA foreign_keys=ON")
            if created:
                self._db.execute("PRAGMA journal_mode=DELETE")
                self._db.execute("PRAGMA synchronous=FULL")
                self._db.execute("PRAGMA trusted_schema=OFF")
                self._db.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA_SQL:
                    self._db.execute(statement)
                self._db.execute(f"PRAGMA user_version={_DB_VERSION}")
                self._db.execute(
                    "INSERT INTO finalization_binding VALUES(1,?,?)",
                    (
                        canonical_json_bytes(self._binding),
                        _digest(_BINDING_DOMAIN, self._binding),
                    ),
                )
                self._db.execute("COMMIT")
            else:
                mode = str(self._db.execute("PRAGMA journal_mode").fetchone()[0])
                if mode.lower() != "delete":
                    raise _error("WINDOW_FINALIZATION_SCHEMA_INVALID")
                self._db.execute("PRAGMA synchronous=FULL")
                self._db.execute("PRAGMA trusted_schema=OFF")
            self._states()
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        owner = os.getpid() == self._pid
        db = getattr(self, "_db", None)
        if db is not None:
            try:
                db.close()
            except sqlite3.Error:
                pass
        if self._lock_handle is not None and not self._lock_handle.closed:
            try:
                if owner:
                    fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._lock_handle.close()
        if owner and self._lock_inode is not None:
            with _OWNERSHIP_LOCK:
                _OWNED_LOCK_INODES.discard(self._lock_inode)
        self._lock_inode = None

    def _require_live(self) -> None:
        if self._closed:
            raise _error("WINDOW_FINALIZATION_STORE_CLOSED")
        if os.getpid() != self._pid:
            raise _error("WINDOW_FINALIZATION_STORE_FORKED")
        try:
            path_info = os.lstat(self.path)
            lock_info = os.lstat(self._lock_path)
        except (OSError, TypeError) as exc:
            raise _error("WINDOW_FINALIZATION_PATH_UNSAFE") from exc
        if (
            not stat.S_ISREG(path_info.st_mode)
            or path_info.st_nlink != 1
            or path_info.st_uid != os.geteuid()
            or stat.S_IMODE(path_info.st_mode) != 0o600
            or (path_info.st_dev, path_info.st_ino) != self._db_inode
            or not stat.S_ISREG(lock_info.st_mode)
            or lock_info.st_nlink != 1
            or lock_info.st_uid != os.geteuid()
            or stat.S_IMODE(lock_info.st_mode) != 0o600
            or (lock_info.st_dev, lock_info.st_ino) != self._lock_inode
        ):
            raise _error("WINDOW_FINALIZATION_PATH_UNSAFE")

    def _verify_schema(self) -> None:
        self._require_live()
        if self._db.execute("PRAGMA user_version").fetchone()[0] != _DB_VERSION:
            raise _error("WINDOW_FINALIZATION_SCHEMA_INVALID")
        actual = {
            row[0]: _normalize_sql(row[1])
            for row in self._db.execute(
                "SELECT name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        expected = {
            statement.split()[2]: _normalize_sql(statement)
            for statement in _SCHEMA_SQL
        }
        if actual != expected:
            raise _error("WINDOW_FINALIZATION_SCHEMA_INVALID")
        row = self._db.execute(
            "SELECT binding_json,binding_sha256 FROM finalization_binding WHERE singleton=1"
        ).fetchone()
        if (
            row is None
            or bytes(row[0]) != canonical_json_bytes(self._binding)
            or row[1] != _digest(_BINDING_DOMAIN, self._binding)
        ):
            raise _error("WINDOW_FINALIZATION_BINDING_MISMATCH")

    def _documents(self) -> tuple[dict[str, object], ...]:
        self._verify_schema()
        documents = []
        previous = None
        for expected, row in enumerate(
            self._db.execute(
                "SELECT event_sequence,window_sequence,phase,prepared_sha256,event_json,previous_event_sha256,event_sha256 FROM finalization_events ORDER BY event_sequence"
            )
        ):
            try:
                document = json.loads(bytes(row[4]).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _error("WINDOW_FINALIZATION_EVENT_INVALID") from exc
            if (
                row[0] != expected
                or bytes(row[4]) != canonical_json_bytes(document)
                or row[5] != previous
                or row[6] != _digest(_EVENT_DOMAIN, document)
                or document.get("event_sequence") != expected
                or document.get("window_sequence") != row[1]
                or document.get("phase") != row[2]
                or document.get("prepared_sha256") != row[3]
                or document.get("previous_event_sha256") != previous
            ):
                raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
            previous = row[6]
            documents.append(document)
        return tuple(documents)

    def _states(self) -> tuple[WindowFinalizationState, ...]:
        states: list[WindowFinalizationState] = []
        for document in self._documents():
            if type(document) is not dict or set(document) != {
                "schema_version",
                "event_sequence",
                "window_sequence",
                "phase",
                "prepared_sha256",
                "details",
                "recorded_at_utc",
                "previous_event_sha256",
            } or document["schema_version"] != _EVENT_SCHEMA:
                raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
            _timestamp(document["recorded_at_utc"])
            try:
                phase = WindowFinalizationPhase(document["phase"])
            except (TypeError, ValueError) as exc:
                raise _error("WINDOW_FINALIZATION_EVENT_INVALID") from exc
            sequence = document["window_sequence"]
            if type(sequence) is not int or sequence < 0:
                raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
            details = document["details"]
            if phase is WindowFinalizationPhase.PREPARED:
                if sequence != len(states) or (states and states[-1].phase is not WindowFinalizationPhase.FINALIZED):
                    raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
                if type(details) is not dict or set(details) != {"prepared_payload"}:
                    raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
                prepared = _prepared_from_payload(
                    details["prepared_payload"], document["prepared_sha256"]
                )
                if prepared.window_sequence != sequence:
                    raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
                states.append(WindowFinalizationState(prepared, phase))
                continue
            if sequence >= len(states):
                raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
            prior = states[sequence]
            if document["prepared_sha256"] != prior.prepared.prepared_sha256:
                raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
            if phase is WindowFinalizationPhase.DETECTOR_COMMITTED:
                if prior.phase is not WindowFinalizationPhase.PREPARED or type(details) is not dict or set(details) != {"detector_event_sha256", "detector_head_sha256", "detector_status"}:
                    raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
                event_sha = _sha(details["detector_event_sha256"], code="WINDOW_FINALIZATION_EVENT_INVALID")
                head_sha = details["detector_head_sha256"]
                if head_sha is not None:
                    _sha(head_sha, code="WINDOW_FINALIZATION_EVENT_INVALID")
                if details["detector_status"] != prior.prepared.expected_detector_status.value:
                    raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
                if (
                    (prior.prepared.expected_detector_status is HostWindowV2Status.REBASELINE)
                    != (head_sha is None)
                ):
                    raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
                states[sequence] = WindowFinalizationState(prior.prepared, phase, event_sha, head_sha)
            elif phase in {WindowFinalizationPhase.ATTESTATION_COMMITTED, WindowFinalizationPhase.ATTESTATION_NOT_REQUIRED}:
                if prior.phase is not WindowFinalizationPhase.DETECTOR_COMMITTED:
                    raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
                if phase is WindowFinalizationPhase.ATTESTATION_COMMITTED:
                    if prior.detector_head_sha256 is None or type(details) is not dict or set(details) != {"attestation_record_sha256", "attestation_sha256"}:
                        raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
                    record_sha = _sha(details["attestation_record_sha256"], code="WINDOW_FINALIZATION_EVENT_INVALID")
                    attestation_sha = _sha(details["attestation_sha256"], code="WINDOW_FINALIZATION_EVENT_INVALID")
                else:
                    if prior.detector_head_sha256 is not None or details != {"reason": "DETECTOR_HEAD_ABSENT"}:
                        raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
                    record_sha = None
                    attestation_sha = None
                states[sequence] = WindowFinalizationState(
                    prior.prepared, phase, prior.detector_event_sha256,
                    prior.detector_head_sha256, record_sha, attestation_sha,
                )
            elif phase is WindowFinalizationPhase.SOURCE_ACKNOWLEDGED:
                if prior.phase not in {WindowFinalizationPhase.ATTESTATION_COMMITTED, WindowFinalizationPhase.ATTESTATION_NOT_REQUIRED} or type(details) is not dict or set(details) != {"acknowledgement_head_sha256", "acknowledged_count"} or details["acknowledged_count"] != (sequence + 1) * 200:
                    raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
                ack_sha = _sha(details["acknowledgement_head_sha256"], code="WINDOW_FINALIZATION_EVENT_INVALID")
                states[sequence] = WindowFinalizationState(
                    prior.prepared, phase, prior.detector_event_sha256,
                    prior.detector_head_sha256, prior.attestation_record_sha256,
                    prior.attestation_sha256, ack_sha,
                )
            elif phase is WindowFinalizationPhase.FINALIZED:
                if prior.phase is not WindowFinalizationPhase.SOURCE_ACKNOWLEDGED or details != {}:
                    raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
                states[sequence] = WindowFinalizationState(
                    prior.prepared, phase, prior.detector_event_sha256,
                    prior.detector_head_sha256, prior.attestation_record_sha256,
                    prior.attestation_sha256, prior.acknowledgement_head_sha256,
                )
            else:
                raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
        return tuple(states)

    def states(self) -> tuple[WindowFinalizationState, ...]:
        with self._mutex:
            return self._states()

    def next_window_sequence(self) -> int:
        states = self.states()
        return len(states) if not states or states[-1].phase is WindowFinalizationPhase.FINALIZED else len(states) - 1

    def pending(self) -> WindowFinalizationState | None:
        states = self.states()
        if states and states[-1].phase is not WindowFinalizationPhase.FINALIZED:
            return states[-1]
        return None

    def _append(
        self,
        *,
        window_sequence: int,
        phase: WindowFinalizationPhase,
        prepared_sha256: str,
        details: dict[str, object],
        recorded_at_utc: str,
    ) -> WindowFinalizationState:
        with self._mutex:
            states = self._states()
            documents = self._documents()
            if phase is WindowFinalizationPhase.PREPARED:
                transition_valid = (
                    window_sequence == len(states)
                    and (not states or states[-1].phase is WindowFinalizationPhase.FINALIZED)
                )
            else:
                required_prior = {
                    WindowFinalizationPhase.DETECTOR_COMMITTED: {
                        WindowFinalizationPhase.PREPARED
                    },
                    WindowFinalizationPhase.ATTESTATION_COMMITTED: {
                        WindowFinalizationPhase.DETECTOR_COMMITTED
                    },
                    WindowFinalizationPhase.ATTESTATION_NOT_REQUIRED: {
                        WindowFinalizationPhase.DETECTOR_COMMITTED
                    },
                    WindowFinalizationPhase.SOURCE_ACKNOWLEDGED: {
                        WindowFinalizationPhase.ATTESTATION_COMMITTED,
                        WindowFinalizationPhase.ATTESTATION_NOT_REQUIRED,
                    },
                    WindowFinalizationPhase.FINALIZED: {
                        WindowFinalizationPhase.SOURCE_ACKNOWLEDGED
                    },
                }
                transition_valid = (
                    bool(states)
                    and window_sequence == len(states) - 1
                    and states[-1].prepared.prepared_sha256 == prepared_sha256
                    and states[-1].phase in required_prior.get(phase, set())
                )
            if not transition_valid:
                raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
            event_sequence = len(documents)
            previous = None if not documents else _digest(_EVENT_DOMAIN, documents[-1])
            payload = {
                "schema_version": _EVENT_SCHEMA,
                "event_sequence": event_sequence,
                "window_sequence": window_sequence,
                "phase": phase.value,
                "prepared_sha256": _sha(prepared_sha256, code="WINDOW_FINALIZATION_EVENT_INVALID"),
                "details": details,
                "recorded_at_utc": _timestamp(recorded_at_utc),
                "previous_event_sha256": previous,
            }
            digest = _digest(_EVENT_DOMAIN, payload)
            try:
                self._db.execute("BEGIN IMMEDIATE")
                if self._states() != states:
                    raise _error("WINDOW_FINALIZATION_HEAD_DRIFT")
                self._db.execute(
                    "INSERT INTO finalization_events VALUES(?,?,?,?,?,?,?)",
                    (
                        event_sequence,
                        window_sequence,
                        phase.value,
                        prepared_sha256,
                        canonical_json_bytes(payload),
                        previous,
                        digest,
                    ),
                )
                self._db.execute("COMMIT")
            except WindowFinalizationError:
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
                raise _error("WINDOW_FINALIZATION_WRITE_FAILED") from exc
            return self._states()[window_sequence]

    def prepare(
        self, prepared: PreparedWindowFinalization, *, recorded_at_utc: str
    ) -> WindowFinalizationState:
        canonical = _prepared_from_payload(
            _prepared_payload(prepared), prepared.prepared_sha256
        )
        states = self.states()
        if (
            canonical.window_sequence != len(states)
            or (states and states[-1].phase is not WindowFinalizationPhase.FINALIZED)
        ):
            raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
        return self._append(
            window_sequence=canonical.window_sequence,
            phase=WindowFinalizationPhase.PREPARED,
            prepared_sha256=canonical.prepared_sha256,
            details={"prepared_payload": _prepared_payload(canonical)},
            recorded_at_utc=recorded_at_utc,
        )

    def record_detector(
        self,
        *,
        detector_event_sha256: str,
        detector_head_sha256: str | None,
        detector_status: HostWindowV2Status,
        recorded_at_utc: str,
    ) -> WindowFinalizationState:
        pending = self.pending()
        if pending is None or pending.phase is not WindowFinalizationPhase.PREPARED:
            raise _error("WINDOW_FINALIZATION_PENDING_MISSING")
        _sha(detector_event_sha256, code="WINDOW_FINALIZATION_EVENT_INVALID")
        if detector_head_sha256 is not None:
            _sha(detector_head_sha256, code="WINDOW_FINALIZATION_EVENT_INVALID")
        if detector_status is not pending.prepared.expected_detector_status:
            raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
        if (
            (detector_status is HostWindowV2Status.REBASELINE)
            != (detector_head_sha256 is None)
        ):
            raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
        return self._append(
            window_sequence=pending.prepared.window_sequence,
            phase=WindowFinalizationPhase.DETECTOR_COMMITTED,
            prepared_sha256=pending.prepared.prepared_sha256,
            details={
                "detector_event_sha256": detector_event_sha256,
                "detector_head_sha256": detector_head_sha256,
                "detector_status": detector_status.value,
            },
            recorded_at_utc=recorded_at_utc,
        )

    def record_attestation(
        self,
        *,
        attestation_record_sha256: str,
        attestation_sha256: str,
        recorded_at_utc: str,
    ) -> WindowFinalizationState:
        pending = self.pending()
        if pending is None or pending.phase is not WindowFinalizationPhase.DETECTOR_COMMITTED:
            raise _error("WINDOW_FINALIZATION_PENDING_MISSING")
        _sha(attestation_record_sha256, code="WINDOW_FINALIZATION_EVENT_INVALID")
        _sha(attestation_sha256, code="WINDOW_FINALIZATION_EVENT_INVALID")
        if pending.detector_head_sha256 is None:
            raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
        return self._append(
            window_sequence=pending.prepared.window_sequence,
            phase=WindowFinalizationPhase.ATTESTATION_COMMITTED,
            prepared_sha256=pending.prepared.prepared_sha256,
            details={
                "attestation_record_sha256": attestation_record_sha256,
                "attestation_sha256": attestation_sha256,
            },
            recorded_at_utc=recorded_at_utc,
        )

    def record_attestation_not_required(
        self, *, recorded_at_utc: str
    ) -> WindowFinalizationState:
        pending = self.pending()
        if pending is None or pending.phase is not WindowFinalizationPhase.DETECTOR_COMMITTED:
            raise _error("WINDOW_FINALIZATION_PENDING_MISSING")
        if pending.detector_head_sha256 is not None:
            raise _error("WINDOW_FINALIZATION_TRANSITION_INVALID")
        return self._append(
            window_sequence=pending.prepared.window_sequence,
            phase=WindowFinalizationPhase.ATTESTATION_NOT_REQUIRED,
            prepared_sha256=pending.prepared.prepared_sha256,
            details={"reason": "DETECTOR_HEAD_ABSENT"},
            recorded_at_utc=recorded_at_utc,
        )

    def record_acknowledged(
        self,
        *,
        acknowledgement_head_sha256: str,
        acknowledged_count: int,
        recorded_at_utc: str,
    ) -> WindowFinalizationState:
        pending = self.pending()
        if pending is None or pending.phase not in {
            WindowFinalizationPhase.ATTESTATION_COMMITTED,
            WindowFinalizationPhase.ATTESTATION_NOT_REQUIRED,
        }:
            raise _error("WINDOW_FINALIZATION_PENDING_MISSING")
        _sha(acknowledgement_head_sha256, code="WINDOW_FINALIZATION_EVENT_INVALID")
        if (
            type(acknowledged_count) is not int
            or acknowledged_count != (pending.prepared.window_sequence + 1) * 200
        ):
            raise _error("WINDOW_FINALIZATION_EVENT_INVALID")
        return self._append(
            window_sequence=pending.prepared.window_sequence,
            phase=WindowFinalizationPhase.SOURCE_ACKNOWLEDGED,
            prepared_sha256=pending.prepared.prepared_sha256,
            details={
                "acknowledgement_head_sha256": acknowledgement_head_sha256,
                "acknowledged_count": acknowledged_count,
            },
            recorded_at_utc=recorded_at_utc,
        )

    def finalize(self, *, recorded_at_utc: str) -> WindowFinalizationState:
        pending = self.pending()
        if pending is None or pending.phase is not WindowFinalizationPhase.SOURCE_ACKNOWLEDGED:
            raise _error("WINDOW_FINALIZATION_PENDING_MISSING")
        return self._append(
            window_sequence=pending.prepared.window_sequence,
            phase=WindowFinalizationPhase.FINALIZED,
            prepared_sha256=pending.prepared.prepared_sha256,
            details={},
            recorded_at_utc=recorded_at_utc,
        )
