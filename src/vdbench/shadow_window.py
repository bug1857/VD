"""Pure EXP-005 Stage 1 assembly of persisted 50-query shadow traces.

This module deliberately stops at a single 200-query ``AssembledShadowWindow``.
It does not select the later 50-query detector audit sample and does not compare
reference/current windows.  Those operations require separately frozen detector
configuration and are outside the Stage 1 boundary.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
from numbers import Integral, Real
import re
import unicodedata

from .config import IndexTrack, Metric, NUMERIC_TOLERANCE
from .drift import canonical_serialize_tuple
from .flat_oracle_agreement import (
    FlatOracleAgreementKind,
    compare_flat_oracle_hits,
)
from .milvus import CollectionIdentity, SearchHit
from .milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from .oracle import OracleHit, OracleResult, capped_threshold_recall, threshold_violations, validate_range


TRACE_PAYLOAD_SCHEMA_VERSION = "shadow-trace-payload-v1"
WINDOW_MANIFEST_SCHEMA_VERSION = "assembled-shadow-window-manifest-v1"
TRACE_COUNT = 4
TRACE_QUERY_COUNT = 50
WINDOW_QUERY_COUNT = TRACE_COUNT * TRACE_QUERY_COUNT
SHA256_HEX = re.compile(r"[0-9a-f]{64}\Z")
RFC3339_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")


class ShadowWindowValidationError(ValueError):
    """Raised when a trace cannot be represented by the canonical payload."""


@dataclass(frozen=True, slots=True)
class PersistedShadowTraceEnvelope:
    """External persistence metadata surrounding an immutable ShadowAuditTrace."""

    trace_id: str
    captured_at_utc: str
    sequence_index: int
    declared_observation_count: int
    expected_trace_sha256: str
    trace: ShadowAuditTrace | None


@dataclass(frozen=True, slots=True)
class AssembledShadowWindow:
    """One immutable, validated 200-query raw evidence window.

    Incomplete results intentionally expose no query records or manifest digest,
    so they cannot be passed on as detector input.
    """

    window_id: object
    metric: Metric | None
    threshold_stratum: str | None
    envelopes: tuple[PersistedShadowTraceEnvelope, ...]
    query_records: tuple[ShadowQueryAuditTrace, ...]
    manifest_sha256: str | None
    complete: bool
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _TraceFacts:
    trace: ShadowAuditTrace
    trace_sha256: str
    query_configuration: tuple[float, float, int]
    flat_identity: dict[str, object]
    hnsw_identity: dict[str, object]


def _add(reasons: list[str], code: str) -> None:
    if code not in reasons:
        reasons.append(code)


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and SHA256_HEX.fullmatch(value) is not None


def _canonical_identifier(value: object, *, field: str) -> int | str:
    if isinstance(value, bool):
        raise ShadowWindowValidationError(f"{field} must not be boolean")
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, str):
        normalized = unicodedata.normalize("NFC", value)
        if not normalized:
            raise ShadowWindowValidationError(f"{field} must be non-empty")
        return normalized
    raise ShadowWindowValidationError(f"{field} must be an integer or string")


def _finite_number(value: object, *, field: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ShadowWindowValidationError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ShadowWindowValidationError(f"{field} must be finite")
    return int(value) if isinstance(value, Integral) else result


def _canonical_json_value(value: object, *, field: str) -> object:
    """Convert only portable, finite JSON values; reject every fallback type."""

    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Real):
        result = float(value)
        if not math.isfinite(result):
            raise ShadowWindowValidationError(f"{field} contains a non-finite float")
        return result
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ShadowWindowValidationError(f"{field} has a non-string mapping key")
            if key in result:
                raise ShadowWindowValidationError(f"{field} has duplicate mapping keys")
            result[key] = _canonical_json_value(item, field=f"{field}.{key}")
        return result
    if isinstance(value, (tuple, list)):
        return [
            _canonical_json_value(item, field=f"{field}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ShadowWindowValidationError(
        f"{field} has unsupported canonical payload type {type(value).__name__}"
    )


def _identity_payload(identity: CollectionIdentity | None, *, field: str) -> object:
    if identity is None:
        return None
    if not isinstance(identity, CollectionIdentity):
        raise ShadowWindowValidationError(f"{field} must be CollectionIdentity or null")
    return {
        "collection_name": _canonical_json_value(
            identity.collection_name, field=f"{field}.collection_name"
        ),
        "metric": _canonical_json_value(identity.metric, field=f"{field}.metric"),
        "index_track": _canonical_json_value(
            identity.index_track, field=f"{field}.index_track"
        ),
        "description": _canonical_json_value(
            identity.description, field=f"{field}.description"
        ),
    }


def _stage_payload(stage: ShadowAuditStageEvidence, *, field: str) -> dict[str, object]:
    if not isinstance(stage, ShadowAuditStageEvidence):
        raise ShadowWindowValidationError(f"{field} must be ShadowAuditStageEvidence")
    return {
        "stage": _canonical_json_value(stage.stage, field=f"{field}.stage"),
        "success": _canonical_json_value(stage.success, field=f"{field}.success"),
        "timed_out": _canonical_json_value(
            stage.timed_out, field=f"{field}.timed_out"
        ),
        "threshold_violation_count": _canonical_json_value(
            stage.threshold_violation_count,
            field=f"{field}.threshold_violation_count",
        ),
        "oracle_agreement": _canonical_json_value(
            stage.oracle_agreement, field=f"{field}.oracle_agreement"
        ),
        "error_type": _canonical_json_value(
            stage.error_type, field=f"{field}.error_type"
        ),
    }


def _search_hit_payload(hit: SearchHit, *, field: str) -> dict[str, object]:
    if not isinstance(hit, SearchHit):
        raise ShadowWindowValidationError(f"{field} must be SearchHit")
    return {
        "id": _canonical_json_value(hit.id, field=f"{field}.id"),
        "score": _canonical_json_value(hit.score, field=f"{field}.score"),
    }


def _oracle_payload(result: OracleResult | None, *, field: str) -> object:
    if result is None:
        return None
    if not isinstance(result, OracleResult):
        raise ShadowWindowValidationError(f"{field} must be OracleResult or null")
    hits: list[dict[str, object]] = []
    for index, hit in enumerate(result.hits):
        if not isinstance(hit, OracleHit):
            raise ShadowWindowValidationError(f"{field}.hits[{index}] must be OracleHit")
        hits.append(
            {
                "id": _canonical_json_value(hit.id, field=f"{field}.hits[{index}].id"),
                "score": _canonical_json_value(
                    hit.score, field=f"{field}.hits[{index}].score"
                ),
            }
        )
    return {
        "hits": hits,
        "full_count": _canonical_json_value(result.full_count, field=f"{field}.full_count"),
        "capped": _canonical_json_value(result.capped, field=f"{field}.capped"),
    }


def _query_payload(query: ShadowQueryAuditTrace, *, field: str) -> dict[str, object]:
    if not isinstance(query, ShadowQueryAuditTrace):
        raise ShadowWindowValidationError(f"{field} must be ShadowQueryAuditTrace")
    if not isinstance(query.stages, tuple):
        raise ShadowWindowValidationError(f"{field}.stages must be a tuple")
    return {
        "query_id": _canonical_identifier(query.query_id, field=f"{field}.query_id"),
        "query_vector": _canonical_json_value(
            query.query_vector, field=f"{field}.query_vector"
        ),
        "threshold_radius": _canonical_json_value(
            query.threshold_radius, field=f"{field}.threshold_radius"
        ),
        "range_filter": _canonical_json_value(
            query.range_filter, field=f"{field}.range_filter"
        ),
        "limit": _canonical_json_value(query.limit, field=f"{field}.limit"),
        "oracle_result": _oracle_payload(query.oracle_result, field=f"{field}.oracle_result"),
        "exact_cardinality": _canonical_json_value(
            query.exact_cardinality, field=f"{field}.exact_cardinality"
        ),
        "flat_hits": (
            None
            if query.flat_hits is None
            else [
                _search_hit_payload(hit, field=f"{field}.flat_hits[{index}]")
                for index, hit in enumerate(query.flat_hits)
            ]
        ),
        "sentinel_hits": (
            None
            if query.sentinel_hits is None
            else [
                _search_hit_payload(hit, field=f"{field}.sentinel_hits[{index}]")
                for index, hit in enumerate(query.sentinel_hits)
            ]
        ),
        "sentinel_recall": _canonical_json_value(
            query.sentinel_recall, field=f"{field}.sentinel_recall"
        ),
        "stages": [
            _stage_payload(stage, field=f"{field}.stages[{index}]")
            for index, stage in enumerate(query.stages)
        ],
    }


def _identity_evidence_payload(
    evidence: ShadowIdentityEvidence, *, field: str
) -> dict[str, object]:
    if not isinstance(evidence, ShadowIdentityEvidence):
        raise ShadowWindowValidationError(f"{field} must be ShadowIdentityEvidence")
    return {
        "track": evidence.track.value if isinstance(evidence.track, IndexTrack) else _canonical_json_value(evidence.track, field=f"{field}.track"),
        "expected_binding_id": _canonical_json_value(
            evidence.expected_binding_id, field=f"{field}.expected_binding_id"
        ),
        "pre_snapshot": _identity_payload(evidence.pre_snapshot, field=f"{field}.pre_snapshot"),
        "post_snapshot": _identity_payload(evidence.post_snapshot, field=f"{field}.post_snapshot"),
        "pre_binding_match": _canonical_json_value(
            evidence.pre_binding_match, field=f"{field}.pre_binding_match"
        ),
        "post_binding_match": _canonical_json_value(
            evidence.post_binding_match, field=f"{field}.post_binding_match"
        ),
        "pre_capture": _stage_payload(evidence.pre_capture, field=f"{field}.pre_capture"),
        "post_capture": _stage_payload(evidence.post_capture, field=f"{field}.post_capture"),
    }


def canonical_shadow_trace_payload(trace: ShadowAuditTrace) -> dict[str, object]:
    """Return the explicit ``shadow-trace-payload-v1`` mapping for a trace."""

    if not isinstance(trace, ShadowAuditTrace):
        raise ShadowWindowValidationError("trace must be ShadowAuditTrace")
    if not isinstance(trace.metric, Metric):
        raise ShadowWindowValidationError("trace.metric must be Metric")
    if not isinstance(trace.queries, tuple):
        raise ShadowWindowValidationError("trace.queries must be a tuple")
    if not isinstance(trace.reason_codes, tuple):
        raise ShadowWindowValidationError("trace.reason_codes must be a tuple")
    return {
        "schema_version": TRACE_PAYLOAD_SCHEMA_VERSION,
        "metric": trace.metric.value,
        "threshold_stratum": _canonical_json_value(
            trace.threshold_stratum, field="trace.threshold_stratum"
        ),
        "candidate_ef": _canonical_json_value(trace.candidate_ef, field="trace.candidate_ef"),
        "last_known_good_ef": _canonical_json_value(
            trace.last_known_good_ef, field="trace.last_known_good_ef"
        ),
        "sentinel_ef": _canonical_json_value(trace.sentinel_ef, field="trace.sentinel_ef"),
        "configuration_identity": _canonical_json_value(
            trace.configuration_identity, field="trace.configuration_identity"
        ),
        "data_identity": _canonical_json_value(trace.data_identity, field="trace.data_identity"),
        "flat_identity": _identity_evidence_payload(
            trace.flat_identity, field="trace.flat_identity"
        ),
        "hnsw_identity": _identity_evidence_payload(
            trace.hnsw_identity, field="trace.hnsw_identity"
        ),
        "queries": [
            _query_payload(query, field=f"trace.queries[{index}]")
            for index, query in enumerate(trace.queries)
        ],
        "complete": _canonical_json_value(trace.complete, field="trace.complete"),
        "reason_codes": [
            _canonical_json_value(value, field=f"trace.reason_codes[{index}]")
            for index, value in enumerate(trace.reason_codes)
        ],
    }


def _canonical_json_bytes(payload: object) -> bytes:
    try:
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ShadowWindowValidationError("canonical JSON encoding failed") from exc


def hash_shadow_audit_trace(trace: ShadowAuditTrace) -> str:
    """Hash exactly the trace payload, never surrounding envelope metadata."""

    return hashlib.sha256(_canonical_json_bytes(canonical_shadow_trace_payload(trace))).hexdigest()


def _valid_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or RFC3339_UTC.fullmatch(value) is None:
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return None
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        return None
    return parsed


def _identity_is_valid(
    evidence: object,
    *,
    expected_track: IndexTrack,
    reasons: list[str],
) -> dict[str, object] | None:
    if not isinstance(evidence, ShadowIdentityEvidence):
        _add(reasons, "INDEX_IDENTITY_INVALID")
        return None
    if evidence.track is not expected_track:
        _add(reasons, "INDEX_IDENTITY_INVALID")
    if not isinstance(evidence.expected_binding_id, str) or not evidence.expected_binding_id:
        _add(reasons, "INDEX_IDENTITY_INVALID")
    if not evidence.pre_binding_match or not evidence.post_binding_match:
        _add(reasons, "INDEX_IDENTITY_INVALID")
    snapshots = (evidence.pre_snapshot, evidence.post_snapshot)
    if any(not isinstance(snapshot, CollectionIdentity) for snapshot in snapshots):
        _add(reasons, "INDEX_IDENTITY_INVALID")
    for snapshot in snapshots:
        if isinstance(snapshot, CollectionIdentity) and (
            snapshot.index_track != expected_track.value
            or not isinstance(snapshot.collection_name, str)
            or not snapshot.collection_name
            or not isinstance(snapshot.metric, str)
            or not snapshot.metric
        ):
            _add(reasons, "INDEX_IDENTITY_INVALID")
    for capture in (evidence.pre_capture, evidence.post_capture):
        if not isinstance(capture, ShadowAuditStageEvidence):
            _add(reasons, "INDEX_IDENTITY_INVALID")
            continue
        if not capture.success or capture.timed_out or capture.threshold_violation_count:
            _add(reasons, "INDEX_IDENTITY_INVALID")
    try:
        payload = _identity_evidence_payload(evidence, field=expected_track.value)
    except ShadowWindowValidationError:
        _add(reasons, "TRACE_PAYLOAD_CANONICALIZATION_FAILED")
        return None
    if payload["pre_snapshot"] != payload["post_snapshot"]:
        _add(reasons, "INDEX_IDENTITY_INVALID")
    return payload


def _validate_query(
    query: object,
    *,
    metric: Metric,
    reasons: list[str],
) -> tuple[int | str, bytes, tuple[float, float, int]] | None:
    if not isinstance(query, ShadowQueryAuditTrace):
        _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
        return None
    try:
        query_id = _canonical_identifier(query.query_id, field="query_id")
        encoded_id = canonical_serialize_tuple((query_id,))
    except (TypeError, ValueError, ShadowWindowValidationError):
        _add(reasons, "QUERY_ID_INVALID")
        return None
    if not isinstance(query.query_vector, tuple) or not query.query_vector:
        _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
    else:
        try:
            for value in query.query_vector:
                _finite_number(value, field="query_vector")
        except ShadowWindowValidationError:
            _add(reasons, "NONFINITE_VALUE")
    try:
        radius = float(_finite_number(query.threshold_radius, field="threshold_radius"))
        range_filter = float(_finite_number(query.range_filter, field="range_filter"))
        if isinstance(query.limit, bool) or not isinstance(query.limit, Integral) or query.limit <= 0:
            raise ShadowWindowValidationError("limit must be positive integer")
        limit = int(query.limit)
        validate_range(metric, radius, range_filter)
    except (ShadowWindowValidationError, ValueError):
        _add(reasons, "QUERY_CONFIGURATION_INCONSISTENT")
        radius, range_filter, limit = 0.0, 0.0, 0
    if not isinstance(query.oracle_result, OracleResult):
        _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
        return query_id, encoded_id, (radius, range_filter, limit)
    oracle = query.oracle_result
    if isinstance(oracle.full_count, bool) or not isinstance(oracle.full_count, Integral) or oracle.full_count < 0:
        _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
    if not isinstance(oracle.capped, bool) or oracle.capped != (oracle.full_count > limit):
        _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
    if len(oracle.hits) > oracle.full_count:
        _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
    oracle_ids: list[int] = []
    for hit in oracle.hits:
        if not isinstance(hit, OracleHit) or isinstance(hit.id, bool) or not isinstance(hit.id, Integral):
            _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
            continue
        oracle_ids.append(int(hit.id))
        try:
            _finite_number(hit.score, field="oracle score")
        except ShadowWindowValidationError:
            _add(reasons, "NONFINITE_VALUE")
    if len(set(oracle_ids)) != len(oracle_ids):
        _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
    if (
        isinstance(query.exact_cardinality, bool)
        or not isinstance(query.exact_cardinality, Integral)
        or int(query.exact_cardinality) != int(oracle.full_count)
    ):
        _add(reasons, "EXACT_CARDINALITY_MISMATCH")
    if not isinstance(query.flat_hits, tuple) or not isinstance(query.sentinel_hits, tuple):
        _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
        return query_id, encoded_id, (radius, range_filter, limit)

    def valid_hits(hits: tuple[SearchHit, ...], *, label: str) -> list[int]:
        ids: list[int] = []
        scores: list[float] = []
        for hit in hits:
            if not isinstance(hit, SearchHit) or isinstance(hit.id, bool) or not isinstance(hit.id, Integral):
                _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
                continue
            ids.append(int(hit.id))
            try:
                scores.append(float(_finite_number(hit.score, field=f"{label} score")))
            except ShadowWindowValidationError:
                _add(reasons, "NONFINITE_VALUE")
        if len(set(ids)) != len(ids):
            _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
        try:
            if threshold_violations(
                scores,
                metric,
                radius=radius,
                range_filter=range_filter,
                tolerance=NUMERIC_TOLERANCE,
            ):
                _add(reasons, "THRESHOLD_VIOLATION")
        except ValueError:
            _add(reasons, "QUERY_CONFIGURATION_INCONSISTENT")
        return ids

    flat_ids = valid_hits(query.flat_hits, label="flat")
    sentinel_ids = valid_hits(query.sentinel_hits, label="sentinel")
    agreement = compare_flat_oracle_hits(
        flat_hits=query.flat_hits,
        oracle_result=oracle,
        metric=metric,
        radius=radius,
        range_filter=range_filter,
        limit=limit,
    )
    if agreement.kind is FlatOracleAgreementKind.MEMBERSHIP_MISMATCH:
        _add(reasons, "FLAT_ORACLE_ID_SET_MISMATCH")
    elif agreement.kind is FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH:
        _add(reasons, "FLAT_ORACLE_ORDER_MISMATCH")
    elif agreement.kind is FlatOracleAgreementKind.INVALID_EVIDENCE:
        for code in agreement.reason_codes:
            _add(reasons, code)
    try:
        recalculated = capped_threshold_recall(sentinel_ids, oracle_ids)
        supplied = float(_finite_number(query.sentinel_recall, field="sentinel_recall"))
        if abs(recalculated - supplied) > 1e-9:
            _add(reasons, "SENTINEL_RECALL_MISMATCH")
    except (ShadowWindowValidationError, ValueError):
        _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
    if not isinstance(query.stages, tuple) or not query.stages:
        _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
    else:
        for stage in query.stages:
            if not isinstance(stage, ShadowAuditStageEvidence):
                _add(reasons, "QUERY_EVIDENCE_INCOMPLETE")
                continue
            if not stage.success:
                _add(reasons, "STAGE_FAILED")
            if stage.timed_out:
                _add(reasons, "STAGE_TIMEOUT")
            if stage.threshold_violation_count:
                _add(reasons, "THRESHOLD_VIOLATION")
            if stage.oracle_agreement is False:
                _add(reasons, "STAGE_FAILED")
    return query_id, encoded_id, (radius, range_filter, limit)


def _trace_facts(trace: ShadowAuditTrace, *, reasons: list[str]) -> _TraceFacts | None:
    if not isinstance(trace.metric, Metric):
        _add(reasons, "METRIC_INVALID")
        return None
    if not isinstance(trace.threshold_stratum, str) or not trace.threshold_stratum:
        _add(reasons, "THRESHOLD_STRATUM_INVALID")
    for value, code in (
        (trace.configuration_identity, "CONFIGURATION_IDENTITY_INVALID"),
        (trace.data_identity, "DATA_IDENTITY_INVALID"),
    ):
        if not isinstance(value, str) or not value:
            _add(reasons, code)
    for value in (trace.candidate_ef, trace.last_known_good_ef, trace.sentinel_ef):
        if isinstance(value, bool) or not isinstance(value, Integral) or value <= 0:
            _add(reasons, "EF_INVALID")
    flat_identity = _identity_is_valid(
        trace.flat_identity, expected_track=IndexTrack.FLAT, reasons=reasons
    )
    hnsw_identity = _identity_is_valid(
        trace.hnsw_identity, expected_track=IndexTrack.HNSW, reasons=reasons
    )
    if not isinstance(trace.queries, tuple) or len(trace.queries) != TRACE_QUERY_COUNT:
        _add(reasons, "ACTUAL_OBSERVATION_COUNT_INVALID")
        return None
    configuration: tuple[float, float, int] | None = None
    for query in trace.queries:
        query_facts = _validate_query(query, metric=trace.metric, reasons=reasons)
        if query_facts is None:
            continue
        _, _, query_configuration = query_facts
        if configuration is None:
            configuration = query_configuration
        elif configuration != query_configuration:
            _add(reasons, "QUERY_CONFIGURATION_INCONSISTENT")
    if configuration is None or flat_identity is None or hnsw_identity is None:
        return None
    try:
        trace_sha256 = hash_shadow_audit_trace(trace)
    except ShadowWindowValidationError:
        _add(reasons, "TRACE_PAYLOAD_CANONICALIZATION_FAILED")
        return None
    return _TraceFacts(
        trace=trace,
        trace_sha256=trace_sha256,
        query_configuration=configuration,
        flat_identity=flat_identity,
        hnsw_identity=hnsw_identity,
    )


def _incomplete(
    *,
    window_id: object,
    envelopes: tuple[PersistedShadowTraceEnvelope, ...],
    reasons: list[str],
    metric: Metric | None = None,
    threshold_stratum: str | None = None,
) -> AssembledShadowWindow:
    return AssembledShadowWindow(
        window_id=window_id,
        metric=metric,
        threshold_stratum=threshold_stratum,
        envelopes=envelopes,
        query_records=(),
        manifest_sha256=None,
        complete=False,
        reason_codes=tuple(reasons),
    )


def _window_manifest_payload(
    *,
    window_id: int | str,
    metric: Metric,
    threshold_stratum: str,
    envelopes: Sequence[PersistedShadowTraceEnvelope],
    trace_sha256s: Sequence[str],
    query_ids: Sequence[int | str],
) -> dict[str, object]:
    return {
        "schema_version": WINDOW_MANIFEST_SCHEMA_VERSION,
        "window_id": window_id,
        "metric": metric.value,
        "threshold_stratum": threshold_stratum,
        "envelopes": [
            {
                "trace_id": unicodedata.normalize("NFC", envelope.trace_id),
                "captured_at_utc": envelope.captured_at_utc,
                "sequence_index": envelope.sequence_index,
                "declared_observation_count": envelope.declared_observation_count,
                "trace_payload_sha256": trace_sha256,
            }
            for envelope, trace_sha256 in zip(envelopes, trace_sha256s, strict=True)
        ],
        "total_observation_count": WINDOW_QUERY_COUNT,
        "ordered_query_ids": list(query_ids),
    }


def validate_persisted_shadow_trace_envelope(
    envelope: object,
) -> tuple[str, ...]:
    """Validate one 50-query envelope independently for fail-fast capture.

    Cross-trace invariants remain the responsibility of
    :func:`assemble_shadow_window`; this boundary exposes every reason already
    derivable from one returned trace so a later physical trace is never
    executed after an earlier canonical failure.
    """

    reasons: list[str] = []
    if not isinstance(envelope, PersistedShadowTraceEnvelope):
        return ("ENVELOPE_INVALID",)
    if not isinstance(envelope.trace_id, str) or not envelope.trace_id:
        _add(reasons, "TRACE_ID_INVALID")
    elif unicodedata.normalize("NFC", envelope.trace_id) != envelope.trace_id:
        _add(reasons, "TRACE_ID_INVALID")
    if _valid_timestamp(envelope.captured_at_utc) is None:
        _add(reasons, "TIMESTAMP_INVALID")
    if (
        isinstance(envelope.sequence_index, bool)
        or not isinstance(envelope.sequence_index, Integral)
        or not 0 <= int(envelope.sequence_index) < TRACE_COUNT
    ):
        _add(reasons, "SEQUENCE_INDEX_SET_INVALID")
    if (
        isinstance(envelope.declared_observation_count, bool)
        or envelope.declared_observation_count != TRACE_QUERY_COUNT
    ):
        _add(reasons, "DECLARED_OBSERVATION_COUNT_INVALID")
    if not _is_sha256(envelope.expected_trace_sha256):
        _add(reasons, "TRACE_SHA256_FORMAT_INVALID")
    if not isinstance(envelope.trace, ShadowAuditTrace):
        _add(reasons, "TRACE_MISSING")
        return tuple(reasons)
    if not envelope.trace.complete:
        _add(reasons, "TRACE_INCOMPLETE")
    trace_reasons: list[str] = []
    facts = _trace_facts(envelope.trace, reasons=trace_reasons)
    for code in trace_reasons:
        _add(reasons, code)
    if (
        facts is not None
        and _is_sha256(envelope.expected_trace_sha256)
        and envelope.expected_trace_sha256 != facts.trace_sha256
    ):
        _add(reasons, "TRACE_PAYLOAD_SHA256_MISMATCH")
    return tuple(reasons)


def assemble_shadow_window(
    *, window_id: object, envelopes: Sequence[PersistedShadowTraceEnvelope]
) -> AssembledShadowWindow:
    """Validate four persisted traces and assemble one complete raw window.

    Invalid input is represented as an incomplete value with explicit reason codes;
    it deliberately contains no detector-consumable query records or digest.
    """

    values = tuple(envelopes) if isinstance(envelopes, Sequence) else ()
    reasons: list[str] = []
    try:
        canonical_window_id = _canonical_identifier(window_id, field="window_id")
    except ShadowWindowValidationError:
        canonical_window_id = None
        _add(reasons, "WINDOW_ID_INVALID")
    if len(values) != TRACE_COUNT:
        _add(reasons, "ENVELOPE_COUNT_INVALID")
    if any(not isinstance(value, PersistedShadowTraceEnvelope) for value in values):
        _add(reasons, "ENVELOPE_INVALID")
    valid_envelopes = tuple(
        value for value in values if isinstance(value, PersistedShadowTraceEnvelope)
    )
    sequence_indexes = tuple(item.sequence_index for item in valid_envelopes)
    if set(sequence_indexes) != set(range(TRACE_COUNT)):
        _add(reasons, "SEQUENCE_INDEX_SET_INVALID")
    if sequence_indexes != tuple(range(TRACE_COUNT)):
        _add(reasons, "MANIFEST_ORDER_MISMATCH")

    timestamps: list[datetime] = []
    canonical_trace_ids: list[str] = []
    original_trace_ids: dict[str, str] = {}
    facts: list[_TraceFacts] = []
    for envelope in valid_envelopes:
        if not isinstance(envelope.trace_id, str) or not envelope.trace_id:
            _add(reasons, "TRACE_ID_INVALID")
        else:
            normalized_trace_id = unicodedata.normalize("NFC", envelope.trace_id)
            previous = original_trace_ids.get(normalized_trace_id)
            if previous is not None:
                _add(
                    reasons,
                    "TRACE_ID_NORMALIZATION_COLLISION"
                    if previous != envelope.trace_id
                    else "TRACE_ID_DUPLICATE",
                )
            original_trace_ids[normalized_trace_id] = envelope.trace_id
            canonical_trace_ids.append(normalized_trace_id)
        timestamp = _valid_timestamp(envelope.captured_at_utc)
        if timestamp is None:
            _add(reasons, "TIMESTAMP_INVALID")
        else:
            timestamps.append(timestamp)
        if (
            isinstance(envelope.declared_observation_count, bool)
            or envelope.declared_observation_count != TRACE_QUERY_COUNT
        ):
            _add(reasons, "DECLARED_OBSERVATION_COUNT_INVALID")
        if not _is_sha256(envelope.expected_trace_sha256):
            _add(reasons, "TRACE_SHA256_FORMAT_INVALID")
        if not isinstance(envelope.trace, ShadowAuditTrace):
            _add(reasons, "TRACE_MISSING")
            continue
        if not envelope.trace.complete:
            _add(reasons, "TRACE_INCOMPLETE")
        trace_reasons: list[str] = []
        facts_for_trace = _trace_facts(envelope.trace, reasons=trace_reasons)
        for code in trace_reasons:
            _add(reasons, code)
        if facts_for_trace is None:
            continue
        if _is_sha256(envelope.expected_trace_sha256) and (
            envelope.expected_trace_sha256 != facts_for_trace.trace_sha256
        ):
            _add(reasons, "TRACE_PAYLOAD_SHA256_MISMATCH")
        facts.append(facts_for_trace)
    if len(timestamps) == len(valid_envelopes) and any(
        later <= earlier for earlier, later in zip(timestamps, timestamps[1:])
    ):
        _add(reasons, "TIMESTAMP_NOT_STRICTLY_INCREASING")
    trace_hashes = [fact.trace_sha256 for fact in facts]
    if len(set(trace_hashes)) != len(trace_hashes):
        _add(reasons, "TRACE_PAYLOAD_DUPLICATE")

    metric: Metric | None = facts[0].trace.metric if facts else None
    stratum: str | None = facts[0].trace.threshold_stratum if facts else None
    if facts:
        first = facts[0]
        for fact in facts[1:]:
            trace = fact.trace
            if trace.metric is not first.trace.metric:
                _add(reasons, "METRIC_MISMATCH")
            if trace.threshold_stratum != first.trace.threshold_stratum:
                _add(reasons, "THRESHOLD_STRATUM_MISMATCH")
            if trace.configuration_identity != first.trace.configuration_identity:
                _add(reasons, "CONFIGURATION_IDENTITY_MISMATCH")
            if trace.data_identity != first.trace.data_identity:
                _add(reasons, "DATA_IDENTITY_MISMATCH")
            if (
                trace.candidate_ef != first.trace.candidate_ef
                or trace.last_known_good_ef != first.trace.last_known_good_ef
                or trace.sentinel_ef != first.trace.sentinel_ef
            ):
                _add(reasons, "EF_MISMATCH")
            if fact.query_configuration != first.query_configuration:
                _add(reasons, "QUERY_CONFIGURATION_INCONSISTENT")
            if (
                fact.flat_identity != first.flat_identity
                or fact.hnsw_identity != first.hnsw_identity
            ):
                _add(reasons, "INDEX_IDENTITY_INVALID")

    ordered_queries: list[ShadowQueryAuditTrace] = []
    canonical_query_ids: list[int | str] = []
    encoded_query_ids: dict[bytes, object] = {}
    query_type: type[object] | None = None
    for fact in facts:
        for query in fact.trace.queries:
            query_facts = _validate_query(query, metric=fact.trace.metric, reasons=reasons)
            if query_facts is None:
                continue
            query_id, encoded_id, _ = query_facts
            type_marker: type[object] = str if isinstance(query_id, str) else int
            if query_type is None:
                query_type = type_marker
            elif query_type is not type_marker:
                _add(reasons, "QUERY_ID_SCHEMA_MIXED")
            previous = encoded_query_ids.get(encoded_id)
            if previous is not None:
                _add(
                    reasons,
                    "QUERY_ID_NORMALIZATION_COLLISION"
                    if previous != query.query_id
                    else "QUERY_ID_DUPLICATE",
                )
            encoded_query_ids[encoded_id] = query.query_id
            ordered_queries.append(query)
            canonical_query_ids.append(query_id)
    if len(ordered_queries) != WINDOW_QUERY_COUNT:
        _add(reasons, "WINDOW_OBSERVATION_COUNT_INVALID")
    if len(encoded_query_ids) != WINDOW_QUERY_COUNT:
        _add(reasons, "QUERY_ID_DUPLICATE")
    if reasons or canonical_window_id is None or metric is None or stratum is None:
        return _incomplete(
            window_id=window_id,
            envelopes=valid_envelopes,
            reasons=reasons,
            metric=metric,
            threshold_stratum=stratum,
        )
    payload = _window_manifest_payload(
        window_id=canonical_window_id,
        metric=metric,
        threshold_stratum=stratum,
        envelopes=valid_envelopes,
        trace_sha256s=trace_hashes,
        query_ids=canonical_query_ids,
    )
    try:
        manifest_sha256 = hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()
    except ShadowWindowValidationError:
        return _incomplete(
            window_id=window_id,
            envelopes=valid_envelopes,
            reasons=[*reasons, "AGGREGATE_MANIFEST_CANONICALIZATION_FAILED"],
            metric=metric,
            threshold_stratum=stratum,
        )
    return AssembledShadowWindow(
        window_id=canonical_window_id,
        metric=metric,
        threshold_stratum=stratum,
        envelopes=valid_envelopes,
        query_records=tuple(ordered_queries),
        manifest_sha256=manifest_sha256,
        complete=True,
    )


def verify_persisted_assembled_window(
    window: AssembledShadowWindow,
    *,
    expected_manifest_sha256: object,
) -> AssembledShadowWindow:
    """Recompute a persisted aggregate manifest and fail closed on mismatch."""

    if not isinstance(window, AssembledShadowWindow):
        return _incomplete(
            window_id=None,
            envelopes=(),
            reasons=["PERSISTED_WINDOW_INVALID"],
        )
    rebuilt = assemble_shadow_window(window_id=window.window_id, envelopes=window.envelopes)
    if not rebuilt.complete:
        return rebuilt
    if not _is_sha256(expected_manifest_sha256):
        return _incomplete(
            window_id=window.window_id,
            envelopes=window.envelopes,
            reasons=["AGGREGATE_MANIFEST_SHA256_FORMAT_INVALID"],
            metric=rebuilt.metric,
            threshold_stratum=rebuilt.threshold_stratum,
        )
    if (
        window.manifest_sha256 != rebuilt.manifest_sha256
        or expected_manifest_sha256 != rebuilt.manifest_sha256
    ):
        return _incomplete(
            window_id=window.window_id,
            envelopes=window.envelopes,
            reasons=["AGGREGATE_MANIFEST_SHA256_MISMATCH"],
            metric=rebuilt.metric,
            threshold_stratum=rebuilt.threshold_stratum,
        )
    return rebuilt


__all__ = [
    "AssembledShadowWindow",
    "PersistedShadowTraceEnvelope",
    "ShadowWindowValidationError",
    "assemble_shadow_window",
    "canonical_shadow_trace_payload",
    "hash_shadow_audit_trace",
    "validate_persisted_shadow_trace_envelope",
    "verify_persisted_assembled_window",
]
