"""Restart-durable persistence for EXP-005 source trace envelopes.

The live acquisition runner will write one immutable document per 50-query
``ShadowAuditTrace``.  This module is intentionally offline: it knows only the
already-captured value objects, serializes their Stage-1 canonical trace
payload, and refuses to load anything whose schema or checksum is not exact.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from typing import Any

from .config import IndexTrack, Metric
from .milvus import CollectionIdentity, SearchHit
from .milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from .oracle import OracleHit, OracleResult
from .shadow_window import (
    PersistedShadowTraceEnvelope,
    ShadowWindowValidationError,
    canonical_shadow_trace_payload,
    hash_shadow_audit_trace,
)


SCHEMA_VERSION = "persisted-shadow-trace-envelope-v1"
_ROOT_FIELDS = frozenset(
    {
        "schema_version",
        "trace_id",
        "captured_at_utc",
        "sequence_index",
        "declared_observation_count",
        "expected_trace_sha256",
        "trace_payload",
    }
)
_TRACE_FIELDS = frozenset(
    {
        "schema_version",
        "metric",
        "threshold_stratum",
        "candidate_ef",
        "last_known_good_ef",
        "sentinel_ef",
        "configuration_identity",
        "data_identity",
        "flat_identity",
        "hnsw_identity",
        "queries",
        "complete",
        "reason_codes",
    }
)
_IDENTITY_EVIDENCE_FIELDS = frozenset(
    {
        "track",
        "expected_binding_id",
        "pre_snapshot",
        "post_snapshot",
        "pre_binding_match",
        "post_binding_match",
        "pre_capture",
        "post_capture",
    }
)
_IDENTITY_FIELDS = frozenset({"collection_name", "metric", "index_track", "description"})
_STAGE_FIELDS = frozenset(
    {"stage", "success", "timed_out", "threshold_violation_count", "oracle_agreement", "error_type"}
)
_QUERY_FIELDS = frozenset(
    {
        "query_id",
        "query_vector",
        "threshold_radius",
        "range_filter",
        "limit",
        "oracle_result",
        "exact_cardinality",
        "flat_hits",
        "sentinel_hits",
        "sentinel_recall",
        "stages",
    }
)
_ORACLE_FIELDS = frozenset({"hits", "full_count", "capped"})
_HIT_FIELDS = frozenset({"id", "score"})


class ShadowTraceArtifactError(ValueError):
    """Raised when a persisted source-trace artifact cannot be trusted."""


def _reject_constant(_: str) -> None:
    raise ValueError("non-finite JSON numeric constant")


def _no_duplicate_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _mapping(value: object, *, fields: frozenset[str], name: str) -> Mapping[str, object]:
    if not isinstance(value, dict) or frozenset(value) != fields:
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}")
    return value


def _string(value: object, *, name: str, nonempty: bool = True) -> str:
    if not isinstance(value, str) or (nonempty and not value):
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}")
    return value


def _integer(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}")
    return value


def _number(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}")
    result = float(value)
    if not math.isfinite(result):
        raise ShadowTraceArtifactError(f"MALFORMED: {name} is non-finite")
    return result


def _bool_or_none(value: object, *, name: str) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}")


def _json_value(value: object, *, name: str) -> object:
    """Accept only finite JSON-compatible values; retain their exact structure."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ShadowTraceArtifactError(f"MALFORMED: {name} is non-finite")
        return value
    if isinstance(value, list):
        return tuple(_json_value(item, name=f"{name}[]") for item in value)
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}")
        return {key: _json_value(item, name=f"{name}.{key}") for key, item in value.items()}
    raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}")


def _decode_hit(value: object, *, name: str) -> SearchHit:
    payload = _mapping(value, fields=_HIT_FIELDS, name=name)
    return SearchHit(id=_integer(payload["id"], name=f"{name}.id"), score=_number(payload["score"], name=f"{name}.score"))


def _decode_oracle(value: object, *, name: str) -> OracleResult | None:
    if value is None:
        return None
    payload = _mapping(value, fields=_ORACLE_FIELDS, name=name)
    if not isinstance(payload["hits"], list):
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}.hits")
    hits = tuple(
        OracleHit(id=hit.id, score=hit.score)
        for index, item in enumerate(payload["hits"])
        for hit in (_decode_hit(item, name=f"{name}.hits[{index}]"),)
    )
    capped = payload["capped"]
    if not isinstance(capped, bool):
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}.capped")
    return OracleResult(
        hits=hits,
        full_count=_integer(payload["full_count"], name=f"{name}.full_count"),
        capped=capped,
    )


def _decode_stage(value: object, *, name: str) -> ShadowAuditStageEvidence:
    payload = _mapping(value, fields=_STAGE_FIELDS, name=name)
    if not isinstance(payload["success"], bool) or not isinstance(payload["timed_out"], bool):
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}")
    error_type = payload["error_type"]
    if error_type is not None and not isinstance(error_type, str):
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}.error_type")
    return ShadowAuditStageEvidence(
        stage=_string(payload["stage"], name=f"{name}.stage"),
        success=payload["success"],
        timed_out=payload["timed_out"],
        threshold_violation_count=_integer(
            payload["threshold_violation_count"], name=f"{name}.threshold_violation_count"
        ),
        oracle_agreement=_bool_or_none(payload["oracle_agreement"], name=f"{name}.oracle_agreement"),
        error_type=error_type,
    )


def _decode_identity(value: object, *, name: str) -> CollectionIdentity | None:
    if value is None:
        return None
    payload = _mapping(value, fields=_IDENTITY_FIELDS, name=name)
    return CollectionIdentity(
        collection_name=_string(payload["collection_name"], name=f"{name}.collection_name"),
        metric=_string(payload["metric"], name=f"{name}.metric"),
        index_track=_string(payload["index_track"], name=f"{name}.index_track"),
        description=_json_value(payload["description"], name=f"{name}.description"),
    )


def _decode_identity_evidence(value: object, *, name: str) -> ShadowIdentityEvidence:
    payload = _mapping(value, fields=_IDENTITY_EVIDENCE_FIELDS, name=name)
    try:
        track = IndexTrack(_string(payload["track"], name=f"{name}.track"))
    except ValueError as exc:
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}.track") from exc
    pre_match = payload["pre_binding_match"]
    post_match = payload["post_binding_match"]
    if not isinstance(pre_match, bool) or not isinstance(post_match, bool):
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}")
    return ShadowIdentityEvidence(
        track=track,
        expected_binding_id=_string(payload["expected_binding_id"], name=f"{name}.expected_binding_id"),
        pre_snapshot=_decode_identity(payload["pre_snapshot"], name=f"{name}.pre_snapshot"),
        post_snapshot=_decode_identity(payload["post_snapshot"], name=f"{name}.post_snapshot"),
        pre_binding_match=pre_match,
        post_binding_match=post_match,
        pre_capture=_decode_stage(payload["pre_capture"], name=f"{name}.pre_capture"),
        post_capture=_decode_stage(payload["post_capture"], name=f"{name}.post_capture"),
    )


def _decode_query(value: object, *, name: str) -> ShadowQueryAuditTrace:
    payload = _mapping(value, fields=_QUERY_FIELDS, name=name)
    query_id = payload["query_id"]
    if isinstance(query_id, bool) or not isinstance(query_id, (int, str)) or query_id == "":
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}.query_id")
    if not isinstance(payload["query_vector"], list) or not payload["query_vector"]:
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}.query_vector")
    for field in ("flat_hits", "sentinel_hits"):
        if payload[field] is not None and not isinstance(payload[field], list):
            raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}.{field}")
    if not isinstance(payload["stages"], list):
        raise ShadowTraceArtifactError(f"SCHEMA_MISMATCH: {name}.stages")
    return ShadowQueryAuditTrace(
        query_id=query_id,
        query_vector=tuple(_number(item, name=f"{name}.query_vector") for item in payload["query_vector"]),
        threshold_radius=_number(payload["threshold_radius"], name=f"{name}.threshold_radius"),
        range_filter=_number(payload["range_filter"], name=f"{name}.range_filter"),
        limit=_integer(payload["limit"], name=f"{name}.limit"),
        oracle_result=_decode_oracle(payload["oracle_result"], name=f"{name}.oracle_result"),
        exact_cardinality=(
            None if payload["exact_cardinality"] is None else _integer(payload["exact_cardinality"], name=f"{name}.exact_cardinality")
        ),
        flat_hits=(
            None if payload["flat_hits"] is None else tuple(_decode_hit(item, name=f"{name}.flat_hits[{index}]") for index, item in enumerate(payload["flat_hits"]))
        ),
        sentinel_hits=(
            None if payload["sentinel_hits"] is None else tuple(_decode_hit(item, name=f"{name}.sentinel_hits[{index}]") for index, item in enumerate(payload["sentinel_hits"]))
        ),
        sentinel_recall=(
            None if payload["sentinel_recall"] is None else _number(payload["sentinel_recall"], name=f"{name}.sentinel_recall")
        ),
        stages=tuple(_decode_stage(item, name=f"{name}.stages[{index}]") for index, item in enumerate(payload["stages"])),
    )


def _decode_trace(value: object) -> ShadowAuditTrace:
    payload = _mapping(value, fields=_TRACE_FIELDS, name="trace_payload")
    if payload["schema_version"] != "shadow-trace-payload-v1":
        raise ShadowTraceArtifactError("SCHEMA_MISMATCH: trace_payload.schema_version")
    try:
        metric = Metric(_string(payload["metric"], name="trace_payload.metric"))
    except ValueError as exc:
        raise ShadowTraceArtifactError("SCHEMA_MISMATCH: trace_payload.metric") from exc
    if not isinstance(payload["queries"], list) or not isinstance(payload["reason_codes"], list):
        raise ShadowTraceArtifactError("SCHEMA_MISMATCH: trace_payload")
    if not isinstance(payload["complete"], bool):
        raise ShadowTraceArtifactError("SCHEMA_MISMATCH: trace_payload.complete")
    return ShadowAuditTrace(
        metric=metric,
        threshold_stratum=_string(payload["threshold_stratum"], name="trace_payload.threshold_stratum"),
        candidate_ef=_integer(payload["candidate_ef"], name="trace_payload.candidate_ef"),
        last_known_good_ef=_integer(payload["last_known_good_ef"], name="trace_payload.last_known_good_ef"),
        sentinel_ef=_integer(payload["sentinel_ef"], name="trace_payload.sentinel_ef"),
        configuration_identity=_string(payload["configuration_identity"], name="trace_payload.configuration_identity"),
        data_identity=_string(payload["data_identity"], name="trace_payload.data_identity"),
        flat_identity=_decode_identity_evidence(payload["flat_identity"], name="trace_payload.flat_identity"),
        hnsw_identity=_decode_identity_evidence(payload["hnsw_identity"], name="trace_payload.hnsw_identity"),
        queries=tuple(_decode_query(item, name=f"trace_payload.queries[{index}]") for index, item in enumerate(payload["queries"])),
        complete=payload["complete"],
        reason_codes=tuple(_string(item, name=f"trace_payload.reason_codes[{index}]", nonempty=False) for index, item in enumerate(payload["reason_codes"])),
    )


def _document_for(envelope: PersistedShadowTraceEnvelope) -> dict[str, object]:
    if not isinstance(envelope, PersistedShadowTraceEnvelope) or envelope.trace is None:
        raise ShadowTraceArtifactError("SCHEMA_MISMATCH: persisted envelope")
    payload = canonical_shadow_trace_payload(envelope.trace)
    actual = hash_shadow_audit_trace(envelope.trace)
    if envelope.expected_trace_sha256 != actual:
        raise ShadowTraceArtifactError("CHECKSUM_MISMATCH: envelope expected trace SHA-256")
    return {
        "schema_version": SCHEMA_VERSION,
        "trace_id": envelope.trace_id,
        "captured_at_utc": envelope.captured_at_utc,
        "sequence_index": envelope.sequence_index,
        "declared_observation_count": envelope.declared_observation_count,
        "expected_trace_sha256": envelope.expected_trace_sha256,
        "trace_payload": payload,
    }


def _decode_document(value: object) -> PersistedShadowTraceEnvelope:
    document = _mapping(value, fields=_ROOT_FIELDS, name="artifact")
    if document["schema_version"] != SCHEMA_VERSION:
        raise ShadowTraceArtifactError("SCHEMA_MISMATCH: artifact.schema_version")
    trace = _decode_trace(document["trace_payload"])
    try:
        payload_matches = canonical_shadow_trace_payload(trace) == document["trace_payload"]
    except ShadowWindowValidationError as exc:
        raise ShadowTraceArtifactError("MALFORMED: trace payload") from exc
    if not payload_matches:
        raise ShadowTraceArtifactError("SCHEMA_MISMATCH: non-canonical trace payload")
    expected = _string(document["expected_trace_sha256"], name="artifact.expected_trace_sha256")
    actual = hash_shadow_audit_trace(trace)
    if expected != actual:
        raise ShadowTraceArtifactError("CHECKSUM_MISMATCH: trace payload SHA-256")
    return PersistedShadowTraceEnvelope(
        trace_id=_string(document["trace_id"], name="artifact.trace_id"),
        captured_at_utc=_string(document["captured_at_utc"], name="artifact.captured_at_utc"),
        sequence_index=_integer(document["sequence_index"], name="artifact.sequence_index"),
        declared_observation_count=_integer(document["declared_observation_count"], name="artifact.declared_observation_count"),
        expected_trace_sha256=expected,
        trace=trace,
    )


def _canonical_document_bytes(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def encode_persisted_shadow_trace_envelope(
    envelope: PersistedShadowTraceEnvelope,
) -> bytes:
    """Return the strict canonical bytes for one persisted trace envelope.

    The durable SQLite shadow-attempt journal uses this same codec as the
    historical filesystem artifacts, avoiding a second trace serialization.
    """

    document = _document_for(envelope)
    rebuilt = _decode_document(document)
    encoded = _canonical_document_bytes(document)
    if _canonical_document_bytes(_document_for(rebuilt)) != encoded:
        raise ShadowTraceArtifactError("SCHEMA_MISMATCH: non-canonical envelope")
    return encoded


def decode_persisted_shadow_trace_envelope(
    source: bytes,
) -> PersistedShadowTraceEnvelope:
    """Strictly decode canonical envelope bytes supplied by a durable store."""

    if type(source) is not bytes:
        raise ShadowTraceArtifactError("SCHEMA_MISMATCH: envelope bytes")
    try:
        document = json.loads(
            source.decode("utf-8"),
            object_pairs_hook=_no_duplicate_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ShadowTraceArtifactError("MALFORMED: persisted trace artifact") from exc
    envelope = _decode_document(document)
    if _canonical_document_bytes(document) != source:
        raise ShadowTraceArtifactError("SCHEMA_MISMATCH: non-canonical envelope bytes")
    return envelope


def persist_shadow_trace_envelope(
    path: str | os.PathLike[str], envelope: PersistedShadowTraceEnvelope
) -> None:
    """Write one new immutable source envelope with file and directory fsyncs.

    The final file is created by an atomic same-directory hard-link operation;
    an existing artifact is never replaced.  A crash before publication leaves
    only an unlinked temporary file, while a crash after publication leaves a
    fully fsynced immutable document.
    """

    target = Path(path)
    if target.exists():
        raise FileExistsError(f"refusing to overwrite immutable trace artifact: {target}")
    if not target.parent.is_dir():
        raise FileNotFoundError(f"artifact parent directory does not exist: {target.parent}")
    encoded = encode_persisted_shadow_trace_envelope(envelope)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError:
            raise FileExistsError(f"refusing to overwrite immutable trace artifact: {target}") from None
        directory_descriptor = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def load_persisted_shadow_trace_envelope(
    path: str | os.PathLike[str],
) -> PersistedShadowTraceEnvelope:
    """Load one envelope after strict schema and canonical checksum checks.

    Historical filesystem artifacts did not require a particular JSON
    whitespace encoding.  Preserve that compatibility here while the byte
    decoder used by the durable attempt store remains deliberately stricter.
    """

    try:
        source = Path(path).read_bytes()
        document = json.loads(
            source.decode("utf-8"),
            object_pairs_hook=_no_duplicate_object,
            parse_constant=_reject_constant,
        )
    except FileNotFoundError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ShadowTraceArtifactError("MALFORMED: persisted trace artifact") from exc
    return _decode_document(document)


__all__ = [
    "SCHEMA_VERSION",
    "ShadowTraceArtifactError",
    "decode_persisted_shadow_trace_envelope",
    "encode_persisted_shadow_trace_envelope",
    "load_persisted_shadow_trace_envelope",
    "persist_shadow_trace_envelope",
]
