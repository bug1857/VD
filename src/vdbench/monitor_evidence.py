"""Strict restart-durable codec for complete ADR-002 ``WindowEvidence``.

The workload monitor persists this envelope after each accepted current window.
It validates a canonical payload hash and independently re-runs
``finalize_window_evidence`` on reload, so a state file cannot substitute a
different signal family or provenance for the evidence that was originally
accepted.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping

from .config import Metric
from .drift import (
    EvidenceProvenance,
    SeedMaterial,
    Signal,
    SignalEvidence,
    WindowEvidence,
    evidence_provenance_valid,
    finalize_window_evidence,
)

SCHEMA_VERSION = "monitor-window-evidence-v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ROOT_FIELDS = frozenset({"schema_version", "payload", "sha256"})
_PAYLOAD_FIELDS = frozenset({"metric", "window_id", "signals", "provenance"})
_SIGNAL_FIELDS = frozenset(
    {
        "signal",
        "complete",
        "reference_count",
        "current_count",
        "statistic",
        "effect",
        "effect_floor",
        "raw_p_value",
        "excluded_dimension_count",
        "excluded_dimension_indices",
        "adjusted_p_value",
        "seed",
        "reason",
    }
)
_SEED_FIELDS = frozenset({"seed_u64", "sha256"})
_PROVENANCE_FIELDS = frozenset(
    {
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
    }
)


class MonitorEvidenceCodecError(ValueError):
    """Raised when a persisted evidence envelope is malformed or untrusted."""


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _exact_mapping(value: object, fields: frozenset[str], *, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or frozenset(value) != fields:
        raise MonitorEvidenceCodecError(f"SCHEMA_MISMATCH:{name}")
    return value


def _integer(value: object, *, name: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MonitorEvidenceCodecError(f"TYPE_INVALID:{name}")
    if minimum is not None and value < minimum:
        raise MonitorEvidenceCodecError(f"RANGE_INVALID:{name}")
    return value


def _text(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise MonitorEvidenceCodecError(f"TYPE_INVALID:{name}")
    return value


def _sha256_text(value: object, *, name: str) -> str:
    text = _text(value, name=name)
    if _SHA256.fullmatch(text) is None:
        raise MonitorEvidenceCodecError(f"SHA256_INVALID:{name}")
    return text


def _canonical_id(value: object, *, name: str) -> int | str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise MonitorEvidenceCodecError(f"TYPE_INVALID:{name}")
    if isinstance(value, str) and not value:
        raise MonitorEvidenceCodecError(f"TYPE_INVALID:{name}")
    return value


def _finite(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MonitorEvidenceCodecError(f"TYPE_INVALID:{name}")
    result = float(value)
    if not math.isfinite(result):
        raise MonitorEvidenceCodecError(f"FINITE_REQUIRED:{name}")
    return result


def _optional_finite(value: object, *, name: str) -> float | None:
    return None if value is None else _finite(value, name=name)


def _seed_document(seed: SeedMaterial | None) -> object:
    if seed is None:
        return None
    return {"seed_u64": seed.seed_u64, "sha256": seed.sha256}


def _seed_from_document(value: object) -> SeedMaterial | None:
    if value is None:
        return None
    payload = _exact_mapping(value, _SEED_FIELDS, name="seed")
    seed = _integer(payload["seed_u64"], name="seed.seed_u64", minimum=0)
    digest = _sha256_text(payload["sha256"], name="seed.sha256")
    return SeedMaterial(seed_u64=seed, sha256=digest)


def _signal_document(signal: SignalEvidence) -> dict[str, object]:
    return {
        "signal": signal.signal.value,
        "complete": signal.complete,
        "reference_count": signal.reference_count,
        "current_count": signal.current_count,
        "statistic": signal.statistic,
        "effect": signal.effect,
        "effect_floor": signal.effect_floor,
        "raw_p_value": signal.raw_p_value,
        "excluded_dimension_count": signal.excluded_dimension_count,
        "excluded_dimension_indices": list(signal.excluded_dimension_indices),
        "adjusted_p_value": signal.adjusted_p_value,
        "seed": _seed_document(signal.seed),
        "reason": signal.reason,
    }


def _signal_from_document(value: object) -> SignalEvidence:
    payload = _exact_mapping(value, _SIGNAL_FIELDS, name="signal")
    try:
        signal = Signal(payload["signal"])
    except (TypeError, ValueError) as exc:
        raise MonitorEvidenceCodecError("SIGNAL_INVALID") from exc
    if payload["complete"] is not True:
        raise MonitorEvidenceCodecError("INCOMPLETE_EVIDENCE_PERSISTENCE_FORBIDDEN")
    indices = payload["excluded_dimension_indices"]
    if not isinstance(indices, list):
        raise MonitorEvidenceCodecError("TYPE_INVALID:excluded_dimension_indices")
    normalized_indices = tuple(
        _integer(index, name="excluded_dimension_index", minimum=0)
        for index in indices
    )
    if normalized_indices != tuple(sorted(set(normalized_indices))):
        raise MonitorEvidenceCodecError("EXCLUDED_DIMENSIONS_INVALID")
    count = _integer(
        payload["excluded_dimension_count"],
        name="excluded_dimension_count",
        minimum=0,
    )
    if count != len(normalized_indices):
        raise MonitorEvidenceCodecError("EXCLUDED_DIMENSION_COUNT_MISMATCH")
    reason = payload["reason"]
    if reason is not None and not isinstance(reason, str):
        raise MonitorEvidenceCodecError("TYPE_INVALID:reason")
    return SignalEvidence(
        signal=signal,
        complete=True,
        reference_count=_integer(payload["reference_count"], name="reference_count", minimum=0),
        current_count=_integer(payload["current_count"], name="current_count", minimum=0),
        statistic=_finite(payload["statistic"], name="statistic"),
        effect=_finite(payload["effect"], name="effect"),
        effect_floor=_finite(payload["effect_floor"], name="effect_floor"),
        raw_p_value=_finite(payload["raw_p_value"], name="raw_p_value"),
        excluded_dimension_count=count,
        excluded_dimension_indices=normalized_indices,
        adjusted_p_value=_optional_finite(payload["adjusted_p_value"], name="adjusted_p_value"),
        seed=_seed_from_document(payload["seed"]),
        reason=reason,
    )


def _provenance_document(provenance: EvidenceProvenance) -> dict[str, object]:
    return {
        "schema_version": provenance.schema_version,
        "metric": provenance.metric.value,
        "threshold_stratum": provenance.threshold_stratum,
        "reference_window_id": provenance.reference_window_id,
        "current_window_id": provenance.current_window_id,
        "reference_manifest_sha256": provenance.reference_manifest_sha256,
        "current_manifest_sha256": provenance.current_manifest_sha256,
        "configuration_identity": provenance.configuration_identity,
        "data_identity": provenance.data_identity,
        "flat_binding_id": provenance.flat_binding_id,
        "hnsw_binding_id": provenance.hnsw_binding_id,
        "reference_audit_ids": list(provenance.reference_audit_ids),
        "reference_audit_rank_digests": list(provenance.reference_audit_rank_digests),
        "current_audit_ids": list(provenance.current_audit_ids),
        "current_audit_rank_digests": list(provenance.current_audit_rank_digests),
        "sha256": provenance.sha256,
    }


def _provenance_from_document(value: object) -> EvidenceProvenance:
    payload = _exact_mapping(value, _PROVENANCE_FIELDS, name="provenance")
    for field in (
        "reference_audit_ids",
        "reference_audit_rank_digests",
        "current_audit_ids",
        "current_audit_rank_digests",
    ):
        if not isinstance(payload[field], list):
            raise MonitorEvidenceCodecError(f"TYPE_INVALID:{field}")
    try:
        provenance = EvidenceProvenance(
            schema_version=_text(payload["schema_version"], name="provenance.schema_version"),
            metric=Metric(payload["metric"]),
            threshold_stratum=_text(payload["threshold_stratum"], name="threshold_stratum"),
            reference_window_id=_canonical_id(payload["reference_window_id"], name="reference_window_id"),
            current_window_id=_canonical_id(payload["current_window_id"], name="current_window_id"),
            reference_manifest_sha256=_sha256_text(
                payload["reference_manifest_sha256"], name="reference_manifest_sha256"
            ),
            current_manifest_sha256=_sha256_text(
                payload["current_manifest_sha256"], name="current_manifest_sha256"
            ),
            configuration_identity=_text(payload["configuration_identity"], name="configuration_identity"),
            data_identity=_text(payload["data_identity"], name="data_identity"),
            flat_binding_id=_text(payload["flat_binding_id"], name="flat_binding_id"),
            hnsw_binding_id=_text(payload["hnsw_binding_id"], name="hnsw_binding_id"),
            reference_audit_ids=tuple(
                _canonical_id(item, name="reference_audit_id")
                for item in payload["reference_audit_ids"]
            ),
            reference_audit_rank_digests=tuple(
                _sha256_text(item, name="reference_audit_rank_digest")
                for item in payload["reference_audit_rank_digests"]
            ),
            current_audit_ids=tuple(
                _canonical_id(item, name="current_audit_id")
                for item in payload["current_audit_ids"]
            ),
            current_audit_rank_digests=tuple(
                _sha256_text(item, name="current_audit_rank_digest")
                for item in payload["current_audit_rank_digests"]
            ),
            sha256=_sha256_text(payload["sha256"], name="provenance.sha256"),
        )
    except (TypeError, ValueError) as exc:
        raise MonitorEvidenceCodecError("PROVENANCE_INVALID") from exc
    if not evidence_provenance_valid(provenance):
        raise MonitorEvidenceCodecError("PROVENANCE_SHA256_MISMATCH")
    return provenance


def _payload(evidence: WindowEvidence) -> dict[str, object]:
    if not isinstance(evidence, WindowEvidence) or not evidence.complete:
        raise MonitorEvidenceCodecError("INCOMPLETE_EVIDENCE_PERSISTENCE_FORBIDDEN")
    if evidence.provenance is None or not evidence_provenance_valid(evidence.provenance):
        raise MonitorEvidenceCodecError("PROVENANCE_INVALID")
    return {
        "metric": evidence.metric.value,
        "window_id": evidence.window_id,
        "signals": [_signal_document(signal) for signal in evidence.signals],
        "provenance": _provenance_document(evidence.provenance),
    }


def encode_persisted_window_evidence(evidence: WindowEvidence) -> dict[str, object]:
    """Return canonical, versioned, SHA-256-bound complete evidence."""

    payload = _payload(evidence)
    # Decode before emission so direct construction of an invalid dataclass cannot
    # become durable state merely because it is JSON serializable.
    restored = _window_from_payload(payload)
    if restored != evidence:
        raise MonitorEvidenceCodecError("EVIDENCE_CANONICALIZATION_MISMATCH")
    return {
        "schema_version": SCHEMA_VERSION,
        "payload": payload,
        "sha256": _sha256(payload),
    }


def _window_from_payload(value: object) -> WindowEvidence:
    payload = _exact_mapping(value, _PAYLOAD_FIELDS, name="payload")
    try:
        metric = Metric(payload["metric"])
    except (TypeError, ValueError) as exc:
        raise MonitorEvidenceCodecError("METRIC_INVALID") from exc
    window_id = _canonical_id(payload["window_id"], name="window_id")
    raw_signals = payload["signals"]
    if not isinstance(raw_signals, list) or len(raw_signals) != len(Signal):
        raise MonitorEvidenceCodecError("SIGNAL_FAMILY_INVALID")
    signals = tuple(_signal_from_document(item) for item in raw_signals)
    if tuple(signal.signal for signal in signals) != tuple(Signal):
        raise MonitorEvidenceCodecError("SIGNAL_ORDER_INVALID")
    provenance = _provenance_from_document(payload["provenance"])
    if provenance.metric is not metric or provenance.current_window_id != window_id:
        raise MonitorEvidenceCodecError("PROVENANCE_WINDOW_MISMATCH")
    reconstructed = finalize_window_evidence(
        metric=metric,
        window_id=window_id,
        signals=signals,
        provenance=provenance,
    )
    if not reconstructed.complete:
        raise MonitorEvidenceCodecError("EVIDENCE_FINALIZATION_FAILED")
    if reconstructed.signals != signals:
        raise MonitorEvidenceCodecError("EVIDENCE_FINALIZATION_MISMATCH")
    return reconstructed


def decode_persisted_window_evidence(value: object) -> WindowEvidence:
    """Restore one evidence snapshot or raise a fail-closed codec error."""

    document = _exact_mapping(value, _ROOT_FIELDS, name="root")
    if document["schema_version"] != SCHEMA_VERSION:
        raise MonitorEvidenceCodecError("SCHEMA_VERSION_INVALID")
    digest = _sha256_text(document["sha256"], name="sha256")
    if digest != _sha256(document["payload"]):
        raise MonitorEvidenceCodecError("SHA256_MISMATCH")
    return _window_from_payload(document["payload"])


__all__ = [
    "SCHEMA_VERSION",
    "MonitorEvidenceCodecError",
    "decode_persisted_window_evidence",
    "encode_persisted_window_evidence",
]
