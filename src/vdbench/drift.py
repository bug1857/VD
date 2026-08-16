"""Offline statistical core for ADR-002 workload-drift detection.

Purpose:
    Compute deterministic drift statistics and evidence without database access.
Inputs:
    Metric-stratified query vectors, thresholds, exact cardinalities, audited
    sentinel recall, and immutable detector/window identifiers.
Outputs:
    Per-signal permutation evidence, Holm-corrected windows, deterministic audit
    selections, and three-state drift decisions.
Dependencies:
    Python standard library and NumPy only. This module never imports PyMilvus.
Complexity:
    MMD kernel construction is O((n+m)^2) memory/time; its fixed-kernel
    permutation evaluation is O(P(n+m)^2), evaluated in deterministic batches.
    KS and recall permutation evaluation are O(P(n+m)).
Failure modes:
    Invalid, incomplete, non-finite, or degenerate contract inputs produce
    incomplete signal/window evidence; no numerical fallback is fabricated.
Configuration:
    ADR-002 fixes 9,999 permutations, alpha=0.01, sample counts, and effect gates.
Extension points:
    Future detectors may consume these evidence objects but must not weaken the
    ADR-002 completeness or deterministic-replay contract.
"""

from __future__ import annotations

import hashlib
import json
import re
import struct
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from numbers import Integral

import numpy as np

from .config import Metric

PERMUTATION_COUNT = 9_999
PERMUTATION_DENOMINATOR = 10_000
PERMUTATION_BATCH_SIZE = 128
FAMILY_WISE_ALPHA = 0.01
ELIGIBLE_QUERY_COUNT = 200
AUDIT_QUERY_COUNT = 50
RESULT_LIMIT = 100
SENTINEL_EF = 100
EVIDENCE_PROVENANCE_SCHEMA_VERSION = "evidence-provenance-v1"
_SHA256_HEX = re.compile(r"[0-9a-f]{64}\Z")


class Signal(StrEnum):
    """The four ADR-002 statistical signals."""

    QUERY_VECTOR = "QUERY_VECTOR"
    THRESHOLD = "THRESHOLD"
    CARDINALITY = "CARDINALITY"
    RECALL = "RECALL"


class DetectorState(StrEnum):
    """Fail-closed ADR-002 detector states."""

    NO_DRIFT = "NO_DRIFT"
    DRIFT = "DRIFT"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class DriftClassification(StrEnum):
    """Attribution class emitted only for a DRIFT decision."""

    NONE = "NONE"
    INPUT_DRIFT = "INPUT_DRIFT"
    QUALITY_DRIFT = "QUALITY_DRIFT"
    INPUT_AND_QUALITY_DRIFT = "INPUT_AND_QUALITY_DRIFT"


class IncompleteEvidenceError(ValueError):
    """Raised internally when a statistic cannot be computed under ADR-002."""


class _IncompleteMMDEvidenceError(IncompleteEvidenceError):
    """MMD failure retaining pooled zero-variance exclusion evidence."""

    def __init__(
        self,
        message: str,
        *,
        excluded_dimension_indices: tuple[int, ...],
    ) -> None:
        super().__init__(message)
        self.excluded_dimension_indices = excluded_dimension_indices


CanonicalValue = int | str
BatchStatistic = Callable[[np.ndarray], np.ndarray]


@dataclass(frozen=True, slots=True)
class SeedMaterial:
    """Persistable SHA-256 seed derivation evidence."""

    seed_u64: int
    sha256: str


@dataclass(frozen=True, slots=True)
class PermutationEvidence:
    """One deterministic Monte Carlo p-value and its seed provenance."""

    p_value: float
    exceedance_count: int
    permutation_count: int
    seed: SeedMaterial


@dataclass(frozen=True, slots=True)
class MMDResult:
    """Hand-checkable MMD² statistic and pooled kernel bandwidth."""

    statistic: float
    sigma: float


@dataclass(frozen=True, slots=True)
class AuditSelection:
    """Deterministic audit selection or a fail-closed completeness result."""

    complete: bool
    query_ids: tuple[CanonicalValue, ...] = ()
    digest_hex: tuple[str, ...] = ()
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class EvidenceProvenance:
    """Immutable binding from persisted shadow windows to detector evidence."""

    schema_version: str
    metric: Metric
    threshold_stratum: str
    reference_window_id: CanonicalValue
    current_window_id: CanonicalValue
    reference_manifest_sha256: str
    current_manifest_sha256: str
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str
    reference_audit_ids: tuple[CanonicalValue, ...]
    reference_audit_rank_digests: tuple[str, ...]
    current_audit_ids: tuple[CanonicalValue, ...]
    current_audit_rank_digests: tuple[str, ...]
    sha256: str


def _provenance_identifier(value: object, *, field: str) -> CanonicalValue:
    if isinstance(value, bool):
        raise ValueError(f"{field} cannot be Boolean")  # domain error type carries the governed reason code  # noqa: TRY004
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, str) and value:
        return unicodedata.normalize("NFC", value)
    raise ValueError(f"{field} must be a non-empty integer or string")


def _provenance_sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_HEX.fullmatch(value) is None:
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _provenance_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return unicodedata.normalize("NFC", value)


def _provenance_payload(
    *,
    metric: Metric,
    threshold_stratum: str,
    reference_window_id: CanonicalValue,
    current_window_id: CanonicalValue,
    reference_manifest_sha256: str,
    current_manifest_sha256: str,
    configuration_identity: str,
    data_identity: str,
    flat_binding_id: str,
    hnsw_binding_id: str,
    reference_audit_ids: tuple[CanonicalValue, ...],
    reference_audit_rank_digests: tuple[str, ...],
    current_audit_ids: tuple[CanonicalValue, ...],
    current_audit_rank_digests: tuple[str, ...],
) -> dict[str, object]:
    return {
        "schema_version": EVIDENCE_PROVENANCE_SCHEMA_VERSION,
        "metric": metric.value,
        "threshold_stratum": threshold_stratum,
        "reference_window_id": reference_window_id,
        "current_window_id": current_window_id,
        "reference_manifest_sha256": reference_manifest_sha256,
        "current_manifest_sha256": current_manifest_sha256,
        "configuration_identity": configuration_identity,
        "data_identity": data_identity,
        "flat_binding_id": flat_binding_id,
        "hnsw_binding_id": hnsw_binding_id,
        "reference_audit_ids": list(reference_audit_ids),
        "reference_audit_rank_digests": list(reference_audit_rank_digests),
        "current_audit_ids": list(current_audit_ids),
        "current_audit_rank_digests": list(current_audit_rank_digests),
    }


def build_evidence_provenance(
    *,
    metric: Metric | str,
    threshold_stratum: str,
    reference_window_id: CanonicalValue,
    current_window_id: CanonicalValue,
    reference_manifest_sha256: str,
    current_manifest_sha256: str,
    configuration_identity: str,
    data_identity: str,
    flat_binding_id: str,
    hnsw_binding_id: str,
    reference_audit_ids: Sequence[CanonicalValue],
    reference_audit_rank_digests: Sequence[str],
    current_audit_ids: Sequence[CanonicalValue],
    current_audit_rank_digests: Sequence[str],
) -> EvidenceProvenance:
    """Build canonical provenance; no caller-supplied digest is accepted."""

    normalized_metric = _coerce_metric(metric)
    stratum = _provenance_text(threshold_stratum, field="threshold_stratum")
    ref_id = _provenance_identifier(reference_window_id, field="reference_window_id")
    current_id = _provenance_identifier(current_window_id, field="current_window_id")
    if ref_id == current_id:
        raise ValueError("reference and current window IDs must differ")
    configuration = _provenance_text(
        configuration_identity, field="configuration_identity"
    )
    data = _provenance_text(data_identity, field="data_identity")
    flat_binding = _provenance_text(flat_binding_id, field="flat_binding_id")
    hnsw_binding = _provenance_text(hnsw_binding_id, field="hnsw_binding_id")
    audit_pairs = (
        (reference_audit_ids, reference_audit_rank_digests, "reference"),
        (current_audit_ids, current_audit_rank_digests, "current"),
    )
    normalized_audits: list[tuple[tuple[CanonicalValue, ...], tuple[str, ...]]] = []
    for ids, digests, label in audit_pairs:
        normalized_ids = tuple(
            _provenance_identifier(value, field=f"{label}_audit_id") for value in ids
        )
        normalized_digests = tuple(
            _provenance_sha256(value, field=f"{label}_audit_rank_digest")
            for value in digests
        )
        if len(normalized_ids) != AUDIT_QUERY_COUNT or len(normalized_digests) != AUDIT_QUERY_COUNT:
            raise ValueError("provenance requires exactly 50 audit IDs and digests")
        if len({canonical_serialize_tuple((value,)) for value in normalized_ids}) != AUDIT_QUERY_COUNT:
            raise ValueError("provenance audit IDs must be canonical-unique")
        normalized_audits.append((normalized_ids, normalized_digests))
    payload = _provenance_payload(
        metric=normalized_metric, threshold_stratum=stratum, reference_window_id=ref_id,
        current_window_id=current_id,
        reference_manifest_sha256=_provenance_sha256(reference_manifest_sha256, field="reference_manifest_sha256"),
        current_manifest_sha256=_provenance_sha256(current_manifest_sha256, field="current_manifest_sha256"),
        configuration_identity=configuration, data_identity=data,
        flat_binding_id=flat_binding, hnsw_binding_id=hnsw_binding,
        reference_audit_ids=normalized_audits[0][0], reference_audit_rank_digests=normalized_audits[0][1],
        current_audit_ids=normalized_audits[1][0], current_audit_rank_digests=normalized_audits[1][1],
    )
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()
    return EvidenceProvenance(
        schema_version=EVIDENCE_PROVENANCE_SCHEMA_VERSION,
        metric=normalized_metric,
        threshold_stratum=stratum,
        reference_window_id=ref_id,
        current_window_id=current_id,
        reference_manifest_sha256=payload["reference_manifest_sha256"],  # type: ignore[arg-type]
        current_manifest_sha256=payload["current_manifest_sha256"],  # type: ignore[arg-type]
        configuration_identity=configuration,
        data_identity=data,
        flat_binding_id=flat_binding,
        hnsw_binding_id=hnsw_binding,
        reference_audit_ids=normalized_audits[0][0],
        reference_audit_rank_digests=normalized_audits[0][1],
        current_audit_ids=normalized_audits[1][0],
        current_audit_rank_digests=normalized_audits[1][1],
        sha256=digest,
    )


def evidence_provenance_valid(provenance: object) -> bool:
    if not isinstance(provenance, EvidenceProvenance):
        return False
    try:
        rebuilt = build_evidence_provenance(
            metric=provenance.metric, threshold_stratum=provenance.threshold_stratum,
            reference_window_id=provenance.reference_window_id, current_window_id=provenance.current_window_id,
            reference_manifest_sha256=provenance.reference_manifest_sha256, current_manifest_sha256=provenance.current_manifest_sha256,
            configuration_identity=provenance.configuration_identity, data_identity=provenance.data_identity,
            flat_binding_id=provenance.flat_binding_id, hnsw_binding_id=provenance.hnsw_binding_id,
            reference_audit_ids=provenance.reference_audit_ids, reference_audit_rank_digests=provenance.reference_audit_rank_digests,
            current_audit_ids=provenance.current_audit_ids, current_audit_rank_digests=provenance.current_audit_rank_digests,
        )
    except (TypeError, ValueError):
        return False
    return provenance.schema_version == EVIDENCE_PROVENANCE_SCHEMA_VERSION and provenance.sha256 == rebuilt.sha256


@dataclass(frozen=True, slots=True)
class RecallAuditSample:
    """One window's exact `(50,)` sentinel-recall audit contract."""

    window_id: CanonicalValue
    metric: Metric | str
    expected_audit_ids: Sequence[CanonicalValue]
    observed_audit_ids: Sequence[CanonicalValue]
    values: Sequence[float] | np.ndarray
    flat_oracle_agreement: Sequence[bool] | np.ndarray
    collection_data_identity: str
    index_build_identity: str
    sentinel_ef: int = SENTINEL_EF
    limit: int = RESULT_LIMIT
    failed_query_count: int = 0
    timeout_query_count: int = 0
    threshold_violation_count: int = 0


@dataclass(frozen=True, slots=True)
class SignalEvidence:
    """Raw and family-adjusted evidence for one signal/window comparison."""

    signal: Signal
    complete: bool
    reference_count: int
    current_count: int
    statistic: float | None
    effect: float | None
    effect_floor: float
    raw_p_value: float | None
    excluded_dimension_count: int = 0
    excluded_dimension_indices: tuple[int, ...] = ()
    adjusted_p_value: float | None = None
    seed: SeedMaterial | None = None
    reason: str | None = None

    @property
    def breach(self) -> bool:
        """Return whether corrected significance and effect gates both pass."""

        return bool(
            self.complete
            and self.adjusted_p_value is not None
            and self.adjusted_p_value <= FAMILY_WISE_ALPHA
            and self.effect is not None
            and self.effect >= self.effect_floor
        )

    @property
    def gate_ratio(self) -> float | None:
        """Return the ADR-002 effect-to-floor ratio when defined."""

        if self.effect is None or self.effect_floor <= 0.0:
            return None
        return self.effect / self.effect_floor


@dataclass(frozen=True, slots=True)
class WindowEvidence:
    """One complete or fail-closed four-signal Holm family."""

    metric: Metric
    window_id: CanonicalValue
    signals: tuple[SignalEvidence, ...]
    complete: bool
    reason_codes: tuple[str, ...] = ()
    provenance: EvidenceProvenance | None = None

    def by_signal(self) -> dict[Signal, SignalEvidence]:
        """Return signal evidence keyed by the four canonical signal names."""

        return {item.signal: item for item in self.signals}


@dataclass(frozen=True, slots=True)
class DriftDecision:
    """ADR-002 three-state decision with optional classification evidence."""

    state: DetectorState
    classification: DriftClassification
    triggering_signals: tuple[Signal, ...] = ()
    significance_evidence_score: float | None = None
    drift_magnitude: float | None = None
    reason_codes: tuple[str, ...] = ()
    evidence_provenance: EvidenceProvenance | None = None


@dataclass(frozen=True, slots=True)
class _PreparedMMD:
    statistic: float
    sigma: float
    kernel: np.ndarray
    reference_count: int
    current_count: int
    excluded_dimension_count: int
    excluded_dimension_indices: tuple[int, ...]


_SIGNAL_ORDER = (
    Signal.QUERY_VECTOR,
    Signal.THRESHOLD,
    Signal.CARDINALITY,
    Signal.RECALL,
)
_INPUT_SIGNALS = frozenset(
    {Signal.QUERY_VECTOR, Signal.THRESHOLD, Signal.CARDINALITY}
)
_EFFECT_FLOORS = {
    Signal.QUERY_VECTOR: 0.01,
    Signal.THRESHOLD: 0.20,
    Signal.CARDINALITY: 0.20,
    Signal.RECALL: 0.02,
}
_EXPECTED_COUNTS = {
    Signal.QUERY_VECTOR: ELIGIBLE_QUERY_COUNT,
    Signal.THRESHOLD: ELIGIBLE_QUERY_COUNT,
    Signal.CARDINALITY: AUDIT_QUERY_COUNT,
    Signal.RECALL: AUDIT_QUERY_COUNT,
}


def _canonical_field_bytes(value: CanonicalValue) -> bytes:
    if isinstance(value, bool):
        raise ValueError("booleans are not canonical integer fields")  # domain error type carries the governed reason code  # noqa: TRY004
    if isinstance(value, Integral):
        text = str(int(value))
    elif isinstance(value, str):
        text = unicodedata.normalize("NFC", value)
    else:
        raise TypeError(f"unsupported canonical field type: {type(value).__name__}")
    return text.encode("utf-8")


def canonical_serialize_tuple(values: Sequence[CanonicalValue]) -> bytes:
    """Serialize a fixed-schema tuple using ADR-002's canonical framing."""

    if len(values) > 0xFFFFFFFF:
        raise ValueError("tuple has too many fields for uint32 framing")
    output = bytearray(struct.pack(">I", len(values)))
    for value in values:
        encoded = _canonical_field_bytes(value)
        if len(encoded) > 0xFFFFFFFFFFFFFFFF:
            raise ValueError("field is too large for uint64 framing")
        output.extend(struct.pack(">Q", len(encoded)))
        output.extend(encoded)
    return bytes(output)


def _coerce_metric(metric: Metric | str) -> Metric:
    try:
        return Metric(metric)
    except (TypeError, ValueError) as exc:
        raise ValueError("metric must be exactly L2 or COSINE") from exc


def _coerce_signal(signal: Signal | str) -> Signal:
    try:
        return Signal(signal)
    except (TypeError, ValueError) as exc:
        raise ValueError("signal must use its exact ADR-002 uppercase name") from exc


def derive_permutation_seed(
    detector_seed: int,
    metric: Metric | str,
    window_id: CanonicalValue,
    signal: Signal | str,
) -> SeedMaterial:
    """Derive the exact unsigned 64-bit PCG64 seed required by ADR-002."""

    normalized_metric = _coerce_metric(metric)
    normalized_signal = _coerce_signal(signal)
    payload = canonical_serialize_tuple(
        (detector_seed, normalized_metric.value, window_id, normalized_signal.value)
    )
    digest = hashlib.sha256(payload).digest()
    return SeedMaterial(
        seed_u64=int.from_bytes(digest[:8], byteorder="big", signed=False),
        sha256=digest.hex(),
    )


def deterministic_permutation_p_value(
    *,
    observed_statistic: float,
    total_count: int,
    reference_count: int,
    detector_seed: int,
    metric: Metric | str,
    window_id: CanonicalValue,
    signal: Signal | str,
    batch_statistic: BatchStatistic,
) -> PermutationEvidence:
    """Generate exactly 9,999 label permutations and compute a p-value.

    ``batch_statistic`` receives a boolean matrix whose rows mark permuted
    reference membership. It must return one statistic per row. Batching does
    not alter the sequence of PCG64 permutation calls.
    """

    if not np.isfinite(observed_statistic):
        raise IncompleteEvidenceError("observed statistic must be finite")
    if not 0 < reference_count < total_count:
        raise ValueError("reference_count must be within total_count")
    seed = derive_permutation_seed(detector_seed, metric, window_id, signal)
    rng = np.random.Generator(np.random.PCG64(seed.seed_u64))
    exceedances = 0
    generated = 0
    while generated < PERMUTATION_COUNT:
        size = min(PERMUTATION_BATCH_SIZE, PERMUTATION_COUNT - generated)
        membership = np.zeros((size, total_count), dtype=bool)
        for row in range(size):
            indices = rng.permutation(total_count)
            membership[row, indices[:reference_count]] = True
        statistics = np.asarray(batch_statistic(membership), dtype=np.float64)
        if statistics.shape != (size,) or not np.all(np.isfinite(statistics)):
            raise IncompleteEvidenceError(
                "permutation statistic must return one finite value per row"
            )
        exceedances += int(np.count_nonzero(statistics >= observed_statistic))
        generated += size

    return PermutationEvidence(
        p_value=(1.0 + exceedances) / PERMUTATION_DENOMINATOR,
        exceedance_count=exceedances,
        permutation_count=PERMUTATION_COUNT,
        seed=seed,
    )


def _query_id_schema(value: CanonicalValue) -> str:
    if isinstance(value, bool):
        raise ValueError("boolean query IDs are invalid")  # domain error type carries the governed reason code  # noqa: TRY004
    if isinstance(value, Integral):
        return "integer"
    if isinstance(value, str):
        return "text"
    raise TypeError(f"unsupported query ID type: {type(value).__name__}")


def _canonical_unique_ids(
    query_ids: Sequence[CanonicalValue], *, expected_count: int
) -> tuple[tuple[CanonicalValue, bytes], ...]:
    if len(query_ids) != expected_count:
        raise IncompleteEvidenceError(
            f"expected exactly {expected_count} query IDs, got {len(query_ids)}"
        )
    schemas = {_query_id_schema(value) for value in query_ids}
    if len(schemas) != 1:
        raise IncompleteEvidenceError("query IDs must use one fixed schema type")
    encoded = tuple(
        (value, canonical_serialize_tuple((value,))) for value in query_ids
    )
    if len({value for _, value in encoded}) != expected_count:
        raise IncompleteEvidenceError(
            "query IDs must be unique after canonical normalization"
        )
    return encoded


def select_audit_sample(
    query_ids: Sequence[CanonicalValue],
    *,
    detector_seed: int,
    metric: Metric | str,
    window_id: CanonicalValue,
) -> AuditSelection:
    """Select the lowest 50 keyed-BLAKE2b ranks from exactly 200 IDs."""

    try:
        normalized_metric = _coerce_metric(metric)
        encoded_ids = _canonical_unique_ids(
            query_ids, expected_count=ELIGIBLE_QUERY_COUNT
        )
        key_payload = canonical_serialize_tuple((detector_seed,))
        key = hashlib.sha256(key_payload).digest()
        ranked: list[tuple[bytes, bytes, CanonicalValue]] = []
        for query_id, encoded_id in encoded_ids:
            message = canonical_serialize_tuple(
                (normalized_metric.value, window_id, query_id)
            )
            digest = hashlib.blake2b(message, key=key, digest_size=32).digest()
            ranked.append((digest, encoded_id, query_id))
        ranked.sort(key=lambda item: (item[0], item[1]))
        selected = ranked[:AUDIT_QUERY_COUNT]
        return AuditSelection(
            complete=True,
            query_ids=tuple(item[2] for item in selected),
            digest_hex=tuple(item[0].hex() for item in selected),
        )
    except (IncompleteEvidenceError, TypeError, ValueError) as exc:
        return AuditSelection(complete=False, reason=str(exc))


def _as_float64_1d(values: Sequence[float] | np.ndarray, *, name: str) -> np.ndarray:
    try:
        result = np.asarray(values, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise IncompleteEvidenceError(f"{name} cannot be converted to float64") from exc
    if result.ndim != 1 or result.size == 0:
        raise IncompleteEvidenceError(f"{name} must be a non-empty 1D array")
    if not np.all(np.isfinite(result)):
        raise IncompleteEvidenceError(f"{name} must contain only finite values")
    return result


def _as_float64_vectors(
    values: Sequence[Sequence[float]] | np.ndarray, *, name: str
) -> np.ndarray:
    try:
        result = np.asarray(values, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise IncompleteEvidenceError(f"{name} cannot be converted to float64") from exc
    if result.ndim != 2 or result.shape[0] < 2 or result.shape[1] < 1:
        raise IncompleteEvidenceError(
            f"{name} must have shape (n, dimensions) with n >= 2"
        )
    if not np.all(np.isfinite(result)):
        raise IncompleteEvidenceError(f"{name} must contain only finite values")
    return result


def _first_dimension_count(values: object) -> int:
    """Return a diagnostic first-axis count without raising on malformed input."""

    try:
        shape = np.asarray(values).shape
    except (TypeError, ValueError):
        return 0
    return int(shape[0]) if shape else 0


def _pairwise_squared_distances(values: np.ndarray) -> np.ndarray:
    squared_norms = np.einsum("ij,ij->i", values, values)
    distances = (
        squared_norms[:, None]
        + squared_norms[None, :]
        - 2.0 * (values @ values.T)
    )
    return np.maximum(distances, 0.0)


def _mmd_from_kernel(kernel: np.ndarray, reference_count: int) -> float:
    current_count = kernel.shape[0] - reference_count
    reference = kernel[:reference_count, :reference_count]
    current = kernel[reference_count:, reference_count:]
    cross = kernel[:reference_count, reference_count:]
    reference_term = (
        float(reference.sum()) - float(np.trace(reference))
    ) / (reference_count * (reference_count - 1))
    current_term = (
        float(current.sum()) - float(np.trace(current))
    ) / (current_count * (current_count - 1))
    cross_term = float(cross.sum()) / (reference_count * current_count)
    return reference_term + current_term - 2.0 * cross_term


def _prepare_mmd(
    reference: Sequence[Sequence[float]] | np.ndarray,
    current: Sequence[Sequence[float]] | np.ndarray,
    *,
    metric: Metric | str,
) -> _PreparedMMD:
    normalized_metric = _coerce_metric(metric)
    reference_values = _as_float64_vectors(reference, name="reference vectors")
    current_values = _as_float64_vectors(current, name="current vectors")
    if reference_values.shape[1] != current_values.shape[1]:
        raise IncompleteEvidenceError("reference/current dimensions must match")

    pooled_values = np.vstack((reference_values, current_values))
    if normalized_metric is Metric.L2:
        mean = np.mean(pooled_values, axis=0, dtype=np.float64)
        deviation = np.std(pooled_values, axis=0, ddof=0, dtype=np.float64)
        variable = deviation > 0.0
        excluded_dimension_indices = tuple(
            int(index) for index in np.flatnonzero(~variable)
        )
        transformed_pooled = (
            pooled_values[:, variable] - mean[variable]
        ) / deviation[variable]
        transformed_reference = transformed_pooled[: reference_values.shape[0]]
        transformed_current = transformed_pooled[reference_values.shape[0] :]
    else:
        excluded_dimension_indices = ()
        reference_norms = np.linalg.norm(reference_values, axis=1)
        current_norms = np.linalg.norm(current_values, axis=1)
        if np.any(reference_norms == 0.0) or np.any(current_norms == 0.0):
            raise IncompleteEvidenceError("COSINE vectors must have non-zero norm")
        if not np.all(np.isfinite(reference_norms)) or not np.all(
            np.isfinite(current_norms)
        ):
            raise IncompleteEvidenceError("COSINE vector norms must be finite")
        transformed_reference = reference_values / reference_norms[:, None]
        transformed_current = current_values / current_norms[:, None]

    combined = np.vstack((transformed_reference, transformed_current))
    pooled_squared = _pairwise_squared_distances(combined)
    upper = np.sqrt(
        pooled_squared[np.triu_indices(combined.shape[0], k=1)]
    )
    finite = upper[np.isfinite(upper)]
    if finite.size == 0 or not np.any(finite > 0.0):
        raise _IncompleteMMDEvidenceError(
            "median-heuristic sigma is undefined",
            excluded_dimension_indices=excluded_dimension_indices,
        )
    sigma = float(np.median(finite))
    if not np.isfinite(sigma) or sigma <= 0.0:
        raise _IncompleteMMDEvidenceError(
            "median-heuristic sigma must be positive",
            excluded_dimension_indices=excluded_dimension_indices,
        )

    kernel = np.exp(-pooled_squared / (2.0 * sigma * sigma)).astype(np.float64)
    statistic = _mmd_from_kernel(kernel, transformed_reference.shape[0])
    if not np.isfinite(statistic):
        raise IncompleteEvidenceError("MMD squared statistic is non-finite")
    return _PreparedMMD(
        statistic=statistic,
        sigma=sigma,
        kernel=kernel,
        reference_count=transformed_reference.shape[0],
        current_count=transformed_current.shape[0],
        excluded_dimension_count=len(excluded_dimension_indices),
        excluded_dimension_indices=excluded_dimension_indices,
    )


def mmd_squared(
    reference: Sequence[Sequence[float]] | np.ndarray,
    current: Sequence[Sequence[float]] | np.ndarray,
    *,
    metric: Metric | str,
) -> MMDResult:
    """Return unbiased Gaussian-kernel MMD² with pooled preprocessing."""

    prepared = _prepare_mmd(reference, current, metric=metric)
    return MMDResult(statistic=prepared.statistic, sigma=prepared.sigma)


def _mmd_batch_statistic(prepared: _PreparedMMD) -> BatchStatistic:
    kernel = prepared.kernel
    reference_count = prepared.reference_count
    current_count = prepared.current_count
    diagonal = np.diag(kernel)
    diagonal_total = float(diagonal.sum())
    row_sums = kernel.sum(axis=1)
    total_sum = float(kernel.sum())

    def statistic(membership: np.ndarray) -> np.ndarray:
        reference_membership = membership.astype(np.float64, copy=False)
        kernel_reference = reference_membership @ kernel
        reference_including_diagonal = np.einsum(
            "bi,bi->b", kernel_reference, reference_membership
        )
        reference_diagonal = reference_membership @ diagonal
        reference_term = (
            reference_including_diagonal - reference_diagonal
        ) / (reference_count * (reference_count - 1))

        reference_to_all = reference_membership @ row_sums
        cross_sum = reference_to_all - reference_including_diagonal
        current_including_diagonal = (
            total_sum - reference_including_diagonal - 2.0 * cross_sum
        )
        current_diagonal = diagonal_total - reference_diagonal
        current_term = (
            current_including_diagonal - current_diagonal
        ) / (current_count * (current_count - 1))
        return reference_term + current_term - 2.0 * cross_sum / (
            reference_count * current_count
        )

    return statistic


def query_vector_signal_test(
    reference: Sequence[Sequence[float]] | np.ndarray,
    current: Sequence[Sequence[float]] | np.ndarray,
    *,
    metric: Metric | str,
    detector_seed: int,
    window_id: CanonicalValue,
) -> SignalEvidence:
    """Compute MMD² and deterministic permutation evidence, or fail closed."""

    reference_count = _first_dimension_count(reference)
    current_count = _first_dimension_count(current)
    excluded_dimension_indices: tuple[int, ...] = ()
    try:
        prepared = _prepare_mmd(reference, current, metric=metric)
        excluded_dimension_indices = prepared.excluded_dimension_indices
        batch_statistic = _mmd_batch_statistic(prepared)
        true_membership = np.zeros(
            (1, prepared.reference_count + prepared.current_count), dtype=bool
        )
        true_membership[0, : prepared.reference_count] = True
        permutation_observed_statistic = float(
            batch_statistic(true_membership)[0]
        )
        permutation = deterministic_permutation_p_value(
            observed_statistic=permutation_observed_statistic,
            total_count=prepared.reference_count + prepared.current_count,
            reference_count=prepared.reference_count,
            detector_seed=detector_seed,
            metric=metric,
            window_id=window_id,
            signal=Signal.QUERY_VECTOR,
            batch_statistic=batch_statistic,
        )
        return SignalEvidence(
            signal=Signal.QUERY_VECTOR,
            complete=True,
            reference_count=prepared.reference_count,
            current_count=prepared.current_count,
            statistic=prepared.statistic,
            effect=prepared.statistic,
            effect_floor=_EFFECT_FLOORS[Signal.QUERY_VECTOR],
            raw_p_value=permutation.p_value,
            excluded_dimension_count=prepared.excluded_dimension_count,
            excluded_dimension_indices=prepared.excluded_dimension_indices,
            seed=permutation.seed,
        )
    except (IncompleteEvidenceError, TypeError, ValueError) as exc:
        excluded_dimension_indices = getattr(
            exc, "excluded_dimension_indices", excluded_dimension_indices
        )
        return SignalEvidence(
            signal=Signal.QUERY_VECTOR,
            complete=False,
            reference_count=reference_count,
            current_count=current_count,
            statistic=None,
            effect=None,
            effect_floor=_EFFECT_FLOORS[Signal.QUERY_VECTOR],
            raw_p_value=None,
            excluded_dimension_count=len(excluded_dimension_indices),
            excluded_dimension_indices=excluded_dimension_indices,
            reason=str(exc),
        )


def two_sample_ks_statistic(
    reference: Sequence[float] | np.ndarray,
    current: Sequence[float] | np.ndarray,
) -> float:
    """Return the exact two-sample empirical-CDF supremum distance."""

    reference_values = _as_float64_1d(reference, name="reference sample")
    current_values = _as_float64_1d(current, name="current sample")
    support = np.unique(np.concatenate((reference_values, current_values)))
    reference_cdf = np.searchsorted(
        np.sort(reference_values), support, side="right"
    ) / reference_values.size
    current_cdf = np.searchsorted(
        np.sort(current_values), support, side="right"
    ) / current_values.size
    return float(np.max(np.abs(reference_cdf - current_cdf)))


def _ks_batch_statistic(values: np.ndarray, reference_count: int) -> BatchStatistic:
    order = np.argsort(values, kind="stable")
    sorted_values = values[order]
    tie_ends = np.concatenate(
        (
            np.flatnonzero(sorted_values[:-1] != sorted_values[1:]),
            np.array([sorted_values.size - 1]),
        )
    )
    current_count = values.size - reference_count
    positions = np.arange(1, values.size + 1, dtype=np.float64)

    def statistic(membership: np.ndarray) -> np.ndarray:
        sorted_membership = membership[:, order]
        cumulative_reference = np.cumsum(sorted_membership, axis=1, dtype=np.int64)
        reference_cdf = cumulative_reference / reference_count
        current_cdf = (positions[None, :] - cumulative_reference) / current_count
        return np.max(
            np.abs(reference_cdf[:, tie_ends] - current_cdf[:, tie_ends]), axis=1
        )

    return statistic


def ks_signal_test(
    reference: Sequence[float] | np.ndarray,
    current: Sequence[float] | np.ndarray,
    *,
    signal: Signal | str,
    metric: Metric | str,
    detector_seed: int,
    window_id: CanonicalValue,
) -> SignalEvidence:
    """Compute threshold/cardinality KS evidence, or fail closed."""

    normalized_signal = _coerce_signal(signal)
    if normalized_signal not in {Signal.THRESHOLD, Signal.CARDINALITY}:
        raise ValueError("KS signal must be THRESHOLD or CARDINALITY")
    reference_count = np.asarray(reference).size
    current_count = np.asarray(current).size
    try:
        reference_values = _as_float64_1d(reference, name="reference sample")
        current_values = _as_float64_1d(current, name="current sample")
        if normalized_signal is Signal.CARDINALITY:
            combined_cardinalities = np.concatenate(
                (reference_values, current_values)
            )
            if np.any(combined_cardinalities < 0.0) or np.any(
                combined_cardinalities != np.floor(combined_cardinalities)
            ):
                raise IncompleteEvidenceError(
                    "exact cardinalities must be non-negative integers"
                )
        observed = two_sample_ks_statistic(reference_values, current_values)
        combined = np.concatenate((reference_values, current_values))
        permutation = deterministic_permutation_p_value(
            observed_statistic=observed,
            total_count=combined.size,
            reference_count=reference_values.size,
            detector_seed=detector_seed,
            metric=metric,
            window_id=window_id,
            signal=normalized_signal,
            batch_statistic=_ks_batch_statistic(combined, reference_values.size),
        )
        return SignalEvidence(
            signal=normalized_signal,
            complete=True,
            reference_count=reference_values.size,
            current_count=current_values.size,
            statistic=observed,
            effect=observed,
            effect_floor=_EFFECT_FLOORS[normalized_signal],
            raw_p_value=permutation.p_value,
            seed=permutation.seed,
        )
    except (IncompleteEvidenceError, TypeError, ValueError) as exc:
        return SignalEvidence(
            signal=normalized_signal,
            complete=False,
            reference_count=reference_count,
            current_count=current_count,
            statistic=None,
            effect=None,
            effect_floor=_EFFECT_FLOORS[normalized_signal],
            raw_p_value=None,
            reason=str(exc),
        )


def _recall_sample_values(sample: RecallAuditSample) -> np.ndarray:
    expected = _canonical_unique_ids(
        sample.expected_audit_ids, expected_count=AUDIT_QUERY_COUNT
    )
    observed = _canonical_unique_ids(
        sample.observed_audit_ids, expected_count=AUDIT_QUERY_COUNT
    )
    if {encoded for _, encoded in expected} != {encoded for _, encoded in observed}:
        raise IncompleteEvidenceError("observed audit IDs do not match expected IDs")
    values = _as_float64_1d(sample.values, name="recall values")
    if values.shape != (AUDIT_QUERY_COUNT,):
        raise IncompleteEvidenceError("recall values must have shape exactly (50,)")
    if np.any(values < 0.0) or np.any(values > 1.0):
        raise IncompleteEvidenceError("recall values must be within [0.0, 1.0]")
    agreement = np.asarray(sample.flat_oracle_agreement)
    if agreement.dtype != np.dtype(bool):
        raise IncompleteEvidenceError("FLAT/oracle agreement flags must be Boolean")
    if agreement.shape != (AUDIT_QUERY_COUNT,) or not np.all(agreement):
        raise IncompleteEvidenceError("all 50 FLAT/oracle checks must agree")
    if isinstance(sample.sentinel_ef, bool) or sample.sentinel_ef != SENTINEL_EF:
        raise IncompleteEvidenceError("every recall value must use sentinel ef=100")
    if isinstance(sample.limit, bool) or sample.limit != RESULT_LIMIT:
        raise IncompleteEvidenceError("recall audit limit must equal 100")
    if any(
        isinstance(value, bool) or not isinstance(value, Integral) or value != 0
        for value in (
            sample.failed_query_count,
            sample.timeout_query_count,
            sample.threshold_violation_count,
        )
    ):
        raise IncompleteEvidenceError(
            "recall audit must have zero failures, timeouts, and threshold violations"
        )
    _coerce_metric(sample.metric)
    if not sample.collection_data_identity or not sample.index_build_identity:
        raise IncompleteEvidenceError("recall audit identity fields must be non-empty")
    return values


def _recall_batch_statistic(values: np.ndarray, reference_count: int) -> BatchStatistic:
    current_count = values.size - reference_count
    centered_values = values - values.mean(dtype=np.float64)

    def statistic(membership: np.ndarray) -> np.ndarray:
        reference_membership = membership.astype(np.float64, copy=False)
        current_membership = (~membership).astype(np.float64, copy=False)
        reference_sum = reference_membership @ centered_values
        current_sum = current_membership @ centered_values
        return reference_sum / reference_count - current_sum / current_count

    return statistic


def recall_signal_test(
    reference: RecallAuditSample,
    current: RecallAuditSample,
    *,
    detector_seed: int,
) -> SignalEvidence:
    """Compute the exact `(50,)/(50,)` one-sided recall permutation test."""

    try:
        reference_metric = _coerce_metric(reference.metric)
        current_metric = _coerce_metric(current.metric)
        if reference_metric is not current_metric:
            raise IncompleteEvidenceError("recall audit metrics must match")
        if reference.window_id == current.window_id:
            raise IncompleteEvidenceError("recall audits must use different windows")
        if reference.collection_data_identity != current.collection_data_identity:
            raise IncompleteEvidenceError("collection/data identities must match")
        if reference.index_build_identity != current.index_build_identity:
            raise IncompleteEvidenceError("index-build identities must match")
        reference_values = _recall_sample_values(reference)
        current_values = _recall_sample_values(current)
        observed = float(reference_values.mean() - current_values.mean())
        combined = np.concatenate((reference_values, current_values))
        permutation = deterministic_permutation_p_value(
            observed_statistic=observed,
            total_count=combined.size,
            reference_count=AUDIT_QUERY_COUNT,
            detector_seed=detector_seed,
            metric=current_metric,
            window_id=current.window_id,
            signal=Signal.RECALL,
            batch_statistic=_recall_batch_statistic(combined, AUDIT_QUERY_COUNT),
        )
        return SignalEvidence(
            signal=Signal.RECALL,
            complete=True,
            reference_count=AUDIT_QUERY_COUNT,
            current_count=AUDIT_QUERY_COUNT,
            statistic=observed,
            effect=observed,
            effect_floor=_EFFECT_FLOORS[Signal.RECALL],
            raw_p_value=permutation.p_value,
            seed=permutation.seed,
        )
    except (IncompleteEvidenceError, TypeError, ValueError) as exc:
        return SignalEvidence(
            signal=Signal.RECALL,
            complete=False,
            reference_count=_first_dimension_count(reference.values),
            current_count=_first_dimension_count(current.values),
            statistic=None,
            effect=None,
            effect_floor=_EFFECT_FLOORS[Signal.RECALL],
            raw_p_value=None,
            reason=str(exc),
        )


def holm_step_down(
    p_values: Mapping[Signal | str, float],
    *,
    alpha: float = FAMILY_WISE_ALPHA,
) -> dict[Signal, float]:
    """Return monotone Holm-adjusted p-values in canonical signal keys."""

    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must lie strictly between 0 and 1")
    normalized: dict[Signal, float] = {}
    for signal, value in p_values.items():
        canonical_signal = _coerce_signal(signal)
        numeric = float(value)
        if canonical_signal in normalized:
            raise ValueError(f"duplicate p-value for {canonical_signal.value}")
        if not np.isfinite(numeric) or not 0.0 <= numeric <= 1.0:
            raise ValueError("p-values must be finite and within [0, 1]")
        normalized[canonical_signal] = numeric

    ordered = sorted(normalized.items(), key=lambda item: (item[1], item[0].value))
    adjusted: dict[Signal, float] = {}
    running = 0.0
    count = len(ordered)
    for index, (signal, value) in enumerate(ordered):
        running = max(running, (count - index) * value)
        adjusted[signal] = min(1.0, running)
    return adjusted


def finalize_window_evidence(
    *,
    metric: Metric | str,
    window_id: CanonicalValue,
    signals: Sequence[SignalEvidence],
    eligible_query_count: int = ELIGIBLE_QUERY_COUNT,
    audit_query_count: int = AUDIT_QUERY_COUNT,
    prerequisites_valid: bool = True,
    reason_codes: Sequence[str] = (),
    provenance: EvidenceProvenance | None = None,
) -> WindowEvidence:
    """Validate the four-signal family and apply Holm correction atomically."""

    normalized_metric = _coerce_metric(metric)
    signal_map: dict[Signal, SignalEvidence] = {}
    reasons = list(reason_codes)
    for item in signals:
        if item.signal in signal_map:
            reasons.append(f"DUPLICATE_SIGNAL:{item.signal.value}")
        signal_map[item.signal] = item
    missing = [signal for signal in _SIGNAL_ORDER if signal not in signal_map]
    reasons.extend(f"MISSING_SIGNAL:{signal.value}" for signal in missing)
    if eligible_query_count != ELIGIBLE_QUERY_COUNT:
        reasons.append("INCOMPLETE_ELIGIBLE_QUERY_WINDOW")
    if audit_query_count != AUDIT_QUERY_COUNT:
        reasons.append("INCOMPLETE_AUDIT_WINDOW")
    if not prerequisites_valid:
        reasons.append("INVALID_WINDOW_PREREQUISITES")
    if provenance is not None and (
        not evidence_provenance_valid(provenance)
        or provenance.metric is not normalized_metric
        or provenance.current_window_id != window_id
    ):
        reasons.append("EVIDENCE_PROVENANCE_INVALID")

    for signal, item in signal_map.items():
        expected = _EXPECTED_COUNTS[signal]
        if item.reference_count != expected or item.current_count != expected:
            reasons.append(f"INVALID_SAMPLE_COUNT:{signal.value}")
        if not item.complete:
            reasons.append(f"INCOMPLETE_SIGNAL:{signal.value}")
        if item.raw_p_value is None or item.effect is None or item.statistic is None:
            reasons.append(f"MISSING_STATISTIC:{signal.value}")
        else:
            if (
                not np.isfinite(item.raw_p_value)
                or not 0.0 <= item.raw_p_value <= 1.0
                or not np.isfinite(item.effect)
                or not np.isfinite(item.statistic)
            ):
                reasons.append(f"INVALID_STATISTIC:{signal.value}")
        if item.effect_floor != _EFFECT_FLOORS[signal]:
            reasons.append(f"INVALID_EFFECT_FLOOR:{signal.value}")

    complete = not reasons and len(signal_map) == len(_SIGNAL_ORDER)
    if not complete:
        ordered_signals = tuple(
            signal_map[signal] for signal in _SIGNAL_ORDER if signal in signal_map
        )
        return WindowEvidence(
            metric=normalized_metric,
            window_id=window_id,
            signals=ordered_signals,
            complete=False,
            reason_codes=tuple(dict.fromkeys(reasons)),
            provenance=provenance,
        )

    adjusted = holm_step_down(
        {
            signal: float(signal_map[signal].raw_p_value)
            for signal in _SIGNAL_ORDER
        }
    )
    ordered_signals = tuple(
        replace(signal_map[signal], adjusted_p_value=adjusted[signal])
        for signal in _SIGNAL_ORDER
    )
    return WindowEvidence(
        metric=normalized_metric,
        window_id=window_id,
        signals=ordered_signals,
        complete=True,
        provenance=provenance,
    )


def evaluate_drift_decision(
    previous: WindowEvidence | None,
    current: WindowEvidence,
) -> DriftDecision:
    """Apply ADR-002 completeness, hysteresis, and classification rules."""

    if previous is None:
        return DriftDecision(
            state=DetectorState.INSUFFICIENT_EVIDENCE,
            classification=DriftClassification.NONE,
            reason_codes=("MISSING_PREVIOUS_WINDOW",),
            evidence_provenance=current.provenance,
        )
    reasons: list[str] = []
    if previous.metric is not current.metric:
        reasons.append("METRIC_MISMATCH")
    if previous.window_id == current.window_id:
        reasons.append("WINDOW_IDS_MUST_DIFFER")
    if (previous.provenance is None) != (current.provenance is None):
        reasons.append("EVIDENCE_PROVENANCE_MISSING")
    elif previous.provenance is not None and current.provenance is not None:
        static_fields = (
            "metric", "threshold_stratum", "reference_window_id",
            "reference_manifest_sha256", "configuration_identity", "data_identity",
            "flat_binding_id", "hnsw_binding_id",
        )
        if (
            not evidence_provenance_valid(previous.provenance)
            or not evidence_provenance_valid(current.provenance)
            or any(
                getattr(previous.provenance, field) != getattr(current.provenance, field)
                for field in static_fields
            )
        ):
            reasons.append("EVIDENCE_PROVENANCE_MISMATCH")
    if not previous.complete:
        reasons.extend(previous.reason_codes or ("PREVIOUS_WINDOW_INCOMPLETE",))
    if not current.complete:
        reasons.extend(current.reason_codes or ("CURRENT_WINDOW_INCOMPLETE",))
    if reasons:
        return DriftDecision(
            state=DetectorState.INSUFFICIENT_EVIDENCE,
            classification=DriftClassification.NONE,
            reason_codes=tuple(dict.fromkeys(reasons)),
            evidence_provenance=current.provenance,
        )

    previous_signals = previous.by_signal()
    current_signals = current.by_signal()
    previous_breaches = {
        signal for signal, evidence in previous_signals.items() if evidence.breach
    }
    current_breaches = {
        signal for signal, evidence in current_signals.items() if evidence.breach
    }
    consecutive = previous_breaches & current_breaches

    if not consecutive:
        if current_breaches:
            return DriftDecision(
                state=DetectorState.INSUFFICIENT_EVIDENCE,
                classification=DriftClassification.NONE,
                reason_codes=("PENDING_CONFIRMATION",),
                evidence_provenance=current.provenance,
            )
        return DriftDecision(
            state=DetectorState.NO_DRIFT,
            classification=DriftClassification.NONE,
            evidence_provenance=current.provenance,
        )

    has_input = bool(consecutive & _INPUT_SIGNALS)
    has_quality = Signal.RECALL in consecutive
    if has_input and has_quality:
        classification = DriftClassification.INPUT_AND_QUALITY_DRIFT
    elif has_input:
        classification = DriftClassification.INPUT_DRIFT
    else:
        classification = DriftClassification.QUALITY_DRIFT

    ordered_triggers = tuple(signal for signal in _SIGNAL_ORDER if signal in consecutive)
    significance_evidence_scores: list[float] = []
    magnitude_values: list[float] = []
    for signal in ordered_triggers:
        previous_evidence = previous_signals[signal]
        current_evidence = current_signals[signal]
        significance_evidence_scores.extend(
            (
                1.0 - float(previous_evidence.adjusted_p_value),
                1.0 - float(current_evidence.adjusted_p_value),
            )
        )
        magnitude_values.extend(
            (
                float(previous_evidence.gate_ratio),
                float(current_evidence.gate_ratio),
            )
        )
    return DriftDecision(
        state=DetectorState.DRIFT,
        classification=classification,
        triggering_signals=ordered_triggers,
        significance_evidence_score=min(significance_evidence_scores),
        drift_magnitude=min(magnitude_values),
        evidence_provenance=current.provenance,
    )


__all__ = [
    "AUDIT_QUERY_COUNT",
    "FAMILY_WISE_ALPHA",
    "PERMUTATION_BATCH_SIZE",
    "PERMUTATION_COUNT",
    "AuditSelection",
    "DetectorState",
    "DriftClassification",
    "DriftDecision",
    "EvidenceProvenance",
    "MMDResult",
    "PermutationEvidence",
    "RecallAuditSample",
    "SeedMaterial",
    "Signal",
    "SignalEvidence",
    "WindowEvidence",
    "build_evidence_provenance",
    "canonical_serialize_tuple",
    "derive_permutation_seed",
    "deterministic_permutation_p_value",
    "evaluate_drift_decision",
    "evidence_provenance_valid",
    "finalize_window_evidence",
    "holm_step_down",
    "ks_signal_test",
    "mmd_squared",
    "query_vector_signal_test",
    "recall_signal_test",
    "select_audit_sample",
    "two_sample_ks_statistic",
]
