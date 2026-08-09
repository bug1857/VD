"""Pure ADR-009 calibrated response-profile statistics and verification.

Purpose:
    Build one immutable four-``ef`` predictive profile from exactly 1,200
    query-major observations, and verify an already parsed profile document by
    complete statistical and identity recomputation.
Scope:
    This module performs no persistence, replay acquisition, Milvus access,
    freshness decision, policy evaluation, or candidate actuation.  A profile
    is predictive evidence only.  ``raw_evidence_sha256`` is an externally
    supplied identity pin; R1 does not authenticate that it names the source of
    the supplied observations.  Because the R1 evidence projection contains no
    per-query payload digests, role manifests, or realized replay schedule, R1
    also does not verify query-payload uniqueness, cross-role disjointness, or
    replay-schedule membership.  A later authenticated raw-evidence boundary
    must establish those population properties before candidate-capable or
    otherwise verified consumption.
Failure modes:
    Unsupported values, incomplete observations, identity/configuration
    mismatches, stored-statistic tamper, and digest tamper raise
    ``ResponseProfileContractError`` with a stable reason code.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import hmac
import math
import re
import unicodedata

from .artifacts import canonical_json_bytes
from .config import IndexTrack, Metric, SearchConfiguration, THRESHOLD_LABELS
from .drift import canonical_serialize_tuple
from .lkg_window_readiness import (
    parse_rfc3339_utc_instant,
    validate_rfc3339_utc,
)
from .search_configuration_digest import search_configuration_document


__all__ = [
    "SUPPORTED_EFS",
    "OBSERVATION_COUNT",
    "MEASURED_SEARCH_COUNT",
    "ALPHA_FAMILY",
    "ALPHA_CELL",
    "P95_POINT_RANK",
    "P95_LCB_RANK",
    "P95_UCB_RANK",
    "PROFILE_SCHEMA_VERSION",
    "ESTIMATOR_CONTRACT_VERSION",
    "PROFILE_HASH_DOMAIN",
    "ResponseProfileContractError",
    "ResponseProfileEfObservation",
    "ResponseProfileQueryObservation",
    "ResponseProfileCalibrationEvidence",
    "ResponseProfileIdentity",
    "ResponseProfileEstimate",
    "CalibratedResponseProfile",
    "derive_v1_latency_ranks",
    "compute_response_profile_estimates",
    "build_calibrated_response_profile",
    "verify_calibrated_response_profile",
    "verify_response_profile_document",
    "response_profile_payload",
    "response_profile_document",
]


SUPPORTED_EFS = (200, 400, 800, 1600)
OBSERVATION_COUNT = 1_200
MEASURED_SEARCH_COUNT = OBSERVATION_COUNT * len(SUPPORTED_EFS)
ALPHA_FAMILY = 0.05
ALPHA_CELL = 0.05 / 16.0
P95_POINT_RANK = 1_140
P95_LCB_RANK = 1_118
P95_UCB_RANK = 1_161

PROFILE_SCHEMA_VERSION = "calibrated-response-profile-v1"
ESTIMATOR_CONTRACT_VERSION = "response-profile-estimator-v1"
PROFILE_HASH_DOMAIN = b"VD::CALIBRATED_RESPONSE_PROFILE::V1\x00"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_PROFILE_ENVELOPE_FIELDS = frozenset({"profile_payload", "profile_sha256"})
_PROFILE_PAYLOAD_FIELDS = frozenset(
    {
        "schema_version",
        "estimator_contract_version",
        "metric",
        "threshold_stratum",
        "supported_efs",
        "search_configurations",
        "hnsw_index_identity",
        "data_identity",
        "workload_manifest_sha256",
        "ordered_query_payload_sha256",
        "replay_schedule_sha256",
        "control_profile_sha256",
        "environment_manifest_sha256",
        "raw_evidence_sha256",
        "source_revision",
        "calibration_started_at_utc",
        "calibration_completed_at_utc",
        "generated_at_utc",
        "estimates",
    }
)
_ESTIMATE_FIELDS = frozenset(
    {
        "ef",
        "observation_count",
        "mean_recall",
        "recall_lcb",
        "recall_ucb",
        "p95_latency_ms",
        "p95_latency_lcb_ms",
        "p95_latency_ucb_ms",
    }
)
_CONSTRUCTION_TOKEN = object()


class ResponseProfileContractError(ValueError):
    """Stable fail-closed response-profile contract error."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileContractError:
    return ResponseProfileContractError(message, code=code)


@dataclass(frozen=True, slots=True)
class ResponseProfileEfObservation:
    """One already-observed recall/latency pair for one exact ``ef``."""

    ef: int
    capped_recall: float
    latency_ms: float


@dataclass(frozen=True, slots=True)
class ResponseProfileQueryObservation:
    """One canonical query and all four response observations in ef order."""

    query_id: int | str
    responses: tuple[ResponseProfileEfObservation, ...]


@dataclass(frozen=True, slots=True)
class ResponseProfileCalibrationEvidence:
    """Immutable in-memory projection used for R1 statistical recomputation.

    ``raw_evidence_sha256`` is compared as an identity field.  This value does
    not prove that the digest authenticates these observations; durable source
    authentication is intentionally deferred beyond R1.  Query-payload
    uniqueness, role disjointness, and replay-schedule membership are likewise
    outside this projection because their raw inputs are not present here.
    """

    raw_evidence_sha256: str
    observations: tuple[ResponseProfileQueryObservation, ...]


@dataclass(frozen=True, slots=True)
class ResponseProfileIdentity:
    """Expected profile identity and control lineage, excluding raw results."""

    metric: Metric
    threshold_stratum: str
    search_configurations: tuple[SearchConfiguration, ...]
    hnsw_index_identity: str
    data_identity: str
    workload_manifest_sha256: str
    ordered_query_payload_sha256: str
    replay_schedule_sha256: str
    control_profile_sha256: str
    environment_manifest_sha256: str
    source_revision: str
    calibration_started_at_utc: str
    calibration_completed_at_utc: str
    generated_at_utc: str


@dataclass(frozen=True, slots=True)
class ResponseProfileEstimate:
    """One formula-derived v1 response point and simultaneous bounds."""

    ef: int
    observation_count: int
    mean_recall: float
    recall_lcb: float
    recall_ucb: float
    p95_latency_ms: float
    p95_latency_lcb_ms: float
    p95_latency_ucb_ms: float


@dataclass(frozen=True, slots=True, init=False)
class CalibratedResponseProfile:
    """Immutable recomputed ADR-009 profile; predictive, never authority.

    Private construction is API discipline rather than cryptographic
    authenticity.  Any consumer crossing a trust boundary must call the full
    verifier with the expected identity and supplied evidence projection.
    """

    schema_version: str
    estimator_contract_version: str
    metric: Metric
    threshold_stratum: str
    supported_efs: tuple[int, ...]
    search_configurations: tuple[SearchConfiguration, ...]
    hnsw_index_identity: str
    data_identity: str
    workload_manifest_sha256: str
    ordered_query_payload_sha256: str
    replay_schedule_sha256: str
    control_profile_sha256: str
    environment_manifest_sha256: str
    raw_evidence_sha256: str
    source_revision: str
    calibration_started_at_utc: str
    calibration_completed_at_utc: str
    generated_at_utc: str
    estimates: tuple[ResponseProfileEstimate, ...]
    profile_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "CalibratedResponseProfile can only be created by the R1 "
            "builder or full verifier"
        )

    @classmethod
    def _from_validated(
        cls,
        *,
        identity: _ValidatedIdentity,
        raw_evidence_sha256: str,
        estimates: tuple[ResponseProfileEstimate, ...],
        profile_sha256: str,
        construction_token: object,
    ) -> CalibratedResponseProfile:
        if construction_token is not _CONSTRUCTION_TOKEN:
            raise TypeError("response-profile construction token is invalid")
        value = object.__new__(cls)
        fields: dict[str, object] = {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "estimator_contract_version": ESTIMATOR_CONTRACT_VERSION,
            "metric": identity.metric,
            "threshold_stratum": identity.threshold_stratum,
            "supported_efs": SUPPORTED_EFS,
            "search_configurations": identity.search_configurations,
            "hnsw_index_identity": identity.hnsw_index_identity,
            "data_identity": identity.data_identity,
            "workload_manifest_sha256": identity.workload_manifest_sha256,
            "ordered_query_payload_sha256": identity.ordered_query_payload_sha256,
            "replay_schedule_sha256": identity.replay_schedule_sha256,
            "control_profile_sha256": identity.control_profile_sha256,
            "environment_manifest_sha256": identity.environment_manifest_sha256,
            "raw_evidence_sha256": raw_evidence_sha256,
            "source_revision": identity.source_revision,
            "calibration_started_at_utc": identity.calibration_started_at_utc,
            "calibration_completed_at_utc": identity.calibration_completed_at_utc,
            "generated_at_utc": identity.generated_at_utc,
            "estimates": estimates,
            "profile_sha256": profile_sha256,
        }
        for field_name, field_value in fields.items():
            object.__setattr__(value, field_name, field_value)
        return value


@dataclass(frozen=True, slots=True)
class _ValidatedIdentity:
    metric: Metric
    threshold_stratum: str
    search_configurations: tuple[SearchConfiguration, ...]
    hnsw_index_identity: str
    data_identity: str
    workload_manifest_sha256: str
    ordered_query_payload_sha256: str
    replay_schedule_sha256: str
    control_profile_sha256: str
    environment_manifest_sha256: str
    source_revision: str
    calibration_started_at_utc: str
    calibration_completed_at_utc: str
    generated_at_utc: str


@dataclass(frozen=True, slots=True)
class _ValidatedEvidence:
    raw_evidence_sha256: str
    recalls_by_ef: tuple[tuple[float, ...], ...]
    latencies_by_ef: tuple[tuple[float, ...], ...]


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise _error("IDENTITY_INVALID", f"{field} must be lowercase SHA-256")
    return value


def _canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise _error("IDENTITY_INVALID", f"{field} must be non-empty text")
    return unicodedata.normalize("NFC", value)


def _canonical_query_id(value: object) -> int | str:
    if type(value) is int:
        return value
    if type(value) is str:
        if not value:
            raise _error("QUERY_ID_INVALID", "string query IDs must be non-empty")
        if unicodedata.normalize("NFC", value) != value:
            raise _error("QUERY_ID_INVALID", "string query IDs must be NFC-normalized")
        return value
    raise _error(
        "QUERY_ID_INVALID",
        "query IDs must be exact int or str values; booleans are forbidden",
    )


def _exact_float(value: object, *, field: str, unit_interval: bool) -> float:
    if type(value) is not float or not math.isfinite(value):
        raise _error(
            "OBSERVATION_VALUE_INVALID",
            f"{field} must be a finite exact float",
        )
    if unit_interval:
        if not 0.0 <= value <= 1.0:
            raise _error(
                "OBSERVATION_VALUE_INVALID", f"{field} must be in [0.0, 1.0]"
            )
    elif value < 0.0:
        raise _error(
            "OBSERVATION_VALUE_INVALID", f"{field} must be non-negative"
        )
    return 0.0 if value == 0.0 else value


def _validate_configuration(
    value: object,
    *,
    metric: Metric,
    threshold_stratum: str,
    expected_ef: int,
) -> SearchConfiguration:
    if type(value) is not SearchConfiguration:
        raise _error(
            "SEARCH_CONFIGURATION_INVALID",
            "every search configuration must be a concrete SearchConfiguration",
        )
    try:
        rebuilt = SearchConfiguration(
            metric=value.metric,
            threshold_label=value.threshold_label,
            radius=value.radius,
            index_track=value.index_track,
            ef=value.ef,
            limit=value.limit,
            consistency_level=value.consistency_level,
        )
        rebuilt.validate()
        search_configuration_document(rebuilt)
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error(
            "SEARCH_CONFIGURATION_INVALID", "search configuration is malformed"
        ) from exc
    if rebuilt != value:
        raise _error(
            "SEARCH_CONFIGURATION_INVALID",
            "search configuration failed exact reconstruction",
        )
    if (
        rebuilt.metric is not metric
        or rebuilt.threshold_label != threshold_stratum
        or rebuilt.index_track is not IndexTrack.HNSW
        or rebuilt.ef != expected_ef
    ):
        raise _error(
            "SEARCH_CONFIGURATION_INVALID",
            "search configuration does not match the profile cell",
        )
    return rebuilt


def _validate_identity(value: object) -> _ValidatedIdentity:
    if type(value) is not ResponseProfileIdentity:
        raise _error("IDENTITY_INVALID", "identity must be a concrete value")
    try:
        for field_name in ResponseProfileIdentity.__dataclass_fields__:
            getattr(value, field_name)
    except AttributeError as exc:
        raise _error("IDENTITY_INVALID", "identity is structurally malformed") from exc
    if type(value.metric) is not Metric:
        raise _error("IDENTITY_INVALID", "metric must be a concrete Metric")
    if (
        not isinstance(value.threshold_stratum, str)
        or value.threshold_stratum not in THRESHOLD_LABELS
    ):
        raise _error("IDENTITY_INVALID", "threshold stratum is unsupported")
    if not isinstance(value.search_configurations, tuple) or len(
        value.search_configurations
    ) != len(SUPPORTED_EFS):
        raise _error(
            "EF_FAMILY_INVALID", "exactly four ordered configurations are required"
        )
    configurations = tuple(
        _validate_configuration(
            configuration,
            metric=value.metric,
            threshold_stratum=value.threshold_stratum,
            expected_ef=ef,
        )
        for ef, configuration in zip(
            SUPPORTED_EFS, value.search_configurations, strict=True
        )
    )
    first = configurations[0]
    for configuration in configurations[1:]:
        if (
            configuration.metric is not first.metric
            or configuration.threshold_label != first.threshold_label
            or configuration.radius != first.radius
            or configuration.range_filter != first.range_filter
            or configuration.index_track is not first.index_track
            or configuration.limit != first.limit
            or configuration.consistency_level != first.consistency_level
        ):
            raise _error(
                "SEARCH_CONFIGURATION_INVALID",
                "search configurations may differ only by ef",
            )

    try:
        started_text = validate_rfc3339_utc(
            value.calibration_started_at_utc, field="calibration_started_at_utc"
        )
        completed_text = validate_rfc3339_utc(
            value.calibration_completed_at_utc,
            field="calibration_completed_at_utc",
        )
        generated_text = validate_rfc3339_utc(
            value.generated_at_utc, field="generated_at_utc"
        )
    except (TypeError, ValueError) as exc:
        raise _error("TIMESTAMP_INVALID", "profile timestamp is invalid") from exc
    started = parse_rfc3339_utc_instant(started_text)
    completed = parse_rfc3339_utc_instant(completed_text)
    generated = parse_rfc3339_utc_instant(generated_text)
    if not started <= completed <= generated:
        raise _error(
            "TIMESTAMP_INVALID",
            "timestamps must satisfy started <= completed <= generated",
        )

    return _ValidatedIdentity(
        metric=value.metric,
        threshold_stratum=value.threshold_stratum,
        search_configurations=configurations,
        hnsw_index_identity=_canonical_text(
            value.hnsw_index_identity, field="hnsw_index_identity"
        ),
        data_identity=_canonical_text(value.data_identity, field="data_identity"),
        workload_manifest_sha256=_sha256(
            value.workload_manifest_sha256, field="workload_manifest_sha256"
        ),
        ordered_query_payload_sha256=_sha256(
            value.ordered_query_payload_sha256,
            field="ordered_query_payload_sha256",
        ),
        replay_schedule_sha256=_sha256(
            value.replay_schedule_sha256, field="replay_schedule_sha256"
        ),
        control_profile_sha256=_sha256(
            value.control_profile_sha256, field="control_profile_sha256"
        ),
        environment_manifest_sha256=_sha256(
            value.environment_manifest_sha256,
            field="environment_manifest_sha256",
        ),
        source_revision=_canonical_text(value.source_revision, field="source_revision"),
        calibration_started_at_utc=started_text,
        calibration_completed_at_utc=completed_text,
        generated_at_utc=generated_text,
    )


def _validate_evidence(value: object) -> _ValidatedEvidence:
    if type(value) is not ResponseProfileCalibrationEvidence:
        raise _error("EVIDENCE_INVALID", "evidence must be a concrete value")
    try:
        raw_digest_value = value.raw_evidence_sha256
        observations = value.observations
    except AttributeError as exc:
        raise _error("EVIDENCE_INVALID", "evidence is structurally malformed") from exc
    raw_evidence_sha256 = _sha256(
        raw_digest_value, field="raw_evidence_sha256"
    )
    if not isinstance(observations, tuple) or len(observations) != OBSERVATION_COUNT:
        raise _error(
            "OBSERVATION_COUNT_INVALID",
            f"exactly {OBSERVATION_COUNT} query observations are required",
        )

    recalls: list[list[float]] = [[] for _ in SUPPORTED_EFS]
    latencies: list[list[float]] = [[] for _ in SUPPORTED_EFS]
    encoded_ids: set[bytes] = set()
    for query_index, query in enumerate(observations):
        if type(query) is not ResponseProfileQueryObservation:
            raise _error("EVIDENCE_INVALID", "query observation type is invalid")
        try:
            query_id_value = query.query_id
            responses = query.responses
        except AttributeError as exc:
            raise _error(
                "EVIDENCE_INVALID", "query observation is structurally malformed"
            ) from exc
        query_id = _canonical_query_id(query_id_value)
        encoded = canonical_serialize_tuple((query_id,))
        if encoded in encoded_ids:
            raise _error(
                "QUERY_ID_DUPLICATE",
                "query IDs must be unique under canonical serialization",
            )
        encoded_ids.add(encoded)
        if not isinstance(responses, tuple) or len(responses) != len(SUPPORTED_EFS):
            raise _error(
                "EF_FAMILY_INVALID", "each query requires exactly four ef results"
            )
        for ef_index, (expected_ef, observation) in enumerate(
            zip(SUPPORTED_EFS, responses, strict=True)
        ):
            if type(observation) is not ResponseProfileEfObservation:
                raise _error("EVIDENCE_INVALID", "ef observation type is invalid")
            try:
                observed_ef = observation.ef
                capped_recall = observation.capped_recall
                latency_ms = observation.latency_ms
            except AttributeError as exc:
                raise _error(
                    "EVIDENCE_INVALID", "ef observation is structurally malformed"
                ) from exc
            if type(observed_ef) is not int or observed_ef != expected_ef:
                raise _error(
                    "EF_FAMILY_INVALID",
                    "ef observations must use the exact supported order",
                )
            recalls[ef_index].append(
                _exact_float(
                    capped_recall,
                    field=f"observations[{query_index}].recall[{expected_ef}]",
                    unit_interval=True,
                )
            )
            latencies[ef_index].append(
                _exact_float(
                    latency_ms,
                    field=f"observations[{query_index}].latency[{expected_ef}]",
                    unit_interval=False,
                )
            )
    return _ValidatedEvidence(
        raw_evidence_sha256=raw_evidence_sha256,
        recalls_by_ef=tuple(tuple(values) for values in recalls),
        latencies_by_ef=tuple(tuple(values) for values in latencies),
    )


@lru_cache(maxsize=1)
def derive_v1_latency_ranks() -> tuple[int, int]:
    """Derive ADR-009's latency ranks using exact integer binomial arithmetic."""

    n = OBSERVATION_COUNT
    # For B~Binomial(n, 19/20), each probability has denominator 20**n and
    # integer numerator C(n,k)*19**k.
    weights = [1]
    for k in range(n):
        weights.append(weights[-1] * (n - k) * 19 // (k + 1))
    denominator = 20**n
    if sum(weights) != denominator:
        raise RuntimeError("exact binomial recurrence failed normalization")
    # 1-alpha_cell == 319/320 exactly.
    target_numerator = 319
    target_denominator = 320

    lower_rank: int | None = None
    tail = 0
    for k in range(n, 0, -1):
        tail += weights[k]
        if tail * target_denominator >= target_numerator * denominator:
            lower_rank = k
            break

    upper_rank: int | None = None
    cdf_through_k_minus_one = weights[0]
    for k in range(1, n + 1):
        if (
            cdf_through_k_minus_one * target_denominator
            >= target_numerator * denominator
        ):
            upper_rank = k
            break
        cdf_through_k_minus_one += weights[k]

    if lower_rank is None or upper_rank is None:
        raise RuntimeError("v1 latency confidence ranks are unavailable")
    return lower_rank, upper_rank


def _compute_estimates(
    evidence: _ValidatedEvidence,
) -> tuple[ResponseProfileEstimate, ...]:
    lower_rank, upper_rank = derive_v1_latency_ranks()
    if (lower_rank, upper_rank) != (P95_LCB_RANK, P95_UCB_RANK):
        raise RuntimeError("derived latency ranks do not match ADR-009 v1")
    epsilon = math.sqrt(
        math.log(1.0 / ALPHA_CELL) / (2.0 * OBSERVATION_COUNT)
    )
    estimates: list[ResponseProfileEstimate] = []
    for index, ef in enumerate(SUPPORTED_EFS):
        recalls = evidence.recalls_by_ef[index]
        latencies = sorted(evidence.latencies_by_ef[index])
        mean_recall = math.fsum(recalls) / OBSERVATION_COUNT
        estimates.append(
            ResponseProfileEstimate(
                ef=ef,
                observation_count=OBSERVATION_COUNT,
                mean_recall=mean_recall,
                recall_lcb=max(0.0, mean_recall - epsilon),
                recall_ucb=min(1.0, mean_recall + epsilon),
                p95_latency_ms=latencies[P95_POINT_RANK - 1],
                p95_latency_lcb_ms=latencies[P95_LCB_RANK - 1],
                p95_latency_ucb_ms=latencies[P95_UCB_RANK - 1],
            )
        )
    return tuple(estimates)


def compute_response_profile_estimates(
    evidence: ResponseProfileCalibrationEvidence,
) -> tuple[ResponseProfileEstimate, ...]:
    """Validate query-major observations and compute all four v1 estimates."""

    return _compute_estimates(_validate_evidence(evidence))


def _estimate_document(estimate: ResponseProfileEstimate) -> dict[str, object]:
    return {
        "ef": estimate.ef,
        "observation_count": estimate.observation_count,
        "mean_recall": estimate.mean_recall,
        "recall_lcb": estimate.recall_lcb,
        "recall_ucb": estimate.recall_ucb,
        "p95_latency_ms": estimate.p95_latency_ms,
        "p95_latency_lcb_ms": estimate.p95_latency_lcb_ms,
        "p95_latency_ucb_ms": estimate.p95_latency_ucb_ms,
    }


def _payload_from_parts(
    *,
    identity: _ValidatedIdentity,
    raw_evidence_sha256: str,
    estimates: tuple[ResponseProfileEstimate, ...],
) -> dict[str, object]:
    return {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "estimator_contract_version": ESTIMATOR_CONTRACT_VERSION,
        "metric": identity.metric.value,
        "threshold_stratum": identity.threshold_stratum,
        "supported_efs": list(SUPPORTED_EFS),
        "search_configurations": [
            search_configuration_document(configuration)
            for configuration in identity.search_configurations
        ],
        "hnsw_index_identity": identity.hnsw_index_identity,
        "data_identity": identity.data_identity,
        "workload_manifest_sha256": identity.workload_manifest_sha256,
        "ordered_query_payload_sha256": identity.ordered_query_payload_sha256,
        "replay_schedule_sha256": identity.replay_schedule_sha256,
        "control_profile_sha256": identity.control_profile_sha256,
        "environment_manifest_sha256": identity.environment_manifest_sha256,
        "raw_evidence_sha256": raw_evidence_sha256,
        "source_revision": identity.source_revision,
        "calibration_started_at_utc": identity.calibration_started_at_utc,
        "calibration_completed_at_utc": identity.calibration_completed_at_utc,
        "generated_at_utc": identity.generated_at_utc,
        "estimates": [_estimate_document(estimate) for estimate in estimates],
    }


def _profile_digest(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(PROFILE_HASH_DOMAIN + canonical_json_bytes(payload)).hexdigest()


def _build_validated_profile(
    *, identity: _ValidatedIdentity, evidence: _ValidatedEvidence
) -> CalibratedResponseProfile:
    estimates = _compute_estimates(evidence)
    payload = _payload_from_parts(
        identity=identity,
        raw_evidence_sha256=evidence.raw_evidence_sha256,
        estimates=estimates,
    )
    return CalibratedResponseProfile._from_validated(
        identity=identity,
        raw_evidence_sha256=evidence.raw_evidence_sha256,
        estimates=estimates,
        profile_sha256=_profile_digest(payload),
        construction_token=_CONSTRUCTION_TOKEN,
    )


def build_calibrated_response_profile(
    *,
    identity: ResponseProfileIdentity,
    evidence: ResponseProfileCalibrationEvidence,
) -> CalibratedResponseProfile:
    """Build a profile exclusively from validated identity and projected values.

    This pure builder recomputes R1 statistics and canonical identity/digest
    fields.  It does not authenticate the raw artifact named by
    ``raw_evidence_sha256`` or establish query-payload uniqueness, role
    disjointness, or replay-schedule membership; the later authenticated
    raw-evidence boundary owns those prerequisites.
    """

    return _build_validated_profile(
        identity=_validate_identity(identity), evidence=_validate_evidence(evidence)
    )


def response_profile_payload(profile: CalibratedResponseProfile) -> dict[str, object]:
    """Return the strict payload, excluding the detached profile digest."""

    if type(profile) is not CalibratedResponseProfile:
        raise _error("PROFILE_OBJECT_INVALID", "profile must be a concrete value")
    try:
        identity = ResponseProfileIdentity(
            metric=profile.metric,
            threshold_stratum=profile.threshold_stratum,
            search_configurations=profile.search_configurations,
            hnsw_index_identity=profile.hnsw_index_identity,
            data_identity=profile.data_identity,
            workload_manifest_sha256=profile.workload_manifest_sha256,
            ordered_query_payload_sha256=profile.ordered_query_payload_sha256,
            replay_schedule_sha256=profile.replay_schedule_sha256,
            control_profile_sha256=profile.control_profile_sha256,
            environment_manifest_sha256=profile.environment_manifest_sha256,
            source_revision=profile.source_revision,
            calibration_started_at_utc=profile.calibration_started_at_utc,
            calibration_completed_at_utc=profile.calibration_completed_at_utc,
            generated_at_utc=profile.generated_at_utc,
        )
        validated_identity = _validate_identity(identity)
        estimates = profile.estimates
        if (
            profile.schema_version != PROFILE_SCHEMA_VERSION
            or profile.estimator_contract_version != ESTIMATOR_CONTRACT_VERSION
            or profile.supported_efs != SUPPORTED_EFS
            or not isinstance(estimates, tuple)
            or tuple(estimate.ef for estimate in estimates) != SUPPORTED_EFS
            or any(type(estimate) is not ResponseProfileEstimate for estimate in estimates)
        ):
            raise _error("PROFILE_OBJECT_INVALID", "profile fields are malformed")
        raw_digest = _sha256(profile.raw_evidence_sha256, field="raw_evidence_sha256")
    except ResponseProfileContractError:
        raise
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("PROFILE_OBJECT_INVALID", "profile is structurally malformed") from exc
    return _payload_from_parts(
        identity=validated_identity,
        raw_evidence_sha256=raw_digest,
        estimates=estimates,
    )


def response_profile_document(profile: CalibratedResponseProfile) -> dict[str, object]:
    """Return the strict two-field profile envelope."""

    payload = response_profile_payload(profile)
    try:
        digest = profile.profile_sha256
    except AttributeError as exc:
        raise _error("PROFILE_OBJECT_INVALID", "profile digest is missing") from exc
    _sha256(digest, field="profile_sha256")
    return {"profile_payload": payload, "profile_sha256": digest}


def _exact_structure_equal(actual: object, expected: object) -> bool:
    if isinstance(expected, dict):
        if not isinstance(actual, Mapping) or set(actual) != set(expected):
            return False
        return all(
            isinstance(key, str)
            and _exact_structure_equal(actual[key], expected_value)
            for key, expected_value in expected.items()
        )
    if isinstance(expected, list):
        return isinstance(actual, list) and len(actual) == len(expected) and all(
            _exact_structure_equal(actual_value, expected_value)
            for actual_value, expected_value in zip(actual, expected, strict=True)
        )
    return type(actual) is type(expected) and actual == expected


def verify_response_profile_document(
    *,
    document: Mapping[str, object],
    identity: ResponseProfileIdentity,
    evidence: ResponseProfileCalibrationEvidence,
) -> CalibratedResponseProfile:
    """Verify an already parsed mapping by complete R1 recomputation.

    Duplicate JSON object keys cannot be detected after JSON parsing and are
    deliberately outside this function's claim.
    """

    if not isinstance(document, Mapping) or set(document) != _PROFILE_ENVELOPE_FIELDS:
        raise _error("PROFILE_SCHEMA_INVALID", "profile envelope fields are invalid")
    payload = document.get("profile_payload")
    stored_digest = document.get("profile_sha256")
    if not isinstance(payload, Mapping) or set(payload) != _PROFILE_PAYLOAD_FIELDS:
        raise _error("PROFILE_SCHEMA_INVALID", "profile payload fields are invalid")
    estimates = payload.get("estimates")
    if not isinstance(estimates, list) or len(estimates) != len(SUPPORTED_EFS):
        raise _error("PROFILE_SCHEMA_INVALID", "profile estimates are invalid")
    if any(
        not isinstance(estimate, Mapping) or set(estimate) != _ESTIMATE_FIELDS
        for estimate in estimates
    ):
        raise _error("PROFILE_SCHEMA_INVALID", "estimate fields are invalid")
    if not isinstance(stored_digest, str) or _SHA256_RE.fullmatch(stored_digest) is None:
        raise _error("PROFILE_DIGEST_INVALID", "profile digest is malformed")

    expected_profile = build_calibrated_response_profile(
        identity=identity, evidence=evidence
    )
    expected_payload = response_profile_payload(expected_profile)
    if not _exact_structure_equal(payload, expected_payload):
        raise _error(
            "PROFILE_RECOMPUTATION_MISMATCH",
            "stored profile differs from complete recomputation",
        )
    recomputed_digest = _profile_digest(payload)
    if not hmac.compare_digest(stored_digest, recomputed_digest):
        raise _error("PROFILE_DIGEST_MISMATCH", "profile digest does not match payload")
    if not hmac.compare_digest(stored_digest, expected_profile.profile_sha256):
        raise _error(
            "PROFILE_RECOMPUTATION_MISMATCH",
            "profile digest differs from expected identity/evidence",
        )
    return expected_profile


def verify_calibrated_response_profile(
    *,
    profile: object,
    identity: ResponseProfileIdentity,
    evidence: ResponseProfileCalibrationEvidence,
) -> CalibratedResponseProfile:
    """Reverify a profile object; never trust private construction alone."""

    if type(profile) is not CalibratedResponseProfile:
        raise _error("PROFILE_OBJECT_INVALID", "profile must be a concrete value")
    try:
        document = response_profile_document(profile)
    except ResponseProfileContractError:
        raise
    except (AttributeError, TypeError, ValueError, OverflowError) as exc:
        raise _error("PROFILE_OBJECT_INVALID", "profile is malformed") from exc
    return verify_response_profile_document(
        document=document, identity=identity, evidence=evidence
    )
