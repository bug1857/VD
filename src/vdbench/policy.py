"""Offline ADR-002 HNSW tuning-policy decision core.

Purpose:
    Turn one immutable drift decision and safety evidence into a recommendation,
    canary-start decision, no-op, or mandatory rollback decision.
Inputs:
    Precomputed response estimates and confidence bounds, pre-action checks,
    optional actual canary observations, a neutral Phase-3 authority pair for
    inactive candidate-capable evaluation, or legacy DRY_RUN qualification.
Outputs:
    An immutable, auditable policy decision. This module never actuates Milvus.
Dependencies:
    Python standard library plus the local detector/config value objects. The
    confidence-bound estimator is intentionally external to this module.
Complexity:
    O(k) in the number of response estimates; the ADR-002 ladder bounds k at 4.
Failure modes:
    Missing, inconsistent, non-finite, stale, or unsafe evidence fails closed to
    NO_CHANGE/RECOMMEND_EF, or to ROLLBACK when a canary is active.
Configuration:
    ADR-002 fixes the ef ladder, SLOs, transition bounds, and exception identity.
Extension points:
    Stage4 admission/live composition may consume START_CANARY; the generic
    safe-actuation boundary permanently refuses it and executes only ROLLBACK.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
import math
from numbers import Integral

from .config import Metric, THRESHOLD_LABELS
from .drift import (
    DetectorState,
    DriftClassification,
    DriftDecision,
    EvidenceProvenance,
    evidence_provenance_valid,
)
from .lkg_phase3_binding import (
    LkgPhase3AuthorityPair,
    bind_lkg_phase3_authority,
)

ACTUATION_LADDER = (200, 400, 800, 1600)
RECALL_FLOOR = 0.95
PAIRED_RECALL_DEGRADATION_LIMIT = 0.01
ABSOLUTE_LATENCY_CEILING_MS = 10.0
DEFAULT_RELATIVE_LATENCY_CEILING = 1.25
L2_TARGET_075_RELATIVE_LATENCY_CEILING = 1.50
MINIMUM_RECALL_IMPROVEMENT = 0.01
MINIMUM_LATENCY_REDUCTION_FRACTION = 0.05
EXCEPTION_MINIMUM_RECALL_IMPROVEMENT = 0.005
DRIFT_MAGNITUDE_FLOOR = 1.0
QUALIFICATION_QUERY_COUNT = 200
CANARY_QUERY_COUNT = 50


class PolicyAction(StrEnum):
    """The only four ADR-002 policy outputs."""

    NO_CHANGE = "NO_CHANGE"
    RECOMMEND_EF = "RECOMMEND_EF"
    START_CANARY = "START_CANARY"
    ROLLBACK = "ROLLBACK"


class PolicyMode(StrEnum):
    """ADR-002 policy modes; neither mode performs actuation here."""

    DRY_RUN = "DRY_RUN"
    CANARY_ENABLED = "CANARY_ENABLED"


@dataclass(frozen=True, slots=True)
class ResponseEstimate:
    """One externally estimated response and its precomputed one-sided bounds."""

    metric: Metric | str
    threshold_stratum: str
    ef: int
    mean_recall: float
    recall_lower_bound_95: float
    p95_latency_ms: float
    latency_upper_bound_95_ms: float
    validated_model: bool
    provenance: str


@dataclass(frozen=True, slots=True)
class PreActionSafety:
    """Pre-canary health, identity, authorization, and rollback evidence."""

    metric: Metric | str
    threshold_stratum: str
    configuration_identity: str
    index_identity: str
    data_identity: str
    response_model_provenance: str
    flat_index_identity: str | None = None
    milvus_healthy: bool = True
    etcd_healthy: bool = True
    minio_healthy: bool = True
    collection_loaded: bool = True
    configuration_valid: bool = True
    index_identity_unchanged: bool = True
    data_identity_unchanged: bool = True
    current_failed_query_count: int = 0
    current_timeout_query_count: int = 0
    current_threshold_violation_count: int = 0
    shadow_audit_complete: bool = True
    shadow_candidate_flat_oracle_agreement: bool = True
    shadow_last_known_good_flat_oracle_agreement: bool = True
    rollback_ready: bool = True
    rollback_tested: bool = True
    action_class_authorized: bool = True
    exception_authorized: bool = False


@dataclass(frozen=True, slots=True)
class CanaryObservation:
    """Actual candidate/last-known-good evidence from an active canary."""

    metric: Metric | str
    threshold_stratum: str
    candidate_ef: int
    last_known_good_ef: int
    completed_query_count: int
    candidate_mean_recall: float
    candidate_recall_lower_bound_95: float
    last_known_good_mean_recall: float
    candidate_p95_latency_ms: float
    candidate_latency_upper_bound_95_ms: float
    last_known_good_p95_latency_ms: float
    configuration_identity: str
    index_identity: str
    data_identity: str
    failed_query_count: int = 0
    timeout_query_count: int = 0
    threshold_violation_count: int = 0
    flat_oracle_agreement: bool = True
    milvus_healthy: bool = True
    etcd_healthy: bool = True
    minio_healthy: bool = True
    collection_loaded: bool = True
    configuration_valid: bool = True
    index_identity_unchanged: bool = True
    audit_record_present: bool = True
    actuation_exception: bool = False


@dataclass(frozen=True, slots=True)
class QualificationWindow:
    """One complete 200-query last-known-good qualification window."""

    window_id: str
    sequence_number: int
    metric: Metric | str
    threshold_stratum: str
    ef: int
    mean_recall: float
    recall_lower_bound_95: float
    p95_latency_ms: float
    latency_upper_bound_95_ms: float
    configuration_identity: str
    index_identity: str
    data_identity: str
    query_count: int = QUALIFICATION_QUERY_COUNT
    complete: bool = True
    health_passed: bool = True
    correctness_passed: bool = True
    recall_slo_passed: bool = True
    latency_slo_passed: bool = True
    rollback_clean: bool = True


@dataclass(frozen=True, slots=True)
class QualificationResult:
    """Fail-closed result of evaluating exactly two qualification windows."""

    qualified: bool
    ef: int | None
    reasons: tuple[str, ...]
    metric: Metric | None = None
    threshold_stratum: str | None = None
    configuration_identity: str | None = None
    index_identity: str | None = None
    data_identity: str | None = None
    qualifying_window_ids: tuple[str, str] = ()


@dataclass(frozen=True, slots=True)
class _PolicyLkgEvidence:
    """Policy-only projection; never a qualification authority or receipt."""

    qualified: bool
    ef: int | None
    reasons: tuple[str, ...]
    metric: Metric | None = None
    threshold_stratum: str | None = None
    configuration_identity: str | None = None
    index_identity: str | None = None
    data_identity: str | None = None
    source: str = "NONE"


@dataclass(frozen=True, slots=True)
class SafetyGateResult:
    """One named, inspectable pre-action or canary safety gate."""

    name: str
    passed: bool
    detail: str


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    """Complete ADR-002 policy output with no actuation side effects."""

    action: PolicyAction
    current_ef: int
    candidate_ef: int | None
    last_known_good_ef: int | None
    expected_mean_recall: float | None
    expected_recall_lower_bound_95: float | None
    expected_p95_latency_ms: float | None
    expected_latency_upper_bound_95_ms: float | None
    predicted_recall_improvement: float | None
    predicted_latency_reduction_fraction: float | None
    reason: str
    detector_confidence: float | None
    detector_magnitude: float | None
    safety_gate_results: tuple[SafetyGateResult, ...]
    mode: PolicyMode
    audit_id: str
    alert_required: bool = False
    evidence_provenance: EvidenceProvenance | None = None


def _coerce_metric(value: Metric | str) -> Metric:
    if isinstance(value, Metric):
        return value
    return Metric(value)


def _is_int(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, Integral)


def _finite(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _valid_threshold_stratum(value: object) -> bool:
    return isinstance(value, str) and value in THRESHOLD_LABELS


def _valid_count(value: object) -> bool:
    return _is_int(value) and int(value) >= 0


def _valid_response_numbers(estimate: ResponseEstimate) -> bool:
    values = (
        estimate.mean_recall,
        estimate.recall_lower_bound_95,
        estimate.p95_latency_ms,
        estimate.latency_upper_bound_95_ms,
    )
    return bool(
        all(_finite(value) for value in values)
        and 0.0 <= estimate.recall_lower_bound_95 <= estimate.mean_recall <= 1.0
        and 0.0 <= estimate.p95_latency_ms <= estimate.latency_upper_bound_95_ms
    )


def _validate_response_estimates(
    response_estimates: Mapping[int, ResponseEstimate],
) -> tuple[dict[int, ResponseEstimate], tuple[str, ...]]:
    validated: dict[int, ResponseEstimate] = {}
    reasons: list[str] = []
    if not isinstance(response_estimates, Mapping):
        return {}, ("RESPONSE_ESTIMATES_NOT_MAPPING",)
    for key, estimate in response_estimates.items():
        reason_count = len(reasons)
        if not _is_int(key) or int(key) not in ACTUATION_LADDER:
            reasons.append("RESPONSE_ESTIMATE_KEY_INVALID")
            continue
        if not isinstance(estimate, ResponseEstimate):
            reasons.append(f"RESPONSE_ESTIMATE_TYPE_INVALID:ef={key}")
            continue
        if not _is_int(estimate.ef) or int(key) != estimate.ef:
            reasons.append(f"RESPONSE_ESTIMATE_KEY_MISMATCH:ef={key}")
        try:
            _coerce_metric(estimate.metric)
        except (TypeError, ValueError):
            reasons.append(f"RESPONSE_ESTIMATE_METRIC_INVALID:ef={key}")
        if not _valid_threshold_stratum(estimate.threshold_stratum):
            reasons.append(f"RESPONSE_ESTIMATE_STRATUM_INVALID:ef={key}")
        if not _valid_response_numbers(estimate):
            reasons.append(f"RESPONSE_ESTIMATE_BOUNDS_INVALID:ef={key}")
        if type(estimate.validated_model) is not bool:
            reasons.append(f"RESPONSE_MODEL_STATUS_INVALID:ef={key}")
        if not _nonempty(estimate.provenance):
            reasons.append(f"RESPONSE_ESTIMATE_PROVENANCE_MISSING:ef={key}")
        if len(reasons) == reason_count:
            validated[int(key)] = estimate
    return validated, tuple(dict.fromkeys(reasons))


def _valid_qualification_numbers(window: QualificationWindow) -> bool:
    values = (
        window.mean_recall,
        window.recall_lower_bound_95,
        window.p95_latency_ms,
        window.latency_upper_bound_95_ms,
    )
    return bool(
        all(_finite(value) for value in values)
        and 0.0 <= window.recall_lower_bound_95 <= window.mean_recall <= 1.0
        and 0.0 <= window.p95_latency_ms <= window.latency_upper_bound_95_ms
    )


def qualify_last_known_good(
    windows: Sequence[QualificationWindow],
    *,
    audit_id: str,
) -> QualificationResult:
    """Apply the exact two-consecutive-window last-known-good contract."""

    reasons: list[str] = []
    if not _nonempty(audit_id):
        reasons.append("AUDIT_ID_MISSING")
    if len(windows) != 2:
        return QualificationResult(
            qualified=False,
            ef=None,
            reasons=tuple(reasons + ["EXACTLY_TWO_QUALIFICATION_WINDOWS_REQUIRED"]),
        )
    first, second = windows
    if not isinstance(first, QualificationWindow) or not isinstance(
        second, QualificationWindow
    ):
        return QualificationResult(
            qualified=False,
            ef=None,
            reasons=tuple(reasons + ["QUALIFICATION_WINDOW_TYPE_INVALID"]),
        )

    if not _is_int(first.sequence_number) or not _is_int(second.sequence_number):
        reasons.append("QUALIFICATION_SEQUENCE_INVALID")
    elif second.sequence_number != first.sequence_number + 1:
        reasons.append("QUALIFICATION_WINDOWS_NOT_CONSECUTIVE")
    if not _nonempty(first.window_id) or not _nonempty(second.window_id):
        reasons.append("QUALIFICATION_WINDOW_ID_MISSING")
    elif first.window_id == second.window_id:
        reasons.append("QUALIFICATION_WINDOW_IDS_MUST_DIFFER")
    if first.ef != second.ef:
        reasons.append("QUALIFICATION_EF_MISMATCH")
    if first.ef not in ACTUATION_LADDER or second.ef not in ACTUATION_LADDER:
        reasons.append("QUALIFICATION_EF_INELIGIBLE")

    try:
        first_metric = _coerce_metric(first.metric)
        second_metric = _coerce_metric(second.metric)
        if first_metric is not second_metric:
            reasons.append("QUALIFICATION_METRIC_MISMATCH")
    except (TypeError, ValueError):
        reasons.append("QUALIFICATION_METRIC_INVALID")
    if not _valid_threshold_stratum(
        first.threshold_stratum
    ) or not _valid_threshold_stratum(second.threshold_stratum):
        reasons.append("QUALIFICATION_STRATUM_INVALID")
    elif first.threshold_stratum != second.threshold_stratum:
        reasons.append("QUALIFICATION_STRATUM_MISMATCH")

    identity_fields = (
        ("CONFIGURATION", first.configuration_identity, second.configuration_identity),
        ("INDEX", first.index_identity, second.index_identity),
        ("DATA", first.data_identity, second.data_identity),
    )
    for name, first_value, second_value in identity_fields:
        if not _nonempty(first_value) or not _nonempty(second_value):
            reasons.append(f"QUALIFICATION_{name}_IDENTITY_MISSING")
        elif first_value != second_value:
            reasons.append(f"QUALIFICATION_{name}_IDENTITY_MISMATCH")

    for position, window in (("FIRST", first), ("SECOND", second)):
        if (
            not _is_int(window.query_count)
            or window.query_count != QUALIFICATION_QUERY_COUNT
        ):
            reasons.append(f"{position}_QUALIFICATION_QUERY_COUNT_INVALID")
        if type(window.complete) is not bool or not window.complete:
            reasons.append(f"{position}_QUALIFICATION_INCOMPLETE")
        if not _valid_qualification_numbers(window):
            reasons.append(f"{position}_QUALIFICATION_BOUNDS_INVALID")
        else:
            if window.recall_lower_bound_95 < RECALL_FLOOR:
                reasons.append(f"{position}_QUALIFICATION_RECALL_FLOOR_FAILED")
            if window.latency_upper_bound_95_ms > ABSOLUTE_LATENCY_CEILING_MS:
                reasons.append(f"{position}_QUALIFICATION_LATENCY_CEILING_FAILED")
        checks = (
            ("HEALTH", window.health_passed),
            ("CORRECTNESS", window.correctness_passed),
            ("RECALL_SLO", window.recall_slo_passed),
            ("LATENCY_SLO", window.latency_slo_passed),
            ("ROLLBACK_CLEAN", window.rollback_clean),
        )
        for name, passed in checks:
            if type(passed) is not bool or not passed:
                reasons.append(f"{position}_QUALIFICATION_{name}_FAILED")

    unique_reasons = tuple(dict.fromkeys(reasons))
    if unique_reasons:
        return QualificationResult(False, None, unique_reasons)
    return QualificationResult(
        qualified=True,
        ef=first.ef,
        reasons=(),
        metric=_coerce_metric(first.metric),
        threshold_stratum=first.threshold_stratum,
        configuration_identity=first.configuration_identity,
        index_identity=first.index_identity,
        data_identity=first.data_identity,
        qualifying_window_ids=(first.window_id, second.window_id),
    )


def _legacy_policy_lkg_evidence(
    qualification: QualificationResult,
) -> _PolicyLkgEvidence:
    return _PolicyLkgEvidence(
        qualified=qualification.qualified,
        ef=qualification.ef,
        reasons=qualification.reasons,
        metric=qualification.metric,
        threshold_stratum=qualification.threshold_stratum,
        configuration_identity=qualification.configuration_identity,
        index_identity=qualification.index_identity,
        data_identity=qualification.data_identity,
        source="LEGACY_DRY_RUN",
    )


def _phase3_policy_lkg_evidence(
    value: object,
) -> _PolicyLkgEvidence | None:
    """Revalidate through the sole neutral binder and project policy fields."""

    if type(value) is not LkgPhase3AuthorityPair:
        return None
    try:
        pair = bind_lkg_phase3_authority(
            authority=value.authority,
            verified_latest_reference=value.verified_latest_reference,
        )
        authority = pair.authority
        metric = _coerce_metric(authority.metric)
        evidence = _PolicyLkgEvidence(
            qualified=True,
            ef=authority.evaluated_ef,
            reasons=(),
            metric=metric,
            threshold_stratum=authority.threshold_stratum,
            index_identity=authority.index_identity,
            data_identity=authority.data_identity,
            source="PHASE3",
        )
    except (AttributeError, TypeError, ValueError):
        return None
    if (
        evidence.ef not in ACTUATION_LADDER
        or not _valid_threshold_stratum(evidence.threshold_stratum)
        or not _nonempty(evidence.index_identity)
        or not _nonempty(evidence.data_identity)
    ):
        return None
    return evidence


def _inactive_lkg_source(
    *,
    mode: PolicyMode,
    lkg_authority: object | None,
    qualification_windows: Sequence[QualificationWindow] | None,
    last_known_good: QualificationResult | None,
    audit_id: str,
) -> tuple[_PolicyLkgEvidence | None, str | None]:
    """Apply accepted source precedence for an inactive policy evaluation."""

    source_count = sum(
        source is not None
        for source in (lkg_authority, qualification_windows, last_known_good)
    )
    if source_count > 1:
        return None, "LKG_AUTHORITY_SOURCES_CONFLICT"

    if mode is PolicyMode.CANARY_ENABLED:
        if lkg_authority is None:
            return None, "PHASE3_LKG_AUTHORITY_REQUIRED"
        evidence = _phase3_policy_lkg_evidence(lkg_authority)
        return (
            (evidence, None)
            if evidence is not None
            else (None, "PHASE3_LKG_AUTHORITY_INVALID")
        )

    if source_count == 0:
        return None, "LKG_AUTHORITY_SOURCE_REQUIRED"
    if lkg_authority is not None:
        evidence = _phase3_policy_lkg_evidence(lkg_authority)
        return (
            (evidence, None)
            if evidence is not None
            else (None, "PHASE3_LKG_AUTHORITY_INVALID")
        )
    if last_known_good is not None:
        if not isinstance(last_known_good, QualificationResult):
            raise TypeError("last_known_good must be a QualificationResult")
        return _legacy_policy_lkg_evidence(last_known_good), None
    if qualification_windows is not None:
        return _legacy_policy_lkg_evidence(
            qualify_last_known_good(qualification_windows, audit_id=audit_id)
        ), None
    raise AssertionError("unreachable inactive LKG source state")


def _gate(name: str, passed: bool, detail: str) -> SafetyGateResult:
    return SafetyGateResult(name=name, passed=bool(passed), detail=detail)


def _decision(
    *,
    action: PolicyAction,
    current_ef: int,
    candidate_ef: int | None,
    last_known_good_ef: int | None,
    estimate: ResponseEstimate | None,
    current_estimate: ResponseEstimate | None,
    reason: str,
    detector: DriftDecision,
    gates: Sequence[SafetyGateResult],
    mode: PolicyMode,
    audit_id: str,
    alert_required: bool = False,
) -> PolicyDecision:
    recall_improvement = None
    latency_reduction = None
    if estimate is not None and current_estimate is not None:
        recall_improvement = estimate.mean_recall - current_estimate.mean_recall
        if current_estimate.p95_latency_ms > 0.0:
            latency_reduction = (
                current_estimate.p95_latency_ms - estimate.p95_latency_ms
            ) / current_estimate.p95_latency_ms
    return PolicyDecision(
        action=action,
        current_ef=current_ef,
        candidate_ef=candidate_ef,
        last_known_good_ef=last_known_good_ef,
        expected_mean_recall=(estimate.mean_recall if estimate else None),
        expected_recall_lower_bound_95=(
            estimate.recall_lower_bound_95 if estimate else None
        ),
        expected_p95_latency_ms=(estimate.p95_latency_ms if estimate else None),
        expected_latency_upper_bound_95_ms=(
            estimate.latency_upper_bound_95_ms if estimate else None
        ),
        predicted_recall_improvement=recall_improvement,
        predicted_latency_reduction_fraction=latency_reduction,
        reason=reason,
        detector_confidence=detector.significance_evidence_score,
        detector_magnitude=detector.drift_magnitude,
        safety_gate_results=tuple(gates),
        mode=mode,
        audit_id=audit_id,
        alert_required=alert_required,
        evidence_provenance=detector.evidence_provenance,
    )


def _evidence_provenance_gate(
    *,
    detector: DriftDecision,
    metric: Metric,
    threshold_stratum: str,
    pre_action: PreActionSafety,
) -> SafetyGateResult:
    """Bind a drift result to the exact pre-action identities, fail closed."""

    provenance = detector.evidence_provenance
    if provenance is None:
        return _gate(
            "EVIDENCE_PROVENANCE_BOUND",
            False,
            "DRIFT recommendation requires immutable evidence provenance",
        )
    if not evidence_provenance_valid(provenance):
        return _gate(
            "EVIDENCE_PROVENANCE_BOUND",
            False,
            "detector evidence provenance is malformed or digest-mismatched",
        )
    matches = bool(
        provenance.metric is metric
        and provenance.threshold_stratum == threshold_stratum
        and provenance.configuration_identity == pre_action.configuration_identity
        and provenance.data_identity == pre_action.data_identity
        and provenance.hnsw_binding_id == pre_action.index_identity
        and _nonempty(pre_action.flat_index_identity)
        and provenance.flat_binding_id == pre_action.flat_index_identity
    )
    return _gate(
        "EVIDENCE_PROVENANCE_BOUND",
        matches,
        "provenance metric, stratum, configuration, data, FLAT, and HNSW bindings must match pre-action evidence",
    )


def _quality_classification(classification: DriftClassification) -> bool:
    return classification in {
        DriftClassification.QUALITY_DRIFT,
        DriftClassification.INPUT_AND_QUALITY_DRIFT,
    }


def _exception_applies(
    *,
    metric: Metric,
    threshold_stratum: str,
    current_ef: int,
    candidate_ef: int,
    classification: DriftClassification,
    exception_authorized: bool,
) -> bool:
    return bool(
        metric is Metric.L2
        and threshold_stratum == "target-075"
        and current_ef == 400
        and candidate_ef == 800
        and _quality_classification(classification)
        and exception_authorized is True
    )


def _canary_hard_failure_gates(
    observation: CanaryObservation,
    *,
    detector: DriftDecision,
    current_ef: int,
    threshold_stratum: str,
    pre_action: PreActionSafety,
) -> tuple[SafetyGateResult, ...]:
    counts_valid = all(
        _valid_count(value)
        for value in (
            observation.completed_query_count,
            observation.failed_query_count,
            observation.timeout_query_count,
            observation.threshold_violation_count,
        )
    )
    metric_matches = False
    pre_action_metric: Metric | None = None
    try:
        pre_action_metric = _coerce_metric(pre_action.metric)
        metric_matches = _coerce_metric(observation.metric) is pre_action_metric
    except (TypeError, ValueError):
        pass
    identity_matches = bool(
        observation.configuration_identity == pre_action.configuration_identity
        and observation.index_identity == pre_action.index_identity
        and observation.data_identity == pre_action.data_identity
    )
    transition_valid = bool(
        current_ef == observation.last_known_good_ef
        and observation.candidate_ef in ACTUATION_LADDER
        and observation.last_known_good_ef in ACTUATION_LADDER
        and abs(
            ACTUATION_LADDER.index(observation.candidate_ef)
            - ACTUATION_LADDER.index(observation.last_known_good_ef)
        )
        == 1
    )
    direction_valid = False
    if _finite(observation.last_known_good_mean_recall):
        if _quality_classification(detector.classification):
            direction_valid = observation.candidate_ef > observation.last_known_good_ef
        elif detector.classification is DriftClassification.INPUT_DRIFT:
            direction_valid = (
                observation.candidate_ef > observation.last_known_good_ef
                if observation.last_known_good_mean_recall < RECALL_FLOOR
                else observation.candidate_ef < observation.last_known_good_ef
            )
    return (
        _gate(
            "CANARY_COUNTS_VALID", counts_valid, "all counts are non-negative integers"
        ),
        _gate(
            "CANARY_QUERY_FAILURES",
            observation.failed_query_count == 0,
            "failed candidate queries must equal zero",
        ),
        _gate(
            "CANARY_TIMEOUTS",
            observation.timeout_query_count == 0,
            "timed-out candidate queries must equal zero",
        ),
        _gate(
            "CANARY_THRESHOLD_SEMANTICS",
            observation.threshold_violation_count == 0,
            "threshold violations must equal zero",
        ),
        _gate(
            "CANARY_FLAT_ORACLE_AGREEMENT",
            observation.flat_oracle_agreement is True,
            "FLAT/oracle must agree",
        ),
        _gate(
            "CANARY_SERVICES_HEALTHY",
            observation.milvus_healthy is True
            and observation.etcd_healthy is True
            and observation.minio_healthy is True,
            "Milvus, etcd, and MinIO must be healthy",
        ),
        _gate(
            "CANARY_COLLECTION_LOADED",
            observation.collection_loaded is True,
            "collection must remain loaded",
        ),
        _gate(
            "CANARY_CONFIGURATION_VALID",
            observation.configuration_valid is True,
            "configuration must remain valid",
        ),
        _gate(
            "CANARY_INDEX_IDENTITY_UNCHANGED",
            observation.index_identity_unchanged is True,
            "index identity must remain unchanged",
        ),
        _gate(
            "CANARY_AUDIT_RECORD_PRESENT",
            observation.audit_record_present is True,
            "canary audit record must be present",
        ),
        _gate(
            "CANARY_NO_ACTUATION_EXCEPTION",
            observation.actuation_exception is False,
            "actuation boundary must not report an exception",
        ),
        _gate(
            "CANARY_METRIC_STRATUM_MATCH",
            metric_matches
            and _valid_threshold_stratum(threshold_stratum)
            and observation.threshold_stratum
            == pre_action.threshold_stratum
            == threshold_stratum,
            "metric and threshold stratum must match the explicit policy and pre-action evidence",
        ),
        _gate(
            "CANARY_IDENTITY_MATCH",
            identity_matches,
            "configuration/index/data identities must match last-known-good",
        ),
        _gate(
            "CANARY_TRANSITION_VALID",
            transition_valid,
            "candidate must be one adjacent ladder step from last-known-good",
        ),
        _gate(
            "CANARY_DIRECTION_VALID",
            direction_valid,
            "candidate direction must match the detector classification and paired current recall",
        ),
    )


def _rollback_reason(gates: Sequence[SafetyGateResult]) -> str | None:
    reason_by_gate = {
        "CANARY_COUNTS_VALID": "CONFIGURATION_VALIDATION_FAILURE",
        "CANARY_QUERY_FAILURES": "QUERY_FAILURE",
        "CANARY_TIMEOUTS": "QUERY_TIMEOUT",
        "CANARY_THRESHOLD_SEMANTICS": "THRESHOLD_VIOLATION",
        "CANARY_FLAT_ORACLE_AGREEMENT": "FLAT_ORACLE_DISAGREEMENT",
        "CANARY_SERVICES_HEALTHY": "REQUIRED_SERVICE_UNHEALTHY",
        "CANARY_COLLECTION_LOADED": "COLLECTION_UNLOADED",
        "CANARY_CONFIGURATION_VALID": "CONFIGURATION_VALIDATION_FAILURE",
        "CANARY_INDEX_IDENTITY_UNCHANGED": "INDEX_IDENTITY_CHANGED",
        "CANARY_AUDIT_RECORD_PRESENT": "AUDIT_ID_MISSING",
        "CANARY_NO_ACTUATION_EXCEPTION": "ACTUATION_EXCEPTION",
        "CANARY_METRIC_STRATUM_MATCH": "CONFIGURATION_VALIDATION_FAILURE",
        "CANARY_IDENTITY_MATCH": "CONFIGURATION_VALIDATION_FAILURE",
        "CANARY_TRANSITION_VALID": "CONFIGURATION_VALIDATION_FAILURE",
        "CANARY_DIRECTION_VALID": "CONFIGURATION_VALIDATION_FAILURE",
    }
    for gate in gates:
        if not gate.passed:
            return reason_by_gate[gate.name]
    return None


def _evaluate_active_canary(
    *,
    detector: DriftDecision,
    current_ef: int,
    observation: CanaryObservation,
    pre_action: PreActionSafety,
    response_estimates: Mapping[int, ResponseEstimate],
    mode: PolicyMode,
    threshold_stratum: str,
    audit_id: str,
) -> PolicyDecision:
    estimate = response_estimates.get(observation.candidate_ef)
    current_estimate = response_estimates.get(current_ef)
    if not _nonempty(audit_id) or observation.audit_record_present is not True:
        gate = _gate(
            "CANARY_AUDIT_RECORD_PRESENT",
            False,
            "active canary requires supplied and persisted audit identity",
        )
        return _decision(
            action=PolicyAction.ROLLBACK,
            current_ef=current_ef,
            candidate_ef=observation.candidate_ef,
            last_known_good_ef=observation.last_known_good_ef,
            estimate=estimate,
            current_estimate=current_estimate,
            reason="AUDIT_ID_MISSING",
            detector=detector,
            gates=(gate,),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    hard_gates = _canary_hard_failure_gates(
        observation,
        detector=detector,
        current_ef=current_ef,
        threshold_stratum=threshold_stratum,
        pre_action=pre_action,
    )
    hard_reason = _rollback_reason(hard_gates)
    if hard_reason is not None:
        return _decision(
            action=PolicyAction.ROLLBACK,
            current_ef=current_ef,
            candidate_ef=observation.candidate_ef,
            last_known_good_ef=observation.last_known_good_ef,
            estimate=estimate,
            current_estimate=current_estimate,
            reason=hard_reason,
            detector=detector,
            gates=hard_gates,
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    numbers_valid = bool(
        all(
            _finite(value)
            for value in (
                observation.candidate_mean_recall,
                observation.candidate_recall_lower_bound_95,
                observation.last_known_good_mean_recall,
                observation.candidate_p95_latency_ms,
                observation.candidate_latency_upper_bound_95_ms,
                observation.last_known_good_p95_latency_ms,
            )
        )
        and 0.0
        <= observation.candidate_recall_lower_bound_95
        <= observation.candidate_mean_recall
        <= 1.0
        and 0.0 <= observation.last_known_good_mean_recall <= 1.0
        and 0.0
        <= observation.candidate_p95_latency_ms
        <= observation.candidate_latency_upper_bound_95_ms
        and observation.last_known_good_p95_latency_ms > 0.0
    )
    if not numbers_valid:
        gate = _gate(
            "CANARY_BOUNDS_VALID",
            False,
            "canary point observations and precomputed bounds must be finite and ordered",
        )
        return _decision(
            action=PolicyAction.ROLLBACK,
            current_ef=current_ef,
            candidate_ef=observation.candidate_ef,
            last_known_good_ef=observation.last_known_good_ef,
            estimate=estimate,
            current_estimate=current_estimate,
            reason="CONFIGURATION_VALIDATION_FAILURE",
            detector=detector,
            gates=hard_gates + (gate,),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    if observation.completed_query_count < CANARY_QUERY_COUNT:
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=observation.candidate_ef,
            last_known_good_ef=observation.last_known_good_ef,
            estimate=estimate,
            current_estimate=current_estimate,
            reason="CANARY_IN_PROGRESS",
            detector=detector,
            gates=hard_gates,
            mode=mode,
            audit_id=audit_id,
        )
    if observation.completed_query_count != CANARY_QUERY_COUNT:
        invalid_count_gate = _gate(
            "CANARY_QUERY_COUNT_EXACT",
            False,
            "completed canary must contain exactly 50 candidate queries",
        )
        return _decision(
            action=PolicyAction.ROLLBACK,
            current_ef=current_ef,
            candidate_ef=observation.candidate_ef,
            last_known_good_ef=observation.last_known_good_ef,
            estimate=estimate,
            current_estimate=current_estimate,
            reason="CONFIGURATION_VALIDATION_FAILURE",
            detector=detector,
            gates=hard_gates + (invalid_count_gate,),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    try:
        metric = _coerce_metric(observation.metric)
    except (TypeError, ValueError):
        metric = Metric.L2
    exception = _exception_applies(
        metric=metric,
        threshold_stratum=observation.threshold_stratum,
        current_ef=observation.last_known_good_ef,
        candidate_ef=observation.candidate_ef,
        classification=detector.classification,
        exception_authorized=(
            pre_action.exception_authorized is True
            and observation.last_known_good_ef == current_ef
        ),
    )
    relative_ceiling = (
        L2_TARGET_075_RELATIVE_LATENCY_CEILING
        if exception
        else DEFAULT_RELATIVE_LATENCY_CEILING
    )
    recall_floor_passed = observation.candidate_recall_lower_bound_95 >= RECALL_FLOOR
    paired_recall_passed = (
        observation.last_known_good_mean_recall
        - observation.candidate_recall_lower_bound_95
        <= PAIRED_RECALL_DEGRADATION_LIMIT
    )
    absolute_latency_passed = (
        observation.candidate_latency_upper_bound_95_ms <= ABSOLUTE_LATENCY_CEILING_MS
    )
    relative_latency_passed = (
        observation.candidate_latency_upper_bound_95_ms
        <= relative_ceiling * observation.last_known_good_p95_latency_ms
    )
    exception_recall_passed = bool(
        not exception
        or observation.candidate_recall_lower_bound_95
        - observation.last_known_good_mean_recall
        >= EXCEPTION_MINIMUM_RECALL_IMPROVEMENT
    )
    slo_gates = (
        _gate(
            "CANARY_RECALL_FLOOR",
            recall_floor_passed,
            "candidate recall LCB95 must be >= 0.95",
        ),
        _gate(
            "CANARY_PAIRED_RECALL",
            paired_recall_passed,
            "candidate recall LCB95 may be at most 0.01 below paired last-known-good mean",
        ),
        _gate(
            "CANARY_ABSOLUTE_LATENCY",
            absolute_latency_passed,
            "candidate latency UCB95 must be <= 10 ms",
        ),
        _gate(
            "CANARY_RELATIVE_LATENCY",
            relative_latency_passed,
            f"candidate latency UCB95 must be <= {relative_ceiling:.2f}x paired last-known-good p95",
        ),
        _gate(
            "CANARY_EXCEPTION_RECALL_IMPROVEMENT",
            exception_recall_passed,
            "authorized exception requires candidate recall LCB95 improvement >= 0.005",
        ),
    )
    rollback_reason = None
    if not recall_floor_passed:
        rollback_reason = "RECALL_FLOOR_FAILURE"
    elif not paired_recall_passed:
        rollback_reason = "PAIRED_RECALL_DEGRADATION_FAILURE"
    elif not exception_recall_passed:
        rollback_reason = "EXCEPTION_RECALL_IMPROVEMENT_FAILURE"
    elif not absolute_latency_passed:
        rollback_reason = "ABSOLUTE_LATENCY_FAILURE"
    elif not relative_latency_passed:
        rollback_reason = "RELATIVE_LATENCY_FAILURE"
    if rollback_reason is not None:
        return _decision(
            action=PolicyAction.ROLLBACK,
            current_ef=current_ef,
            candidate_ef=observation.candidate_ef,
            last_known_good_ef=observation.last_known_good_ef,
            estimate=estimate,
            current_estimate=current_estimate,
            reason=rollback_reason,
            detector=detector,
            gates=hard_gates + slo_gates,
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )
    return _decision(
        action=PolicyAction.NO_CHANGE,
        current_ef=current_ef,
        candidate_ef=observation.candidate_ef,
        last_known_good_ef=observation.last_known_good_ef,
        estimate=estimate,
        current_estimate=current_estimate,
        reason="CANARY_PASSED",
        detector=detector,
        gates=hard_gates + slo_gates,
        mode=mode,
        audit_id=audit_id,
    )


def _pre_action_gates(
    *,
    detector: DriftDecision,
    metric: Metric,
    threshold_stratum: str,
    current_ef: int,
    candidate_ef: int,
    current_estimate: ResponseEstimate,
    candidate_estimate: ResponseEstimate,
    last_known_good_estimate: ResponseEstimate,
    pre_action: PreActionSafety,
    last_known_good: _PolicyLkgEvidence,
) -> tuple[SafetyGateResult, ...]:
    exception = _exception_applies(
        metric=metric,
        threshold_stratum=threshold_stratum,
        current_ef=current_ef,
        candidate_ef=candidate_ef,
        classification=detector.classification,
        exception_authorized=(
            pre_action.exception_authorized is True and last_known_good.ef == current_ef
        ),
    )
    relative_ceiling = (
        L2_TARGET_075_RELATIVE_LATENCY_CEILING
        if exception
        else DEFAULT_RELATIVE_LATENCY_CEILING
    )
    authority_identity_matches = bool(
        last_known_good.qualified
        and last_known_good.metric is metric
        and last_known_good.threshold_stratum == threshold_stratum
        and pre_action.index_identity == last_known_good.index_identity
        and pre_action.data_identity == last_known_good.data_identity
    )
    configuration_identity_matches = bool(
        last_known_good.source == "PHASE3"
        or pre_action.configuration_identity == last_known_good.configuration_identity
    )
    identity_matches = authority_identity_matches and configuration_identity_matches
    estimate_identity_matches = all(
        _coerce_metric(estimate.metric) is metric
        and estimate.threshold_stratum == threshold_stratum
        for estimate in (
            current_estimate,
            candidate_estimate,
            last_known_good_estimate,
        )
    )
    candidate_recall_floor = candidate_estimate.recall_lower_bound_95 >= RECALL_FLOOR
    paired_recall = (
        last_known_good_estimate.mean_recall - candidate_estimate.recall_lower_bound_95
        <= PAIRED_RECALL_DEGRADATION_LIMIT
    )
    absolute_latency = (
        candidate_estimate.latency_upper_bound_95_ms <= ABSOLUTE_LATENCY_CEILING_MS
    )
    relative_latency = (
        candidate_estimate.latency_upper_bound_95_ms
        <= relative_ceiling * last_known_good_estimate.p95_latency_ms
    )
    exception_recall = bool(
        not exception
        or candidate_estimate.recall_lower_bound_95
        - last_known_good_estimate.mean_recall
        >= EXCEPTION_MINIMUM_RECALL_IMPROVEMENT
    )
    if current_estimate.mean_recall < RECALL_FLOOR:
        improvement = (
            candidate_estimate.mean_recall - current_estimate.mean_recall
            >= MINIMUM_RECALL_IMPROVEMENT
        )
        improvement_detail = "predicted mean recall improvement must be >= 0.01"
    elif candidate_ef < current_ef:
        improvement = bool(
            current_estimate.p95_latency_ms > 0.0
            and (current_estimate.p95_latency_ms - candidate_estimate.p95_latency_ms)
            / current_estimate.p95_latency_ms
            >= MINIMUM_LATENCY_REDUCTION_FRACTION
        )
        improvement_detail = "predicted p95 latency reduction must be >= 5%"
    else:
        improvement = False
        improvement_detail = "no ADR-002 minimum-improvement gate authorizes a higher ef when current recall already meets the floor"

    validated_model_required = (
        detector.classification is DriftClassification.INPUT_DRIFT
    )
    model_valid = bool(
        not validated_model_required
        or (
            current_estimate.validated_model
            and candidate_estimate.validated_model
            and last_known_good_estimate.validated_model
            and _nonempty(pre_action.response_model_provenance)
        )
    )
    return (
        _gate(
            "CANDIDATE_IN_LADDER",
            candidate_ef in ACTUATION_LADDER,
            "candidate must be in the actuation ladder",
        ),
        _gate(
            "ADJACENT_TRANSITION",
            abs(
                ACTUATION_LADDER.index(candidate_ef)
                - ACTUATION_LADDER.index(current_ef)
            )
            == 1,
            "normal transition must move exactly one ladder step",
        ),
        _gate(
            "DRIFT_MAGNITUDE",
            detector.drift_magnitude is not None
            and detector.drift_magnitude >= DRIFT_MAGNITUDE_FLOOR,
            "drift magnitude must be >= 1.0",
        ),
        _gate(
            "METRIC_STRATUM_MATCH",
            _coerce_metric(pre_action.metric) is metric
            and pre_action.threshold_stratum == threshold_stratum
            and estimate_identity_matches,
            "all policy evidence must use the exact metric and threshold stratum",
        ),
        _gate(
            "IDENTITY_MATCH",
            identity_matches,
            "pre-action and last-known-good identities must match",
        ),
        _gate(
            "RESPONSE_MODEL_VALIDATED",
            model_valid,
            "INPUT_DRIFT requires a separately validated response model and provenance",
        ),
        _gate("MINIMUM_PREDICTED_IMPROVEMENT", improvement, improvement_detail),
        _gate(
            "RECALL_FLOOR",
            candidate_recall_floor,
            "candidate recall LCB95 must be >= 0.95",
        ),
        _gate(
            "PAIRED_RECALL_DEGRADATION",
            paired_recall,
            "candidate recall LCB95 may be at most 0.01 below last-known-good mean",
        ),
        _gate(
            "ABSOLUTE_LATENCY_CEILING",
            absolute_latency,
            "candidate latency UCB95 must be <= 10 ms",
        ),
        _gate(
            "RELATIVE_LATENCY_CEILING",
            relative_latency,
            f"candidate latency UCB95 must be <= {relative_ceiling:.2f}x last-known-good p95",
        ),
        _gate(
            "EXCEPTION_RECALL_IMPROVEMENT",
            exception_recall,
            "authorized L2:target-075 400->800 exception requires recall LCB95 improvement >= 0.005",
        ),
        _gate(
            "SERVICES_HEALTHY",
            pre_action.milvus_healthy is True
            and pre_action.etcd_healthy is True
            and pre_action.minio_healthy is True,
            "Milvus, etcd, and MinIO must be healthy",
        ),
        _gate(
            "COLLECTION_LOADED",
            pre_action.collection_loaded is True,
            "collection must be loaded",
        ),
        _gate(
            "CONFIGURATION_VALID",
            pre_action.configuration_valid is True,
            "configuration validation must pass",
        ),
        _gate(
            "INDEX_IDENTITY_UNCHANGED",
            pre_action.index_identity_unchanged is True,
            "index identity must be unchanged",
        ),
        _gate(
            "DATA_IDENTITY_UNCHANGED",
            pre_action.data_identity_unchanged is True,
            "data identity must be unchanged",
        ),
        _gate(
            "CURRENT_QUERY_HEALTH",
            all(
                _valid_count(value) and value == 0
                for value in (
                    pre_action.current_failed_query_count,
                    pre_action.current_timeout_query_count,
                    pre_action.current_threshold_violation_count,
                )
            ),
            "current failures, timeouts, and threshold violations must be integer zeroes",
        ),
        _gate(
            "SHADOW_AUDIT_COMPLETE",
            pre_action.shadow_audit_complete is True,
            "50-query paired shadow audit must be complete",
        ),
        _gate(
            "SHADOW_SEMANTICS",
            pre_action.shadow_candidate_flat_oracle_agreement is True
            and pre_action.shadow_last_known_good_flat_oracle_agreement is True,
            "candidate and last-known-good shadow results must agree with FLAT/oracle",
        ),
        _gate(
            "ROLLBACK_READY",
            pre_action.rollback_ready is True and pre_action.rollback_tested is True,
            "rollback must be available and tested for this transition",
        ),
        _gate(
            "ACTION_CLASS_AUTHORIZED",
            pre_action.action_class_authorized is True,
            "a prior dedicated EXP entry must authorize the action class",
        ),
    )


def evaluate_tuning_policy(
    detector: DriftDecision,
    *,
    current_ef: int,
    response_estimates: Mapping[int, ResponseEstimate],
    pre_action: PreActionSafety,
    canary_observation: CanaryObservation | None,
    qualification_windows: Sequence[QualificationWindow] | None = None,
    mode: PolicyMode,
    threshold_stratum: str,
    audit_id: str,
    last_known_good: QualificationResult | None = None,
    lkg_authority: LkgPhase3AuthorityPair | None = None,
) -> PolicyDecision:
    """Evaluate ADR-002 policy evidence without database or actuation access."""

    if not isinstance(mode, PolicyMode):
        raise ValueError("mode must be DRY_RUN or CANARY_ENABLED")
    if not isinstance(detector, DriftDecision):
        raise TypeError("detector must be a DriftDecision")

    estimates, estimate_reasons = _validate_response_estimates(response_estimates)
    current_estimate = estimates.get(current_ef)

    # Active-canary safety has precedence over all qualification sources.  A
    # mandatory rollback must remain available even when no D1/D2 or legacy
    # LKG evidence is supplied.
    if canary_observation is not None:
        return _evaluate_active_canary(
            detector=detector,
            current_ef=current_ef,
            observation=canary_observation,
            pre_action=pre_action,
            response_estimates=estimates,
            mode=mode,
            threshold_stratum=threshold_stratum,
            audit_id=audit_id,
        )

    if not _nonempty(audit_id):
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=None,
            last_known_good_ef=None,
            estimate=None,
            current_estimate=current_estimate,
            reason="AUDIT_ID_MISSING",
            detector=detector,
            gates=(
                _gate(
                    "AUDIT_ID_PRESENT",
                    False,
                    "inactive policy evaluation requires an immutable audit ID",
                ),
            ),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    qualification, source_reason = _inactive_lkg_source(
        mode=mode,
        lkg_authority=lkg_authority,
        qualification_windows=qualification_windows,
        last_known_good=last_known_good,
        audit_id=audit_id,
    )
    if source_reason is not None or qualification is None:
        reason = source_reason or "PHASE3_LKG_AUTHORITY_INVALID"
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=None,
            last_known_good_ef=None,
            estimate=None,
            current_estimate=current_estimate,
            reason=reason,
            detector=detector,
            gates=(
                _gate(
                    "LKG_AUTHORITY_SOURCE_VALID",
                    False,
                    "inactive policy evaluation requires exactly one permitted LKG authority source",
                ),
            ),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    if detector.state is DetectorState.NO_DRIFT:
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=None,
            last_known_good_ef=qualification.ef,
            estimate=None,
            current_estimate=current_estimate,
            reason="DETECTOR_NO_DRIFT",
            detector=detector,
            gates=(),
            mode=mode,
            audit_id=audit_id,
        )
    if detector.state is DetectorState.INSUFFICIENT_EVIDENCE:
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=None,
            last_known_good_ef=qualification.ef,
            estimate=None,
            current_estimate=current_estimate,
            reason="DETECTOR_INSUFFICIENT_EVIDENCE",
            detector=detector,
            gates=(),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )
    if detector.state is not DetectorState.DRIFT:
        raise ValueError("unsupported detector state")

    if not _is_int(current_ef) or current_ef not in ACTUATION_LADDER:
        reason = (
            "EF_100_SENTINEL_CANNOT_ACTUATE"
            if current_ef == 100
            else "CURRENT_EF_OUTSIDE_ACTUATION_LADDER"
        )
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=None,
            last_known_good_ef=qualification.ef,
            estimate=None,
            current_estimate=current_estimate,
            reason=reason,
            detector=detector,
            gates=(
                _gate(
                    "CURRENT_EF_ELIGIBLE",
                    False,
                    "serving ef must be in {200, 400, 800, 1600}",
                ),
            ),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )
    if estimate_reasons or current_estimate is None:
        reason = (
            estimate_reasons[0]
            if estimate_reasons
            else "CURRENT_RESPONSE_ESTIMATE_MISSING"
        )
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=None,
            last_known_good_ef=qualification.ef,
            estimate=None,
            current_estimate=current_estimate,
            reason=reason,
            detector=detector,
            gates=(_gate("RESPONSE_ESTIMATES_VALID", False, reason),),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    try:
        metric = _coerce_metric(pre_action.metric)
    except (TypeError, ValueError):
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=None,
            last_known_good_ef=qualification.ef,
            estimate=None,
            current_estimate=current_estimate,
            reason="PRE_ACTION_METRIC_INVALID",
            detector=detector,
            gates=(
                _gate("METRIC_VALID", False, "pre-action metric must be L2 or COSINE"),
            ),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )
    if not _valid_threshold_stratum(threshold_stratum):
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=None,
            last_known_good_ef=qualification.ef,
            estimate=None,
            current_estimate=current_estimate,
            reason="THRESHOLD_STRATUM_INVALID",
            detector=detector,
            gates=(
                _gate(
                    "THRESHOLD_STRATUM_VALID",
                    False,
                    "threshold stratum must be canonical",
                ),
            ),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    provenance_gate = _evidence_provenance_gate(
        detector=detector,
        metric=metric,
        threshold_stratum=threshold_stratum,
        pre_action=pre_action,
    )
    if not provenance_gate.passed:
        reason = (
            "EVIDENCE_PROVENANCE_MISSING"
            if detector.evidence_provenance is None
            else "EVIDENCE_PROVENANCE_MISMATCH"
        )
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=None,
            last_known_good_ef=qualification.ef,
            estimate=None,
            current_estimate=current_estimate,
            reason=reason,
            detector=detector,
            gates=(provenance_gate,),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    ladder_index = ACTUATION_LADDER.index(current_ef)
    move_higher = _quality_classification(detector.classification) or (
        detector.classification is DriftClassification.INPUT_DRIFT
        and current_estimate.mean_recall < RECALL_FLOOR
    )
    direction = 1 if move_higher else -1
    candidate_index = ladder_index + direction
    if candidate_index < 0 or candidate_index >= len(ACTUATION_LADDER):
        reason = (
            "QUALITY_SLO_UNSATISFIED_AT_MAX_EF"
            if move_higher and current_ef == ACTUATION_LADDER[-1]
            else "NO_ADJACENT_CANDIDATE_FOR_OBJECTIVE"
        )
        return _decision(
            action=PolicyAction.NO_CHANGE,
            current_ef=current_ef,
            candidate_ef=None,
            last_known_good_ef=qualification.ef,
            estimate=None,
            current_estimate=current_estimate,
            reason=reason,
            detector=detector,
            gates=(
                _gate(
                    "ADJACENT_CANDIDATE_AVAILABLE",
                    False,
                    "objective has no adjacent candidate in the ladder",
                ),
            ),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    candidate_ef = ACTUATION_LADDER[candidate_index]
    candidate_estimate = estimates.get(candidate_ef)
    last_known_good_estimate = (
        estimates.get(qualification.ef) if qualification.ef is not None else None
    )
    if candidate_estimate is None or last_known_good_estimate is None:
        reason = (
            "CANDIDATE_RESPONSE_ESTIMATE_MISSING"
            if candidate_estimate is None
            else "LAST_KNOWN_GOOD_RESPONSE_ESTIMATE_MISSING"
        )
        return _decision(
            action=PolicyAction.RECOMMEND_EF,
            current_ef=current_ef,
            candidate_ef=candidate_ef,
            last_known_good_ef=qualification.ef,
            estimate=candidate_estimate,
            current_estimate=current_estimate,
            reason=reason,
            detector=detector,
            gates=(_gate("REQUIRED_RESPONSE_ESTIMATES_PRESENT", False, reason),),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )
    if not qualification.qualified:
        reason = (
            qualification.reasons[0]
            if qualification.reasons
            else "LAST_KNOWN_GOOD_NOT_QUALIFIED"
        )
        return _decision(
            action=PolicyAction.RECOMMEND_EF,
            current_ef=current_ef,
            candidate_ef=candidate_ef,
            last_known_good_ef=None,
            estimate=candidate_estimate,
            current_estimate=current_estimate,
            reason=reason,
            detector=detector,
            gates=(_gate("LAST_KNOWN_GOOD_QUALIFIED", False, reason),),
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )

    gates = _pre_action_gates(
        detector=detector,
        metric=metric,
        threshold_stratum=threshold_stratum,
        current_ef=current_ef,
        candidate_ef=candidate_ef,
        current_estimate=current_estimate,
        candidate_estimate=candidate_estimate,
        last_known_good_estimate=last_known_good_estimate,
        pre_action=pre_action,
        last_known_good=qualification,
    )
    failed_gate = next((gate for gate in gates if not gate.passed), None)
    if failed_gate is not None:
        return _decision(
            action=PolicyAction.RECOMMEND_EF,
            current_ef=current_ef,
            candidate_ef=candidate_ef,
            last_known_good_ef=qualification.ef,
            estimate=candidate_estimate,
            current_estimate=current_estimate,
            reason=f"SAFETY_GATE_FAILED:{failed_gate.name}",
            detector=detector,
            gates=gates,
            mode=mode,
            audit_id=audit_id,
            alert_required=True,
        )
    if mode is PolicyMode.DRY_RUN:
        return _decision(
            action=PolicyAction.RECOMMEND_EF,
            current_ef=current_ef,
            candidate_ef=candidate_ef,
            last_known_good_ef=qualification.ef,
            estimate=candidate_estimate,
            current_estimate=current_estimate,
            reason="DRY_RUN_RECOMMENDATION",
            detector=detector,
            gates=gates,
            mode=mode,
            audit_id=audit_id,
        )
    return _decision(
        action=PolicyAction.START_CANARY,
        current_ef=current_ef,
        candidate_ef=candidate_ef,
        last_known_good_ef=qualification.ef,
        estimate=candidate_estimate,
        current_estimate=current_estimate,
        reason="SAFETY_GATES_PASSED",
        detector=detector,
        gates=gates,
        mode=mode,
        audit_id=audit_id,
    )


__all__ = [
    "ABSOLUTE_LATENCY_CEILING_MS",
    "ACTUATION_LADDER",
    "CanaryObservation",
    "PolicyAction",
    "PolicyDecision",
    "PolicyMode",
    "PreActionSafety",
    "QualificationResult",
    "QualificationWindow",
    "RECALL_FLOOR",
    "ResponseEstimate",
    "SafetyGateResult",
    "evaluate_tuning_policy",
    "qualify_last_known_good",
]
