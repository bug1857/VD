"""Fail-closed, non-actuating admission checks for EXP-009 Stage 4.

Purpose:
    Bind the immutable Stage-1 workload/selection/route plan to the exact
    policy, LKG, repository, and runtime-readiness evidence required before a
    human-approved live reference canary may be *considered* for activation.
Inputs:
    Strict value objects produced by the verified Stage-1/2/3 boundaries and
    an independently collected read-only runtime-preflight result.
Outputs:
    An immutable admission receipt or stable non-sensitive refusal codes.
Dependencies:
    Offline workload, routing, policy, provenance, and route-binding values.
    This module intentionally has no Milvus, activation, route-authority,
    rollback, audit-persistence, networking, or filesystem dependency.
Failure modes:
    Every malformed, stale, incomplete, or mismatched prerequisite refuses
    before any side effect. A passing receipt is neither a grant nor a token;
    a later composition root must still verify and reserve an external grant.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
import re
import unicodedata

from .canary_route_state import RouteStateBinding
from .canary_routing import CanaryRoutePlan, build_canary_route_plan
from .canary_workload import CandidateSelectionRecord, EligibleWorkloadManifest
from .config import Metric
from .drift import evidence_provenance_valid
from .policy import (
    PolicyAction,
    PolicyDecision,
    PolicyMode,
    QualificationResult,
    SafetyGateResult,
)


__all__ = [
    "Stage4AdmissionRequest",
    "Stage4AdmissionResult",
    "Stage4RepositoryEvidence",
    "Stage4RuntimeReadiness",
    "evaluate_stage4_admission",
]


_COMMIT_SHA = re.compile(r"[0-9a-f]{40}\Z")
_UTC = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_REASON = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_EXPERIMENT_METRIC = Metric.L2
_EXPERIMENT_STRATUM = "target-075"
_EXPERIMENT_LKG_EF = 400
_EXPERIMENT_CANDIDATE_EF = 800
_RECALL_FLOOR = 0.95
_MAX_LATENCY_MS = 10.0
_EXCEPTION_MINIMUM_RECALL_IMPROVEMENT = 0.005


@dataclass(frozen=True, slots=True)
class Stage4RepositoryEvidence:
    """A composition-root attestation of the exact clean source revision."""

    commit_sha: str
    clean: bool
    observed_at_utc: str


@dataclass(frozen=True, slots=True)
class Stage4RuntimeReadiness:
    """Read-only serving readiness mapped from the live preflight adapter.

    The composition root must create this only after it runs the real health,
    load-state, and exact-identity checks for the serving plan.  Keeping this
    small value object here prevents the admission boundary from importing or
    executing a Milvus client itself.
    """

    binding: RouteStateBinding
    serving_preflight_complete: bool
    observed_at_utc: str
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Stage4AdmissionRequest:
    """All immutable prerequisites for one exact Stage-4 admission decision."""

    manifest: EligibleWorkloadManifest
    selection: CandidateSelectionRecord
    plan: CanaryRoutePlan
    policy_decision: PolicyDecision
    qualification: QualificationResult
    repository: Stage4RepositoryEvidence
    runtime: Stage4RuntimeReadiness


@dataclass(frozen=True, slots=True)
class Stage4AdmissionResult:
    """Non-sensitive result that cannot authorize, install, or claim a route."""

    admitted: bool
    reason_codes: tuple[str, ...]
    plan_sha256: str | None
    policy_audit_id: str | None
    repository_commit_sha: str | None


def evaluate_stage4_admission(request: object) -> Stage4AdmissionResult:
    """Fail closed on every missing or mismatched Stage-4 prerequisite.

    This is intentionally a value-only validation boundary. It must be called
    after a composition root has rebuilt the manifest/selection from durable
    artifacts, and again immediately before a later activation coordinator
    validates a real external grant. It neither validates that grant nor has
    a dependency capable of dispatching a query.
    """

    if not isinstance(request, Stage4AdmissionRequest):
        return _result("ADMISSION_REQUEST_INVALID")

    reasons: list[str] = []
    plan = _validate_artifact_binding(request, reasons)
    _validate_repository(request.repository, reasons)
    if plan is not None:
        _validate_frozen_transition(plan, reasons)
        _validate_policy(request.policy_decision, plan, reasons)
        _validate_qualification(request.qualification, plan, reasons)
        _validate_runtime(request.runtime, plan, reasons)
    else:
        _append_once(reasons, "ARTIFACT_BINDING_INVALID")

    return Stage4AdmissionResult(
        admitted=not reasons,
        reason_codes=tuple(reasons),
        plan_sha256=plan.plan_sha256 if plan is not None else None,
        policy_audit_id=(
            request.policy_decision.audit_id
            if isinstance(request.policy_decision, PolicyDecision)
            else None
        ),
        repository_commit_sha=(
            request.repository.commit_sha
            if isinstance(request.repository, Stage4RepositoryEvidence)
            else None
        ),
    )


def _validate_artifact_binding(
    request: Stage4AdmissionRequest, reasons: list[str]
) -> CanaryRoutePlan | None:
    """Rebuild the sole accepted partition from immutable value contracts."""

    if not isinstance(request.manifest, EligibleWorkloadManifest):
        _append_once(reasons, "WORKLOAD_MANIFEST_INVALID")
        return None
    if not isinstance(request.selection, CandidateSelectionRecord):
        _append_once(reasons, "CANDIDATE_SELECTION_INVALID")
        return None
    if not isinstance(request.plan, CanaryRoutePlan):
        _append_once(reasons, "ROUTE_PLAN_INVALID")
        return None
    try:
        request.manifest.validate()
        request.selection.validate()
        rebuilt = build_canary_route_plan(request.manifest, request.selection)
    except Exception:
        _append_once(reasons, "WORKLOAD_OR_SELECTION_VERIFICATION_FAILED")
        return None
    if request.plan != rebuilt:
        _append_once(reasons, "PLAN_REBUILD_MISMATCH")
        return None
    return rebuilt


def _validate_frozen_transition(plan: CanaryRoutePlan, reasons: list[str]) -> None:
    if (
        plan.metric is not _EXPERIMENT_METRIC
        or plan.threshold_stratum != _EXPERIMENT_STRATUM
        or plan.last_known_good_ef != _EXPERIMENT_LKG_EF
        or plan.candidate_ef != _EXPERIMENT_CANDIDATE_EF
        or plan.population_count != 600
        or plan.candidate_count != 60
    ):
        _append_once(reasons, "FROZEN_TRANSITION_MISMATCH")


def _validate_repository(
    value: object, reasons: list[str]
) -> None:
    if not isinstance(value, Stage4RepositoryEvidence):
        _append_once(reasons, "REPOSITORY_EVIDENCE_INVALID")
        return
    if not isinstance(value.commit_sha, str) or _COMMIT_SHA.fullmatch(value.commit_sha) is None:
        _append_once(reasons, "REPOSITORY_REVISION_INVALID")
    if value.clean is not True:
        _append_once(reasons, "REPOSITORY_NOT_CLEAN")
    if not _valid_utc(value.observed_at_utc):
        _append_once(reasons, "REPOSITORY_TIMESTAMP_INVALID")


def _validate_policy(
    decision: object, plan: CanaryRoutePlan, reasons: list[str]
) -> None:
    if not isinstance(decision, PolicyDecision):
        _append_once(reasons, "POLICY_DECISION_INVALID")
        return
    if decision.action is not PolicyAction.START_CANARY:
        _append_once(reasons, "POLICY_ACTION_INVALID")
    if decision.mode is not PolicyMode.CANARY_ENABLED:
        _append_once(reasons, "POLICY_MODE_INVALID")
    if (
        decision.current_ef != plan.last_known_good_ef
        or decision.candidate_ef != plan.candidate_ef
        or decision.last_known_good_ef != plan.last_known_good_ef
    ):
        _append_once(reasons, "POLICY_TRANSITION_MISMATCH")
    if not _canonical_text(decision.audit_id):
        _append_once(reasons, "POLICY_AUDIT_ID_INVALID")
    if not _safety_gates_all_passed(decision.safety_gate_results):
        _append_once(reasons, "POLICY_SAFETY_GATES_FAILED")
    provenance = decision.evidence_provenance
    if not evidence_provenance_valid(provenance):
        _append_once(reasons, "POLICY_PROVENANCE_INVALID")
    elif (
        provenance.metric is not plan.metric
        or provenance.threshold_stratum != plan.threshold_stratum
        or provenance.configuration_identity != plan.configuration_identity
        or provenance.data_identity != plan.data_identity
        or provenance.flat_binding_id != plan.flat_binding_id
        or provenance.hnsw_binding_id != plan.hnsw_binding_id
    ):
        _append_once(reasons, "POLICY_PROVENANCE_BINDING_MISMATCH")
    _validate_policy_bounds(decision, reasons)


def _validate_policy_bounds(decision: PolicyDecision, reasons: list[str]) -> None:
    values = (
        decision.expected_mean_recall,
        decision.expected_recall_lower_bound_95,
        decision.expected_p95_latency_ms,
        decision.expected_latency_upper_bound_95_ms,
        decision.predicted_recall_improvement,
    )
    if not all(_finite(value) for value in values):
        _append_once(reasons, "POLICY_BOUND_EVIDENCE_INVALID")
        return
    expected_mean_recall = float(decision.expected_mean_recall)
    expected_recall_lower_bound = float(decision.expected_recall_lower_bound_95)
    expected_p95_latency = float(decision.expected_p95_latency_ms)
    expected_latency_upper_bound = float(
        decision.expected_latency_upper_bound_95_ms
    )
    predicted_recall_improvement = float(decision.predicted_recall_improvement)
    if not (
        0.0
        <= expected_recall_lower_bound
        <= expected_mean_recall
        <= 1.0
        and 0.0
        <= expected_p95_latency
        <= expected_latency_upper_bound
    ):
        _append_once(reasons, "POLICY_BOUND_EVIDENCE_INVALID")
    if expected_recall_lower_bound < _RECALL_FLOOR:
        _append_once(reasons, "POLICY_RECALL_FLOOR_UNMET")
    if expected_latency_upper_bound > _MAX_LATENCY_MS:
        _append_once(reasons, "POLICY_LATENCY_CEILING_EXCEEDED")
    if predicted_recall_improvement < _EXCEPTION_MINIMUM_RECALL_IMPROVEMENT:
        _append_once(reasons, "POLICY_EXCEPTION_IMPROVEMENT_UNMET")


def _validate_qualification(
    value: object, plan: CanaryRoutePlan, reasons: list[str]
) -> None:
    if not isinstance(value, QualificationResult) or value.qualified is not True:
        _append_once(reasons, "LAST_KNOWN_GOOD_NOT_QUALIFIED")
        return
    if not isinstance(value.reasons, tuple) or value.reasons:
        _append_once(reasons, "LAST_KNOWN_GOOD_REASONS_PRESENT")
    if (
        value.ef != plan.last_known_good_ef
        or value.metric is not plan.metric
        or value.threshold_stratum != plan.threshold_stratum
        or value.configuration_identity != plan.configuration_identity
        or value.index_identity != plan.hnsw_binding_id
        or value.data_identity != plan.data_identity
    ):
        _append_once(reasons, "LAST_KNOWN_GOOD_BINDING_MISMATCH")
    identifiers = value.qualifying_window_ids
    if (
        not isinstance(identifiers, tuple)
        or len(identifiers) != 2
        or identifiers[0] == identifiers[1]
        or any(not _canonical_text(item) for item in identifiers)
    ):
        _append_once(reasons, "LAST_KNOWN_GOOD_WINDOW_IDS_INVALID")


def _validate_runtime(
    value: object, plan: CanaryRoutePlan, reasons: list[str]
) -> None:
    if not isinstance(value, Stage4RuntimeReadiness):
        _append_once(reasons, "RUNTIME_EVIDENCE_INVALID")
        return
    if value.serving_preflight_complete is not True:
        _append_once(reasons, "RUNTIME_PREFLIGHT_INCOMPLETE")
    if not _valid_utc(value.observed_at_utc):
        _append_once(reasons, "RUNTIME_TIMESTAMP_INVALID")
    if (
        not isinstance(value.reason_codes, tuple)
        or any(not isinstance(code, str) or _REASON.fullmatch(code) is None for code in value.reason_codes)
        or (value.serving_preflight_complete is True and value.reason_codes)
    ):
        _append_once(reasons, "RUNTIME_REASON_CODES_INVALID")
    binding = value.binding
    expected = RouteStateBinding(
        metric=plan.metric,
        threshold_stratum=plan.threshold_stratum,
        last_known_good_ef=plan.last_known_good_ef,
        configuration_identity=plan.configuration_identity,
        data_identity=plan.data_identity,
        flat_binding_id=plan.flat_binding_id,
        hnsw_binding_id=plan.hnsw_binding_id,
    )
    if not isinstance(binding, RouteStateBinding) or binding != expected:
        _append_once(reasons, "RUNTIME_BINDING_MISMATCH")


def _result(reason: str) -> Stage4AdmissionResult:
    return Stage4AdmissionResult(False, (reason,), None, None, None)


def _append_once(reasons: list[str], code: str) -> None:
    if code not in reasons:
        reasons.append(code)


def _canonical_text(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and value == unicodedata.normalize("NFC", value)
        and all(ord(character) >= 0x20 and ord(character) != 0x7F for character in value)
    )


def _safety_gates_all_passed(value: object) -> bool:
    return bool(
        isinstance(value, tuple)
        and value
        and all(
            isinstance(gate, SafetyGateResult)
            and gate.passed is True
            and _canonical_text(gate.name)
            and _canonical_text(gate.detail)
            for gate in value
        )
    )


def _finite(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _valid_utc(value: object) -> bool:
    if not isinstance(value, str) or _UTC.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() == timezone.utc.utcoffset(parsed)
