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

from dataclasses import dataclass, fields, replace
from datetime import datetime, timezone
import hashlib
import math
import re
import unicodedata

from .artifacts import canonical_json_bytes
from .canary_route_state import RouteStateBinding
from .canary_runtime_types import Stage4RuntimeReadiness
from .canary_routing import CanaryRoutePlan, build_canary_route_plan
from .canary_schedule import (
    Stage4ExecutionSchedule,
    build_stage4_execution_schedule,
)
from .canary_stage4_evidence_binding import Stage4EvidenceBinding
from .canary_workload import (
    CandidateSelectionRecord,
    EligibleWorkloadManifest,
    WorkloadIdentityBinding,
)
from .config import IndexTrack, Metric, SearchConfiguration
from .drift import evidence_provenance_valid
from .lkg_phase3_binding import (
    LkgPhase3AuthorityPair,
    bind_lkg_phase3_authority,
)
from .policy import (
    PolicyAction,
    PolicyDecision,
    PolicyMode,
    SafetyGateResult,
)
from .search_configuration_digest import search_configuration_sha256


__all__ = [
    "Stage4AdmissionReceipt",
    "Stage4AdmissionRequest",
    "Stage4AdmissionResult",
    "Stage4LkgAuthorityPair",
    "Stage4RepositoryEvidence",
    "Stage4RuntimeReadiness",
    "bind_stage4_lkg_authority",
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
_RECEIPT_SCHEMA_VERSION = "stage4-admission-receipt-v1"
_RECEIPT_HASH_DOMAIN = b"vdbench.stage4-admission-receipt.v1\0"
_RECEIPT_CONSTRUCTION_TOKEN = object()


@dataclass(frozen=True, slots=True)
class Stage4RepositoryEvidence:
    """A composition-root attestation of the exact clean source revision."""

    commit_sha: str
    clean: bool
    observed_at_utc: str


# Narrow compatibility aliases.  The neutral module owns the sole real pair
# implementation and validator.
Stage4LkgAuthorityPair = LkgPhase3AuthorityPair
bind_stage4_lkg_authority = bind_lkg_phase3_authority


@dataclass(frozen=True, slots=True)
class Stage4AdmissionRequest:
    """All immutable prerequisites for one exact Stage-4 admission decision."""

    manifest: EligibleWorkloadManifest
    selection: CandidateSelectionRecord
    plan: CanaryRoutePlan
    schedule: Stage4ExecutionSchedule
    policy_decision: PolicyDecision
    lkg_authority: LkgPhase3AuthorityPair
    evidence_binding: Stage4EvidenceBinding
    repository: Stage4RepositoryEvidence
    runtime: Stage4RuntimeReadiness


@dataclass(frozen=True, slots=True, init=False)
class Stage4AdmissionReceipt:
    """Canonical passing receipt; private construction is API discipline only."""

    receipt_schema_version: str
    checkpoint_c_evaluation_digest: str
    d2_canonical_record_digest: str
    d2_sequence_number: int
    d2_persisted_at_utc: str
    source_run_id: str
    source_run_binding_sha256: str
    source_run_seal_digest: str
    source_sealed_phase1_chain_head_sha256: str
    phase2_source_binding_digest: str
    evaluated_lkg_ef: int
    lkg_search_configuration_digest: str
    stage4_evidence_binding_sha256: str
    route_plan_sha256: str
    execution_schedule_sha256: str
    policy_audit_id: str
    repository_commit_sha: str
    configuration_identity: str
    data_identity: str
    hnsw_identity: str
    lkg_source_revision: str
    runtime_observed_at_utc: str
    canonical_receipt_digest: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "Stage4AdmissionReceipt can only be created by successful "
            "evaluate_stage4_admission()"
        )

    @classmethod
    def _from_validated(
        cls,
        *,
        payload: dict[str, object],
        construction_token: object,
    ) -> Stage4AdmissionReceipt:
        if construction_token is not _RECEIPT_CONSTRUCTION_TOKEN:
            raise TypeError("Stage-4 admission receipt construction token is invalid")
        value = object.__new__(cls)
        for field, field_value in payload.items():
            object.__setattr__(value, field, field_value)
        object.__setattr__(
            value,
            "canonical_receipt_digest",
            hashlib.sha256(
                _RECEIPT_HASH_DOMAIN + canonical_json_bytes(payload)
            ).hexdigest(),
        )
        return value

    def to_document(self) -> dict[str, object]:
        document = self._payload()
        document["canonical_receipt_digest"] = self.canonical_receipt_digest
        return document

    def matches_canonical_digest(self) -> bool:
        """Verify the schema-pinned digest before a later consumer trusts it."""

        try:
            return (
                self.receipt_schema_version == _RECEIPT_SCHEMA_VERSION
                and self.canonical_receipt_digest
                == hashlib.sha256(
                    _RECEIPT_HASH_DOMAIN + canonical_json_bytes(self._payload())
                ).hexdigest()
            )
        except (AttributeError, TypeError, ValueError, OverflowError, UnicodeError):
            return False

    def stable_lineage_matches(self, other: object) -> bool:
        """Compare only the accepted D3 stable lineage of canonical receipts."""

        return bool(
            type(other) is Stage4AdmissionReceipt
            and self.matches_canonical_digest()
            and other.matches_canonical_digest()
            and self._stable_lineage() == other._stable_lineage()
        )

    def _stable_lineage(self) -> tuple[object, ...]:
        return (
            self.checkpoint_c_evaluation_digest,
            self.d2_canonical_record_digest,
            self.d2_sequence_number,
            self.stage4_evidence_binding_sha256,
            self.execution_schedule_sha256,
            self.route_plan_sha256,
            self.policy_audit_id,
            self.configuration_identity,
            self.data_identity,
            self.hnsw_identity,
            self.evaluated_lkg_ef,
            self.lkg_search_configuration_digest,
        )

    def _payload(self) -> dict[str, object]:
        return {
            field: getattr(self, field)
            for field in self.__dataclass_fields__
            if field != "canonical_receipt_digest"
        }


@dataclass(frozen=True, slots=True)
class Stage4AdmissionResult:
    """Non-authorizing envelope: only a private receipt represents success."""

    receipt: Stage4AdmissionReceipt | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.reason_codes, tuple):
            raise TypeError("reason_codes must be a tuple")
        if any(_REASON.fullmatch(code) is None for code in self.reason_codes):
            raise ValueError("reason_codes must contain canonical stable codes")
        if self.reason_codes != tuple(dict.fromkeys(self.reason_codes)):
            raise ValueError("reason_codes must be unique and ordered")
        if self.receipt is None:
            if not self.reason_codes:
                raise ValueError("a refusal requires at least one reason code")
        elif (
            type(self.receipt) is not Stage4AdmissionReceipt
            or self.reason_codes
            or not self.receipt.matches_canonical_digest()
        ):
            raise ValueError(
                "a passing result requires one canonical receipt and no reasons"
            )

    @property
    def admitted(self) -> bool:
        return self.receipt is not None

    @property
    def plan_sha256(self) -> str | None:
        return None if self.receipt is None else self.receipt.route_plan_sha256

    @property
    def policy_audit_id(self) -> str | None:
        return None if self.receipt is None else self.receipt.policy_audit_id

    @property
    def repository_commit_sha(self) -> str | None:
        return None if self.receipt is None else self.receipt.repository_commit_sha


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
    validated_binding = _revalidate_evidence_binding(
        request.evidence_binding, reasons
    )
    _validate_repository(request.repository, reasons)
    if plan is not None:
        _validate_frozen_transition(plan, reasons)
        schedule = _validate_schedule(request, plan, reasons)
        _validate_policy(request.policy_decision, plan, reasons)
        pair = _validate_lkg_pair(request.lkg_authority, reasons)
        if pair is not None and validated_binding is not None:
            _validate_configuration_bridge(
                pair=pair,
                binding=validated_binding,
                manifest=request.manifest,
                plan=plan,
                schedule=schedule,
                reasons=reasons,
            )
        _validate_runtime(request.runtime, plan, reasons)
    else:
        schedule = None
        pair = None
        _append_once(reasons, "ARTIFACT_BINDING_INVALID")

    if reasons:
        return Stage4AdmissionResult(receipt=None, reason_codes=tuple(reasons))
    assert plan is not None
    assert schedule is not None
    assert pair is not None
    assert validated_binding is not None
    assert isinstance(request.policy_decision, PolicyDecision)
    assert isinstance(request.repository, Stage4RepositoryEvidence)
    assert isinstance(request.runtime, Stage4RuntimeReadiness)
    return Stage4AdmissionResult(
        receipt=_build_receipt(
            pair=pair,
            binding=validated_binding,
            plan=plan,
            schedule=schedule,
            policy=request.policy_decision,
            repository=request.repository,
            runtime=request.runtime,
        ),
        reason_codes=(),
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


def _validate_schedule(
    request: Stage4AdmissionRequest,
    plan: CanaryRoutePlan,
    reasons: list[str],
) -> Stage4ExecutionSchedule | None:
    if type(request.schedule) is not Stage4ExecutionSchedule:
        _append_once(reasons, "EXECUTION_SCHEDULE_INVALID")
        return None
    try:
        rebuilt = build_stage4_execution_schedule(request.manifest, plan)
    except (TypeError, ValueError):
        _append_once(reasons, "EXECUTION_SCHEDULE_REBUILD_FAILED")
        return None
    if request.schedule != rebuilt:
        _append_once(reasons, "EXECUTION_SCHEDULE_REBUILD_MISMATCH")
        return None
    if request.schedule.plan_sha256 != plan.plan_sha256:
        _append_once(reasons, "EXECUTION_SCHEDULE_PLAN_MISMATCH")
        return None
    return rebuilt


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


def _validate_lkg_pair(
    value: object,
    reasons: list[str],
) -> LkgPhase3AuthorityPair | None:
    if type(value) is not LkgPhase3AuthorityPair:
        _append_once(reasons, "PHASE3_LKG_AUTHORITY_PAIR_INVALID")
        return None
    try:
        rebound = bind_lkg_phase3_authority(
            authority=value.authority,
            verified_latest_reference=value.verified_latest_reference,
        )
    except (TypeError, ValueError):
        _append_once(reasons, "PHASE3_LKG_AUTHORITY_PAIR_INVALID")
        return None
    return rebound


def _revalidate_evidence_binding(
    value: object,
    reasons: list[str],
) -> Stage4EvidenceBinding | None:
    """Replay the complete public constructor contract on every declared field."""

    if type(value) is not Stage4EvidenceBinding:
        _append_once(reasons, "STAGE4_EVIDENCE_BINDING_INVALID")
        return None
    try:
        reconstructed = Stage4EvidenceBinding(
            **{
                field.name: getattr(value, field.name)
                for field in fields(Stage4EvidenceBinding)
            }
        )
    except (AttributeError, TypeError, ValueError):
        _append_once(reasons, "STAGE4_EVIDENCE_BINDING_INVALID")
        return None
    if reconstructed != value:
        _append_once(reasons, "STAGE4_EVIDENCE_BINDING_INVALID")
        return None
    return reconstructed


def _validate_configuration_bridge(
    *,
    pair: LkgPhase3AuthorityPair,
    binding: Stage4EvidenceBinding,
    manifest: EligibleWorkloadManifest,
    plan: CanaryRoutePlan,
    schedule: Stage4ExecutionSchedule | None,
    reasons: list[str],
) -> None:
    expected_identity = WorkloadIdentityBinding(
        configuration_identity=plan.configuration_identity,
        data_identity=plan.data_identity,
        flat_binding_id=plan.flat_binding_id,
        hnsw_binding_id=plan.hnsw_binding_id,
    )
    if (
        binding.metric is not plan.metric
        or binding.threshold_stratum != plan.threshold_stratum
        or binding.current_ef != plan.last_known_good_ef
        or binding.last_known_good_ef != plan.last_known_good_ef
        or binding.candidate_ef != plan.candidate_ef
        or binding.eligible_workload_sha256 != plan.eligible_workload_sha256
        or binding.candidate_selection_sha256 != plan.candidate_selection_sha256
        or binding.identity != expected_identity
        or binding.dataset002_manifest_sha256 != manifest.dataset002_manifest_sha256
    ):
        _append_once(reasons, "STAGE4_EVIDENCE_BINDING_MISMATCH")
    if schedule is None or binding.execution_schedule_sha256 != schedule.schedule_sha256:
        _append_once(reasons, "STAGE4_EVIDENCE_SCHEDULE_MISMATCH")

    authority = pair.authority
    configuration = authority.search_configuration
    try:
        if type(configuration) is not SearchConfiguration:
            raise TypeError("LKG search configuration is not concrete")
        configuration.validate()
        if search_configuration_sha256(configuration) != authority.search_configuration_digest:
            raise ValueError("LKG search configuration digest mismatch")
        if authority.run_binding.sha256 != authority.source_run_binding_sha256:
            raise ValueError("LKG source run-binding digest mismatch")
    except (TypeError, ValueError):
        _append_once(reasons, "PHASE3_LKG_SEARCH_CONFIGURATION_INVALID")
        return
    if (
        configuration.index_track is not IndexTrack.HNSW
        or configuration.ef != plan.last_known_good_ef
        or configuration.metric is not plan.metric
        or configuration.threshold_label != plan.threshold_stratum
        or authority.evaluated_ef != plan.last_known_good_ef
        or authority.metric is not plan.metric
        or authority.threshold_stratum != plan.threshold_stratum
        or authority.data_identity != plan.data_identity
        or authority.index_identity != plan.hnsw_binding_id
    ):
        _append_once(reasons, "PHASE3_LKG_PLAN_BINDING_MISMATCH")
    try:
        candidate_configuration = replace(configuration, ef=plan.candidate_ef)
        candidate_configuration.validate()
    except (TypeError, ValueError):
        _append_once(reasons, "CANDIDATE_SEARCH_CONFIGURATION_INVALID")
        return
    if binding.candidate_search_configuration != candidate_configuration:
        _append_once(reasons, "CANDIDATE_SEARCH_CONFIGURATION_MISMATCH")
    if any(
        occurrence.threshold_radius != configuration.radius
        or occurrence.range_filter != configuration.range_filter
        or occurrence.limit != configuration.limit
        for occurrence in plan.occurrences
    ):
        _append_once(reasons, "ROUTING_SEARCH_CONTRACT_MISMATCH")


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


def _build_receipt(
    *,
    pair: LkgPhase3AuthorityPair,
    binding: Stage4EvidenceBinding,
    plan: CanaryRoutePlan,
    schedule: Stage4ExecutionSchedule,
    policy: PolicyDecision,
    repository: Stage4RepositoryEvidence,
    runtime: Stage4RuntimeReadiness,
) -> Stage4AdmissionReceipt:
    authority = pair.authority
    reference = pair.verified_latest_reference.reference
    payload: dict[str, object] = {
        "receipt_schema_version": _RECEIPT_SCHEMA_VERSION,
        "checkpoint_c_evaluation_digest": authority.canonical_evaluation_digest,
        "d2_canonical_record_digest": reference.canonical_record_digest,
        "d2_sequence_number": reference.sequence_number,
        "d2_persisted_at_utc": reference.persisted_at_utc,
        "source_run_id": authority.source_run_id,
        "source_run_binding_sha256": authority.source_run_binding_sha256,
        "source_run_seal_digest": authority.source_run_seal_digest,
        "source_sealed_phase1_chain_head_sha256": (
            authority.source_sealed_phase1_chain_head_sha256
        ),
        "phase2_source_binding_digest": authority.phase2_source_binding_digest,
        "evaluated_lkg_ef": authority.evaluated_ef,
        "lkg_search_configuration_digest": authority.search_configuration_digest,
        "stage4_evidence_binding_sha256": binding.sha256,
        "route_plan_sha256": plan.plan_sha256,
        "execution_schedule_sha256": schedule.schedule_sha256,
        "policy_audit_id": policy.audit_id,
        "repository_commit_sha": repository.commit_sha,
        "configuration_identity": plan.configuration_identity,
        "data_identity": plan.data_identity,
        "hnsw_identity": plan.hnsw_binding_id,
        "lkg_source_revision": authority.source_revision,
        "runtime_observed_at_utc": runtime.observed_at_utc,
    }
    return Stage4AdmissionReceipt._from_validated(
        payload=payload,
        construction_token=_RECEIPT_CONSTRUCTION_TOKEN,
    )


def _result(reason: str) -> Stage4AdmissionResult:
    return Stage4AdmissionResult(receipt=None, reason_codes=(reason,))


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
