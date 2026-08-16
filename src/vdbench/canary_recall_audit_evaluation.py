"""Pure evaluation of EXP-009's 1,200-query recall-audit evidence.

Purpose:
    Compose completeness/binding checks with the existing, unmodified
    ``one_sided_hoeffding_recall_lower_bound`` to decide whether real
    background recall-audit evidence supports ADR-008's mean-capped-recall
    floor for exactly one authorized configuration/identity contract.
Inputs:
    The frozen DATASET-002 recall-audit query-ID population, the exact
    ``SearchConfiguration``/``WorkloadIdentityBinding`` contract under
    evaluation, and durable observations already read from
    ``CanaryRecallAuditLedger``.
Outputs:
    An explicit pass/fail/incomplete result with reason codes; never a
    fabricated benign value on incomplete or inconsistent evidence.
Dependencies:
    Pure ``canary_statistics`` and ``canary_recall_audit_ledger`` value
    objects only. No Milvus, PyMilvus, network, or live-execution import.
Failure modes:
    Any non-1200 sample count, duplicate/foreign query ID, or mismatched
    configuration/identity/manifest context yields an explicit
    ``EvaluationStatus.INCOMPLETE`` result.
Scope:
    This module never touches ``canary_schedule_evaluation.py`` or its
    ``recall_bound_evaluated`` field, which remains latency-only and
    unconditionally ``False`` by that module's own, unchanged, honest scope
    statement. Every reason code here is explanatory evidence only; the
    authoritative pass/fail/incomplete signal is the typed ``status`` field,
    set directly at each return point in ``evaluate_recall_audit_evidence``'s
    own control flow -- never reverse-engineered from reason-code strings by
    a later, external classifier.

    ``Stage4RecallAuditReport`` is a recall-only status. It must never be
    read as a complete Stage-4 qualification claim: that requires combining
    it with latency evidence via ``canary_stage4_decision.Stage4Decision``.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from .artifacts import canonical_json_bytes
from .canary_recall_audit_ledger import RecallAuditObservation
from .canary_stage4_evidence_binding import Stage4EvidenceBinding
from .canary_statistics import (
    EXP009_RECALL_AUDIT_COUNT,
    one_sided_hoeffding_recall_lower_bound,
)

__all__ = [
    "RECALL_AUDIT_EVALUATOR_VERSION",
    "EvaluationStatus",
    "Stage4RecallAuditEvaluation",
    "Stage4RecallAuditReport",
    "build_recall_audit_report",
    "evaluate_recall_audit_evidence",
]


RECALL_AUDIT_EVALUATOR_VERSION = "recall-audit-hoeffding-1200-v1"

_ALPHA = 0.05


class EvaluationStatus(StrEnum):
    """The only three outcomes any Stage-4 evidence evaluation may report."""

    PASSING = "PASSING"
    FAILING = "FAILING"
    INCOMPLETE = "INCOMPLETE"


@dataclass(frozen=True, slots=True)
class Stage4RecallAuditEvaluation:
    """A non-IID-claiming result over DATASET-002's fixed 1,200-query audit set.

    ``recall_audit_complete_and_passing`` is never a live-interference, canary
    no-interference, or production-traffic claim. It is true only when every
    completeness/binding check below passes and the Hoeffding lower bound
    clears the required floor. ``status`` is the authoritative signal for any
    downstream combination; ``reason_codes`` is explanatory only.
    """

    recall_audit_complete_and_passing: bool
    reason_codes: tuple[str, ...]
    sample_count: int
    observed_mean: float | None
    margin: float | None
    lower_bound: float | None
    confidence_level: float
    recall_floor: float
    alpha: float
    evaluator_method_version: str
    evidence_digest: str | None
    status: EvaluationStatus
    evidence_binding_sha256: str | None


@dataclass(frozen=True, slots=True)
class Stage4RecallAuditReport:
    """A recall-only status report. Never a complete Stage-4 qualification.

    ``report_kind`` is a fixed, self-identifying discriminator so this can
    never be mistaken for ``canary_stage4_decision.Stage4Decision`` by a
    reader inspecting the JSON/object alone.
    """

    report_kind: str
    status: EvaluationStatus
    recall_evaluation: Stage4RecallAuditEvaluation


def build_recall_audit_report(
    evaluation: Stage4RecallAuditEvaluation,
) -> Stage4RecallAuditReport:
    """Wrap a recall evaluation as an explicitly recall-only report."""

    if not isinstance(evaluation, Stage4RecallAuditEvaluation):
        raise TypeError("evaluation must be a Stage4RecallAuditEvaluation")
    return Stage4RecallAuditReport(
        report_kind="RECALL_AUDIT_ONLY",
        status=evaluation.status,
        recall_evaluation=evaluation,
    )


def evaluate_recall_audit_evidence(
    *,
    expected_query_ids: frozenset[int],
    search_configuration,
    identity,
    dataset002_manifest_sha256: str,
    dataset002_schema_version: int,
    observations: Iterable[RecallAuditObservation],
    binding: Stage4EvidenceBinding,
    frozen_query_ids_sha256: str,
    recall_floor: float = 0.95,
) -> Stage4RecallAuditEvaluation:
    """Evaluate pre-verified recall-audit observations against one frozen,
    authorized (configuration, identity, manifest) contract.

    ``expected_query_ids`` must be exactly the DATASET-002 recall-audit
    population size; a caller supplying anything else has the wrong
    population and must fail before any observation is examined. No two
    observations from different contracts may be pooled: every field of the
    caller-supplied context is checked against every observation.

    ADR-008 Stage-4 evidence-binding repair: the supplied context is also
    checked against ``binding`` (an immutable, independently hash-verified
    ``Stage4EvidenceBinding``) *before* any observation is examined.
    ``evidence_binding_sha256`` on the result is set to ``binding.sha256``
    only once that match is confirmed here -- it is never copied from the
    caller's claim, so a caller cannot assert a binding this evaluator did
    not itself verify.

    Schema-v2 repair: ``search_configuration`` is compared for complete
    field-wise equality against ``binding.candidate_search_configuration``
    (metric, threshold_label, radius, index_track, ef, limit,
    consistency_level), not merely metric/threshold_label/ef. A candidate
    search run at a different, unregistered radius under an otherwise
    unchanged binding no longer reaches the Hoeffding bound at all.
    """

    if len(expected_query_ids) != EXP009_RECALL_AUDIT_COUNT:
        raise ValueError(
            f"expected_query_ids must contain exactly {EXP009_RECALL_AUDIT_COUNT} entries"
        )
    if not isinstance(binding, Stage4EvidenceBinding):
        raise TypeError("binding must be a Stage4EvidenceBinding")
    # R-001: the v1 evaluator is bound to exactly one recall floor. A caller
    # cannot shop for a different threshold under this evaluator's method
    # identity -- a genuinely different floor requires a new, separately
    # versioned evaluator contract, not a parameter change here.
    if (
        type(recall_floor) is not float
        or not math.isfinite(recall_floor)
        or recall_floor != 0.95
    ):
        raise ValueError("recall_floor must be exactly 0.95 under the v1 evaluator contract")

    if (
        search_configuration != binding.candidate_search_configuration
        or identity != binding.identity
        or dataset002_manifest_sha256 != binding.dataset002_manifest_sha256
        or frozen_query_ids_sha256 != binding.frozen_recall_audit_ids_sha256
    ):
        return _not_applicable(["EVIDENCE_BINDING_MISMATCH"], recall_floor=recall_floor)

    observations = tuple(observations)
    reasons: list[str] = []

    if len(observations) != EXP009_RECALL_AUDIT_COUNT:
        reasons.append("OBSERVATION_COUNT_INVALID")

    seen_query_ids: set[int] = set()
    for observation in observations:
        if not isinstance(observation, RecallAuditObservation):
            _append_once(reasons, "OBSERVATION_TYPE_INVALID")
            continue
        if observation.query_id in seen_query_ids:
            _append_once(reasons, "DUPLICATE_QUERY_ID")
            continue
        seen_query_ids.add(observation.query_id)
        if observation.query_id not in expected_query_ids:
            _append_once(reasons, "QUERY_ID_NOT_IN_FROZEN_SET")
        if (
            observation.search_configuration != search_configuration
            or observation.identity != identity
            or observation.dataset002_manifest_sha256 != dataset002_manifest_sha256
            or observation.dataset002_schema_version != dataset002_schema_version
        ):
            _append_once(reasons, "OBSERVATION_CONTEXT_MISMATCH")
        if observation.producer_run_id != binding.run_id:
            _append_once(reasons, "OBSERVATION_PRODUCER_RUN_MISMATCH")

    if reasons:
        # The binding itself matched (the gate above already passed); this
        # evidence is incomplete for an unrelated reason, so the binding
        # digest is still meaningful metadata about which contract was
        # actually under evaluation.
        return _not_applicable(
            reasons, recall_floor=recall_floor, evidence_binding_sha256=binding.sha256
        )

    recalls = tuple(observation.capped_recall for observation in observations)
    bound = one_sided_hoeffding_recall_lower_bound(recalls)
    passing = bound.lower_bound >= recall_floor

    return Stage4RecallAuditEvaluation(
        recall_audit_complete_and_passing=passing,
        reason_codes=(),
        sample_count=bound.observation_count,
        observed_mean=bound.observed_mean,
        margin=bound.margin,
        lower_bound=bound.lower_bound,
        confidence_level=bound.confidence_level,
        recall_floor=recall_floor,
        alpha=_ALPHA,
        evaluator_method_version=RECALL_AUDIT_EVALUATOR_VERSION,
        evidence_digest=_evidence_digest(observations),
        status=EvaluationStatus.PASSING if passing else EvaluationStatus.FAILING,
        evidence_binding_sha256=binding.sha256,
    )


def _evidence_digest(observations: tuple[RecallAuditObservation, ...]) -> str:
    ordered = sorted(observations, key=lambda observation: observation.query_id)
    document = [
        [
            observation.query_id,
            observation.oracle_result_sha256,
            observation.candidate_result_sha256,
            observation.producer_run_id,
        ]
        for observation in ordered
    ]
    return hashlib.sha256(canonical_json_bytes(document)).hexdigest()


def _append_once(reasons: list[str], code: str) -> None:
    if code not in reasons:
        reasons.append(code)


def _not_applicable(
    reasons: list[str],
    *,
    recall_floor: float,
    evidence_binding_sha256: str | None = None,
) -> Stage4RecallAuditEvaluation:
    return Stage4RecallAuditEvaluation(
        recall_audit_complete_and_passing=False,
        reason_codes=tuple(reasons),
        sample_count=0,
        observed_mean=None,
        margin=None,
        lower_bound=None,
        confidence_level=0.95,
        recall_floor=recall_floor,
        alpha=_ALPHA,
        evaluator_method_version=RECALL_AUDIT_EVALUATOR_VERSION,
        evidence_digest=None,
        status=EvaluationStatus.INCOMPLETE,
        evidence_binding_sha256=evidence_binding_sha256,
    )
