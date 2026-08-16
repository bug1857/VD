"""The single canonical Stage-4 qualification decision.

Purpose:
    Combine ``Stage4LatencyEvidence`` (latency, itself an ADR-008 binding
    wrapper around the existing, unmodified ``Stage4ScheduleEvaluation``)
    with ``Stage4RecallAuditEvaluation`` (recall) into exactly one
    authoritative result. This is the only place either evidence stream may
    be combined; no other module may compute a second, differently-named
    Stage-4 pass/fail signal.
Inputs:
    Two already-computed evaluations, either of which may be absent
    (``None``) if that evidence stream was not evaluated in this report.
Outputs:
    ``Stage4Decision`` -- ``PASSING`` only when both evaluations are present,
    complete, passing, and bound to the identical ADR-008
    ``Stage4EvidenceBinding`` digest.
Dependencies:
    Pure value objects from ``canary_stage4_latency_evidence`` and
    ``canary_recall_audit_evaluation`` only. No Milvus, network, or
    live-execution import.
Failure modes:
    Never fabricates a benign result: any missing, non-passing, or
    binding-mismatched input evaluation yields ``INCOMPLETE`` or
    ``FAILING``, never ``PASSING``.
Scope:
    This module never inspects reason-code strings to classify an outcome.
    Each source evaluation already carries an explicit typed ``status``.
    Reason codes are carried through purely as explanatory evidence.

    ADR-008 Stage-4 evidence-binding repair: ``latency_evidence`` and
    ``recall_evaluation`` are combined only when both carry a non-``None``
    ``evidence_binding_sha256`` and those two digests are equal. A missing
    or mismatched binding yields ``INCOMPLETE`` regardless of what each
    individual evaluation's own status says -- this is checked before, and
    independently of, the PASSING/FAILING status combination below, exactly
    as ADR-008 requires ("absent, malformed, unequal, or unverified
    bindings yield INCOMPLETE, never PASSING or a generic SLO failure").
    Each individual evaluation's own ``latency_status``/``recall_status`` is
    still reported verbatim even when the combined binding does not match,
    so a reviewer can see that each side individually completed even though
    they cannot be proven to describe the same run.

    ``Stage4Decision`` is distinct from
    ``canary_recall_audit_evaluation.Stage4RecallAuditReport``, which may
    legitimately report a passing recall-only status with no latency
    evidence at all. The two must never be confused: only this module's
    ``Stage4Decision`` is a complete Stage-4 qualification claim.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .canary_recall_audit_evaluation import (
    EvaluationStatus,
    Stage4RecallAuditEvaluation,
)
from .canary_schedule_evaluation import Stage4ScheduleEvaluation
from .canary_stage4_latency_evidence import Stage4LatencyEvidence

__all__ = [
    "Stage4Decision",
    "Stage4DecisionStatus",
    "combine_stage4_decision",
]


class Stage4DecisionStatus(StrEnum):
    """The only three outcomes a complete Stage-4 qualification may report."""

    PASSING = "PASSING"
    FAILING = "FAILING"
    INCOMPLETE = "INCOMPLETE"


@dataclass(frozen=True, slots=True)
class Stage4Decision:
    """The authoritative combination of latency and recall Stage-4 evidence.

    Never a live-actuation authorization: this is a read-only evidence
    verdict, not a candidate-promotion or routing action.
    """

    latency_status: EvaluationStatus
    recall_status: EvaluationStatus
    decision_status: Stage4DecisionStatus
    reason_codes: tuple[str, ...]
    latency_evaluation: Stage4ScheduleEvaluation | None
    recall_evaluation: Stage4RecallAuditEvaluation | None
    evidence_binding_sha256: str | None
    consumed_evidence_digests: tuple[str, ...]


def combine_stage4_decision(
    *,
    latency_evidence: Stage4LatencyEvidence | None,
    recall_evaluation: Stage4RecallAuditEvaluation | None,
) -> Stage4Decision:
    """Combine two independently-computed evaluations into one verdict.

    Neither evaluation's reason codes are inspected here to decide the
    outcome -- only their already-typed statuses, plus an explicit ADR-008
    evidence-binding equality check, are combined.
    """

    if latency_evidence is not None and not isinstance(latency_evidence, Stage4LatencyEvidence):
        raise TypeError("latency_evidence must be a Stage4LatencyEvidence or None")
    if recall_evaluation is not None and not isinstance(
        recall_evaluation, Stage4RecallAuditEvaluation
    ):
        raise TypeError("recall_evaluation must be a Stage4RecallAuditEvaluation or None")

    latency_status = (
        latency_evidence.status if latency_evidence is not None else EvaluationStatus.INCOMPLETE
    )
    recall_status = (
        recall_evaluation.status if recall_evaluation is not None else EvaluationStatus.INCOMPLETE
    )

    reason_codes: list[str] = []
    if latency_evidence is not None:
        for code in latency_evidence.reason_codes:
            if code not in reason_codes:
                reason_codes.append(code)
    if recall_evaluation is not None:
        for code in recall_evaluation.reason_codes:
            if code not in reason_codes:
                reason_codes.append(code)

    evidence_binding_sha256: str | None = None
    if latency_evidence is not None and recall_evaluation is not None:
        latency_binding = latency_evidence.evidence_binding_sha256
        recall_binding = recall_evaluation.evidence_binding_sha256
        if (
            latency_binding is not None
            and recall_binding is not None
            and latency_binding == recall_binding
        ):
            evidence_binding_sha256 = latency_binding
        elif "EVIDENCE_BINDING_MISMATCH" not in reason_codes:
            reason_codes.append("EVIDENCE_BINDING_MISMATCH")

    if EvaluationStatus.INCOMPLETE in (latency_status, recall_status):
        decision_status = Stage4DecisionStatus.INCOMPLETE
    elif latency_evidence is not None and recall_evaluation is not None and evidence_binding_sha256 is None:
        # Both sides individually completed, but cannot be proven to
        # describe the same run/configuration: never PASSING or a generic
        # SLO FAILING on an unverified or mismatched binding.
        decision_status = Stage4DecisionStatus.INCOMPLETE
    elif latency_status is EvaluationStatus.PASSING and recall_status is EvaluationStatus.PASSING:
        decision_status = Stage4DecisionStatus.PASSING
    else:
        decision_status = Stage4DecisionStatus.FAILING

    digests = tuple(
        digest
        for digest in (
            latency_evidence.execution_ledger_chain_head_sha256
            if latency_evidence is not None
            else None,
            recall_evaluation.evidence_digest if recall_evaluation is not None else None,
        )
        if digest is not None
    )

    return Stage4Decision(
        latency_status=latency_status,
        recall_status=recall_status,
        decision_status=decision_status,
        reason_codes=tuple(reason_codes),
        latency_evaluation=(
            latency_evidence.schedule_evaluation if latency_evidence is not None else None
        ),
        recall_evaluation=recall_evaluation,
        evidence_binding_sha256=evidence_binding_sha256,
        consumed_evidence_digests=digests,
    )
