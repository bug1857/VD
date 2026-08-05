"""Bind EXP-009 Stage-4 latency evidence to one canonical evidence binding.

Purpose:
    Rebuild ``Stage4ScheduleEvaluation`` from the real, hash-chain-verified
    ``Stage4ExecutionLedger`` and wrap it with the immutable
    ``Stage4EvidenceBinding`` digest and the verified ledger chain-head
    digest that ADR-008's Stage-4 evidence-binding repair requires before
    latency evidence may ever be combined with recall evidence.
Inputs:
    A ``Stage4EvidenceBinding``, the matching ``Stage4ExecutionSchedule``,
    and an already-open ``Stage4ExecutionLedger`` for that same schedule.
Outputs:
    ``Stage4LatencyEvidence`` -- INCOMPLETE if the binding, schedule, or
    ledger disagree with one another; otherwise the real, ledger-rebuilt
    schedule evaluation plus its binding and chain digests.
Dependencies:
    ``canary_schedule_evaluation.py`` is deliberately kept untouched and
    external, per its own docstring and per ``canary_stage4_decision.py``'s
    existing design note -- this module only composes it, never reimplements
    or edits its arithmetic. No Milvus, PyMilvus, network, routing, or
    actuation import.
Failure modes:
    Any binding/schedule/ledger mismatch is INCOMPLETE with
    ``evidence_binding_sha256=None``; the ledger is never read past that
    check on a mismatch. A free-form, hand-authored latency document is
    structurally impossible here: every input is an already-typed,
    independently self-validating or hash-chain-verified object.
Scope:
    ``finite_manifest_latency_applicable=False`` continues to collapse both
    "incomplete evidence" and "evidence complete but a ceiling was breached"
    into one boolean, exactly as ``Stage4ScheduleEvaluation`` and
    ``canary_stage4_decision.py`` already document and rely on. This module
    does not change that collapsed semantic: a binding mismatch (checked
    before the ledger is read at all) is the only path that yields
    INCOMPLETE here; a matched binding with incomplete or failing ledger
    evidence yields FAILING, matching the existing convention exactly.
"""

from __future__ import annotations

from dataclasses import dataclass

from .canary_execution_ledger import Stage4ExecutionLedger
from .canary_recall_audit_evaluation import EvaluationStatus
from .canary_schedule import Stage4ExecutionSchedule
from .canary_schedule_evaluation import (
    Stage4ScheduleEvaluation,
    evaluate_stage4_execution_ledger,
)
from .canary_stage4_evidence_binding import Stage4EvidenceBinding


__all__ = ["Stage4LatencyEvidence", "build_stage4_latency_evidence"]


@dataclass(frozen=True, slots=True)
class Stage4LatencyEvidence:
    """The ADR-008 evidence-binding wrapper around one latency evaluation.

    ``evidence_binding_sha256`` is set only once this module has itself
    confirmed the supplied schedule and ledger match the declared binding --
    it is never copied from a caller's unverified claim.
    """

    status: EvaluationStatus
    reason_codes: tuple[str, ...]
    schedule_evaluation: Stage4ScheduleEvaluation | None
    execution_ledger_chain_head_sha256: str | None
    evidence_binding_sha256: str | None


def build_stage4_latency_evidence(
    *,
    binding: Stage4EvidenceBinding,
    schedule: Stage4ExecutionSchedule,
    ledger: Stage4ExecutionLedger,
) -> Stage4LatencyEvidence:
    """Rebuild latency evidence from the verified schedule/ledger, then wrap
    it with the binding digest and the verified ledger chain-head digest.
    """

    if not isinstance(binding, Stage4EvidenceBinding):
        raise TypeError("binding must be a Stage4EvidenceBinding")
    if not isinstance(schedule, Stage4ExecutionSchedule):
        raise TypeError("schedule must be a Stage4ExecutionSchedule")
    if not isinstance(ledger, Stage4ExecutionLedger):
        raise TypeError("ledger must be a Stage4ExecutionLedger")

    if (
        schedule.schedule_sha256 != binding.execution_schedule_sha256
        or ledger.schedule_sha256 != binding.execution_schedule_sha256
        or schedule.metric is not binding.metric
        or schedule.threshold_stratum != binding.threshold_stratum
        or schedule.candidate_ef != binding.candidate_ef
        or schedule.last_known_good_ef != binding.last_known_good_ef
    ):
        return Stage4LatencyEvidence(
            status=EvaluationStatus.INCOMPLETE,
            reason_codes=("EVIDENCE_BINDING_MISMATCH",),
            schedule_evaluation=None,
            execution_ledger_chain_head_sha256=None,
            evidence_binding_sha256=None,
        )

    schedule_evaluation = evaluate_stage4_execution_ledger(schedule=schedule, ledger=ledger)
    chain_head = ledger.progress().chain_head_sha256
    status = (
        EvaluationStatus.PASSING
        if schedule_evaluation.finite_manifest_latency_applicable
        else EvaluationStatus.FAILING
    )
    return Stage4LatencyEvidence(
        status=status,
        reason_codes=schedule_evaluation.reason_codes,
        schedule_evaluation=schedule_evaluation,
        execution_ledger_chain_head_sha256=chain_head,
        evidence_binding_sha256=binding.sha256,
    )
