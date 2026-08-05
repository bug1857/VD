"""Tests for the single canonical Stage-4 decision combination.

``Stage4Decision`` requires BOTH latency and recall evaluations to be
present, complete, passing, and bound to the identical ADR-008
``Stage4EvidenceBinding`` digest to reach ``PASSING``. This is distinct from
``Stage4RecallAuditReport`` (see test_canary_recall_audit_evaluation.py),
which may legitimately report a passing recall-only status with no latency
evidence at all -- the two must never be confused for one another.

The combiner never inspects reason-code strings to classify outcomes: each
source evaluation already carries an explicit typed ``EvaluationStatus``, and
this module only combines those statuses plus an explicit binding-digest
equality check. Reason codes are carried through purely as explanatory
evidence.
"""

from __future__ import annotations

import unittest

from vdbench.canary_recall_audit_evaluation import (
    RECALL_AUDIT_EVALUATOR_VERSION,
    EvaluationStatus,
    Stage4RecallAuditEvaluation,
)
from vdbench.canary_schedule_evaluation import Stage4ScheduleEvaluation
from vdbench.canary_stage4_decision import (
    Stage4Decision,
    Stage4DecisionStatus,
    combine_stage4_decision,
)
from vdbench.canary_stage4_latency_evidence import Stage4LatencyEvidence


_BINDING_SHA256 = "a" * 64
_OTHER_BINDING_SHA256 = "b" * 64


def _schedule_evaluation(*, applicable: bool = True, reason_codes: tuple[str, ...] = ()):
    return Stage4ScheduleEvaluation(
        finite_manifest_latency_applicable=applicable,
        reason_codes=reason_codes,
        control_sweeps=(),
        baseline_median_ms=1.0 if applicable else None,
        baseline_p95_ms=2.0 if applicable else None,
        candidate_latency_count=60 if applicable else 0,
        candidate_latency_max_ms=3.0 if applicable else None,
        finite_population_coverage_probability=0.961003033592,
        recall_bound_evaluated=False,
    )


def _latency_evidence(
    *,
    status: EvaluationStatus = EvaluationStatus.PASSING,
    reason_codes: tuple[str, ...] = (),
    evidence_binding_sha256: str | None = _BINDING_SHA256,
    chain_head_sha256: str | None = "c" * 64,
):
    return Stage4LatencyEvidence(
        status=status,
        reason_codes=reason_codes,
        schedule_evaluation=_schedule_evaluation(
            applicable=status is EvaluationStatus.PASSING, reason_codes=reason_codes
        ),
        execution_ledger_chain_head_sha256=chain_head_sha256,
        evidence_binding_sha256=evidence_binding_sha256,
    )


def _recall_evaluation(
    *,
    status: EvaluationStatus,
    reason_codes: tuple[str, ...] = (),
    evidence_digest: str | None = "d" * 64,
    evidence_binding_sha256: str | None = _BINDING_SHA256,
):
    passing = status is EvaluationStatus.PASSING
    return Stage4RecallAuditEvaluation(
        recall_audit_complete_and_passing=passing,
        reason_codes=reason_codes,
        sample_count=1200 if status is not EvaluationStatus.INCOMPLETE else 0,
        observed_mean=0.99 if passing else 0.5,
        margin=0.035330182290,
        lower_bound=0.96 if passing else 0.1,
        confidence_level=0.95,
        recall_floor=0.95,
        alpha=0.05,
        evaluator_method_version=RECALL_AUDIT_EVALUATOR_VERSION,
        evidence_digest=evidence_digest,
        status=status,
        evidence_binding_sha256=evidence_binding_sha256,
    )


class CombineStage4DecisionTests(unittest.TestCase):
    def test_both_passing_with_matching_binding_yields_passing_status(self) -> None:
        decision = combine_stage4_decision(
            latency_evidence=_latency_evidence(status=EvaluationStatus.PASSING),
            recall_evaluation=_recall_evaluation(status=EvaluationStatus.PASSING),
        )

        self.assertIsInstance(decision, Stage4Decision)
        self.assertEqual(decision.decision_status, Stage4DecisionStatus.PASSING)
        self.assertEqual(decision.latency_status, EvaluationStatus.PASSING)
        self.assertEqual(decision.recall_status, EvaluationStatus.PASSING)
        self.assertEqual(decision.evidence_binding_sha256, _BINDING_SHA256)

    def test_latency_evidence_none_is_incomplete(self) -> None:
        decision = combine_stage4_decision(
            latency_evidence=None,
            recall_evaluation=_recall_evaluation(status=EvaluationStatus.PASSING),
        )

        self.assertEqual(decision.decision_status, Stage4DecisionStatus.INCOMPLETE)
        self.assertEqual(decision.latency_status, EvaluationStatus.INCOMPLETE)
        self.assertIsNone(decision.evidence_binding_sha256)

    def test_recall_evaluation_none_is_incomplete(self) -> None:
        decision = combine_stage4_decision(
            latency_evidence=_latency_evidence(status=EvaluationStatus.PASSING),
            recall_evaluation=None,
        )

        self.assertEqual(decision.decision_status, Stage4DecisionStatus.INCOMPLETE)
        self.assertEqual(decision.recall_status, EvaluationStatus.INCOMPLETE)

    def test_both_none_is_incomplete(self) -> None:
        decision = combine_stage4_decision(latency_evidence=None, recall_evaluation=None)
        self.assertEqual(decision.decision_status, Stage4DecisionStatus.INCOMPLETE)

    def test_recall_present_but_internally_incomplete_is_incomplete(self) -> None:
        """A recall evaluation object can be present yet still carry an
        INCOMPLETE status (malformed evidence) -- this must propagate to
        Stage4Decision as INCOMPLETE, not be miscategorized as FAILING."""

        decision = combine_stage4_decision(
            latency_evidence=_latency_evidence(status=EvaluationStatus.PASSING),
            recall_evaluation=_recall_evaluation(
                status=EvaluationStatus.INCOMPLETE,
                reason_codes=("OBSERVATION_COUNT_INVALID",),
                evidence_digest=None,
                evidence_binding_sha256=None,
            ),
        )

        self.assertEqual(decision.decision_status, Stage4DecisionStatus.INCOMPLETE)

    def test_recall_below_floor_with_complete_evidence_and_matching_binding_yields_failing(
        self,
    ) -> None:
        decision = combine_stage4_decision(
            latency_evidence=_latency_evidence(status=EvaluationStatus.PASSING),
            recall_evaluation=_recall_evaluation(status=EvaluationStatus.FAILING),
        )

        self.assertEqual(decision.decision_status, Stage4DecisionStatus.FAILING)
        self.assertFalse(decision.recall_status is EvaluationStatus.PASSING)
        self.assertEqual(decision.evidence_binding_sha256, _BINDING_SHA256)

    def test_latency_not_applicable_with_matching_binding_yields_failing_status(self) -> None:
        """Stage4LatencyEvidence itself doesn't distinguish 'incomplete'
        from 'ceiling breach' at the wrapped-schedule level -- both collapse
        to a FAILING status once the binding matched. Present, matched
        binding, and not-applicable maps to FAILING, never a reason-code
        lookup."""

        decision = combine_stage4_decision(
            latency_evidence=_latency_evidence(
                status=EvaluationStatus.FAILING,
                reason_codes=("CONTROL_ABSOLUTE_P95_CEILING_BREACH",),
            ),
            recall_evaluation=_recall_evaluation(status=EvaluationStatus.PASSING),
        )

        self.assertEqual(decision.decision_status, Stage4DecisionStatus.FAILING)
        self.assertEqual(decision.latency_status, EvaluationStatus.FAILING)

    def test_reason_codes_are_deduplicated_and_stable_ordered(self) -> None:
        decision = combine_stage4_decision(
            latency_evidence=_latency_evidence(
                status=EvaluationStatus.FAILING, reason_codes=("X_CODE", "Y_CODE")
            ),
            recall_evaluation=_recall_evaluation(
                status=EvaluationStatus.FAILING, reason_codes=("Y_CODE", "Z_CODE")
            ),
        )

        self.assertEqual(decision.reason_codes, ("X_CODE", "Y_CODE", "Z_CODE"))

    def test_consumed_evidence_digests_include_recall_digest_and_ledger_chain_head(self) -> None:
        decision = combine_stage4_decision(
            latency_evidence=_latency_evidence(
                status=EvaluationStatus.PASSING, chain_head_sha256="e" * 64
            ),
            recall_evaluation=_recall_evaluation(
                status=EvaluationStatus.PASSING, evidence_digest="f" * 64
            ),
        )

        self.assertIn("f" * 64, decision.consumed_evidence_digests)
        self.assertIn("e" * 64, decision.consumed_evidence_digests)

    def test_source_evaluations_are_preserved_verbatim(self) -> None:
        latency = _latency_evidence(status=EvaluationStatus.PASSING)
        recall = _recall_evaluation(status=EvaluationStatus.PASSING)
        decision = combine_stage4_decision(latency_evidence=latency, recall_evaluation=recall)

        self.assertIs(decision.latency_evaluation, latency.schedule_evaluation)
        self.assertIs(decision.recall_evaluation, recall)

    # -- ADR-008 Stage-4 evidence-binding repair -----------------------------

    def test_mismatched_evidence_bindings_is_incomplete_even_when_both_individually_passing(
        self,
    ) -> None:
        """This is the exact vulnerability ADR-008's evidence-binding repair
        closes: two individually-passing evaluations from two different
        runs/configurations must never combine into a PASSING decision."""

        decision = combine_stage4_decision(
            latency_evidence=_latency_evidence(
                status=EvaluationStatus.PASSING, evidence_binding_sha256=_BINDING_SHA256
            ),
            recall_evaluation=_recall_evaluation(
                status=EvaluationStatus.PASSING, evidence_binding_sha256=_OTHER_BINDING_SHA256
            ),
        )

        self.assertEqual(decision.decision_status, Stage4DecisionStatus.INCOMPLETE)
        self.assertEqual(decision.latency_status, EvaluationStatus.PASSING)
        self.assertEqual(decision.recall_status, EvaluationStatus.PASSING)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", decision.reason_codes)
        self.assertIsNone(decision.evidence_binding_sha256)

    def test_missing_latency_binding_with_matching_recall_is_incomplete(self) -> None:
        decision = combine_stage4_decision(
            latency_evidence=_latency_evidence(
                status=EvaluationStatus.PASSING, evidence_binding_sha256=None
            ),
            recall_evaluation=_recall_evaluation(status=EvaluationStatus.PASSING),
        )

        self.assertEqual(decision.decision_status, Stage4DecisionStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", decision.reason_codes)

    def test_missing_recall_binding_with_matching_latency_is_incomplete(self) -> None:
        decision = combine_stage4_decision(
            latency_evidence=_latency_evidence(status=EvaluationStatus.PASSING),
            recall_evaluation=_recall_evaluation(
                status=EvaluationStatus.PASSING, evidence_binding_sha256=None
            ),
        )

        self.assertEqual(decision.decision_status, Stage4DecisionStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", decision.reason_codes)

    def test_wrong_type_latency_evidence_raises(self) -> None:
        with self.assertRaises(TypeError):
            combine_stage4_decision(
                latency_evidence=_schedule_evaluation(),  # the wrong, unwrapped type
                recall_evaluation=_recall_evaluation(status=EvaluationStatus.PASSING),
            )


if __name__ == "__main__":
    unittest.main()
