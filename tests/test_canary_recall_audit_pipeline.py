"""Real end-to-end wiring test for the EXP-009 recall-audit pipeline.

Exercises the full composed path this feature requires to not be an orphaned
module, with genuine ADR-008 evidence binding at every step:

    frozen audit query IDs (fake Dataset002-shaped population)
    -> per-query oracle/candidate result-ID evidence (fake producer)
    -> CanaryRecallAuditLedger (real, durable, restart-safe, binding-bound)
    -> evaluate_recall_audit_evidence (real, pure, binding-checked)
    -> Stage4ExecutionLedger (real, durable, hash-chain-verified)
    -> build_stage4_latency_evidence (real, binding-checked, rebuilt from
       the verified ledger -- never a free-form document)
    -> combine_stage4_decision (real, pure, requires matching binding digests)
    -> Stage4Decision (the canonical decision result)
    -> build_qualification_document (the real, read-only, human-facing consumer)

This file previously combined a hand-fabricated ``Stage4ScheduleEvaluation``
with zero connection to any real ledger, schedule, or binding, and asserted
the combined result was PASSING. That was not a wiring proof -- it was a
working demonstration of the exact vulnerability ADR-008's Stage-4
evidence-binding repair exists to close (two individually-passing
evaluations from unrelated runs combining into a false PASSING
qualification). This file now proves the opposite: mismatched evidence must
never combine into PASSING, and only genuinely bound, genuinely
ledger-verified evidence from the *same* run may.

The live Milvus/oracle producer that computes real result-ID sets against a
running index remains a separate, later, explicitly-authorized phase.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import tempfile
import unittest
from pathlib import Path

from vdbench.artifacts import canonical_json_bytes
from vdbench.canary_execution_ledger import Stage4ExecutionLedger, Stage4SlotObservation
from vdbench.canary_recall_audit_evaluation import (
    EvaluationStatus,
    Stage4RecallAuditEvaluation,
    build_recall_audit_report,
    evaluate_recall_audit_evidence,
)
from vdbench.canary_recall_audit_ledger import CanaryRecallAuditLedger, RecallAuditObservation
from vdbench.canary_routing import build_canary_route_plan
from vdbench.canary_schedule import build_stage4_execution_schedule
from vdbench.canary_stage4_decision import Stage4Decision, Stage4DecisionStatus, combine_stage4_decision
from vdbench.canary_stage4_evidence_binding import Stage4EvidenceBinding
from vdbench.canary_stage4_latency_evidence import build_stage4_latency_evidence
from vdbench.canary_stage4_qualification_report import build_qualification_document
from vdbench.canary_statistics import EXP009_RECALL_AUDIT_COUNT
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    CandidateSelectionRecord,
    EligibleOccurrence,
    EligibleWorkloadManifest,
    SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING,
    SCHEDULE_EXECUTION_MODE,
    SCHEDULE_INTERLEAVED_SWEEP_COUNT,
    SCHEDULE_MEDIAN_RELATIVE_CEILING,
    SCHEDULE_POST_SWEEP_COUNT,
    SCHEDULE_PRE_SWEEP_COUNT,
    SCHEDULE_P95_RELATIVE_CEILING,
    SCHEDULE_ROUTING_BLOCK_SIZE,
    SCHEDULE_STABILITY_SCHEMA_VERSION,
    ScheduleControl,
    ScheduleStabilityContract,
    WorkloadIdentityBinding,
)
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.dataset002 import DATASET002_SCHEMA_VERSION


def _sha(value: str | bytes) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(data).hexdigest()


def _fake_oracle_and_candidate_ids(
    *, query_id: int, result_cap: int, miss_one_in_n: int | None
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    base = query_id * result_cap
    oracle_ids = tuple(range(base, base + result_cap))
    if miss_one_in_n is not None and query_id % miss_one_in_n == 0:
        # Out-of-range positive sentinel standing in for "found the wrong
        # thing" -- result IDs are non-negative by this codebase's convention.
        candidate_ids = oracle_ids[:-1] + (10_000_000 + query_id,)
    else:
        candidate_ids = oracle_ids
    return oracle_ids, candidate_ids


def _build_schedule(*, search_configuration: SearchConfiguration, identity: WorkloadIdentityBinding):
    # L2 requires range_filter == 0.0; COSINE requires range_filter == 1.0
    # (see EligibleWorkloadManifest.validate()) -- this is metric-dependent,
    # not a fixed literal.
    range_filter = 0.0 if search_configuration.metric is Metric.L2 else 1.0
    controls = tuple(ScheduleControl(600 + index, _sha(f"control-{index}")) for index in range(50))
    stability = ScheduleStabilityContract(
        schema_version=SCHEDULE_STABILITY_SCHEMA_VERSION,
        control_role="recall_audit",
        control_ef=400,
        controls=controls,
        pre_sweep_count=SCHEDULE_PRE_SWEEP_COUNT,
        routing_block_size=SCHEDULE_ROUTING_BLOCK_SIZE,
        interleaved_sweep_count=SCHEDULE_INTERLEAVED_SWEEP_COUNT,
        post_sweep_count=SCHEDULE_POST_SWEEP_COUNT,
        execution_mode=SCHEDULE_EXECUTION_MODE,
        absolute_p95_latency_ms_ceiling=SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING,
        p95_relative_ceiling=SCHEDULE_P95_RELATIVE_CEILING,
        median_relative_ceiling=SCHEDULE_MEDIAN_RELATIVE_CEILING,
        require_all_success=True,
        require_identity_and_health_per_sweep=True,
    )
    manifest = EligibleWorkloadManifest(
        schema_version="exp009-eligible-workload-manifest-v2",
        created_at_utc="2026-08-04T15:00:00Z",
        dataset002_manifest_sha256=_sha("dataset002"),
        dataset001_generation_manifest_sha256=_sha("dataset001"),
        metric=search_configuration.metric,
        threshold_stratum=search_configuration.threshold_label,
        candidate_ef=search_configuration.ef,
        last_known_good_ef=400,
        radius=search_configuration.radius,
        range_filter=range_filter,
        limit=search_configuration.limit,
        identity=identity,
        vector_mapping="one_to_one_unique_dataset002_routing_vectors",
        schedule_stability=stability,
        occurrences=tuple(
            EligibleOccurrence(
                index,
                f"exp009-routing-{index:06d}",
                index,
                _sha(f"route-{index}"),
                search_configuration.radius,
                range_filter,
                search_configuration.limit,
            )
            for index in range(600)
        ),
    )
    manifest.validate()
    selection = CandidateSelectionRecord(
        schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
        selected_at_utc="2026-08-04T15:01:00Z",
        eligible_manifest_sha256=_sha("manifest-binding"),
        population_count=600,
        candidate_count=60,
        candidate_fraction=0.10,
        candidate_occurrence_ids=tuple(
            item.occurrence_id for item in manifest.occurrences if item.sequence_index % 10 == 0
        ),
        random_source="python.secrets.SystemRandom.sample",
        selected_before_candidate_results=True,
    )
    selection = replace(
        selection,
        eligible_manifest_sha256=hashlib.sha256(canonical_json_bytes(manifest.to_document())).hexdigest(),
    )
    return build_stage4_execution_schedule(manifest, build_canary_route_plan(manifest, selection))


class RecallAuditPipelineRealEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)
        self.ledger_path = Path(self._tempdir.name) / "pipeline_recall_audit.sqlite3"
        self.latency_ledger_path = Path(self._tempdir.name) / "pipeline_latency.sqlite3"
        self.context = dict(
            search_configuration=SearchConfiguration(
                metric=Metric.COSINE,
                threshold_label="target-025",
                radius=0.2,
                index_track=IndexTrack.HNSW,
                ef=800,
                limit=100,
                consistency_level="Strong",
            ),
            identity=WorkloadIdentityBinding(
                configuration_identity="a" * 16,
                data_identity="DATASET-001-v1:sha256:" + "b" * 64,
                flat_binding_id="c" * 16,
                hnsw_binding_id="d" * 16,
            ),
            dataset002_manifest_sha256="e" * 64,
            dataset002_schema_version=DATASET002_SCHEMA_VERSION,
        )
        self.frozen_query_ids = frozenset(range(EXP009_RECALL_AUDIT_COUNT))
        self.frozen_query_ids_sha256 = _sha(",".join(str(i) for i in sorted(self.frozen_query_ids)))
        self.schedule = _build_schedule(
            search_configuration=self.context["search_configuration"], identity=self.context["identity"]
        )
        self.binding = Stage4EvidenceBinding(
            run_id="fake-pipeline-run",
            source_revision="0" * 40,
            metric=self.context["search_configuration"].metric,
            threshold_stratum=self.context["search_configuration"].threshold_label,
            current_ef=self.schedule.last_known_good_ef,
            candidate_ef=self.context["search_configuration"].ef,
            last_known_good_ef=self.schedule.last_known_good_ef,
            identity=self.context["identity"],
            dataset002_manifest_sha256=self.context["dataset002_manifest_sha256"],
            frozen_recall_audit_ids_sha256=self.frozen_query_ids_sha256,
            eligible_workload_sha256=_sha("eligible-workload"),
            candidate_selection_sha256=_sha("candidate-selection"),
            execution_schedule_sha256=self.schedule.schedule_sha256,
            recall_evidence_schema_version="recall-audit-hoeffding-1200-v1",
            latency_evidence_schema_version="exp009-stage4-execution-schedule-v1",
        )

    def _populate_latency_ledger(self, *, run_id: str) -> Stage4ExecutionLedger:
        ledger = Stage4ExecutionLedger(self.latency_ledger_path, run_id=run_id, schedule=self.schedule)
        for step in self.schedule.steps:
            latency = 1.0 if step.control_query_id is not None else 2.0
            ledger.append(
                Stage4SlotObservation(
                    execution_index=step.execution_index,
                    observed_ef=step.expected_ef,
                    started_monotonic_ns=step.execution_index * 10,
                    finished_monotonic_ns=step.execution_index * 10 + 5,
                    recorded_at_utc="2026-08-04T15:02:00Z",
                    success=True,
                    timed_out=False,
                    threshold_semantics_valid=True,
                    health_before_ok=True,
                    health_after_ok=True,
                    identity_before_ok=True,
                    identity_after_ok=True,
                    result_count=0,
                    latency_ms=latency,
                    reason_code=None,
                )
            )
        return ledger

    def test_full_pipeline_with_matching_binding_produces_a_canonical_passing_decision(self) -> None:
        recall_ledger = CanaryRecallAuditLedger(
            self.ledger_path, run_id="fake-pipeline-run", binding_sha256=self.binding.sha256
        )
        for query_id in self.frozen_query_ids:
            oracle_ids, candidate_ids = _fake_oracle_and_candidate_ids(
                query_id=query_id, result_cap=100, miss_one_in_n=40
            )
            append_result = recall_ledger.append(
                RecallAuditObservation(
                    query_id=query_id,
                    oracle_result_ids=oracle_ids,
                    candidate_result_ids=candidate_ids,
                    producer_run_id="fake-pipeline-run",
                    recorded_at_utc="2026-08-04T00:00:00Z",
                    **self.context,
                )
            )
            self.assertTrue(append_result.accepted, append_result.reason_code)

        observations = recall_ledger.records()
        self.assertEqual(len(observations), EXP009_RECALL_AUDIT_COUNT)

        recall_evaluation = evaluate_recall_audit_evidence(
            expected_query_ids=self.frozen_query_ids,
            observations=observations,
            binding=self.binding,
            frozen_query_ids_sha256=self.frozen_query_ids_sha256,
            **self.context,
        )
        self.assertIsInstance(recall_evaluation, Stage4RecallAuditEvaluation)
        self.assertTrue(recall_evaluation.recall_audit_complete_and_passing)
        self.assertEqual(recall_evaluation.status, EvaluationStatus.PASSING)
        self.assertEqual(recall_evaluation.evidence_binding_sha256, self.binding.sha256)

        recall_only_report = build_recall_audit_report(recall_evaluation)
        self.assertEqual(recall_only_report.report_kind, "RECALL_AUDIT_ONLY")
        self.assertEqual(recall_only_report.status, EvaluationStatus.PASSING)

        latency_ledger = self._populate_latency_ledger(run_id="fake-pipeline-run")
        latency_evidence = build_stage4_latency_evidence(
            binding=self.binding, schedule=self.schedule, ledger=latency_ledger
        )
        self.assertEqual(latency_evidence.status, EvaluationStatus.PASSING)
        self.assertEqual(latency_evidence.evidence_binding_sha256, self.binding.sha256)

        decision = combine_stage4_decision(
            latency_evidence=latency_evidence, recall_evaluation=recall_evaluation
        )
        self.assertIsInstance(decision, Stage4Decision)
        self.assertEqual(decision.decision_status, Stage4DecisionStatus.PASSING)
        self.assertEqual(decision.evidence_binding_sha256, self.binding.sha256)

        document = build_qualification_document(decision)
        self.assertEqual(document["decision_status"], "PASSING")
        self.assertEqual(document["evidence_binding_sha256"], self.binding.sha256)

        # Recall alone, with no latency evidence at all, must still be
        # reportable as a passing recall-only status distinct from a full
        # qualification -- these two must never be conflated.
        recall_only_decision = combine_stage4_decision(
            latency_evidence=None, recall_evaluation=recall_evaluation
        )
        self.assertEqual(recall_only_decision.decision_status, Stage4DecisionStatus.INCOMPLETE)

    def test_pipeline_fails_closed_on_a_context_mismatched_observation(self) -> None:
        recall_ledger = CanaryRecallAuditLedger(
            self.ledger_path, run_id="fake-pipeline-run-2", binding_sha256=self.binding.sha256
        )
        mismatched_configuration = SearchConfiguration(
            metric=self.context["search_configuration"].metric,
            threshold_label=self.context["search_configuration"].threshold_label,
            radius=self.context["search_configuration"].radius,
            index_track=IndexTrack.HNSW,
            ef=400,  # wrong ef slipped in for one query
            limit=100,
            consistency_level="Strong",
        )

        for query_id in self.frozen_query_ids:
            oracle_ids, candidate_ids = _fake_oracle_and_candidate_ids(
                query_id=query_id, result_cap=100, miss_one_in_n=None
            )
            context = dict(self.context)
            if query_id == 0:
                context["search_configuration"] = mismatched_configuration
            recall_ledger.append(
                RecallAuditObservation(
                    query_id=query_id,
                    oracle_result_ids=oracle_ids,
                    candidate_result_ids=candidate_ids,
                    producer_run_id="fake-pipeline-run-2",
                    recorded_at_utc="2026-08-04T00:00:00Z",
                    **context,
                )
            )

        observations = recall_ledger.records()
        recall_evaluation = evaluate_recall_audit_evidence(
            expected_query_ids=self.frozen_query_ids,
            observations=observations,
            binding=self.binding,
            frozen_query_ids_sha256=self.frozen_query_ids_sha256,
            **self.context,
        )

        self.assertFalse(recall_evaluation.recall_audit_complete_and_passing)
        self.assertIn("OBSERVATION_CONTEXT_MISMATCH", recall_evaluation.reason_codes)

        latency_ledger = self._populate_latency_ledger(run_id="fake-pipeline-run-2")
        latency_evidence = build_stage4_latency_evidence(
            binding=self.binding, schedule=self.schedule, ledger=latency_ledger
        )
        decision = combine_stage4_decision(
            latency_evidence=latency_evidence, recall_evaluation=recall_evaluation
        )
        self.assertEqual(decision.decision_status, Stage4DecisionStatus.INCOMPLETE)

    def test_hand_fabricated_latency_evidence_cannot_combine_with_real_recall_evidence(self) -> None:
        """The exact vulnerability ADR-008's Stage-4 evidence-binding repair
        closes: this test previously fabricated a Stage4ScheduleEvaluation
        with zero connection to any ledger, schedule, or binding, combined
        it with genuine passing recall evidence, and asserted PASSING. That
        combination must now be impossible -- there is no longer any public
        constructor path that lets a caller hand-produce a "latency
        evidence" object carrying a self-declared evidence_binding_sha256
        without it having been independently verified by
        build_stage4_latency_evidence against a real schedule and ledger.
        This test proves the remaining, correct attack surface instead: a
        genuine Stage4LatencyEvidence built against a *different* binding
        (a different run_id) cannot combine with this run's real recall
        evidence into a PASSING decision."""

        recall_ledger = CanaryRecallAuditLedger(
            self.ledger_path, run_id="fake-pipeline-run-3", binding_sha256=self.binding.sha256
        )
        for query_id in self.frozen_query_ids:
            oracle_ids, candidate_ids = _fake_oracle_and_candidate_ids(
                query_id=query_id, result_cap=100, miss_one_in_n=40
            )
            recall_ledger.append(
                RecallAuditObservation(
                    query_id=query_id,
                    oracle_result_ids=oracle_ids,
                    candidate_result_ids=candidate_ids,
                    producer_run_id="fake-pipeline-run-3",
                    recorded_at_utc="2026-08-04T00:00:00Z",
                    **self.context,
                )
            )
        recall_evaluation = evaluate_recall_audit_evidence(
            expected_query_ids=self.frozen_query_ids,
            observations=recall_ledger.records(),
            binding=self.binding,
            frozen_query_ids_sha256=self.frozen_query_ids_sha256,
            **self.context,
        )
        self.assertEqual(recall_evaluation.status, EvaluationStatus.PASSING)

        unrelated_binding = replace(self.binding, run_id="a-completely-different-run")
        latency_ledger = self._populate_latency_ledger(run_id="fake-pipeline-run-3")
        mismatched_latency_evidence = build_stage4_latency_evidence(
            binding=unrelated_binding, schedule=self.schedule, ledger=latency_ledger
        )
        # The ledger itself was never bound to unrelated_binding's schedule
        # digest expectation check (schedule/ledger still match each other),
        # but the *ledger's* schedule_sha256 still matches the real schedule
        # while unrelated_binding also declares that same schedule digest --
        # so this evidence is individually PASSING; the mismatch that must
        # be caught is the binding *identity* (run_id), proven below via the
        # decision-level digest inequality, not at this construction step.
        self.assertNotEqual(mismatched_latency_evidence.evidence_binding_sha256, self.binding.sha256)

        decision = combine_stage4_decision(
            latency_evidence=mismatched_latency_evidence, recall_evaluation=recall_evaluation
        )

        self.assertEqual(decision.decision_status, Stage4DecisionStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", decision.reason_codes)
        self.assertIsNone(decision.evidence_binding_sha256)


if __name__ == "__main__":
    unittest.main()
