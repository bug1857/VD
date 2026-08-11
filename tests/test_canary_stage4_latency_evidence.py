"""TDD coverage for the ADR-008 Stage-4 latency evidence-binding wrapper.

``canary_schedule_evaluation.py`` stays untouched/external per its own
docstring and per ``canary_stage4_decision.py``'s existing design note; this
module only wraps its real, ledger-verified output with the immutable
``Stage4EvidenceBinding`` digest and the verified execution-ledger chain-head
digest that ADR-008's Stage-4 evidence-binding repair requires before latency
evidence may ever be combined with recall evidence.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
from pathlib import Path
import tempfile
import unittest

from vdbench.artifacts import canonical_json_bytes
from vdbench.canary_execution_ledger import Stage4ExecutionLedger, Stage4SlotObservation
from vdbench.canary_recall_audit_evaluation import EvaluationStatus
from vdbench.canary_routing import build_canary_route_plan
from vdbench.canary_schedule import build_stage4_execution_schedule
from vdbench.canary_stage4_evidence_binding import Stage4EvidenceBinding
from vdbench.canary_stage4_latency_evidence import (
    Stage4LatencyEvidence,
    build_stage4_latency_evidence,
)
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
from vdbench.config import IndexTrack, Metric, RESULT_LIMIT, SearchConfiguration


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _build_schedule(*, radius: float = 0.75):
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
        metric=Metric.L2,
        threshold_stratum="target-075",
        candidate_ef=800,
        last_known_good_ef=400,
        radius=radius,
        range_filter=0.0,
        limit=RESULT_LIMIT,
        identity=WorkloadIdentityBinding("config", "data", "flat", "hnsw"),
        vector_mapping="one_to_one_unique_dataset002_routing_vectors",
        schedule_stability=stability,
        occurrences=tuple(
            EligibleOccurrence(
                index, f"exp009-routing-{index:06d}", index, _sha(f"route-{index}"), radius, 0.0, RESULT_LIMIT
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


def _binding_for(schedule, **overrides) -> Stage4EvidenceBinding:
    fields = dict(
        run_id="exp009-stage4-latency-test",
        source_revision="0" * 40,
        metric=schedule.metric,
        threshold_stratum=schedule.threshold_stratum,
        current_ef=schedule.last_known_good_ef,
        candidate_ef=schedule.candidate_ef,
        last_known_good_ef=schedule.last_known_good_ef,
        candidate_search_configuration=SearchConfiguration(
            metric=schedule.metric,
            threshold_label=schedule.threshold_stratum,
            radius=0.75,
            index_track=IndexTrack.HNSW,
            ef=schedule.candidate_ef,
            limit=RESULT_LIMIT,
            consistency_level="Strong",
        ),
        identity=WorkloadIdentityBinding("config", "data", "flat", "hnsw"),
        dataset002_manifest_sha256=_sha("dataset002"),
        frozen_recall_audit_ids_sha256=_sha("frozen-ids"),
        eligible_workload_sha256=_sha("eligible-workload"),
        candidate_selection_sha256=_sha("candidate-selection"),
        execution_schedule_sha256=schedule.schedule_sha256,
        recall_evidence_schema_version="recall-audit-hoeffding-1200-v1",
        latency_evidence_schema_version="exp009-stage4-execution-schedule-v1",
    )
    fields.update(overrides)
    return Stage4EvidenceBinding(**fields)


class Stage4LatencyEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schedule = _build_schedule()

    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)
        private = Path(self._tempdir.name) / "private"
        private.mkdir(mode=0o700)
        self.ledger_path = private / "stage4.sqlite3"

    def _ledger(self, *, run_id: str = "exp009-stage4-latency-test") -> Stage4ExecutionLedger:
        return Stage4ExecutionLedger(self.ledger_path, run_id=run_id, schedule=self.schedule)

    def test_changing_occurrence_radius_changes_schedule_sha256(self) -> None:
        """Non-regression guard for the radius-binding-omission repair: the
        latency side was found NOT exploitable specifically because radius is
        baked into every routing step's hashed document, which feeds
        schedule_sha256. This locks that property in so a future refactor of
        canary_schedule.py cannot silently drop it."""
        altered = _build_schedule(radius=0.50)
        self.assertNotEqual(self.schedule.schedule_sha256, altered.schedule_sha256)

        ledger = self._ledger()
        # The unchanged, original binding must reject the altered schedule.
        binding = _binding_for(self.schedule)
        result = build_stage4_latency_evidence(binding=binding, schedule=altered, ledger=ledger)
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)

    def test_mismatched_execution_schedule_binding_is_incomplete(self) -> None:
        ledger = self._ledger()
        binding = _binding_for(self.schedule, execution_schedule_sha256="f" * 64)

        result = build_stage4_latency_evidence(binding=binding, schedule=self.schedule, ledger=ledger)

        self.assertIsInstance(result, Stage4LatencyEvidence)
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)
        self.assertIsNone(result.evidence_binding_sha256)
        self.assertIsNone(result.execution_ledger_chain_head_sha256)
        self.assertIsNone(result.schedule_evaluation)

    def test_execution_ledger_run_lineage_cannot_be_relabelled(self) -> None:
        ledger = self._ledger(run_id="different-stage4-run")
        binding = _binding_for(self.schedule)

        result = build_stage4_latency_evidence(
            binding=binding, schedule=self.schedule, ledger=ledger
        )

        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertEqual(result.reason_codes, ("EVIDENCE_BINDING_MISMATCH",))
        self.assertIsNone(result.evidence_binding_sha256)
        self.assertIsNone(result.execution_ledger_chain_head_sha256)

    def test_mismatched_metric_binding_is_incomplete(self) -> None:
        ledger = self._ledger()
        binding = _binding_for(
            self.schedule,
            metric=Metric.COSINE,
            candidate_search_configuration=SearchConfiguration(
                metric=Metric.COSINE,
                threshold_label=self.schedule.threshold_stratum,
                radius=0.5,
                index_track=IndexTrack.HNSW,
                ef=self.schedule.candidate_ef,
                limit=RESULT_LIMIT,
                consistency_level="Strong",
            ),
        )

        result = build_stage4_latency_evidence(binding=binding, schedule=self.schedule, ledger=ledger)

        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)

    def test_mismatched_candidate_ef_binding_is_incomplete(self) -> None:
        ledger = self._ledger()
        binding = _binding_for(
            self.schedule,
            candidate_ef=1600,
            candidate_search_configuration=SearchConfiguration(
                metric=self.schedule.metric,
                threshold_label=self.schedule.threshold_stratum,
                radius=0.75,
                index_track=IndexTrack.HNSW,
                ef=1600,
                limit=RESULT_LIMIT,
                consistency_level="Strong",
            ),
        )

        result = build_stage4_latency_evidence(binding=binding, schedule=self.schedule, ledger=ledger)

        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)

    def test_incomplete_ledger_with_matching_binding_is_failing_not_incomplete(self) -> None:
        """Preserves canary_stage4_decision.py's existing, deliberate design:
        Stage4ScheduleEvaluation collapses "incomplete" and "ceiling breach"
        into one boolean, so a matched-binding-but-incomplete-ledger result
        maps to FAILING here too, not INCOMPLETE. Only a binding mismatch
        (checked before the ledger is even read) is INCOMPLETE at this layer."""
        ledger = self._ledger()
        binding = _binding_for(self.schedule, run_id=ledger.run_id)

        result = build_stage4_latency_evidence(binding=binding, schedule=self.schedule, ledger=ledger)

        self.assertEqual(result.status, EvaluationStatus.FAILING)
        self.assertIn("LEDGER_NOT_COMPLETE", result.reason_codes)
        # Binding is meaningful metadata here: it matched, evidence was just incomplete.
        self.assertEqual(result.evidence_binding_sha256, binding.sha256)
        self.assertIsNotNone(result.execution_ledger_chain_head_sha256)
        self.assertIsNotNone(result.schedule_evaluation)

    def test_fully_populated_ledger_yields_passing_evidence_with_real_chain_head(self) -> None:
        ledger = self._ledger(run_id="exp009-stage4-latency-full")
        for step in self.schedule.steps:
            latency = 1.0 if step.control_query_id is not None else 2.0
            observation = Stage4SlotObservation(
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
            start_result = ledger.start_slot(
                step.execution_index,
                started_monotonic_ns=step.execution_index * 10,
                recorded_at_utc="2026-08-04T15:01:00Z",
            )
            self.assertTrue(start_result.accepted)
            self.assertTrue(
                ledger.complete_slot(
                    observation, started_record_sha256=start_result.start_sha256
                ).accepted
            )
        binding = _binding_for(self.schedule, run_id=ledger.run_id)

        result = build_stage4_latency_evidence(binding=binding, schedule=self.schedule, ledger=ledger)

        self.assertEqual(result.status, EvaluationStatus.PASSING)
        self.assertEqual(result.reason_codes, ())
        self.assertEqual(result.evidence_binding_sha256, binding.sha256)
        self.assertEqual(
            result.execution_ledger_chain_head_sha256, ledger.progress().chain_head_sha256
        )
        self.assertTrue(result.schedule_evaluation.finite_manifest_latency_applicable)

    def test_wrong_type_inputs_raise_type_error(self) -> None:
        ledger = self._ledger()
        binding = _binding_for(self.schedule)
        with self.assertRaises(TypeError):
            build_stage4_latency_evidence(binding="not-a-binding", schedule=self.schedule, ledger=ledger)
        with self.assertRaises(TypeError):
            build_stage4_latency_evidence(binding=binding, schedule="not-a-schedule", ledger=ledger)
        with self.assertRaises(TypeError):
            build_stage4_latency_evidence(binding=binding, schedule=self.schedule, ledger="not-a-ledger")

    def test_module_has_no_pymilvus_or_actuation_import(self) -> None:
        source = Path("src/vdbench/canary_stage4_latency_evidence.py").read_text(encoding="utf-8")
        forbidden = (
            "pymilvus", "milvus_serving", "canary_activation", "canary_route_authority",
            "canary_approval", "milvus_actuation",
        )
        for name in forbidden:
            self.assertNotIn(name, source)


if __name__ == "__main__":
    unittest.main()
