"""TDD coverage for pure EXP-009 Stage-4 schedule evaluation."""

from __future__ import annotations

import ast
from dataclasses import replace
import hashlib
from pathlib import Path
import tempfile
import unittest

from vdbench.artifacts import canonical_json_bytes
from vdbench.canary_execution_ledger import (
    Stage4ExecutionLedger,
    Stage4ExecutionRecord,
    Stage4LedgerProgress,
    Stage4LedgerStatus,
    Stage4SlotObservation,
)
from vdbench.canary_routing import build_canary_route_plan
from vdbench.canary_schedule import build_stage4_execution_schedule
from vdbench.canary_schedule_evaluation import (
    evaluate_stage4_execution_ledger,
    evaluate_stage4_schedule_evidence,
)
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    CandidateSelectionRecord,
    EligibleOccurrence,
    EligibleWorkloadManifest,
    SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING,
    SCHEDULE_CONTROL_COUNT,
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
from vdbench.config import Metric, RESULT_LIMIT


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class CanaryScheduleEvaluationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
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
            radius=0.75,
            range_filter=0.0,
            limit=RESULT_LIMIT,
            identity=WorkloadIdentityBinding("config", "data", "flat", "hnsw"),
            vector_mapping="one_to_one_unique_dataset002_routing_vectors",
            schedule_stability=stability,
            occurrences=tuple(
                EligibleOccurrence(
                    index,
                    f"exp009-routing-{index:06d}",
                    index,
                    _sha(f"route-{index}"),
                    0.75,
                    0.0,
                    RESULT_LIMIT,
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
        # The routing builder binds selection to the manifest's canonical document.
        selection = replace(
            selection,
            eligible_manifest_sha256=hashlib.sha256(
                canonical_json_bytes(manifest.to_document())
            ).hexdigest(),
        )
        cls.schedule = build_stage4_execution_schedule(
            manifest, build_canary_route_plan(manifest, selection)
        )

    def _records(self, *, control_latency: float = 1.0) -> tuple[Stage4ExecutionRecord, ...]:
        result: list[Stage4ExecutionRecord] = []
        previous = _sha("genesis")
        for step in self.schedule.steps:
            latency = control_latency if step.control_query_id is not None else 2.0
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
            digest = _sha(f"record-{step.execution_index}")
            result.append(Stage4ExecutionRecord(observation, previous, digest))
            previous = digest
        return tuple(result)

    def _complete_progress(self, records: tuple[Stage4ExecutionRecord, ...]) -> Stage4LedgerProgress:
        return Stage4LedgerProgress(
            Stage4LedgerStatus.COMPLETE,
            len(records),
            None,
            records[-1].record_sha256,
        )

    def test_complete_stable_schedule_yields_conditional_finite_manifest_result(self) -> None:
        records = self._records()

        result = evaluate_stage4_schedule_evidence(
            schedule=self.schedule,
            progress=self._complete_progress(records),
            records=records,
        )

        self.assertTrue(result.finite_manifest_latency_applicable)
        self.assertEqual(result.reason_codes, ())
        self.assertEqual(len(result.control_sweeps), 12)
        self.assertEqual(result.baseline_median_ms, 1.0)
        self.assertEqual(result.baseline_p95_ms, 1.0)
        self.assertEqual(result.candidate_latency_count, 60)
        self.assertEqual(result.candidate_latency_max_ms, 2.0)
        self.assertAlmostEqual(result.finite_population_coverage_probability, 0.9610030335925056)
        self.assertFalse(result.recall_bound_evaluated)

    def test_control_ceiling_breach_makes_finite_manifest_result_not_applicable(self) -> None:
        records = list(self._records())
        # For 50 controls, nearest-rank p95 is rank 48; three high values are
        # therefore the smallest breach fixture (one or two remain above it).
        for breach_index in (250, 251, 252):
            records[breach_index] = replace(
                records[breach_index],
                observation=replace(records[breach_index].observation, latency_ms=11.0),
            )
        frozen = tuple(records)

        result = evaluate_stage4_schedule_evidence(
            schedule=self.schedule,
            progress=self._complete_progress(frozen),
            records=frozen,
        )

        self.assertFalse(result.finite_manifest_latency_applicable)
        self.assertIn("CONTROL_ABSOLUTE_P95_CEILING_BREACH", result.reason_codes)

    def test_incomplete_or_binding_mismatched_ledger_is_not_applicable(self) -> None:
        records = self._records()
        incomplete = evaluate_stage4_schedule_evidence(
            schedule=self.schedule,
            progress=Stage4LedgerProgress(Stage4LedgerStatus.IN_PROGRESS, 1, None, _sha("head")),
            records=records[:1],
        )
        mismatched = list(records)
        mismatched[0] = replace(
            mismatched[0], observation=replace(mismatched[0].observation, observed_ef=800)
        )
        binding = evaluate_stage4_schedule_evidence(
            schedule=self.schedule,
            progress=self._complete_progress(tuple(mismatched)),
            records=tuple(mismatched),
        )

        self.assertFalse(incomplete.finite_manifest_latency_applicable)
        self.assertIn("LEDGER_NOT_COMPLETE", incomplete.reason_codes)
        self.assertFalse(binding.finite_manifest_latency_applicable)
        self.assertIn("RECORD_SCHEDULE_MISMATCH", binding.reason_codes)

    def test_verified_ledger_wrapper_refuses_incomplete_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            private = Path(temporary) / "private"
            private.mkdir(mode=0o700)
            ledger = Stage4ExecutionLedger(
                private / "stage4.sqlite3",
                run_id="exp009-evaluator-wrapper-test",
                schedule=self.schedule,
            )
            observation = self._records()[0].observation
            self.assertTrue(ledger.append(observation).accepted)

            result = evaluate_stage4_execution_ledger(
                schedule=self.schedule,
                ledger=ledger,
            )

        self.assertFalse(result.finite_manifest_latency_applicable)
        self.assertEqual(result.reason_codes, ("LEDGER_NOT_COMPLETE",))

    def test_evaluator_has_no_milvus_or_activation_imports(self) -> None:
        source = Path("src/vdbench/canary_schedule_evaluation.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = {
            node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        }
        forbidden = {"milvus", "milvus_serving", "canary_activation", "canary_route_authority"}
        self.assertFalse(any(name.split(".")[-1] in forbidden for name in imports))


if __name__ == "__main__":
    unittest.main()
