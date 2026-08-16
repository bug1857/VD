"""TDD coverage for EXP-009's immutable Stage-4 serial schedule."""

from __future__ import annotations

import ast
import hashlib
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from vdbench.artifacts import canonical_json_bytes, sha256_file, write_dataset_artifacts
from vdbench.canary_routing import CanaryRouteKind, build_canary_route_plan
from vdbench.canary_schedule import (
    Stage4ScheduleStepKind,
    build_stage4_execution_schedule,
)
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    CandidateSelectionRecord,
    WorkloadIdentityBinding,
    build_eligible_workload_manifest,
)
from vdbench.config import EXP001_DATASET_SPEC, Metric
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import (
    Dataset002Spec,
    generate_dataset002,
    write_dataset002_artifacts,
)


class CanaryScheduleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        root = Path(cls._temporary.name)
        dataset001 = root / "dataset001"
        dataset002 = root / "dataset002"
        source = generate_dataset(
            replace(
                EXP001_DATASET_SPEC,
                version="dataset001-schedule-fixture-v1",
                dimensions=4,
                base_count=100,
                calibration_query_count=5,
                measured_query_count=7,
            )
        )
        write_dataset_artifacts(
            dataset001,
            source,
            calibrate_thresholds(source.base_vectors, source.calibration_queries),
            boundary_fixtures(),
        )
        write_dataset002_artifacts(
            dataset002,
            generate_dataset002(
                Dataset002Spec(
                    dataset_id="DATASET-002",
                    version="dataset002-schedule-fixture-v1",
                    seed=20260815,
                    dimensions=4,
                    routing_query_count=600,
                    recall_audit_query_count=1200,
                    dtype="<f4",
                    distribution="independent standard normal",
                    generator="numpy.random.Generator(numpy.random.PCG64(seed))",
                )
            ),
            dataset001_dir=dataset001,
        )
        identity = WorkloadIdentityBinding(
            configuration_identity="exp009-schedule-config-v1",
            data_identity=(
                "dataset001-schedule-fixture-v1:sha256:"
                + sha256_file(dataset001 / "generation_manifest.json")
            ),
            flat_binding_id="flat-schedule-binding-v1",
            hnsw_binding_id="hnsw-schedule-binding-v1",
        )
        cls.manifest = build_eligible_workload_manifest(
            dataset002_dir=dataset002,
            dataset001_dir=dataset001,
            metric=Metric.L2,
            threshold_stratum="target-075",
            candidate_ef=800,
            last_known_good_ef=400,
            identity=identity,
            created_at_utc="2026-08-04T13:00:00Z",
        )
        manifest_sha = hashlib.sha256(
            canonical_json_bytes(cls.manifest.to_document())
        ).hexdigest()
        selection = CandidateSelectionRecord(
            schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
            selected_at_utc="2026-08-04T13:01:00Z",
            eligible_manifest_sha256=manifest_sha,
            population_count=600,
            candidate_count=60,
            candidate_fraction=0.10,
            candidate_occurrence_ids=tuple(
                occurrence.occurrence_id
                for occurrence in cls.manifest.occurrences
                if occurrence.sequence_index % 10 == 0
            ),
            random_source="python.secrets.SystemRandom.sample",
            selected_before_candidate_results=True,
        )
        cls.plan = build_canary_route_plan(cls.manifest, selection)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def test_builds_exact_1200_slot_serial_schedule(self) -> None:
        schedule = build_stage4_execution_schedule(self.manifest, self.plan)

        self.assertEqual(schedule.plan_sha256, self.plan.plan_sha256)
        self.assertEqual(len(schedule.steps), 1200)
        self.assertEqual(
            [step.execution_index for step in schedule.steps], list(range(1200))
        )
        controls = [
            step for step in schedule.steps if step.kind is Stage4ScheduleStepKind.CONTROL
        ]
        routes = [
            step for step in schedule.steps if step.kind is Stage4ScheduleStepKind.ROUTING
        ]
        self.assertEqual(len(controls), 600)
        self.assertEqual(len(routes), 600)
        self.assertEqual(
            [step.routing_sequence_index for step in routes], list(range(600))
        )
        self.assertEqual(
            sum(step.route_kind is CanaryRouteKind.CANDIDATE for step in routes), 60
        )
        self.assertEqual(
            sum(step.route_kind is CanaryRouteKind.LAST_KNOWN_GOOD for step in routes), 540
        )
        self.assertEqual(
            [step.expected_ef for step in routes if step.route_kind is CanaryRouteKind.CANDIDATE],
            [800] * 60,
        )
        self.assertEqual(
            [step.expected_ef for step in routes if step.route_kind is CanaryRouteKind.LAST_KNOWN_GOOD],
            [400] * 540,
        )
        self.assertEqual(
            [step.execution_index for step in routes[:3]], [150, 151, 152]
        )
        self.assertEqual(
            [step.execution_index for step in routes[-3:]], [997, 998, 999]
        )
        for step in routes:
            resolution = self.plan.resolve(step.occurrence_id)
            self.assertTrue(resolution.accepted)
            self.assertEqual(step.dataset_query_id, resolution.dataset_query_id)
            self.assertEqual(step.route_kind, resolution.kind)
            self.assertEqual(step.expected_ef, resolution.ef)
        for sweep_index in range(12):
            sweep = [step for step in controls if step.sweep_index == sweep_index]
            self.assertEqual(
                [step.control_query_id for step in sweep], list(range(600, 650))
            )
            self.assertEqual([step.expected_ef for step in sweep], [400] * 50)

    def test_rejects_manifest_plan_binding_mismatch(self) -> None:
        mismatched_manifest = replace(
            self.manifest,
            identity=replace(
                self.manifest.identity,
                configuration_identity="different-configuration-identity",
            ),
        )

        with self.assertRaisesRegex(ValueError, "SCHEDULE_PLAN_MANIFEST_MISMATCH"):
            build_stage4_execution_schedule(mismatched_manifest, self.plan)

    def test_direct_schedule_digest_tampering_fails_closed(self) -> None:
        schedule = build_stage4_execution_schedule(self.manifest, self.plan)

        with self.assertRaisesRegex(ValueError, "STAGE4_SCHEDULE_INVALID"):
            replace(schedule, schedule_sha256="0" * 64)

    def test_schedule_module_stays_pure_and_has_no_raw_vector_field(self) -> None:
        source = Path("src/vdbench/canary_schedule.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_modules = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        forbidden = {
            "milvus",
            "milvus_serving",
            "canary_activation",
            "canary_route_authority",
            "canary_query_source",
            "pathlib",
            "os",
        }
        self.assertFalse(
            any(module.split(".")[-1] in forbidden for module in imported_modules)
        )
        self.assertNotIn("vector:", source)


if __name__ == "__main__":
    unittest.main()
