"""Offline route-plan tests for EXP-009 Stage 2."""

from __future__ import annotations

from dataclasses import replace
import hashlib
from pathlib import Path
import tempfile
import unittest

from vdbench.artifacts import canonical_json_bytes, sha256_file, write_dataset_artifacts
from vdbench.canary_routing import (
    CanaryRouteKind,
    build_canary_route_plan,
)
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    CandidateSelectionRecord,
    WorkloadIdentityBinding,
    build_eligible_workload_manifest,
)
from vdbench.config import EXP001_DATASET_SPEC, Metric
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import Dataset002Spec, generate_dataset002, write_dataset002_artifacts


class CanaryRoutePlanTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        root = Path(cls._temporary.name)
        cls.dataset001 = root / "dataset001"
        cls.dataset002 = root / "dataset002"
        source = generate_dataset(
            replace(
                EXP001_DATASET_SPEC,
                version="dataset001-routing-fixture-v1",
                dimensions=4,
                base_count=100,
                calibration_query_count=5,
                measured_query_count=7,
            )
        )
        write_dataset_artifacts(
            cls.dataset001,
            source,
            calibrate_thresholds(source.base_vectors, source.calibration_queries),
            boundary_fixtures(),
        )
        write_dataset002_artifacts(
            cls.dataset002,
            generate_dataset002(
                Dataset002Spec(
                    dataset_id="DATASET-002",
                    version="dataset002-routing-fixture-v1",
                    seed=20260809,
                    dimensions=4,
                    routing_query_count=600,
                    recall_audit_query_count=1200,
                    dtype="<f4",
                    distribution="independent standard normal",
                    generator="numpy.random.Generator(numpy.random.PCG64(seed))",
                )
            ),
            dataset001_dir=cls.dataset001,
        )
        identity = WorkloadIdentityBinding(
            configuration_identity="exp009-routing-config-v1",
            data_identity=(
                "dataset001-routing-fixture-v1:sha256:"
                + sha256_file(cls.dataset001 / "generation_manifest.json")
            ),
            flat_binding_id="flat-routing-binding-v1",
            hnsw_binding_id="hnsw-routing-binding-v1",
        )
        cls.manifest = build_eligible_workload_manifest(
            dataset002_dir=cls.dataset002,
            dataset001_dir=cls.dataset001,
            metric=Metric.L2,
            threshold_stratum="target-075",
            candidate_ef=800,
            last_known_good_ef=400,
            identity=identity,
            created_at_utc="2026-08-04T05:00:00Z",
        )
        cls.manifest_sha256 = hashlib.sha256(
            canonical_json_bytes(cls.manifest.to_document())
        ).hexdigest()
        candidate_ids = tuple(
            occurrence.occurrence_id
            for occurrence in cls.manifest.occurrences
            if occurrence.sequence_index % 10 == 0
        )
        cls.selection = CandidateSelectionRecord(
            schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
            selected_at_utc="2026-08-04T05:01:00Z",
            eligible_manifest_sha256=cls.manifest_sha256,
            population_count=600,
            candidate_count=60,
            candidate_fraction=0.10,
            candidate_occurrence_ids=candidate_ids,
            random_source="python.secrets.SystemRandom.sample",
            selected_before_candidate_results=True,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def test_builds_exact_disjoint_60_candidate_540_lkg_partition(self) -> None:
        plan = build_canary_route_plan(self.manifest, self.selection)
        candidate = plan.resolve("exp009-routing-000000")
        last_known_good = plan.resolve("exp009-routing-000001")

        self.assertEqual(plan.population_count, 600)
        self.assertEqual(plan.candidate_count, 60)
        self.assertEqual(len(plan.candidate_occurrence_ids), 60)
        self.assertEqual(len(plan.last_known_good_occurrence_ids), 540)
        self.assertEqual(candidate.kind, CanaryRouteKind.CANDIDATE)
        self.assertEqual(candidate.ef, 800)
        self.assertEqual(candidate.dataset_query_id, 0)
        self.assertEqual(last_known_good.kind, CanaryRouteKind.LAST_KNOWN_GOOD)
        self.assertEqual(last_known_good.ef, 400)
        self.assertEqual(last_known_good.dataset_query_id, 1)

    def test_plan_digest_is_stable_and_binds_manifest_and_selection_digests(self) -> None:
        first = build_canary_route_plan(self.manifest, self.selection)
        second = build_canary_route_plan(self.manifest, self.selection)

        self.assertEqual(first.plan_sha256, second.plan_sha256)
        self.assertEqual(first.eligible_workload_sha256, self.manifest_sha256)
        self.assertEqual(
            first.candidate_selection_sha256,
            hashlib.sha256(canonical_json_bytes(self.selection.to_document())).hexdigest(),
        )

    def test_unknown_or_noncanonical_occurrence_is_refused_without_a_route(self) -> None:
        plan = build_canary_route_plan(self.manifest, self.selection)

        unknown = plan.resolve("exp009-routing-999999")
        malformed = plan.resolve("not-a-routing-occurrence")

        self.assertFalse(unknown.accepted)
        self.assertIsNone(unknown.ef)
        self.assertEqual(unknown.reason_code, "OCCURRENCE_UNKNOWN")
        self.assertFalse(malformed.accepted)
        self.assertEqual(malformed.reason_code, "OCCURRENCE_ID_INVALID")

    def test_59_and_61_candidate_boundaries_fail_closed(self) -> None:
        for candidate_count in (59, 61):
            with self.subTest(candidate_count=candidate_count):
                invalid = replace(
                    self.selection,
                    candidate_count=candidate_count,
                    candidate_occurrence_ids=self.selection.candidate_occurrence_ids[
                        :candidate_count
                    ]
                    if candidate_count < 60
                    else self.selection.candidate_occurrence_ids + ("exp009-routing-000001",),
                )
                with self.assertRaisesRegex(ValueError, "CANDIDATE_SELECTION_INVALID"):
                    build_canary_route_plan(self.manifest, invalid)

    def test_599_and_601_population_boundaries_fail_closed(self) -> None:
        for population_count in (599, 601):
            with self.subTest(population_count=population_count):
                occurrences = self.manifest.occurrences[:population_count]
                if population_count > 600:
                    occurrences = self.manifest.occurrences + (self.manifest.occurrences[-1],)
                invalid_manifest = replace(self.manifest, occurrences=occurrences)
                with self.assertRaisesRegex(ValueError, "ELIGIBLE_WORKLOAD_INVALID"):
                    build_canary_route_plan(invalid_manifest, self.selection)

    def test_selection_manifest_digest_mismatch_fails_closed(self) -> None:
        invalid = replace(self.selection, eligible_manifest_sha256="0" * 64)

        with self.assertRaisesRegex(ValueError, "SELECTION_MANIFEST_MISMATCH"):
            build_canary_route_plan(self.manifest, invalid)

    def test_selection_outside_manifest_and_noncanonical_order_fail_closed(self) -> None:
        outside = replace(
            self.selection,
            candidate_occurrence_ids=(
                *self.selection.candidate_occurrence_ids[:-1],
                "exp009-routing-999999",
            ),
        )
        reversed_order = replace(
            self.selection,
            candidate_occurrence_ids=tuple(reversed(self.selection.candidate_occurrence_ids)),
        )

        with self.assertRaisesRegex(ValueError, "CANDIDATE_SELECTION_OUTSIDE_WORKLOAD"):
            build_canary_route_plan(self.manifest, outside)
        with self.assertRaisesRegex(ValueError, "CANDIDATE_SELECTION_ORDER_INVALID"):
            build_canary_route_plan(self.manifest, reversed_order)

    def test_forged_immutable_plan_reconstruction_fails_closed(self) -> None:
        plan = build_canary_route_plan(self.manifest, self.selection)

        with self.assertRaisesRegex(ValueError, "ROUTE_PLAN_INVALID"):
            replace(plan, candidate_occurrence_ids=frozenset())
        with self.assertRaisesRegex(ValueError, "ROUTE_PLAN_INVALID"):
            replace(plan, plan_sha256="0" * 64)
        with self.assertRaisesRegex(ValueError, "ROUTE_PLAN_INVALID"):
            replace(plan, occurrences=plan.occurrences[:-1] + (plan.occurrences[0],))


if __name__ == "__main__":
    unittest.main()
