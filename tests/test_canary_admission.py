"""Fail-closed tests for the EXP-009 Stage-4 admission boundary."""

from __future__ import annotations

from dataclasses import replace
import ast
import hashlib
from pathlib import Path
import tempfile
import unittest

from vdbench.artifacts import canonical_json_bytes, sha256_file, write_dataset_artifacts
from vdbench.canary_admission import (
    Stage4AdmissionRequest,
    Stage4RepositoryEvidence,
    Stage4RuntimeReadiness,
    evaluate_stage4_admission,
)
from vdbench.canary_routing import build_canary_route_plan
from vdbench.canary_route_state import RouteStateBinding
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    CandidateSelectionRecord,
    WorkloadIdentityBinding,
    build_eligible_workload_manifest,
)
from vdbench.config import EXP001_DATASET_SPEC, Metric
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import Dataset002Spec, generate_dataset002, write_dataset002_artifacts
from vdbench.drift import build_evidence_provenance
from vdbench.policy import (
    PolicyAction,
    PolicyDecision,
    PolicyMode,
    QualificationResult,
    SafetyGateResult,
)


def _sha(character: str) -> str:
    return character * 64


class CanaryAdmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        root = Path(cls._temporary.name)
        dataset001 = root / "dataset001"
        dataset002 = root / "dataset002"
        source = generate_dataset(
            replace(
                EXP001_DATASET_SPEC,
                version="dataset001-admission-fixture-v1",
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
                    version="dataset002-admission-fixture-v1",
                    seed=20260814,
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
            configuration_identity="exp009-admission-config-v1",
            data_identity=(
                "dataset001-admission-fixture-v1:sha256:"
                + sha256_file(dataset001 / "generation_manifest.json")
            ),
            flat_binding_id="flat-admission-binding-v1",
            hnsw_binding_id="hnsw-admission-binding-v1",
        )
        cls.manifest = build_eligible_workload_manifest(
            dataset002_dir=dataset002,
            dataset001_dir=dataset001,
            metric=Metric.L2,
            threshold_stratum="target-075",
            candidate_ef=800,
            last_known_good_ef=400,
            identity=identity,
            created_at_utc="2026-08-04T12:00:00Z",
        )
        manifest_sha = hashlib.sha256(
            canonical_json_bytes(cls.manifest.to_document())
        ).hexdigest()
        cls.selection = CandidateSelectionRecord(
            schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
            selected_at_utc="2026-08-04T12:01:00Z",
            eligible_manifest_sha256=manifest_sha,
            population_count=600,
            candidate_count=60,
            candidate_fraction=0.10,
            candidate_occurrence_ids=tuple(
                item.occurrence_id
                for item in cls.manifest.occurrences
                if item.sequence_index % 10 == 0
            ),
            random_source="python.secrets.SystemRandom.sample",
            selected_before_candidate_results=True,
        )
        cls.plan = build_canary_route_plan(cls.manifest, cls.selection)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def _binding(self) -> RouteStateBinding:
        return RouteStateBinding(
            metric=self.plan.metric,
            threshold_stratum=self.plan.threshold_stratum,
            last_known_good_ef=self.plan.last_known_good_ef,
            configuration_identity=self.plan.configuration_identity,
            data_identity=self.plan.data_identity,
            flat_binding_id=self.plan.flat_binding_id,
            hnsw_binding_id=self.plan.hnsw_binding_id,
        )

    def _decision(self, *, provenance_stratum: str | None = None) -> PolicyDecision:
        provenance = build_evidence_provenance(
            metric=self.plan.metric,
            threshold_stratum=(
                self.plan.threshold_stratum
                if provenance_stratum is None
                else provenance_stratum
            ),
            reference_window_id="reference-window-001",
            current_window_id="current-window-001",
            reference_manifest_sha256=_sha("1"),
            current_manifest_sha256=_sha("2"),
            configuration_identity=self.plan.configuration_identity,
            data_identity=self.plan.data_identity,
            flat_binding_id=self.plan.flat_binding_id,
            hnsw_binding_id=self.plan.hnsw_binding_id,
            reference_audit_ids=tuple(f"reference-audit-{index:02d}" for index in range(50)),
            reference_audit_rank_digests=tuple(_sha("3") for _ in range(50)),
            current_audit_ids=tuple(f"current-audit-{index:02d}" for index in range(50)),
            current_audit_rank_digests=tuple(_sha("4") for _ in range(50)),
        )
        return PolicyDecision(
            action=PolicyAction.START_CANARY,
            current_ef=400,
            candidate_ef=800,
            last_known_good_ef=400,
            expected_mean_recall=0.99,
            expected_recall_lower_bound_95=0.98,
            expected_p95_latency_ms=4.0,
            expected_latency_upper_bound_95_ms=5.0,
            predicted_recall_improvement=0.02,
            predicted_latency_reduction_fraction=None,
            reason="QUALITY_DRIFT_RECOVERY",
            detector_confidence=0.999,
            detector_magnitude=2.0,
            safety_gate_results=(SafetyGateResult("PRE_ACTION", True, "passed"),),
            mode=PolicyMode.CANARY_ENABLED,
            audit_id="policy-audit-admission-001",
            evidence_provenance=provenance,
        )

    def _qualification(self) -> QualificationResult:
        return QualificationResult(
            qualified=True,
            ef=400,
            reasons=(),
            metric=self.plan.metric,
            threshold_stratum=self.plan.threshold_stratum,
            configuration_identity=self.plan.configuration_identity,
            index_identity=self.plan.hnsw_binding_id,
            data_identity=self.plan.data_identity,
            qualifying_window_ids=("lkg-window-001", "lkg-window-002"),
        )

    def _request(self, **changes: object) -> Stage4AdmissionRequest:
        values: dict[str, object] = {
            "manifest": self.manifest,
            "selection": self.selection,
            "plan": self.plan,
            "policy_decision": self._decision(),
            "qualification": self._qualification(),
            "repository": Stage4RepositoryEvidence(
                commit_sha="a" * 40,
                clean=True,
                observed_at_utc="2026-08-04T12:02:00Z",
            ),
            "runtime": Stage4RuntimeReadiness(
                binding=self._binding(),
                serving_preflight_complete=True,
                observed_at_utc="2026-08-04T12:03:00Z",
                reason_codes=(),
            ),
        }
        values.update(changes)
        return Stage4AdmissionRequest(**values)

    def test_exact_complete_evidence_admits_without_any_side_effect_port(self) -> None:
        result = evaluate_stage4_admission(self._request())

        self.assertTrue(result.admitted)
        self.assertEqual(result.reason_codes, ())
        self.assertEqual(result.plan_sha256, self.plan.plan_sha256)
        self.assertEqual(result.policy_audit_id, "policy-audit-admission-001")

    def test_dirty_revision_and_invalid_artifact_plan_fail_closed(self) -> None:
        dirty = evaluate_stage4_admission(
            self._request(repository=Stage4RepositoryEvidence(
                commit_sha="a" * 40,
                clean=False,
                observed_at_utc="2026-08-04T12:02:00Z",
            ))
        )
        substituted_selection = replace(
            self.selection,
            candidate_occurrence_ids=tuple(
                item.occurrence_id
                for item in self.manifest.occurrences
                if item.sequence_index % 10 == 1
            ),
        )
        substituted = evaluate_stage4_admission(
            self._request(plan=build_canary_route_plan(self.manifest, substituted_selection))
        )

        self.assertFalse(dirty.admitted)
        self.assertIn("REPOSITORY_NOT_CLEAN", dirty.reason_codes)
        self.assertFalse(substituted.admitted)
        self.assertIn("PLAN_REBUILD_MISMATCH", substituted.reason_codes)

    def test_policy_action_mode_provenance_and_gate_fail_closed(self) -> None:
        invalid_action = evaluate_stage4_admission(
            self._request(policy_decision=replace(self._decision(), action=PolicyAction.NO_CHANGE))
        )
        dry_run = evaluate_stage4_admission(
            self._request(policy_decision=replace(self._decision(), mode=PolicyMode.DRY_RUN))
        )
        failed_gate = evaluate_stage4_admission(
            self._request(policy_decision=replace(
                self._decision(),
                safety_gate_results=(SafetyGateResult("PRE_ACTION", False, "failed"),),
            ))
        )
        invalid_provenance = evaluate_stage4_admission(
            self._request(policy_decision=replace(self._decision(), evidence_provenance=None))
        )

        self.assertIn("POLICY_ACTION_INVALID", invalid_action.reason_codes)
        self.assertIn("POLICY_MODE_INVALID", dry_run.reason_codes)
        self.assertIn("POLICY_SAFETY_GATES_FAILED", failed_gate.reason_codes)
        self.assertIn("POLICY_PROVENANCE_INVALID", invalid_provenance.reason_codes)

    def test_malformed_nested_evidence_fails_closed_without_raising(self) -> None:
        malformed_gate = evaluate_stage4_admission(
            self._request(policy_decision=replace(
                self._decision(), safety_gate_results=(object(),)
            ))
        )
        malformed_lkg = evaluate_stage4_admission(
            self._request(qualification=replace(self._qualification(), reasons=None))
        )
        malformed_runtime = evaluate_stage4_admission(
            self._request(runtime=Stage4RuntimeReadiness(
                binding=self._binding(),
                serving_preflight_complete=True,
                observed_at_utc="2026-08-04T12:03:00Z",
                reason_codes=("lowercase reason",),
            ))
        )

        self.assertIn("POLICY_SAFETY_GATES_FAILED", malformed_gate.reason_codes)
        self.assertIn("LAST_KNOWN_GOOD_REASONS_PRESENT", malformed_lkg.reason_codes)
        self.assertIn("RUNTIME_REASON_CODES_INVALID", malformed_runtime.reason_codes)

    def test_lkg_runtime_and_identity_mismatches_fail_closed(self) -> None:
        unqualified = evaluate_stage4_admission(
            self._request(qualification=replace(self._qualification(), qualified=False))
        )
        lkg_identity = evaluate_stage4_admission(
            self._request(qualification=replace(
                self._qualification(), index_identity="hnsw-other-binding-v1"
            ))
        )
        runtime_incomplete = evaluate_stage4_admission(
            self._request(runtime=Stage4RuntimeReadiness(
                binding=self._binding(),
                serving_preflight_complete=False,
                observed_at_utc="2026-08-04T12:03:00Z",
                reason_codes=("STACK_HEALTH_UNHEALTHY",),
            ))
        )
        runtime_identity = evaluate_stage4_admission(
            self._request(runtime=Stage4RuntimeReadiness(
                binding=replace(self._binding(), data_identity="dataset-other"),
                serving_preflight_complete=True,
                observed_at_utc="2026-08-04T12:03:00Z",
                reason_codes=(),
            ))
        )

        self.assertIn("LAST_KNOWN_GOOD_NOT_QUALIFIED", unqualified.reason_codes)
        self.assertIn("LAST_KNOWN_GOOD_BINDING_MISMATCH", lkg_identity.reason_codes)
        self.assertIn("RUNTIME_PREFLIGHT_INCOMPLETE", runtime_incomplete.reason_codes)
        self.assertIn("RUNTIME_BINDING_MISMATCH", runtime_identity.reason_codes)

    def test_any_metric_stratum_or_transition_mismatch_is_rejected(self) -> None:
        metric = evaluate_stage4_admission(
            self._request(policy_decision=replace(self._decision(), current_ef=200))
        )
        stratum = evaluate_stage4_admission(
            self._request(policy_decision=self._decision(provenance_stratum="target-025"))
        )
        runtime_metric = evaluate_stage4_admission(
            self._request(runtime=Stage4RuntimeReadiness(
                binding=replace(self._binding(), metric=Metric.COSINE),
                serving_preflight_complete=True,
                observed_at_utc="2026-08-04T12:03:00Z",
                reason_codes=(),
            ))
        )

        self.assertIn("POLICY_TRANSITION_MISMATCH", metric.reason_codes)
        self.assertIn("POLICY_PROVENANCE_BINDING_MISMATCH", stratum.reason_codes)
        self.assertIn("RUNTIME_BINDING_MISMATCH", runtime_metric.reason_codes)

    def test_source_has_no_live_execution_or_authority_import(self) -> None:
        source_path = Path(__file__).parents[1] / "src" / "vdbench" / "canary_admission.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        imported = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            node.module.split(".")[-1]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        )
        self.assertFalse(
            {
                "milvus",
                "milvus_serving",
                "milvus_host_executor",
                "canary_activation",
                "canary_rollback",
                "canary_route_authority",
                "actuation",
            }
            & imported
        )


if __name__ == "__main__":
    unittest.main()
