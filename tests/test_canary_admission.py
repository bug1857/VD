"""Adversarial tests for the Phase-3 D3-B Stage-4 admission boundary."""

from __future__ import annotations

import ast
from dataclasses import fields, replace
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from vdbench.artifacts import canonical_json_bytes, sha256_file, write_dataset_artifacts
from vdbench.canary_admission import (
    Stage4AdmissionReceipt,
    Stage4AdmissionRequest,
    Stage4LkgAuthorityPair,
    Stage4RepositoryEvidence,
    bind_stage4_lkg_authority,
    evaluate_stage4_admission,
)
from vdbench.canary_route_state import RouteStateBinding
from vdbench.canary_routing import build_canary_route_plan
from vdbench.canary_runtime_types import Stage4RuntimeReadiness
from vdbench.canary_schedule import build_stage4_execution_schedule
from vdbench.canary_stage4_evidence_binding import Stage4EvidenceBinding
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    CandidateSelectionRecord,
    WorkloadIdentityBinding,
    build_eligible_workload_manifest,
)
from vdbench.config import EXP001_DATASET_SPEC, IndexTrack, Metric, SearchConfiguration
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import Dataset002Spec, generate_dataset002, write_dataset002_artifacts
from vdbench.drift import build_evidence_provenance
from vdbench.lkg_phase2_readiness_ledger import Phase2ReadinessLedger
from vdbench.lkg_phase3_authority import LkgPhase3Authority, resolve_lkg_phase3_authority
from vdbench.lkg_phase3_persistence import (
    LkgPhase3AuthorityReferenceStore,
    PersistedLkgPhase3AuthorityReference,
    VerifiedLatestLkgPhase3AuthorityReference,
)
from vdbench.lkg_qualification_evaluation import (
    LkgQualificationEvaluation,
    LkgQualificationStatus,
)
from vdbench.lkg_qualification_evaluation_ledger import LkgQualificationEvaluationLedger
from vdbench.lkg_qualification_ledger import LkgQualificationLedger
from vdbench.lkg_run_binding import LkgRunBinding
from vdbench.policy import (
    PolicyAction,
    PolicyDecision,
    PolicyMode,
    QualificationResult,
    SafetyGateResult,
)
from vdbench.search_configuration_digest import search_configuration_sha256


def _sha(character: str) -> str:
    return character * 64


def _forge_frozen(value: object, **changes: object) -> object:
    forged = object.__new__(type(value))
    for field in fields(value):
        object.__setattr__(
            forged,
            field.name,
            changes.get(field.name, getattr(value, field.name)),
        )
    return forged


def _forge_verified_latest(
    reference: PersistedLkgPhase3AuthorityReference,
) -> VerifiedLatestLkgPhase3AuthorityReference:
    forged = object.__new__(VerifiedLatestLkgPhase3AuthorityReference)
    object.__setattr__(forged, "_reference", reference)
    return forged


def _phase3_authority(
    *,
    plan: object,
    radius: float,
    identifier: int,
) -> LkgPhase3Authority:
    search_configuration = SearchConfiguration(
        metric=plan.metric,
        threshold_label=plan.threshold_stratum,
        radius=radius,
        index_track=IndexTrack.HNSW,
        ef=plan.last_known_good_ef,
        limit=100,
        consistency_level="Strong",
    )
    run_binding = LkgRunBinding(
        run_id=f"phase3-admission-run-{identifier:03d}",
        producer_identity="checkpoint-a-producer-v1",
        search_configuration=search_configuration,
        collection_name=f"phase3-admission-hnsw-{identifier:03d}",
        base_data_identity=plan.data_identity,
        index_identity=plan.hnsw_binding_id,
        qualification_dataset_id="DATASET-003",
        qualification_dataset_version="DATASET-003-v1",
        qualification_manifest_sha256=_sha("a"),
        qualification_query_role="lkg_qualification",
        qualification_query_id_array_sha256=_sha("b"),
        qualification_ordered_query_ids_sha256=_sha("c"),
        qualification_query_array_sha256=_sha("d"),
        qualification_expected_query_count=2_400,
        environment_identity="ENV-001-verified",
        source_revision=f"{identifier:040x}",
    )
    evaluation_digest = f"{identifier:064x}"
    evaluation = mock.create_autospec(LkgQualificationEvaluation, instance=True)
    values = {
        "canonical_evaluation_digest": evaluation_digest,
        "source_run_id": run_binding.run_id,
        "source_run_binding_sha256": run_binding.sha256,
        "source_run_seal_digest": _sha("1"),
        "source_sealed_phase1_chain_head_sha256": _sha("2"),
        "phase2_source_binding_digest": _sha("3"),
        "evaluated_ef": plan.last_known_good_ef,
        "search_configuration_digest": search_configuration_sha256(search_configuration),
        "qualification_dataset_id": run_binding.qualification_dataset_id,
        "qualification_dataset_version": run_binding.qualification_dataset_version,
        "qualification_manifest_sha256": run_binding.qualification_manifest_sha256,
        "qualification_query_role": run_binding.qualification_query_role,
        "qualification_ordered_query_ids_sha256": (
            run_binding.qualification_ordered_query_ids_sha256
        ),
        "status": LkgQualificationStatus.PASSING,
        "qualified": True,
        "status_reason_codes": (),
        "evaluator_identity": "checkpoint-c-evaluator-v1",
        "evaluator_source_revision": "checkpoint-c-revision-v1",
        "evaluated_at_utc": "2026-08-08T12:00:00.000000Z",
    }
    for field, value in values.items():
        setattr(evaluation, field, value)
    ledger = mock.create_autospec(LkgQualificationEvaluationLedger, instance=True)
    ledger.get_final_evaluation.return_value = evaluation
    ledger.evaluate_and_finalize.return_value = evaluation
    resolution = resolve_lkg_phase3_authority(
        evaluation_ledger=ledger,
        phase1_ledger=mock.create_autospec(LkgQualificationLedger, instance=True),
        phase2_readiness_ledger=mock.create_autospec(
            Phase2ReadinessLedger, instance=True
        ),
        run_binding=run_binding,
        expected_canonical_evaluation_digest=evaluation_digest,
    )
    if resolution.authority is None:
        raise AssertionError(f"D1 fixture failed: {resolution.reason_codes}")
    return resolution.authority


class CanaryAdmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls._temporary.name)
        dataset001 = cls.root / "dataset001"
        dataset002 = cls.root / "dataset002"
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
        cls.schedule = build_stage4_execution_schedule(cls.manifest, cls.plan)
        cls.authority = _phase3_authority(
            plan=cls.plan,
            radius=cls.manifest.radius,
            identifier=1,
        )
        with LkgPhase3AuthorityReferenceStore(cls.root / "default-phase3.db") as store:
            cls.persisted = store.append(
                cls.authority,
                persisted_at_utc="2026-08-08T13:00:00.000000Z",
            ).reference
            verified = store.load_verified_latest()
        assert verified is not None
        cls.verified_latest = verified
        cls.lkg_pair = bind_stage4_lkg_authority(
            authority=cls.authority,
            verified_latest_reference=verified,
        )

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
            reference_audit_ids=tuple(
                f"reference-audit-{index:02d}" for index in range(50)
            ),
            reference_audit_rank_digests=tuple(_sha("3") for _ in range(50)),
            current_audit_ids=tuple(
                f"current-audit-{index:02d}" for index in range(50)
            ),
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

    def _evidence_binding(
        self,
        *,
        authority: LkgPhase3Authority | None = None,
        **changes: object,
    ) -> Stage4EvidenceBinding:
        source = self.authority if authority is None else authority
        values: dict[str, object] = {
            "run_id": "exp009-admission-run-001",
            "source_revision": "a" * 40,
            "metric": self.plan.metric,
            "threshold_stratum": self.plan.threshold_stratum,
            "current_ef": self.plan.last_known_good_ef,
            "candidate_ef": self.plan.candidate_ef,
            "last_known_good_ef": self.plan.last_known_good_ef,
            "candidate_search_configuration": replace(
                source.search_configuration, ef=self.plan.candidate_ef
            ),
            "identity": self.manifest.identity,
            "dataset002_manifest_sha256": self.manifest.dataset002_manifest_sha256,
            "frozen_recall_audit_ids_sha256": _sha("e"),
            "eligible_workload_sha256": self.plan.eligible_workload_sha256,
            "candidate_selection_sha256": self.plan.candidate_selection_sha256,
            "execution_schedule_sha256": self.schedule.schedule_sha256,
            "recall_evidence_schema_version": "recall-audit-hoeffding-1200-v1",
            "latency_evidence_schema_version": "stage4-schedule-evaluation-v1",
        }
        values.update(changes)
        return Stage4EvidenceBinding(**values)

    def _request(self, **changes: object) -> Stage4AdmissionRequest:
        values: dict[str, object] = {
            "manifest": self.manifest,
            "selection": self.selection,
            "plan": self.plan,
            "schedule": self.schedule,
            "policy_decision": self._decision(),
            "lkg_authority": self.lkg_pair,
            "evidence_binding": self._evidence_binding(),
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

    def _pair_for_authority(
        self,
        authority: LkgPhase3Authority,
        *,
        name: str,
        persisted_at_utc: str = "2026-08-08T13:01:00.000000Z",
    ) -> Stage4LkgAuthorityPair:
        with LkgPhase3AuthorityReferenceStore(self.root / f"{name}.db") as store:
            store.append(authority, persisted_at_utc=persisted_at_utc)
            latest = store.load_verified_latest()
        assert latest is not None
        return bind_stage4_lkg_authority(
            authority=authority,
            verified_latest_reference=latest,
        )

    def test_exact_d1_d2_binding_schedule_produces_one_private_receipt(self) -> None:
        result = evaluate_stage4_admission(self._request())

        self.assertTrue(result.admitted)
        self.assertEqual(result.reason_codes, ())
        self.assertIsInstance(result.receipt, Stage4AdmissionReceipt)
        assert result.receipt is not None
        self.assertEqual(
            result.receipt.checkpoint_c_evaluation_digest,
            self.authority.canonical_evaluation_digest,
        )
        self.assertEqual(
            result.receipt.d2_canonical_record_digest,
            self.persisted.canonical_record_digest,
        )
        self.assertEqual(result.receipt.execution_schedule_sha256, self.schedule.schedule_sha256)
        self.assertEqual(result.plan_sha256, self.plan.plan_sha256)
        self.assertEqual(result.policy_audit_id, "policy-audit-admission-001")
        self.assertEqual(len(result.receipt.canonical_receipt_digest), 64)
        self.assertTrue(result.receipt.matches_canonical_digest())

    def test_legacy_qualification_result_cannot_authorize_d3(self) -> None:
        legacy = QualificationResult(
            qualified=True,
            ef=400,
            reasons=(),
            metric=self.plan.metric,
            threshold_stratum=self.plan.threshold_stratum,
            configuration_identity=self.plan.configuration_identity,
            index_identity=self.plan.hnsw_binding_id,
            data_identity=self.plan.data_identity,
            qualifying_window_ids=("old-1", "old-2"),
        )
        self.assertNotIn("qualification", Stage4AdmissionRequest.__annotations__)
        result = evaluate_stage4_admission(legacy)
        self.assertFalse(result.admitted)
        self.assertEqual(result.reason_codes, ("ADMISSION_REQUEST_INVALID",))

    def test_plain_historical_reference_cannot_substitute_for_verified_latest(self) -> None:
        with self.assertRaises(TypeError):
            bind_stage4_lkg_authority(
                authority=self.authority,
                verified_latest_reference=self.persisted,
            )
        result = evaluate_stage4_admission(
            self._request(lkg_authority=self.persisted)
        )
        self.assertIn("PHASE3_LKG_AUTHORITY_PAIR_INVALID", result.reason_codes)

    def test_nonconcrete_or_malformed_d1_d2_values_refuse(self) -> None:
        with self.assertRaises(TypeError):
            Stage4LkgAuthorityPair()
        with self.assertRaises(TypeError):
            Stage4LkgAuthorityPair._from_validated(
                authority=self.authority,
                verified_latest_reference=self.verified_latest,
                construction_token=object(),
            )
        with self.assertRaises(TypeError):
            bind_stage4_lkg_authority(
                authority=object(),
                verified_latest_reference=self.verified_latest,
            )
        with self.assertRaises(TypeError):
            bind_stage4_lkg_authority(
                authority=self.authority,
                verified_latest_reference=object(),
            )
        forged = object.__new__(Stage4LkgAuthorityPair)
        object.__setattr__(forged, "_authority", object())
        object.__setattr__(forged, "_verified_latest_reference", self.verified_latest)
        result = evaluate_stage4_admission(self._request(lkg_authority=forged))
        self.assertIn("PHASE3_LKG_AUTHORITY_PAIR_INVALID", result.reason_codes)

    def test_every_d2_persisted_identity_field_is_compared_to_d1(self) -> None:
        digest_fields = {
            "canonical_evaluation_digest",
            "source_run_binding_sha256",
            "source_run_seal_digest",
            "source_sealed_phase1_chain_head_sha256",
            "phase2_source_binding_digest",
            "search_configuration_digest",
            "qualification_manifest_sha256",
            "qualification_ordered_query_ids_sha256",
            "qualification_query_id_array_sha256",
            "qualification_query_array_sha256",
        }
        integer_fields = {"evaluated_ef", "qualification_expected_query_count"}
        fields_to_check = (
            "canonical_evaluation_digest",
            "source_run_id",
            "source_run_binding_sha256",
            "source_run_seal_digest",
            "source_sealed_phase1_chain_head_sha256",
            "phase2_source_binding_digest",
            "evaluated_ef",
            "search_configuration_digest",
            "metric",
            "threshold_stratum",
            "collection_name",
            "index_identity",
            "data_identity",
            "qualification_dataset_id",
            "qualification_dataset_version",
            "qualification_manifest_sha256",
            "qualification_query_role",
            "qualification_ordered_query_ids_sha256",
            "qualification_query_id_array_sha256",
            "qualification_query_array_sha256",
            "qualification_expected_query_count",
            "environment_identity",
            "source_revision",
        )
        for field in fields_to_check:
            if field in digest_fields:
                replacement_value: object = _sha("f")
            elif field in integer_fields:
                replacement_value = getattr(self.persisted, field) + 1
            elif field == "metric":
                replacement_value = Metric.COSINE.value
            else:
                replacement_value = f"different-{field}"
            changed = replace(self.persisted, **{field: replacement_value})
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, field):
                bind_stage4_lkg_authority(
                    authority=self.authority,
                    verified_latest_reference=_forge_verified_latest(changed),
                )

    def test_checkpoint_c_digest_and_nonmatching_latest_head_fail(self) -> None:
        changed = replace(self.persisted, canonical_evaluation_digest=_sha("f"))
        with self.assertRaisesRegex(ValueError, "canonical_evaluation_digest"):
            bind_stage4_lkg_authority(
                authority=self.authority,
                verified_latest_reference=_forge_verified_latest(changed),
            )
        other = _phase3_authority(
            plan=self.plan,
            radius=self.manifest.radius,
            identifier=22,
        )
        other_pair = self._pair_for_authority(other, name="nonmatching-head")
        with self.assertRaises(ValueError):
            bind_stage4_lkg_authority(
                authority=self.authority,
                verified_latest_reference=other_pair.verified_latest_reference,
            )

    def test_configuration_identity_is_not_search_configuration_digest(self) -> None:
        self.assertNotEqual(
            self.plan.configuration_identity,
            self.authority.search_configuration_digest,
        )
        result = evaluate_stage4_admission(self._request())
        self.assertTrue(result.admitted)

    def test_constructor_revalidated_evidence_binding_still_admits(self) -> None:
        binding = self._evidence_binding()
        result = evaluate_stage4_admission(
            self._request(evidence_binding=binding)
        )
        self.assertTrue(result.admitted)
        assert result.receipt is not None
        self.assertEqual(result.receipt.stage4_evidence_binding_sha256, binding.sha256)

    def test_forged_evidence_binding_unsupported_schema_refuses(self) -> None:
        forged = _forge_frozen(
            self._evidence_binding(), schema_version="unsupported-binding-v999"
        )
        result = evaluate_stage4_admission(self._request(evidence_binding=forged))
        self.assertFalse(result.admitted)
        self.assertIn("STAGE4_EVIDENCE_BINDING_INVALID", result.reason_codes)

    def test_forged_evidence_binding_invalid_run_id_refuses(self) -> None:
        forged = _forge_frozen(self._evidence_binding(), run_id=" invalid-run-id")
        result = evaluate_stage4_admission(self._request(evidence_binding=forged))
        self.assertFalse(result.admitted)
        self.assertIn("STAGE4_EVIDENCE_BINDING_INVALID", result.reason_codes)

    def test_forged_evidence_binding_invalid_source_revision_refuses(self) -> None:
        forged = _forge_frozen(
            self._evidence_binding(), source_revision="not-a-40-hex-revision"
        )
        result = evaluate_stage4_admission(self._request(evidence_binding=forged))
        self.assertFalse(result.admitted)
        self.assertIn("STAGE4_EVIDENCE_BINDING_INVALID", result.reason_codes)

    def test_forged_evidence_binding_malformed_recall_audit_digest_refuses(self) -> None:
        forged = _forge_frozen(
            self._evidence_binding(), frozen_recall_audit_ids_sha256="malformed"
        )
        result = evaluate_stage4_admission(self._request(evidence_binding=forged))
        self.assertFalse(result.admitted)
        self.assertIn("STAGE4_EVIDENCE_BINDING_INVALID", result.reason_codes)

    def test_forged_evidence_binding_malformed_evidence_schema_versions_refuse(self) -> None:
        for field in (
            "recall_evidence_schema_version",
            "latency_evidence_schema_version",
        ):
            forged = _forge_frozen(
                self._evidence_binding(), **{field: " malformed-schema"}
            )
            with self.subTest(field=field):
                result = evaluate_stage4_admission(
                    self._request(evidence_binding=forged)
                )
                self.assertFalse(result.admitted)
                self.assertIn(
                    "STAGE4_EVIDENCE_BINDING_INVALID", result.reason_codes
                )

    def test_candidate_configuration_any_non_ef_difference_fails(self) -> None:
        base_binding = self._evidence_binding()
        candidate = base_binding.candidate_search_configuration
        changes = {
            "metric": Metric.COSINE,
            "threshold_label": "target-025",
            "radius": candidate.radius + 0.25,
            "index_track": IndexTrack.FLAT,
            "limit": 101,
            "consistency_level": "Eventually",
        }
        for field, value in changes.items():
            changed_configuration = replace(candidate, **{field: value})
            forged_binding = _forge_frozen(
                base_binding,
                candidate_search_configuration=changed_configuration,
            )
            with self.subTest(field=field):
                result = evaluate_stage4_admission(
                    self._request(evidence_binding=forged_binding)
                )
                expected_reason = (
                    "CANDIDATE_SEARCH_CONFIGURATION_MISMATCH"
                    if field == "radius"
                    else "STAGE4_EVIDENCE_BINDING_INVALID"
                )
                self.assertIn(
                    expected_reason,
                    result.reason_codes,
                )

    def test_route_radius_range_filter_and_limit_contracts_fail_closed(self) -> None:
        radius_authority = _phase3_authority(
            plan=self.plan,
            radius=self.manifest.radius + 0.25,
            identifier=31,
        )
        radius_pair = self._pair_for_authority(radius_authority, name="radius-mismatch")
        radius_result = evaluate_stage4_admission(
            self._request(
                lkg_authority=radius_pair,
                evidence_binding=self._evidence_binding(authority=radius_authority),
            )
        )
        self.assertIn("ROUTING_SEARCH_CONTRACT_MISMATCH", radius_result.reason_codes)

        original_configuration = self.authority.run_binding.search_configuration
        range_request = self._request()
        try:
            object.__setattr__(
                self.authority.run_binding,
                "search_configuration",
                replace(original_configuration, metric=Metric.COSINE, radius=0.5),
            )
            range_result = evaluate_stage4_admission(range_request)
            self.assertIn("PHASE3_LKG_AUTHORITY_PAIR_INVALID", range_result.reason_codes)
        finally:
            object.__setattr__(
                self.authority.run_binding,
                "search_configuration",
                original_configuration,
            )

        limit_request = self._request()
        try:
            object.__setattr__(
                self.authority.run_binding,
                "search_configuration",
                replace(original_configuration, limit=101),
            )
            limit_result = evaluate_stage4_admission(limit_request)
            self.assertIn("PHASE3_LKG_SEARCH_CONFIGURATION_INVALID", limit_result.reason_codes)
        finally:
            object.__setattr__(
                self.authority.run_binding,
                "search_configuration",
                original_configuration,
            )

    def test_evidence_binding_identity_workload_and_selection_mismatch_fail(self) -> None:
        cases = (
            self._evidence_binding(
                identity=replace(
                    self.manifest.identity,
                    configuration_identity="different-config-identity",
                )
            ),
            self._evidence_binding(eligible_workload_sha256=_sha("f")),
            self._evidence_binding(candidate_selection_sha256=_sha("f")),
        )
        for binding in cases:
            with self.subTest(digest=binding.sha256):
                result = evaluate_stage4_admission(
                    self._request(evidence_binding=binding)
                )
                self.assertIn("STAGE4_EVIDENCE_BINDING_MISMATCH", result.reason_codes)

    def test_schedule_substitution_plan_and_binding_digest_mismatch_fail(self) -> None:
        substituted_selection = replace(
            self.selection,
            candidate_occurrence_ids=tuple(
                item.occurrence_id
                for item in self.manifest.occurrences
                if item.sequence_index % 10 == 1
            ),
        )
        substituted_plan = build_canary_route_plan(self.manifest, substituted_selection)
        substituted_schedule = build_stage4_execution_schedule(
            self.manifest, substituted_plan
        )
        schedule_result = evaluate_stage4_admission(
            self._request(schedule=substituted_schedule)
        )
        plan_result = evaluate_stage4_admission(
            self._request(plan=substituted_plan)
        )
        binding_result = evaluate_stage4_admission(
            self._request(
                evidence_binding=self._evidence_binding(
                    execution_schedule_sha256=_sha("f")
                )
            )
        )
        self.assertIn("EXECUTION_SCHEDULE_REBUILD_MISMATCH", schedule_result.reason_codes)
        self.assertIn("PLAN_REBUILD_MISMATCH", plan_result.reason_codes)
        self.assertIn("STAGE4_EVIDENCE_SCHEDULE_MISMATCH", binding_result.reason_codes)

    def test_receipt_public_or_dataclass_reconstruction_is_rejected(self) -> None:
        with self.assertRaises(TypeError):
            Stage4AdmissionReceipt()
        receipt = evaluate_stage4_admission(self._request()).receipt
        assert receipt is not None
        with self.assertRaises(TypeError):
            replace(receipt, policy_audit_id="forged-audit-id")

    def test_malformed_forged_receipt_digest_check_returns_false(self) -> None:
        receipt = evaluate_stage4_admission(self._request()).receipt
        assert receipt is not None
        forged = _forge_frozen(receipt, source_run_id=object())
        self.assertFalse(forged.matches_canonical_digest())

    def test_receipt_digest_changes_when_bound_lineage_changes(self) -> None:
        first = evaluate_stage4_admission(self._request()).receipt
        other = _phase3_authority(
            plan=self.plan,
            radius=self.manifest.radius,
            identifier=41,
        )
        other_pair = self._pair_for_authority(other, name="receipt-lineage")
        second = evaluate_stage4_admission(
            self._request(
                lkg_authority=other_pair,
                evidence_binding=self._evidence_binding(authority=other),
            )
        ).receipt
        assert first is not None and second is not None
        self.assertNotEqual(first.checkpoint_c_evaluation_digest, second.checkpoint_c_evaluation_digest)
        self.assertNotEqual(first.d2_canonical_record_digest, second.d2_canonical_record_digest)
        self.assertNotEqual(first.canonical_receipt_digest, second.canonical_receipt_digest)

    def test_runtime_timestamp_is_validated_and_bound_into_receipt(self) -> None:
        invalid = evaluate_stage4_admission(
            self._request(
                runtime=Stage4RuntimeReadiness(
                    binding=self._binding(),
                    serving_preflight_complete=True,
                    observed_at_utc="2026-13-04T12:03:00Z",
                    reason_codes=(),
                )
            )
        )
        self.assertIn("RUNTIME_TIMESTAMP_INVALID", invalid.reason_codes)
        first = evaluate_stage4_admission(self._request()).receipt
        second_timestamp = "2026-08-04T12:04:00Z"
        second = evaluate_stage4_admission(
            self._request(
                runtime=Stage4RuntimeReadiness(
                    binding=self._binding(),
                    serving_preflight_complete=True,
                    observed_at_utc=second_timestamp,
                    reason_codes=(),
                )
            )
        ).receipt
        assert first is not None and second is not None
        self.assertEqual(second.runtime_observed_at_utc, second_timestamp)
        self.assertNotEqual(first.canonical_receipt_digest, second.canonical_receipt_digest)
        self.assertTrue(first.stable_lineage_matches(second))

    def test_repository_policy_and_runtime_validation_remain_fail_closed(self) -> None:
        dirty = evaluate_stage4_admission(
            self._request(
                repository=Stage4RepositoryEvidence(
                    commit_sha="a" * 40,
                    clean=False,
                    observed_at_utc="2026-08-04T12:02:00Z",
                )
            )
        )
        dry_run = evaluate_stage4_admission(
            self._request(
                policy_decision=replace(self._decision(), mode=PolicyMode.DRY_RUN)
            )
        )
        failed_gate = evaluate_stage4_admission(
            self._request(
                policy_decision=replace(
                    self._decision(),
                    safety_gate_results=(SafetyGateResult("PRE_ACTION", False, "failed"),),
                )
            )
        )
        incomplete = evaluate_stage4_admission(
            self._request(
                runtime=Stage4RuntimeReadiness(
                    binding=self._binding(),
                    serving_preflight_complete=False,
                    observed_at_utc="2026-08-04T12:03:00Z",
                    reason_codes=("STACK_HEALTH_UNHEALTHY",),
                )
            )
        )
        self.assertIn("REPOSITORY_NOT_CLEAN", dirty.reason_codes)
        self.assertIn("POLICY_MODE_INVALID", dry_run.reason_codes)
        self.assertIn("POLICY_SAFETY_GATES_FAILED", failed_gate.reason_codes)
        self.assertIn("RUNTIME_PREFLIGHT_INCOMPLETE", incomplete.reason_codes)

    def test_source_has_no_upstream_statistical_recomputation_or_live_import(self) -> None:
        source_path = Path(__file__).parents[1] / "src" / "vdbench" / "canary_admission.py"
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
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
        self.assertNotIn("QualificationResult", source)
        self.assertFalse(
            {
                "lkg_qualification_ledger",
                "lkg_phase2_readiness_ledger",
                "lkg_qualification_evaluation_ledger",
                "milvus",
                "canary_activation",
                "canary_route_authority",
                "actuation",
            }
            & imported
        )


if __name__ == "__main__":
    unittest.main()
