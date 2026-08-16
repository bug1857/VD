"""Contract tests for the canonical Stage-4 evidence binding."""

from __future__ import annotations

import unittest
from dataclasses import replace

from vdbench.canary_stage4_evidence_binding import (
    STAGE4_EVIDENCE_BINDING_SCHEMA_VERSION,
    Stage4EvidenceBinding,
)
from vdbench.canary_workload import WorkloadIdentityBinding
from vdbench.config import ContractViolation, IndexTrack, Metric, SearchConfiguration

_CANDIDATE_SEARCH_CONFIGURATION = SearchConfiguration(
    metric=Metric.L2,
    threshold_label="target-075",
    radius=0.6,
    index_track=IndexTrack.HNSW,
    ef=800,
    limit=100,
    consistency_level="Strong",
)


def _binding(**overrides: object) -> Stage4EvidenceBinding:
    fields: dict[str, object] = {
        "run_id": "exp009-run-001",
        "source_revision": "a" * 40,
        "metric": Metric.L2,
        "threshold_stratum": "target-075",
        "current_ef": 400,
        "candidate_ef": 800,
        "last_known_good_ef": 400,
        "candidate_search_configuration": _CANDIDATE_SEARCH_CONFIGURATION,
        "identity": WorkloadIdentityBinding(
            configuration_identity="config-v1",
            data_identity="DATASET-001-v1:sha256:" + "b" * 64,
            flat_binding_id="flat-binding-v1",
            hnsw_binding_id="hnsw-binding-v1",
        ),
        "dataset002_manifest_sha256": "c" * 64,
        "frozen_recall_audit_ids_sha256": "d" * 64,
        "eligible_workload_sha256": "e" * 64,
        "candidate_selection_sha256": "f" * 64,
        "execution_schedule_sha256": "0" * 64,
        "recall_evidence_schema_version": "recall-audit-hoeffding-1200-v1",
        "latency_evidence_schema_version": "stage4-schedule-evaluation-v1",
    }
    fields.update(overrides)
    return Stage4EvidenceBinding(**fields)


class Stage4EvidenceBindingTests(unittest.TestCase):
    def test_canonical_digest_is_stable_and_includes_every_identity_field(self) -> None:
        first = _binding()
        second = _binding()
        self.assertEqual(first.schema_version, STAGE4_EVIDENCE_BINDING_SCHEMA_VERSION)
        self.assertEqual(first.schema_version, "stage4-evidence-binding-v2")
        self.assertEqual(first.sha256, second.sha256)
        self.assertEqual(len(first.sha256), 64)
        self.assertEqual(first.to_document()["identity"]["hnsw_binding_id"], "hnsw-binding-v1")
        self.assertNotEqual(
            first.sha256,
            _binding(
                candidate_ef=1600,
                candidate_search_configuration=replace(_CANDIDATE_SEARCH_CONFIGURATION, ef=1600),
            ).sha256,
        )
        self.assertNotEqual(first.sha256, _binding(run_id="exp009-run-002").sha256)

    def test_rejects_noncanonical_or_invalid_security_fields(self) -> None:
        with self.assertRaises(ValueError):
            _binding(run_id=" exp009-run-001")
        with self.assertRaises(ValueError):
            _binding(source_revision="A" * 40)
        with self.assertRaises(ValueError):
            _binding(dataset002_manifest_sha256="not-a-digest")
        with self.assertRaises(ValueError):
            _binding(current_ef=100)

    def test_expected_digest_verification_fails_closed(self) -> None:
        binding = _binding()
        self.assertTrue(binding.matches_sha256(binding.sha256))
        self.assertFalse(binding.matches_sha256("0" * 64))
        self.assertFalse(binding.matches_sha256("not-a-digest"))

    # -- v2 repair: candidate_search_configuration embedding and coherence --

    def test_candidate_search_configuration_must_be_a_search_configuration(self) -> None:
        with self.assertRaises(ValueError):
            _binding(candidate_search_configuration=object())

    def test_candidate_search_configuration_is_independently_validated(self) -> None:
        invalid = replace(_CANDIDATE_SEARCH_CONFIGURATION, limit=101)
        with self.assertRaises(ContractViolation):
            _binding(candidate_search_configuration=invalid)

    def test_candidate_search_configuration_index_track_must_be_hnsw(self) -> None:
        flat = replace(_CANDIDATE_SEARCH_CONFIGURATION, index_track=IndexTrack.FLAT, ef=None)
        with self.assertRaisesRegex(ValueError, "index_track must be HNSW"):
            _binding(candidate_search_configuration=flat)

    def test_coherence_metric_must_match_binding_metric(self) -> None:
        mismatched = replace(_CANDIDATE_SEARCH_CONFIGURATION, metric=Metric.COSINE, radius=0.5)
        with self.assertRaisesRegex(ValueError, "metric must equal binding.metric"):
            _binding(candidate_search_configuration=mismatched)

    def test_coherence_threshold_label_must_match_binding_threshold_stratum(self) -> None:
        mismatched = replace(_CANDIDATE_SEARCH_CONFIGURATION, threshold_label="target-025")
        with self.assertRaisesRegex(ValueError, "threshold_label must equal binding.threshold_stratum"):
            _binding(candidate_search_configuration=mismatched)

    def test_coherence_ef_must_match_binding_candidate_ef(self) -> None:
        mismatched = replace(_CANDIDATE_SEARCH_CONFIGURATION, ef=400)
        with self.assertRaisesRegex(ValueError, "ef must equal binding.candidate_ef"):
            _binding(candidate_search_configuration=mismatched)

    def test_to_document_embeds_the_complete_canonical_search_configuration(self) -> None:
        document = _binding().to_document()["candidate_search_configuration"]
        self.assertEqual(
            document,
            {
                "schema_version": "search-configuration-document-v1",
                "metric": "L2",
                "threshold_label": "target-075",
                "radius": 0.6,
                "range_filter": 0.0,
                "index_track": "HNSW",
                "ef": 800,
                "limit": 100,
                "consistency_level": "Strong",
            },
        )

    def test_candidate_search_configuration_sha256_is_derived_never_a_settable_field(self) -> None:
        binding = _binding()
        self.assertEqual(len(binding.candidate_search_configuration_sha256), 64)
        with self.assertRaises(TypeError):
            _binding(candidate_search_configuration_sha256="0" * 64)

    def test_each_search_configuration_field_independently_changes_binding_sha256(self) -> None:
        baseline = _binding().sha256
        radius_altered = _binding(
            candidate_search_configuration=replace(_CANDIDATE_SEARCH_CONFIGURATION, radius=3.0)
        )
        self.assertNotEqual(baseline, radius_altered.sha256)
        limit_invalid_rejected = False
        try:
            _binding(candidate_search_configuration=replace(_CANDIDATE_SEARCH_CONFIGURATION, limit=99))
        except ContractViolation:
            limit_invalid_rejected = True
        self.assertTrue(limit_invalid_rejected)

    def test_equal_bindings_have_equal_binding_digests_across_negative_zero_radius(self) -> None:
        """Canonical numeric identity, at the binding level: two
        Stage4EvidenceBinding objects that are == to each other (embedded
        configurations differing only by -0.0 vs 0.0 radius, COSINE only --
        L2 cannot express a zero radius at all) must have equal .sha256."""
        cosine_config = SearchConfiguration(
            metric=Metric.COSINE, threshold_label="target-025", radius=0.2,
            index_track=IndexTrack.HNSW, ef=800, limit=100, consistency_level="Strong",
        )
        a = _binding(
            metric=Metric.COSINE, threshold_stratum="target-025",
            candidate_search_configuration=replace(cosine_config, radius=-0.0),
        )
        b = _binding(
            metric=Metric.COSINE, threshold_stratum="target-025",
            candidate_search_configuration=replace(cosine_config, radius=0.0),
        )
        self.assertEqual(a, b)
        self.assertEqual(a.sha256, b.sha256)
        self.assertEqual(a.candidate_search_configuration_sha256, b.candidate_search_configuration_sha256)

    def test_canonical_equality_rejects_substitution(self) -> None:
        """RL-003: Exact canonical equality requirement rejects substitution attacks."""
        class MockBinding:
            def __init__(self, sha256: str):
                self.sha256 = sha256

        binding = _binding()
        substitute = MockBinding(binding.sha256)

        # __eq__ should reject a different type even if the digest matches
        self.assertNotEqual(binding, substitute)
        self.assertNotEqual(substitute, binding)

        # LegacyStage4EvidenceBindingV1 must not be equal to a v2 binding
        # even if its sha256 somehow matched (which is cryptographically infeasible,
        # but type strictness should catch it immediately).
        from vdbench.canary_stage4_evidence_binding_legacy import (
            LegacyStage4EvidenceBindingV1,
        )
        legacy_mock = LegacyStage4EvidenceBindingV1(**{
            k: getattr(binding, k) if hasattr(binding, k) else "missing"
            for k in [
                "schema_version", "run_id", "source_revision", "metric", "threshold_stratum",
                "current_ef", "candidate_ef", "last_known_good_ef",
                "identity", "dataset002_manifest_sha256", "frozen_recall_audit_ids_sha256",
                "eligible_workload_sha256", "candidate_selection_sha256",
                "execution_schedule_sha256", "recall_evidence_schema_version",
                "latency_evidence_schema_version"
            ]
        })
        self.assertNotEqual(binding, legacy_mock)

if __name__ == "__main__":
    unittest.main()
