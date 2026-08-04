"""Contract tests for the canonical Stage-4 evidence binding."""

from __future__ import annotations

import unittest

from vdbench.canary_stage4_evidence_binding import (
    STAGE4_EVIDENCE_BINDING_SCHEMA_VERSION,
    Stage4EvidenceBinding,
)
from vdbench.canary_workload import WorkloadIdentityBinding
from vdbench.config import Metric


def _binding(**overrides: object) -> Stage4EvidenceBinding:
    fields: dict[str, object] = {
        "run_id": "exp009-run-001",
        "source_revision": "a" * 40,
        "metric": Metric.L2,
        "threshold_stratum": "target-075",
        "current_ef": 400,
        "candidate_ef": 800,
        "last_known_good_ef": 400,
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
        self.assertEqual(first.sha256, second.sha256)
        self.assertEqual(len(first.sha256), 64)
        self.assertEqual(first.to_document()["identity"]["hnsw_binding_id"], "hnsw-binding-v1")
        self.assertNotEqual(first.sha256, _binding(candidate_ef=1600).sha256)
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


if __name__ == "__main__":
    unittest.main()
