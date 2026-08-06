"""TDD coverage for the read-only stage4-evidence-binding-v1 inspection parser."""

from __future__ import annotations

import unittest

from vdbench.canary_recall_audit_evaluation import evaluate_recall_audit_evidence
from vdbench.canary_statistics import EXP009_RECALL_AUDIT_COUNT
from vdbench.canary_recall_audit_producer import Stage4RecallAuditProducer
from vdbench.canary_stage4_evidence_binding_legacy import (
    LEGACY_STAGE4_EVIDENCE_BINDING_V1_SCHEMA_VERSION,
    LegacyStage4EvidenceBindingV1,
    parse_legacy_stage4_evidence_binding_v1,
)
from vdbench.canary_stage4_evidence_binding import Stage4EvidenceBinding
from vdbench.canary_stage4_latency_evidence import build_stage4_latency_evidence
from vdbench.canary_stage4_decision import combine_stage4_decision


_V1_DOCUMENT = {
    "schema_version": "stage4-evidence-binding-v1",
    "run_id": "exp009-run-001",
    "source_revision": "a" * 40,
    "metric": "L2",
    "threshold_stratum": "target-075",
    "current_ef": 400,
    "candidate_ef": 800,
    "last_known_good_ef": 400,
    "identity": {
        "configuration_identity": "config-v1",
        "data_identity": "DATASET-001-v1:sha256:" + "b" * 64,
        "flat_binding_id": "flat-binding-v1",
        "hnsw_binding_id": "hnsw-binding-v1",
    },
    "dataset002_manifest_sha256": "c" * 64,
    "frozen_recall_audit_ids_sha256": "d" * 64,
    "eligible_workload_sha256": "e" * 64,
    "candidate_selection_sha256": "f" * 64,
    "execution_schedule_sha256": "0" * 64,
    "recall_evidence_schema_version": "recall-audit-hoeffding-1200-v1",
    "latency_evidence_schema_version": "stage4-schedule-evaluation-v1",
}

# The v2 document shape: same fields plus candidate_search_configuration,
# and a v2 schema_version literal.
_V2_SHAPED_DOCUMENT = {
    **_V1_DOCUMENT,
    "schema_version": "stage4-evidence-binding-v2",
    "candidate_search_configuration": {
        "metric": "L2",
        "threshold_label": "target-075",
        "radius": 0.6,
        "index_track": "HNSW",
        "ef": 800,
        "limit": 100,
        "consistency_level": "Strong",
    },
}


class LegacyStage4EvidenceBindingV1Tests(unittest.TestCase):
    def test_parses_a_well_formed_v1_document(self) -> None:
        parsed = parse_legacy_stage4_evidence_binding_v1(_V1_DOCUMENT)
        self.assertIsInstance(parsed, LegacyStage4EvidenceBindingV1)
        self.assertEqual(parsed.schema_version, LEGACY_STAGE4_EVIDENCE_BINDING_V1_SCHEMA_VERSION)
        self.assertEqual(parsed.run_id, "exp009-run-001")
        self.assertEqual(parsed.candidate_ef, 800)
        self.assertEqual(parsed.identity["hnsw_binding_id"], "hnsw-binding-v1")

    def test_rejects_a_v2_shaped_document(self) -> None:
        with self.assertRaises(ValueError):
            parse_legacy_stage4_evidence_binding_v1(_V2_SHAPED_DOCUMENT)

    def test_rejects_missing_or_extra_fields(self) -> None:
        missing = dict(_V1_DOCUMENT)
        del missing["run_id"]
        with self.assertRaises(ValueError):
            parse_legacy_stage4_evidence_binding_v1(missing)

        extra = dict(_V1_DOCUMENT)
        extra["unexpected_field"] = "x"
        with self.assertRaises(ValueError):
            parse_legacy_stage4_evidence_binding_v1(extra)

    def test_rejects_non_mapping_input(self) -> None:
        with self.assertRaises(ValueError):
            parse_legacy_stage4_evidence_binding_v1("not-a-document")

    def test_is_not_a_stage4_evidence_binding_and_shares_no_base_class(self) -> None:
        parsed = parse_legacy_stage4_evidence_binding_v1(_V1_DOCUMENT)
        self.assertNotIsInstance(parsed, Stage4EvidenceBinding)
        self.assertFalse(
            set(type(parsed).__mro__) & set(Stage4EvidenceBinding.__mro__) - {object}
        )

    def test_v1_parse_result_is_rejected_by_every_v2_aware_function(self) -> None:
        legacy = parse_legacy_stage4_evidence_binding_v1(_V1_DOCUMENT)

        with self.assertRaises(TypeError):
            evaluate_recall_audit_evidence(
                expected_query_ids=frozenset(range(EXP009_RECALL_AUDIT_COUNT)),
                search_configuration=None,
                identity=None,
                dataset002_manifest_sha256="",
                dataset002_schema_version=1,
                observations=(),
                binding=legacy,
                frozen_query_ids_sha256="0" * 64,
            )

        with self.assertRaises(TypeError):
            build_stage4_latency_evidence(binding=legacy, schedule=None, ledger=None)

        with self.assertRaises(TypeError):
            Stage4RecallAuditProducer(
                binding=legacy,
                search_configuration=None,
                dataset002_schema_version=1,
                query_source=None,
                oracle_result_ids_by_query_id={},
                client=None,
                ledger=None,
                utc_now=lambda: "2026-08-06T00:00:00Z",
            )


if __name__ == "__main__":
    unittest.main()
