"""TDD coverage for Checkpoint B: Phase2SourceBinding + LkgWindowReadinessIngestion."""

from __future__ import annotations

import unittest

from vdbench.config import ContractViolation
from vdbench.lkg_qualification_seal import LkgSealWorkloadIdentity
from vdbench.lkg_phase2_source_binding import (
    EXPECTED_QUERY_COUNT,
    INGESTION_SCHEMA_VERSION,
    PHASE1_LEDGER_SCHEMA_VERSION,
    SEAL_SCHEMA_VERSION_PIN,
    SOURCE_BINDING_SCHEMA_VERSION,
    ingestion_payload_document,
    ingestion_payload_document_digest,
    lkg_window_readiness_ingestion_from_payload,
    phase2_source_binding_from_payload,
    source_binding_payload_document,
    source_binding_payload_document_digest,
)
from vdbench.lkg_window_readiness import (
    FakeLkgWindowOperationalReadinessProvider,
    readiness_payload_document,
)


def _workload_identity_document() -> dict:
    return {
        "dataset_id": "DATASET-003",
        "dataset_version": "DATASET-003-v1",
        "manifest_sha256": "a" * 64,
        "query_role": "lkg_qualification",
    }


def _binding_payload(**overrides) -> dict:
    payload = dict(
        source_binding_schema_version=SOURCE_BINDING_SCHEMA_VERSION,
        source_run_id="run-1",
        source_run_binding_sha256="b" * 64,
        source_phase1_ledger_schema_version=PHASE1_LEDGER_SCHEMA_VERSION,
        source_seal_schema_version=SEAL_SCHEMA_VERSION_PIN,
        source_run_seal_digest="c" * 64,
        source_sealed_chain_head_sha256="d" * 64,
        workload_identity=_workload_identity_document(),
        qualification_ordered_query_ids_sha256="e" * 64,
        expected_query_count=EXPECTED_QUERY_COUNT,
    )
    payload.update(overrides)
    return payload


def _binding(**overrides):
    payload = _binding_payload(**overrides)
    digest = source_binding_payload_document_digest(payload)
    return phase2_source_binding_from_payload(payload, canonical_source_binding_digest=digest)


_PROVIDER = FakeLkgWindowOperationalReadinessProvider()
_EVIDENCE = _PROVIDER.capture_or_return(
    readiness_check_id="chk-1",
    source_run_id="run-1",
    source_run_binding_sha256="b" * 64,
    window_index=0,
    epoch_index=0,
    first_attempt_sequence=0,
    last_attempt_sequence=199,
)


def _ingestion_payload(**overrides) -> dict:
    payload = dict(
        ingestion_schema_version=INGESTION_SCHEMA_VERSION,
        source_run_id="run-1",
        window_index=0,
        epoch_index=0,
        original_evidence=readiness_payload_document(_EVIDENCE),
        original_evidence_digest=_EVIDENCE.canonical_document_digest,
        source_run_seal_digest="1" * 64,
        phase2_source_binding_digest="2" * 64,
        ingested_at_utc="2026-01-01T00:00:01Z",
    )
    payload.update(overrides)
    return payload


def _ingestion(**overrides):
    payload = _ingestion_payload(**overrides)
    digest = ingestion_payload_document_digest(payload)
    return lkg_window_readiness_ingestion_from_payload(payload, canonical_ingestion_digest=digest)


class Phase2SourceBindingTests(unittest.TestCase):
    def test_payload_excludes_digest(self) -> None:
        payload = source_binding_payload_document(_binding())
        self.assertNotIn("canonical_source_binding_digest", payload)

    def test_round_trip(self) -> None:
        binding = _binding()
        payload = source_binding_payload_document(binding)
        digest = source_binding_payload_document_digest(payload)
        reconstructed = phase2_source_binding_from_payload(payload, canonical_source_binding_digest=digest)
        self.assertEqual(reconstructed, binding)

    def test_digest_self_verification_rejects_mismatch(self) -> None:
        payload = _binding_payload()
        with self.assertRaises(ContractViolation):
            phase2_source_binding_from_payload(payload, canonical_source_binding_digest="f" * 64)

    def test_source_binding_schema_version_pin(self) -> None:
        for bad in (0, 2, 99):
            with self.assertRaises(ContractViolation):
                payload = _binding_payload(source_binding_schema_version=bad)
                digest = source_binding_payload_document_digest(payload)
                phase2_source_binding_from_payload(payload, canonical_source_binding_digest=digest)

    def test_source_phase1_ledger_schema_version_pin(self) -> None:
        for bad in (0, 4, 6, 99):
            with self.assertRaises(ContractViolation):
                payload = _binding_payload(source_phase1_ledger_schema_version=bad)
                digest = source_binding_payload_document_digest(payload)
                phase2_source_binding_from_payload(payload, canonical_source_binding_digest=digest)

    def test_source_seal_schema_version_pin(self) -> None:
        for bad in (0, 2, 99):
            with self.assertRaises(ContractViolation):
                payload = _binding_payload(source_seal_schema_version=bad)
                digest = source_binding_payload_document_digest(payload)
                phase2_source_binding_from_payload(payload, canonical_source_binding_digest=digest)

    def test_expected_query_count_must_equal_2400(self) -> None:
        for bad in (0, 1, 200, 2399, 2401, 4800):
            with self.assertRaises(ContractViolation):
                payload = _binding_payload(expected_query_count=bad)
                digest = source_binding_payload_document_digest(payload)
                phase2_source_binding_from_payload(payload, canonical_source_binding_digest=digest)

    def test_unknown_top_level_field_rejected(self) -> None:
        payload = _binding_payload()
        payload["bogus"] = "x"
        with self.assertRaises(ContractViolation):
            phase2_source_binding_from_payload(
                payload, canonical_source_binding_digest=source_binding_payload_document_digest(payload)
            )

    def test_missing_top_level_field_rejected(self) -> None:
        payload = _binding_payload()
        del payload["source_run_seal_digest"]
        with self.assertRaises(ContractViolation):
            phase2_source_binding_from_payload(
                payload, canonical_source_binding_digest="0" * 64
            )

    def test_noncanonical_workload_identity_field_rejected(self) -> None:
        payload = _binding_payload()
        payload["workload_identity"]["bogus"] = "x"
        with self.assertRaises(ContractViolation):
            phase2_source_binding_from_payload(
                payload, canonical_source_binding_digest=source_binding_payload_document_digest(payload)
            )

    def test_workload_identity_mismatch_changes_digest(self) -> None:
        base_digest = source_binding_payload_document_digest(_binding_payload())
        other_identity = _workload_identity_document()
        other_identity["dataset_version"] = "DATASET-003-v2"
        changed_digest = source_binding_payload_document_digest(
            _binding_payload(workload_identity=other_identity)
        )
        self.assertNotEqual(base_digest, changed_digest)


class LkgWindowReadinessIngestionTests(unittest.TestCase):
    def test_payload_excludes_digest(self) -> None:
        payload = ingestion_payload_document(_ingestion())
        self.assertNotIn("canonical_ingestion_digest", payload)

    def test_payload_embeds_evidence_payload_and_digest(self) -> None:
        payload = ingestion_payload_document(_ingestion())
        self.assertEqual(payload["original_evidence"], readiness_payload_document(_EVIDENCE))
        self.assertEqual(payload["original_evidence_digest"], _EVIDENCE.canonical_document_digest)

    def test_round_trip_through_nested_evidence(self) -> None:
        ingestion = _ingestion()
        payload = ingestion_payload_document(ingestion)
        digest = ingestion_payload_document_digest(payload)
        reconstructed = lkg_window_readiness_ingestion_from_payload(payload, canonical_ingestion_digest=digest)
        self.assertEqual(reconstructed, ingestion)
        self.assertEqual(reconstructed.original_evidence, _EVIDENCE)

    def test_digest_self_verification_rejects_mismatch(self) -> None:
        payload = _ingestion_payload()
        with self.assertRaises(ContractViolation):
            lkg_window_readiness_ingestion_from_payload(payload, canonical_ingestion_digest="f" * 64)

    def test_ingestion_schema_version_pin(self) -> None:
        for bad in (0, 2, 99):
            with self.assertRaises(ContractViolation):
                payload = _ingestion_payload(ingestion_schema_version=bad)
                digest = ingestion_payload_document_digest(payload)
                lkg_window_readiness_ingestion_from_payload(payload, canonical_ingestion_digest=digest)

    def test_source_run_id_mismatch_with_embedded_evidence_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _ingestion(source_run_id="a-different-run")

    def test_window_index_mismatch_with_embedded_evidence_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _ingestion(window_index=1)

    def test_epoch_index_mismatch_with_embedded_evidence_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _ingestion(epoch_index=1)

    def test_unknown_top_level_field_rejected(self) -> None:
        payload = _ingestion_payload()
        payload["bogus"] = "x"
        with self.assertRaises(ContractViolation):
            lkg_window_readiness_ingestion_from_payload(
                payload, canonical_ingestion_digest=ingestion_payload_document_digest(payload)
            )

    def test_missing_top_level_field_rejected(self) -> None:
        payload = _ingestion_payload()
        del payload["ingested_at_utc"]
        with self.assertRaises(ContractViolation):
            lkg_window_readiness_ingestion_from_payload(payload, canonical_ingestion_digest="0" * 64)


if __name__ == "__main__":
    unittest.main()
