"""TDD coverage for Checkpoint B: pre-seal operational-readiness capture."""

from __future__ import annotations

import threading
import unittest

from vdbench.config import ContractViolation
from vdbench.lkg_window_readiness import (
    FakeLkgWindowOperationalReadinessProvider,
    LkgWindowOperationalReadinessEvidence,
    LkgWindowOperationalReadinessProviderError,
    READINESS_SCHEMA_VERSION,
    lkg_window_operational_readiness_evidence_from_payload,
    parse_rfc3339_utc_instant,
    readiness_payload_document,
    readiness_payload_document_digest,
    validate_rfc3339_utc,
)


def _evidence(**overrides) -> LkgWindowOperationalReadinessEvidence:
    payload = dict(
        readiness_schema_version=READINESS_SCHEMA_VERSION,
        source_run_id="run-1",
        source_run_binding_sha256="a" * 64,
        window_index=0,
        epoch_index=0,
        first_attempt_sequence=0,
        last_attempt_sequence=199,
        readiness_check_id="chk-1",
        provider_run_id="provider-run-1",
        health_checked=True,
        health_passed=True,
        health_evidence_source_identity="fake-health-source",
        health_evidence_source_digest="b" * 64,
        rollback_tested=True,
        rollback_ready=True,
        rollback_evidence_source_identity="fake-rollback-source",
        rollback_evidence_source_digest="c" * 64,
        checked_at_utc="2026-01-01T00:00:00Z",
        check_start_ns=0,
        check_end_ns=1_000,
        reason_codes=[],
    )
    payload.update(overrides)
    digest = readiness_payload_document_digest(payload)
    return lkg_window_operational_readiness_evidence_from_payload(payload, canonical_document_digest=digest)


class LkgWindowOperationalReadinessEvidenceTests(unittest.TestCase):
    def test_payload_excludes_digest(self) -> None:
        payload = readiness_payload_document(_evidence())
        self.assertNotIn("canonical_document_digest", payload)

    def test_round_trip_reconstructs_equal_evidence(self) -> None:
        evidence = _evidence()
        payload = readiness_payload_document(evidence)
        digest = readiness_payload_document_digest(payload)
        reconstructed = lkg_window_operational_readiness_evidence_from_payload(
            payload, canonical_document_digest=digest
        )
        self.assertEqual(reconstructed, evidence)

    def test_digest_is_stable(self) -> None:
        payload = readiness_payload_document(_evidence())
        self.assertEqual(readiness_payload_document_digest(payload), readiness_payload_document_digest(payload))

    def test_digest_changes_when_a_field_changes(self) -> None:
        base = readiness_payload_document_digest(readiness_payload_document(_evidence()))
        changed = readiness_payload_document_digest(
            readiness_payload_document(_evidence(readiness_check_id="chk-2"))
        )
        self.assertNotEqual(base, changed)

    def test_direct_construction_rejects_mismatched_digest(self) -> None:
        payload = readiness_payload_document(_evidence())
        with self.assertRaises(ContractViolation):
            lkg_window_operational_readiness_evidence_from_payload(
                payload, canonical_document_digest="f" * 64
            )

    def test_window_epoch_sequence_derivation_enforced(self) -> None:
        with self.assertRaises(ContractViolation):
            _evidence(epoch_index=1)  # window_index=0 implies epoch_index=0
        with self.assertRaises(ContractViolation):
            _evidence(first_attempt_sequence=1)
        with self.assertRaises(ContractViolation):
            _evidence(last_attempt_sequence=200)
        with self.assertRaises(ContractViolation):
            _evidence(window_index=12)
        with self.assertRaises(ContractViolation):
            _evidence(window_index=-1)

    def test_health_passed_without_checked_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _evidence(health_checked=False, health_passed=True)

    def test_rollback_ready_without_tested_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _evidence(rollback_tested=False, rollback_ready=True)

    def test_check_end_before_start_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _evidence(check_start_ns=100, check_end_ns=50)

    def test_reason_codes_valid_sorted_deduplicated_accepted(self) -> None:
        evidence = _evidence(reason_codes=["ALPHA_CODE", "BETA_CODE"])
        self.assertEqual(evidence.reason_codes, ("ALPHA_CODE", "BETA_CODE"))

    def test_reason_codes_unsorted_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _evidence(reason_codes=["BETA_CODE", "ALPHA_CODE"])

    def test_reason_codes_duplicate_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _evidence(reason_codes=["ALPHA_CODE", "ALPHA_CODE"])

    def test_reason_codes_over_bound_rejected(self) -> None:
        codes = sorted(f"CODE_{i:02d}" for i in range(17))
        with self.assertRaises(ContractViolation):
            _evidence(reason_codes=codes)

    def test_reason_codes_malformed_pattern_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _evidence(reason_codes=["not-canonical"])


class Rfc3339UtcTests(unittest.TestCase):
    def test_valid_utc_with_fraction_accepted(self) -> None:
        validate_rfc3339_utc("2026-01-01T00:00:00.123456Z", field="x")

    def test_valid_utc_without_fraction_accepted(self) -> None:
        validate_rfc3339_utc("2026-01-01T00:00:00Z", field="x")

    def test_missing_z_suffix_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            validate_rfc3339_utc("2026-01-01T00:00:00+00:00", field="x")

    def test_mixed_fractional_precision_compares_by_parsed_instant(self) -> None:
        earlier = "2026-01-01T00:00:00Z"
        later = "2026-01-01T00:00:00.500000Z"
        # Naive string comparison gets this backwards: after the common
        # prefix, `later` continues with '.' (0x2E) while `earlier`
        # continues with 'Z' (0x5A), so `later < earlier` as raw strings
        # even though `later` is chronologically after `earlier` --
        # exactly why ordering must use parsed instants, never strings.
        self.assertLess(later, earlier)  # the misleading string order
        self.assertLess(parse_rfc3339_utc_instant(earlier), parse_rfc3339_utc_instant(later))


class FakeProviderTests(unittest.TestCase):
    def _context(self, **overrides):
        fields = dict(
            source_run_id="run-1", source_run_binding_sha256="a" * 64,
            window_index=0, epoch_index=0, first_attempt_sequence=0, last_attempt_sequence=199,
        )
        fields.update(overrides)
        return fields

    def test_first_capture_executes_and_stores(self) -> None:
        provider = FakeLkgWindowOperationalReadinessProvider()
        evidence = provider.capture_or_return(readiness_check_id="chk-1", **self._context())
        self.assertEqual(evidence.readiness_check_id, "chk-1")
        self.assertTrue(evidence.health_passed)

    def test_identical_retry_returns_byte_identical_evidence(self) -> None:
        provider = FakeLkgWindowOperationalReadinessProvider()
        first = provider.capture_or_return(readiness_check_id="chk-1", **self._context())
        second = provider.capture_or_return(readiness_check_id="chk-1", **self._context())
        self.assertEqual(first, second)
        self.assertEqual(first.provider_run_id, second.provider_run_id)
        self.assertEqual(first.checked_at_utc, second.checked_at_utc)

    def test_same_check_id_conflicting_context_rejected(self) -> None:
        provider = FakeLkgWindowOperationalReadinessProvider()
        provider.capture_or_return(readiness_check_id="chk-1", **self._context())
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as cm:
            provider.capture_or_return(readiness_check_id="chk-1", **self._context(window_index=1, epoch_index=0, first_attempt_sequence=200, last_attempt_sequence=399))
        self.assertEqual(str(cm.exception), "READINESS_CHECK_ID_CONFLICTING_RESULT")

    def test_same_window_different_check_id_rejected(self) -> None:
        provider = FakeLkgWindowOperationalReadinessProvider()
        provider.capture_or_return(readiness_check_id="chk-1", **self._context())
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as cm:
            provider.capture_or_return(readiness_check_id="chk-2", **self._context())
        self.assertEqual(str(cm.exception), "READINESS_WINDOW_ALREADY_CHECKED")

    def test_lookup_returns_stored_evidence_unchanged(self) -> None:
        provider = FakeLkgWindowOperationalReadinessProvider()
        captured = provider.capture_or_return(readiness_check_id="chk-1", **self._context())
        looked_up = provider.lookup(readiness_check_id="chk-1")
        self.assertEqual(captured, looked_up)

    def test_lookup_never_invokes_builder(self) -> None:
        calls = []

        def counting_builder(**kwargs):
            calls.append(1)
            from vdbench.lkg_window_readiness import _default_readiness_builder

            return _default_readiness_builder(**kwargs)

        provider = FakeLkgWindowOperationalReadinessProvider(builder=counting_builder)
        provider.capture_or_return(readiness_check_id="chk-1", **self._context())
        self.assertEqual(len(calls), 1)
        provider.lookup(readiness_check_id="chk-1")
        self.assertEqual(len(calls), 1)  # lookup must not call the builder

    def test_unknown_lookup_does_not_invoke_builder(self) -> None:
        calls = []

        def counting_builder(**kwargs):
            calls.append(1)
            from vdbench.lkg_window_readiness import _default_readiness_builder

            return _default_readiness_builder(**kwargs)

        provider = FakeLkgWindowOperationalReadinessProvider(builder=counting_builder)
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as cm:
            provider.lookup(readiness_check_id="never-captured")
        self.assertEqual(str(cm.exception), "RESULT_NOT_RECOVERABLE")
        self.assertEqual(len(calls), 0)

    def test_poisoned_lookup_does_not_invoke_builder(self) -> None:
        calls = []

        def counting_builder(**kwargs):
            calls.append(1)
            from vdbench.lkg_window_readiness import _default_readiness_builder

            return _default_readiness_builder(**kwargs)

        provider = FakeLkgWindowOperationalReadinessProvider(builder=counting_builder)
        provider.capture_or_return(readiness_check_id="chk-1", **self._context())
        self.assertEqual(len(calls), 1)
        provider.poison("chk-1")
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as cm:
            provider.lookup(readiness_check_id="chk-1")
        self.assertEqual(str(cm.exception), "RESULT_NOT_RECOVERABLE")
        self.assertEqual(len(calls), 1)  # poison does not re-invoke the builder either

    def test_poison_does_not_free_window_ownership_for_new_check_id(self) -> None:
        provider = FakeLkgWindowOperationalReadinessProvider()
        provider.capture_or_return(readiness_check_id="chk-a", **self._context())
        provider.poison("chk-a")
        with self.assertRaises(LkgWindowOperationalReadinessProviderError) as cm:
            provider.capture_or_return(readiness_check_id="chk-b", **self._context())
        self.assertEqual(str(cm.exception), "READINESS_WINDOW_ALREADY_CHECKED")

    def test_concurrent_identical_capture_executes_builder_exactly_once(self) -> None:
        calls = []
        call_lock = threading.Lock()

        def counting_builder(**kwargs):
            with call_lock:
                calls.append(1)
            from vdbench.lkg_window_readiness import _default_readiness_builder

            return _default_readiness_builder(**kwargs)

        provider = FakeLkgWindowOperationalReadinessProvider(builder=counting_builder)
        barrier = threading.Barrier(2)
        results: dict[str, LkgWindowOperationalReadinessEvidence] = {}

        def worker(name: str) -> None:
            barrier.wait(timeout=10)
            results[name] = provider.capture_or_return(readiness_check_id="chk-shared", **self._context())

        threads = [threading.Thread(target=worker, args=(f"t{i}",)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(len(calls), 1)
        self.assertEqual(results["t0"], results["t1"])

    def test_concurrent_different_check_ids_same_window_exactly_one_wins(self) -> None:
        provider = FakeLkgWindowOperationalReadinessProvider()
        barrier = threading.Barrier(2)
        errors: dict[str, Exception] = {}
        results: dict[str, LkgWindowOperationalReadinessEvidence] = {}

        def worker(name: str, check_id: str) -> None:
            barrier.wait(timeout=10)
            try:
                results[name] = provider.capture_or_return(readiness_check_id=check_id, **self._context())
            except LkgWindowOperationalReadinessProviderError as exc:
                errors[name] = exc

        t1 = threading.Thread(target=worker, args=("t1", "chk-a"))
        t2 = threading.Thread(target=worker, args=("t2", "chk-b"))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        self.assertEqual(len(results) + len(errors), 2)
        self.assertEqual(len(results), 1, "exactly one caller must win")
        self.assertEqual(len(errors), 1, "exactly one caller must lose")
        (losing_error,) = errors.values()
        self.assertEqual(str(losing_error), "READINESS_WINDOW_ALREADY_CHECKED")


if __name__ == "__main__":
    unittest.main()
