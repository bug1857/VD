"""Offline security-contract tests for EXP-009 Stage 2 approval grants."""

from __future__ import annotations

import base64
import unittest
from dataclasses import replace

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from vdbench.canary_approval import (
    ApprovalVerificationContext,
    CanaryApprovalGrant,
    StaticCanaryApprovalTrustStore,
    approval_grant_from_bytes,
    approval_grant_signing_bytes,
    approval_grant_to_bytes,
    policy_decision_sha256,
    verify_canary_approval_grant,
)
from vdbench.config import Metric
from vdbench.drift import build_evidence_provenance
from vdbench.policy import (
    PolicyAction,
    PolicyDecision,
    PolicyMode,
    SafetyGateResult,
)


def _sha(character: str) -> str:
    return character * 64


def _provenance():
    return build_evidence_provenance(
        metric=Metric.L2,
        threshold_stratum="target-075",
        reference_window_id="reference-001",
        current_window_id="current-001",
        reference_manifest_sha256=_sha("a"),
        current_manifest_sha256=_sha("b"),
        configuration_identity="configuration-001",
        data_identity="dataset-001",
        flat_binding_id="flat-binding-001",
        hnsw_binding_id="hnsw-binding-001",
        reference_audit_ids=tuple(f"reference-{index:02d}" for index in range(50)),
        reference_audit_rank_digests=tuple(_sha("c") for _ in range(50)),
        current_audit_ids=tuple(f"current-{index:02d}" for index in range(50)),
        current_audit_rank_digests=tuple(_sha("d") for _ in range(50)),
    )


def _decision() -> PolicyDecision:
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
        safety_gate_results=(
            SafetyGateResult("PRE_ACTION", True, "all checks passed"),
        ),
        mode=PolicyMode.CANARY_ENABLED,
        audit_id="policy-audit-001",
        evidence_provenance=_provenance(),
    )


def _unsigned_grant(decision: PolicyDecision) -> CanaryApprovalGrant:
    provenance = decision.evidence_provenance
    assert provenance is not None
    return CanaryApprovalGrant(
        grant_id="grant-001",
        key_id="operator-key-001",
        issued_at_utc="2026-08-04T04:00:00Z",
        expires_at_utc="2026-08-04T04:30:00Z",
        experiment_id="EXP-009",
        policy_decision_sha256=policy_decision_sha256(decision),
        policy_audit_id=decision.audit_id,
        metric=Metric.L2,
        threshold_stratum="target-075",
        current_ef=400,
        candidate_ef=800,
        last_known_good_ef=400,
        configuration_identity=provenance.configuration_identity,
        data_identity=provenance.data_identity,
        flat_binding_id=provenance.flat_binding_id,
        hnsw_binding_id=provenance.hnsw_binding_id,
        eligible_workload_sha256=_sha("e"),
        candidate_selection_sha256=_sha("f"),
        routing_population_count=600,
        candidate_count=60,
        maximum_fraction=0.10,
        rollback_pre_authorized=True,
        signature=None,
    )


def _signed_grant(
    private_key: Ed25519PrivateKey,
    decision: PolicyDecision,
) -> CanaryApprovalGrant:
    unsigned = _unsigned_grant(decision)
    signature = private_key.sign(approval_grant_signing_bytes(unsigned))
    return replace(
        unsigned,
        signature=base64.urlsafe_b64encode(signature).decode("ascii").rstrip("="),
    )


def _context(decision: PolicyDecision, *, now_utc: str = "2026-08-04T04:05:00Z") -> ApprovalVerificationContext:
    return ApprovalVerificationContext(
        decision=decision,
        expected_experiment_id="EXP-009",
        eligible_workload_sha256=_sha("e"),
        candidate_selection_sha256=_sha("f"),
        now_utc=now_utc,
    )


class CanaryApprovalGrantTests(unittest.TestCase):
    def setUp(self) -> None:
        self.private_key = Ed25519PrivateKey.generate()
        self.decision = _decision()
        self.grant = _signed_grant(self.private_key, self.decision)
        self.trust_store = StaticCanaryApprovalTrustStore(
            public_keys={"operator-key-001": self.private_key.public_key()},
        )

    def test_valid_signed_grant_is_approved_and_binds_every_required_input(self) -> None:
        result = verify_canary_approval_grant(
            self.grant,
            trust_store=self.trust_store,
            context=_context(self.decision),
        )

        self.assertTrue(result.approved)
        self.assertIsNone(result.reason_code)
        self.assertEqual(result.grant, self.grant)

    def test_policy_digest_changes_when_a_nested_safety_gate_changes(self) -> None:
        changed = replace(
            self.decision,
            safety_gate_results=(
                SafetyGateResult("PRE_ACTION", False, "health failed"),
            ),
        )

        self.assertNotEqual(
            policy_decision_sha256(self.decision),
            policy_decision_sha256(changed),
        )
        result = verify_canary_approval_grant(
            self.grant,
            trust_store=self.trust_store,
            context=_context(changed),
        )
        self.assertFalse(result.approved)
        self.assertEqual(result.reason_code, "POLICY_DECISION_MISMATCH")

    def test_invalid_signature_is_refused_before_any_route_can_exist(self) -> None:
        tampered = replace(
            self.grant,
            configuration_identity="different-but-canonical-identity",
        )

        result = verify_canary_approval_grant(
            tampered,
            trust_store=self.trust_store,
            context=_context(self.decision),
        )

        self.assertFalse(result.approved)
        self.assertEqual(result.reason_code, "GRANT_SIGNATURE_INVALID")

    def test_signed_grant_round_trips_through_its_strict_storage_document(self) -> None:
        self.assertEqual(
            approval_grant_from_bytes(approval_grant_to_bytes(self.grant)),
            self.grant,
        )
        with self.assertRaisesRegex(ValueError, "noncanonical"):
            approval_grant_from_bytes(b" " + approval_grant_to_bytes(self.grant))

    def test_expired_and_revoked_grants_are_independently_refused(self) -> None:
        expired = verify_canary_approval_grant(
            self.grant,
            trust_store=self.trust_store,
            context=_context(self.decision, now_utc="2026-08-04T04:30:00Z"),
        )
        revoked = verify_canary_approval_grant(
            self.grant,
            trust_store=StaticCanaryApprovalTrustStore(
                public_keys={"operator-key-001": self.private_key.public_key()},
                revoked_grant_ids=frozenset({"grant-001"}),
            ),
            context=_context(self.decision),
        )

        self.assertEqual(expired.reason_code, "GRANT_EXPIRED")
        self.assertEqual(revoked.reason_code, "GRANT_REVOKED")

    def test_not_yet_valid_unknown_and_revoked_keys_are_independently_refused(self) -> None:
        not_yet_valid = verify_canary_approval_grant(
            self.grant,
            trust_store=self.trust_store,
            context=_context(self.decision, now_utc="2026-08-04T03:59:59Z"),
        )
        unknown_key = verify_canary_approval_grant(
            self.grant,
            trust_store=StaticCanaryApprovalTrustStore(public_keys={}),
            context=_context(self.decision),
        )
        revoked_key = verify_canary_approval_grant(
            self.grant,
            trust_store=StaticCanaryApprovalTrustStore(
                public_keys={"operator-key-001": self.private_key.public_key()},
                revoked_key_ids=frozenset({"operator-key-001"}),
            ),
            context=_context(self.decision),
        )

        self.assertEqual(not_yet_valid.reason_code, "GRANT_NOT_YET_VALID")
        self.assertEqual(unknown_key.reason_code, "SIGNING_KEY_UNKNOWN")
        self.assertEqual(revoked_key.reason_code, "SIGNING_KEY_REVOKED")

    def test_workload_and_selection_mismatches_are_refused_even_with_valid_signature(self) -> None:
        wrong_workload = verify_canary_approval_grant(
            self.grant,
            trust_store=self.trust_store,
            context=replace(_context(self.decision), eligible_workload_sha256=_sha("0")),
        )
        wrong_selection = verify_canary_approval_grant(
            self.grant,
            trust_store=self.trust_store,
            context=replace(_context(self.decision), candidate_selection_sha256=_sha("1")),
        )

        self.assertEqual(wrong_workload.reason_code, "ELIGIBLE_WORKLOAD_MISMATCH")
        self.assertEqual(wrong_selection.reason_code, "CANDIDATE_SELECTION_MISMATCH")

    def test_missing_grant_and_wrong_identity_fail_closed(self) -> None:
        missing = verify_canary_approval_grant(
            None,
            trust_store=self.trust_store,
            context=_context(self.decision),
        )
        unsigned_wrong_identity = replace(
            _unsigned_grant(self.decision),
            configuration_identity="different-configuration",
        )
        wrong_identity = verify_canary_approval_grant(
            replace(
                unsigned_wrong_identity,
                signature=base64.urlsafe_b64encode(
                    self.private_key.sign(
                        approval_grant_signing_bytes(unsigned_wrong_identity)
                    )
                )
                .decode("ascii")
                .rstrip("="),
            ),
            trust_store=self.trust_store,
            context=_context(self.decision),
        )

        self.assertEqual(missing.reason_code, "GRANT_MISSING")
        self.assertEqual(
            wrong_identity.reason_code,
            "CONFIGURATION_IDENTITY_MISMATCH",
        )

    def test_non_canary_policy_and_duplicate_json_fields_are_refused(self) -> None:
        no_change = replace(self.decision, action=PolicyAction.NO_CHANGE)
        ineligible_grant = _signed_grant(self.private_key, no_change)
        ineligible = verify_canary_approval_grant(
            ineligible_grant,
            trust_store=self.trust_store,
            context=_context(no_change),
        )
        duplicate_payload = approval_grant_to_bytes(self.grant).replace(
            b'"candidate_count":60,',
            b'"candidate_count":60,"candidate_count":60,',
            1,
        )

        self.assertEqual(ineligible.reason_code, "POLICY_DECISION_NOT_CANARY")
        with self.assertRaisesRegex(ValueError, "malformed"):
            approval_grant_from_bytes(duplicate_payload)

    def test_malformed_policy_gate_fails_closed_without_raising(self) -> None:
        malformed = replace(self.decision, safety_gate_results=(object(),))

        result = verify_canary_approval_grant(
            self.grant,
            trust_store=self.trust_store,
            context=_context(malformed),
        )

        self.assertFalse(result.approved)
        self.assertEqual(result.reason_code, "POLICY_DECISION_INVALID")


if __name__ == "__main__":
    unittest.main()
