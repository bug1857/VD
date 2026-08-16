from __future__ import annotations

import ast
import unittest
from dataclasses import fields
from pathlib import Path

from tests.test_response_profile_semantic import _digest, _SemanticFixture
from vdbench.response_profile import ResponseProfileIdentity
from vdbench.response_profile_projection import project_root_pinned_response_profile
from vdbench.response_profile_root_pin import (
    ResponseProfileRootPinError,
    RootPinnedResponseProfileEvidence,
    issue_root_pinned_response_profile_evidence,
    root_pinned_response_profile_evidence_payload,
    verify_root_pinned_response_profile_evidence,
)

ROOT_MODULE = Path(__file__).parents[1] / "src" / "vdbench" / "response_profile_root_pin.py"
PROJECTION_MODULE = Path(__file__).parents[1] / "src" / "vdbench" / "response_profile_projection.py"
EXPECTED_ROOT = "e88ed05c5e961a21cfe768b872f0cb721458713d5b755c0da5501f0bd32a05d2"


def _forge(value: object, **changes: object):
    forged = object.__new__(type(value))
    for field in fields(value):
        object.__setattr__(
            forged, field.name, changes.get(field.name, getattr(value, field.name))
        )
    return forged


class ResponseProfileRootPinTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = _SemanticFixture()
        cls.capability = issue_root_pinned_response_profile_evidence(
            bundle=cls.fixture.bundle,
            expectation=cls.fixture.expectation,
            expected_raw_evidence_sha256=EXPECTED_ROOT,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.fixture.close()

    def test_root_pinned_issuance_reruns_r2c_and_binds_exact_root(self) -> None:
        self.assertIs(type(self.capability), RootPinnedResponseProfileEvidence)
        self.assertEqual(self.capability.raw_evidence_sha256, EXPECTED_ROOT)
        self.assertEqual(len(self.capability.observations), 1200)
        payload = root_pinned_response_profile_evidence_payload(self.capability)
        self.assertEqual(payload["raw_evidence_sha256"], EXPECTED_ROOT)

    def test_wrong_independent_root_refuses_issuance(self) -> None:
        with self.assertRaises(ResponseProfileRootPinError) as raised:
            issue_root_pinned_response_profile_evidence(
                bundle=self.fixture.bundle,
                expectation=self.fixture.expectation,
                expected_raw_evidence_sha256=_digest("f"),
            )
        self.assertEqual(raised.exception.code, "RAW_EVIDENCE_ROOT_MISMATCH")

    def test_public_manual_capability_construction_is_rejected(self) -> None:
        with self.assertRaises(TypeError):
            RootPinnedResponseProfileEvidence()

    def test_forged_capability_digest_and_observations_fail_closed(self) -> None:
        for label, forged in (
            ("digest", _forge(self.capability, capability_sha256=_digest("f"))),
            ("count", _forge(self.capability, observations=self.capability.observations[:-1])),
        ):
            with (
                self.subTest(label=label),
                self.assertRaises(ResponseProfileRootPinError),
            ):
                verify_root_pinned_response_profile_evidence(
                    forged,
                    expected_raw_evidence_sha256=EXPECTED_ROOT,
                    expected_identity=self.fixture.identity,
                )

    def test_expected_identity_mismatch_fails_closed(self) -> None:
        identity = self.fixture.identity
        mismatched = ResponseProfileIdentity(
            metric=identity.metric,
            threshold_stratum=identity.threshold_stratum,
            search_configurations=identity.search_configurations,
            hnsw_index_identity="different-index",
            data_identity=identity.data_identity,
            workload_manifest_sha256=identity.workload_manifest_sha256,
            ordered_query_payload_sha256=identity.ordered_query_payload_sha256,
            replay_schedule_sha256=identity.replay_schedule_sha256,
            control_profile_sha256=identity.control_profile_sha256,
            environment_manifest_sha256=identity.environment_manifest_sha256,
            source_revision=identity.source_revision,
            calibration_started_at_utc=identity.calibration_started_at_utc,
            calibration_completed_at_utc=identity.calibration_completed_at_utc,
            generated_at_utc=identity.generated_at_utc,
        )
        with self.assertRaises(ResponseProfileRootPinError) as raised:
            verify_root_pinned_response_profile_evidence(
                self.capability,
                expected_raw_evidence_sha256=EXPECTED_ROOT,
                expected_identity=mismatched,
            )
        self.assertEqual(raised.exception.code, "PROFILE_IDENTITY_MISMATCH")

    def test_projection_uses_unchanged_r1_builder_and_preserves_root(self) -> None:
        profile = project_root_pinned_response_profile(
            capability=self.capability,
            expected_raw_evidence_sha256=EXPECTED_ROOT,
            expected_identity=self.fixture.identity,
        )
        self.assertEqual(profile.raw_evidence_sha256, EXPECTED_ROOT)
        self.assertEqual(len(profile.estimates), 4)
        self.assertTrue(all(item.mean_recall == 1.0 for item in profile.estimates))
        self.assertTrue(all(item.p95_latency_ms == 1.0 for item in profile.estimates))

    def test_root_pin_and_projection_modules_have_no_candidate_dependencies(self) -> None:
        forbidden = {
            "policy",
            "canary_admission",
            "canary_live_runner",
            "lkg_phase3_authority",
            "actuation",
            "pymilvus",
        }
        for path in (ROOT_MODULE, PROJECTION_MODULE):
            with self.subTest(path=path.name):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                modules = {
                    node.module or ""
                    for node in ast.walk(tree)
                    if isinstance(node, ast.ImportFrom)
                } | {
                    alias.name
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Import)
                    for alias in node.names
                }
                self.assertFalse(
                    {
                        item
                        for item in modules
                        if any(item == name or item.endswith(f".{name}") for name in forbidden)
                    }
                )


if __name__ == "__main__":
    unittest.main()
