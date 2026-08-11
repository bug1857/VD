from __future__ import annotations

import ast
from dataclasses import fields
from pathlib import Path
import unittest

from tests.test_response_profile_semantic import _SemanticFixture, _digest
from tests.test_policy import decide
from vdbench.drift import DetectorState, DriftClassification
from vdbench.policy import PolicyAction
from vdbench.response_profile_detector_head import build_response_profile_detector_head
from vdbench.response_profile_freshness import (
    FreshResponseProfileEvidence,
    ResponseProfileFreshnessError,
    bind_fresh_response_profile_evidence,
    fresh_response_profile_evidence_payload,
    verify_fresh_response_profile_evidence,
)
from vdbench.response_profile_monitor_store import ResponseProfileMonitorStateStore
from vdbench.response_profile_projection import project_root_pinned_response_profile
from vdbench.response_profile_root_pin import issue_root_pinned_response_profile_evidence
from vdbench.shadow_event_types import MonitorStreamKey
from vdbench.workload_monitor import MonitorStreamState


MODULE = Path(__file__).parents[1] / "src" / "vdbench" / "response_profile_freshness.py"


def _forge(value: object, **changes: object):
    result = object.__new__(type(value))
    for item in fields(value):
        object.__setattr__(result, item.name, changes.get(item.name, getattr(value, item.name)))
    return result


class ResponseProfileFreshnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = _SemanticFixture()
        verification = issue_root_pinned_response_profile_evidence(
            bundle=cls.fixture.bundle,
            expectation=cls.fixture.expectation,
            expected_raw_evidence_sha256=(
                "e88ed05c5e961a21cfe768b872f0cb721458713d5b755c0da5501f0bd32a05d2"
            ),
        )
        cls.capability = verification
        cls.profile = project_root_pinned_response_profile(
            capability=verification,
            expected_raw_evidence_sha256=verification.raw_evidence_sha256,
            expected_identity=cls.fixture.identity,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.fixture.close()

    def setUp(self) -> None:
        self.store = self.fixture.monitor_store
        self.latest = self.fixture.latest_detector_head

    def _bind(self, **changes: object):
        values = {
            "capability": self.capability,
            "profile": self.profile,
            "control": self.fixture.control,
            "verified_latest_detector_head": self.latest,
        }
        values.update(changes)
        return bind_fresh_response_profile_evidence(**values)

    def test_exact_profile_control_and_latest_head_bind_deterministically(self) -> None:
        value = self._bind()
        self.assertIs(type(value), FreshResponseProfileEvidence)
        self.assertEqual(verify_fresh_response_profile_evidence(value), value)
        self.assertEqual(
            value.fresh_evidence_sha256,
            "3358b0ac7e881d75e01860a151f37fbc262e99841a6b19d9b6e1f3fa01f811ad",
        )
        self.assertEqual(
            fresh_response_profile_evidence_payload(value)["detector_head_record_sequence"],
            0,
        )

    def test_plain_head_and_malformed_latest_wrapper_fail_closed(self) -> None:
        with self.assertRaises(ResponseProfileFreshnessError) as raised:
            self._bind(verified_latest_detector_head=self.latest.head)
        self.assertEqual(raised.exception.code, "LATEST_DETECTOR_HEAD_REQUIRED")
        malformed = _forge(self.latest, head_record_sequence=False)
        with self.assertRaises(ResponseProfileFreshnessError) as raised:
            self._bind(verified_latest_detector_head=malformed)
        self.assertEqual(raised.exception.code, "LATEST_DETECTOR_HEAD_INVALID")

    def test_control_or_latest_head_identity_mismatch_fails_closed(self) -> None:
        altered_head = build_response_profile_detector_head(
            stream_key=self.fixture.control.stream_key,
            window_sequence=3,
            detector_state=DetectorState.NO_DRIFT,
            detector_classification=DriftClassification.NONE,
            detector_provenance=self.fixture.control.detector_provenance,
        )
        altered_latest = _forge(self.latest, head=altered_head)
        with self.assertRaises(ResponseProfileFreshnessError) as raised:
            self._bind(verified_latest_detector_head=altered_latest)
        self.assertEqual(raised.exception.code, "DETECTOR_HEAD_MISMATCH")

    def test_substituted_durable_head_record_digest_fails_closed(self) -> None:
        substituted = _forge(self.latest, head_record_sha256=_digest("f"))
        with self.assertRaises(ResponseProfileFreshnessError) as raised:
            self._bind(verified_latest_detector_head=substituted)
        self.assertEqual(raised.exception.code, "DETECTOR_HEAD_MISMATCH")

    def test_cross_stream_head_substitution_fails_closed(self) -> None:
        stream = self.fixture.control.stream_key
        other_stream = MonitorStreamKey(
            "other-stream",
            stream.metric,
            stream.threshold_stratum,
            stream.configuration_identity,
            stream.data_identity,
            stream.flat_binding_id,
            stream.hnsw_binding_id,
        )
        other_head = build_response_profile_detector_head(
            stream_key=other_stream,
            window_sequence=self.latest.head.window_sequence,
            detector_state=self.latest.head.detector_state,
            detector_classification=self.latest.head.detector_classification,
            detector_provenance=self.latest.head.detector_provenance,
        )
        with self.assertRaises(ResponseProfileFreshnessError) as raised:
            self._bind(
                verified_latest_detector_head=_forge(self.latest, head=other_head)
            )
        self.assertEqual(raised.exception.code, "DETECTOR_HEAD_MISMATCH")

    def test_historical_evidence_is_non_authorizing_and_cannot_cross_b001(self) -> None:
        historical = self._bind()
        later_head = build_response_profile_detector_head(
            stream_key=self.fixture.control.stream_key,
            window_sequence=3,
            detector_state=DetectorState.NO_DRIFT,
            detector_classification=DriftClassification.NONE,
            detector_provenance=self.fixture.control.detector_provenance,
        )
        self.store.save(
            MonitorStreamState(
                stream_key=self.fixture.control.stream_key,
                next_window_sequence=4,
                latest_detector_head=later_head,
            )
        )
        result = decide(profile_authority=historical)
        self.assertIs(result.action, PolicyAction.RECOMMEND_EF)
        self.assertEqual(result.reason, "RESPONSE_PROFILE_AUTHORITY_UNAVAILABLE")
        self.assertNotEqual(
            self.store.load_verified_latest(self.fixture.control.stream_key).head_record_sha256,
            historical.verified_latest_detector_head.head_record_sha256,
        )

    def test_forged_profile_or_fresh_digest_fails_reconstruction(self) -> None:
        with self.assertRaises(ResponseProfileFreshnessError):
            self._bind(profile=_forge(self.profile, profile_sha256=_digest("f")))
        with self.assertRaises(ResponseProfileFreshnessError):
            verify_fresh_response_profile_evidence(
                _forge(self._bind(), fresh_evidence_sha256=_digest("f"))
            )

    def test_boundary_has_no_policy_admission_or_actuation_dependency(self) -> None:
        tree = ast.parse(MODULE.read_text(encoding="utf-8"))
        imported = {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        } | {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        forbidden = {"policy", "canary_admission", "canary_live_runner", "actuation", "pymilvus"}
        self.assertFalse({
            item for item in imported
            if any(item == name or item.endswith(f".{name}") for name in forbidden)
        })


if __name__ == "__main__":
    unittest.main()
