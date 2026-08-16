from __future__ import annotations

import unittest
from dataclasses import fields

from vdbench.config import Metric
from vdbench.drift import (
    DetectorState,
    DriftClassification,
    build_evidence_provenance,
)
from vdbench.response_profile_detector_head import (
    ResponseProfileDetectorHead,
    ResponseProfileDetectorHeadError,
    build_response_profile_detector_head,
    response_profile_detector_head_payload,
    verify_response_profile_detector_head,
)
from vdbench.shadow_event_types import MonitorStreamKey


def _digest(character: str) -> str:
    return character * 64


def _stream() -> MonitorStreamKey:
    return MonitorStreamKey("stream", Metric.L2, "target-075", "cfg", "data", "flat", "hnsw")


def _provenance(**changes: object):
    values = {
        "metric": Metric.L2,
        "threshold_stratum": "target-075",
        "reference_window_id": "reference",
        "current_window_id": "current",
        "reference_manifest_sha256": _digest("a"),
        "current_manifest_sha256": _digest("b"),
        "configuration_identity": "cfg",
        "data_identity": "data",
        "flat_binding_id": "flat",
        "hnsw_binding_id": "hnsw",
        "reference_audit_ids": tuple(range(50)),
        "reference_audit_rank_digests": tuple(_digest("c") for _ in range(50)),
        "current_audit_ids": tuple(range(50, 100)),
        "current_audit_rank_digests": tuple(_digest("d") for _ in range(50)),
    }
    values.update(changes)
    return build_evidence_provenance(**values)


def _head(**changes: object):
    values = {
        "stream_key": _stream(),
        "window_sequence": 2,
        "detector_state": DetectorState.NO_DRIFT,
        "detector_classification": DriftClassification.NONE,
        "detector_provenance": _provenance(),
    }
    values.update(changes)
    return build_response_profile_detector_head(**values)


def _forge(value: object, **changes: object):
    result = object.__new__(type(value))
    for item in fields(value):
        object.__setattr__(result, item.name, changes.get(item.name, getattr(value, item.name)))
    return result


class ResponseProfileDetectorHeadTests(unittest.TestCase):
    def test_golden_digest_and_reconstruction(self) -> None:
        value = _head()
        self.assertEqual(
            value.detector_head_sha256,
            "ce9d12e0d746d3f406e213ca3dadb485a893561b8af2be38c1532f8ed5551a7b",
        )
        self.assertEqual(verify_response_profile_detector_head(value), value)
        self.assertEqual(response_profile_detector_head_payload(value)["window_sequence"], 2)

    def test_public_constructor_and_forged_values_fail_closed(self) -> None:
        with self.assertRaises(TypeError):
            ResponseProfileDetectorHead()
        for value in (
            _forge(_head(), window_sequence=False),
            _forge(_head(), detector_head_sha256=_digest("0")),
        ):
            with (
                self.subTest(value=value),
                self.assertRaises(ResponseProfileDetectorHeadError),
            ):
                verify_response_profile_detector_head(value)

    def test_stream_and_provenance_mismatch_fails_closed(self) -> None:
        with self.assertRaises(ResponseProfileDetectorHeadError) as raised:
            _head(detector_provenance=_provenance(data_identity="other"))
        self.assertEqual(raised.exception.code, "DETECTOR_HEAD_STREAM_MISMATCH")

    def test_terminal_detector_outcome_is_bound_and_must_be_consistent(self) -> None:
        drift = _head(
            detector_state=DetectorState.DRIFT,
            detector_classification=DriftClassification.INPUT_DRIFT,
        )
        self.assertNotEqual(drift.detector_head_sha256, _head().detector_head_sha256)
        with self.assertRaises(ResponseProfileDetectorHeadError) as raised:
            _head(detector_classification=DriftClassification.INPUT_DRIFT)
        self.assertEqual(raised.exception.code, "DETECTOR_HEAD_OUTCOME_INVALID")

    def test_sequence_before_first_evaluation_is_rejected(self) -> None:
        for value in (False, 0, 1):
            with (
                self.subTest(value=value),
                self.assertRaises(ResponseProfileDetectorHeadError),
            ):
                _head(window_sequence=value)


if __name__ == "__main__":
    unittest.main()
