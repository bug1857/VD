from __future__ import annotations

import copy
from dataclasses import fields
import unittest

from vdbench.config import Metric
from vdbench.drift import build_evidence_provenance
from vdbench.response_profile_control import (
    ResponseProfileControl,
    ResponseProfileControlError,
    build_response_profile_control,
    response_profile_control_document,
    response_profile_control_from_document,
    response_profile_control_payload,
    verify_response_profile_control,
)
from vdbench.shadow_event_types import MonitorStreamKey


def _digest(character: str) -> str:
    return character * 64


def _provenance(**changes: object):
    values = {
        "metric": Metric.L2,
        "threshold_stratum": "target-075",
        "reference_window_id": "ref",
        "current_window_id": "cur",
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


def _control(**changes: object):
    values = {
        "stream_key": MonitorStreamKey(
            "stream", Metric.L2, "target-075", "cfg", "data", "flat", "hnsw"
        ),
        "detector_provenance": _provenance(),
        "trigger_window_sequence": 2,
        "detector_head_sha256": _digest("4"),
        "detector_head_record_sequence": 0,
        "detector_head_record_sha256": _digest("5"),
        "detector_head_persisted_at_utc": "2026-08-10T23:59:58Z",
        "calibration_population_sha256": _digest("e"),
        "warmup_role_manifest_sha256": _digest("f"),
        "ordered_query_payload_sha256": _digest("1"),
        "replay_schedule_sha256": _digest("2"),
        "environment_manifest_sha256": _digest("3"),
        "source_revision": "rev",
        "frozen_at_utc": "2026-08-11T00:00:00Z",
    }
    values.update(changes)
    return build_response_profile_control(**values)


def _forge(value: object, **changes: object):
    result = object.__new__(type(value))
    for item in fields(value):
        object.__setattr__(
            result, item.name, changes.get(item.name, getattr(value, item.name))
        )
    return result


class ResponseProfileControlTests(unittest.TestCase):
    def test_golden_control_digest_and_reconstruction(self) -> None:
        value = _control()
        self.assertEqual(
            value.control_profile_sha256,
            "88403cdc59e2df21eaab63b6180ed95799263caf123808ba53f31f607ffeb7a1",
        )
        self.assertEqual(verify_response_profile_control(value), value)
        self.assertEqual(
            response_profile_control_payload(value)["trigger_window_sequence"], 2
        )

    def test_public_constructor_and_forged_digest_are_rejected(self) -> None:
        with self.assertRaises(TypeError):
            ResponseProfileControl()
        with self.assertRaises(ResponseProfileControlError):
            verify_response_profile_control(
                _forge(_control(), control_profile_sha256=_digest("0"))
            )

    def test_stream_and_detector_provenance_must_match_exactly(self) -> None:
        with self.assertRaises(ResponseProfileControlError) as raised:
            _control(detector_provenance=_provenance(data_identity="other"))
        self.assertEqual(raised.exception.code, "CONTROL_PROVENANCE_MISMATCH")

    def test_boolean_sequence_and_noncanonical_timestamp_fail_closed(self) -> None:
        for changes in (
            {"trigger_window_sequence": False},
            {"frozen_at_utc": "2026-13-11T00:00:00Z"},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises((ResponseProfileControlError, ValueError)):
                    _control(**changes)

    def test_control_must_follow_the_bound_durable_head_record(self) -> None:
        for persisted_at in (
            "2026-08-11T00:00:00Z",
            "2026-08-11T00:00:01Z",
        ):
            with self.subTest(persisted_at=persisted_at):
                with self.assertRaises(ResponseProfileControlError) as raised:
                    _control(detector_head_persisted_at_utc=persisted_at)
                self.assertEqual(
                    raised.exception.code,
                    "CONTROL_FROZEN_BEFORE_HEAD_COMMIT",
                )

    def test_object_forged_plain_string_metric_is_rejected(self) -> None:
        stream = _control().stream_key
        forged_stream = object.__new__(MonitorStreamKey)
        for item in fields(stream):
            object.__setattr__(
                forged_stream,
                item.name,
                "L2" if item.name == "metric" else getattr(stream, item.name),
            )
        with self.assertRaises(ResponseProfileControlError):
            _control(stream_key=forged_stream)


class ResponseProfileControlDocumentTests(unittest.TestCase):
    """Coverage for response_profile_control_document/_from_document."""

    def test_canonical_round_trip_is_exact(self) -> None:
        value = _control()
        document = response_profile_control_document(value)
        rebuilt = response_profile_control_from_document(document)
        self.assertEqual(rebuilt, value)
        self.assertEqual(response_profile_control_document(rebuilt), document)

    def test_missing_top_level_field_rejected(self) -> None:
        document = copy.deepcopy(response_profile_control_document(_control()))
        del document["control_profile_sha256"]
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(document)

    def test_unknown_top_level_field_rejected(self) -> None:
        document = copy.deepcopy(response_profile_control_document(_control()))
        document["extra"] = "x"
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(document)

    def test_missing_payload_field_rejected(self) -> None:
        document = copy.deepcopy(response_profile_control_document(_control()))
        del document["control_payload"]["source_revision"]
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(document)

    def test_wrong_schema_version_rejected(self) -> None:
        document = copy.deepcopy(response_profile_control_document(_control()))
        document["control_payload"]["schema_version"] = "response-profile-control-v0"
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(document)

    def test_bool_as_int_for_trigger_window_sequence_rejected(self) -> None:
        document = copy.deepcopy(response_profile_control_document(_control()))
        document["control_payload"]["trigger_window_sequence"] = True
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(document)

    def test_malformed_stream_metric_enum_rejected(self) -> None:
        document = copy.deepcopy(response_profile_control_document(_control()))
        document["control_payload"]["stream"]["metric"] = "NOT_A_METRIC"
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(document)

    def test_malformed_sha256_rejected(self) -> None:
        document = copy.deepcopy(response_profile_control_document(_control()))
        document["control_payload"]["detector_head_sha256"] = "not-a-digest"
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(document)

    def test_malformed_timestamp_rejected(self) -> None:
        document = copy.deepcopy(response_profile_control_document(_control()))
        document["control_payload"]["frozen_at_utc"] = "2026-13-40T00:00:00Z"
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(document)

    def test_tampered_provenance_digest_rejected(self) -> None:
        document = copy.deepcopy(response_profile_control_document(_control()))
        document["control_payload"]["detector_provenance"]["sha256"] = _digest("9")
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(document)

    def test_tampered_outer_digest_rejected(self) -> None:
        document = copy.deepcopy(response_profile_control_document(_control()))
        document["control_profile_sha256"] = _digest("0")
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(document)

    def test_wrong_type_document_rejected(self) -> None:
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document("not a dict")
        with self.assertRaises(ResponseProfileControlError):
            response_profile_control_from_document(None)

    # -- cross-object substitution (§16 of the governing task) -----------

    def test_control_from_a_different_stream_is_not_equal(self) -> None:
        """A structurally valid control document bound to a *different*
        stream lineage must never be silently accepted as interchangeable
        with the original -- proven here by exact non-equality of the
        reconstructed object and its digest, which is what any downstream
        cross-object equality check in the CLI/producer composition relies
        on to reject a substituted lineage."""

        original = _control()
        other_stream_control = _control(
            stream_key=MonitorStreamKey(
                "other-stream", Metric.L2, "target-075", "cfg", "data", "flat", "hnsw"
            ),
            detector_provenance=_provenance(configuration_identity="cfg"),
        )
        self.assertNotEqual(
            original.control_profile_sha256, other_stream_control.control_profile_sha256
        )
        rebuilt = response_profile_control_from_document(
            response_profile_control_document(other_stream_control)
        )
        self.assertNotEqual(rebuilt.control_profile_sha256, original.control_profile_sha256)


if __name__ == "__main__":
    unittest.main()
