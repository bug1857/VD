from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from dataclasses import fields
from pathlib import Path
from unittest import mock

from tests.test_gate_c_bounded_execution import _fixture, _fixture_v2
from vdbench.canonical_serialization import strict_canonical_json_bytes
from vdbench.gate_c_bounded_execution import (
    GateCBoundedExecutionError,
    GateCBoundedExecutionEnvelopeV2,
    GateCWindowExecutionBound,
    build_gate_c_canonical_state,
    build_gate_c_checkpoint_result,
    build_gate_c_checkpoint_result_v2,
    build_gate_c_window_checkpoint_effect,
)
from vdbench.gate_c_checkpoint_store import (
    GateCCheckpointLedgerBinding,
    GateCCheckpointLedgerError,
    SQLiteGateCCheckpointLedger,
)


def _binding(envelope):
    return GateCCheckpointLedgerBinding(
        campaign_identity=envelope.campaign_identity,
        campaign_binding_sha256=envelope.campaign_binding_sha256,
        scale_contract_sha256=envelope.scale_contract.contract_sha256,
        source_revision=envelope.source_revision,
    )


def _result(envelope):
    pre = build_gate_c_canonical_state(
        next_window_sequence=0,
        acknowledgement_count=0,
        acknowledgement_head_sha256=None,
        attempt_count=0,
        attempt_event_count=0,
        attempt_event_head_sha256=None,
        detector_event_count=0,
        detector_event_head_sha256=None,
        attestation_record_count=0,
        attestation_record_head_sha256=None,
        finalization_window_count=0,
        finalization_event_count=0,
        finalization_event_head_sha256=None,
        telemetry_record_count=0,
        telemetry_record_head_sha256=None,
    )
    effect = build_gate_c_window_checkpoint_effect(
        window_sequence=0,
        source_window_sha256="1" * 64,
        attempt_sha256s=tuple(f"{index:064x}" for index in range(1, 5)),
        attempt_event_head_sha256="5" * 64,
        detector_event_sha256="6" * 64,
        detector_status="REBASELINE",
        detector_head_sha256=None,
        attestation_disposition="NOT_REQUIRED",
        attestation_record_sha256=None,
        attestation_record_head_sha256=None,
        attestation_sha256=None,
        prepared_sha256="7" * 64,
        acknowledgement_head_sha256="8" * 64,
        finalization_event_head_sha256="9" * 64,
        telemetry_record_count=400,
        telemetry_record_head_sha256="a" * 64,
    )
    post = build_gate_c_canonical_state(
        next_window_sequence=1,
        acknowledgement_count=200,
        acknowledgement_head_sha256="8" * 64,
        attempt_count=4,
        attempt_event_count=8,
        attempt_event_head_sha256="5" * 64,
        detector_event_count=1,
        detector_event_head_sha256="6" * 64,
        attestation_record_count=0,
        attestation_record_head_sha256=None,
        finalization_window_count=1,
        finalization_event_count=5,
        finalization_event_head_sha256="9" * 64,
        telemetry_record_count=400,
        telemetry_record_head_sha256="a" * 64,
    )
    builder = (
        build_gate_c_checkpoint_result_v2
        if type(envelope) is GateCBoundedExecutionEnvelopeV2
        else build_gate_c_checkpoint_result
    )
    return builder(
        envelope=envelope,
        pre_state=pre,
        post_state=post,
        processed_window_sequences=(0,),
        checkpoint_effects=(effect,),
    )


class GateCCheckpointStoreTests(unittest.TestCase):
    def test_v1_and_v2_started_event_golden_digests_are_frozen(self) -> None:
        cases = (
            (
                _fixture,
                "842b8d3d4492383b0ef33416d051c37caa71986d496d3c7723172116c7a0ddbd",
                "4641cb4643f76120522fcf27fae05b6d3dcdf7125fe506ffc41b4763a3f402e8",
            ),
            (
                _fixture_v2,
                "2c9a2d1bdc95d237b2ae1abf18ac9cad0f08c3b1f1699b240c351daf76dee456",
                "00bb0dadd429c8ede83e474bda8b7ef2ff00f873805a65d4c051ffd92f2b96cc",
            ),
        )
        for fixture, expected_event, expected_result in cases:
            with self.subTest(fixture=fixture.__name__), tempfile.TemporaryDirectory() as directory:
                *_unused, envelope = fixture()
                root = Path(directory)
                os.chmod(root, 0o700)
                with SQLiteGateCCheckpointLedger(
                    root / "checkpoints.sqlite3", binding=_binding(envelope)
                ) as ledger:
                    state = ledger.start(
                        envelope, recorded_at_utc="2026-08-23T00:00:00Z"
                    )
                self.assertEqual(state.started_event_sha256, expected_event)
                self.assertEqual(
                    _result(envelope)["checkpoint_result_sha256"], expected_result
                )

    def test_started_and_completed_survive_reopen(self) -> None:
        *_unused, envelope = _fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "checkpoints.sqlite3"
            with SQLiteGateCCheckpointLedger(path, binding=_binding(envelope)) as ledger:
                started = ledger.start(envelope, recorded_at_utc="2026-08-23T00:00:00Z")
                self.assertIsNone(started.completed_event_sha256)
            with SQLiteGateCCheckpointLedger(path, binding=_binding(envelope)) as ledger:
                state = ledger.state(envelope)
                self.assertIsNotNone(state)
                self.assertIsNone(state.completed_event_sha256)
                completed = ledger.complete(
                    envelope,
                    checkpoint_result=_result(envelope),
                    recorded_at_utc="2026-08-23T00:01:00Z",
                )
                self.assertIsNotNone(completed.completed_event_sha256)
            with SQLiteGateCCheckpointLedger(path, binding=_binding(envelope)) as ledger:
                final = ledger.state(envelope)
                self.assertEqual(final.checkpoint_result, _result(envelope))

    def test_v2_events_bind_execution_revision_and_survive_reopen(self) -> None:
        *_unused, envelope = _fixture_v2()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "checkpoints.sqlite3"
            with SQLiteGateCCheckpointLedger(path, binding=_binding(envelope)) as ledger:
                ledger.start(envelope, recorded_at_utc="2026-08-25T00:00:00Z")
                ledger.complete(
                    envelope,
                    checkpoint_result=_result(envelope),
                    recorded_at_utc="2026-08-25T00:01:00Z",
                )
                rows = ledger._db.execute(
                    "SELECT document FROM gate_c_checkpoint_events ORDER BY event_sequence"
                ).fetchall()
                for row in rows:
                    payload = json.loads(bytes(row[0]).decode())["event_payload"]
                    self.assertEqual(
                        payload["schema_version"],
                        "exp012-scale-gate-c-checkpoint-event-v2",
                    )
                    self.assertEqual(
                        payload["execution_source_revision"], "b" * 40
                    )
            with SQLiteGateCCheckpointLedger(path, binding=_binding(envelope)) as ledger:
                state = ledger.state(envelope)
                self.assertEqual(state.checkpoint_result, _result(envelope))
                self.assertEqual(
                    state.checkpoint_result["checkpoint_result_payload"][
                        "execution_source_revision"
                    ],
                    "b" * 40,
                )

    def test_ledger_refuses_mixed_v1_v2_event_chain_before_write(self) -> None:
        *_unused, legacy = _fixture()
        *_unused, current = _fixture_v2()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "checkpoints.sqlite3"
            with SQLiteGateCCheckpointLedger(path, binding=_binding(legacy)) as ledger:
                ledger.start(legacy, recorded_at_utc="2026-08-23T00:00:00Z")
                ledger.complete(
                    legacy,
                    checkpoint_result=_result(legacy),
                    recorded_at_utc="2026-08-23T00:01:00Z",
                )
                with self.assertRaises(GateCCheckpointLedgerError) as raised:
                    ledger.start(current, recorded_at_utc="2026-08-25T00:00:00Z")
                self.assertEqual(
                    raised.exception.code,
                    "GATE_C_CHECKPOINT_EVENT_VERSION_MIXED",
                )
                self.assertEqual(
                    ledger._db.execute(
                        "SELECT COUNT(*) FROM gate_c_checkpoint_events"
                    ).fetchone(),
                    (2,),
                )

    def test_v2_result_execution_revision_tamper_refuses_before_append(self) -> None:
        *_unused, envelope = _fixture_v2()
        tampered = _result(envelope)
        tampered["checkpoint_result_payload"]["execution_source_revision"] = "c" * 40
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "checkpoints.sqlite3"
            with SQLiteGateCCheckpointLedger(path, binding=_binding(envelope)) as ledger:
                ledger.start(envelope, recorded_at_utc="2026-08-25T00:00:00Z")
                with self.assertRaises(GateCBoundedExecutionError):
                    ledger.complete(
                        envelope,
                        checkpoint_result=tampered,
                        recorded_at_utc="2026-08-25T00:01:00Z",
                    )
                self.assertEqual(
                    ledger._db.execute(
                        "SELECT COUNT(*) FROM gate_c_checkpoint_events"
                    ).fetchone(),
                    (1,),
                )

    def test_forged_oversized_envelope_refuses_before_ledger_materialization(self) -> None:
        *_unused, envelope = _fixture()
        forged = object.__new__(type(envelope))
        for field in fields(type(envelope)):
            object.__setattr__(forged, field.name, getattr(envelope, field.name))
        object.__setattr__(
            forged,
            "execution_bound",
            GateCWindowExecutionBound(
                0, envelope.scale_contract.expected_windows + 1
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "checkpoints.sqlite3"
            with SQLiteGateCCheckpointLedger(
                path, binding=_binding(envelope)
            ) as ledger, mock.patch.object(
                GateCWindowExecutionBound,
                "allowed_window_sequences",
                new_callable=mock.PropertyMock,
                side_effect=AssertionError("ledger materialized invalid range"),
            ) as allowed, self.assertRaises(GateCBoundedExecutionError):
                ledger.state(forged)
            allowed.assert_not_called()

    def test_start_is_idempotent_but_completion_is_one_shot(self) -> None:
        *_unused, envelope = _fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "checkpoints.sqlite3"
            with SQLiteGateCCheckpointLedger(path, binding=_binding(envelope)) as ledger:
                first = ledger.start(envelope, recorded_at_utc="2026-08-23T00:00:00Z")
                second = ledger.start(envelope, recorded_at_utc="different-metadata")
                self.assertEqual(first, second)
                ledger.complete(
                    envelope, checkpoint_result=_result(envelope),
                    recorded_at_utc="2026-08-23T00:01:00Z",
                )
                with self.assertRaises(GateCCheckpointLedgerError):
                    ledger.complete(
                        envelope, checkpoint_result=_result(envelope),
                        recorded_at_utc="2026-08-23T00:02:00Z",
                    )

    def test_different_start_refuses_while_another_checkpoint_is_unfinished(self) -> None:
        *_unused, first = _fixture(count=2)
        *_unused, conflicting = _fixture(count=1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "checkpoints.sqlite3"
            with SQLiteGateCCheckpointLedger(path, binding=_binding(first)) as ledger:
                ledger.start(first, recorded_at_utc="2026-08-23T00:00:00Z")
                with self.assertRaises(GateCCheckpointLedgerError) as raised:
                    ledger.start(
                        conflicting,
                        recorded_at_utc="2026-08-23T00:00:01Z",
                    )
                self.assertEqual(raised.exception.code, "GATE_C_CHECKPOINT_CONFLICT")
                self.assertIsNotNone(ledger.state(first))
                self.assertIsNone(ledger.state(conflicting))

    def test_binding_substitution_and_concurrent_owner_refuse(self) -> None:
        *_unused, envelope = _fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "checkpoints.sqlite3"
            ledger = SQLiteGateCCheckpointLedger(path, binding=_binding(envelope))
            try:
                with self.assertRaises(GateCCheckpointLedgerError):
                    SQLiteGateCCheckpointLedger(path, binding=_binding(envelope))
            finally:
                ledger.close()
            changed = GateCCheckpointLedgerBinding(
                campaign_identity=envelope.campaign_identity,
                campaign_binding_sha256="f" * 64,
                scale_contract_sha256=envelope.scale_contract.contract_sha256,
                source_revision=envelope.source_revision,
            )
            with self.assertRaises(GateCCheckpointLedgerError):
                SQLiteGateCCheckpointLedger(path, binding=changed)

    def test_event_tamper_and_update_delete_fail_closed(self) -> None:
        *_unused, envelope = _fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "checkpoints.sqlite3"
            with SQLiteGateCCheckpointLedger(path, binding=_binding(envelope)) as ledger:
                ledger.start(envelope, recorded_at_utc="2026-08-23T00:00:00Z")
                db = ledger._db
                for command in (
                    "UPDATE gate_c_checkpoint_events SET event_kind='CHECKPOINT_COMPLETED' WHERE event_sequence=0",
                    "DELETE FROM gate_c_checkpoint_events WHERE event_sequence=0",
                ):
                    with self.assertRaises(sqlite3.IntegrityError):
                        db.execute(command)
            connection = sqlite3.connect(path)
            try:
                row = connection.execute(
                    "SELECT document FROM gate_c_checkpoint_events WHERE event_sequence=0"
                ).fetchone()
                document = json.loads(bytes(row[0]).decode())
                document["event_payload"]["recorded_at_utc"] = "tampered"
                connection.execute(
                    "DROP TRIGGER gate_c_checkpoint_events_no_update"
                )
                connection.execute(
                    "UPDATE gate_c_checkpoint_events SET document=? WHERE event_sequence=0",
                    (strict_canonical_json_bytes(document),),
                )
                connection.commit()
            finally:
                connection.close()
            with self.assertRaises(GateCCheckpointLedgerError):
                SQLiteGateCCheckpointLedger(path, binding=_binding(envelope))

    def test_checkpoint_store_has_no_candidate_authority_vocabulary(self) -> None:
        path = Path(__file__).parents[1] / "src" / "vdbench" / "gate_c_checkpoint_store.py"
        source = path.read_text(encoding="utf-8")
        for forbidden in (
            "policy", "admission", "grant", "route", "actuation", "pymilvus"
        ):
            self.assertNotIn(f"from .{forbidden}", source)

    def test_symlink_or_unsafe_parent_refuses(self) -> None:
        *_unused, envelope = _fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            target = root / "target.sqlite3"
            target.write_bytes(b"")
            os.chmod(target, 0o600)
            link = root / "checkpoints.sqlite3"
            link.symlink_to(target)
            with self.assertRaises(GateCCheckpointLedgerError):
                SQLiteGateCCheckpointLedger(link, binding=_binding(envelope))
            link.unlink()
            os.chmod(root, 0o777)
            with self.assertRaises(GateCCheckpointLedgerError):
                SQLiteGateCCheckpointLedger(link, binding=_binding(envelope))

    def test_forked_instance_cannot_read_or_append(self) -> None:
        *_unused, envelope = _fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            with SQLiteGateCCheckpointLedger(
                root / "checkpoints.sqlite3", binding=_binding(envelope)
            ) as ledger, mock.patch(
                "vdbench.gate_c_checkpoint_store.os.getpid",
                return_value=ledger._owner_pid + 1,
            ):
                with self.assertRaises(GateCCheckpointLedgerError) as raised:
                    ledger.state(envelope)
                self.assertEqual(raised.exception.code, "GATE_C_CHECKPOINT_LEDGER_FORKED")

    def test_forked_close_never_unlocks_parent_ownership(self) -> None:
        *_unused, envelope = _fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            ledger = SQLiteGateCCheckpointLedger(
                root / "checkpoints.sqlite3", binding=_binding(envelope)
            )
            with mock.patch(
                "vdbench.gate_c_checkpoint_store.os.getpid",
                return_value=ledger._owner_pid + 1,
            ), mock.patch(
                "vdbench.gate_c_checkpoint_store.fcntl.flock"
            ) as flock:
                ledger.close()
            flock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
