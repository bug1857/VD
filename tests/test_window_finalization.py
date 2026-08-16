"""Adversarial tests for ADR-015 cross-store finalization coordination.

All evidence is produced by injected offline fakes.  These tests exercise only
the reconciliation journal's canonical lifecycle; they contact no service and
create no detector, policy, grant, routing, or actuation authority.
"""

from __future__ import annotations

import ast
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tests.test_exp010_live_runner import _Harness
from vdbench import window_finalization
from vdbench.host_window_detector_v2 import HostWindowV2Status
from vdbench.window_finalization import (
    SQLiteWindowFinalizationStore,
    WindowFinalizationError,
    WindowFinalizationPhase,
    build_prepared_window_finalization,
)

MODULE_PATH = (
    Path(__file__).parents[1] / "src" / "vdbench" / "window_finalization.py"
)


def _prepared(root: Path):
    harness = _Harness(root / "capture")
    try:
        harness.serve_many(200)
        sources = harness.runner.composition.response_store.load_window(0)
        assert sources is not None
        bundle = harness.runner.composition.shadow_worker.build(tuple(sources))
        attempts = harness.runner.composition.shadow_attempt_store.records_for_window(0)
        prepared = build_prepared_window_finalization(
            bundle=bundle,
            attempts=attempts,
            source_revision=harness.runner.composition.source_revision,
            environment_manifest_sha256=(
                harness.runner.composition.environment_manifest_sha256
            ),
            expected_detector_status=HostWindowV2Status.REBASELINE,
        )
        return (
            prepared,
            harness.runner.composition.stream_key,
            harness.runner.composition.source_revision,
            harness.runner.composition.environment_manifest_sha256,
        )
    finally:
        harness.close()


def _store(path: Path, identity):
    _prepared_value, stream, revision, environment = identity
    return SQLiteWindowFinalizationStore(
        path,
        stream_key=stream,
        source_revision=revision,
        environment_manifest_sha256=environment,
    )


class WindowFinalizationStoreTests(unittest.TestCase):
    def test_append_only_lifecycle_reconstructs_identically_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = _prepared(root)
            prepared = identity[0]
            path = root / "finalization.sqlite3"
            with _store(path, identity) as store:
                self.assertIs(store.prepare(
                    prepared, recorded_at_utc="2026-08-14T00:00:01Z"
                ).phase, WindowFinalizationPhase.PREPARED)
                self.assertIs(store.record_detector(
                    detector_event_sha256="a" * 64,
                    detector_head_sha256=None,
                    detector_status=HostWindowV2Status.REBASELINE,
                    recorded_at_utc="2026-08-14T00:00:02Z",
                ).phase, WindowFinalizationPhase.DETECTOR_COMMITTED)
                self.assertIs(store.record_attestation_not_required(
                    recorded_at_utc="2026-08-14T00:00:03Z"
                ).phase, WindowFinalizationPhase.ATTESTATION_NOT_REQUIRED)
                self.assertIs(store.record_acknowledged(
                    acknowledgement_head_sha256="b" * 64,
                    acknowledged_count=200,
                    recorded_at_utc="2026-08-14T00:00:04Z",
                ).phase, WindowFinalizationPhase.SOURCE_ACKNOWLEDGED)
                finalized = store.finalize(
                    recorded_at_utc="2026-08-14T00:00:05Z"
                )
                self.assertIs(finalized.phase, WindowFinalizationPhase.FINALIZED)
                self.assertEqual(store.next_window_sequence(), 1)
                expected = store.states()
            with _store(path, identity) as reopened:
                self.assertEqual(reopened.states(), expected)
                self.assertIsNone(reopened.pending())
                self.assertEqual(reopened.next_window_sequence(), 1)

    def test_illegal_transitions_and_status_head_contradictions_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = _prepared(root)
            with _store(root / "finalization.sqlite3", identity) as store:
                store.prepare(
                    identity[0], recorded_at_utc="2026-08-14T00:00:01Z"
                )
                with self.assertRaises(WindowFinalizationError) as early:
                    store.record_acknowledged(
                        acknowledgement_head_sha256="b" * 64,
                        acknowledged_count=200,
                        recorded_at_utc="2026-08-14T00:00:02Z",
                    )
                self.assertEqual(
                    early.exception.code, "WINDOW_FINALIZATION_PENDING_MISSING"
                )
                with self.assertRaises(WindowFinalizationError) as contradictory:
                    store.record_detector(
                        detector_event_sha256="a" * 64,
                        detector_head_sha256="c" * 64,
                        detector_status=HostWindowV2Status.REBASELINE,
                        recorded_at_utc="2026-08-14T00:00:03Z",
                    )
                self.assertEqual(
                    contradictory.exception.code,
                    "WINDOW_FINALIZATION_EVENT_INVALID",
                )
                self.assertIs(
                    store.pending().phase, WindowFinalizationPhase.PREPARED
                )

    def test_raw_update_delete_and_canonical_row_tamper_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = _prepared(root)
            path = root / "finalization.sqlite3"
            with _store(path, identity) as store:
                store.prepare(
                    identity[0], recorded_at_utc="2026-08-14T00:00:01Z"
                )
                with self.assertRaises(sqlite3.DatabaseError):
                    store._db.execute(
                        "UPDATE finalization_events SET phase='FINALIZED'"
                    )
                with self.assertRaises(sqlite3.DatabaseError):
                    store._db.execute(
                        "DELETE FROM finalization_events"
                    )
            connection = sqlite3.connect(path)
            trigger_sql = connection.execute(
                "SELECT sql FROM sqlite_schema "
                "WHERE name='finalization_events_no_update'"
            ).fetchone()[0]
            connection.execute("DROP TRIGGER finalization_events_no_update")
            connection.execute(
                "UPDATE finalization_events SET event_json=? WHERE event_sequence=0",
                (b"{}",),
            )
            connection.execute(trigger_sql)
            connection.commit()
            connection.close()
            with self.assertRaises(WindowFinalizationError) as tampered:
                _store(path, identity)
            self.assertEqual(
                tampered.exception.code, "WINDOW_FINALIZATION_EVENT_INVALID"
            )

    def test_forged_noncanonical_prepared_value_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = _prepared(root)
            prepared = identity[0]
            object.__setattr__(
                prepared,
                "source_sequences",
                (False, *prepared.source_sequences[1:]),
            )
            forged_payload = window_finalization._prepared_payload(prepared)
            object.__setattr__(
                prepared,
                "prepared_sha256",
                window_finalization._digest(
                    window_finalization._PREPARED_DOMAIN, forged_payload
                ),
            )
            with _store(root / "finalization.sqlite3", identity) as store:
                with self.assertRaises(WindowFinalizationError) as raised:
                    store.prepare(
                        prepared, recorded_at_utc="2026-08-14T00:00:01Z"
                    )
                self.assertEqual(
                    raised.exception.code, "WINDOW_FINALIZATION_PREPARED_INVALID"
                )

    def test_module_has_no_service_or_candidate_authority_dependency(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
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
        forbidden = {
            "pymilvus",
            "policy",
            "actuation",
            "canary_admission",
            "canary_activation",
            "canary_grant_store",
            "canary_routing",
        }
        offending = {
            item
            for item in imported
            if any(item == name or item.endswith(f".{name}") for name in forbidden)
        }
        self.assertFalse(offending, offending)


if __name__ == "__main__":
    unittest.main()
