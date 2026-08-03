"""TDD coverage for the ADR-006 durable shadow-trace event source."""

from __future__ import annotations

import ast
from dataclasses import replace
import inspect
import json
from pathlib import Path
import tempfile
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch

from vdbench.config import IndexTrack, Metric
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from vdbench.oracle import OracleHit, OracleResult
from vdbench.policy import PreActionSafety, QualificationResult
from vdbench.shadow_artifacts import load_persisted_shadow_trace_envelope
from vdbench.workload_monitor import (
    DryRunPolicyInputs,
    FileMonitorStateStore,
    MonitorAuditRecord,
    MonitorRecordStatus,
    MonitorStreamKey,
    WorkloadMonitor,
)

from vdbench.shadow_event_source import (
    FileShadowTraceEventSource,
    PublicationStatus,
    ShadowEventSourceError,
    TracePublicationContext,
)


def _identity(track: IndexTrack, metric: Metric) -> ShadowIdentityEvidence:
    identity = CollectionIdentity(
        collection_name=f"source_{metric.value.lower()}_{track.value.lower()}",
        metric=metric.value,
        index_track=track.value,
        description={"index_type": track.value, "metric_type": metric.value},
    )
    stage = ShadowAuditStageEvidence(stage=f"{track.value}_IDENTITY", success=True)
    return ShadowIdentityEvidence(
        track=track,
        expected_binding_id=f"{metric.value.lower()}-{track.value.lower()}-binding-v1",
        pre_snapshot=identity,
        post_snapshot=identity,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=stage,
        post_capture=stage,
    )


def _trace(*, metric: Metric = Metric.L2, trace_offset: int = 0) -> ShadowAuditTrace:
    score, radius, range_filter = (1.0, 2.0, 0.0) if metric is Metric.L2 else (0.5, 0.0, 1.0)
    queries: list[ShadowQueryAuditTrace] = []
    for index in range(50):
        query_id = trace_offset + index
        oracle = OracleResult(
            hits=(OracleHit(id=query_id, score=score),), full_count=1, capped=False
        )
        hit = SearchHit(id=query_id, score=score)
        queries.append(
            ShadowQueryAuditTrace(
                query_id=query_id,
                query_vector=(98765.25, float(query_id + 1)),
                threshold_radius=radius,
                range_filter=range_filter,
                limit=100,
                oracle_result=oracle,
                exact_cardinality=1,
                flat_hits=(hit,),
                sentinel_hits=(hit,),
                sentinel_recall=1.0,
                stages=(
                    ShadowAuditStageEvidence("ORACLE", success=True),
                    ShadowAuditStageEvidence("FLAT", success=True, oracle_agreement=True),
                    ShadowAuditStageEvidence("SENTINEL_HNSW", success=True),
                ),
            )
        )
    return ShadowAuditTrace(
        metric=metric,
        threshold_stratum="target-075",
        candidate_ef=400,
        last_known_good_ef=200,
        sentinel_ef=100,
        configuration_identity="config-v1",
        data_identity="dataset-v1",
        flat_identity=_identity(IndexTrack.FLAT, metric),
        hnsw_identity=_identity(IndexTrack.HNSW, metric),
        queries=tuple(queries),
        complete=True,
    )


def _stream_key(metric: Metric = Metric.L2) -> MonitorStreamKey:
    return MonitorStreamKey(
        stream_id=f"source-{metric.value.lower()}-v1",
        metric=metric,
        threshold_stratum="target-075",
        configuration_identity="config-v1",
        data_identity="dataset-v1",
        flat_binding_id=f"{metric.value.lower()}-flat-binding-v1",
        hnsw_binding_id=f"{metric.value.lower()}-hnsw-binding-v1",
    )


def _context(
    *,
    metric: Metric = Metric.L2,
    window_id: int = 0,
    window_sequence: int = 0,
    trace_sequence_index: int = 0,
    trace_id: str = "trace-0",
) -> TracePublicationContext:
    return TracePublicationContext(
        stream_key=_stream_key(metric),
        window_id=window_id,
        window_sequence=window_sequence,
        trace_sequence_index=trace_sequence_index,
        trace_id=trace_id,
        captured_at_utc=f"2026-08-03T12:00:{window_sequence * 4 + trace_sequence_index:02d}Z",
    )


class ShadowEventSourceTests(unittest.TestCase):
    def test_persist_before_publish_and_data_minimizing_event(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = FileShadowTraceEventSource(Path(temporary) / "outbox", max_pending_events=4, max_pending_bytes=4096)
            receipt = source.publish(trace=_trace(), context=_context())

            self.assertEqual(receipt.status, PublicationStatus.PUBLISHED)
            self.assertIsNotNone(receipt.event)
            assert receipt.event is not None
            self.assertEqual(source.poll(limit=1), (receipt.event,))
            envelope = load_persisted_shadow_trace_envelope(receipt.event.envelope_path)
            self.assertEqual(envelope.trace_id, "trace-0")
            self.assertEqual(envelope.expected_trace_sha256, receipt.event.expected_trace_sha256)

            pending_payload = next((Path(temporary) / "outbox" / "pending").glob("*.json")).read_text(encoding="utf-8")
            self.assertNotIn("98765.25", pending_payload)
            self.assertNotIn("threshold_radius", pending_payload)
            self.assertNotIn("oracle_result", pending_payload)
            self.assertIn("98765.25", receipt.event.envelope_path.read_text(encoding="utf-8"))

    def test_idempotency_conflict_and_restart_safe_acknowledgement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outbox"
            source = FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)
            first = source.publish(trace=_trace(), context=_context())
            duplicate = source.publish(trace=_trace(), context=_context())
            self.assertEqual(duplicate.status, PublicationStatus.IDEMPOTENT)
            self.assertEqual(first.event, duplicate.event)

            with self.assertRaisesRegex(ShadowEventSourceError, "PUBLICATION_CONFLICT"):
                source.publish(trace=_trace(trace_offset=1000), context=_context())

            assert first.event is not None
            restarted = FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)
            self.assertEqual(restarted.poll(limit=4), (first.event,))
            restarted.acknowledge((first.event.event_id,))
            restarted.acknowledge((first.event.event_id,))
            self.assertEqual(restarted.poll(limit=4), ())
            self.assertEqual(len(list((root / "acknowledged").glob("*.json"))), 1)

    def test_pending_events_are_deterministically_ordered_per_stream(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = FileShadowTraceEventSource(Path(temporary) / "outbox", max_pending_events=8, max_pending_bytes=8192)
            second = source.publish(
                trace=_trace(trace_offset=50),
                context=_context(window_id=1, window_sequence=1, trace_id="trace-1"),
            )
            first = source.publish(trace=_trace(), context=_context())
            self.assertEqual(
                tuple(event.window_sequence for event in source.poll(limit=4)),
                (0, 1),
            )
            self.assertIsNotNone(first.event)
            self.assertIsNotNone(second.event)

    def test_backpressure_rejects_before_trace_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outbox"
            source = FileShadowTraceEventSource(root, max_pending_events=1, max_pending_bytes=4096)
            self.assertEqual(source.publish(trace=_trace(), context=_context()).status, PublicationStatus.PUBLISHED)
            dropped = source.publish(
                trace=_trace(trace_offset=50),
                context=_context(window_id=1, window_sequence=1, trace_id="trace-1"),
            )
            self.assertEqual(dropped.status, PublicationStatus.DROPPED_BACKPRESSURE)
            self.assertEqual(dropped.reason_code, "PENDING_EVENT_CAPACITY_EXCEEDED")
            self.assertEqual(len(list((root / "traces").glob("*.json"))), 1)
            self.assertEqual(len(source.poll(limit=4)), 1)

            byte_limited_root = Path(temporary) / "byte-limited"
            byte_limited = FileShadowTraceEventSource(byte_limited_root, max_pending_events=4, max_pending_bytes=1)
            self.assertEqual(
                byte_limited.publish(trace=_trace(), context=_context()).status,
                PublicationStatus.DROPPED_BACKPRESSURE,
            )
            self.assertEqual(len(list((byte_limited_root / "traces").glob("*.json"))), 0)

    def test_corrupt_pending_event_is_quarantined_without_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outbox"
            source = FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)
            receipt = source.publish(trace=_trace(), context=_context())
            assert receipt.event is not None
            pending = root / "pending" / f"{receipt.event.event_id}.json"
            pending.write_text("{ malformed", encoding="utf-8")
            self.assertEqual(source.poll(limit=4), ())
            self.assertFalse(pending.exists())
            self.assertTrue((root / "rejected" / f"{receipt.event.event_id}.json").exists())
            self.assertIn("EVENT_MALFORMED", source.rejected_reason_codes())

    def test_duplicate_json_field_and_tampered_envelope_are_quarantined(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outbox"
            source = FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)
            first = source.publish(trace=_trace(), context=_context())
            second = source.publish(
                trace=_trace(trace_offset=50),
                context=_context(window_id=1, window_sequence=1, trace_id="trace-1"),
            )
            assert first.event is not None and second.event is not None
            first_pending = root / "pending" / f"{first.event.event_id}.json"
            first_pending.write_text(
                first_pending.read_text(encoding="utf-8").replace(
                    '"event_id":', '"event_id":"forged","event_id":', 1
                ),
                encoding="utf-8",
            )
            second.event.envelope_path.write_text("{ malformed", encoding="utf-8")
            self.assertEqual(source.poll(limit=4), ())
            reasons = source.rejected_reason_codes()
            self.assertIn("EVENT_MALFORMED", reasons)
            self.assertIn("ENVELOPE_INVALID", reasons)

    def test_same_window_slot_cannot_be_reused_by_a_different_trace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = FileShadowTraceEventSource(Path(temporary) / "outbox", max_pending_events=4, max_pending_bytes=4096)
            source.publish(trace=_trace(), context=_context())
            with self.assertRaisesRegex(ShadowEventSourceError, "WINDOW_SLOT_CONFLICT"):
                source.publish(
                    trace=_trace(trace_offset=500),
                    context=_context(trace_id="different-trace"),
                )

    def test_acknowledgement_rejects_a_tampered_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outbox"
            source = FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)
            receipt = source.publish(trace=_trace(), context=_context())
            assert receipt.event is not None
            receipt.event.envelope_path.write_text("{ malformed", encoding="utf-8")
            with self.assertRaisesRegex(ShadowEventSourceError, "ENVELOPE_INVALID"):
                source.acknowledge((receipt.event.event_id,))
            self.assertTrue((root / "pending" / f"{receipt.event.event_id}.json").exists())

    def test_incomplete_trace_and_unsafe_outbox_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            unsafe = Path(temporary) / "unsafe"
            unsafe.mkdir(mode=0o755)
            unsafe.chmod(0o755)
            with self.assertRaisesRegex(ShadowEventSourceError, "OUTBOX_UNSAFE_PERMISSIONS"):
                FileShadowTraceEventSource(unsafe, max_pending_events=1, max_pending_bytes=1024)

            source = FileShadowTraceEventSource(Path(temporary) / "safe", max_pending_events=4, max_pending_bytes=4096)
            with self.assertRaisesRegex(ShadowEventSourceError, "TRACE_INCOMPLETE"):
                source.publish(trace=replace(_trace(), complete=False), context=_context())

    def test_symlinked_pending_event_or_envelope_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outbox"
            source = FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)
            receipt = source.publish(trace=_trace(), context=_context())
            assert receipt.event is not None
            pending = root / "pending" / f"{receipt.event.event_id}.json"
            replacement = root / "replacement.json"
            replacement.write_text(pending.read_text(encoding="utf-8"), encoding="utf-8")
            pending.unlink()
            pending.symlink_to(replacement)
            self.assertEqual(source.poll(limit=4), ())
            self.assertIn("EVENT_SYMLINK_REJECTED", source.rejected_reason_codes())

    def test_permission_drift_after_construction_blocks_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outbox"
            source = FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)
            source.publish(trace=_trace(), context=_context())
            root.chmod(0o755)
            with self.assertRaisesRegex(ShadowEventSourceError, "OUTBOX_UNSAFE_PERMISSIONS"):
                source.poll(limit=4)

    def test_concurrent_publishers_preserve_both_atomic_events(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outbox"
            FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)

            def publish(window_sequence: int) -> PublicationStatus:
                source = FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)
                return source.publish(
                    trace=_trace(trace_offset=window_sequence * 50),
                    context=_context(
                        window_id=window_sequence,
                        window_sequence=window_sequence,
                        trace_id=f"concurrent-{window_sequence}",
                    ),
                ).status

            with ThreadPoolExecutor(max_workers=2) as executor:
                statuses = tuple(executor.map(publish, (0, 1)))
            self.assertEqual(statuses, (PublicationStatus.PUBLISHED, PublicationStatus.PUBLISHED))
            source = FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)
            self.assertEqual(tuple(event.window_sequence for event in source.poll(limit=4)), (0, 1))

    def test_post_envelope_publication_failure_leaves_only_an_orphan(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outbox"
            source = FileShadowTraceEventSource(root, max_pending_events=4, max_pending_bytes=4096)
            with patch.object(source, "_publish_event_document", side_effect=OSError("synthetic failure")):
                with self.assertRaisesRegex(OSError, "synthetic failure"):
                    source.publish(trace=_trace(), context=_context())
            self.assertEqual(source.poll(limit=4), ())
            self.assertEqual(len(source.orphaned_trace_paths()), 1)

    def test_source_events_drive_the_real_dry_run_monitor(self) -> None:
        class Audit:
            def __init__(self) -> None:
                self.records: list[MonitorAuditRecord] = []

            def contains(self, record_id: str) -> bool:
                return any(record.record_id == record_id for record in self.records)

            def append(self, record: MonitorAuditRecord) -> None:
                if self.contains(record.record_id):
                    raise AssertionError("duplicate monitor audit record")
                self.records.append(record)

        class Provider:
            def resolve(self, *, decision, provenance):
                return DryRunPolicyInputs(
                    current_ef=400,
                    response_estimates={},
                    pre_action=PreActionSafety(
                        metric=provenance.metric,
                        threshold_stratum=provenance.threshold_stratum,
                        configuration_identity=provenance.configuration_identity,
                        index_identity=provenance.hnsw_binding_id,
                        flat_index_identity=provenance.flat_binding_id,
                        data_identity=provenance.data_identity,
                        response_model_provenance="offline-source-test",
                    ),
                    last_known_good=QualificationResult(
                        qualified=False,
                        ef=None,
                        reasons=("EXP007_OFFLINE_ONLY",),
                    ),
                    audit_id=f"exp007-audit:{provenance.current_window_id}",
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outbox"
            source = FileShadowTraceEventSource(root, max_pending_events=16, max_pending_bytes=16384)
            for window_sequence in range(3):
                for trace_sequence_index in range(4):
                    receipt = source.publish(
                        trace=_trace(trace_offset=trace_sequence_index * 50),
                        context=_context(
                            window_id=window_sequence,
                            window_sequence=window_sequence,
                            trace_sequence_index=trace_sequence_index,
                            trace_id=f"window-{window_sequence}-trace-{trace_sequence_index}",
                        ),
                    )
                    self.assertEqual(receipt.status, PublicationStatus.PUBLISHED)

            audit = Audit()
            monitor = WorkloadMonitor(
                source=source,
                state_store=FileMonitorStateStore(Path(temporary) / "monitor-state"),
                policy_input_provider=Provider(),
                audit_sink=audit,
                detector_seed=20260804,
            )
            results = monitor.run_once(max_events=12)
            self.assertEqual(len(results), 12)
            self.assertEqual(source.poll(limit=12), ())
            evaluated = [record for record in audit.records if record.status is MonitorRecordStatus.EVALUATED]
            self.assertEqual(len(evaluated), 1)
            self.assertEqual(evaluated[0].detector_state, "NO_DRIFT")
            self.assertEqual(evaluated[0].policy_action, "NO_CHANGE")

    def test_module_has_no_policy_actuation_or_live_database_dependency(self) -> None:
        import vdbench.shadow_event_source as shadow_event_source

        tree = ast.parse(inspect.getsource(shadow_event_source))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }
        forbidden = {"pymilvus", "vdbench.policy", "vdbench.actuation", "vdbench.milvus_actuation"}
        self.assertTrue(forbidden.isdisjoint(imported), imported)
        self.assertNotIn("WorkloadMonitor", inspect.getsource(shadow_event_source))


if __name__ == "__main__":
    unittest.main()
