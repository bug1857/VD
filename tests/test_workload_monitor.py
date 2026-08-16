"""TDD coverage for the ADR-005 DRY_RUN workload-monitor boundary."""

from __future__ import annotations

import ast
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from vdbench.actuation import (
    ActuationIdentityContext,
    ActuationOutcome,
    SafeActuationBoundary,
)
from vdbench.config import IndexTrack, Metric
from vdbench.drift import (
    EvidenceProvenance,
    Signal,
    SignalEvidence,
    WindowEvidence,
    build_evidence_provenance,
    finalize_window_evidence,
)
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from vdbench.oracle import OracleHit, OracleResult
from vdbench.policy import (
    PolicyAction,
    PolicyMode,
    PreActionSafety,
    QualificationResult,
)
from vdbench.shadow_artifacts import persist_shadow_trace_envelope
from vdbench.shadow_window import PersistedShadowTraceEnvelope, hash_shadow_audit_trace
from vdbench.workload_monitor import (
    DryRunPolicyInputs,
    FileMonitorStateStore,
    MonitorAuditRecord,
    MonitorStreamKey,
    ShadowTraceEvent,
    WorkloadMonitor,
)

REPOSITORY = Path(__file__).parents[1]
MODULE_PATH = REPOSITORY / "src" / "vdbench" / "workload_monitor.py"
DETECTOR_SEED = 20260804


class FakeSource:
    """Deterministic, acknowledgement-recording event source."""

    def __init__(self, events: list[ShadowTraceEvent]) -> None:
        self.events = list(events)
        self.acknowledged: list[str] = []

    def poll(self, *, limit: int) -> tuple[ShadowTraceEvent, ...]:
        result = tuple(self.events[:limit])
        del self.events[:limit]
        return result

    def acknowledge(self, event_ids: tuple[str, ...]) -> None:
        self.acknowledged.extend(event_ids)


class FakeAuditSink:
    """Append-only, idempotency-aware audit sink for monitor tests."""

    def __init__(self) -> None:
        self.records: list[MonitorAuditRecord] = []

    def contains(self, record_id: str) -> bool:
        return any(record.record_id == record_id for record in self.records)

    def append(self, record: MonitorAuditRecord) -> None:
        if self.contains(record.record_id):
            raise AssertionError(f"duplicate audit record: {record.record_id}")
        self.records.append(record)


class FailOnceAuditSink(FakeAuditSink):
    """Makes a durable-outbox retry observable without a real filesystem sink."""

    def __init__(self) -> None:
        super().__init__()
        self._fail_next_append = True

    def append(self, record: MonitorAuditRecord) -> None:
        if self._fail_next_append:
            self._fail_next_append = False
            raise OSError("synthetic audit sink outage")
        super().append(record)


class RecordingPolicyInputProvider:
    """Supplies only externally owned DRY_RUN policy inputs."""

    def __init__(self) -> None:
        self.calls = []

    def resolve(self, *, decision, provenance: EvidenceProvenance) -> DryRunPolicyInputs:
        self.calls.append((decision, provenance))
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
                response_model_provenance="offline-monitor-test",
            ),
            last_known_good=QualificationResult(
                qualified=False,
                ef=None,
                reasons=("EXP006_OFFLINE_ONLY",),
            ),
            audit_id=f"external-audit:{provenance.current_window_id}",
        )


def _identity(track: IndexTrack, metric: Metric) -> ShadowIdentityEvidence:
    description: dict[str, object] = {
        "index_type": track.value,
        "metric_type": metric.value,
    }
    if track is IndexTrack.HNSW:
        description.update({"M": "16", "efConstruction": "200"})
    identity = CollectionIdentity(
        collection_name=f"monitor_{metric.value.lower()}_{track.value.lower()}",
        metric=metric.value,
        index_track=track.value,
        description=description,
    )
    capture = ShadowAuditStageEvidence(stage=f"{track.value}_IDENTITY", success=True)
    return ShadowIdentityEvidence(
        track=track,
        expected_binding_id=f"{metric.value.lower()}-{track.value.lower()}-binding-v1",
        pre_snapshot=identity,
        post_snapshot=identity,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=capture,
        post_capture=capture,
    )


def _query(query_id: int, metric: Metric) -> ShadowQueryAuditTrace:
    score, radius, range_filter = (
        (1.0, 2.0, 0.0) if metric is Metric.L2 else (0.5, 0.0, 1.0)
    )
    oracle = OracleResult((OracleHit(query_id, score),), full_count=1, capped=False)
    hit = SearchHit(query_id, score)
    return ShadowQueryAuditTrace(
        query_id=query_id,
        query_vector=(float(query_id + 1), 1.0),
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


def _stream_key(
    *, metric: Metric = Metric.L2, stream_id: str = "stream-a", configuration: str = "config-v1"
) -> MonitorStreamKey:
    return MonitorStreamKey(
        stream_id=stream_id,
        metric=metric,
        threshold_stratum="target-075",
        configuration_identity=configuration,
        data_identity="dataset-v1",
        flat_binding_id=f"{metric.value.lower()}-flat-binding-v1",
        hnsw_binding_id=f"{metric.value.lower()}-hnsw-binding-v1",
    )


def _persist_events(
    root: Path,
    *,
    stream_key: MonitorStreamKey,
    window_sequence: int,
) -> list[ShadowTraceEvent]:
    events: list[ShadowTraceEvent] = []
    for sequence_index in range(4):
        trace = ShadowAuditTrace(
            metric=stream_key.metric,
            threshold_stratum=stream_key.threshold_stratum,
            candidate_ef=400,
            last_known_good_ef=200,
            sentinel_ef=100,
            configuration_identity=stream_key.configuration_identity,
            data_identity=stream_key.data_identity,
            flat_identity=_identity(IndexTrack.FLAT, stream_key.metric),
            hnsw_identity=_identity(IndexTrack.HNSW, stream_key.metric),
            queries=tuple(
                _query(query_id, stream_key.metric)
                for query_id in range(sequence_index * 50, (sequence_index + 1) * 50)
            ),
            complete=True,
        )
        envelope = PersistedShadowTraceEnvelope(
            trace_id=f"{stream_key.stream_id}-{window_sequence}-trace-{sequence_index}",
            captured_at_utc=f"2026-08-03T12:00:{window_sequence * 4 + sequence_index:02d}Z",
            sequence_index=sequence_index,
            declared_observation_count=50,
            expected_trace_sha256=hash_shadow_audit_trace(trace),
            trace=trace,
        )
        path = root / f"{stream_key.stream_id}-{window_sequence}-{sequence_index}.json"
        persist_shadow_trace_envelope(path, envelope)
        events.append(
            ShadowTraceEvent(
                event_id=f"event:{stream_key.stream_id}:{window_sequence}:{sequence_index}",
                stream_key=stream_key,
                window_id=f"{stream_key.stream_id}:window:{window_sequence}",
                window_sequence=window_sequence,
                envelope_path=path,
                expected_trace_sha256=envelope.expected_trace_sha256,
            )
        )
    return events


def _fast_evidence(
    *, reference_window, current_window, metric, detector_seed
) -> WindowEvidence:
    del detector_seed
    trace = current_window.envelopes[0].trace
    assert trace is not None
    audit_ids = tuple(range(50))
    digests = tuple("0" * 64 for _ in audit_ids)
    provenance = build_evidence_provenance(
        metric=metric,
        threshold_stratum=current_window.threshold_stratum,
        reference_window_id=reference_window.window_id,
        current_window_id=current_window.window_id,
        reference_manifest_sha256=reference_window.manifest_sha256,
        current_manifest_sha256=current_window.manifest_sha256,
        configuration_identity=trace.configuration_identity,
        data_identity=trace.data_identity,
        flat_binding_id=trace.flat_identity.expected_binding_id,
        hnsw_binding_id=trace.hnsw_identity.expected_binding_id,
        reference_audit_ids=audit_ids,
        reference_audit_rank_digests=digests,
        current_audit_ids=audit_ids,
        current_audit_rank_digests=digests,
    )
    counts = {
        Signal.QUERY_VECTOR: 200,
        Signal.THRESHOLD: 200,
        Signal.CARDINALITY: 50,
        Signal.RECALL: 50,
    }
    floors = {
        Signal.QUERY_VECTOR: 0.01,
        Signal.THRESHOLD: 0.20,
        Signal.CARDINALITY: 0.20,
        Signal.RECALL: 0.02,
    }
    return finalize_window_evidence(
        metric=metric,
        window_id=current_window.window_id,
        signals=tuple(
            SignalEvidence(
                signal=signal,
                complete=True,
                reference_count=counts[signal],
                current_count=counts[signal],
                statistic=0.0,
                effect=0.0,
                effect_floor=floors[signal],
                raw_p_value=1.0,
            )
            for signal in Signal
        ),
        provenance=provenance,
    )


class WorkloadMonitorTests(unittest.TestCase):
    maxDiff = None

    def _monitor(self, root: Path, events: list[ShadowTraceEvent], *, store=None):
        source = FakeSource(events)
        state_store = store or FileMonitorStateStore(root / "monitor-state")
        sink = FakeAuditSink()
        provider = RecordingPolicyInputProvider()
        monitor = WorkloadMonitor(
            source=source,
            state_store=state_store,
            policy_input_provider=provider,
            audit_sink=sink,
            detector_seed=DETECTOR_SEED,
        )
        return monitor, source, state_store, sink, provider

    def test_valid_path_uses_real_assembly_extraction_detector_and_dry_run_policy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for metric in (Metric.L2, Metric.COSINE):
                with self.subTest(metric=metric):
                    key = _stream_key(metric=metric, stream_id=f"stream-{metric.value.lower()}")
                    events = [
                        *_persist_events(root, stream_key=key, window_sequence=0),
                        *_persist_events(root, stream_key=key, window_sequence=1),
                        *_persist_events(root, stream_key=key, window_sequence=2),
                    ]
                    monitor, source, _, sink, provider = self._monitor(root, events)

                    results = monitor.run_once(max_events=12)

                    evaluated = [result for result in results if result.policy_decision is not None]
                    self.assertEqual(len(evaluated), 1)
                    self.assertEqual(evaluated[0].drift_decision.state.value, "NO_DRIFT")
                    self.assertEqual(evaluated[0].policy_decision.action, PolicyAction.NO_CHANGE)
                    self.assertEqual(evaluated[0].policy_decision.mode, PolicyMode.DRY_RUN)
                    self.assertEqual(len(provider.calls), 1)
                    self.assertEqual(source.acknowledged, [event.event_id for event in events])
                    self.assertEqual(len({record.record_id for record in sink.records}), len(sink.records))

    def test_restart_recovery_matches_uninterrupted_replay_and_rebuilds_prior_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = _stream_key()
            events = [
                *_persist_events(root, stream_key=key, window_sequence=0),
                *_persist_events(root, stream_key=key, window_sequence=1),
                *_persist_events(root, stream_key=key, window_sequence=2),
            ]
            with patch("vdbench.workload_monitor.extract_window_evidence", side_effect=_fast_evidence):
                baseline, _, _, baseline_sink, _ = self._monitor(root, list(events))
                baseline_results = baseline.run_once(max_events=12)

                store = FileMonitorStateStore(root / "restart-state")
                first, _, _, restart_sink, _ = self._monitor(root, events[:7], store=store)
                first.run_once(max_events=7)
                restarted, _, _, _, restarted_provider = self._monitor(
                    root, events[7:], store=store
                )
                restarted.audit_sink = restart_sink
                resumed_results = restarted.run_once(max_events=5)

            baseline_eval = next(item for item in baseline_results if item.policy_decision)
            resumed_eval = next(item for item in resumed_results if item.policy_decision)
            self.assertEqual(baseline_eval.drift_decision, resumed_eval.drift_decision)
            self.assertEqual(baseline_eval.policy_decision, resumed_eval.policy_decision)
            self.assertEqual(len(restarted_provider.calls), 1)
            self.assertEqual(
                [record.record_id for record in baseline_sink.records],
                [record.record_id for record in restart_sink.records],
            )

    def test_restart_uses_checksum_bound_persisted_prior_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = _stream_key()
            events = [
                *_persist_events(root, stream_key=key, window_sequence=0),
                *_persist_events(root, stream_key=key, window_sequence=1),
                *_persist_events(root, stream_key=key, window_sequence=2),
            ]
            store = FileMonitorStateStore(root / "restart-state")
            with patch(
                "vdbench.workload_monitor.extract_window_evidence",
                side_effect=_fast_evidence,
            ) as extraction:
                first, _, _, sink, _ = self._monitor(root, events[:8], store=store)
                first.run_once(max_events=8)
                persisted = store.load(key)
                self.assertIsNotNone(persisted.previous_current_evidence)
                extraction.reset_mock()

                restarted, _, _, _, provider = self._monitor(root, events[8:], store=store)
                restarted.audit_sink = sink
                results = restarted.run_once(max_events=4)

            self.assertEqual(extraction.call_count, 1)
            self.assertEqual(len(provider.calls), 1)
            self.assertEqual(
                next(result for result in results if result.policy_decision).policy_decision.action,
                PolicyAction.NO_CHANGE,
            )

    def test_redelivery_drains_durable_outbox_before_duplicate_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = _stream_key()
            first_event = _persist_events(root, stream_key=key, window_sequence=0)[0]
            store = FileMonitorStateStore(root / "monitor-state")
            failing_sink = FailOnceAuditSink()
            monitor, _, _, _, _ = self._monitor(root, [first_event], store=store)
            monitor.audit_sink = failing_sink

            with self.assertRaisesRegex(OSError, "synthetic audit sink outage"):
                monitor.run_once(max_events=1)
            self.assertEqual(failing_sink.records, [])
            self.assertEqual(len(store.load(key).outbox), 1)

            restarted, source, _, _, _ = self._monitor(root, [first_event], store=store)
            restarted.audit_sink = failing_sink
            result = restarted.run_once(max_events=1)[0]

            self.assertIn("DUPLICATE_EVENT", result.reason_codes)
            self.assertEqual(
                [record.record_id for record in failing_sink.records],
                ["event:event:stream-a:0:0", "duplicate:event:stream-a:0:0"],
            )
            self.assertEqual(source.acknowledged, [first_event.event_id])
            self.assertEqual(store.load(key).outbox, ())

    def test_duplicate_event_is_idempotent_and_duplicate_envelope_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = _stream_key()
            events = _persist_events(root, stream_key=key, window_sequence=0)
            replay = events[0]
            conflicting = replace(
                events[1],
                event_id="event:conflicting-envelope",
                envelope_path=events[0].envelope_path,
                expected_trace_sha256=events[0].expected_trace_sha256,
            )
            monitor, _, _, sink, provider = self._monitor(
                root, [events[0], replay, events[1], conflicting]
            )

            results = monitor.run_once(max_events=4)

            self.assertTrue(any("DUPLICATE_EVENT" in item.reason_codes for item in results))
            self.assertTrue(
                any("DUPLICATE_ENVELOPE_REFERENCE" in item.reason_codes for item in results)
            )
            self.assertEqual(provider.calls, [])
            self.assertEqual(len({record.record_id for record in sink.records}), len(sink.records))
            duplicate_audits = [
                record for record in sink.records if "DUPLICATE_EVENT" in record.reason_codes
            ]
            self.assertEqual(len(duplicate_audits), 1)
            self.assertEqual(duplicate_audits[0].stream_key, key)
            self.assertEqual(
                duplicate_audits[0].event_trace_sha256,
                (events[0].expected_trace_sha256,),
            )

    def test_malformed_envelope_fails_closed_before_extraction_detector_or_policy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "malformed.json"
            path.write_text("{not-json", encoding="utf-8")
            event = ShadowTraceEvent(
                event_id="event:malformed",
                stream_key=_stream_key(),
                window_id="stream-a:window:0",
                window_sequence=0,
                envelope_path=path,
                expected_trace_sha256="0" * 64,
            )
            monitor, _, _, sink, provider = self._monitor(root, [event])

            result = monitor.run_once(max_events=1)[0]

            self.assertFalse(result.accepted)
            self.assertIn("ENVELOPE_LOAD_FAILED", result.reason_codes)
            self.assertEqual(provider.calls, [])
            self.assertEqual(len(sink.records), 1)

    def test_all_registered_malformed_envelope_variants_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = _stream_key()
            variants = {
                "invalid_schema": (
                    {"schema_version": "persisted-shadow-trace-envelope-v1"},
                    "ENVELOPE_LOAD_FAILED",
                    False,
                ),
                "invalid_timestamp": (
                    {"captured_at_utc": "not-a-timestamp"},
                    "TIMESTAMP_INVALID",
                    True,
                ),
                "checksum_mismatch": (
                    {"expected_trace_sha256": "0" * 64},
                    "ENVELOPE_LOAD_FAILED",
                    False,
                ),
                "count_mismatch": (
                    {"declared_observation_count": 49},
                    "DECLARED_OBSERVATION_COUNT_INVALID",
                    True,
                ),
            }
            for name, (mutation, expected_reason, requires_assembly) in variants.items():
                with self.subTest(name=name):
                    case_root = root / name
                    case_root.mkdir()
                    events = _persist_events(
                        case_root, stream_key=key, window_sequence=0
                    )
                    path = case_root / f"{name}.json"
                    document = json.loads(
                        events[0].envelope_path.read_text(encoding="utf-8")
                    )
                    if name == "invalid_schema":
                        document = mutation
                    else:
                        document.update(mutation)
                    path.write_text(json.dumps(document), encoding="utf-8")
                    event = replace(
                        events[0],
                        event_id=f"event:{name}",
                        envelope_path=path,
                    )
                    events = [event, *events[1:]] if requires_assembly else [event]
                    monitor, _, _, sink, provider = self._monitor(case_root, events)

                    result = monitor.run_once(max_events=len(events))[-1]

                    self.assertFalse(result.accepted)
                    self.assertIn(expected_reason, result.reason_codes)
                    self.assertEqual(provider.calls, [])
                    self.assertTrue(
                        any(expected_reason in record.reason_codes for record in sink.records)
                    )

    def test_missing_nonreference_state_fails_closed_before_policy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            event = _persist_events(root, stream_key=_stream_key(), window_sequence=1)[0]
            monitor, _, _, sink, provider = self._monitor(root, [event])

            result = monitor.run_once(max_events=1)[0]

            self.assertFalse(result.accepted)
            self.assertIn("STATE_MISSING_FOR_NONREFERENCE", result.reason_codes)
            self.assertEqual(provider.calls, [])
            self.assertEqual(len(sink.records), 1)

    def test_corrupted_persisted_reference_fails_closed_on_later_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = _stream_key()
            reference = _persist_events(root, stream_key=key, window_sequence=0)
            current = _persist_events(root, stream_key=key, window_sequence=1)
            monitor, _, _, sink, provider = self._monitor(root, reference)
            monitor.run_once(max_events=4)
            reference[0].envelope_path.write_text("{tampered", encoding="utf-8")
            monitor.source = FakeSource(current)

            results = monitor.run_once(max_events=4)

            self.assertFalse(results[-1].accepted)
            self.assertIn("SAVED_WINDOW_LOAD_FAILED", results[-1].reason_codes)
            self.assertEqual(provider.calls, [])
            self.assertTrue(
                any("SAVED_WINDOW_LOAD_FAILED" in record.reason_codes for record in sink.records)
            )

    def test_identity_change_in_same_stream_id_is_quarantined_without_rebaseline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = _stream_key()
            reference = _persist_events(root, stream_key=key, window_sequence=0)
            changed_key = _stream_key(configuration="config-v2")
            changed = _persist_events(root, stream_key=key, window_sequence=1)
            changed = [replace(event, stream_key=changed_key) for event in changed]
            monitor, _, store, sink, provider = self._monitor(root, [*reference, *changed])

            results = monitor.run_once(max_events=8)
            state = store.load(key)

            self.assertIsNotNone(state.reference)
            self.assertEqual(state.reference.window_id, reference[0].window_id)
            self.assertTrue(any("STREAM_IDENTITY_CHANGED" in item.reason_codes for item in results))
            self.assertEqual(provider.calls, [])
            self.assertTrue(any("STREAM_IDENTITY_CHANGED" in record.reason_codes for record in sink.records))

    def test_backpressure_processes_only_limit_and_keeps_future_sequences_pending(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_key = _stream_key(stream_id="stream-a")
            second_key = _stream_key(stream_id="stream-b")
            events = [
                *_persist_events(root, stream_key=first_key, window_sequence=0),
                *_persist_events(root, stream_key=second_key, window_sequence=0),
            ]
            monitor, source, _, _, _ = self._monitor(root, events)

            first = monitor.run_once(max_events=3)
            second = monitor.run_once(max_events=3)

            self.assertEqual(len(first), 3)
            self.assertEqual(len(second), 3)
            self.assertEqual(len(source.events), 2)
            self.assertTrue(all(result.processed_event_count <= 3 for result in (*first, *second)))

    def test_future_window_is_buffered_then_evaluated_in_monotonic_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = _stream_key()
            reference = _persist_events(root, stream_key=key, window_sequence=0)
            current_one = _persist_events(root, stream_key=key, window_sequence=1)
            current_two = _persist_events(root, stream_key=key, window_sequence=2)
            monitor, _, store, _, provider = self._monitor(
                root, [*reference, *current_two, *current_one]
            )

            results = monitor.run_once(max_events=12)

            evaluated = [result for result in results if result.policy_decision is not None]
            self.assertEqual(len(evaluated), 1)
            self.assertEqual(evaluated[0].policy_decision.action, PolicyAction.NO_CHANGE)
            self.assertEqual(len(provider.calls), 1)
            state = store.load(key)
            self.assertEqual(state.next_window_sequence, 3)
            self.assertEqual(state.pending_windows, ())

    def test_dry_run_has_no_actuation_dependency_and_never_allows_canary_input(self) -> None:
        source = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
        imported = {
            alias.name
            for node in ast.walk(source)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        }
        self.assertFalse(
            any("actuation" in name or "pymilvus" in name.lower() for name in imported),
            imported,
        )
        monitor_source = MODULE_PATH.read_text(encoding="utf-8")
        self.assertIn("mode=PolicyMode.DRY_RUN", monitor_source)
        self.assertIn("canary_observation=None", monitor_source)
        self.assertNotIn("CANARY_ENABLED", monitor_source)

    def test_real_safe_boundary_is_a_zero_call_noop_for_monitor_policy_output(self) -> None:
        class TrapClient:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def __getattr__(self, name: str):
                self.calls.append(name)
                raise AssertionError(f"unexpected actuation call: {name}")

        class BoundaryAudit:
            def __init__(self) -> None:
                self.records = {}

            def contains(self, audit_id: str) -> bool:
                return audit_id in self.records

            def append(self, record) -> None:
                self.records[record.audit_id] = record

        class Controller:
            def __init__(self) -> None:
                self.calls = []

            def disable_automatic_actions(self, *, audit_id: str, reason: str) -> None:
                self.calls.append((audit_id, reason))

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = _stream_key()
            events = [
                *_persist_events(root, stream_key=key, window_sequence=0),
                *_persist_events(root, stream_key=key, window_sequence=1),
                *_persist_events(root, stream_key=key, window_sequence=2),
            ]
            monitor, _, _, _, _ = self._monitor(root, events)
            policy = next(
                item.policy_decision
                for item in monitor.run_once(max_events=12)
                if item.policy_decision is not None
            )
            client = TrapClient()
            sink = BoundaryAudit()
            controller = Controller()
            result = SafeActuationBoundary(client, sink, controller).execute(
                policy,
                ActuationIdentityContext(
                    metric=Metric.L2,
                    threshold_stratum="target-075",
                    collection_name="monitor_l2_hnsw",
                    configuration_identity="config-v1",
                    index_identity="l2-hnsw-binding-v1",
                    flat_index_identity="l2-flat-binding-v1",
                    data_identity="dataset-v1",
                    occurred_at_utc="2026-08-03T12:00:00Z",
                ),
            )

        self.assertEqual(result.outcome, ActuationOutcome.NO_OP)
        self.assertFalse(result.executed)
        self.assertEqual(client.calls, [])
        self.assertEqual(controller.calls, [])


if __name__ == "__main__":
    unittest.main()
