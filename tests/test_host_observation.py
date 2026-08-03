"""TDD coverage for the ADR-007 host observation and shadow-worker boundary."""

from __future__ import annotations

import ast
from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from vdbench.config import IndexTrack, Metric
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from vdbench.oracle import OracleHit, OracleResult
from vdbench.shadow_event_source import PublicationStatus, TracePublicationReceipt
from vdbench.workload_monitor import MonitorStreamKey

from vdbench.host_observation import (
    BackgroundShadowWorker,
    BoundedHostObservationRecorder,
    CompletedRangeQueryObservation,
    FileHostWorkerStateStore,
    HostWorkerState,
    HostWorkerStateError,
    InMemoryHostWorkerStateStore,
    ObservationStatus,
    RangeQueryRequest,
    RegisteredTraceParameters,
    ReferenceRangeGateway,
    ServedQueryOutcome,
    StreamWorkerState,
)


_REGISTERED_TRACE_PARAMETERS = RegisteredTraceParameters(
    allowed_candidate_and_lkg_efs=frozenset({200, 400, 800, 1600}),
    sentinel_ef=100,
)


def _stream_key(metric: Metric = Metric.L2, *, suffix: str = "v1") -> MonitorStreamKey:
    return MonitorStreamKey(
        stream_id=f"host-{metric.value.lower()}-{suffix}",
        metric=metric,
        threshold_stratum="target-075",
        configuration_identity=f"configuration-{suffix}",
        data_identity=f"dataset-{suffix}",
        flat_binding_id=f"{metric.value.lower()}-flat-{suffix}",
        hnsw_binding_id=f"{metric.value.lower()}-hnsw-{suffix}",
    )


def _observation(
    request_id: int,
    *,
    stream_key: MonitorStreamKey | None = None,
) -> CompletedRangeQueryObservation:
    return CompletedRangeQueryObservation(
        request_id=request_id,
        captured_at_utc="2026-08-03T18:00:00Z",
        stream_key=stream_key or _stream_key(),
        query_vector=(float(request_id), 1.0),
        threshold_radius=2.0,
        range_filter=0.0,
        limit=100,
        served_ef=400,
        served_outcome=ServedQueryOutcome(
            success=True,
            timed_out=False,
            result_count=1,
            latency_ms=0.25,
        ),
    )


def _identity(track: IndexTrack, metric: Metric, suffix: str) -> ShadowIdentityEvidence:
    snapshot = CollectionIdentity(
        collection_name=f"host_{metric.value.lower()}_{track.value.lower()}_{suffix}",
        metric=metric.value,
        index_track=track.value,
        description={"index_type": track.value, "metric_type": metric.value},
    )
    stage = ShadowAuditStageEvidence(stage=f"{track.value}_IDENTITY", success=True)
    return ShadowIdentityEvidence(
        track=track,
        expected_binding_id=f"{metric.value.lower()}-{track.value.lower()}-{suffix}",
        pre_snapshot=snapshot,
        post_snapshot=snapshot,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=stage,
        post_capture=stage,
    )


def _trace(observations: tuple[CompletedRangeQueryObservation, ...]) -> ShadowAuditTrace:
    stream = observations[0].stream_key
    queries: list[ShadowQueryAuditTrace] = []
    for observation in observations:
        oracle = OracleResult(
            hits=(OracleHit(id=observation.request_id, score=1.0),),
            full_count=1,
            capped=False,
        )
        hit = SearchHit(id=observation.request_id, score=1.0)
        queries.append(
            ShadowQueryAuditTrace(
                query_id=observation.request_id,
                query_vector=observation.query_vector,
                threshold_radius=observation.threshold_radius,
                range_filter=observation.range_filter,
                limit=observation.limit,
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
        metric=stream.metric,
        threshold_stratum=stream.threshold_stratum,
        candidate_ef=800,
        last_known_good_ef=400,
        sentinel_ef=100,
        configuration_identity=stream.configuration_identity,
        data_identity=stream.data_identity,
        flat_identity=_identity(IndexTrack.FLAT, stream.metric, stream.stream_id.split("-")[-1]),
        hnsw_identity=_identity(IndexTrack.HNSW, stream.metric, stream.stream_id.split("-")[-1]),
        queries=tuple(queries),
        complete=True,
    )


class _FakeExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[CompletedRangeQueryObservation, ...]] = []
        self.trace_override: ShadowAuditTrace | None = None

    def capture(
        self, observations: tuple[CompletedRangeQueryObservation, ...]
    ) -> ShadowAuditTrace:
        self.calls.append(observations)
        return self.trace_override or _trace(observations)


class _FakePublisher:
    def __init__(self, *, status: PublicationStatus = PublicationStatus.PUBLISHED) -> None:
        self.status = status
        self.calls: list[tuple[object, object]] = []

    def publish(self, *, trace: object, context: object) -> TracePublicationReceipt:
        self.calls.append((trace, context))
        return TracePublicationReceipt(
            status=self.status,
            event_id=f"event-{len(self.calls)}",
            event=None,
            reason_code="PENDING_EVENT_CAPACITY_EXCEEDED"
            if self.status is PublicationStatus.DROPPED_BACKPRESSURE
            else None,
        )


class _RaisingPublisher:
    def __init__(self) -> None:
        self.calls = 0

    def publish(self, *, trace: object, context: object) -> TracePublicationReceipt:
        self.calls += 1
        raise RuntimeError("publisher unavailable")


class _ServingExecutor:
    def __init__(self) -> None:
        self.calls: list[RangeQueryRequest] = []

    def execute(self, request: RangeQueryRequest) -> ServedQueryOutcome:
        self.calls.append(request)
        return ServedQueryOutcome(success=True, timed_out=False, result_count=3, latency_ms=0.5)


class _FailingRecorder:
    def offer(self, observation: CompletedRangeQueryObservation) -> ObservationReceipt:
        raise RuntimeError("host telemetry unavailable")


class HostObservationTests(unittest.TestCase):
    def test_recorder_is_bounded_and_nonblocking(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=1)
        self.assertEqual(recorder.offer(_observation(1)).status, ObservationStatus.ACCEPTED)
        receipt = recorder.offer(_observation(2))
        self.assertEqual(receipt.status, ObservationStatus.DROPPED_BACKPRESSURE)
        self.assertEqual(receipt.reason_code, "PENDING_OBSERVATION_CAPACITY_EXCEEDED")
        self.assertEqual(tuple(item.request_id for item in recorder.drain(limit=2)), (1,))
        recorder.close()
        self.assertEqual(recorder.offer(_observation(3)).status, ObservationStatus.CLOSED)

    def test_reference_gateway_only_serves_then_records_until_worker_runs(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=50)
        serving = _ServingExecutor()
        gateway = ReferenceRangeGateway(
            serving_executor=serving,
            recorder=recorder,
            clock=lambda: "2026-08-03T18:00:00Z",
        )
        request = RangeQueryRequest(
            request_id="served-1",
            stream_key=_stream_key(),
            query_vector=(0.0, 1.0),
            threshold_radius=2.0,
            range_filter=0.0,
            limit=100,
            served_ef=400,
        )

        result = gateway.execute(request)

        self.assertTrue(result.served_outcome.success)
        self.assertEqual(result.observation_receipt.status, ObservationStatus.ACCEPTED)
        self.assertEqual(serving.calls, [request])
        self.assertEqual(recorder.pending_count, 1)

    def test_gateway_normalizes_request_id_before_serving_and_recording(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=1)
        serving = _ServingExecutor()
        gateway = ReferenceRangeGateway(
            serving_executor=serving,
            recorder=recorder,
            clock=lambda: "2026-08-03T18:00:00Z",
        )
        request = RangeQueryRequest(
            request_id="e\u0301",
            stream_key=_stream_key(),
            query_vector=(0.0, 1.0),
            threshold_radius=2.0,
            range_filter=0.0,
            limit=100,
            served_ef=400,
        )

        gateway.execute(request)

        self.assertEqual(request.request_id, "é")
        self.assertEqual(serving.calls[0].request_id, "é")
        self.assertEqual(recorder.drain(limit=1)[0].request_id, "é")

    def test_gateway_preserves_served_result_when_recorder_fails(self) -> None:
        serving = _ServingExecutor()
        gateway = ReferenceRangeGateway(
            serving_executor=serving,
            recorder=_FailingRecorder(),
            clock=lambda: "2026-08-03T18:00:00Z",
        )
        request = RangeQueryRequest(
            request_id="served-2",
            stream_key=_stream_key(),
            query_vector=(0.0, 1.0),
            threshold_radius=2.0,
            range_filter=0.0,
            limit=100,
            served_ef=400,
        )

        result = gateway.execute(request)

        self.assertTrue(result.served_outcome.success)
        self.assertEqual(result.observation_receipt.status, ObservationStatus.REJECTED_INVALID)
        self.assertEqual(result.observation_receipt.reason_code, "RECORDER_FAILED")
        self.assertEqual(serving.calls, [request])

    def test_worker_captures_exactly_fifty_and_advances_slot_only_after_publish(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=100)
        stream = _stream_key()
        for request_id in range(50):
            self.assertEqual(recorder.offer(_observation(request_id, stream_key=stream)).status, ObservationStatus.ACCEPTED)
        executor = _FakeExecutor()
        publisher = _FakePublisher()
        state_store = InMemoryHostWorkerStateStore()
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=executor,
            publisher=publisher,
            state_store=state_store,
            registered_trace_parameters=_REGISTERED_TRACE_PARAMETERS,
            max_partial_streams=2,
            max_observation_age_seconds=60.0,
            clock=lambda: "2026-08-03T18:00:00Z",
        )

        result = worker.run_once(max_observations=50)

        self.assertEqual(result.captured_trace_count, 1)
        self.assertEqual(result.published_trace_count, 1)
        self.assertEqual(tuple(item.request_id for item in executor.calls[0]), tuple(range(50)))
        _, context = publisher.calls[0]
        self.assertEqual(context.window_sequence, 0)
        self.assertEqual(context.trace_sequence_index, 0)
        self.assertEqual(context.window_id, "host-l2-v1:window:0")
        state = state_store.snapshot()
        self.assertEqual(state.streams[stream].next_trace_ordinal, 1)
        self.assertEqual(state.streams[stream].partial_observation_count, 0)

    def test_worker_keeps_metrics_and_lineages_isolated(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=200)
        l2 = _stream_key(Metric.L2, suffix="one")
        cosine = _stream_key(Metric.COSINE, suffix="two")
        for request_id in range(50):
            recorder.offer(_observation(request_id, stream_key=l2))
            recorder.offer(_observation(request_id + 100, stream_key=cosine))
        executor = _FakeExecutor()
        publisher = _FakePublisher()
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=executor,
            publisher=publisher,
            state_store=InMemoryHostWorkerStateStore(),
            registered_trace_parameters=_REGISTERED_TRACE_PARAMETERS,
            max_partial_streams=3,
            max_observation_age_seconds=60.0,
            clock=lambda: "2026-08-03T18:00:00Z",
        )

        result = worker.run_once(max_observations=100)

        self.assertEqual(result.published_trace_count, 2)
        self.assertEqual([items[0].stream_key.metric for items in executor.calls], [Metric.L2, Metric.COSINE])
        self.assertEqual([context.trace_sequence_index for _, context in publisher.calls], [0, 0])

    def test_trace_identity_or_query_mismatch_is_rejected_without_publication(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=50)
        stream = _stream_key()
        observations = tuple(_observation(request_id, stream_key=stream) for request_id in range(50))
        for observation in observations:
            recorder.offer(observation)
        executor = _FakeExecutor()
        executor.trace_override = _trace(observations)
        broken_query = replace(executor.trace_override.queries[0], query_id=9999)
        executor.trace_override = replace(
            executor.trace_override,
            queries=(broken_query, *executor.trace_override.queries[1:]),
        )
        publisher = _FakePublisher()
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=executor,
            publisher=publisher,
            state_store=InMemoryHostWorkerStateStore(),
            registered_trace_parameters=_REGISTERED_TRACE_PARAMETERS,
            max_partial_streams=2,
            max_observation_age_seconds=60.0,
            clock=lambda: "2026-08-03T18:00:00Z",
        )

        result = worker.run_once(max_observations=50)

        self.assertEqual(result.published_trace_count, 0)
        self.assertIn("TRACE_QUERY_IDS_MISMATCH", result.reason_codes)
        self.assertEqual(publisher.calls, [])

    def test_known_backpressure_does_not_advance_trace_slot(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=50)
        stream = _stream_key()
        for request_id in range(50):
            recorder.offer(_observation(request_id, stream_key=stream))
        store = InMemoryHostWorkerStateStore()
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=_FakeExecutor(),
            publisher=_FakePublisher(status=PublicationStatus.DROPPED_BACKPRESSURE),
            state_store=store,
            registered_trace_parameters=_REGISTERED_TRACE_PARAMETERS,
            max_partial_streams=2,
            max_observation_age_seconds=60.0,
            clock=lambda: "2026-08-03T18:00:00Z",
        )

        result = worker.run_once(max_observations=50)

        self.assertEqual(result.published_trace_count, 0)
        self.assertIn("PENDING_EVENT_CAPACITY_EXCEEDED", result.reason_codes)
        self.assertEqual(store.snapshot().streams[stream].next_trace_ordinal, 0)
        self.assertIsNone(store.snapshot().streams[stream].blocked_reason_code)

    def test_worker_rejects_unregistered_parameters_and_failed_stages(self) -> None:
        stream = _stream_key()
        for mode in ("parameters", "stage"):
            with self.subTest(mode=mode):
                recorder = BoundedHostObservationRecorder(max_pending_observations=50)
                observations = tuple(_observation(request_id, stream_key=stream) for request_id in range(50))
                for observation in observations:
                    recorder.offer(observation)
                executor = _FakeExecutor()
                trace = _trace(observations)
                if mode == "parameters":
                    executor.trace_override = replace(trace, candidate_ef=999)
                    expected_reason = "TRACE_QUERY_PARAMETER_UNREGISTERED"
                else:
                    failed_stage = replace(trace.queries[0].stages[1], oracle_agreement=False)
                    executor.trace_override = replace(
                        trace,
                        queries=(
                            replace(trace.queries[0], stages=(trace.queries[0].stages[0], failed_stage, trace.queries[0].stages[2])),
                            *trace.queries[1:],
                        ),
                    )
                    expected_reason = "TRACE_STAGE_FAILED"
                publisher = _FakePublisher()
                worker = BackgroundShadowWorker(
                    recorder=recorder,
                    executor=executor,
                    publisher=publisher,
                    state_store=InMemoryHostWorkerStateStore(),
                    registered_trace_parameters=_REGISTERED_TRACE_PARAMETERS,
                    max_partial_streams=2,
                    max_observation_age_seconds=60.0,
                    clock=lambda: "2026-08-03T18:00:00Z",
                )

                result = worker.run_once(max_observations=50)

                self.assertIn(expected_reason, result.reason_codes)
                self.assertEqual(publisher.calls, [])

    def test_malformed_trace_evidence_fails_closed_without_crashing_worker(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=50)
        stream = _stream_key()
        observations = tuple(_observation(request_id, stream_key=stream) for request_id in range(50))
        for observation in observations:
            recorder.offer(observation)
        trace = _trace(observations)
        malformed_stage = replace(trace.queries[0].stages[0], threshold_violation_count="not-an-integer")
        executor = _FakeExecutor()
        executor.trace_override = replace(
            trace,
            queries=(
                replace(trace.queries[0], stages=(malformed_stage, *trace.queries[0].stages[1:])),
                *trace.queries[1:],
            ),
        )
        publisher = _FakePublisher()
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=executor,
            publisher=publisher,
            state_store=InMemoryHostWorkerStateStore(),
            registered_trace_parameters=_REGISTERED_TRACE_PARAMETERS,
            max_partial_streams=2,
            max_observation_age_seconds=60.0,
            clock=lambda: "2026-08-03T18:00:00Z",
        )

        result = worker.run_once(max_observations=50)

        self.assertIn("TRACE_VALIDATION_FAILED", result.reason_codes)
        self.assertEqual(publisher.calls, [])

    def test_worker_enforces_age_and_partial_stream_limits(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=3)
        l2 = _stream_key(Metric.L2, suffix="one")
        cosine = _stream_key(Metric.COSINE, suffix="two")
        recorder.offer(_observation(1, stream_key=l2))
        recorder.offer(_observation(2, stream_key=cosine))
        recorder.offer(
            replace(
                _observation(3, stream_key=l2),
                captured_at_utc="2026-08-03T17:00:00Z",
            )
        )
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=_FakeExecutor(),
            publisher=_FakePublisher(),
            state_store=InMemoryHostWorkerStateStore(),
            registered_trace_parameters=_REGISTERED_TRACE_PARAMETERS,
            max_partial_streams=1,
            max_observation_age_seconds=60.0,
            clock=lambda: "2026-08-03T18:00:00Z",
        )

        result = worker.run_once(max_observations=3)

        self.assertIn("PARTIAL_STREAM_CAPACITY_EXCEEDED", result.reason_codes)
        self.assertIn("OBSERVATION_STALE", result.reason_codes)
        self.assertEqual(result.published_trace_count, 0)

    def test_trace_slot_rolls_over_after_four_published_traces(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=250)
        stream = _stream_key()
        for request_id in range(250):
            recorder.offer(_observation(request_id, stream_key=stream))
        publisher = _FakePublisher()
        store = InMemoryHostWorkerStateStore()
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=_FakeExecutor(),
            publisher=publisher,
            state_store=store,
            registered_trace_parameters=_REGISTERED_TRACE_PARAMETERS,
            max_partial_streams=1,
            max_observation_age_seconds=60.0,
            clock=lambda: "2026-08-03T18:00:00Z",
        )

        result = worker.run_once(max_observations=250)

        self.assertEqual(result.published_trace_count, 5)
        self.assertEqual(
            [(context.window_sequence, context.trace_sequence_index) for _, context in publisher.calls],
            [(0, 0), (0, 1), (0, 2), (0, 3), (1, 0)],
        )
        self.assertEqual(store.snapshot().streams[stream].next_trace_ordinal, 5)

    def test_unknown_publish_failure_blocks_stream_without_reusing_slot(self) -> None:
        recorder = BoundedHostObservationRecorder(max_pending_observations=100)
        stream = _stream_key()
        for request_id in range(100):
            recorder.offer(_observation(request_id, stream_key=stream))
        publisher = _RaisingPublisher()
        store = InMemoryHostWorkerStateStore()
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=_FakeExecutor(),
            publisher=publisher,
            state_store=store,
            registered_trace_parameters=_REGISTERED_TRACE_PARAMETERS,
            max_partial_streams=2,
            max_observation_age_seconds=60.0,
            clock=lambda: "2026-08-03T18:00:00Z",
        )

        first = worker.run_once(max_observations=50)
        second = worker.run_once(max_observations=50)

        self.assertIn("PUBLISH_OUTCOME_UNKNOWN", first.reason_codes)
        self.assertEqual(publisher.calls, 1)
        self.assertEqual(second.published_trace_count, 0)
        self.assertEqual(publisher.calls, 1)
        state = store.snapshot().streams[stream]
        self.assertEqual(state.next_trace_ordinal, 0)
        self.assertEqual(state.blocked_reason_code, "PUBLISH_OUTCOME_UNKNOWN")

    def test_restart_records_exact_loss_without_persisting_raw_query_data(self) -> None:
        stream = _stream_key()
        state = HostWorkerState(
            streams={
                stream: StreamWorkerState(
                    stream_key=stream,
                    next_trace_ordinal=3,
                    partial_observation_count=7,
                    inflight_observation_count=50,
                )
            }
        )
        with tempfile.TemporaryDirectory() as temporary:
            store = FileHostWorkerStateStore(Path(temporary))
            store.save(state)
            raw = next(Path(temporary).glob("*.json")).read_text(encoding="utf-8")
            self.assertNotIn("query_vector", raw)
            self.assertNotIn("98765.25", raw)
            recovered = store.recover()

            restored = recovered.streams[stream]
            self.assertEqual(restored.partial_observation_count, 0)
            self.assertEqual(restored.inflight_observation_count, 0)
            self.assertEqual(restored.restart_loss_count, 57)
            self.assertEqual(store.recover().streams[stream].restart_loss_count, 57)

    def test_dangling_worker_state_symlink_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "host-worker-state.json").symlink_to(directory / "missing-state.json")
            store = FileHostWorkerStateStore(directory)

            with self.assertRaisesRegex(HostWorkerStateError, "HOST_WORKER_STATE_SYMLINK_REJECTED"):
                store.snapshot()

    def test_host_boundary_has_no_policy_actuation_monitor_or_pymilvus_imports(self) -> None:
        module_path = Path(__file__).parents[1] / "src" / "vdbench" / "host_observation.py"
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        modules = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        modules.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        forbidden = ("pymilvus", "policy", "actuation", "workload_monitor")
        self.assertFalse(
            [module for module in modules if any(word in module.lower() for word in forbidden)],
            modules,
        )

    def test_importing_host_boundary_does_not_load_detector_policy_or_actuation(self) -> None:
        root = str(Path(__file__).parents[1] / "src")
        environment = dict(os.environ, PYTHONPATH=root, PYTHONDONTWRITEBYTECODE="1")
        process = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import sys; import vdbench.host_observation; "
                    "print(sorted(name for name in sys.modules if name.startswith('vdbench.') "
                    "and any(token in name for token in ('policy', 'actuation', 'workload_monitor', 'pymilvus'))))"
                ),
            ],
            check=True,
            capture_output=True,
            encoding="utf-8",
            env=environment,
        )
        self.assertEqual(process.stdout, "[]\n")


if __name__ == "__main__":
    unittest.main()
