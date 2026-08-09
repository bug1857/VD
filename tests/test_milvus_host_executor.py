from __future__ import annotations

import ast
from dataclasses import dataclass, replace
from pathlib import Path
import threading
import time
import unittest

import numpy as np

from vdbench.actuation import ShadowActuationContext, ShadowResult
from vdbench.config import IndexTrack, Metric
from vdbench.host_observation import (
    BackgroundShadowWorker,
    BoundedHostObservationRecorder,
    CompletedRangeQueryObservation,
    InMemoryHostWorkerStateStore,
    RegisteredTraceParameters,
    ServedQueryOutcome,
)
from vdbench.milvus import CollectionIdentity
from vdbench.milvus_actuation import (
    ActuationWorkload,
    CollectionIdentityBinding,
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
    StackHealth,
)
from vdbench.oracle import OracleResult
from vdbench.shadow_event_types import (
    MonitorStreamKey,
    PublicationStatus,
    TracePublicationContext,
    TracePublicationReceipt,
)

from vdbench.milvus_host_executor import (
    HostShadowExecutionError,
    HostShadowPlan,
    MilvusHostShadowExecutor,
)


REPOSITORY = Path(__file__).parents[1]
MODULE_PATH = REPOSITORY / "src" / "vdbench" / "milvus_host_executor.py"
METRIC = Metric.L2
STRATUM = "target-025"
CONFIGURATION_ID = "config-host-v1"
DATA_ID = "data-host-v1"
FLAT_BINDING_ID = "flat-host-v1"
HNSW_BINDING_ID = "hnsw-host-v1"
FLAT_COLLECTION = "host_l2_flat"
HNSW_COLLECTION = "host_l2_hnsw"
TIMESTAMP = "2026-08-03T12:00:00Z"


def _description(track: IndexTrack) -> dict[str, object]:
    description: dict[str, object] = {
        "index_name": "vector_index",
        "index_type": track.value,
        "metric_type": METRIC.value,
        "state": "Finished",
    }
    if track is IndexTrack.HNSW:
        description.update({"M": "16", "efConstruction": "200"})
    return description


def _identity(name: str, track: IndexTrack) -> CollectionIdentity:
    return CollectionIdentity(name, METRIC.value, track.value, _description(track))


@dataclass
class _FakeClient:
    loaded: bool = True

    def __post_init__(self) -> None:
        self.load_calls: list[str] = []

    def get_load_state(self, *, collection_name: str) -> object:
        self.load_calls.append(collection_name)
        return {"state": "Loaded" if self.loaded else "NotLoaded"}


@dataclass
class _FakeHarness:
    identities: dict[str, CollectionIdentity]
    identity_matches: bool = True

    def __post_init__(self) -> None:
        self.calls: list[tuple[str, Metric, IndexTrack]] = []

    def index_identity(
        self, name: str, metric: Metric, track: IndexTrack
    ) -> CollectionIdentity:
        self.calls.append((name, metric, track))
        identity = self.identities[name]
        if not self.identity_matches:
            return CollectionIdentity(
                identity.collection_name,
                identity.metric,
                identity.index_track,
                {**_description(track), "unexpected": "changed"},
            )
        return identity


@dataclass
class _FakeHealthProbe:
    result: StackHealth = StackHealth(True, True, "healthy")

    def __post_init__(self) -> None:
        self.calls = 0

    def check(self) -> StackHealth:
        self.calls += 1
        return self.result


class _FakeAdapter:
    def __init__(self) -> None:
        query_vectors = {
            query_id: np.asarray((query_id / 1000.0, 0.25), dtype="<f4")
            for query_id in range(50)
        }
        flat = _identity(FLAT_COLLECTION, IndexTrack.FLAT)
        hnsw = _identity(HNSW_COLLECTION, IndexTrack.HNSW)
        self.workload = ActuationWorkload(
            query_vectors=query_vectors,
            base_ids=np.asarray((0,), dtype=np.int64),
            base_vectors=np.asarray(((0.0, 0.0),), dtype="<f4"),
            threshold_radii={(METRIC, STRATUM): 1.0},
            collection_names={
                (METRIC, IndexTrack.FLAT): FLAT_COLLECTION,
                (METRIC, IndexTrack.HNSW): HNSW_COLLECTION,
            },
            identity_bindings={
                (METRIC, IndexTrack.FLAT): CollectionIdentityBinding(
                    FLAT_BINDING_ID, flat
                ),
                (METRIC, IndexTrack.HNSW): CollectionIdentityBinding(
                    HNSW_BINDING_ID, hnsw
                ),
            },
            configuration_identity=CONFIGURATION_ID,
            data_identity=DATA_ID,
        )
        self.client = _FakeClient()
        self.harness = _FakeHarness({FLAT_COLLECTION: flat, HNSW_COLLECTION: hnsw})
        self.stack_health_probe = _FakeHealthProbe()
        self.shadow_trace_sink: object | None = None
        self.shadow_calls: list[tuple[ShadowActuationContext, int, int]] = []
        self.shadow_result = ShadowResult(
            success=True,
            audited_query_count=50,
            failed_query_count=0,
            timeout_query_count=0,
            threshold_violation_count=0,
            candidate_flat_oracle_agreement=True,
            last_known_good_flat_oracle_agreement=True,
        )
        self.trace_complete = True
        self.emit_trace = True
        self.after_shadow = None
        self.trace_transform = None
        self.shadow_entered: threading.Event | None = None
        self.shadow_release: threading.Event | None = None

    def shadow_candidate(
        self,
        *,
        context: ShadowActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult:
        self.shadow_calls.append((context, candidate_ef, last_known_good_ef))
        if self.emit_trace:
            assert self.shadow_trace_sink is not None
            trace = _trace(
                context,
                candidate_ef=candidate_ef,
                last_known_good_ef=last_known_good_ef,
                complete=self.trace_complete,
            )
            if self.trace_transform is not None:
                trace = self.trace_transform(trace)
            self.shadow_trace_sink.append(trace)  # type: ignore[union-attr]
        if self.shadow_entered is not None:
            self.shadow_entered.set()
        if self.shadow_release is not None:
            self.shadow_release.wait(timeout=5)
        if self.after_shadow is not None:
            self.after_shadow()
        return self.shadow_result

    def start_canary(self, **kwargs: object) -> object:
        raise AssertionError("automatic canary must never be invoked")

    def stop_candidate(self) -> None:
        raise AssertionError("automatic stop must never be invoked")

    def restore_last_known_good(self, ef: int) -> None:
        raise AssertionError("automatic restore must never be invoked")

    def verify_restoration(self, **kwargs: object) -> object:
        raise AssertionError("automatic verification must never be invoked")


class _FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[ShadowAuditTrace, TracePublicationContext]] = []

    def publish(
        self, *, trace: object, context: TracePublicationContext
    ) -> TracePublicationReceipt:
        assert isinstance(trace, ShadowAuditTrace)
        self.published.append((trace, context))
        return TracePublicationReceipt(
            status=PublicationStatus.PUBLISHED,
            event_id="event-1",
            event=None,
        )


def _trace(
    context: ShadowActuationContext,
    *,
    candidate_ef: int,
    last_known_good_ef: int,
    complete: bool,
) -> ShadowAuditTrace:
    flat_snapshot = _identity(FLAT_COLLECTION, IndexTrack.FLAT)
    hnsw_snapshot = _identity(HNSW_COLLECTION, IndexTrack.HNSW)
    good_stage = ShadowAuditStageEvidence(stage="IDENTITY", success=True)
    flat_identity = ShadowIdentityEvidence(
        track=IndexTrack.FLAT,
        expected_binding_id=FLAT_BINDING_ID,
        pre_snapshot=flat_snapshot,
        post_snapshot=flat_snapshot,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=good_stage,
        post_capture=good_stage,
    )
    hnsw_identity = ShadowIdentityEvidence(
        track=IndexTrack.HNSW,
        expected_binding_id=HNSW_BINDING_ID,
        pre_snapshot=hnsw_snapshot,
        post_snapshot=hnsw_snapshot,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=good_stage,
        post_capture=good_stage,
    )
    queries = tuple(
        ShadowQueryAuditTrace(
            query_id=query_id,
            query_vector=(query_id / 1000.0, 0.25),
            threshold_radius=1.0,
            range_filter=0.0,
            limit=100,
            oracle_result=OracleResult(hits=(), full_count=0, capped=False),
            exact_cardinality=0,
            flat_hits=(),
            sentinel_hits=(),
            sentinel_recall=1.0,
            stages=(
                ShadowAuditStageEvidence(stage="ORACLE", success=True),
                ShadowAuditStageEvidence(
                    stage="FLAT", success=True, oracle_agreement=True
                ),
                ShadowAuditStageEvidence(stage="CANDIDATE_HNSW", success=True),
                ShadowAuditStageEvidence(
                    stage="LAST_KNOWN_GOOD_HNSW", success=True
                ),
                ShadowAuditStageEvidence(stage="SENTINEL_HNSW", success=True),
            ),
        )
        for query_id in context.audited_query_ids
    )
    return ShadowAuditTrace(
        metric=METRIC,
        threshold_stratum=STRATUM,
        candidate_ef=candidate_ef,
        last_known_good_ef=last_known_good_ef,
        sentinel_ef=100,
        configuration_identity=CONFIGURATION_ID,
        data_identity=DATA_ID,
        flat_identity=flat_identity,
        hnsw_identity=hnsw_identity,
        queries=queries,
        complete=complete,
        reason_codes=() if complete else ("TRACE_INCOMPLETE",),
    )


def _stream_key() -> MonitorStreamKey:
    return MonitorStreamKey(
        stream_id="host-l2-target025",
        metric=METRIC,
        threshold_stratum=STRATUM,
        configuration_identity=CONFIGURATION_ID,
        data_identity=DATA_ID,
        flat_binding_id=FLAT_BINDING_ID,
        hnsw_binding_id=HNSW_BINDING_ID,
    )


def _observations(*, served_ef: int = 400) -> tuple[CompletedRangeQueryObservation, ...]:
    stream_key = _stream_key()
    return tuple(
        CompletedRangeQueryObservation(
            request_id=query_id,
            captured_at_utc=TIMESTAMP,
            stream_key=stream_key,
            query_vector=(query_id / 1000.0, 0.25),
            threshold_radius=1.0,
            range_filter=0.0,
            limit=100,
            served_ef=served_ef,
            served_outcome=ServedQueryOutcome(
                success=True,
                timed_out=False,
                result_count=0,
                latency_ms=0.1,
            ),
        )
        for query_id in range(50)
    )


def _executor(adapter: _FakeAdapter) -> MilvusHostShadowExecutor:
    return MilvusHostShadowExecutor(
        adapter=adapter,
        plans={
            _stream_key(): HostShadowPlan(
                candidate_ef=800,
                last_known_good_ef=400,
                required_served_ef=400,
            )
        },
        clock=lambda: TIMESTAMP,
    )


class MilvusHostShadowExecutorTests(unittest.TestCase):
    def test_capture_preflights_runs_one_shadow_and_returns_one_trace(self) -> None:
        adapter = _FakeAdapter()

        trace = _executor(adapter).capture(_observations())

        self.assertTrue(trace.complete)
        self.assertEqual(len(adapter.shadow_calls), 1)
        context, candidate_ef, last_known_good_ef = adapter.shadow_calls[0]
        self.assertIs(type(context), ShadowActuationContext)
        self.assertFalse(hasattr(context, "last_known_good"))
        self.assertEqual((candidate_ef, last_known_good_ef), (800, 400))
        self.assertEqual(context.metric, METRIC)
        self.assertEqual(context.threshold_stratum, STRATUM)
        self.assertEqual(context.audited_query_ids, tuple(range(50)))
        self.assertEqual(context.configuration_identity, CONFIGURATION_ID)
        self.assertEqual(context.flat_index_identity, FLAT_BINDING_ID)
        self.assertEqual(context.index_identity, HNSW_BINDING_ID)
        self.assertIsNone(adapter.shadow_trace_sink)
        self.assertEqual(adapter.stack_health_probe.calls, 2)
        self.assertEqual(adapter.client.load_calls, [
            FLAT_COLLECTION, HNSW_COLLECTION, FLAT_COLLECTION, HNSW_COLLECTION
        ])
        self.assertEqual(len(adapter.harness.calls), 4)

    def test_observation_contract_mismatch_rejects_before_shadow(self) -> None:
        for field, replacement in (
            ("query_vector", (99.0, 0.25)),
            ("threshold_radius", 2.0),
            ("range_filter", 0.5),
            ("limit", 99),
            ("served_ef", 800),
        ):
            with self.subTest(field=field):
                adapter = _FakeAdapter()
                values = list(_observations())
                values[0] = replace(values[0], **{field: replacement})
                with self.assertRaises(HostShadowExecutionError):
                    _executor(adapter).capture(tuple(values))
                self.assertEqual(adapter.shadow_calls, [])

    def test_preflight_and_postflight_fail_closed_before_or_after_shadow(self) -> None:
        for failure in ("health", "load", "identity"):
            with self.subTest(phase="pre", failure=failure):
                adapter = _FakeAdapter()
                if failure == "health":
                    adapter.stack_health_probe.result = StackHealth(False, True)
                elif failure == "load":
                    adapter.client.loaded = False
                else:
                    adapter.harness.identity_matches = False
                with self.assertRaises(HostShadowExecutionError):
                    _executor(adapter).capture(_observations())
                self.assertEqual(adapter.shadow_calls, [])

            with self.subTest(phase="post", failure=failure):
                adapter = _FakeAdapter()
                if failure == "health":
                    adapter.after_shadow = lambda: setattr(
                        adapter.stack_health_probe, "result", StackHealth(False, True)
                    )
                elif failure == "load":
                    adapter.after_shadow = lambda: setattr(adapter.client, "loaded", False)
                else:
                    adapter.after_shadow = lambda: setattr(
                        adapter.harness, "identity_matches", False
                    )
                with self.assertRaises(HostShadowExecutionError):
                    _executor(adapter).capture(_observations())
                self.assertEqual(len(adapter.shadow_calls), 1)
                self.assertIsNone(adapter.shadow_trace_sink)

    def test_sink_conflict_bad_shadow_and_trace_cardinality_fail_closed(self) -> None:
        adapter = _FakeAdapter()
        adapter.shadow_trace_sink = object()
        with self.assertRaisesRegex(HostShadowExecutionError, "SINK_OWNERSHIP"):
            _executor(adapter).capture(_observations())
        self.assertEqual(adapter.shadow_calls, [])

        adapter = _FakeAdapter()
        adapter.shadow_result = ShadowResult(False, 50, 1, 0, 0, False, False)
        with self.assertRaisesRegex(HostShadowExecutionError, "SHADOW_RESULT"):
            _executor(adapter).capture(_observations())
        self.assertIsNone(adapter.shadow_trace_sink)

        adapter = _FakeAdapter()
        adapter.emit_trace = False
        with self.assertRaisesRegex(HostShadowExecutionError, "TRACE_CAPTURE"):
            _executor(adapter).capture(_observations())
        self.assertIsNone(adapter.shadow_trace_sink)

    def test_incomplete_trace_is_returned_for_worker_to_reject(self) -> None:
        adapter = _FakeAdapter()
        adapter.trace_complete = False

        trace = _executor(adapter).capture(_observations())

        self.assertFalse(trace.complete)
        self.assertEqual(trace.reason_codes, ("TRACE_INCOMPLETE",))

    def test_tampered_trace_metadata_and_binding_fail_closed(self) -> None:
        adapter = _FakeAdapter()
        adapter.trace_transform = lambda trace: replace(trace, candidate_ef=400)
        with self.assertRaisesRegex(HostShadowExecutionError, "TRACE_METADATA"):
            _executor(adapter).capture(_observations())
        self.assertIsNone(adapter.shadow_trace_sink)

        adapter = _FakeAdapter()
        adapter.trace_transform = lambda trace: replace(
            trace,
            flat_identity=replace(
                trace.flat_identity,
                expected_binding_id="unexpected-flat-binding",
            ),
        )
        with self.assertRaisesRegex(HostShadowExecutionError, "TRACE_BINDING"):
            _executor(adapter).capture(_observations())
        self.assertIsNone(adapter.shadow_trace_sink)

    def test_lock_serializes_temporary_sink_ownership(self) -> None:
        adapter = _FakeAdapter()
        adapter.shadow_entered = threading.Event()
        adapter.shadow_release = threading.Event()
        executor = _executor(adapter)
        errors: list[BaseException] = []

        def capture() -> None:
            try:
                executor.capture(_observations())
            except BaseException as exc:  # test collects thread failures
                errors.append(exc)

        first = threading.Thread(target=capture)
        second = threading.Thread(target=capture)
        first.start()
        self.assertTrue(adapter.shadow_entered.wait(timeout=2))
        second.start()
        time.sleep(0.05)
        self.assertEqual(len(adapter.shadow_calls), 1)
        adapter.shadow_release.set()
        first.join(timeout=2)
        second.join(timeout=2)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(adapter.shadow_calls), 2)
        self.assertIsNone(adapter.shadow_trace_sink)

    def test_worker_accepts_complete_capture_and_rejects_incomplete_capture(self) -> None:
        adapter = _FakeAdapter()
        recorder = BoundedHostObservationRecorder(max_pending_observations=100)
        for observation in _observations():
            recorder.offer(observation)
        publisher = _FakePublisher()
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=_executor(adapter),
            publisher=publisher,
            state_store=InMemoryHostWorkerStateStore(),
            registered_trace_parameters=RegisteredTraceParameters(
                allowed_candidate_and_lkg_efs=frozenset({400, 800}),
                sentinel_ef=100,
            ),
            max_partial_streams=1,
            max_observation_age_seconds=60.0,
            clock=lambda: TIMESTAMP,
        )

        result = worker.run_once(max_observations=50)

        self.assertEqual(result.captured_trace_count, 1)
        self.assertEqual(result.published_trace_count, 1)
        self.assertEqual(result.reason_codes, ())
        self.assertEqual(len(publisher.published), 1)

        adapter = _FakeAdapter()
        adapter.trace_complete = False
        recorder = BoundedHostObservationRecorder(max_pending_observations=100)
        for observation in _observations():
            recorder.offer(observation)
        publisher = _FakePublisher()
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=_executor(adapter),
            publisher=publisher,
            state_store=InMemoryHostWorkerStateStore(),
            registered_trace_parameters=RegisteredTraceParameters(
                allowed_candidate_and_lkg_efs=frozenset({400, 800}),
                sentinel_ef=100,
            ),
            max_partial_streams=1,
            max_observation_age_seconds=60.0,
            clock=lambda: TIMESTAMP,
        )

        result = worker.run_once(max_observations=50)

        self.assertEqual(result.captured_trace_count, 0)
        self.assertEqual(result.published_trace_count, 0)
        self.assertIn("TRACE_INCOMPLETE", result.reason_codes)
        self.assertEqual(publisher.published, [])

    def test_unknown_plan_and_non_successful_served_outcome_reject_before_shadow(self) -> None:
        adapter = _FakeAdapter()
        other_key = MonitorStreamKey(
            stream_id="other",
            metric=METRIC,
            threshold_stratum=STRATUM,
            configuration_identity=CONFIGURATION_ID,
            data_identity=DATA_ID,
            flat_binding_id=FLAT_BINDING_ID,
            hnsw_binding_id=HNSW_BINDING_ID,
        )
        values = list(_observations())
        values[0] = CompletedRangeQueryObservation(
            request_id=values[0].request_id,
            captured_at_utc=values[0].captured_at_utc,
            stream_key=other_key,
            query_vector=values[0].query_vector,
            threshold_radius=values[0].threshold_radius,
            range_filter=values[0].range_filter,
            limit=values[0].limit,
            served_ef=values[0].served_ef,
            served_outcome=values[0].served_outcome,
        )
        with self.assertRaises(HostShadowExecutionError):
            _executor(adapter).capture(tuple(values))
        self.assertEqual(adapter.shadow_calls, [])

        adapter = _FakeAdapter()
        values = list(_observations())
        values[0] = CompletedRangeQueryObservation(
            request_id=values[0].request_id,
            captured_at_utc=values[0].captured_at_utc,
            stream_key=values[0].stream_key,
            query_vector=values[0].query_vector,
            threshold_radius=values[0].threshold_radius,
            range_filter=values[0].range_filter,
            limit=values[0].limit,
            served_ef=values[0].served_ef,
            served_outcome=ServedQueryOutcome(False, False, 0, 0.1, "SERVE_FAILED"),
        )
        with self.assertRaisesRegex(HostShadowExecutionError, "SERVED_OUTCOME"):
            _executor(adapter).capture(tuple(values))
        self.assertEqual(adapter.shadow_calls, [])

    def test_source_has_no_automatic_action_references(self) -> None:
        source = MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        attribute_names = {
            node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
        }
        names = {
            node.id for node in ast.walk(tree) if isinstance(node, ast.Name)
        }
        forbidden = {
            "SafeActuationBoundary",
            "evaluate_tuning_policy",
            "start_canary",
            "stop_candidate",
            "restore_last_known_good",
            "verify_restoration",
        }
        self.assertFalse(forbidden & (attribute_names | names))
        mutations = {
            "create_collection",
            "drop_collection",
            "create_index",
            "load_collection",
            "insert",
            "delete",
        }
        self.assertFalse(mutations & attribute_names)
        imports = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }
        self.assertFalse({"pymilvus", "workload_monitor"} & imports)


if __name__ == "__main__":
    unittest.main()
