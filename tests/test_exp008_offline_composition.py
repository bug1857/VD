"""ADR-007/EXP-008 offline host-to-monitor composition proof.

This test intentionally uses a fake read-only adapter.  It drives the actual
reference gateway, bounded recorder, background worker, durable trace outbox,
window assembler/extractor, detector, and DRY_RUN monitor for 600 stationary
completed requests per metric.  It neither imports PyMilvus nor permits an
automatic action method to run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

import numpy as np

from vdbench.actuation import ActuationContext, ShadowResult
from vdbench.config import IndexTrack, Metric
from vdbench.drift import DetectorState, EvidenceProvenance
from vdbench.host_observation import (
    BackgroundShadowWorker,
    BoundedHostObservationRecorder,
    InMemoryHostWorkerStateStore,
    RangeQueryRequest,
    ReferenceRangeGateway,
    RegisteredTraceParameters,
    ServedQueryOutcome,
)
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ActuationWorkload,
    CollectionIdentityBinding,
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
    StackHealth,
)
from vdbench.milvus_host_executor import HostShadowPlan, MilvusHostShadowExecutor
from vdbench.oracle import OracleHit, OracleResult
from vdbench.policy import (
    PolicyAction,
    PreActionSafety,
    QualificationResult,
)
from vdbench.shadow_event_source import FileShadowTraceEventSource
from vdbench.shadow_event_types import MonitorStreamKey
from vdbench.workload_monitor import (
    DryRunPolicyInputs,
    FileMonitorStateStore,
    MonitorAuditRecord,
    WorkloadMonitor,
)


DETECTOR_SEED = 20260805
CONFIGURATION_ID = "exp008-offline-config-v1"
DATA_ID = "exp008-offline-data-v1"


class _UtcStepClock:
    def __init__(self) -> None:
        self._value = datetime(2026, 8, 3, 12, 0, 0, tzinfo=timezone.utc)

    def __call__(self) -> str:
        value = self._value
        self._value += timedelta(milliseconds=1)
        return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _names(metric: Metric, track: IndexTrack) -> str:
    return f"exp008_{metric.value.lower()}_{track.value.lower()}"


def _binding_id(metric: Metric, track: IndexTrack) -> str:
    return f"exp008-{metric.value.lower()}-{track.value.lower()}-binding-v1"


def _description(metric: Metric, track: IndexTrack) -> dict[str, object]:
    value: dict[str, object] = {
        "index_name": "vector_index",
        "index_type": track.value,
        "metric_type": metric.value,
        "state": "Finished",
    }
    if track is IndexTrack.HNSW:
        value.update({"M": "16", "efConstruction": "200"})
    return value


def _identity(metric: Metric, track: IndexTrack) -> CollectionIdentity:
    return CollectionIdentity(
        _names(metric, track), metric.value, track.value, _description(metric, track)
    )


def _stream_key(metric: Metric) -> MonitorStreamKey:
    return MonitorStreamKey(
        stream_id=f"exp008-{metric.value.lower()}-stationary",
        metric=metric,
        threshold_stratum="target-025",
        configuration_identity=CONFIGURATION_ID,
        data_identity=DATA_ID,
        flat_binding_id=_binding_id(metric, IndexTrack.FLAT),
        hnsw_binding_id=_binding_id(metric, IndexTrack.HNSW),
    )


def _radius(metric: Metric) -> float:
    return 2.0 if metric is Metric.L2 else 0.5


class _FakeClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_load_state(self, *, collection_name: str) -> object:
        self.calls.append(collection_name)
        return {"state": "Loaded"}


class _FakeHarness:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Metric, IndexTrack]] = []

    def index_identity(
        self, name: str, metric: Metric, track: IndexTrack
    ) -> CollectionIdentity:
        self.calls.append((name, metric, track))
        return _identity(metric, track)


class _FakeHealth:
    def __init__(self) -> None:
        self.calls = 0

    def check(self) -> StackHealth:
        self.calls += 1
        return StackHealth(etcd_healthy=True, minio_healthy=True, detail="offline")


class _TraceAdapter:
    """Fake adapter whose only permitted query method is read-only shadow capture."""

    def __init__(self) -> None:
        vectors = {
            query_id: np.asarray((float(query_id + 1), 1.0), dtype="<f4")
            for query_id in range(200)
        }
        thresholds = {
            (metric, "target-025"): _radius(metric) for metric in Metric
        }
        names = {
            (metric, track): _names(metric, track)
            for metric in Metric
            for track in IndexTrack
        }
        bindings = {
            (metric, track): CollectionIdentityBinding(
                _binding_id(metric, track), _identity(metric, track)
            )
            for metric in Metric
            for track in IndexTrack
        }
        self.workload = ActuationWorkload(
            query_vectors=vectors,
            base_ids=np.asarray((0,), dtype=np.int64),
            base_vectors=np.asarray(((0.0, 0.0),), dtype="<f4"),
            threshold_radii=thresholds,
            collection_names=names,
            identity_bindings=bindings,
            configuration_identity=CONFIGURATION_ID,
            data_identity=DATA_ID,
        )
        self.client = _FakeClient()
        self.harness = _FakeHarness()
        self.stack_health_probe = _FakeHealth()
        self.shadow_trace_sink = None
        self.shadow_calls: list[tuple[ActuationContext, int, int]] = []
        self.action_calls: list[str] = []

    def shadow_candidate(
        self,
        *,
        context: ActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult:
        self.shadow_calls.append((context, candidate_ef, last_known_good_ef))
        assert self.shadow_trace_sink is not None
        metric = Metric(context.metric)
        score = 1.0 if metric is Metric.L2 else 0.75
        queries = tuple(
            ShadowQueryAuditTrace(
                query_id=query_id,
                query_vector=tuple(
                    float(item) for item in self.workload.query_vectors[query_id]
                ),
                threshold_radius=_radius(metric),
                range_filter=0.0 if metric is Metric.L2 else 1.0,
                limit=100,
                oracle_result=OracleResult(
                    hits=(OracleHit(query_id, score),), full_count=1, capped=False
                ),
                exact_cardinality=1,
                flat_hits=(SearchHit(query_id, score),),
                sentinel_hits=(SearchHit(query_id, score),),
                sentinel_recall=1.0,
                stages=(
                    ShadowAuditStageEvidence("ORACLE", success=True),
                    ShadowAuditStageEvidence(
                        "FLAT", success=True, oracle_agreement=True
                    ),
                    ShadowAuditStageEvidence("CANDIDATE_HNSW", success=True),
                    ShadowAuditStageEvidence(
                        "LAST_KNOWN_GOOD_HNSW", success=True
                    ),
                    ShadowAuditStageEvidence("SENTINEL_HNSW", success=True),
                ),
            )
            for query_id in context.audited_query_ids
        )
        trace = ShadowAuditTrace(
            metric=metric,
            threshold_stratum=context.threshold_stratum,
            candidate_ef=candidate_ef,
            last_known_good_ef=last_known_good_ef,
            sentinel_ef=100,
            configuration_identity=CONFIGURATION_ID,
            data_identity=DATA_ID,
            flat_identity=_trace_identity(metric, IndexTrack.FLAT),
            hnsw_identity=_trace_identity(metric, IndexTrack.HNSW),
            queries=queries,
            complete=True,
        )
        self.shadow_trace_sink.append(trace)
        return ShadowResult(True, 50, 0, 0, 0, True, True)

    def __getattr__(self, name: str) -> object:
        if name in {
            "start_canary",
            "stop_candidate",
            "restore_last_known_good",
            "verify_restoration",
        }:
            self.action_calls.append(name)
            raise AssertionError(f"automatic action called: {name}")
        raise AttributeError(name)


def _trace_identity(metric: Metric, track: IndexTrack) -> ShadowIdentityEvidence:
    stage = ShadowAuditStageEvidence(stage="IDENTITY", success=True)
    snapshot = _identity(metric, track)
    return ShadowIdentityEvidence(
        track=track,
        expected_binding_id=_binding_id(metric, track),
        pre_snapshot=snapshot,
        post_snapshot=snapshot,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=stage,
        post_capture=stage,
    )


class _ServingExecutor:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, request: RangeQueryRequest) -> ServedQueryOutcome:
        self.calls += 1
        return ServedQueryOutcome(True, False, 1, 0.1)


class _AuditSink:
    def __init__(self) -> None:
        self.records: list[MonitorAuditRecord] = []

    def contains(self, record_id: str) -> bool:
        return any(record.record_id == record_id for record in self.records)

    def append(self, record: MonitorAuditRecord) -> None:
        if self.contains(record.record_id):
            raise AssertionError("duplicate monitor audit record")
        self.records.append(record)


class _PolicyInputs:
    def __init__(self) -> None:
        self.calls: list[EvidenceProvenance] = []

    def resolve(self, *, decision, provenance: EvidenceProvenance) -> DryRunPolicyInputs:
        self.calls.append(provenance)
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
                response_model_provenance="exp008-offline-test",
            ),
            last_known_good=QualificationResult(
                qualified=False,
                ef=None,
                reasons=("EXP008_OFFLINE_NO_QUALIFICATION",),
            ),
            audit_id=f"exp008-offline:{provenance.metric.value}:{provenance.current_window_id}",
        )


def _request(metric: Metric, query_id: int) -> RangeQueryRequest:
    return RangeQueryRequest(
        request_id=query_id,
        stream_key=_stream_key(metric),
        query_vector=(float(query_id + 1), 1.0),
        threshold_radius=_radius(metric),
        range_filter=0.0 if metric is Metric.L2 else 1.0,
        limit=100,
        served_ef=400,
    )


class Exp008OfflineCompositionTests(unittest.TestCase):
    def test_two_stationary_host_streams_reach_no_drift_and_dry_run_no_change(self) -> None:
        adapter = _TraceAdapter()
        plans = {
            _stream_key(metric): HostShadowPlan(800, 400, 400)
            for metric in Metric
        }
        clock = _UtcStepClock()
        recorder = BoundedHostObservationRecorder(max_pending_observations=50)
        source_root: Path
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_root = root / "source"
            source = FileShadowTraceEventSource(
                source_root, max_pending_events=32, max_pending_bytes=2_000_000
            )
            worker = BackgroundShadowWorker(
                recorder=recorder,
                executor=MilvusHostShadowExecutor(
                    adapter=adapter, plans=plans, clock=clock
                ),
                publisher=source,
                state_store=InMemoryHostWorkerStateStore(),
                registered_trace_parameters=RegisteredTraceParameters(
                    allowed_candidate_and_lkg_efs=frozenset({400, 800}),
                    sentinel_ef=100,
                ),
                max_partial_streams=2,
                max_observation_age_seconds=60.0,
                clock=clock,
            )
            serving = _ServingExecutor()
            gateway = ReferenceRangeGateway(
                serving_executor=serving, recorder=recorder, clock=clock
            )

            for metric in Metric:
                for index in range(600):
                    response = gateway.execute(_request(metric, index % 200))
                    self.assertTrue(response.served_outcome.success)
                    self.assertEqual(response.observation_receipt.status.value, "ACCEPTED")
                    if (index + 1) % 50 == 0:
                        worker_result = worker.run_once(max_observations=50)
                        self.assertEqual(worker_result.published_trace_count, 1)
                        self.assertEqual(worker_result.reason_codes, ())

            self.assertEqual(serving.calls, 1200)
            self.assertEqual(len(adapter.shadow_calls), 24)
            self.assertEqual(len(tuple((source_root / "traces").glob("*.json"))), 24)
            self.assertEqual(len(source.poll(limit=32)), 24)

            provider = _PolicyInputs()
            audit = _AuditSink()
            monitor = WorkloadMonitor(
                source=source,
                state_store=FileMonitorStateStore(root / "monitor-state"),
                policy_input_provider=provider,
                audit_sink=audit,
                detector_seed=DETECTOR_SEED,
            )
            results = monitor.run_once(max_events=24)
            evaluated = [item for item in results if item.policy_decision is not None]

            self.assertEqual(len(results), 24)
            result_summary = [
                (
                    item.event_id,
                    item.reason_codes,
                    None if item.drift_decision is None else item.drift_decision.state,
                    None
                    if item.policy_decision is None
                    else item.policy_decision.action,
                )
                for item in results
            ]
            self.assertEqual(len(evaluated), 2, result_summary)
            self.assertEqual({item.drift_decision.state for item in evaluated}, {DetectorState.NO_DRIFT})
            self.assertEqual(
                {item.policy_decision.action for item in evaluated}, {PolicyAction.NO_CHANGE}
            )
            self.assertEqual(len(provider.calls), 2)
            self.assertEqual(source.poll(limit=1), ())
            self.assertEqual(adapter.action_calls, [])
            self.assertTrue(all(record.policy_action in {None, "NO_CHANGE"} for record in audit.records))


if __name__ == "__main__":
    unittest.main()
