from __future__ import annotations

from dataclasses import fields, replace
from pathlib import Path
import tempfile
import unittest

from tests.test_policy import decide
from tests.test_response_profile_milvus_adapter import (
    _FakeMilvusClient,
    _FakeStackHealthProbe,
)
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.drift import (
    DetectorState,
    DriftClassification,
    DriftDecision,
    build_evidence_provenance,
)
from vdbench.exp011_live_acquisition import run_exp011_live_acquisition
from vdbench.exp011_preparation import Exp011PreparationError, prepare_exp011_acquisition_inputs
from vdbench.host_observation import RangeQueryRequest, ServedQueryOutcome
from vdbench.host_window_detector_v2 import (
    SQLiteHostWindowDetectorV2Store,
    build_v2_shadow_position,
    build_v2_shadow_window,
)
from vdbench.host_window_lineage import (
    InjectedReadOnlyCaptureMetadataProvider,
    ReferenceV2Host,
    SQLiteHostResponseCommitStore,
    V2GenuineWorkloadObservationSource,
)
from vdbench.policy import PolicyAction, PolicyMode
from vdbench.response_profile import SUPPORTED_EFS
from vdbench.response_profile_semantic import build_response_profile_oracle_record
from vdbench.response_profile_v2_capture import (
    V2PopulationCaptureError,
    capture_v2_post_trigger_population,
)
from vdbench.shadow_event_types import MonitorStreamKey


class _ServingExecutor:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, _request: RangeQueryRequest) -> ServedQueryOutcome:
        self.calls += 1
        return ServedQueryOutcome(True, False, 0, 1.0)


class _StaticObservationSource:
    def __init__(self, observations) -> None:
        self.observations = observations
        self.acknowledged = False

    def poll(self, *, limit: int):
        return self.observations[:limit]

    def acknowledge(self, _event_ids) -> None:
        self.acknowledged = True


def _stream() -> MonitorStreamKey:
    return MonitorStreamKey(
        "v2-engine-stream", Metric.L2, "target-075", "v2-config",
        "v2-data", "v2-flat", "v2-hnsw",
    )


def _decision(reference, current, state: DetectorState) -> DriftDecision:
    provenance = build_evidence_provenance(
        metric=Metric.L2, threshold_stratum="target-075",
        reference_window_id=reference.window_sequence,
        current_window_id=current.window_sequence,
        reference_manifest_sha256=reference.source_window_sha256,
        current_manifest_sha256=current.source_window_sha256,
        configuration_identity="v2-config", data_identity="v2-data",
        flat_binding_id="v2-flat", hnsw_binding_id="v2-hnsw",
        reference_audit_ids=tuple(range(50)),
        reference_audit_rank_digests=tuple("a" * 64 for _ in range(50)),
        current_audit_ids=tuple(range(50, 100)),
        current_audit_rank_digests=tuple("b" * 64 for _ in range(50)),
    )
    return DriftDecision(
        state=state,
        classification=(
            DriftClassification.INPUT_DRIFT
            if state is DetectorState.DRIFT else DriftClassification.NONE
        ),
        reason_codes=(state.value,), evidence_provenance=provenance,
    )


def _shadow_window(records, sequence: int):
    sources = tuple(records[sequence * 200:(sequence + 1) * 200])
    positions = tuple(
        build_v2_shadow_position(
            source=item, evaluation_eligible=True,
            evaluation_evidence_sha256=f"{item.source_sequence + 1:064x}"[-64:],
        )
        for item in sources
    )
    return build_v2_shadow_window(sources=sources, positions=positions)


class V2OfflineEngineTests(unittest.TestCase):
    def test_full_structural_engine_and_b001_refusal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = InjectedReadOnlyCaptureMetadataProvider(
                lambda: {
                    "milvus_uri": "offline://no-service",
                    "deployment_identity": "offline-v2-engine",
                    "collection_name": "offline-v2-hnsw",
                    "dimensions": 1,
                    "metric": Metric.L2,
                    "hnsw_index_identity": "v2-hnsw",
                    "data_identity": "v2-data",
                    "source_revision": "revision/v2-engine",
                    "observed_at_utc": "2026-08-12T00:00:00Z",
                    "environment_manifest": {"engine": "structural-fake"},
                }
            ).capture()
            executor = _ServingExecutor()
            source_path = root / "source.sqlite3"
            with SQLiteHostResponseCommitStore(
                source_path, stream_key=_stream(),
                source_revision="revision/v2-engine",
                environment_manifest_sha256=metadata.environment_manifest_sha256,
            ) as source_store:
                host = ReferenceV2Host(
                    serving_executor=executor, response_store=source_store,
                    clock=lambda: "2026-08-12T00:00:00Z",
                )
                for index in range(2000):
                    visible = host.execute(
                        RangeQueryRequest(
                            request_id=index, stream_key=_stream(),
                            query_vector=(float(index + 1),),
                            threshold_radius=0.75, range_filter=0.0,
                            limit=100, served_ef=400,
                        )
                    )
                    self.assertEqual(visible.committed_observation.source_sequence, index)
                records = source_store.poll(consumer_id="shadow-v2", limit=2000)
                detector_path = root / "detector.sqlite3"
                with SQLiteHostWindowDetectorV2Store(detector_path, stream_key=_stream()) as detector:
                    detector.process_window(
                        window=_shadow_window(records, 0),
                        evaluator=lambda reference, current: _decision(
                            reference, current, DetectorState.NO_DRIFT
                        ),
                        persisted_at_utc="2026-08-12T00:00:01Z",
                    )
                    detector.process_window(
                        window=_shadow_window(records, 1),
                        evaluator=lambda reference, current: _decision(
                            reference, current, DetectorState.NO_DRIFT
                        ),
                        persisted_at_utc="2026-08-12T00:00:02Z",
                    )
                    detector.process_window(
                        window=_shadow_window(records, 2),
                        evaluator=lambda reference, current: _decision(
                            reference, current, DetectorState.DRIFT
                        ),
                        persisted_at_utc="2026-08-12T00:00:03Z",
                    )
                    trigger = detector.latest_drift_head()
                    self.assertIsNotNone(trigger)
                    source = V2GenuineWorkloadObservationSource(
                        store=source_store, consumer_id="exp010-v2",
                        clock=lambda: "2026-08-12T00:00:04Z",
                        start_source_sequence=600,
                    )
                    captured = capture_v2_post_trigger_population(
                        source=source, trigger_head=trigger,  # type: ignore[arg-type]
                        source_workload_manifest_sha256="c" * 64,
                        run_id="exp011-v2-structural", created_at_utc="2026-08-12T00:00:04Z",
                        source_revision="revision/v2-engine",
                    )
                    self.assertEqual(captured.warmup_window_sequence, 3)
                    self.assertEqual(captured.calibration_window_sequences, (4, 5, 6, 7, 8, 9))
                    self.assertEqual((captured.first_source_sequence, captured.last_source_sequence), (600, 1999))
                    replay = V2GenuineWorkloadObservationSource(
                        store=source_store, consumer_id="exp010-adversarial",
                        clock=lambda: "2026-08-12T00:00:04Z",
                        start_source_sequence=600,
                    ).poll(limit=1400)
                    for changed in (
                        (*replay[:1], replace(replay[1], source_sequence=999), *replay[2:]),
                        (*replay[:1], replace(replay[1], event_id=replay[0].event_id), *replay[2:]),
                        (*replay[:1], replace(replay[1], environment_manifest_sha256="0" * 64), *replay[2:]),
                    ):
                        invalid_source = _StaticObservationSource(tuple(changed))
                        with self.assertRaises(V2PopulationCaptureError):
                            capture_v2_post_trigger_population(
                                source=invalid_source,
                                trigger_head=trigger,  # type: ignore[arg-type]
                                source_workload_manifest_sha256="c" * 64,
                                run_id="invalid", created_at_utc="2026-08-12T00:00:04Z",
                                source_revision="revision/v2-engine",
                            )
                        self.assertFalse(invalid_source.acknowledged)
                    forged_head = object.__new__(type(trigger))
                    for item in fields(trigger):
                        object.__setattr__(
                            forged_head, item.name,
                            MonitorStreamKey(
                                "substituted", Metric.L2, "target-075", "v2-config",
                                "v2-data", "v2-flat", "v2-hnsw",
                            ) if item.name == "stream_key" else getattr(trigger, item.name),
                        )
                    with self.assertRaises(V2PopulationCaptureError):
                        capture_v2_post_trigger_population(
                            source=_StaticObservationSource(replay),
                            trigger_head=forged_head,
                            source_workload_manifest_sha256="c" * 64,
                            run_id="substituted", created_at_utc="2026-08-12T00:00:04Z",
                            source_revision="revision/v2-engine",
                        )
                    members = captured.population.calibration_role_manifest.members
                    oracle_records = tuple(
                        build_response_profile_oracle_record(
                            observation_identity_sha256=member.observation_identity.observation_identity_sha256,
                            query_id_sha256=member.query_identity.query_id_sha256,
                            query_payload_sha256=member.query_payload_identity.query_payload_sha256,
                            limit=member.query_payload_identity.limit,
                            full_count=0, capped_ids=(), capped_distances=(),
                            metric=Metric.L2,
                            radius=member.query_payload_identity.radius,
                            range_filter=member.query_payload_identity.range_filter,
                        )
                        for member in members
                    )
                    configurations = tuple(
                        SearchConfiguration(
                            Metric.L2, "target-075", 0.75, IndexTrack.HNSW, ef
                        )
                        for ef in SUPPORTED_EFS
                    )
                    prepared = prepare_exp011_acquisition_inputs(
                        output_dir=root / "prepared", monitor_store=detector,
                        stream_key=_stream(), population=captured.population,
                        warmup_role_manifest=captured.warmup_role_manifest,
                        oracle_records=oracle_records,
                        run_id="exp011-v2-prepared", created_at_utc="2026-08-12T00:00:04Z",
                        source_revision="revision/v2-engine",
                        search_configurations=configurations,
                        hnsw_index_identity="v2-hnsw", data_identity="v2-data",
                        environment_manifest_sha256=metadata.environment_manifest_sha256,
                        frozen_at_utc="2026-08-12T00:00:04Z",
                    )
                self.assertEqual(len(tuple(prepared.output_dir.iterdir())), 5)
                result = run_exp011_live_acquisition(
                    client=_FakeMilvusClient(search_response=[[]]),
                    stack_health_probe=_FakeStackHealthProbe(),
                    collection_name="offline-v2-hnsw", dimensions=1, metric=Metric.L2,
                    ledger_path=root / "lifecycle.sqlite3",
                    run_binding=prepared.run_binding,
                    static_identity=prepared.static_identity,
                    control=prepared.control,
                    oracle_manifest=prepared.oracle_manifest,
                    output_dir=root / "structural-acquisition",
                    evidence_status="STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE",
                )
                self.assertTrue(result.producer_complete)
                self.assertIsNotNone(result.profile)
                self.assertIsNotNone(result.root_pinned_capability)
                policy = decide(
                    mode=PolicyMode.CANARY_ENABLED,
                    threshold="target-075",
                    profile_authority=result.root_pinned_capability,
                )
                self.assertNotEqual(policy.action, PolicyAction.START_CANARY)
                b001_policy = decide(
                    mode=PolicyMode.CANARY_ENABLED,
                    profile_authority=result.root_pinned_capability,
                )
                self.assertEqual(b001_policy.action, PolicyAction.RECOMMEND_EF)
                self.assertEqual(
                    b001_policy.reason, "RESPONSE_PROFILE_AUTHORITY_UNAVAILABLE"
                )
            self.assertEqual(executor.calls, 2000)

    def test_adversarial_capture_and_preparation_bindings_fail_closed(self) -> None:
        class _WrongSource:
            def poll(self, *, limit: int):
                self.limit = limit
                return ()
            def acknowledge(self, _event_ids):
                raise AssertionError("must not acknowledge invalid capture")

        head = object.__new__(type("NotAHead", (), {}))
        with self.assertRaises(V2PopulationCaptureError):
            capture_v2_post_trigger_population(
                source=_WrongSource(), trigger_head=head,  # type: ignore[arg-type]
                source_workload_manifest_sha256="a" * 64,
                run_id="bad", created_at_utc="2026-08-12T00:00:00Z",
                source_revision="revision",
            )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with SQLiteHostWindowDetectorV2Store(root / "empty.sqlite3", stream_key=_stream()) as store:
                with self.assertRaises(Exp011PreparationError) as raised:
                    prepare_exp011_acquisition_inputs(
                        output_dir=root / "prepared", monitor_store=store,
                        stream_key=_stream(), population=object(),  # type: ignore[arg-type]
                        warmup_role_manifest=object(), oracle_records=(),  # type: ignore[arg-type]
                        run_id="bad", created_at_utc="2026-08-12T00:00:00Z",
                        source_revision="revision", search_configurations=(),
                        hnsw_index_identity="hnsw", data_identity="data",
                        environment_manifest_sha256="a" * 64,
                        frozen_at_utc="2026-08-12T00:00:01Z",
                    )
                self.assertEqual(raised.exception.code, "PREPARATION_INPUT_INVALID")


if __name__ == "__main__":
    unittest.main()
