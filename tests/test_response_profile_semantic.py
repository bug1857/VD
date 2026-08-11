from __future__ import annotations

from dataclasses import fields
import ast
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.drift import (
    DetectorState,
    DriftClassification,
    build_evidence_provenance,
)
from vdbench.response_profile import ResponseProfileIdentity, SUPPORTED_EFS
from vdbench.response_profile_control import build_response_profile_control
from vdbench.response_profile_detector_head import build_response_profile_detector_head
from vdbench.response_profile_monitor_store import ResponseProfileMonitorStateStore
from vdbench.response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    WARMUP_QUERY_COUNT,
    ResponseProfileRoleKind,
    build_artifact_source_namespace,
    build_calibration_population_manifest,
    build_canonical_query_identity,
    build_query_vector_identity,
    build_response_profile_cell,
    build_response_profile_query_payload,
    build_response_profile_replay_schedule,
    build_response_profile_role,
    build_response_profile_role_manifest,
    build_response_profile_role_member,
)
from vdbench.response_profile_lifecycle import (
    LifecycleEventKind,
    OpaqueEvidenceRole,
    build_opaque_evidence_blob,
    build_response_profile_lifecycle_event,
    build_response_profile_run_binding,
)
from vdbench.response_profile_semantic import (
    MeasuredResultOutcome,
    ResponseProfileSemanticBundle,
    ResponseProfileSemanticError,
    ResponseProfileSemanticExpectation,
    RuntimeSnapshotPhase,
    build_response_profile_oracle_manifest,
    build_response_profile_oracle_record,
    build_response_profile_identity_from_static,
    build_response_profile_semantic_encoder,
    build_response_profile_semantic_encoder_from_static,
    build_response_profile_static_identity,
    semantic_report_payload,
    verify_response_profile_semantic_bundle,
)
from vdbench.shadow_event_types import MonitorStreamKey
from vdbench.workload_monitor import MonitorStreamState


MODULE = Path(__file__).parents[1] / "src" / "vdbench" / "response_profile_semantic.py"


def _digest(character: str) -> str:
    return character * 64


def _flat_configuration() -> SearchConfiguration:
    return SearchConfiguration(
        metric=Metric.L2,
        threshold_label="target-075",
        radius=0.75,
        index_track=IndexTrack.FLAT,
        ef=None,
    )


def _hnsw_configurations() -> tuple[SearchConfiguration, ...]:
    return tuple(
        SearchConfiguration(
            metric=Metric.L2,
            threshold_label="target-075",
            radius=0.75,
            index_track=IndexTrack.HNSW,
            ef=ef,
        )
        for ef in SUPPORTED_EFS
    )


def _member(index: int, *, namespace: object, offset: float = 0.0):
    vector = build_query_vector_identity(
        np.asarray([float(index + 1) + offset], dtype="<f4")
    )
    return build_response_profile_role_member(
        source_namespace=namespace,
        query_identity=build_canonical_query_identity(index),
        vector_identity=vector,
        query_payload_identity=build_response_profile_query_payload(
            vector_identity=vector,
            search_configuration=_flat_configuration(),
        ),
    )


def _manifest(kind: ResponseProfileRoleKind, members: tuple[object, ...]):
    return build_response_profile_role_manifest(
        role=build_response_profile_role(kind=kind), members=members
    )


def _forge(value: object, **changes: object):
    forged = object.__new__(type(value))
    for field in fields(value):
        object.__setattr__(
            forged, field.name, changes.get(field.name, getattr(value, field.name))
        )
    return forged


class _SemanticFixture:
    def __init__(self, *, failed_position: int | None = None) -> None:
        namespace = build_artifact_source_namespace(
            dataset_id="DATASET-EXP010-SEMANTIC",
            dataset_version="v1",
            generation_manifest_sha256=_digest("a"),
        )
        calibration_members = tuple(
            _member(index, namespace=namespace)
            for index in range(CALIBRATION_QUERY_COUNT)
        )
        calibration_manifest = _manifest(
            ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION,
            calibration_members,
        )
        self.population = build_calibration_population_manifest(
            cell=build_response_profile_cell(
                metric=Metric.L2, threshold_stratum="target-075"
            ),
            calibration_role_manifest=calibration_manifest,
        )
        self.schedule = build_response_profile_replay_schedule(
            population=self.population, source_revision="revision/r2-c-v1"
        )
        warmup_members = tuple(
            _member(index + 20_000, namespace=namespace, offset=40_000.0)
            for index in range(WARMUP_QUERY_COUNT)
        )
        self.warmup = _manifest(
            ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP, warmup_members
        )
        self.binding = build_response_profile_run_binding(
            run_id="exp010-r2-c-fixture",
            created_at_utc="2026-08-10T00:00:00Z",
            population=self.population,
            replay_schedule=self.schedule,
            warmup_role_manifest=self.warmup,
            source_revision="revision/r2-c-v1",
        )
        self.identity = ResponseProfileIdentity(
            metric=Metric.L2,
            threshold_stratum="target-075",
            search_configurations=_hnsw_configurations(),
            hnsw_index_identity="hnsw-r2-c-v1",
            data_identity="data-r2-c-v1",
            workload_manifest_sha256=self.population.workload_manifest_sha256,
            ordered_query_payload_sha256=self.population.ordered_query_payload_sha256,
            replay_schedule_sha256=self.schedule.replay_schedule_sha256,
            control_profile_sha256=_digest("b"),
            environment_manifest_sha256=_digest("c"),
            source_revision="revision/r2-c-v1",
            calibration_started_at_utc="2026-08-10T00:00:02Z",
            calibration_completed_at_utc="2026-08-10T00:00:03Z",
            generated_at_utc="2026-08-10T00:00:04Z",
        )
        provenance = build_evidence_provenance(
            metric=Metric.L2,
            threshold_stratum="target-075",
            reference_window_id="detector-reference",
            current_window_id="detector-current",
            reference_manifest_sha256=_digest("d"),
            current_manifest_sha256=_digest("e"),
            configuration_identity="detector-configuration-r2-c-v1",
            data_identity=self.identity.data_identity,
            flat_binding_id="flat-r2-c-v1",
            hnsw_binding_id=self.identity.hnsw_index_identity,
            reference_audit_ids=tuple(range(50)),
            reference_audit_rank_digests=tuple(_digest("1") for _ in range(50)),
            current_audit_ids=tuple(range(50, 100)),
            current_audit_rank_digests=tuple(_digest("2") for _ in range(50)),
        )
        stream_key = MonitorStreamKey(
                "detector-stream-r2-c-v1",
                Metric.L2,
                "target-075",
                "detector-configuration-r2-c-v1",
                self.identity.data_identity,
                "flat-r2-c-v1",
                self.identity.hnsw_index_identity,
            )
        detector_head = build_response_profile_detector_head(
            stream_key=stream_key,
            window_sequence=2,
            detector_state=DetectorState.NO_DRIFT,
            detector_classification=DriftClassification.NONE,
            detector_provenance=provenance,
        )
        self.monitor_directory = tempfile.TemporaryDirectory()
        head_times = iter(("2026-08-09T23:59:58Z", "2026-08-09T23:59:58.500000Z"))
        with patch(
            "vdbench.response_profile_monitor_store.secrets.token_hex",
            return_value=_digest("6"),
        ):
            self.monitor_store = ResponseProfileMonitorStateStore(
                Path(self.monitor_directory.name) / "monitor.sqlite3",
                expected_stream_key=stream_key,
                utc_now=lambda: next(head_times),
            )
        self.monitor_store.save(
            MonitorStreamState(
                stream_key=stream_key,
                next_window_sequence=3,
                latest_detector_head=detector_head,
            )
        )
        self.latest_detector_head = self.monitor_store.load_verified_latest(stream_key)
        assert self.latest_detector_head is not None
        self.control = build_response_profile_control(
            stream_key=stream_key,
            detector_provenance=provenance,
            trigger_window_sequence=2,
            detector_head_sha256=detector_head.detector_head_sha256,
            detector_head_record_sequence=self.latest_detector_head.head_record_sequence,
            detector_head_record_sha256=self.latest_detector_head.head_record_sha256,
            detector_head_persisted_at_utc=(
                self.latest_detector_head.head_record_persisted_at_utc
            ),
            calibration_population_sha256=self.population.workload_manifest_sha256,
            warmup_role_manifest_sha256=self.warmup.role_manifest_sha256,
            ordered_query_payload_sha256=self.population.ordered_query_payload_sha256,
            replay_schedule_sha256=self.schedule.replay_schedule_sha256,
            environment_manifest_sha256=self.identity.environment_manifest_sha256,
            source_revision=self.identity.source_revision,
            frozen_at_utc="2026-08-09T23:59:59Z",
        )
        self.identity = ResponseProfileIdentity(
            **{
                item.name: (
                    self.control.control_profile_sha256
                    if item.name == "control_profile_sha256"
                    else getattr(self.identity, item.name)
                )
                for item in fields(self.identity)
            }
        )
        encoder = build_response_profile_semantic_encoder(
            run_binding=self.binding, identity=self.identity
        )
        records = tuple(
            build_response_profile_oracle_record(
                observation_identity_sha256=member.observation_identity.observation_identity_sha256,
                query_id_sha256=member.query_identity.query_id_sha256,
                query_payload_sha256=member.query_payload_identity.query_payload_sha256,
                limit=member.query_payload_identity.limit,
                full_count=0,
                capped_ids=(),
                capped_distances=(),
                metric=Metric.L2,
                radius=member.query_payload_identity.radius,
                range_filter=member.query_payload_identity.range_filter,
            )
            for member in calibration_members
        )
        self.oracle = build_response_profile_oracle_manifest(
            population=self.population, records=records
        )
        self.events: list[object] = []
        self.blobs: list[object] = []

        def add_event(
            kind: LifecycleEventKind,
            *,
            epoch: int | None,
            block: int | None,
            position: int | None,
            data: dict[str, object],
            timestamp: str,
        ):
            event = build_response_profile_lifecycle_event(
                run_binding_sha256=self.binding.run_binding_sha256,
                event_seq=len(self.events),
                event_kind=kind,
                epoch_index=epoch,
                block_index=block,
                position_index=position,
                recorded_at_utc=timestamp,
                event_data=data,
                previous_event_sha256=(
                    self.binding.run_binding_sha256
                    if not self.events
                    else self.events[-1].lifecycle_event_sha256
                ),
            )
            self.events.append(event)
            return event

        def add_blob(role: OpaqueEvidenceRole, evidence_bytes: bytes):
            blob = build_opaque_evidence_blob(
                run_binding_sha256=self.binding.run_binding_sha256,
                event_seq=len(self.events),
                evidence_role=role,
                evidence_bytes=evidence_bytes,
            )
            self.blobs.append(blob)
            return blob

        add_event(
            LifecycleEventKind.EPOCH_STARTED,
            epoch=0,
            block=None,
            position=None,
            data={},
            timestamp="2026-08-10T00:00:00Z",
        )
        warmup_blob = add_blob(
            OpaqueEvidenceRole.WARMUP_EXECUTION,
            encoder.warmup_execution(epoch_index=0),
        )
        add_event(
            LifecycleEventKind.WARMUP_COMPLETED,
            epoch=0,
            block=None,
            position=None,
            data={
                "warmup_role_manifest_sha256": self.warmup.role_manifest_sha256,
                "warmup_execution_blob_sha256": warmup_blob.opaque_evidence_sha256,
            },
            timestamp="2026-08-10T00:00:01Z",
        )
        for block in self.schedule.blocks:
            pre_blob = add_blob(
                OpaqueEvidenceRole.PRE_BLOCK_RUNTIME_SNAPSHOT,
                encoder.runtime_snapshot(
                    epoch_index=0,
                    block_index=block.block_index,
                    phase=RuntimeSnapshotPhase.PRE_BLOCK,
                    observed_at_utc="2026-08-10T00:00:01Z",
                ),
            )
            block_started = add_event(
                LifecycleEventKind.BLOCK_STARTED,
                epoch=0,
                block=block.block_index,
                position=None,
                data={
                    "pre_block_runtime_snapshot_blob_sha256": pre_blob.opaque_evidence_sha256
                },
                timestamp="2026-08-10T00:00:01Z",
            )
            completions = []
            for position in block.positions:
                started_ns = position.position_index * 2_000_000
                started = add_event(
                    LifecycleEventKind.MEASUREMENT_STARTED,
                    epoch=0,
                    block=block.block_index,
                    position=position.position_index,
                    data={
                        "within_block_index": position.within_block_index,
                        "canonical_query_index": position.canonical_query_index,
                        "query_id": position.query_id,
                        "query_id_sha256": position.query_id_sha256,
                        "observation_identity_sha256": position.observation_identity_sha256,
                        "ef": position.ef,
                        "started_monotonic_ns": started_ns,
                    },
                    timestamp="2026-08-10T00:00:02Z",
                )
                oracle = records[position.canonical_query_index]
                failed = position.position_index == failed_position
                outcome = (
                    MeasuredResultOutcome.FAILED
                    if failed
                    else MeasuredResultOutcome.SUCCESS
                )
                result_blob = add_blob(
                    OpaqueEvidenceRole.MEASURED_RESULT,
                    encoder.measured_result(
                        epoch_index=0,
                        block_index=block.block_index,
                        position_index=position.position_index,
                        measurement_started_event_sha256=started.lifecycle_event_sha256,
                        observation_identity_sha256=position.observation_identity_sha256,
                        query_id_sha256=position.query_id_sha256,
                        query_payload_sha256=calibration_members[
                            position.canonical_query_index
                        ].query_payload_identity.query_payload_sha256,
                        ef=position.ef,
                        oracle_record_sha256=oracle.oracle_record_sha256,
                        outcome=outcome,
                        candidate_ids=(),
                        candidate_distances=(),
                        failure_code="SEARCH_FAILED" if failed else None,
                    ),
                )
                completions.append(
                    add_event(
                        LifecycleEventKind.MEASUREMENT_COMPLETED,
                        epoch=0,
                        block=block.block_index,
                        position=position.position_index,
                        data={
                            "measurement_started_event_sha256": started.lifecycle_event_sha256,
                            "measured_result_blob_sha256": result_blob.opaque_evidence_sha256,
                            "completed_monotonic_ns": started_ns + 1_000_000,
                        },
                        timestamp="2026-08-10T00:00:03Z",
                    )
                )
            post_blob = add_blob(
                OpaqueEvidenceRole.POST_BLOCK_RUNTIME_SNAPSHOT,
                encoder.runtime_snapshot(
                    epoch_index=0,
                    block_index=block.block_index,
                    phase=RuntimeSnapshotPhase.POST_BLOCK,
                    observed_at_utc="2026-08-10T00:00:03Z",
                ),
            )
            add_event(
                LifecycleEventKind.BLOCK_CLOSED,
                epoch=0,
                block=block.block_index,
                position=None,
                data={
                    "block_started_event_sha256": block_started.lifecycle_event_sha256,
                    "measurement_completed_event_sha256": [
                        item.lifecycle_event_sha256 for item in completions
                    ],
                    "post_block_runtime_snapshot_blob_sha256": post_blob.opaque_evidence_sha256,
                },
                timestamp="2026-08-10T00:00:03Z",
            )
        self.bundle = ResponseProfileSemanticBundle(
            calibration_population=self.population,
            warmup_role_manifest=self.warmup,
            replay_schedule=self.schedule,
            run_binding=self.binding,
            events=tuple(self.events),
            opaque_evidence=tuple(self.blobs),
            oracle_manifest=self.oracle,
            control=self.control,
        )
        self.expectation = ResponseProfileSemanticExpectation(
            profile_identity=self.identity,
            expected_oracle_manifest_sha256=self.oracle.oracle_manifest_sha256,
        )

    def close(self) -> None:
        self.monitor_store.close()
        self.monitor_directory.cleanup()


class ResponseProfileSemanticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = _SemanticFixture()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.fixture.close()

    def test_complete_bundle_recomputes_1200_query_observations(self) -> None:
        result = verify_response_profile_semantic_bundle(
            bundle=self.fixture.bundle, expectation=self.fixture.expectation
        )
        self.assertTrue(result.report.complete)
        self.assertEqual(len(result.report.observations), 1200)
        self.assertEqual(result.report.completed_position_count, 4800)
        self.assertTrue(all(len(item.responses) == 4 for item in result.report.observations))
        self.assertTrue(
            all(
                response.capped_recall == 1.0 and response.latency_ms == 1.0
                for item in result.report.observations
                for response in item.responses
            )
        )

    def test_verification_is_deterministic(self) -> None:
        first = verify_response_profile_semantic_bundle(
            bundle=self.fixture.bundle, expectation=self.fixture.expectation
        )
        second = verify_response_profile_semantic_bundle(
            bundle=self.fixture.bundle, expectation=self.fixture.expectation
        )
        self.assertEqual(first, second)

    def test_static_runtime_identity_needs_no_planned_calibration_timestamps(self) -> None:
        identity = self.fixture.identity
        static = build_response_profile_static_identity(
            metric=identity.metric,
            threshold_stratum=identity.threshold_stratum,
            search_configurations=identity.search_configurations,
            hnsw_index_identity=identity.hnsw_index_identity,
            data_identity=identity.data_identity,
            workload_manifest_sha256=identity.workload_manifest_sha256,
            ordered_query_payload_sha256=identity.ordered_query_payload_sha256,
            replay_schedule_sha256=identity.replay_schedule_sha256,
            control_profile_sha256=identity.control_profile_sha256,
            environment_manifest_sha256=identity.environment_manifest_sha256,
            source_revision=identity.source_revision,
        )
        static_encoder = build_response_profile_semantic_encoder_from_static(
            run_binding=self.fixture.binding, static_identity=static
        )
        final_encoder = build_response_profile_semantic_encoder(
            run_binding=self.fixture.binding, identity=identity
        )
        self.assertEqual(
            static_encoder.warmup_execution(epoch_index=0),
            final_encoder.warmup_execution(epoch_index=0),
        )
        rebuilt = build_response_profile_identity_from_static(
            static_identity=static,
            calibration_started_at_utc=identity.calibration_started_at_utc,
            calibration_completed_at_utc=identity.calibration_completed_at_utc,
            generated_at_utc=identity.generated_at_utc,
        )
        self.assertEqual(rebuilt, identity)
        forged = _forge(static, search_configurations=list(static.search_configurations))
        with self.assertRaises(ResponseProfileSemanticError):
            build_response_profile_semantic_encoder_from_static(
                run_binding=self.fixture.binding, static_identity=forged
            )

    def test_full_contract_golden_digests_are_frozen(self) -> None:
        result = verify_response_profile_semantic_bundle(
            bundle=self.fixture.bundle, expectation=self.fixture.expectation
        )
        self.assertEqual(
            self.fixture.oracle.oracle_manifest_sha256,
            "9b17641086b7f6b94ad4909f752f70e6328d36f4002907be16272b678fb53eba",
        )
        self.assertEqual(
            result.report.semantic_report_sha256,
            "9c34a2fe0f9cea83b5c76682f051738862e41b230e5f72034e4b3ab1c14018e2",
        )
        self.assertEqual(
            result.raw_evidence_sha256,
            "e88ed05c5e961a21cfe768b872f0cb721458713d5b755c0da5501f0bd32a05d2",
        )

    def test_object_forged_semantic_report_constants_fail_reconstruction(self) -> None:
        result = verify_response_profile_semantic_bundle(
            bundle=self.fixture.bundle, expectation=self.fixture.expectation
        )
        forged = _forge(result.report, complete=False)
        with self.assertRaises(ResponseProfileSemanticError) as raised:
            semantic_report_payload(forged)
        self.assertEqual(raised.exception.code, "SEMANTIC_REPORT_INVALID")

    def test_independent_oracle_root_mismatch_fails_closed(self) -> None:
        expectation = ResponseProfileSemanticExpectation(
            profile_identity=self.fixture.identity,
            expected_oracle_manifest_sha256=_digest("f"),
        )
        with self.assertRaises(ResponseProfileSemanticError) as raised:
            verify_response_profile_semantic_bundle(
                bundle=self.fixture.bundle, expectation=expectation
            )
        self.assertEqual(raised.exception.code, "ORACLE_ROOT_MISMATCH")

    def test_control_profile_mismatch_and_late_freeze_fail_closed(self) -> None:
        with self.assertRaises(ResponseProfileSemanticError) as raised:
            verify_response_profile_semantic_bundle(
                bundle=_forge(
                    self.fixture.bundle,
                    control=_forge(
                        self.fixture.control,
                        control_profile_sha256=_digest("0"),
                    ),
                ),
                expectation=self.fixture.expectation,
            )
        self.assertEqual(raised.exception.code, "CONTROL_PROFILE_INVALID")

        late = build_response_profile_control(
            stream_key=self.fixture.control.stream_key,
            detector_provenance=self.fixture.control.detector_provenance,
            trigger_window_sequence=self.fixture.control.trigger_window_sequence,
            detector_head_sha256=self.fixture.control.detector_head_sha256,
            detector_head_record_sequence=(
                self.fixture.control.detector_head_record_sequence
            ),
            detector_head_record_sha256=(
                self.fixture.control.detector_head_record_sha256
            ),
            detector_head_persisted_at_utc=(
                self.fixture.control.detector_head_persisted_at_utc
            ),
            calibration_population_sha256=self.fixture.control.calibration_population_sha256,
            warmup_role_manifest_sha256=self.fixture.control.warmup_role_manifest_sha256,
            ordered_query_payload_sha256=self.fixture.control.ordered_query_payload_sha256,
            replay_schedule_sha256=self.fixture.control.replay_schedule_sha256,
            environment_manifest_sha256=self.fixture.control.environment_manifest_sha256,
            source_revision=self.fixture.control.source_revision,
            frozen_at_utc="2026-08-10T00:00:02.000001Z",
        )
        late_identity = ResponseProfileIdentity(
            **{
                item.name: (
                    late.control_profile_sha256
                    if item.name == "control_profile_sha256"
                    else getattr(self.fixture.identity, item.name)
                )
                for item in fields(self.fixture.identity)
            }
        )
        with self.assertRaises(ResponseProfileSemanticError) as raised:
            verify_response_profile_semantic_bundle(
                bundle=_forge(self.fixture.bundle, control=late),
                expectation=ResponseProfileSemanticExpectation(
                    profile_identity=late_identity,
                    expected_oracle_manifest_sha256=self.fixture.oracle.oracle_manifest_sha256,
                ),
            )
        self.assertEqual(raised.exception.code, "CONTROL_PROFILE_LATE")

    def test_object_forged_bundle_collection_type_fails_closed(self) -> None:
        forged = _forge(self.fixture.bundle, events=list(self.fixture.bundle.events))
        with self.assertRaises(ResponseProfileSemanticError):
            verify_response_profile_semantic_bundle(
                bundle=forged, expectation=self.fixture.expectation
            )

    def test_failed_measured_search_fails_semantic_completion(self) -> None:
        failed = _SemanticFixture(failed_position=0)
        self.addCleanup(failed.close)
        with self.assertRaises(ResponseProfileSemanticError) as raised:
            verify_response_profile_semantic_bundle(
                bundle=failed.bundle, expectation=failed.expectation
            )
        self.assertEqual(raised.exception.code, "MEASURED_SEARCH_FAILED")

    def test_oracle_result_rejects_bool_and_nonfinite_values(self) -> None:
        member = self.fixture.population.calibration_role_manifest.members[0]
        for ids, distances in (((True,), (0.1,)), ((1,), (float("nan"),))):
            with self.subTest(ids=ids, distances=distances):
                with self.assertRaises(ResponseProfileSemanticError):
                    build_response_profile_oracle_record(
                        observation_identity_sha256=member.observation_identity.observation_identity_sha256,
                        query_id_sha256=member.query_identity.query_id_sha256,
                        query_payload_sha256=member.query_payload_identity.query_payload_sha256,
                        limit=100,
                        full_count=1,
                        capped_ids=ids,
                        capped_distances=distances,
                        metric=Metric.L2,
                        radius=0.75,
                        range_filter=0.0,
                    )

    def test_module_has_no_candidate_authority_or_live_dependencies(self) -> None:
        tree = ast.parse(MODULE.read_text(encoding="utf-8"))
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
            "policy",
            "canary_admission",
            "canary_live_runner",
            "lkg_phase3_authority",
            "actuation",
            "pymilvus",
        }
        self.assertFalse(
            {
                item
                for item in imported
                if any(item == name or item.endswith(f".{name}") for name in forbidden)
            }
        )


if __name__ == "__main__":
    unittest.main()
