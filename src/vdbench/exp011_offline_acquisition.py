"""EXP-011 offline structural scenario runner.

Purpose:
    Exercise the real ADR-010 boundary (`ResponseProfileMonitorStateStore`,
    `ResponseProfileLifecycleLedger`, `bind_fresh_response_profile_evidence`/
    `verify_fresh_response_profile_evidence`) against every offline-composable
    scenario preregistered in EXPERIMENT_LOG.md's EXP-011 entry, using
    deterministic locally-defined fake query-execution and runtime-probe
    ports.
Inputs:
    None external. Builds its own real, in-memory fixture objects via the
    same production builder functions the rest of the response-profile track
    uses; never reads or writes anything outside its own ``output_dir``.
Outputs:
    One canonical JSON evidence document per scenario plus a summary
    manifest, each carrying ``evidence_status:
    "STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE"``.
Dependencies:
    No PyMilvus import, no network call, no ``policy`` import. This module
    never contacts Milvus and never touches or lifts the B-001 interlock.
Failure modes:
    An individual scenario's assertions failing is recorded as
    ``passed=False`` in its own evidence document (structural evidence, not a
    silent pass); only genuinely unexpected construction failures raise
    ``Exp011OfflineError``.

This module produces STRUCTURAL/OFFLINE evidence only. It is not a run of
EXP-011 and does not supply real prospective evidence -- see
EXPERIMENT_LOG.md's EXP-011 entry for the still-required live protocol.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np

from .artifacts import canonical_json_bytes, write_immutable_json
from .config import IndexTrack, Metric, SearchConfiguration
from .drift import (
    DetectorState,
    DriftClassification,
    EvidenceProvenance,
    build_evidence_provenance,
)
from .response_profile import SUPPORTED_EFS, ResponseProfileIdentity
from .response_profile_control import build_response_profile_control
from .response_profile_detector_head import build_response_profile_detector_head
from .response_profile_evidence import (
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
from .response_profile_freshness import (
    FreshResponseProfileEvidence,
    ResponseProfileFreshnessError,
    bind_fresh_response_profile_evidence,
    verify_fresh_response_profile_evidence,
)
from .response_profile_lifecycle import (
    LifecycleEventKind,
    OpaqueEvidenceRole,
    build_opaque_evidence_blob,
    build_response_profile_lifecycle_event,
    build_response_profile_run_binding,
)
from .response_profile_monitor_store import (
    ResponseProfileMonitorStateStore,
    ResponseProfileMonitorStoreError,
)
from .response_profile_projection import project_root_pinned_response_profile
from .response_profile_root_pin import issue_root_pinned_response_profile_evidence
from .response_profile_semantic import (
    MeasuredResultOutcome,
    ResponseProfileSemanticBundle,
    ResponseProfileSemanticExpectation,
    RuntimeSnapshotPhase,
    build_response_profile_oracle_manifest,
    build_response_profile_oracle_record,
    build_response_profile_semantic_encoder,
    verify_response_profile_semantic_bundle,
)
from .shadow_event_types import MonitorStreamKey
from .workload_monitor import MonitorStreamState

__all__ = [
    "EVIDENCE_STATUS",
    "Exp011OfflineError",
    "Exp011OfflineResult",
    "Exp011OfflineScenarioResult",
    "main",
    "run_exp011_offline",
]

EVIDENCE_STATUS = "STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE"


class Exp011OfflineError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Exp011OfflineScenarioResult:
    scenario_id: str
    passed: bool
    reason_codes: tuple[str, ...]
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class Exp011OfflineResult:
    output_dir: Path
    scenarios: tuple[Exp011OfflineScenarioResult, ...]
    evidence_status: str = EVIDENCE_STATUS


def _default_utc_now() -> str:
    now = datetime.now(UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"


def _digest(character: str) -> str:
    return character * 64


def _forge(value: object, **changes: object) -> object:
    """Construct an alternate wrapper without re-running its own builder.

    Used only to build adversarial inputs (a tampered field the object's own
    constructor would refuse) so a *downstream* cross-object check can be
    exercised in isolation. Confined to this offline scenario module, mirrors
    the identical technique already reviewed in
    ``tests/test_response_profile_freshness.py``.
    """

    forged = object.__new__(type(value))
    for field in fields(value):
        object.__setattr__(
            forged, field.name, changes.get(field.name, getattr(value, field.name))
        )
    return forged


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


@dataclass
class _FastFixture:
    """One complete, real, internally consistent response-profile lineage.

    Built the same way ``tests/test_response_profile_semantic.py``'s
    ``_SemanticFixture`` is built (same real builder calls, same shape) so
    this module never reimplements calibration/lifecycle/semantic logic --
    it only assembles already-reviewed real objects for scenario use.
    """

    stream_key: MonitorStreamKey
    control: object
    identity: ResponseProfileIdentity
    capability: object
    profile: object
    monitor_store: ResponseProfileMonitorStateStore
    latest: object
    fresh: FreshResponseProfileEvidence

    def close(self) -> None:
        self.monitor_store.close()


def _build_fast_fixture(*, store_path: Path) -> _FastFixture:
    namespace = build_artifact_source_namespace(
        dataset_id="DATASET-EXP011-OFFLINE",
        dataset_version="v1",
        generation_manifest_sha256=_digest("a"),
    )
    calibration_members = tuple(
        _member(index, namespace=namespace) for index in range(CALIBRATION_QUERY_COUNT)
    )
    calibration_manifest = _manifest(
        ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION, calibration_members
    )
    population = build_calibration_population_manifest(
        cell=build_response_profile_cell(metric=Metric.L2, threshold_stratum="target-075"),
        calibration_role_manifest=calibration_manifest,
    )
    schedule = build_response_profile_replay_schedule(
        population=population, source_revision="revision/exp011-offline-v1"
    )
    warmup_members = tuple(
        _member(index + 20_000, namespace=namespace, offset=40_000.0)
        for index in range(WARMUP_QUERY_COUNT)
    )
    warmup = _manifest(ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP, warmup_members)
    binding = build_response_profile_run_binding(
        run_id="exp011-offline-fixture",
        created_at_utc="2026-08-11T00:00:00Z",
        population=population,
        replay_schedule=schedule,
        warmup_role_manifest=warmup,
        source_revision="revision/exp011-offline-v1",
    )
    identity = ResponseProfileIdentity(
        metric=Metric.L2,
        threshold_stratum="target-075",
        search_configurations=_hnsw_configurations(),
        hnsw_index_identity="hnsw-exp011-offline-v1",
        data_identity="data-exp011-offline-v1",
        workload_manifest_sha256=population.workload_manifest_sha256,
        ordered_query_payload_sha256=population.ordered_query_payload_sha256,
        replay_schedule_sha256=schedule.replay_schedule_sha256,
        control_profile_sha256=_digest("b"),
        environment_manifest_sha256=_digest("c"),
        source_revision="revision/exp011-offline-v1",
        calibration_started_at_utc="2026-08-11T00:00:02Z",
        calibration_completed_at_utc="2026-08-11T00:00:03Z",
        generated_at_utc="2026-08-11T00:00:04Z",
    )
    provenance = build_evidence_provenance(
        metric=Metric.L2,
        threshold_stratum="target-075",
        reference_window_id="exp011-detector-reference",
        current_window_id="exp011-detector-current",
        reference_manifest_sha256=_digest("d"),
        current_manifest_sha256=_digest("e"),
        configuration_identity="exp011-detector-configuration-v1",
        data_identity=identity.data_identity,
        flat_binding_id="flat-exp011-offline-v1",
        hnsw_binding_id=identity.hnsw_index_identity,
        reference_audit_ids=tuple(range(50)),
        reference_audit_rank_digests=tuple(_digest("1") for _ in range(50)),
        current_audit_ids=tuple(range(50, 100)),
        current_audit_rank_digests=tuple(_digest("2") for _ in range(50)),
    )
    stream_key = MonitorStreamKey(
        "exp011-offline-stream-v1",
        Metric.L2,
        "target-075",
        "exp011-detector-configuration-v1",
        identity.data_identity,
        "flat-exp011-offline-v1",
        identity.hnsw_index_identity,
    )
    detector_head = build_response_profile_detector_head(
        stream_key=stream_key,
        window_sequence=2,
        detector_state=DetectorState.NO_DRIFT,
        detector_classification=DriftClassification.NONE,
        detector_provenance=provenance,
    )
    head_times = iter(("2026-08-10T23:59:58Z", "2026-08-10T23:59:58.500000Z"))
    with patch(
        "vdbench.response_profile_monitor_store.secrets.token_hex",
        return_value=_digest("6"),
    ):
        monitor_store = ResponseProfileMonitorStateStore(
            store_path, expected_stream_key=stream_key, utc_now=lambda: next(head_times)
        )
    monitor_store.save(
        MonitorStreamState(
            stream_key=stream_key, next_window_sequence=3, latest_detector_head=detector_head
        )
    )
    latest = monitor_store.load_verified_latest(stream_key)
    assert latest is not None
    control = build_response_profile_control(
        stream_key=stream_key,
        detector_provenance=provenance,
        trigger_window_sequence=2,
        detector_head_sha256=detector_head.detector_head_sha256,
        detector_head_record_sequence=latest.head_record_sequence,
        detector_head_record_sha256=latest.head_record_sha256,
        detector_head_persisted_at_utc=latest.head_record_persisted_at_utc,
        calibration_population_sha256=population.workload_manifest_sha256,
        warmup_role_manifest_sha256=warmup.role_manifest_sha256,
        ordered_query_payload_sha256=population.ordered_query_payload_sha256,
        replay_schedule_sha256=schedule.replay_schedule_sha256,
        environment_manifest_sha256=identity.environment_manifest_sha256,
        source_revision=identity.source_revision,
        frozen_at_utc="2026-08-10T23:59:59Z",
    )
    identity = ResponseProfileIdentity(
        **{
            item.name: (
                control.control_profile_sha256
                if item.name == "control_profile_sha256"
                else getattr(identity, item.name)
            )
            for item in fields(identity)
        }
    )
    encoder = build_response_profile_semantic_encoder(run_binding=binding, identity=identity)
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
    oracle = build_response_profile_oracle_manifest(population=population, records=records)

    events: list[object] = []
    blobs: list[object] = []

    def add_event(kind, *, epoch, block, position, data, timestamp):
        event = build_response_profile_lifecycle_event(
            run_binding_sha256=binding.run_binding_sha256,
            event_seq=len(events),
            event_kind=kind,
            epoch_index=epoch,
            block_index=block,
            position_index=position,
            recorded_at_utc=timestamp,
            event_data=data,
            previous_event_sha256=(
                binding.run_binding_sha256 if not events else events[-1].lifecycle_event_sha256
            ),
        )
        events.append(event)
        return event

    def add_blob(role, evidence_bytes):
        blob = build_opaque_evidence_blob(
            run_binding_sha256=binding.run_binding_sha256,
            event_seq=len(events),
            evidence_role=role,
            evidence_bytes=evidence_bytes,
        )
        blobs.append(blob)
        return blob

    add_event(
        LifecycleEventKind.EPOCH_STARTED,
        epoch=0, block=None, position=None, data={}, timestamp="2026-08-11T00:00:00Z",
    )
    warmup_blob = add_blob(
        OpaqueEvidenceRole.WARMUP_EXECUTION, encoder.warmup_execution(epoch_index=0)
    )
    add_event(
        LifecycleEventKind.WARMUP_COMPLETED,
        epoch=0, block=None, position=None,
        data={
            "warmup_role_manifest_sha256": warmup.role_manifest_sha256,
            "warmup_execution_blob_sha256": warmup_blob.opaque_evidence_sha256,
        },
        timestamp="2026-08-11T00:00:01Z",
    )
    for block in schedule.blocks:
        pre_blob = add_blob(
            OpaqueEvidenceRole.PRE_BLOCK_RUNTIME_SNAPSHOT,
            encoder.runtime_snapshot(
                epoch_index=0, block_index=block.block_index,
                phase=RuntimeSnapshotPhase.PRE_BLOCK, observed_at_utc="2026-08-11T00:00:01Z",
            ),
        )
        block_started = add_event(
            LifecycleEventKind.BLOCK_STARTED,
            epoch=0, block=block.block_index, position=None,
            data={"pre_block_runtime_snapshot_blob_sha256": pre_blob.opaque_evidence_sha256},
            timestamp="2026-08-11T00:00:01Z",
        )
        completions = []
        for position in block.positions:
            started_ns = position.position_index * 2_000_000
            started = add_event(
                LifecycleEventKind.MEASUREMENT_STARTED,
                epoch=0, block=block.block_index, position=position.position_index,
                data={
                    "within_block_index": position.within_block_index,
                    "canonical_query_index": position.canonical_query_index,
                    "query_id": position.query_id,
                    "query_id_sha256": position.query_id_sha256,
                    "observation_identity_sha256": position.observation_identity_sha256,
                    "ef": position.ef,
                    "started_monotonic_ns": started_ns,
                },
                timestamp="2026-08-11T00:00:02Z",
            )
            oracle_record = records[position.canonical_query_index]
            result_blob = add_blob(
                OpaqueEvidenceRole.MEASURED_RESULT,
                encoder.measured_result(
                    epoch_index=0, block_index=block.block_index,
                    position_index=position.position_index,
                    measurement_started_event_sha256=started.lifecycle_event_sha256,
                    observation_identity_sha256=position.observation_identity_sha256,
                    query_id_sha256=position.query_id_sha256,
                    query_payload_sha256=calibration_members[
                        position.canonical_query_index
                    ].query_payload_identity.query_payload_sha256,
                    ef=position.ef,
                    oracle_record_sha256=oracle_record.oracle_record_sha256,
                    outcome=MeasuredResultOutcome.SUCCESS,
                    candidate_ids=(),
                    candidate_distances=(),
                    failure_code=None,
                ),
            )
            completions.append(
                add_event(
                    LifecycleEventKind.MEASUREMENT_COMPLETED,
                    epoch=0, block=block.block_index, position=position.position_index,
                    data={
                        "measurement_started_event_sha256": started.lifecycle_event_sha256,
                        "measured_result_blob_sha256": result_blob.opaque_evidence_sha256,
                        "completed_monotonic_ns": started_ns + 1_000_000,
                    },
                    timestamp="2026-08-11T00:00:03Z",
                )
            )
        post_blob = add_blob(
            OpaqueEvidenceRole.POST_BLOCK_RUNTIME_SNAPSHOT,
            encoder.runtime_snapshot(
                epoch_index=0, block_index=block.block_index,
                phase=RuntimeSnapshotPhase.POST_BLOCK, observed_at_utc="2026-08-11T00:00:03Z",
            ),
        )
        add_event(
            LifecycleEventKind.BLOCK_CLOSED,
            epoch=0, block=block.block_index, position=None,
            data={
                "block_started_event_sha256": block_started.lifecycle_event_sha256,
                "measurement_completed_event_sha256": [
                    item.lifecycle_event_sha256 for item in completions
                ],
                "post_block_runtime_snapshot_blob_sha256": post_blob.opaque_evidence_sha256,
            },
            timestamp="2026-08-11T00:00:03Z",
        )

    bundle = ResponseProfileSemanticBundle(
        calibration_population=population,
        warmup_role_manifest=warmup,
        replay_schedule=schedule,
        run_binding=binding,
        events=tuple(events),
        opaque_evidence=tuple(blobs),
        oracle_manifest=oracle,
        control=control,
    )
    expectation = ResponseProfileSemanticExpectation(
        profile_identity=identity, expected_oracle_manifest_sha256=oracle.oracle_manifest_sha256
    )
    # Compute this fixture's own raw-evidence root rather than reusing another
    # fixture's hardcoded digest -- the root is a function of every identity
    # field above (dataset/run/source-revision strings, timestamps, stream
    # identity), all of which are exp011-local and differ from other fixtures.
    verified_bundle = verify_response_profile_semantic_bundle(bundle=bundle, expectation=expectation)
    capability = issue_root_pinned_response_profile_evidence(
        bundle=bundle,
        expectation=expectation,
        expected_raw_evidence_sha256=verified_bundle.raw_evidence_sha256,
    )
    profile = project_root_pinned_response_profile(
        capability=capability,
        expected_raw_evidence_sha256=capability.raw_evidence_sha256,
        expected_identity=identity,
    )
    fresh = bind_fresh_response_profile_evidence(
        capability=capability, profile=profile, control=control, verified_latest_detector_head=latest
    )
    return _FastFixture(
        stream_key=stream_key,
        control=control,
        identity=identity,
        capability=capability,
        profile=profile,
        monitor_store=monitor_store,
        latest=latest,
        fresh=fresh,
    )


def _result(scenario_id: str, *, passed: bool, reason_codes: tuple[str, ...]) -> Exp011OfflineScenarioResult:
    document = {
        "schema_version": "exp011-offline-scenario-v1",
        "evidence_status": EVIDENCE_STATUS,
        "scenario_id": scenario_id,
        "passed": passed,
        "reason_codes": list(reason_codes),
    }
    digest = hashlib.sha256(canonical_json_bytes(document)).hexdigest()
    return Exp011OfflineScenarioResult(
        scenario_id=scenario_id, passed=passed, reason_codes=reason_codes, evidence_digest=digest
    )


def _scenario_canonical_pre_result_control_binding(fixture: _FastFixture) -> Exp011OfflineScenarioResult:
    try:
        assert fixture.control.frozen_at_utc < fixture.identity.calibration_started_at_utc
        rebound = verify_fresh_response_profile_evidence(fixture.fresh)
        assert rebound == fixture.fresh
        return _result(
            "canonical_pre_result_control_binding",
            passed=True,
            reason_codes=("CONTROL_FROZEN_BEFORE_RESULT", "FRESH_BIND_OK"),
        )
    except (AssertionError, ResponseProfileFreshnessError) as exc:
        return _result(
            "canonical_pre_result_control_binding", passed=False, reason_codes=(str(exc),)
        )


def _scenario_atomic_monitor_state_head_append(store_path: Path) -> Exp011OfflineScenarioResult:
    stream_key = MonitorStreamKey(
        "exp011-atomic-append-stream",
        Metric.L2,
        "target-075",
        "config-v1",
        "data-v1",
        "flat-v1",
        "hnsw-v1",
    )
    with ResponseProfileMonitorStateStore(store_path, expected_stream_key=stream_key) as store:
        store.save(MonitorStreamState(stream_key=stream_key))
        provenance = build_evidence_provenance(
            metric=Metric.L2, threshold_stratum="target-075",
            reference_window_id="ref", current_window_id="cur",
            reference_manifest_sha256=_digest("1"), current_manifest_sha256=_digest("2"),
            configuration_identity="config-v1", data_identity="data-v1",
            flat_binding_id="flat-v1", hnsw_binding_id="hnsw-v1",
            reference_audit_ids=tuple(range(50)),
            reference_audit_rank_digests=tuple(_digest("3") for _ in range(50)),
            current_audit_ids=tuple(range(50, 100)),
            current_audit_rank_digests=tuple(_digest("4") for _ in range(50)),
        )
        head = build_response_profile_detector_head(
            stream_key=stream_key, window_sequence=2,
            detector_state=DetectorState.NO_DRIFT, detector_classification=DriftClassification.NONE,
            detector_provenance=provenance,
        )
        store.save(MonitorStreamState(stream_key=stream_key, next_window_sequence=3, latest_detector_head=head))
        latest = store.load_verified_latest(stream_key)
        loaded_state = store.load(stream_key)
        passed = (
            latest is not None
            and latest.head_record_sequence == 0
            and loaded_state is not None
            and loaded_state.latest_detector_head == head
        )
    return _result(
        "atomic_monitor_state_head_append",
        passed=bool(passed),
        reason_codes=("HEAD_AND_STATE_ATOMIC",) if passed else ("ATOMICITY_CHECK_FAILED",),
    )


def _scenario_restart_and_complete_hash_chain_replay(fixture: _FastFixture, store_path: Path) -> Exp011OfflineScenarioResult:
    before = fixture.monitor_store.load_verified_latest(fixture.stream_key)
    fixture.monitor_store.close()
    reopened = ResponseProfileMonitorStateStore(store_path, expected_stream_key=fixture.stream_key)
    try:
        after = reopened.load_verified_latest(fixture.stream_key)
        passed = (
            before is not None
            and after is not None
            and before.head_record_sha256 == after.head_record_sha256
            and before.head.detector_head_sha256 == after.head.detector_head_sha256
        )
    finally:
        reopened.close()
    # Restore the fixture's own store handle for any later scenario reuse.
    fixture.monitor_store = ResponseProfileMonitorStateStore(store_path, expected_stream_key=fixture.stream_key)
    return _result(
        "restart_and_complete_hash_chain_replay",
        passed=bool(passed),
        reason_codes=("RESTART_REPLAY_MATCHES",) if passed else ("RESTART_REPLAY_MISMATCH",),
    )


def _scenario_stale_superseded_head_refusal(fixture: _FastFixture) -> Exp011OfflineScenarioResult:
    old_digest = fixture.fresh.verified_latest_detector_head.head_record_sha256
    new_provenance = fixture.control.detector_provenance
    new_head = build_response_profile_detector_head(
        stream_key=fixture.stream_key, window_sequence=fixture.latest.head.window_sequence + 1,
        detector_state=DetectorState.NO_DRIFT, detector_classification=DriftClassification.NONE,
        detector_provenance=new_provenance,
    )
    fixture.monitor_store.save(
        MonitorStreamState(
            stream_key=fixture.stream_key,
            next_window_sequence=fixture.latest.head.window_sequence + 2,
            latest_detector_head=new_head,
        )
    )
    new_latest = fixture.monitor_store.load_verified_latest(fixture.stream_key)
    reasons = []
    passed = True
    if new_latest is None or new_latest.head_record_sha256 == old_digest:
        passed = False
        reasons.append("NEW_HEAD_NOT_OBSERVED")
    else:
        reasons.append("NEW_HEAD_DIGEST_ADVANCED")
    try:
        bind_fresh_response_profile_evidence(
            capability=fixture.capability, profile=fixture.profile,
            control=fixture.control, verified_latest_detector_head=new_latest,
        )
        passed = False
        reasons.append("STALE_CONTROL_WAS_NOT_REFUSED")
    except ResponseProfileFreshnessError as exc:
        if exc.code == "DETECTOR_HEAD_MISMATCH":
            reasons.append("DETECTOR_HEAD_MISMATCH")
        else:
            passed = False
            reasons.append(f"UNEXPECTED_CODE:{exc.code}")
    try:
        verify_fresh_response_profile_evidence(fixture.fresh)
        reasons.append("OLD_BIND_STILL_HISTORICALLY_VALID")
    except ResponseProfileFreshnessError as exc:
        passed = False
        reasons.append(f"OLD_BIND_UNEXPECTEDLY_INVALID:{exc.code}")
    return _result("stale_superseded_head_refusal", passed=passed, reason_codes=tuple(reasons))


def _scenario_forged_head_refusal(fixture: _FastFixture) -> Exp011OfflineScenarioResult:
    forged = _forge(fixture.latest, head_record_sha256=_digest("f"))
    try:
        bind_fresh_response_profile_evidence(
            capability=fixture.capability, profile=fixture.profile,
            control=fixture.control, verified_latest_detector_head=forged,
        )
        return _result("forged_head_refusal", passed=False, reason_codes=("FORGED_HEAD_WAS_NOT_REFUSED",))
    except ResponseProfileFreshnessError as exc:
        passed = exc.code == "DETECTOR_HEAD_MISMATCH"
        return _result(
            "forged_head_refusal", passed=passed,
            reason_codes=(exc.code,) if passed else (f"UNEXPECTED_CODE:{exc.code}",),
        )


def _scenario_concurrent_append_vs_refresh(store_path: Path, stream_key: MonitorStreamKey) -> Exp011OfflineScenarioResult:
    with ResponseProfileMonitorStateStore(store_path, expected_stream_key=stream_key):
        try:
            ResponseProfileMonitorStateStore(store_path, expected_stream_key=stream_key)
            return _result(
                "concurrent_append_vs_refresh", passed=False,
                reason_codes=("SECOND_OPEN_WAS_NOT_REFUSED",),
            )
        except ResponseProfileMonitorStoreError as exc:
            passed = exc.code == "STORE_ALREADY_OPEN"
            return _result(
                "concurrent_append_vs_refresh", passed=passed,
                reason_codes=(exc.code,) if passed else (f"UNEXPECTED_CODE:{exc.code}",),
            )


def _scenario_monitor_state_head_divergence_fails_closed(store_path: Path, stream_key: MonitorStreamKey) -> Exp011OfflineScenarioResult:
    with ResponseProfileMonitorStateStore(store_path, expected_stream_key=stream_key) as store:
        store.save(MonitorStreamState(stream_key=stream_key))
    connection = sqlite3.connect(store_path)
    try:
        connection.execute("DROP TRIGGER monitor_state_records_no_update")
        connection.commit()
    finally:
        connection.close()
    try:
        ResponseProfileMonitorStateStore(store_path, expected_stream_key=stream_key)
        return _result(
            "monitor_state_head_divergence_fails_closed", passed=False,
            reason_codes=("SCHEMA_TAMPER_WAS_NOT_REFUSED",),
        )
    except ResponseProfileMonitorStoreError as exc:
        passed = exc.code == "STORE_SCHEMA_INVALID"
        return _result(
            "monitor_state_head_divergence_fails_closed", passed=passed,
            reason_codes=(exc.code,) if passed else (f"UNEXPECTED_CODE:{exc.code}",),
        )


def _scenario_bare_profile_refusal(fixture: _FastFixture) -> Exp011OfflineScenarioResult:
    try:
        bind_fresh_response_profile_evidence(
            capability=fixture.capability, profile=fixture.identity,
            control=fixture.control, verified_latest_detector_head=fixture.latest,
        )
        return _result("bare_profile_refusal", passed=False, reason_codes=("BARE_PROFILE_WAS_NOT_REFUSED",))
    except ResponseProfileFreshnessError as exc:
        passed = exc.code == "RESPONSE_PROFILE_INVALID"
        return _result(
            "bare_profile_refusal", passed=passed,
            reason_codes=(exc.code,) if passed else (f"UNEXPECTED_CODE:{exc.code}",),
        )


def _scenario_bare_root_capability_refusal(fixture: _FastFixture) -> Exp011OfflineScenarioResult:
    try:
        bind_fresh_response_profile_evidence(
            capability=fixture.profile, profile=fixture.profile,
            control=fixture.control, verified_latest_detector_head=fixture.latest,
        )
        return _result(
            "bare_root_capability_refusal", passed=False,
            reason_codes=("BARE_CAPABILITY_WAS_NOT_REFUSED",),
        )
    except ResponseProfileFreshnessError as exc:
        passed = exc.code == "ROOT_PINNED_CAPABILITY_INVALID"
        return _result(
            "bare_root_capability_refusal", passed=passed,
            reason_codes=(exc.code,) if passed else (f"UNEXPECTED_CODE:{exc.code}",),
        )


def _scenario_rollback_available_without_profile_evidence() -> Exp011OfflineScenarioResult:
    module_path = Path(__file__).with_name("canary_rollback.py")
    tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
    imported = {
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    } | {
        alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    }
    forbidden_suffixes = (
        "response_profile",
        "response_profile_freshness",
        "response_profile_producer",
        "response_profile_monitor_store",
        "response_profile_control",
    )
    offending = {
        item for item in imported
        if any(item == suffix or item.endswith(f".{suffix}") for suffix in forbidden_suffixes)
    }
    passed = not offending
    return _result(
        "rollback_available_without_profile_evidence",
        passed=passed,
        reason_codes=("ROLLBACK_HAS_NO_RESPONSE_PROFILE_DEPENDENCY",) if passed else tuple(sorted(offending)),
    )


_MISMATCH_AXES: tuple[str, ...] = (
    "window_sequence", "provenance_window_id", "provenance_manifest", "metric",
    "stratum", "configuration", "data", "flat", "hnsw", "environment", "source",
)


def _scenario_mismatch_matrix(fixture: _FastFixture) -> Exp011OfflineScenarioResult:
    reasons: list[str] = []
    passed = True
    base_head = fixture.latest.head
    base_provenance = base_head.detector_provenance

    def alternate_head(**head_changes: object):
        provenance_changes = head_changes.pop("_provenance", {})
        provenance = build_evidence_provenance(
            **{**_provenance_kwargs(base_provenance), **provenance_changes}
        )
        stream = base_head.stream_key
        stream_changes = head_changes.pop("_stream", {})
        if stream_changes:
            stream = MonitorStreamKey(
                stream_changes.get("stream_id", stream.stream_id),
                stream_changes.get("metric", stream.metric),
                stream_changes.get("threshold_stratum", stream.threshold_stratum),
                stream_changes.get("configuration_identity", stream.configuration_identity),
                stream_changes.get("data_identity", stream.data_identity),
                stream_changes.get("flat_binding_id", stream.flat_binding_id),
                stream_changes.get("hnsw_binding_id", stream.hnsw_binding_id),
            )
            provenance = build_evidence_provenance(
                **{
                    **_provenance_kwargs(base_provenance),
                    "metric": stream.metric,
                    "threshold_stratum": stream.threshold_stratum,
                    "configuration_identity": stream.configuration_identity,
                    "data_identity": stream.data_identity,
                    "flat_binding_id": stream.flat_binding_id,
                    "hnsw_binding_id": stream.hnsw_binding_id,
                }
            )
        return build_response_profile_detector_head(
            stream_key=stream,
            window_sequence=head_changes.get("window_sequence", base_head.window_sequence),
            detector_state=DetectorState.NO_DRIFT,
            detector_classification=DriftClassification.NONE,
            detector_provenance=provenance,
        )

    axis_heads = {
        "window_sequence": alternate_head(window_sequence=base_head.window_sequence + 5),
        "provenance_window_id": alternate_head(_provenance={"current_window_id": "alternate-window"}),
        "provenance_manifest": alternate_head(_provenance={"current_manifest_sha256": _digest("9")}),
        "metric": alternate_head(_stream={"metric": Metric.COSINE if base_head.stream_key.metric is Metric.L2 else Metric.L2}),
        "stratum": alternate_head(_stream={"threshold_stratum": "target-090"}),
        "configuration": alternate_head(_stream={"configuration_identity": "alternate-configuration"}),
        "data": alternate_head(_stream={"data_identity": "alternate-data"}),
        "flat": alternate_head(_stream={"flat_binding_id": "alternate-flat"}),
        "hnsw": alternate_head(_stream={"hnsw_binding_id": "alternate-hnsw"}),
    }
    for axis, alt_head in axis_heads.items():
        forged_latest = _forge(fixture.latest, head=alt_head)
        try:
            bind_fresh_response_profile_evidence(
                capability=fixture.capability, profile=fixture.profile,
                control=fixture.control, verified_latest_detector_head=forged_latest,
            )
            passed = False
            reasons.append(f"AXIS_FAILED:{axis}:NOT_REFUSED")
        except ResponseProfileFreshnessError as exc:
            if exc.code == "DETECTOR_HEAD_MISMATCH":
                reasons.append(f"AXIS_OK:{axis}:{exc.code}")
            else:
                passed = False
                reasons.append(f"AXIS_FAILED:{axis}:{exc.code}")

    for axis, field_name, value in (
        ("environment", "environment_manifest_sha256", _digest("7")),
        ("source", "source_revision", "revision/alternate"),
    ):
        control_kwargs = {item.name: getattr(fixture.control, item.name) for item in fields(fixture.control)}
        control_kwargs[field_name] = value
        alt_control = build_response_profile_control(
            stream_key=control_kwargs["stream_key"],
            detector_provenance=control_kwargs["detector_provenance"],
            trigger_window_sequence=control_kwargs["trigger_window_sequence"],
            detector_head_sha256=control_kwargs["detector_head_sha256"],
            detector_head_record_sequence=control_kwargs["detector_head_record_sequence"],
            detector_head_record_sha256=control_kwargs["detector_head_record_sha256"],
            detector_head_persisted_at_utc=control_kwargs["detector_head_persisted_at_utc"],
            calibration_population_sha256=control_kwargs["calibration_population_sha256"],
            warmup_role_manifest_sha256=control_kwargs["warmup_role_manifest_sha256"],
            ordered_query_payload_sha256=control_kwargs["ordered_query_payload_sha256"],
            replay_schedule_sha256=control_kwargs["replay_schedule_sha256"],
            environment_manifest_sha256=control_kwargs["environment_manifest_sha256"],
            source_revision=control_kwargs["source_revision"],
            frozen_at_utc=control_kwargs["frozen_at_utc"],
        )
        try:
            bind_fresh_response_profile_evidence(
                capability=fixture.capability, profile=fixture.profile,
                control=alt_control, verified_latest_detector_head=fixture.latest,
            )
            passed = False
            reasons.append(f"AXIS_FAILED:{axis}:NOT_REFUSED")
        except ResponseProfileFreshnessError as exc:
            if exc.code == "PROFILE_CONTROL_MISMATCH":
                reasons.append(f"AXIS_OK:{axis}:{exc.code}")
            else:
                passed = False
                reasons.append(f"AXIS_FAILED:{axis}:{exc.code}")

    return _result("mismatch_matrix", passed=passed, reason_codes=tuple(reasons))


def _provenance_kwargs(provenance: EvidenceProvenance) -> dict[str, object]:
    return {
        "metric": provenance.metric,
        "threshold_stratum": provenance.threshold_stratum,
        "reference_window_id": provenance.reference_window_id,
        "current_window_id": provenance.current_window_id,
        "reference_manifest_sha256": provenance.reference_manifest_sha256,
        "current_manifest_sha256": provenance.current_manifest_sha256,
        "configuration_identity": provenance.configuration_identity,
        "data_identity": provenance.data_identity,
        "flat_binding_id": provenance.flat_binding_id,
        "hnsw_binding_id": provenance.hnsw_binding_id,
        "reference_audit_ids": provenance.reference_audit_ids,
        "reference_audit_rank_digests": provenance.reference_audit_rank_digests,
        "current_audit_ids": provenance.current_audit_ids,
        "current_audit_rank_digests": provenance.current_audit_rank_digests,
    }


_SCENARIO_IDS: tuple[str, ...] = (
    "canonical_pre_result_control_binding",
    "atomic_monitor_state_head_append",
    "restart_and_complete_hash_chain_replay",
    "stale_superseded_head_refusal",
    "forged_head_refusal",
    "concurrent_append_vs_refresh",
    "monitor_state_head_divergence_fails_closed",
    "bare_profile_refusal",
    "bare_root_capability_refusal",
    "rollback_available_without_profile_evidence",
    "mismatch_matrix",
)


def run_exp011_offline(
    *, output_dir: Path, utc_now: Callable[[], str] | None = None
) -> Exp011OfflineResult:
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise Exp011OfflineError(f"refusing to overwrite existing directory: {output_dir}")
    output_dir.mkdir(parents=True, mode=0o700)
    fixtures_dir = output_dir / "_fixtures"
    fixtures_dir.mkdir(mode=0o700)
    now = utc_now or _default_utc_now

    fixture = _build_fast_fixture(store_path=fixtures_dir / "fast_fixture.sqlite3")
    results: list[Exp011OfflineScenarioResult] = []
    try:
        results.append(_scenario_canonical_pre_result_control_binding(fixture))
        results.append(
            _scenario_atomic_monitor_state_head_append(fixtures_dir / "atomic_append.sqlite3")
        )
        results.append(
            _scenario_restart_and_complete_hash_chain_replay(
                fixture, fixtures_dir / "fast_fixture.sqlite3"
            )
        )
        results.append(_scenario_stale_superseded_head_refusal(fixture))
        results.append(_scenario_forged_head_refusal(fixture))
        results.append(
            _scenario_concurrent_append_vs_refresh(
                fixtures_dir / "concurrent.sqlite3", fixture.stream_key
            )
        )
        results.append(
            _scenario_monitor_state_head_divergence_fails_closed(
                fixtures_dir / "divergence.sqlite3", fixture.stream_key
            )
        )
        results.append(_scenario_bare_profile_refusal(fixture))
        results.append(_scenario_bare_root_capability_refusal(fixture))
        results.append(_scenario_rollback_available_without_profile_evidence())
        results.append(_scenario_mismatch_matrix(fixture))
    finally:
        fixture.close()

    for scenario in results:
        document = {
            "schema_version": "exp011-offline-scenario-v1",
            "evidence_status": EVIDENCE_STATUS,
            "scenario_id": scenario.scenario_id,
            "passed": scenario.passed,
            "reason_codes": list(scenario.reason_codes),
            "generated_at_utc": now(),
            "evidence_digest": scenario.evidence_digest,
        }
        write_immutable_json(output_dir / f"{scenario.scenario_id}.json", document)

    summary = {
        "schema_version": "exp011-offline-summary-v1",
        "evidence_status": EVIDENCE_STATUS,
        "generated_at_utc": now(),
        "scenario_count": len(results),
        "all_passed": all(item.passed for item in results),
        "scenarios": [
            {
                "scenario_id": item.scenario_id,
                "passed": item.passed,
                "reason_codes": list(item.reason_codes),
                "evidence_digest": item.evidence_digest,
            }
            for item in results
        ],
    }
    write_immutable_json(output_dir / "summary.json", summary)
    return Exp011OfflineResult(output_dir=output_dir, scenarios=tuple(results))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/exp-011/offline"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = run_exp011_offline(output_dir=args.output_dir)
    all_passed = all(item.passed for item in result.scenarios)
    print(json.dumps({"output_dir": str(result.output_dir), "all_passed": all_passed}, sort_keys=True))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
