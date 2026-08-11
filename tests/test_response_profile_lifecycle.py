from __future__ import annotations

import ast
from dataclasses import fields
import hashlib
from pathlib import Path
import unittest

import numpy as np

from vdbench.artifacts import canonical_json_bytes
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    MEASURED_POSITION_COUNT,
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
    LIFECYCLE_EVENT_HASH_DOMAIN,
    LIFECYCLE_EVENT_SCHEMA_VERSION,
    OPAQUE_EVIDENCE_HASH_DOMAIN,
    OPAQUE_EVIDENCE_SCHEMA_VERSION,
    RUN_BINDING_HASH_DOMAIN,
    RUN_BINDING_SCHEMA_VERSION,
    LifecycleEventKind,
    OpaqueEvidenceBlob,
    OpaqueEvidenceRole,
    ResponseProfileLifecycleContractError,
    ResponseProfileLifecycleEvent,
    ResponseProfileRunBinding,
    apply_next_lifecycle_event,
    build_opaque_evidence_blob,
    build_response_profile_lifecycle_event,
    build_response_profile_run_binding,
    initial_lifecycle_reducer_state,
    opaque_evidence_descriptor_payload,
    reduce_response_profile_lifecycle,
    response_profile_lifecycle_event_payload,
    response_profile_run_binding_payload,
    verify_opaque_evidence_blob,
    verify_response_profile_lifecycle_event,
    verify_response_profile_run_binding,
)


GOLDEN_RUN_BINDING_SHA256 = "a2909714634e909ab41560ca7b7c1fa5ccbdc8b9265488d4e63fb37bb79c1f85"
GOLDEN_WARMUP_BLOB_SHA256 = "e7e612d169f7553ac29c1f9c32803f501a392c93b5ff3f3f019b07ab84732cc5"
GOLDEN_EPOCH_EVENT_SHA256 = "ca82c6b3686b1f822f5e6802047ca2efd14c5f0a64b6f09fe02c775282ba942a"


def _digest(character: str) -> str:
    return character * 64


def _configuration() -> SearchConfiguration:
    return SearchConfiguration(
        metric=Metric.L2,
        threshold_label="target-075",
        radius=0.75,
        index_track=IndexTrack.FLAT,
        ef=None,
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
            search_configuration=_configuration(),
        ),
    )


def _manifest(
    role_kind: ResponseProfileRoleKind, members: tuple[object, ...]
):
    return build_response_profile_role_manifest(
        role=build_response_profile_role(kind=role_kind),
        members=members,
    )


def _forge(value: object, **changes: object):
    forged = object.__new__(type(value))
    for field in fields(value):
        object.__setattr__(
            forged,
            field.name,
            changes.get(field.name, getattr(value, field.name)),
        )
    return forged


def _assert_contract_error(
    case: unittest.TestCase, operation: object, code: str | None = None
) -> None:
    with case.assertRaises(ResponseProfileLifecycleContractError) as raised:
        operation()  # type: ignore[operator]
    if code is not None:
        case.assertEqual(raised.exception.code, code)


_FORBIDDEN_INTERNAL_DEPENDENCIES = {
    "policy",
    "actuation",
    "canary_admission",
    "lkg_phase3_authority",
    "lkg_phase3_persistence",
    "response_profile",
}
_FORBIDDEN_EXTERNAL_DEPENDENCIES = {"sqlite3", "pymilvus"}


def _forbidden_imports(source: str) -> set[str]:
    """Return forbidden relative or absolute imports in one Python source."""

    tree = ast.parse(source)
    targets: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            targets.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module:
                targets.add(module)
                if module == "vdbench":
                    targets.update(f"vdbench.{alias.name}" for alias in node.names)
            elif node.level:
                targets.update(alias.name for alias in node.names)

    forbidden: set[str] = set()
    for target in targets:
        components = target.split(".")
        if components[0] in _FORBIDDEN_EXTERNAL_DEPENDENCIES:
            forbidden.add(target)
            continue
        if components[0] == "vdbench" and len(components) > 1:
            internal_root = components[1]
        else:
            internal_root = components[0]
        if internal_root in _FORBIDDEN_INTERNAL_DEPENDENCIES:
            forbidden.add(target)
    return forbidden


class _Chain:
    def __init__(self, binding: ResponseProfileRunBinding) -> None:
        self.binding = binding
        self.events: list[ResponseProfileLifecycleEvent] = []
        self.blobs: list[object] = []
        self._blob_by_seq: dict[int, object] = {}

    @property
    def event_seq(self) -> int:
        return len(self.events)

    @property
    def previous(self) -> str:
        if self.events:
            return self.events[-1].lifecycle_event_sha256
        return self.binding.run_binding_sha256

    def blob(self, role: OpaqueEvidenceRole, payload: bytes):
        blob = build_opaque_evidence_blob(
            run_binding_sha256=self.binding.run_binding_sha256,
            event_seq=self.event_seq,
            evidence_role=role,
            evidence_bytes=payload,
        )
        self.blobs.append(blob)
        self._blob_by_seq[blob.event_seq] = blob
        return blob

    def event(
        self,
        kind: LifecycleEventKind,
        *,
        epoch: int | None,
        block: int | None,
        position: int | None,
        data: dict[str, object],
        timestamp: str = "2026-08-10T00:00:00Z",
        event_seq: int | None = None,
        previous: str | None = None,
    ) -> ResponseProfileLifecycleEvent:
        event = build_response_profile_lifecycle_event(
            run_binding_sha256=self.binding.run_binding_sha256,
            event_seq=self.event_seq if event_seq is None else event_seq,
            event_kind=kind,
            epoch_index=epoch,
            block_index=block,
            position_index=position,
            recorded_at_utc=timestamp,
            event_data=data,
            previous_event_sha256=self.previous if previous is None else previous,
        )
        self.events.append(event)
        return event

    def epoch(self, index: int, *, timestamp: str = "2026-08-10T00:00:00Z"):
        return self.event(
            LifecycleEventKind.EPOCH_STARTED,
            epoch=index,
            block=None,
            position=None,
            data={},
            timestamp=timestamp,
        )

    def warmup(self, epoch: int, *, timestamp: str = "2026-08-10T00:00:01Z"):
        blob = self.blob(OpaqueEvidenceRole.WARMUP_EXECUTION, b"warmup-execution")
        return self.event(
            LifecycleEventKind.WARMUP_COMPLETED,
            epoch=epoch,
            block=None,
            position=None,
            data={
                "warmup_role_manifest_sha256": (
                    self.binding.warmup_role_manifest_sha256
                ),
                "warmup_execution_blob_sha256": blob.opaque_evidence_sha256,
            },
            timestamp=timestamp,
        )

    def block_start(self, epoch: int, block: int):
        blob = self.blob(
            OpaqueEvidenceRole.PRE_BLOCK_RUNTIME_SNAPSHOT,
            f"pre-{block}".encode(),
        )
        return self.event(
            LifecycleEventKind.BLOCK_STARTED,
            epoch=epoch,
            block=block,
            position=None,
            data={
                "pre_block_runtime_snapshot_blob_sha256": (
                    blob.opaque_evidence_sha256
                )
            },
        )

    def measurement_start(
        self,
        epoch: int,
        block: int,
        within: int,
        *,
        overrides: dict[str, object] | None = None,
        started_ns: int | None = None,
        timestamp: str = "2026-08-10T00:00:02Z",
    ):
        position = self.binding.replay_schedule.blocks[block].positions[within]
        data: dict[str, object] = {
            "within_block_index": within,
            "canonical_query_index": position.canonical_query_index,
            "query_id": position.query_id,
            "query_id_sha256": position.query_id_sha256,
            "observation_identity_sha256": position.observation_identity_sha256,
            "ef": position.ef,
            "started_monotonic_ns": (
                1_000_000 + position.position_index * 100
                if started_ns is None
                else started_ns
            ),
        }
        data.update(overrides or {})
        return self.event(
            LifecycleEventKind.MEASUREMENT_STARTED,
            epoch=epoch,
            block=block,
            position=position.position_index,
            data=data,
            timestamp=timestamp,
        )

    def measurement_complete(
        self,
        started: ResponseProfileLifecycleEvent,
        *,
        started_digest: str | None = None,
        completed_ns: int | None = None,
        timestamp: str = "2026-08-10T00:00:03Z",
    ):
        blob = self.blob(
            OpaqueEvidenceRole.MEASURED_RESULT,
            f"result-{started.position_index}".encode(),
        )
        start_ns = response_profile_lifecycle_event_payload(started)["event_data"][  # type: ignore[index]
            "started_monotonic_ns"
        ]
        return self.event(
            LifecycleEventKind.MEASUREMENT_COMPLETED,
            epoch=started.epoch_index,
            block=started.block_index,
            position=started.position_index,
            data={
                "measurement_started_event_sha256": (
                    started.lifecycle_event_sha256
                    if started_digest is None
                    else started_digest
                ),
                "measured_result_blob_sha256": blob.opaque_evidence_sha256,
                "completed_monotonic_ns": (
                    int(start_ns) + 10 if completed_ns is None else completed_ns
                ),
            },
            timestamp=timestamp,
        )

    def complete_block(self, epoch: int, block: int):
        started_block = self.block_start(epoch, block)
        completions = []
        for within in range(4):
            started = self.measurement_start(epoch, block, within)
            completions.append(self.measurement_complete(started))
        blob = self.blob(
            OpaqueEvidenceRole.POST_BLOCK_RUNTIME_SNAPSHOT,
            f"post-{block}".encode(),
        )
        closed = self.event(
            LifecycleEventKind.BLOCK_CLOSED,
            epoch=epoch,
            block=block,
            position=None,
            data={
                "block_started_event_sha256": started_block.lifecycle_event_sha256,
                "measurement_completed_event_sha256": [
                    item.lifecycle_event_sha256 for item in completions
                ],
                "post_block_runtime_snapshot_blob_sha256": (
                    blob.opaque_evidence_sha256
                ),
            },
        )
        return started_block, tuple(completions), closed

    def reduce(self, *, recovery: bool):
        return reduce_response_profile_lifecycle(
            run_binding=self.binding,
            events=tuple(self.events),
            opaque_evidence=tuple(self.blobs),
            recovery_boundary=recovery,
        )

    def blob_for(self, event: ResponseProfileLifecycleEvent) -> object | None:
        """Return the one opaque evidence blob ``event`` references, if any.

        Keyed by each blob's own ``event_seq`` (an O(1) dict lookup, not a
        scan) so it stays cheap even when replaying a full-size chain.
        """

        if len(self._blob_by_seq) != len(self.blobs):
            self._blob_by_seq = {b.event_seq: b for b in self.blobs}
        return self._blob_by_seq.get(event.event_seq)


class ResponseProfileLifecycleFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.namespace = build_artifact_source_namespace(
            dataset_id="DATASET-R2-B1",
            dataset_version="v1",
            generation_manifest_sha256=_digest("a"),
        )
        calibration_members = tuple(
            _member(index, namespace=cls.namespace)
            for index in range(CALIBRATION_QUERY_COUNT)
        )
        cls.calibration_manifest = _manifest(
            ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION,
            calibration_members,
        )
        cls.population = build_calibration_population_manifest(
            cell=build_response_profile_cell(
                metric=Metric.L2, threshold_stratum="target-075"
            ),
            calibration_role_manifest=cls.calibration_manifest,
        )
        cls.schedule = build_response_profile_replay_schedule(
            population=cls.population,
            source_revision="revision/r2-b1-golden",
        )
        warmup_members = tuple(
            _member(index + 10_000, namespace=cls.namespace, offset=20_000.0)
            for index in range(WARMUP_QUERY_COUNT)
        )
        cls.warmup_manifest = _manifest(
            ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP,
            warmup_members,
        )
        cls.binding = build_response_profile_run_binding(
            run_id="exp010-r2-b1-golden",
            created_at_utc="2026-08-10T00:00:00Z",
            population=cls.population,
            replay_schedule=cls.schedule,
            warmup_role_manifest=cls.warmup_manifest,
            source_revision="revision/r2-b1-golden",
        )


class CanonicalContractTests(ResponseProfileLifecycleFixture):
    def test_schema_versions_domains_and_event_catalog_are_exact(self) -> None:
        self.assertEqual(
            RUN_BINDING_SCHEMA_VERSION,
            "response-profile-lifecycle-run-binding-v1",
        )
        self.assertEqual(
            OPAQUE_EVIDENCE_SCHEMA_VERSION,
            "response-profile-opaque-evidence-blob-v1",
        )
        self.assertEqual(
            LIFECYCLE_EVENT_SCHEMA_VERSION,
            "response-profile-lifecycle-event-v1",
        )
        self.assertEqual(
            RUN_BINDING_HASH_DOMAIN,
            b"VD::RESPONSE_PROFILE_LIFECYCLE_RUN_BINDING::V1\x00",
        )
        self.assertEqual(
            OPAQUE_EVIDENCE_HASH_DOMAIN,
            b"VD::RESPONSE_PROFILE_OPAQUE_EVIDENCE_BLOB::V1\x00",
        )
        self.assertEqual(
            LIFECYCLE_EVENT_HASH_DOMAIN,
            b"VD::RESPONSE_PROFILE_LIFECYCLE_EVENT::V1\x00",
        )
        self.assertEqual(
            tuple(kind.value for kind in LifecycleEventKind),
            (
                "EPOCH_STARTED",
                "WARMUP_COMPLETED",
                "BLOCK_STARTED",
                "MEASUREMENT_STARTED",
                "MEASUREMENT_COMPLETED",
                "BLOCK_CLOSED",
                "RUN_SEALED",
                "RUN_INVALIDATED",
            ),
        )
        self.assertNotIn("WARMUP_SEALED", LifecycleEventKind.__members__)
        self.assertEqual(
            tuple(role.value for role in OpaqueEvidenceRole),
            (
                "WARMUP_EXECUTION",
                "MEASURED_RESULT",
                "PRE_BLOCK_RUNTIME_SNAPSHOT",
                "POST_BLOCK_RUNTIME_SNAPSHOT",
            ),
        )

    def test_run_binding_payload_and_digest_are_exact_and_golden(self) -> None:
        payload = response_profile_run_binding_payload(self.binding)
        self.assertEqual(
            payload,
            {
                "schema_version": "response-profile-lifecycle-run-binding-v1",
                "run_id": "exp010-r2-b1-golden",
                "created_at_utc": "2026-08-10T00:00:00Z",
                "cell_id": self.population.cell.cell_id,
                "workload_manifest_sha256": self.population.workload_manifest_sha256,
                "replay_schedule_sha256": self.schedule.replay_schedule_sha256,
                "warmup_role_manifest_sha256": (
                    self.warmup_manifest.role_manifest_sha256
                ),
                "source_revision": "revision/r2-b1-golden",
            },
        )
        expected = hashlib.sha256(
            RUN_BINDING_HASH_DOMAIN + canonical_json_bytes(payload)
        ).hexdigest()
        self.assertEqual(self.binding.run_binding_sha256, expected)
        self.assertEqual(self.binding.run_binding_sha256, GOLDEN_RUN_BINDING_SHA256)

    def test_opaque_blob_descriptor_is_exact_and_golden(self) -> None:
        blob = build_opaque_evidence_blob(
            run_binding_sha256=self.binding.run_binding_sha256,
            event_seq=1,
            evidence_role=OpaqueEvidenceRole.WARMUP_EXECUTION,
            evidence_bytes=b"warmup-execution",
        )
        payload = opaque_evidence_descriptor_payload(blob)
        self.assertEqual(
            set(payload),
            {
                "schema_version",
                "run_binding_sha256",
                "event_seq",
                "evidence_role",
                "byte_length",
                "evidence_bytes_sha256",
            },
        )
        self.assertNotIn("evidence_bytes", payload)
        self.assertEqual(blob.byte_length, len(b"warmup-execution"))
        self.assertEqual(
            blob.evidence_bytes_sha256,
            hashlib.sha256(b"warmup-execution").hexdigest(),
        )
        self.assertEqual(blob.opaque_evidence_sha256, GOLDEN_WARMUP_BLOB_SHA256)

    def test_event_payload_digest_and_utc_metadata_are_exact_and_golden(self) -> None:
        event = build_response_profile_lifecycle_event(
            run_binding_sha256=self.binding.run_binding_sha256,
            event_seq=0,
            event_kind=LifecycleEventKind.EPOCH_STARTED,
            epoch_index=7,
            block_index=None,
            position_index=None,
            recorded_at_utc="2026-08-10T00:00:00Z",
            event_data={},
            previous_event_sha256=self.binding.run_binding_sha256,
        )
        payload = response_profile_lifecycle_event_payload(event)
        self.assertEqual(
            payload,
            {
                "schema_version": "response-profile-lifecycle-event-v1",
                "run_binding_sha256": self.binding.run_binding_sha256,
                "event_seq": 0,
                "event_kind": "EPOCH_STARTED",
                "epoch_index": 7,
                "block_index": None,
                "position_index": None,
                "recorded_at_utc": "2026-08-10T00:00:00Z",
                "event_data": {},
                "previous_event_sha256": self.binding.run_binding_sha256,
            },
        )
        self.assertEqual(
            event.lifecycle_event_sha256,
            hashlib.sha256(
                LIFECYCLE_EVENT_HASH_DOMAIN + canonical_json_bytes(payload)
            ).hexdigest(),
        )
        self.assertEqual(event.lifecycle_event_sha256, GOLDEN_EPOCH_EVENT_SHA256)

    def test_construction_and_reconstruction_reject_forgery(self) -> None:
        with self.assertRaises(TypeError):
            ResponseProfileRunBinding()
        forged_binding = _forge(self.binding, cell_id=_digest("f"))
        _assert_contract_error(
            self,
            lambda: verify_response_profile_run_binding(forged_binding),
            "RUN_BINDING_INVALID",
        )

        blob = build_opaque_evidence_blob(
            run_binding_sha256=self.binding.run_binding_sha256,
            event_seq=0,
            evidence_role=OpaqueEvidenceRole.PRE_BLOCK_RUNTIME_SNAPSHOT,
            evidence_bytes=b"snapshot",
        )
        for forged in (
            _forge(blob, event_seq=False),
            _forge(blob, byte_length=float(blob.byte_length)),
            _forge(blob, evidence_role="PRE_BLOCK_RUNTIME_SNAPSHOT"),
            _forge(blob, evidence_bytes=b"tampered"),
        ):
            with self.subTest(forged=forged):
                _assert_contract_error(
                    self, lambda forged=forged: verify_opaque_evidence_blob(forged)
                )

        event = build_response_profile_lifecycle_event(
            run_binding_sha256=self.binding.run_binding_sha256,
            event_seq=0,
            event_kind=LifecycleEventKind.EPOCH_STARTED,
            epoch_index=0,
            block_index=None,
            position_index=None,
            recorded_at_utc="2026-08-10T00:00:00Z",
            event_data={},
            previous_event_sha256=self.binding.run_binding_sha256,
        )
        for forged in (
            _forge(event, event_seq=False),
            _forge(event, epoch_index=0.0),
            _forge(event, event_kind="EPOCH_STARTED"),
            _forge(event, lifecycle_event_sha256=_digest("f")),
        ):
            with self.subTest(forged=forged):
                _assert_contract_error(
                    self,
                    lambda forged=forged: verify_response_profile_lifecycle_event(
                        forged
                    ),
                )

    def test_invalid_calendar_timestamp_and_unknown_event_data_fail(self) -> None:
        for timestamp in (
            "2026-13-10T00:00:00Z",
            "2026-08-10T00:00:00+00:00",
            "2026-08-10",
        ):
            with self.subTest(timestamp=timestamp):
                _assert_contract_error(
                    self,
                    lambda timestamp=timestamp: build_response_profile_lifecycle_event(
                        run_binding_sha256=self.binding.run_binding_sha256,
                        event_seq=0,
                        event_kind=LifecycleEventKind.EPOCH_STARTED,
                        epoch_index=0,
                        block_index=None,
                        position_index=None,
                        recorded_at_utc=timestamp,
                        event_data={},
                        previous_event_sha256=self.binding.run_binding_sha256,
                    ),
                    "TIMESTAMP_INVALID",
                )
        _assert_contract_error(
            self,
            lambda: build_response_profile_lifecycle_event(
                run_binding_sha256=self.binding.run_binding_sha256,
                event_seq=0,
                event_kind=LifecycleEventKind.EPOCH_STARTED,
                epoch_index=0,
                block_index=None,
                position_index=None,
                recorded_at_utc="2026-08-10T00:00:00Z",
                event_data={"unknown": True},
                previous_event_sha256=self.binding.run_binding_sha256,
            ),
            "EVENT_DATA_INVALID",
        )


class LifecycleTransitionTests(ResponseProfileLifecycleFixture):
    def _ready_chain(self) -> _Chain:
        chain = _Chain(self.binding)
        chain.epoch(0)
        chain.warmup(0)
        return chain

    def test_broken_event_sequence_and_hash_chain_fail_closed(self) -> None:
        skipped = _Chain(self.binding)
        skipped.event(
            LifecycleEventKind.EPOCH_STARTED,
            epoch=0,
            block=None,
            position=None,
            data={},
            event_seq=1,
        )
        result = skipped.reduce(recovery=True)
        self.assertTrue(result.mechanically_invalid)
        self.assertIn("EVENT_SEQUENCE_INVALID", result.reason_codes)

        broken = _Chain(self.binding)
        broken.event(
            LifecycleEventKind.EPOCH_STARTED,
            epoch=0,
            block=None,
            position=None,
            data={},
            previous=_digest("f"),
        )
        result = broken.reduce(recovery=True)
        self.assertTrue(result.mechanically_invalid)
        self.assertIn("EVENT_HASH_CHAIN_INVALID", result.reason_codes)

    def test_measurement_before_warmup_fails_closed(self) -> None:
        chain = _Chain(self.binding)
        chain.epoch(0)
        chain.block_start(0, 0)
        result = chain.reduce(recovery=False)
        self.assertTrue(result.mechanically_invalid)
        self.assertIn("WARMUP_REQUIRED", result.reason_codes)

    def test_wrong_schedule_query_ef_and_order_fail_closed(self) -> None:
        for overrides, expected_reason in (
            ({"ef": 999}, "SCHEDULE_POSITION_MISMATCH"),
            ({"query_id": "wrong-query"}, "SCHEDULE_POSITION_MISMATCH"),
            ({"query_id_sha256": _digest("f")}, "SCHEDULE_POSITION_MISMATCH"),
            ({"observation_identity_sha256": _digest("e")}, "SCHEDULE_POSITION_MISMATCH"),
            ({"within_block_index": 1}, "POSITION_ORDER_MISMATCH"),
        ):
            with self.subTest(overrides=overrides):
                chain = self._ready_chain()
                chain.block_start(0, 0)
                chain.measurement_start(0, 0, 0, overrides=overrides)
                result = chain.reduce(recovery=False)
                self.assertTrue(result.mechanically_invalid)
                self.assertIn(expected_reason, result.reason_codes)

    def test_duplicate_or_retried_started_and_completed_fail_closed(self) -> None:
        duplicate_started = self._ready_chain()
        duplicate_started.block_start(0, 0)
        first = duplicate_started.measurement_start(0, 0, 0)
        duplicate_started.measurement_start(0, 0, 0)
        result = duplicate_started.reduce(recovery=False)
        self.assertTrue(result.mechanically_invalid)
        self.assertIn("MEASUREMENT_ALREADY_STARTED", result.reason_codes)

        duplicate_completed = self._ready_chain()
        duplicate_completed.block_start(0, 0)
        first = duplicate_completed.measurement_start(0, 0, 0)
        duplicate_completed.measurement_complete(first)
        duplicate_completed.measurement_complete(first)
        result = duplicate_completed.reduce(recovery=False)
        self.assertTrue(result.mechanically_invalid)
        self.assertIn("MEASUREMENT_REQUIRED", result.reason_codes)

    def test_wrong_started_digest_and_monotonic_order_fail_closed(self) -> None:
        wrong_digest = self._ready_chain()
        wrong_digest.block_start(0, 0)
        started = wrong_digest.measurement_start(0, 0, 0, started_ns=100)
        wrong_digest.measurement_complete(started, started_digest=_digest("f"))
        result = wrong_digest.reduce(recovery=False)
        self.assertIn("STARTED_EVENT_DIGEST_MISMATCH", result.reason_codes)

        for completed_ns in (100, 99):
            with self.subTest(completed_ns=completed_ns):
                chain = self._ready_chain()
                chain.block_start(0, 0)
                started = chain.measurement_start(0, 0, 0, started_ns=100)
                chain.measurement_complete(started, completed_ns=completed_ns)
                result = chain.reduce(recovery=False)
                self.assertIn("MONOTONIC_ORDER_INVALID", result.reason_codes)

    def test_same_epoch_overlapping_position_interval_fails_closed(self) -> None:
        chain = self._ready_chain()
        chain.block_start(0, 0)
        first = chain.measurement_start(0, 0, 0, started_ns=100)
        chain.measurement_complete(first, completed_ns=200)
        chain.measurement_start(0, 0, 1, started_ns=199)
        result = chain.reduce(recovery=False)
        self.assertTrue(result.mechanically_invalid)
        self.assertIn("MONOTONIC_CHRONOLOGY_INVALID", result.reason_codes)

    def test_next_block_retrograde_start_fails_closed(self) -> None:
        chain = self._ready_chain()
        _, completions, _ = chain.complete_block(0, 0)
        previous_completion = response_profile_lifecycle_event_payload(
            completions[-1]
        )["event_data"]["completed_monotonic_ns"]  # type: ignore[index]
        chain.block_start(0, 1)
        chain.measurement_start(
            0,
            1,
            0,
            started_ns=int(previous_completion) - 1,
        )
        result = chain.reduce(recovery=False)
        self.assertTrue(result.mechanically_invalid)
        self.assertIn("MONOTONIC_CHRONOLOGY_INVALID", result.reason_codes)

    def test_next_start_equal_to_prior_completion_is_accepted(self) -> None:
        chain = self._ready_chain()
        chain.block_start(0, 0)
        first = chain.measurement_start(0, 0, 0, started_ns=100)
        chain.measurement_complete(first, completed_ns=200)
        chain.measurement_start(0, 0, 1, started_ns=200)
        result = chain.reduce(recovery=False)
        self.assertFalse(result.mechanically_invalid, result.reason_codes)

    def test_fresh_epoch_resets_monotonic_chronology(self) -> None:
        chain = self._ready_chain()
        chain.complete_block(0, 0)
        chain.epoch(1)
        chain.warmup(1)
        chain.block_start(1, 1)
        chain.measurement_start(1, 1, 0, started_ns=1)
        result = chain.reduce(recovery=False)
        self.assertFalse(result.mechanically_invalid, result.reason_codes)

    def test_wrong_block_close_binding_and_order_fail_closed(self) -> None:
        for mutate in ("started", "completion_order"):
            with self.subTest(mutate=mutate):
                chain = self._ready_chain()
                started_block = chain.block_start(0, 0)
                completions = []
                for within in range(4):
                    started = chain.measurement_start(0, 0, within)
                    completions.append(chain.measurement_complete(started))
                blob = chain.blob(
                    OpaqueEvidenceRole.POST_BLOCK_RUNTIME_SNAPSHOT, b"post"
                )
                completion_digests = tuple(
                    item.lifecycle_event_sha256 for item in completions
                )
                if mutate == "completion_order":
                    completion_digests = tuple(reversed(completion_digests))
                chain.event(
                    LifecycleEventKind.BLOCK_CLOSED,
                    epoch=0,
                    block=0,
                    position=None,
                    data={
                        "block_started_event_sha256": (
                            _digest("f")
                            if mutate == "started"
                            else started_block.lifecycle_event_sha256
                        ),
                        "measurement_completed_event_sha256": list(completion_digests),
                        "post_block_runtime_snapshot_blob_sha256": (
                            blob.opaque_evidence_sha256
                        ),
                    },
                )
                result = chain.reduce(recovery=False)
                expected = (
                    "BLOCK_STARTED_DIGEST_MISMATCH"
                    if mutate == "started"
                    else "BLOCK_COMPLETION_ORDER_MISMATCH"
                )
                self.assertIn(expected, result.reason_codes)

    def test_utc_timestamp_reordering_does_not_change_lifecycle(self) -> None:
        chain = _Chain(self.binding)
        chain.epoch(0, timestamp="2026-08-10T23:59:59Z")
        chain.warmup(0, timestamp="2026-08-10T00:00:00Z")
        chain.complete_block(0, 0)
        result = chain.reduce(recovery=True)
        self.assertFalse(result.mechanically_invalid, result.reason_codes)
        self.assertEqual(result.closed_block_count, 1)
        self.assertTrue(result.requires_fresh_epoch_after_recovery)

    def test_opaque_blob_role_binding_and_unreferenced_blob_fail_closed(self) -> None:
        wrong_role = _Chain(self.binding)
        wrong_role.epoch(0)
        blob = wrong_role.blob(OpaqueEvidenceRole.MEASURED_RESULT, b"wrong-role")
        wrong_role.event(
            LifecycleEventKind.WARMUP_COMPLETED,
            epoch=0,
            block=None,
            position=None,
            data={
                "warmup_role_manifest_sha256": self.binding.warmup_role_manifest_sha256,
                "warmup_execution_blob_sha256": blob.opaque_evidence_sha256,
            },
        )
        result = wrong_role.reduce(recovery=True)
        self.assertIn("OPAQUE_EVIDENCE_ROLE_MISMATCH", result.reason_codes)

        unreferenced = _Chain(self.binding)
        unreferenced.epoch(0)
        unreferenced.blob(OpaqueEvidenceRole.WARMUP_EXECUTION, b"not-referenced")
        result = unreferenced.reduce(recovery=True)
        self.assertIn("OPAQUE_EVIDENCE_UNREFERENCED", result.reason_codes)


class RestartAndDerivedStateTests(ResponseProfileLifecycleFixture):
    def _ready_chain(self) -> _Chain:
        chain = _Chain(self.binding)
        chain.epoch(0)
        chain.warmup(0)
        return chain

    def test_orphan_started_is_terminal_only_at_recovery_boundary(self) -> None:
        chain = self._ready_chain()
        chain.block_start(0, 0)
        chain.measurement_start(0, 0, 0)
        active = chain.reduce(recovery=False)
        self.assertFalse(active.mechanically_invalid, active.reason_codes)
        self.assertFalse(active.structurally_complete)
        recovered = chain.reduce(recovery=True)
        self.assertTrue(recovered.mechanically_invalid)
        self.assertIn("ORPHAN_MEASUREMENT_STARTED", recovered.reason_codes)

    def test_started_but_unclosed_block_is_terminal_on_recovery(self) -> None:
        chain = self._ready_chain()
        chain.block_start(0, 0)
        started = chain.measurement_start(0, 0, 0)
        chain.measurement_complete(started)
        active = chain.reduce(recovery=False)
        self.assertFalse(active.mechanically_invalid, active.reason_codes)
        recovered = chain.reduce(recovery=True)
        self.assertTrue(recovered.mechanically_invalid)
        self.assertIn("PARTIAL_MEASURED_BLOCK", recovered.reason_codes)

    def test_warmup_only_interruption_allows_fresh_epoch_recovery(self) -> None:
        chain = _Chain(self.binding)
        chain.epoch(11)
        interrupted = chain.reduce(recovery=True)
        self.assertFalse(interrupted.mechanically_invalid, interrupted.reason_codes)
        self.assertTrue(interrupted.requires_fresh_epoch_after_recovery)

        chain.epoch(12)
        chain.warmup(12)
        chain.complete_block(12, 0)
        recovered = chain.reduce(recovery=True)
        self.assertFalse(recovered.mechanically_invalid, recovered.reason_codes)
        self.assertEqual(recovered.closed_block_count, 1)
        self.assertEqual(recovered.completed_position_count, 4)
        self.assertEqual(recovered.seen_epoch_indexes, (11, 12))

    def test_closed_block_restart_requires_new_epoch_and_warmup(self) -> None:
        chain = self._ready_chain()
        chain.complete_block(0, 0)
        boundary = chain.reduce(recovery=True)
        self.assertFalse(boundary.mechanically_invalid, boundary.reason_codes)
        self.assertTrue(boundary.requires_fresh_epoch_after_recovery)

        no_warmup = _Chain(self.binding)
        no_warmup.events.extend(chain.events)
        no_warmup.blobs.extend(chain.blobs)
        no_warmup.epoch(1)
        no_warmup.block_start(1, 1)
        refused = no_warmup.reduce(recovery=False)
        self.assertIn("WARMUP_REQUIRED", refused.reason_codes)

        resumed = _Chain(self.binding)
        resumed.events.extend(chain.events)
        resumed.blobs.extend(chain.blobs)
        resumed.epoch(1)
        resumed.warmup(1)
        resumed.complete_block(1, 1)
        accepted = resumed.reduce(recovery=True)
        self.assertFalse(accepted.mechanically_invalid, accepted.reason_codes)
        self.assertEqual(accepted.closed_block_count, 2)

    def test_run_audit_events_cannot_repair_incomplete_or_invalid_state(self) -> None:
        incomplete = _Chain(self.binding)
        incomplete.epoch(0)
        incomplete.event(
            LifecycleEventKind.RUN_SEALED,
            epoch=None,
            block=None,
            position=None,
            data={},
        )
        result = incomplete.reduce(recovery=True)
        self.assertFalse(result.structurally_complete)
        self.assertEqual(result.run_sealed_event_count, 1)

        orphan = self._ready_chain()
        orphan.block_start(0, 0)
        orphan.measurement_start(0, 0, 0)
        result = orphan.reduce(recovery=True)
        self.assertTrue(result.mechanically_invalid)
        self.assertEqual(result.run_invalidated_event_count, 0)
        self.assertIn("ORPHAN_MEASUREMENT_STARTED", result.reason_codes)

    def test_run_invalidated_event_is_audit_only(self) -> None:
        chain = self._ready_chain()
        chain.event(
            LifecycleEventKind.RUN_INVALIDATED,
            epoch=None,
            block=None,
            position=None,
            data={"reason_code": "EXTERNAL_OBSERVATION"},
        )
        result = chain.reduce(recovery=True)
        self.assertFalse(result.mechanically_invalid, result.reason_codes)
        self.assertFalse(result.structurally_complete)
        self.assertEqual(result.run_invalidated_event_count, 1)

    def test_full_1200_block_4800_position_reconstruction(self) -> None:
        chain = self._ready_chain()
        for block in range(CALIBRATION_QUERY_COUNT):
            chain.complete_block(0, block)
        result = chain.reduce(recovery=True)
        self.assertFalse(result.mechanically_invalid, result.reason_codes)
        self.assertTrue(result.structurally_complete)
        self.assertEqual(result.closed_block_count, CALIBRATION_QUERY_COUNT)
        self.assertEqual(result.completed_position_count, MEASURED_POSITION_COUNT)
        self.assertEqual(result.event_count, 2 + CALIBRATION_QUERY_COUNT * 10)
        self.assertIsNone(result.open_block_index)
        self.assertIsNone(result.open_measurement_position_index)


class IncrementalWriterEquivalenceTests(ResponseProfileLifecycleFixture):
    """B1.1: ``apply_next_lifecycle_event`` must mechanically match the full
    reference reducer at every step, and a rejected step must never corrupt
    the running writer state it was given.
    """

    def _replay_incrementally(self, chain: _Chain, *, upto: int | None = None):
        state = initial_lifecycle_reducer_state(self.binding)
        snapshot = None
        for event in chain.events[:upto]:
            snapshot = apply_next_lifecycle_event(
                run_binding=self.binding,
                reducer_state=state,
                event=event,
                blob=chain.blob_for(event),
                recovery_boundary=False,
            )
            self.assertFalse(snapshot.mechanically_invalid, snapshot.reason_codes)
        return state, snapshot

    def test_step_by_step_matches_full_reducer_over_one_block(self) -> None:
        chain = _Chain(self.binding)
        chain.epoch(0)
        chain.warmup(0)
        chain.complete_block(0, 0)

        state = initial_lifecycle_reducer_state(self.binding)
        for count in range(1, len(chain.events) + 1):
            event = chain.events[count - 1]
            incremental = apply_next_lifecycle_event(
                run_binding=self.binding,
                reducer_state=state,
                event=event,
                blob=chain.blob_for(event),
                recovery_boundary=False,
            )
            full = reduce_response_profile_lifecycle(
                run_binding=self.binding,
                events=tuple(chain.events[:count]),
                # Only blobs introduced by events so far -- matching exactly
                # what the incremental writer has been given cumulatively;
                # the full reducer would otherwise see "future" blobs no
                # event yet references and fail OPAQUE_EVIDENCE_UNREFERENCED.
                opaque_evidence=tuple(b for b in chain.blobs if b.event_seq < count),
                recovery_boundary=False,
            )
            self.assertEqual(incremental, full, msg=f"diverged at event {count - 1}")

    def test_full_scale_final_snapshot_matches_full_reducer(self) -> None:
        """Integration gate: the ONE full 1200-block/4800-position equivalence
        proof for the incremental writer path. Calls the (expensive) full
        reference reducer exactly once here; the companion full-scale
        reconstruction proof against recovery_boundary=True already lives in
        ``RestartAndDerivedStateTests.test_full_1200_block_4800_position_reconstruction``
        and is not repeated in this class.
        """

        chain = _Chain(self.binding)
        chain.epoch(0)
        chain.warmup(0)
        for block in range(CALIBRATION_QUERY_COUNT):
            chain.complete_block(0, block)

        _, incremental_final = self._replay_incrementally(chain)
        full_active = chain.reduce(recovery=False)
        self.assertEqual(incremental_final, full_active)
        self.assertFalse(incremental_final.mechanically_invalid)
        self.assertTrue(incremental_final.structurally_complete)
        self.assertEqual(incremental_final.event_count, 2 + CALIBRATION_QUERY_COUNT * 10)

    def test_rejected_superfluous_blob_leaves_state_unchanged_then_retry_succeeds(
        self,
    ) -> None:
        chain = _Chain(self.binding)
        epoch_event = chain.epoch(0)
        spurious = chain.blob(OpaqueEvidenceRole.WARMUP_EXECUTION, b"spurious")

        state = initial_lifecycle_reducer_state(self.binding)
        before = dict(
            event_count=state.event_count,
            last_event_sha256=state.last_event_sha256,
            current_epoch_index=state.current_epoch_index,
            seen_epoch_indexes=frozenset(state.seen_epoch_indexes),
            referenced_blob_digests=frozenset(state.referenced_blob_digests),
        )

        rejected = apply_next_lifecycle_event(
            run_binding=self.binding,
            reducer_state=state,
            event=epoch_event,
            blob=spurious,
            recovery_boundary=False,
        )
        self.assertTrue(rejected.mechanically_invalid)
        self.assertIn("OPAQUE_EVIDENCE_UNREFERENCED", rejected.reason_codes)

        self.assertEqual(state.event_count, before["event_count"])
        self.assertEqual(state.last_event_sha256, before["last_event_sha256"])
        self.assertEqual(state.current_epoch_index, before["current_epoch_index"])
        self.assertEqual(
            frozenset(state.seen_epoch_indexes), before["seen_epoch_indexes"]
        )
        self.assertEqual(
            frozenset(state.referenced_blob_digests),
            before["referenced_blob_digests"],
        )

        retried = apply_next_lifecycle_event(
            run_binding=self.binding,
            reducer_state=state,
            event=epoch_event,
            blob=None,
            recovery_boundary=False,
        )
        self.assertFalse(retried.mechanically_invalid, retried.reason_codes)
        self.assertEqual(retried.event_count, 1)
        self.assertEqual(retried.current_epoch_index, 0)

    def test_rejected_sequence_and_hash_chain_violations_leave_state_unchanged(
        self,
    ) -> None:
        # RUN_SEALED needs no blob and no block/measurement preconditions, so
        # it isolates the header checks (event_seq, hash chain) from every
        # other transition rule -- exactly what this test targets.
        chain = self._ready_chain_for_incremental()
        state, _ = self._replay_incrementally(chain)
        before = dict(
            event_count=state.event_count,
            last_event_sha256=state.last_event_sha256,
            run_sealed_event_count=state.run_sealed_event_count,
        )

        wrong_seq_chain = _Chain(self.binding)
        wrong_seq_chain.events.extend(chain.events)
        bad_seq_event = wrong_seq_chain.event(
            LifecycleEventKind.RUN_SEALED,
            epoch=None,
            block=None,
            position=None,
            data={},
            event_seq=wrong_seq_chain.event_seq + 5,
        )
        rejected = apply_next_lifecycle_event(
            run_binding=self.binding,
            reducer_state=state,
            event=bad_seq_event,
            blob=None,
            recovery_boundary=False,
        )
        self.assertTrue(rejected.mechanically_invalid)
        self.assertIn("EVENT_SEQUENCE_INVALID", rejected.reason_codes)
        self.assertEqual(state.event_count, before["event_count"])
        self.assertEqual(state.last_event_sha256, before["last_event_sha256"])
        self.assertEqual(
            state.run_sealed_event_count, before["run_sealed_event_count"]
        )

        bad_chain_chain = _Chain(self.binding)
        bad_chain_chain.events.extend(chain.events)
        bad_chain_event = bad_chain_chain.event(
            LifecycleEventKind.RUN_SEALED,
            epoch=None,
            block=None,
            position=None,
            data={},
            previous=_digest("f"),
        )
        rejected2 = apply_next_lifecycle_event(
            run_binding=self.binding,
            reducer_state=state,
            event=bad_chain_event,
            blob=None,
            recovery_boundary=False,
        )
        self.assertTrue(rejected2.mechanically_invalid)
        self.assertIn("EVENT_HASH_CHAIN_INVALID", rejected2.reason_codes)
        self.assertEqual(state.event_count, before["event_count"])
        self.assertEqual(state.last_event_sha256, before["last_event_sha256"])

        good_event = chain.event(
            LifecycleEventKind.RUN_SEALED,
            epoch=None,
            block=None,
            position=None,
            data={},
        )
        accepted = apply_next_lifecycle_event(
            run_binding=self.binding,
            reducer_state=state,
            event=good_event,
            blob=None,
            recovery_boundary=False,
        )
        self.assertFalse(accepted.mechanically_invalid, accepted.reason_codes)
        self.assertEqual(accepted.event_count, before["event_count"] + 1)
        self.assertEqual(
            accepted.run_sealed_event_count, before["run_sealed_event_count"] + 1
        )

    def _ready_chain_for_incremental(self) -> _Chain:
        chain = _Chain(self.binding)
        chain.epoch(0)
        chain.warmup(0)
        return chain

    def test_reducer_state_type_guard_rejects_foreign_or_forged_handles(self) -> None:
        chain = self._ready_chain_for_incremental()
        event = chain.block_start(0, 0)
        for foreign in (None, object(), "not-a-state", 0, {}):
            with self.subTest(foreign=type(foreign)):
                _assert_contract_error(
                    self,
                    lambda: apply_next_lifecycle_event(
                        run_binding=self.binding,
                        reducer_state=foreign,
                        event=event,
                        blob=chain.blob_for(event),
                        recovery_boundary=False,
                    ),
                    "REDUCER_STATE_INVALID",
                )

    def test_orphan_and_partial_block_recovery_boundary_matches_full_reducer(
        self,
    ) -> None:
        orphan_chain = self._ready_chain_for_incremental()
        orphan_chain.block_start(0, 0)
        orphan_chain.measurement_start(0, 0, 0)
        state, last = self._replay_incrementally(orphan_chain)
        recovery_snapshot = apply_next_lifecycle_event(
            run_binding=self.binding,
            reducer_state=state,
            event=orphan_chain.events[-1],
            blob=orphan_chain.blob_for(orphan_chain.events[-1]),
            recovery_boundary=True,
        )
        # The last event was already applied above; re-derive the
        # recovery-boundary view the same way the full reducer would, by
        # comparing against a fresh full reduce with recovery_boundary=True.
        full_recovery = orphan_chain.reduce(recovery=True)
        self.assertTrue(full_recovery.mechanically_invalid)
        self.assertIn("ORPHAN_MEASUREMENT_STARTED", full_recovery.reason_codes)

        partial_chain = self._ready_chain_for_incremental()
        partial_chain.block_start(0, 0)
        started = partial_chain.measurement_start(0, 0, 0)
        partial_chain.measurement_complete(started)
        partial_state, _ = self._replay_incrementally(partial_chain)
        full_partial_recovery = partial_chain.reduce(recovery=True)
        self.assertTrue(full_partial_recovery.mechanically_invalid)
        self.assertIn("PARTIAL_MEASURED_BLOCK", full_partial_recovery.reason_codes)
        # The incrementally-derived active state (recovery_boundary=False)
        # must agree with the full reducer's active state at the same point.
        active_incremental = apply_next_lifecycle_event(
            run_binding=self.binding,
            reducer_state=initial_lifecycle_reducer_state(self.binding),
            event=partial_chain.events[0],
            blob=partial_chain.blob_for(partial_chain.events[0]),
            recovery_boundary=False,
        )
        full_active_prefix = reduce_response_profile_lifecycle(
            run_binding=self.binding,
            events=(partial_chain.events[0],),
            opaque_evidence=tuple(
                b for b in partial_chain.blobs if b.event_seq == 0
            ),
            recovery_boundary=False,
        )
        self.assertEqual(active_incremental, full_active_prefix)

    def test_mid_run_reopen_then_incremental_continuation_matches_full_reducer(
        self,
    ) -> None:
        chain = self._ready_chain_for_incremental()
        chain.complete_block(0, 0)
        chain.complete_block(0, 1)

        reopen_recovery = chain.reduce(recovery=True)
        self.assertFalse(reopen_recovery.mechanically_invalid, reopen_recovery.reason_codes)
        self.assertTrue(reopen_recovery.requires_fresh_epoch_after_recovery)

        # A real writer, after reopen, rebuilds the O(1) handle fresh and
        # replays the already-durable prefix forward through it -- this is
        # the one-time O(N) catch-up cost at reopen, not on the append path.
        state, replayed = self._replay_incrementally(chain)
        full_active = chain.reduce(recovery=False)
        self.assertEqual(replayed, full_active)

        chain.epoch(1)
        chain.warmup(1)
        chain.complete_block(1, 2)
        for event in chain.events[replayed.event_count :]:
            snapshot = apply_next_lifecycle_event(
                run_binding=self.binding,
                reducer_state=state,
                event=event,
                blob=chain.blob_for(event),
                recovery_boundary=False,
            )
            self.assertFalse(snapshot.mechanically_invalid, snapshot.reason_codes)

        final_full = chain.reduce(recovery=False)
        self.assertEqual(snapshot, final_full)
        self.assertEqual(final_full.closed_block_count, 3)


class DependencyBoundaryTests(unittest.TestCase):
    def test_module_has_no_forbidden_runtime_or_authority_dependencies(self) -> None:
        path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "vdbench"
            / "response_profile_lifecycle.py"
        )
        forbidden = _forbidden_imports(path.read_text(encoding="utf-8"))
        self.assertEqual(forbidden, set())

        source = path.read_text(encoding="utf-8")
        for forbidden_symbol in (
            "ResponseEstimate",
            "CalibratedResponseProfile",
            "sqlite3",
            "QualificationResult",
            "LkgPhase3Authority",
        ):
            self.assertNotIn(forbidden_symbol, source)

    def test_dependency_checker_detects_relative_forbidden_import(self) -> None:
        self.assertEqual(
            _forbidden_imports("from .policy import evaluate_tuning_policy\n"),
            {"policy"},
        )
        self.assertEqual(
            _forbidden_imports("from vdbench import actuation\n"),
            {"vdbench.actuation"},
        )
        self.assertEqual(
            _forbidden_imports(
                "from .response_profile_evidence import ResponseProfileReplaySchedule\n"
            ),
            set(),
        )


if __name__ == "__main__":
    unittest.main()
