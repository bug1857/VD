from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

import numpy as np

from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.drift import build_evidence_provenance
from vdbench.response_profile import SUPPORTED_EFS
from vdbench.response_profile_control import build_response_profile_control
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
from vdbench.response_profile_lifecycle import build_response_profile_run_binding
from vdbench.response_profile_lifecycle_ledger import ResponseProfileLifecycleLedger
from vdbench.response_profile_producer import (
    ResponseProfileExecutionQuery,
    ResponseProfileProducer,
    _SystemClock,
    build_response_profile_runtime_readiness,
    build_response_profile_search_result,
)
from vdbench.response_profile_semantic import (
    build_response_profile_oracle_manifest,
    build_response_profile_oracle_record,
    build_response_profile_static_identity,
)
from vdbench.shadow_event_types import MonitorStreamKey


MODULE = Path(__file__).parents[1] / "src" / "vdbench" / "response_profile_producer.py"


def _digest(character: str) -> str:
    return character * 64


def _flat() -> SearchConfiguration:
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
            vector_identity=vector, search_configuration=_flat()
        ),
    )


class _Clock:
    def __init__(self) -> None:
        self._utc = datetime(2026, 8, 10, tzinfo=timezone.utc)
        self._monotonic = 0

    def utc_now(self) -> str:
        self._utc += timedelta(microseconds=1)
        return self._utc.isoformat(timespec="microseconds").replace("+00:00", "Z")

    def monotonic_ns(self) -> int:
        self._monotonic += 1_000_000
        return self._monotonic


class _Probe:
    def __init__(self, *, healthy: bool = True) -> None:
        self.healthy = healthy

    def collect(self):
        return build_response_profile_runtime_readiness(
            collection_loaded=self.healthy,
            milvus_healthy=self.healthy,
            etcd_healthy=self.healthy,
            minio_healthy=self.healthy,
        )


class _Executor:
    def __init__(self, ledger: ResponseProfileLifecycleLedger, *, fail_at: int | None = None) -> None:
        self.ledger = ledger
        self.calls: list[ResponseProfileExecutionQuery] = []
        self.fail_at = fail_at

    def execute(self, query: ResponseProfileExecutionQuery):
        self.calls.append(query)
        if query.measured:
            self.assert_started_before_search()
        if self.fail_at is not None and len(self.calls) == self.fail_at:
            raise RuntimeError("deliberate search failure")
        return build_response_profile_search_result(
            candidate_ids=(), candidate_distances=()
        )

    def assert_started_before_search(self) -> None:
        if self.ledger.current_view().open_measurement_position_index is None:
            raise AssertionError("measured search ran before durable STARTED")


class ResponseProfileProducerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        namespace = build_artifact_source_namespace(
            dataset_id="DATASET-EXP010-PRODUCER",
            dataset_version="v1",
            generation_manifest_sha256=_digest("a"),
        )
        calibration_members = tuple(
            _member(index, namespace=namespace)
            for index in range(CALIBRATION_QUERY_COUNT)
        )
        calibration = build_response_profile_role_manifest(
            role=build_response_profile_role(
                kind=ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION
            ),
            members=calibration_members,
        )
        cls.population = build_calibration_population_manifest(
            cell=build_response_profile_cell(
                metric=Metric.L2, threshold_stratum="target-075"
            ),
            calibration_role_manifest=calibration,
        )
        cls.schedule = build_response_profile_replay_schedule(
            population=cls.population, source_revision="revision/r2-f-v1"
        )
        warmup = build_response_profile_role_manifest(
            role=build_response_profile_role(
                kind=ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP
            ),
            members=tuple(
                _member(index + 20_000, namespace=namespace, offset=40_000.0)
                for index in range(WARMUP_QUERY_COUNT)
            ),
        )
        cls.binding = build_response_profile_run_binding(
            run_id="exp010-r2-f",
            created_at_utc="2026-08-10T00:00:00Z",
            population=cls.population,
            replay_schedule=cls.schedule,
            warmup_role_manifest=warmup,
            source_revision="revision/r2-f-v1",
        )
        configurations = tuple(
            SearchConfiguration(
                metric=Metric.L2,
                threshold_label="target-075",
                radius=0.75,
                index_track=IndexTrack.HNSW,
                ef=ef,
            )
            for ef in SUPPORTED_EFS
        )
        provenance = build_evidence_provenance(
            metric=Metric.L2,
            threshold_stratum="target-075",
            reference_window_id="detector-reference",
            current_window_id="detector-current",
            reference_manifest_sha256=_digest("d"),
            current_manifest_sha256=_digest("e"),
            configuration_identity="detector-configuration-r2-f-v1",
            data_identity="data-r2-f-v1",
            flat_binding_id="flat-r2-f-v1",
            hnsw_binding_id="hnsw-r2-f-v1",
            reference_audit_ids=tuple(range(50)),
            reference_audit_rank_digests=tuple(_digest("1") for _ in range(50)),
            current_audit_ids=tuple(range(50, 100)),
            current_audit_rank_digests=tuple(_digest("2") for _ in range(50)),
        )
        cls.control = build_response_profile_control(
            stream_key=MonitorStreamKey(
                "detector-stream-r2-f-v1",
                Metric.L2,
                "target-075",
                "detector-configuration-r2-f-v1",
                "data-r2-f-v1",
                "flat-r2-f-v1",
                "hnsw-r2-f-v1",
            ),
            detector_provenance=provenance,
            trigger_window_sequence=2,
            detector_head_sha256=_digest("4"),
            detector_head_record_sequence=0,
            detector_head_record_sha256=_digest("5"),
            detector_head_persisted_at_utc="2026-08-09T23:59:58Z",
            calibration_population_sha256=cls.population.workload_manifest_sha256,
            warmup_role_manifest_sha256=warmup.role_manifest_sha256,
            ordered_query_payload_sha256=cls.population.ordered_query_payload_sha256,
            replay_schedule_sha256=cls.schedule.replay_schedule_sha256,
            environment_manifest_sha256=_digest("c"),
            source_revision="revision/r2-f-v1",
            frozen_at_utc="2026-08-09T23:59:59Z",
        )
        cls.static_identity = build_response_profile_static_identity(
            metric=Metric.L2,
            threshold_stratum="target-075",
            search_configurations=configurations,
            hnsw_index_identity="hnsw-r2-f-v1",
            data_identity="data-r2-f-v1",
            workload_manifest_sha256=cls.population.workload_manifest_sha256,
            ordered_query_payload_sha256=cls.population.ordered_query_payload_sha256,
            replay_schedule_sha256=cls.schedule.replay_schedule_sha256,
            control_profile_sha256=cls.control.control_profile_sha256,
            environment_manifest_sha256=_digest("c"),
            source_revision="revision/r2-f-v1",
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
        cls.oracle = build_response_profile_oracle_manifest(
            population=cls.population, records=records
        )

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "producer.sqlite3"

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _ledger(self) -> ResponseProfileLifecycleLedger:
        return ResponseProfileLifecycleLedger(
            self.path, expected_run_binding=self.binding
        )

    def _producer(
        self,
        ledger: ResponseProfileLifecycleLedger,
        executor: _Executor,
        *,
        probe: _Probe | None = None,
    ):
        return ResponseProfileProducer(
            ledger=ledger,
            run_binding=self.binding,
            static_identity=self.static_identity,
            control=self.control,
            oracle_manifest=self.oracle,
            query_executor=executor,
            runtime_probe=_Probe() if probe is None else probe,
            clock=_Clock(),
        )

    def test_bounded_run_commits_started_before_search_and_stops_closed(self) -> None:
        with self._ledger() as ledger:
            executor = _Executor(ledger)
            result = self._producer(ledger, executor).run(max_blocks=1)
            self.assertFalse(result.complete)
            self.assertEqual(result.reason_codes, ("BOUNDED_PROGRESS",))
            self.assertEqual(result.closed_block_count, 1)
            self.assertEqual(result.completed_position_count, 4)
            self.assertEqual(len(executor.calls), 804)
            self.assertEqual(sum(item.measured for item in executor.calls), 4)
            self.assertIsNone(ledger.current_view().open_block_index)

    def test_closed_block_restart_uses_new_epoch_and_full_warmup(self) -> None:
        with self._ledger() as ledger:
            self._producer(ledger, _Executor(ledger)).run(max_blocks=1)
        with self._ledger() as reopened:
            executor = _Executor(reopened)
            result = self._producer(reopened, executor).run(max_blocks=1)
            self.assertEqual(result.closed_block_count, 2)
            self.assertEqual(len(executor.calls), 804)
            self.assertEqual(reopened.current_view().seen_epoch_indexes, (0, 1))

    def test_failed_measured_search_is_persisted_once_and_never_retried(self) -> None:
        with self._ledger() as ledger:
            executor = _Executor(ledger, fail_at=801)
            result = self._producer(ledger, executor).run(max_blocks=1)
            self.assertFalse(result.complete)
            self.assertEqual(result.reason_codes, ("SEARCH_FAILED",))
            self.assertEqual(result.measured_search_calls, 1)
            self.assertEqual(ledger.current_view().completed_position_count, 1)
        with self._ledger() as reopened:
            self.assertTrue(reopened.current_view().terminal_recovery)

    def test_unhealthy_pre_snapshot_dispatches_zero_measured_searches(self) -> None:
        with self._ledger() as ledger:
            executor = _Executor(ledger)
            result = self._producer(
                ledger, executor, probe=_Probe(healthy=False)
            ).run(max_blocks=1)
            self.assertEqual(result.reason_codes, ("RUNTIME_READINESS_FAILED",))
            self.assertEqual(sum(item.measured for item in executor.calls), 0)
            self.assertEqual(len(executor.calls), 800)
            self.assertEqual(ledger.current_view().completed_position_count, 0)

    def test_complete_run_exports_and_r2c_verifies_all_positions(self) -> None:
        with self._ledger() as ledger:
            executor = _Executor(ledger)
            result = self._producer(ledger, executor).run()
            self.assertTrue(result.complete)
            self.assertEqual(result.closed_block_count, 1200)
            self.assertEqual(result.completed_position_count, 4800)
            self.assertEqual(result.warmup_search_calls, 800)
            self.assertEqual(result.measured_search_calls, 4800)
            self.assertEqual(len(executor.calls), 5600)
            self.assertIsNotNone(result.semantic_verification)
            self.assertEqual(
                len(result.semantic_verification.report.observations), 1200  # type: ignore[union-attr]
            )

    def test_module_has_no_candidate_or_live_service_dependencies(self) -> None:
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
            "policy", "canary_admission", "canary_live_runner", "actuation",
            "pymilvus", "lkg_phase3_authority",
        }
        self.assertFalse({
            item for item in imported
            if any(item == name or item.endswith(f".{name}") for name in forbidden)
        })

    def test_default_system_clock_produces_strict_rfc3339_timestamps(self) -> None:
        """Regression test: the default clock's own timestamps must satisfy
        `response_profile_lifecycle.py`'s strict RFC3339 validator (at most 6
        fractional digits), found and fixed while building the EXP-011 live
        acquisition composition root -- any real (clock=None) producer run
        previously failed on its very first durable event with
        TIMESTAMP_INVALID because the default clock emitted 9 fractional
        (nanosecond) digits."""

        value = _SystemClock().utc_now()
        self.assertRegex(
            value,
            r"\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,6}Z\Z",
        )
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        self.assertIsNotNone(parsed.tzinfo)
        self.assertEqual(parsed.utcoffset(), timezone.utc.utcoffset(parsed))


if __name__ == "__main__":
    unittest.main()
