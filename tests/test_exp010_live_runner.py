"""ADR-014 coverage for the EXP-010 operator launcher and windowing loop.

Fully offline: serving and shadow capture are injected fakes, construction
contacts nothing, and no gate beyond in-memory composition is executed. The
runner never manufactures a query -- every source in these tests originates
from an explicit `serve(...)` call, which is the genuine-workload boundary.
"""

from __future__ import annotations

import ast
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from tests.test_shadow_extraction import _identity, _query
from vdbench.config import IndexTrack, Metric
from vdbench.drift import DetectorState
from vdbench.exp010_live_runner import (
    Exp010LiveRunner,
    Exp010LiveRunnerError,
    Exp010OperatorConfiguration,
    build_environment_manifest_sha256,
)
from vdbench.host_observation import RangeQueryRequest, ServedQueryOutcome
from vdbench.gate_c_bounded_execution import GateCWindowExecutionBound
from vdbench.host_window_detector_v2 import HostWindowV2Status
from vdbench.milvus_actuation import ShadowAuditTrace
from vdbench.shadow_window import WINDOW_QUERY_COUNT
from vdbench.v2_shadow_worker import V2ShadowWorkerError
from vdbench.window_finalization import (
    WindowFinalizationPhase,
    restore_prepared_evaluation,
)


def _trace_for(sources, *, metric: Metric = Metric.L2) -> ShadowAuditTrace:
    """One 50-query trace whose identities match the runner's pinned stream.

    `data_identity` must be the DATASET-001-derived value the composition root
    pins, because the real detector copies it into EvidenceProvenance and the
    V2 head requires provenance identities to equal the stream key.
    """

    return ShadowAuditTrace(
        metric=metric,
        threshold_stratum="target-075",
        candidate_ef=400,
        last_known_good_ef=400,
        sentinel_ef=100,
        configuration_identity="config-v1",
        data_identity=DATA_IDENTITY,
        flat_identity=_identity(IndexTrack.FLAT, metric),
        hnsw_identity=_identity(IndexTrack.HNSW, metric),
        queries=tuple(_query(int(item.query_id), metric) for item in sources),
        complete=True,
    )

MODULE_PATH = Path(__file__).parents[1] / "src" / "vdbench" / "exp010_live_runner.py"
DATASET001 = Path(__file__).parents[1] / "artifacts" / "exp-001" / "dataset"
DATA_IDENTITY = (
    "DATASET-001-v1:sha256:"
    "b6cb56a3eee60f6728be1d08a465e2a2500eec4089b4466da76fe2e886b51da9"
)
_ENVIRONMENT = "e" * 64
_REVISION = "revision/exp010-live"


class _Serving:
    """Injected serving port. Records calls; contacts nothing."""

    def __init__(self) -> None:
        self.calls = 0

    def execute(self, request: RangeQueryRequest) -> ServedQueryOutcome:
        self.calls += 1
        return ServedQueryOutcome(True, False, 1, 1.0)


class _ShadowCapture:
    """Injected shadow capture producing valid 50-query traces offline."""

    def __init__(self) -> None:
        self.calls = 0

    def capture(self, sources, *, trace_sequence_index: int):
        self.calls += 1
        return _trace_for(sources)


def _configuration(root: Path) -> Exp010OperatorConfiguration:
    return Exp010OperatorConfiguration(
        milvus_uri="http://milvus.invalid:19530",
        flat_collection_name="exp001_l2_flat",
        hnsw_collection_name="exp001_l2_hnsw",
        metric=Metric.L2,
        threshold_stratum="target-075",
        threshold_radius=2.0,
        served_ef=400,
        detector_seed=20260812,
        stream_id="v2-live",
        configuration_identity="config-v1",
        flat_binding_id="flat-index-v1",
        hnsw_binding_id="hnsw-index-v1",
        source_revision=_REVISION,
        environment_manifest_sha256=_ENVIRONMENT,
        store_root=root / "stores",
        dataset001_dir=DATASET001,
        exp010_output_dir=root / "exp010",
    )


class _Harness:
    def __init__(self, root: Path) -> None:
        self.serving = _Serving()
        self.shadow = _ShadowCapture()
        self._tick = 0
        self.runner = Exp010LiveRunner(
            configuration=_configuration(root),
            serving_executor=self.serving,
            shadow_capture_executor=self.shadow,
            clock=lambda: "2026-08-12T00:00:00Z",
            shadow_captured_at_clock=self._shadow_clock,
        )
        self._request_id = 0

    def _shadow_clock(self) -> str:
        self._tick += 1
        return (
            f"2026-08-12T{self._tick // 3600:02d}:"
            f"{(self._tick // 60) % 60:02d}:{self._tick % 60:02d}Z"
        )

    def serve_many(self, count: int) -> None:
        """Genuine ingress: every source exists because serve() was called."""

        generator = np.random.Generator(np.random.PCG64(99))
        for _ in range(count):
            vector = generator.standard_normal(2).astype("<f4")
            self.runner.serve(
                RangeQueryRequest(
                    self._request_id,
                    self.runner.composition.stream_key,
                    tuple(float(v) for v in vector),
                    2.0, 0.0, 100, 400,
                )
            )
            self._request_id += 1

    def close(self) -> None:
        self.runner.close()


class Exp010LiveRunnerTests(unittest.TestCase):
    def test_construction_contacts_nothing_and_derives_data_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                self.assertEqual(
                    harness.runner.composition.data_identity, DATA_IDENTITY
                )
                self.assertEqual(harness.serving.calls, 0)
                self.assertEqual(harness.shadow.calls, 0)
            finally:
                harness.close()

    def test_runner_generates_no_workload_of_its_own(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                # No serve() call: nothing may be processed into existence.
                self.assertEqual(harness.runner.process_ready_windows(), ())
                self.assertEqual(harness.serving.calls, 0)
                state = harness.runner.trigger_state()
                self.assertFalse(state.trigger_ready)
                self.assertEqual(state.reason, "NO_VERIFIED_REAL_HEAD")
            finally:
                harness.close()

    def test_serve_commits_membership_before_returning_a_visible_response(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                harness.serve_many(1)
                self.assertEqual(harness.serving.calls, 1)
                committed = harness.runner.composition.response_store.poll(
                    consumer_id="probe", limit=5
                )
                self.assertEqual(len(committed), 1)
                self.assertEqual(committed[0].source_sequence, 0)
            finally:
                harness.close()

    def test_incomplete_window_stays_pending(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                harness.serve_many(WINDOW_QUERY_COUNT - 1)
                self.assertEqual(harness.runner.process_ready_windows(), ())
                self.assertEqual(harness.shadow.calls, 0)
            finally:
                harness.close()

    def test_bounded_one_window_stops_before_second_ready_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                results = harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                self.assertEqual(tuple(item.window_sequence for item in results), (0,))
                self.assertEqual(harness.shadow.calls, 4)
                self.assertEqual(
                    harness.runner.composition.finalization_store.next_window_sequence(), 1
                )
                acknowledgement = harness.runner.composition.response_store.consumer_acknowledgement_state(
                    consumer_id="v2-shadow"
                )
                self.assertEqual(len(acknowledgement.event_ids), WINDOW_QUERY_COUNT)
                self.assertEqual(
                    harness.runner.composition.shadow_attempt_store.records_for_window(1), ()
                )
            finally:
                harness.close()

    def test_bounded_two_windows_and_resume_at_one_are_exact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 3)
                first = harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                self.assertEqual(tuple(item.window_sequence for item in first), (0,))
                second = harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(1, 1)
                )
                self.assertEqual(tuple(item.window_sequence for item in second), (1,))
                self.assertEqual(harness.shadow.calls, 8)
                self.assertEqual(
                    harness.runner.composition.shadow_attempt_store.records_for_window(2), ()
                )
            finally:
                harness.close()

    def test_bounded_count_two_processes_exactly_first_two_of_three(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 3)
                results = harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 2)
                )
                self.assertEqual(tuple(item.window_sequence for item in results), (0, 1))
                self.assertEqual(harness.shadow.calls, 8)
                self.assertEqual(
                    harness.runner.composition.finalization_store.next_window_sequence(), 2
                )
                self.assertEqual(
                    harness.runner.composition.shadow_attempt_store.records_for_window(2), ()
                )
            finally:
                harness.close()

    def test_bounded_unavailable_or_start_mismatch_refuses_before_capture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                harness.serve_many(WINDOW_QUERY_COUNT)
                with self.assertRaises(Exp010LiveRunnerError) as unavailable:
                    harness.runner.process_ready_windows(
                        execution_bound=GateCWindowExecutionBound(0, 2)
                    )
                self.assertEqual(
                    unavailable.exception.code,
                    "WINDOW_EXECUTION_BOUND_SOURCE_UNAVAILABLE",
                )
                self.assertEqual(harness.shadow.calls, 0)
                harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                calls = harness.shadow.calls
                with self.assertRaises(Exp010LiveRunnerError) as replay:
                    harness.runner.process_ready_windows(
                        execution_bound=GateCWindowExecutionBound(0, 1)
                    )
                self.assertEqual(
                    replay.exception.code, "WINDOW_EXECUTION_BOUND_START_MISMATCH"
                )
                self.assertEqual(harness.shadow.calls, calls)
            finally:
                harness.close()

    def test_oversized_bound_refuses_before_materialization_or_capture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                harness.serve_many(WINDOW_QUERY_COUNT)
                with self.assertRaises(Exp010LiveRunnerError) as raised:
                    harness.runner.process_ready_windows(
                        execution_bound=GateCWindowExecutionBound(0, 10**100)
                    )
                self.assertEqual(
                    raised.exception.code,
                    "WINDOW_EXECUTION_BOUND_SOURCE_UNAVAILABLE",
                )
                self.assertEqual(harness.shadow.calls, 0)
            finally:
                harness.close()

    def test_offline_fifty_ready_windows_bound_invokes_only_window_zero(self) -> None:
        runner = object.__new__(Exp010LiveRunner)

        class _Finalization:
            next_sequence = 0

            def next_window_sequence(self):
                return self.next_sequence

        finalization = _Finalization()
        runner.composition = SimpleNamespace(finalization_store=finalization)
        invoked: list[int] = []
        runner._validate_bounded_windows = lambda _bound: None

        def transition(*, expected_window_sequence):
            invoked.append(expected_window_sequence)
            finalization.next_sequence += 1
            return SimpleNamespace(window_sequence=expected_window_sequence)

        runner._process_next_ready_window = transition
        results = runner.process_ready_windows(
            execution_bound=GateCWindowExecutionBound(0, 1)
        )
        self.assertEqual(invoked, [0])
        self.assertEqual(tuple(item.window_sequence for item in results), (0,))
        self.assertEqual(finalization.next_sequence, 1)

    def test_canonical_200_boundary_and_rebaseline_then_insufficient(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                results = harness.runner.process_ready_windows()
                self.assertEqual(len(results), 2)
                self.assertEqual(results[0].status, HostWindowV2Status.REBASELINE)
                self.assertIsNone(results[0].detector_state)
                self.assertEqual(results[1].status, HostWindowV2Status.EVALUATED)
                self.assertIs(
                    results[1].detector_state, DetectorState.INSUFFICIENT_EVIDENCE
                )
                self.assertTrue(results[1].attested)
                self.assertEqual(harness.shadow.calls, 2 * 4)
            finally:
                harness.close()

    def test_second_adjacent_comparison_may_decide_but_never_auto_captures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 3)
                results = harness.runner.process_ready_windows()
                self.assertEqual(len(results), 3)
                self.assertIn(
                    results[2].detector_state,
                    {DetectorState.DRIFT, DetectorState.NO_DRIFT},
                )
                state = harness.runner.trigger_state()
                if results[2].detector_state is DetectorState.NO_DRIFT:
                    # A verified real NO_DRIFT head must not expose eligibility.
                    self.assertFalse(state.trigger_ready)
                    self.assertTrue(state.reason.startswith("REAL_HEAD_NOT_DRIFT"))
                    with self.assertRaises(Exp010LiveRunnerError) as raised:
                        harness.runner.capture_exp010_population(
                            run_id="r", source_workload_manifest_sha256="a" * 64
                        )
                    self.assertEqual(
                        raised.exception.code, "EXP010_TRIGGER_NOT_READY"
                    )
            finally:
                harness.close()

    def test_plain_head_without_attestation_is_not_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory))
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                harness.runner.process_ready_windows()
                # Structural head exists; only an attested DRIFT head qualifies.
                self.assertFalse(harness.runner.trigger_state().trigger_ready)
            finally:
                harness.close()

    def test_restart_retains_the_exact_next_source_position(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                harness.runner.process_ready_windows()
                committed = harness.runner.composition.response_store.poll(
                    consumer_id="probe", limit=WINDOW_QUERY_COUNT * 3
                )
                self.assertEqual(len(committed), WINDOW_QUERY_COUNT * 2)
            finally:
                harness.close()
            # Reopening the same store root must not lose or duplicate sources.
            reopened = _Harness(root)
            try:
                again = reopened.runner.composition.response_store.poll(
                    consumer_id="probe2", limit=WINDOW_QUERY_COUNT * 3
                )
                self.assertEqual(len(again), WINDOW_QUERY_COUNT * 2)
                self.assertEqual(again[0].source_sequence, 0)
                self.assertEqual(
                    again[-1].source_sequence, WINDOW_QUERY_COUNT * 2 - 1
                )
            finally:
                reopened.close()

    def test_shadow_failure_does_not_remove_committed_membership(self) -> None:
        class _FailingCapture:
            def capture(self, sources, *, trace_sequence_index: int):
                raise RuntimeError("injected shadow failure")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT)
                harness.runner.composition.shadow_worker._executor = _FailingCapture()
                with self.assertRaises(V2ShadowWorkerError) as raised:
                    harness.runner.process_ready_windows()
                self.assertEqual(raised.exception.code, "SHADOW_CAPTURE_EXCEPTION")
                committed = harness.runner.composition.response_store.poll(
                    consumer_id="probe", limit=WINDOW_QUERY_COUNT * 2
                )
                self.assertEqual(len(committed), WINDOW_QUERY_COUNT)
                detector_event_count = harness.runner.composition.detector_store._db.execute(
                    "SELECT COUNT(*) FROM detector_events"
                ).fetchone()[0]
                self.assertEqual(detector_event_count, 0)
            finally:
                harness.close()

    def test_prepared_only_restart_reconciles_without_shadow_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT)
                with patch.object(
                    harness.runner.composition.detector_store,
                    "process_window",
                    side_effect=RuntimeError("crash-before-detector"),
                ), self.assertRaises(RuntimeError):
                    harness.runner.process_ready_windows()
                self.assertEqual(
                    harness.runner.composition.finalization_store.pending().phase,
                    WindowFinalizationPhase.PREPARED,
                )
                self.assertEqual(harness.shadow.calls, 4)
            finally:
                harness.close()
            reopened = _Harness(root)
            try:
                recovered = reopened.runner.process_ready_windows()
                self.assertEqual(len(recovered), 1)
                self.assertEqual(recovered[0].status, HostWindowV2Status.REBASELINE)
                self.assertEqual(reopened.shadow.calls, 0)
                self.assertIsNone(
                    reopened.runner.composition.finalization_store.pending()
                )
            finally:
                reopened.close()

    def test_bounded_restart_reconciles_pending_window_without_touching_next(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                with patch.object(
                    harness.runner.composition.detector_store,
                    "process_window",
                    side_effect=RuntimeError("crash-before-detector"),
                ), self.assertRaises(RuntimeError):
                    harness.runner.process_ready_windows(
                        execution_bound=GateCWindowExecutionBound(0, 1)
                    )
                self.assertEqual(harness.shadow.calls, 4)
            finally:
                harness.close()
            reopened = _Harness(root)
            try:
                recovered = reopened.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                self.assertEqual(tuple(item.window_sequence for item in recovered), (0,))
                self.assertEqual(reopened.shadow.calls, 0)
                self.assertEqual(
                    reopened.runner.composition.shadow_attempt_store.records_for_window(1), ()
                )
            finally:
                reopened.close()

    def test_completed_shadows_before_prepare_reconstruct_without_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT)
                with patch.object(
                    harness.runner.composition.finalization_store,
                    "prepare",
                    side_effect=RuntimeError("crash-before-prepared"),
                ), self.assertRaises(RuntimeError):
                    harness.runner.process_ready_windows()
                self.assertEqual(harness.shadow.calls, 4)
                self.assertIsNone(
                    harness.runner.composition.finalization_store.pending()
                )
            finally:
                harness.close()
            reopened = _Harness(root)
            try:
                recovered = reopened.runner.process_ready_windows()
                self.assertEqual(len(recovered), 1)
                self.assertEqual(recovered[0].status, HostWindowV2Status.REBASELINE)
                self.assertEqual(reopened.shadow.calls, 0)
            finally:
                reopened.close()

    def test_all_cross_store_crash_points_reconcile_exactly_once(self) -> None:
        cases = (
            ("after_detector_commit", "finalization", "record_detector"),
            ("before_attestation", "attestation", "append"),
            ("after_attestation", "finalization", "record_attestation"),
            ("before_ack", "source", "acknowledge"),
            ("after_ack", "finalization", "record_acknowledged"),
            ("before_finalized", "finalization", "finalize"),
        )
        for label, owner, method in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                harness = _Harness(root)
                try:
                    harness.serve_many(WINDOW_QUERY_COUNT)
                    harness.runner.process_ready_windows()
                    harness.serve_many(WINDOW_QUERY_COUNT)
                    target = {
                        "finalization": harness.runner.composition.finalization_store,
                        "attestation": harness.runner.composition.attestation_store,
                        "source": harness.runner.composition.shadow_source,
                    }[owner]
                    with patch.object(
                        target, method, side_effect=RuntimeError(label)
                    ), self.assertRaises(RuntimeError):
                        harness.runner.process_ready_windows()
                    self.assertEqual(harness.shadow.calls, 8)
                finally:
                    harness.close()

                reopened = _Harness(root)
                try:
                    recovered = reopened.runner.process_ready_windows()
                    self.assertEqual(len(recovered), 1)
                    self.assertEqual(
                        recovered[0].status, HostWindowV2Status.EVALUATED
                    )
                    self.assertEqual(reopened.shadow.calls, 0)
                    detector_count = reopened.runner.composition.detector_store._db.execute(
                        "SELECT COUNT(*) FROM detector_events"
                    ).fetchone()[0]
                    attestation_count = reopened.runner.composition.attestation_store._connection.execute(
                        "SELECT COUNT(*) FROM attestation_records"
                    ).fetchone()[0]
                    acknowledgement = reopened.runner.composition.response_store.consumer_acknowledgement_state(
                        consumer_id="v2-shadow"
                    )
                    self.assertEqual(detector_count, 2)
                    self.assertEqual(attestation_count, 1)
                    self.assertEqual(len(acknowledgement.event_ids), 400)
                    self.assertIsNone(
                        reopened.runner.composition.finalization_store.pending()
                    )
                finally:
                    reopened.close()

    def test_contradictory_coordinator_and_detector_artifacts_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT)
                with patch.object(
                    harness.runner.composition.finalization_store,
                    "record_detector",
                    side_effect=RuntimeError("after-detector-before-coordinator"),
                ), self.assertRaises(RuntimeError):
                    harness.runner.process_ready_windows()
                harness.runner.composition.finalization_store.record_detector(
                    detector_event_sha256="f" * 64,
                    detector_head_sha256=None,
                    detector_status=HostWindowV2Status.REBASELINE,
                    recorded_at_utc="2026-08-12T00:10:00Z",
                )
            finally:
                harness.close()
            reopened = _Harness(root)
            try:
                with self.assertRaises(Exp010LiveRunnerError) as raised:
                    reopened.runner.process_ready_windows()
                self.assertEqual(
                    raised.exception.code, "WINDOW_DETECTOR_ARTIFACT_MISMATCH"
                )
                self.assertEqual(reopened.shadow.calls, 0)
            finally:
                reopened.close()

    def test_correct_detector_event_with_wrong_journal_head_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT)
                harness.runner.process_ready_windows()
                harness.serve_many(WINDOW_QUERY_COUNT)
                with patch.object(
                    harness.runner.composition.finalization_store,
                    "record_detector",
                    side_effect=RuntimeError("after-detector-before-journal"),
                ), self.assertRaises(RuntimeError):
                    harness.runner.process_ready_windows()
                persisted = harness.runner.composition.detector_store.load_persisted_window(1)
                self.assertIsNotNone(persisted)
                self.assertIsNotNone(persisted.result.detector_head)
                wrong_head = "f" * 64
                self.assertNotEqual(
                    wrong_head,
                    persisted.result.detector_head.detector_head_sha256,
                )
                harness.runner.composition.finalization_store.record_detector(
                    detector_event_sha256=persisted.event_sha256,
                    detector_head_sha256=wrong_head,
                    detector_status=HostWindowV2Status.EVALUATED,
                    recorded_at_utc="2026-08-12T00:10:00Z",
                )
            finally:
                harness.close()

            reopened = _Harness(root)
            try:
                with self.assertRaises(Exp010LiveRunnerError) as raised:
                    reopened.runner.process_ready_windows()
                self.assertEqual(
                    raised.exception.code, "WINDOW_DETECTOR_ARTIFACT_MISMATCH"
                )
                self.assertEqual(reopened.shadow.calls, 0)
                self.assertEqual(
                    reopened.runner.composition.finalization_store.pending().phase,
                    WindowFinalizationPhase.DETECTOR_COMMITTED,
                )
                attestation_count = reopened.runner.composition.attestation_store._connection.execute(
                    "SELECT COUNT(*) FROM attestation_records"
                ).fetchone()[0]
                acknowledgement = reopened.runner.composition.response_store.consumer_acknowledgement_state(
                    consumer_id="v2-shadow"
                )
                self.assertEqual(attestation_count, 0)
                self.assertEqual(len(acknowledgement.event_ids), WINDOW_QUERY_COUNT)
            finally:
                reopened.close()

    def test_detector_reason_codes_must_equal_prepared_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT)
                harness.runner.process_ready_windows()
                harness.serve_many(WINDOW_QUERY_COUNT)
                sources = harness.runner.composition.response_store.load_window(1)
                self.assertIsNotNone(sources)
                harness.runner._prepare_window(tuple(sources))
                state = harness.runner.composition.finalization_store.pending()
                self.assertEqual(state.phase, WindowFinalizationPhase.PREPARED)
                restored = restore_prepared_evaluation(state.prepared)
                self.assertIsNotNone(restored)
                prepared_decision, _pending = restored
                conflicting_decision = replace(
                    prepared_decision,
                    reason_codes=(
                        *prepared_decision.reason_codes,
                        "INJECTED_REASON_MISMATCH",
                    ),
                )
                bundle = harness.runner._load_bound_bundle(state.prepared)
                harness.runner.composition.detector_store.process_window(
                    window=bundle.shadow_window,
                    evaluator=lambda _reference, _current: conflicting_decision,
                    persisted_at_utc="2026-08-12T00:10:00Z",
                )
                persisted = harness.runner.composition.detector_store.load_persisted_window(1)
                self.assertIsNotNone(persisted)
                self.assertEqual(
                    persisted.result.reason_codes,
                    conflicting_decision.reason_codes,
                )
                harness.runner.composition.finalization_store.record_detector(
                    detector_event_sha256=persisted.event_sha256,
                    detector_head_sha256=(
                        persisted.result.detector_head.detector_head_sha256
                    ),
                    detector_status=HostWindowV2Status.EVALUATED,
                    recorded_at_utc="2026-08-12T00:10:01Z",
                )
            finally:
                harness.close()

            reopened = _Harness(root)
            try:
                with self.assertRaises(Exp010LiveRunnerError) as raised:
                    reopened.runner.process_ready_windows()
                self.assertEqual(
                    raised.exception.code, "WINDOW_DETECTOR_ARTIFACT_MISMATCH"
                )
                self.assertEqual(reopened.shadow.calls, 0)
                self.assertEqual(
                    reopened.runner.composition.finalization_store.pending().phase,
                    WindowFinalizationPhase.DETECTOR_COMMITTED,
                )
                attestation_count = reopened.runner.composition.attestation_store._connection.execute(
                    "SELECT COUNT(*) FROM attestation_records"
                ).fetchone()[0]
                acknowledgement = reopened.runner.composition.response_store.consumer_acknowledgement_state(
                    consumer_id="v2-shadow"
                )
                self.assertEqual(attestation_count, 0)
                self.assertEqual(len(acknowledgement.event_ids), WINDOW_QUERY_COUNT)
            finally:
                reopened.close()

    def test_configuration_rejects_forbidden_operands(self) -> None:
        signature = Exp010OperatorConfiguration.__dataclass_fields__
        for forbidden in (
            "data_identity", "detector_contract_identity",
            "real_detector_attestation", "drift", "is_drift",
        ):
            self.assertNotIn(forbidden, signature)

    def test_environment_helper_is_deterministic_and_complete(self) -> None:
        observed = {
            "milvus_uri": "http://milvus.invalid:19530",
            "deployment_identity": "env-001",
            "flat_collection_name": "exp001_l2_flat",
            "hnsw_collection_name": "exp001_l2_hnsw",
            "metric": Metric.L2,
            "threshold_stratum": "target-075",
            "dimensions": 128,
            "flat_index_identity": "flat-index-v1",
            "hnsw_index_identity": "hnsw-index-v1",
            "data_identity": DATA_IDENTITY,
            "source_revision": _REVISION,
            "served_ef": 400,
            "observed_at_utc": "2026-08-12T00:00:00Z",
        }
        first = build_environment_manifest_sha256(observed)
        self.assertEqual(first, build_environment_manifest_sha256(dict(observed)))
        # A stale observation time yields a different identity, so a historical
        # digest can never masquerade as a fresh Gate-A capture.
        stale = dict(observed, observed_at_utc="2026-01-01T00:00:00Z")
        self.assertNotEqual(first, build_environment_manifest_sha256(stale))
        with self.assertRaises(Exp010LiveRunnerError) as raised:
            build_environment_manifest_sha256({"milvus_uri": "x"})
        self.assertEqual(raised.exception.code, "ENVIRONMENT_METADATA_INCOMPLETE")


class Exp010LiveRunnerGuardTests(unittest.TestCase):
    def test_runner_has_no_authority_or_pymilvus_dependency(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"), filename=str(MODULE_PATH))
        imported = {
            node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        } | {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        forbidden = {
            "policy", "actuation", "canary_admission", "canary_approval",
            "canary_activation", "canary_route_authority", "canary_routing",
            "canary_live_runner", "canary_grant_store", "pymilvus",
            "exp012_scale_contract", "exp012_scale_campaign",
            "gate_c_bounded_execution", "gate_c_checkpoint_store",
        }
        offending = {
            item for item in imported
            if any(item == name or item.endswith(f".{name}") for name in forbidden)
        }
        self.assertFalse(offending, offending)
        source = MODULE_PATH.read_text(encoding="utf-8")
        self.assertNotIn("START_CANARY", source)
        self.assertNotIn("MilvusClient", source)

    def test_runner_never_touches_adr_007_offer(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"), filename=str(MODULE_PATH))
        called = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertNotIn("offer", called)
        imported_names = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        self.assertNotIn("HostObservationRecorder", imported_names)

    def test_starting_the_runner_cannot_reach_gate_e(self) -> None:
        """Gate E must be a separate method, never called by the loop."""

        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"), filename=str(MODULE_PATH))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in {
                "process_ready_windows", "_process_window", "serve", "trigger_state"
            }:
                calls = {
                    inner.func.attr
                    for inner in ast.walk(node)
                    if isinstance(inner, ast.Call)
                    and isinstance(inner.func, ast.Attribute)
                }
                self.assertNotIn("capture_exp010_population", calls)
                self.assertNotIn("capture_real_v2_post_trigger_population", calls)


if __name__ == "__main__":
    unittest.main()
