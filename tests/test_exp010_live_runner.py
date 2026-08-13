"""ADR-014 coverage for the EXP-010 operator launcher and windowing loop.

Fully offline: serving and shadow capture are injected fakes, construction
contacts nothing, and no gate beyond in-memory composition is executed. The
runner never manufactures a query -- every source in these tests originates
from an explicit `serve(...)` call, which is the genuine-workload boundary.
"""

from __future__ import annotations

import ast
from pathlib import Path
import tempfile
import unittest

import numpy as np

from vdbench.config import Metric
from vdbench.drift import DetectorState
from vdbench.exp010_live_runner import (
    Exp010LiveRunner,
    Exp010LiveRunnerError,
    Exp010OperatorConfiguration,
    build_environment_manifest_sha256,
)
from vdbench.host_observation import RangeQueryRequest, ServedQueryOutcome
from vdbench.host_window_detector_v2 import HostWindowV2Status
from vdbench.shadow_window import TRACE_QUERY_COUNT, WINDOW_QUERY_COUNT

from vdbench.config import IndexTrack
from vdbench.milvus_actuation import ShadowAuditTrace
from tests.test_shadow_extraction import _identity, _query


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
                with self.assertRaises(RuntimeError):
                    harness.runner.process_ready_windows()
                committed = harness.runner.composition.response_store.poll(
                    consumer_id="probe", limit=WINDOW_QUERY_COUNT * 2
                )
                self.assertEqual(len(committed), WINDOW_QUERY_COUNT)
            finally:
                harness.close()

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
