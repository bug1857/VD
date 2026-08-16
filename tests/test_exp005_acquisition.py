"""Offline tests for EXP-005's reviewed-baseline capture runner."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from vdbench.actuation import ShadowActuationContext, ShadowResult
from vdbench.config import IndexTrack, Metric
from vdbench.exp005_acquisition import (
    Exp005AcquisitionError,
    _preflight_live_adapter,
    capture_identity_baseline,
    capture_stationary_replay,
    load_identity_baseline,
    persist_identity_baseline,
)
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
    StackHealth,
)
from vdbench.oracle import OracleHit, OracleResult
from vdbench.shadow_artifacts import load_persisted_shadow_trace_envelope
from vdbench.shadow_window import assemble_shadow_window


def _identity(track: IndexTrack) -> CollectionIdentity:
    description: dict[str, object] = {
        "index_type": track.value,
        "metric_type": Metric.L2.value,
    }
    if track is IndexTrack.HNSW:
        description.update({"M": "16", "efConstruction": "200"})
    return CollectionIdentity(
        collection_name=f"exp005_l2_{track.value.lower()}",
        metric=Metric.L2.value,
        index_track=track.value,
        description=description,
    )


class _BaselineClient:
    def describe_index(self, *, collection_name: str, index_name: str):
        if collection_name.endswith("flat"):
            return _identity(IndexTrack.FLAT).description
        return _identity(IndexTrack.HNSW).description


class _CaptureAdapter:
    def __init__(self, baseline) -> None:
        self.baseline = baseline
        self.shadow_trace_sink = None
        self.calls: list[tuple[int, ...]] = []
        self.contexts: list[ShadowActuationContext] = []
        self.forbidden_calls: list[str] = []
        self.fail_on_call: int | None = None

    def shadow_candidate(self, *, context, candidate_ef: int, last_known_good_ef: int):
        self.assert_shadow_context(context)
        self.contexts.append(context)
        self.calls.append(context.audited_query_ids)
        assert self.shadow_trace_sink is not None
        flat = ShadowIdentityEvidence(
            track=IndexTrack.FLAT,
            expected_binding_id=self.baseline.flat_binding.identity_id,
            pre_snapshot=self.baseline.flat_binding.expected,
            post_snapshot=self.baseline.flat_binding.expected,
            pre_binding_match=True,
            post_binding_match=True,
            pre_capture=ShadowAuditStageEvidence("PRE_FLAT_IDENTITY", True),
            post_capture=ShadowAuditStageEvidence("POST_FLAT_IDENTITY", True),
        )
        hnsw = ShadowIdentityEvidence(
            track=IndexTrack.HNSW,
            expected_binding_id=self.baseline.hnsw_binding.identity_id,
            pre_snapshot=self.baseline.hnsw_binding.expected,
            post_snapshot=self.baseline.hnsw_binding.expected,
            pre_binding_match=True,
            post_binding_match=True,
            pre_capture=ShadowAuditStageEvidence("PRE_HNSW_IDENTITY", True),
            post_capture=ShadowAuditStageEvidence("POST_HNSW_IDENTITY", True),
        )
        records = tuple(
            ShadowQueryAuditTrace(
                query_id=query_id,
                query_vector=(float(query_id), 1.0),
                threshold_radius=2.0,
                range_filter=0.0,
                limit=100,
                oracle_result=OracleResult((OracleHit(query_id, 1.0),), 1, False),
                exact_cardinality=1,
                flat_hits=(SearchHit(query_id, 1.0),),
                sentinel_hits=(SearchHit(query_id, 1.0),),
                sentinel_recall=1.0,
                stages=(
                    ShadowAuditStageEvidence("ORACLE", True),
                    ShadowAuditStageEvidence("FLAT", True, oracle_agreement=True),
                    ShadowAuditStageEvidence("CANDIDATE_HNSW", True),
                    ShadowAuditStageEvidence("LAST_KNOWN_GOOD_HNSW", True),
                    ShadowAuditStageEvidence("SENTINEL_HNSW", True),
                ),
            )
            for query_id in context.audited_query_ids
        )
        self.shadow_trace_sink.append(
            ShadowAuditTrace(
                metric=Metric.L2,
                threshold_stratum="target-075",
                candidate_ef=candidate_ef,
                last_known_good_ef=last_known_good_ef,
                sentinel_ef=100,
                configuration_identity=self.baseline.configuration_identity,
                data_identity=self.baseline.data_identity,
                flat_identity=flat,
                hnsw_identity=hnsw,
                queries=records,
                complete=True,
            )
        )
        succeeded = self.fail_on_call != len(self.calls)
        return ShadowResult(succeeded, 50, 0 if succeeded else 1, 0, 0, succeeded, succeeded)

    @staticmethod
    def assert_shadow_context(context: object) -> None:
        if type(context) is not ShadowActuationContext:
            raise AssertionError("capture requires a ShadowActuationContext")
        if hasattr(context, "last_known_good"):
            raise AssertionError("shadow context must not carry qualification")

    def start_canary(self, *args, **kwargs):
        self.forbidden_calls.append("start_canary")
        raise AssertionError("capture must never start a canary")

    def restore_last_known_good(self, *args, **kwargs):
        self.forbidden_calls.append("restore_last_known_good")
        raise AssertionError("capture must never restore")


class _PreflightProbe:
    def __init__(self, health: StackHealth) -> None:
        self.health = health
        self.calls = 0

    def check(self) -> StackHealth:
        self.calls += 1
        return self.health


class _PreflightClient:
    def __init__(self, *, state: str = "Loaded") -> None:
        self.state = state
        self.calls: list[str] = []

    def get_load_state(self, *, collection_name: str):
        self.calls.append(collection_name)
        return {"state": self.state}


class _PreflightHarness:
    def __init__(self, baseline, *, mismatch: bool = False) -> None:
        self.baseline = baseline
        self.mismatch = mismatch
        self.calls: list[tuple[str, IndexTrack]] = []

    def index_identity(self, name: str, metric: Metric, track: IndexTrack):
        self.calls.append((name, track))
        expected = (
            self.baseline.flat_binding.expected
            if track is IndexTrack.FLAT
            else self.baseline.hnsw_binding.expected
        )
        if not self.mismatch:
            return expected
        return CollectionIdentity(
            collection_name=expected.collection_name,
            metric=expected.metric,
            index_track=expected.index_track,
            description={"identity": "mismatch"},
        )


class _PreflightAdapter:
    def __init__(self, baseline, *, health: StackHealth, state: str = "Loaded", mismatch: bool = False) -> None:
        self.stack_health_probe = _PreflightProbe(health)
        self.client = _PreflightClient(state=state)
        self.harness = _PreflightHarness(baseline, mismatch=mismatch)


class Exp005AcquisitionTests(unittest.TestCase):
    def _baseline(self):
        return capture_identity_baseline(
            client=_BaselineClient(),
            metric=Metric.L2,
            threshold_stratum="target-075",
            candidate_ef=400,
            last_known_good_ef=200,
            flat_collection_name="exp005_l2_flat",
            hnsw_collection_name="exp005_l2_hnsw",
            configuration_identity="configuration-v1",
            data_identity="dataset-v1",
        )

    def test_reviewed_identity_baseline_is_restart_round_trippable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "identity-baseline.json"
            expected = self._baseline()
            persist_identity_baseline(path, expected)
            self.assertEqual(load_identity_baseline(path), expected)

    def test_capture_persists_twelve_traces_in_three_complete_windows(self) -> None:
        baseline = self._baseline()
        adapter = _CaptureAdapter(baseline)
        with tempfile.TemporaryDirectory() as temporary:
            result = capture_stationary_replay(
                adapter=adapter,
                baseline=baseline,
                measured_query_ids=tuple(range(200)),
                output_dir=Path(temporary) / "capture",
                capture_id="capture-001",
            )
            self.assertEqual(len(adapter.calls), 12)
            self.assertEqual(len(adapter.contexts), 12)
            self.assertTrue(
                all(
                    not hasattr(context, "last_known_good")
                    for context in adapter.contexts
                )
            )
            self.assertEqual(adapter.forbidden_calls, [])
            self.assertEqual(len(result.trace_paths), 12)
            self.assertTrue(all(path.is_file() for path in result.trace_paths))
            self.assertEqual(
                tuple(query_id for group in adapter.calls[:4] for query_id in group),
                tuple(range(200)),
            )
            for role in ("reference", "current-1", "current-2"):
                envelopes = tuple(
                    load_persisted_shadow_trace_envelope(path)
                    for path in result.trace_paths_by_role[role]
                )
                window = assemble_shadow_window(window_id=f"capture-001-{role}", envelopes=envelopes)
                self.assertTrue(window.complete, window.reason_codes)

    def test_duplicate_or_non_200_query_ids_fail_before_any_shadow_call(self) -> None:
        baseline = self._baseline()
        adapter = _CaptureAdapter(baseline)
        with (
            tempfile.TemporaryDirectory() as temporary,
            self.assertRaisesRegex(Exp005AcquisitionError, "QUERY_IDS"),
        ):
            capture_stationary_replay(
                adapter=adapter,
                baseline=baseline,
                measured_query_ids=tuple(range(199)) + (198,),
                output_dir=Path(temporary) / "capture",
                capture_id="capture-001",
            )
        self.assertEqual(adapter.calls, [])

    def test_failed_shadow_stops_before_later_trace_or_actuation(self) -> None:
        baseline = self._baseline()
        adapter = _CaptureAdapter(baseline)
        adapter.fail_on_call = 1
        with (
            tempfile.TemporaryDirectory() as temporary,
            self.assertRaisesRegex(Exp005AcquisitionError, "SHADOW_RESULT_FAILED"),
        ):
            capture_stationary_replay(
                adapter=adapter,
                baseline=baseline,
                measured_query_ids=tuple(range(200)),
                output_dir=Path(temporary) / "capture",
                capture_id="capture-001",
            )
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(adapter.forbidden_calls, [])

    def test_preflight_rejects_unhealthy_etcd_before_collection_reads(self) -> None:
        baseline = self._baseline()
        adapter = _PreflightAdapter(
            baseline,
            health=StackHealth(etcd_healthy=False, minio_healthy=True, detail="etcd unhealthy"),
        )
        with self.assertRaisesRegex(Exp005AcquisitionError, "STACK_HEALTH_FAILED"):
            _preflight_live_adapter(adapter, baseline)
        self.assertEqual(adapter.client.calls, [])
        self.assertEqual(adapter.harness.calls, [])

    def test_preflight_rejects_unhealthy_minio_before_collection_reads(self) -> None:
        baseline = self._baseline()
        adapter = _PreflightAdapter(
            baseline,
            health=StackHealth(etcd_healthy=True, minio_healthy=False, detail="minio unhealthy"),
        )
        with self.assertRaisesRegex(Exp005AcquisitionError, "STACK_HEALTH_FAILED"):
            _preflight_live_adapter(adapter, baseline)
        self.assertEqual(adapter.client.calls, [])
        self.assertEqual(adapter.harness.calls, [])

    def test_preflight_rejects_unloaded_collection(self) -> None:
        baseline = self._baseline()
        adapter = _PreflightAdapter(
            baseline,
            health=StackHealth(True, True, "healthy"),
            state="NotLoaded",
        )
        with self.assertRaisesRegex(Exp005AcquisitionError, "PREFLIGHT_IDENTITY_OR_LOAD_FAILED:FLAT"):
            _preflight_live_adapter(adapter, baseline)
        self.assertEqual(adapter.client.calls, [baseline.flat_binding.expected.collection_name])

    def test_preflight_rejects_identity_mismatch(self) -> None:
        baseline = self._baseline()
        adapter = _PreflightAdapter(
            baseline,
            health=StackHealth(True, True, "healthy"),
            mismatch=True,
        )
        with self.assertRaisesRegex(Exp005AcquisitionError, "PREFLIGHT_IDENTITY_OR_LOAD_FAILED:FLAT"):
            _preflight_live_adapter(adapter, baseline)
        self.assertEqual(adapter.client.calls, [baseline.flat_binding.expected.collection_name])

    def test_preflight_returns_both_matching_loaded_tracks(self) -> None:
        baseline = self._baseline()
        adapter = _PreflightAdapter(
            baseline,
            health=StackHealth(True, True, "all healthy"),
        )
        evidence = _preflight_live_adapter(adapter, baseline)
        self.assertEqual(evidence["stack_health"], "all healthy")
        self.assertEqual(evidence["tracks"]["FLAT"]["loaded"], True)
        self.assertEqual(evidence["tracks"]["HNSW"]["binding_match"], True)
        self.assertEqual(
            adapter.client.calls,
            [
                baseline.flat_binding.expected.collection_name,
                baseline.hnsw_binding.expected.collection_name,
            ],
        )
