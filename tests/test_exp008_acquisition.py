"""Focused composition tests for the preflight-gated EXP-008 runner."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from vdbench.actuation import ShadowResult
from vdbench.config import IndexTrack, Metric
from vdbench.host_observation import RangeQueryRequest, ServedQueryOutcome
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from vdbench.oracle import OracleHit, OracleResult
from vdbench.shadow_event_types import MonitorStreamKey


@dataclass
class _FakeServing:
    complete: bool = True

    def __post_init__(self) -> None:
        self.calls: list[RangeQueryRequest] = []

    def preflight(self) -> dict[MonitorStreamKey, object]:
        from vdbench.exp008_acquisition import prepare_exp008_configuration
        from vdbench.milvus_serving import ServingPreflightResult

        configuration = prepare_exp008_configuration(
            dataset_dir="artifacts/exp-001/dataset",
            l2_baseline_path="artifacts/exp-005/baselines/l2-target-075-ef800-lkg400.json",
            cosine_baseline_path="artifacts/exp-005/baselines/cosine-target-025-ef400-lkg200.json",
        )
        return {
            stream.stream_key: ServingPreflightResult(
                self.complete, 1 if self.complete else 0,
                () if self.complete else ("SYNTHETIC_PREFLIGHT_FAILURE",),
            )
            for stream in configuration.streams
        }

    def execute(self, request: RangeQueryRequest) -> ServedQueryOutcome:
        self.calls.append(request)
        return ServedQueryOutcome(True, False, 1, 0.01)


class _FakeShadow:
    def __init__(self) -> None:
        self.calls = 0

    def capture(self, observations: tuple[object, ...]) -> ShadowAuditTrace:
        self.calls += 1
        first = observations[0]
        stream = first.stream_key
        candidate, lkg = (
            (800, 400) if stream.metric is Metric.L2 else (400, 200)
        )

        score = 1.0 if stream.metric is Metric.L2 else 0.75
        stage = ShadowAuditStageEvidence("IDENTITY", success=True)

        def identity(track: IndexTrack, binding_id: str) -> ShadowIdentityEvidence:
            snapshot = CollectionIdentity(
                "flat" if track is IndexTrack.FLAT else "hnsw",
                stream.metric.value,
                track.value,
                {"index_type": track.value, "metric_type": stream.metric.value},
            )
            return ShadowIdentityEvidence(
                track=track,
                expected_binding_id=binding_id,
                pre_snapshot=snapshot,
                post_snapshot=snapshot,
                pre_binding_match=True,
                post_binding_match=True,
                pre_capture=stage,
                post_capture=stage,
            )

        queries = tuple(
            ShadowQueryAuditTrace(
                query_id=observation.request_id,
                query_vector=observation.query_vector,
                threshold_radius=observation.threshold_radius,
                range_filter=observation.range_filter,
                limit=observation.limit,
                oracle_result=OracleResult(
                    hits=(OracleHit(0, score),), full_count=1, capped=False
                ),
                exact_cardinality=1,
                flat_hits=(SearchHit(0, score),),
                sentinel_hits=(SearchHit(0, score),),
                sentinel_recall=1.0,
                stages=(
                    ShadowAuditStageEvidence("ORACLE", success=True),
                    ShadowAuditStageEvidence("FLAT", success=True, oracle_agreement=True),
                    ShadowAuditStageEvidence("CANDIDATE_HNSW", success=True),
                    ShadowAuditStageEvidence("LAST_KNOWN_GOOD_HNSW", success=True),
                    ShadowAuditStageEvidence("SENTINEL_HNSW", success=True),
                ),
            )
            for observation in observations
        )
        return ShadowAuditTrace(
            metric=stream.metric,
            threshold_stratum=stream.threshold_stratum,
            candidate_ef=candidate,
            last_known_good_ef=lkg,
            sentinel_ef=100,
            configuration_identity=stream.configuration_identity,
            data_identity=stream.data_identity,
            flat_identity=identity(IndexTrack.FLAT, stream.flat_binding_id),
            hnsw_identity=identity(IndexTrack.HNSW, stream.hnsw_binding_id),
            queries=queries,
            complete=True,
        )


class _FactoryAdapter:
    def __init__(self, *, workload: object, **_: object) -> None:
        self.workload = workload
        self.client = object()
        self.harness = object()
        self.stack_health_probe = object()
        self.shadow_trace_sink = None

    def shadow_candidate(self, **_: object) -> ShadowResult:
        raise AssertionError("runtime-construction test must not query")


def _configuration():
    from vdbench.exp008_acquisition import prepare_exp008_configuration

    return prepare_exp008_configuration(
        dataset_dir="artifacts/exp-001/dataset",
        l2_baseline_path="artifacts/exp-005/baselines/l2-target-075-ef800-lkg400.json",
        cosine_baseline_path="artifacts/exp-005/baselines/cosine-target-025-ef400-lkg200.json",
    )


class Exp008AcquisitionContractTests(unittest.TestCase):
    def test_prepared_configuration_is_pinned_to_the_two_registered_streams(self) -> None:
        from vdbench.exp008_acquisition import (
            EXP008_DETECTOR_SEED,
            prepare_exp008_configuration,
        )

        configuration = _configuration()

        self.assertEqual(EXP008_DETECTOR_SEED, 20260805)
        self.assertEqual(
            [
                (stream.metric.value, stream.threshold_stratum, stream.candidate_ef, stream.last_known_good_ef, stream.served_ef)
                for stream in configuration.streams
            ],
            [
                ("L2", "target-075", 800, 400, 400),
                ("COSINE", "target-025", 400, 200, 200),
            ],
        )
        self.assertEqual(configuration.measured_query_count, 200)

    def test_full_fake_run_writes_complete_1200_request_dry_run_evidence(self) -> None:
        from vdbench.exp008_acquisition import Exp008Runtime, run_exp008

        configuration = _configuration()
        serving = _FakeServing()
        shadow = _FakeShadow()
        close_calls: list[str] = []
        runtime = Exp008Runtime(
            serving=serving,
            shadow=shadow,
            close=lambda: close_calls.append("closed"),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "exp008"
            result = run_exp008(
                configuration=configuration,
                runtime=runtime,
                output_dir=root,
                resource_snapshot=lambda timestamp: {
                    "timestamp_utc": timestamp,
                    "runtime_closed": bool(close_calls),
                },
            )

            self.assertEqual(len(serving.calls), 1200)
            self.assertEqual(shadow.calls, 24)
            self.assertEqual(result.trace_count, 24)
            self.assertEqual(result.evaluated_stream_count, 2)
            completion = (root / "completion.json").read_text(encoding="utf-8")
            self.assertIn('"status":"COMPLETE"', completion)
            self.assertIn('"policy_mode_dry_run":true', completion)
            self.assertTrue((root / "monitor-audit.jsonl").is_file())
            self.assertTrue((root / "run_manifest.json").is_file())
            self.assertEqual(close_calls, ["closed"])
            self.assertIn('"runtime_closed":true', (root / "post_run_resources.json").read_text(encoding="utf-8"))

    def test_failed_preflight_stops_before_foreground_or_shadow_work(self) -> None:
        from vdbench.exp008_acquisition import (
            EXP008AcquisitionError,
            Exp008Runtime,
            run_exp008,
        )

        serving = _FakeServing(complete=False)
        shadow = _FakeShadow()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "exp008"
            with self.assertRaisesRegex(EXP008AcquisitionError, "SERVING_PREFLIGHT_INCOMPLETE"):
                run_exp008(
                    configuration=_configuration(),
                    runtime=Exp008Runtime(serving=serving, shadow=shadow),
                    output_dir=root,
                    resource_snapshot=lambda timestamp: {"timestamp_utc": timestamp},
                )
            self.assertEqual(serving.calls, [])
            self.assertEqual(shadow.calls, 0)
            self.assertIn("SERVING_PREFLIGHT_INCOMPLETE", (root / "failure.json").read_text(encoding="utf-8"))

    def test_runner_contains_no_mutation_or_action_invocation(self) -> None:
        source_path = Path(__file__).parents[1] / "src/vdbench/exp008_acquisition.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
        invoked_attributes = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        forbidden = {
            "start_canary",
            "stop_candidate",
            "restore_last_known_good",
            "verify_restoration",
            "create_collection",
            "drop_collection",
            "create_index",
            "release_collection",
            "alter_alias",
        }
        self.assertFalse(invoked_attributes & forbidden)

    def test_live_factory_keeps_l2_and_cosine_workload_identities_isolated(self) -> None:
        from vdbench.exp008_acquisition import build_live_runtime

        created: list[_FactoryAdapter] = []

        def factory(uri: str, **kwargs: object) -> _FactoryAdapter:
            self.assertEqual(uri, "http://example.invalid:19530")
            adapter = _FactoryAdapter(**kwargs)
            created.append(adapter)
            return adapter

        with patch(
            "vdbench.exp008_acquisition.MilvusActuationClient.from_uri",
            side_effect=factory,
        ):
            build_live_runtime(
                configuration=_configuration(), uri="http://example.invalid:19530"
            )

        self.assertEqual(len(created), 2)
        self.assertEqual(
            {adapter.workload.configuration_identity for adapter in created},
            {stream.baseline.configuration_identity for stream in _configuration().streams},
        )
        self.assertEqual(
            [len(adapter.workload.identity_bindings) for adapter in created], [2, 2]
        )


if __name__ == "__main__":
    unittest.main()
