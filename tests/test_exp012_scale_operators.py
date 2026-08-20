from __future__ import annotations

import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from vdbench.config import Metric
from vdbench.exp010_gate_b_operator import Exp010GateBOperands
from vdbench.exp010_gate_c_operator import Exp010GateCOperands
from vdbench.exp010_serving_configuration import (
    Exp010ServingConfiguration,
    derive_serving_configuration_identity,
)
from vdbench.exp012_scale_contract import Exp012ScaleProfile, build_exp012_scale_contract
from vdbench.exp012_scale_gate_b_operator import (
    EXP012_GATE_B_PLAN_SCHEMA_VERSION,
    _FIELDS as GATE_B_FIELDS,
    Exp012ScaleGateBOperands,
    build_gate_b_plan,
    run_gate_b,
)
from vdbench.exp012_scale_gate_c_operator import (
    EXP012_GATE_C_PLAN_SCHEMA_VERSION,
    _FIELDS as GATE_C_FIELDS,
    Exp012ScaleGateCOperands,
    build_gate_c_plan,
    run_gate_c,
)


def _base_b(profile: Exp012ScaleProfile) -> Exp012ScaleGateBOperands:
    contract = build_exp012_scale_contract(profile)
    authority = {
        "metric": "L2", "threshold_stratum": "target-075",
        "threshold_radius": 1.0, "range_filter": 0.0, "limit": 100,
        "served_ef": 400, "dimensions": 128, "consistency_level": "Strong",
        "stream_id": "stream", "configuration_identity": "cfg",
        "flat_binding_id": "flat", "hnsw_binding_id": "hnsw",
        "environment_manifest_sha256": "a" * 64, "source_revision": "revision",
        "milvus_uri": "http://127.0.0.1:19530",
        "flat_collection_name": "flat", "hnsw_collection_name": "hnsw",
        "dataset001_dir": "/dataset",
    }
    return Exp012ScaleGateBOperands(
        contract=contract,
        base=Exp010GateBOperands(
            campaign_root=Path("/campaign"), detector_seed=20260813,
            host_address="127.0.0.1", host_port=59051,
            target_source_records=contract.target_source_records,
            etcd_container="etcd", minio_container="minio", authority=authority,
            deployment_identity="deployment", data_identity="data",
            gate_a_evidence_sha256="b" * 64,
        ),
        gate_a_campaign_root=Path("/gate-a-authority"),
    )


def _base_b_plan(operands: Exp012ScaleGateBOperands):
    contract = operands.contract
    return {
        "canonical_ingress": "vdbench.exp010_ingress.Exp010RequestIngress.admit",
        "canonical_boundary": "vdbench.exp010_live_runner.Exp010LiveRunner.serve",
        "campaign": {"campaign_root": "/campaign"},
        "gate_a": {"evidence_sha256": "b" * 64},
        "stream": {"stream_id": "stream"},
        "serving": {"served_ef": 400},
        "detector_seed": 20260813,
        "endpoint": {"address": "127.0.0.1", "loopback_only": True},
        "source_target": {
            "target_source_records": contract.target_source_records,
            "expected_windows": contract.expected_windows,
            "durable_source_records": 0,
        },
        "restart": {"state": "FRESH", "stores_present": False},
        "gate_c": {"next_window_sequence": 0},
        "source_revision": "revision",
    }


def _base_c(profile: Exp012ScaleProfile) -> Exp012ScaleGateCOperands:
    serving = Exp010ServingConfiguration(
        metric=Metric.L2, threshold_stratum="target-075", threshold_radius=1.0,
        range_filter=0.0, limit=100, served_ef=400, dimensions=128,
        consistency_level="Strong",
    )
    base = Exp010GateCOperands(
        stream_id="stream", metric=Metric.L2, threshold_stratum="target-075",
        threshold_radius=1.0, range_filter=0.0, limit=100, served_ef=400,
        dimensions=128, consistency_level="Strong",
        configuration_identity=derive_serving_configuration_identity(serving),
        flat_binding_id="flat", hnsw_binding_id="hnsw", source_revision="revision",
        environment_manifest_sha256="a" * 64, detector_seed=20260813,
        milvus_uri="http://127.0.0.1:19530", flat_collection_name="flat",
        hnsw_collection_name="hnsw", store_root=Path("/campaign/stores"),
        dataset001_dir=Path("/dataset"), exp010_output_dir=Path("/output"),
        etcd_container="etcd", minio_container="minio",
    )
    return Exp012ScaleGateCOperands(
        contract=build_exp012_scale_contract(profile),
        base=base,
        gate_a_campaign_root=Path("/gate-a-authority"),
    )


def _base_c_plan(operands: Exp012ScaleGateCOperands, *, finalized=0):
    contract = operands.contract
    return {
        "canonical_entrypoint": "vdbench.exp010_live_runner.Exp010LiveRunner.process_ready_windows",
        "canonical_composition": "vdbench.exp010_v2_host.Exp010V2HostComposition",
        "canonical_capture_executor": "vdbench.v2_milvus_shadow_capture.V2MilvusShadowCaptureExecutor",
        "stream": {"stream_id": "stream"}, "source_revision": "revision",
        "environment_manifest_sha256": "a" * 64,
        "gate_a_authority": {"evidence_sha256": "b" * 64},
        "detector_seed": 20260813, "serving": {"served_ef": 400},
        "milvus": {"uri": "http://127.0.0.1:19530"},
        "stores": {"root": "/campaign/stores"}, "dataset001_dir": "/dataset",
        "observed": {
            "shadow_acknowledged_count": finalized * 200,
            "complete_source_windows": contract.expected_windows,
            "next_window_sequence": finalized,
            "windows_pending": contract.expected_windows - finalized,
        },
        "projected_physical_work": {
            "flat_searches": (contract.expected_windows - finalized) * 200,
            "hnsw_sentinel_searches": (contract.expected_windows - finalized) * 200,
        },
    }


class Exp012ScaleOperatorTests(unittest.TestCase):
    def test_scale_operands_separate_campaign_from_gate_a_authority(self) -> None:
        self.assertIn("gate_a_campaign_root", GATE_B_FIELDS)
        self.assertIn("gate_a_campaign_root", GATE_C_FIELDS)
        self.assertIn("scale_output_dir", GATE_C_FIELDS)
        self.assertNotIn("exp010_output_dir", GATE_C_FIELDS)

    def test_gate_b_plans_exact_2400_and_10000_without_exp010_schema(self) -> None:
        for profile in Exp012ScaleProfile:
            operands = _base_b(profile)
            with mock.patch(
                "vdbench.exp012_scale_gate_b_operator.build_exp010_gate_b_plan",
                return_value=_base_b_plan(operands),
            ), mock.patch(
                "vdbench.exp012_scale_gate_b_operator._verify_external_gate_a_authority"
            ):
                plan = build_gate_b_plan(operands)
            self.assertEqual(plan["schema_version"], EXP012_GATE_B_PLAN_SCHEMA_VERSION)
            self.assertEqual(
                plan["source_target"]["expected_windows"], operands.contract.expected_windows
            )
            self.assertNotEqual(plan["schema_version"], "exp010-gate-b-plan-v1")

    def test_gate_b_requires_exact_completion(self) -> None:
        for profile in Exp012ScaleProfile:
            operands = _base_b(profile)
            contract = operands.contract
            with mock.patch(
                "vdbench.exp012_scale_gate_b_operator.build_exp010_gate_b_plan",
                return_value=_base_b_plan(operands),
            ), mock.patch(
                "vdbench.exp012_scale_gate_b_operator._verify_external_gate_a_authority"
            ), mock.patch(
                "vdbench.exp012_scale_gate_b_operator.run_gate_b_host_from_cli",
                return_value={
                    "durable_source_records": contract.target_source_records,
                    "complete_windows": contract.expected_windows,
                    "gate_c": {"next_window_sequence": 0},
                },
            ), mock.patch(
                "vdbench.exp012_scale_gate_b_operator.write_scale_campaign_marker"
            ):
                result = run_gate_b(operands)
            self.assertEqual(
                result["projected_physical_searches"],
                contract.expected_physical_searches,
            )

    def test_gate_c_plans_12_and_50_windows_and_exact_searches(self) -> None:
        for profile in Exp012ScaleProfile:
            operands = _base_c(profile)
            with mock.patch(
                "vdbench.exp012_scale_gate_c_operator.build_exp010_gate_c_plan",
                return_value=_base_c_plan(operands),
            ), mock.patch(
                "vdbench.exp012_scale_gate_c_operator._telemetry_binding",
                return_value=object(),
            ), mock.patch(
                "vdbench.exp012_scale_gate_c_operator._verified_source_count",
                return_value=operands.contract.target_source_records,
            ), mock.patch(
                "vdbench.exp012_scale_campaign.load_scale_campaign_marker"
            ):
                plan = build_gate_c_plan(operands)
            self.assertEqual(plan["schema_version"], EXP012_GATE_C_PLAN_SCHEMA_VERSION)
            projected = plan["projected_physical_work"]
            self.assertEqual(
                projected["flat_searches"] + projected["hnsw_sentinel_searches"],
                operands.contract.expected_physical_searches,
            )

    def test_gate_c_rejects_short_or_overadvanced_progression(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_2400)
        short = _base_c_plan(operands)
        short["observed"]["complete_source_windows"] = 11
        advanced = _base_c_plan(operands)
        advanced["observed"]["next_window_sequence"] = 13
        for base in (short, advanced):
            with mock.patch(
                "vdbench.exp012_scale_gate_c_operator.build_exp010_gate_c_plan",
                return_value=base,
            ), mock.patch(
                "vdbench.exp012_scale_gate_c_operator._telemetry_binding",
                return_value=object(),
            ), mock.patch(
                "vdbench.exp012_scale_gate_c_operator._verified_source_count",
                return_value=operands.contract.target_source_records,
            ), mock.patch(
                "vdbench.exp012_scale_campaign.load_scale_campaign_marker"
            ), self.assertRaises(Exception):
                build_gate_c_plan(operands)

    def test_gate_c_rejects_extra_source_even_when_window_count_matches(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_2400)
        with mock.patch(
            "vdbench.exp012_scale_gate_c_operator.build_exp010_gate_c_plan",
            return_value=_base_c_plan(operands),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator._telemetry_binding",
            return_value=object(),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verified_source_count",
            return_value=2401,
        ), mock.patch(
            "vdbench.exp012_scale_campaign.load_scale_campaign_marker"
        ), self.assertRaises(Exception):
            build_gate_c_plan(operands)

    def test_gate_c_progresses_beyond_three_windows_to_exact_finalization(self) -> None:
        for profile in Exp012ScaleProfile:
            operands = _base_c(profile)
            contract = operands.contract
            for finalized in (4, contract.expected_windows - 1, contract.expected_windows):
                with mock.patch(
                    "vdbench.exp012_scale_gate_c_operator.build_exp010_gate_c_plan",
                    return_value=_base_c_plan(operands, finalized=finalized),
                ), mock.patch(
                    "vdbench.exp012_scale_gate_c_operator._telemetry_binding",
                    return_value=object(),
                ), mock.patch(
                    "vdbench.exp012_scale_gate_c_operator._verified_source_count",
                    return_value=contract.target_source_records,
                ), mock.patch(
                    "vdbench.exp012_scale_campaign.load_scale_campaign_marker"
                ):
                    plan = build_gate_c_plan(operands)
                self.assertEqual(
                    plan["observed"]["next_window_sequence"], finalized
                )
                self.assertEqual(
                    plan["observed"]["windows_pending"],
                    contract.expected_windows - finalized,
                )

    def test_fake_scale_planning_benchmarks_are_history_independent(self) -> None:
        durations = {}
        for profile in Exp012ScaleProfile:
            operands = _base_b(profile)
            with mock.patch(
                "vdbench.exp012_scale_gate_b_operator.build_exp010_gate_b_plan",
                return_value=_base_b_plan(operands),
            ), mock.patch(
                "vdbench.exp012_scale_gate_b_operator._verify_external_gate_a_authority"
            ):
                started = time.perf_counter()
                for _ in range(200):
                    build_gate_b_plan(operands)
                durations[profile] = time.perf_counter() - started
        self.assertLess(durations[Exp012ScaleProfile.SCALE_2400], 1.0)
        self.assertLess(durations[Exp012ScaleProfile.SCALE_10000], 1.0)

    def test_gate_b_rejects_gate_a_authority_substitution(self) -> None:
        operands = _base_b(Exp012ScaleProfile.SCALE_2400)
        with mock.patch(
            "vdbench.exp010_gate_b_operator._inherit_gate_a_authority",
            return_value=(
                {**dict(operands.base.authority), "source_revision": "f" * 40},
                operands.base.deployment_identity,
                operands.base.data_identity,
                operands.base.gate_a_evidence_sha256,
            ),
        ), self.assertRaisesRegex(
            Exception, "EXP012_GATE_B_GATE_A_AUTHORITY_MISMATCH"
        ):
            build_gate_b_plan(operands)

    def test_gate_c_result_requires_finalization_and_complete_telemetry(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        initial = _base_c_plan(operands)
        final = _base_c_plan(operands, finalized=50)

        class _Telemetry:
            def __init__(self, *_args, **_kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def verify_completion(self, _sources):
                return SimpleNamespace(complete=True, record_count=20000, head_sha256="c" * 64)

        class _SourceStore:
            def __init__(self, *_args, **_kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def poll(self, **_kwargs):
                return tuple(range(10000))

        with mock.patch(
            "vdbench.exp012_scale_gate_c_operator.build_exp010_gate_c_plan",
            side_effect=(initial, final),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verified_source_count",
            return_value=operands.contract.target_source_records,
        ), mock.patch(
            "vdbench.exp012_scale_campaign.load_scale_campaign_marker"
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.SQLiteShadowSearchTelemetryStore",
            _Telemetry,
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator._telemetry_binding",
            return_value=SimpleNamespace(stream_key=object()),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.SQLiteHostResponseCommitStore",
            _SourceStore,
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.run_gate_c_execute_from_cli",
            return_value=tuple(SimpleNamespace() for _ in range(50)),
        ):
            result = run_gate_c(operands)
        self.assertEqual(result["finalized_windows"], 50)
        self.assertEqual(result["physical_searches"], 20000)


if __name__ == "__main__":
    unittest.main()
