from __future__ import annotations

import copy
import time
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from tests.test_gate_c_bounded_execution import _fixture_v2 as _bounded_fixture
from tests.test_exp010_live_runner import (
    DATASET001,
    _ENVIRONMENT,
    _Harness,
    _REVISION,
)

from vdbench.config import Metric
from vdbench.canonical_serialization import strict_canonical_digest
from vdbench import exp012_scale_gate_c_operator as gate_c_scale_operator
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
    run_gate_c_checkpoint,
    run_gate_c,
)
from vdbench.gate_c_bounded_execution import (
    GateCBoundedExecutionError,
    GateCWindowExecutionBound,
    build_gate_c_bounded_execution_envelope_v2,
    build_gate_c_canonical_state,
    build_gate_c_window_checkpoint_effect,
    gate_c_bounded_execution_envelope_document_v2,
)
from vdbench.gate_c_execution_source import (
    GateCExecutionSourceError,
    VerifiedGateCExecutionSource,
)
from vdbench.shadow_attempt_store import build_shadow_attempt_identity
from vdbench.shadow_search_telemetry import (
    SQLiteShadowSearchTelemetryStore,
    ShadowSearchOutcome,
    ShadowSearchRole,
)
from vdbench.shadow_window import TRACE_QUERY_COUNT, WINDOW_QUERY_COUNT


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


def _operator_v2_fixture():
    plan, campaign, head, sources, _envelope = _bounded_fixture(
        target_profile=Exp012ScaleProfile.SCALE_10000
    )
    plan = copy.deepcopy(plan)
    plan["observed"].update(
        {
            "shadow_acknowledged_count": 0,
            "windows_pending": 50,
        }
    )
    plan["projected_physical_work"] = {
        "flat_searches": 10000,
        "hnsw_sentinel_searches": 10000,
    }
    plan.pop("plan_sha256")
    plan["plan_sha256"] = strict_canonical_digest(
        b"VD::EXP012_SCALE_GATE_C_PLAN::V1\x00", plan
    )
    envelope = build_gate_c_bounded_execution_envelope_v2(
        plan=plan,
        campaign_binding=campaign,
        source_head=head,
        sources=sources,
        execution_bound=GateCWindowExecutionBound(0, 1),
        execution_source_revision="b" * 40,
    )
    return plan, campaign, head, sources, envelope


def _checkpoint_transition_fixture():
    pre = build_gate_c_canonical_state(
        next_window_sequence=0, acknowledgement_count=0,
        acknowledgement_head_sha256=None, attempt_count=0,
        attempt_event_count=0, attempt_event_head_sha256=None,
        detector_event_count=0, detector_event_head_sha256=None,
        attestation_record_count=0, attestation_record_head_sha256=None,
        finalization_window_count=0, finalization_event_count=0,
        finalization_event_head_sha256=None, telemetry_record_count=0,
        telemetry_record_head_sha256=None,
    )
    effect = build_gate_c_window_checkpoint_effect(
        window_sequence=0, source_window_sha256="1" * 64,
        attempt_sha256s=tuple(f"{item:064x}" for item in range(1, 5)),
        attempt_event_head_sha256="5" * 64,
        detector_event_sha256="6" * 64, detector_status="REBASELINE",
        detector_head_sha256=None, attestation_disposition="NOT_REQUIRED",
        attestation_record_sha256=None, attestation_record_head_sha256=None,
        attestation_sha256=None,
        prepared_sha256="7" * 64, acknowledgement_head_sha256="8" * 64,
        finalization_event_head_sha256="9" * 64,
        telemetry_record_count=400, telemetry_record_head_sha256="a" * 64,
    )
    post = build_gate_c_canonical_state(
        next_window_sequence=1, acknowledgement_count=200,
        acknowledgement_head_sha256="8" * 64, attempt_count=4,
        attempt_event_count=8, attempt_event_head_sha256="5" * 64,
        detector_event_count=1, detector_event_head_sha256="6" * 64,
        attestation_record_count=0, attestation_record_head_sha256=None,
        finalization_window_count=1, finalization_event_count=5,
        finalization_event_head_sha256="9" * 64, telemetry_record_count=400,
        telemetry_record_head_sha256="a" * 64,
    )
    return pre, post, (effect,)


def _canonical_checkpoint_operands(root: Path) -> Exp012ScaleGateCOperands:
    """Point the scale wrapper at the real offline runner stores."""

    return Exp012ScaleGateCOperands(
        contract=build_exp012_scale_contract(Exp012ScaleProfile.SCALE_10000),
        base=Exp010GateCOperands(
            stream_id="v2-live",
            metric=Metric.L2,
            threshold_stratum="target-075",
            threshold_radius=2.0,
            range_filter=0.0,
            limit=100,
            served_ef=400,
            dimensions=2,
            consistency_level="Strong",
            configuration_identity="config-v1",
            flat_binding_id="flat-index-v1",
            hnsw_binding_id="hnsw-index-v1",
            source_revision=_REVISION,
            environment_manifest_sha256=_ENVIRONMENT,
            detector_seed=20260812,
            milvus_uri="http://milvus.invalid:19530",
            flat_collection_name="exp001_l2_flat",
            hnsw_collection_name="exp001_l2_hnsw",
            store_root=root / "stores",
            dataset001_dir=DATASET001,
            exp010_output_dir=root / "exp010",
            etcd_container="etcd",
            minio_container="minio",
        ),
        gate_a_campaign_root=root / "gate-a-authority",
    )


def _append_checkpoint_telemetry(
    operands: Exp012ScaleGateCOperands,
    sources: tuple[object, ...],
    *,
    start_window_sequence: int,
    window_count: int,
) -> None:
    """Append exact successful telemetry through repository production APIs."""

    first_source = start_window_sequence * WINDOW_QUERY_COUNT
    final_source = (start_window_sequence + window_count) * WINDOW_QUERY_COUNT
    with SQLiteShadowSearchTelemetryStore(
        operands.telemetry_path,
        binding=gate_c_scale_operator._telemetry_binding(operands),
    ) as telemetry:
        next_tick = len(telemetry.records()) * 2
        attempts: dict[tuple[int, int], str] = {}
        for source in sources[first_source:final_source]:
            trace_sequence_index = (
                source.within_window_index // TRACE_QUERY_COUNT
            )
            attempt_key = (source.window_sequence, trace_sequence_index)
            attempt_sha256 = attempts.get(attempt_key)
            if attempt_sha256 is None:
                trace_start = (
                    source.window_sequence * WINDOW_QUERY_COUNT
                    + trace_sequence_index * TRACE_QUERY_COUNT
                )
                attempt_sha256 = build_shadow_attempt_identity(
                    tuple(sources[trace_start : trace_start + TRACE_QUERY_COUNT]),
                    trace_sequence_index=trace_sequence_index,
                ).attempt_sha256
                attempts[attempt_key] = attempt_sha256
            for role in ShadowSearchRole:
                telemetry.append(
                    window_sequence=source.window_sequence,
                    trace_sequence_index=trace_sequence_index,
                    attempt_sha256=attempt_sha256,
                    source_sequence=source.source_sequence,
                    source_sha256=source.source_sha256,
                    query_id_sha256=source.query_id_sha256,
                    role=role,
                    started_monotonic_ns=next_tick,
                    completed_monotonic_ns=next_tick + 1,
                    outcome=ShadowSearchOutcome.SUCCEEDED,
                    error_classification=None,
                    result_count=1,
                )
                next_tick += 2


class _RecoveringCheckpointLedger:
    completed_result = None

    def __init__(self, *_args, **_kwargs):
        type(self).completed_result = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def state(self, _envelope):
        return SimpleNamespace(completed_event_sha256=None)

    def complete(self, _envelope, *, checkpoint_result, **_kwargs):
        type(self).completed_result = checkpoint_result


def _reconstruct_checkpoint(
    operands: Exp012ScaleGateCOperands,
    envelope: object,
    sources: tuple[object, ...],
    *,
    durable_next_window_sequence: int,
):
    with mock.patch(
        "vdbench.exp012_scale_gate_c_operator._verify_current_checkpoint_envelope",
        return_value=(
            envelope,
            {"observed": {"next_window_sequence": durable_next_window_sequence}},
            sources,
        ),
    ), mock.patch(
        "vdbench.exp012_scale_gate_c_operator.SQLiteGateCCheckpointLedger",
        _RecoveringCheckpointLedger,
    ), mock.patch(
        "vdbench.exp012_scale_gate_c_operator.run_gate_c_execute_from_cli",
        side_effect=AssertionError("physical-search seam reached during recovery"),
    ):
        return run_gate_c_checkpoint(operands, {"ignored": True})


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

    def test_checkpoint_executes_exact_bound_and_returns_distinct_result(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        _, _, _, _, envelope = _bounded_fixture(
            target_profile=Exp012ScaleProfile.SCALE_10000
        )
        current_plan = {"observed": {"next_window_sequence": 0}}

        class _Ledger:
            def __init__(self, *_args, **_kwargs):
                self.completed = None

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def state(self, _envelope):
                return None

            def start(self, _envelope, **_kwargs):
                return SimpleNamespace(completed_event_sha256=None)

            def complete(self, _envelope, *, checkpoint_result, **_kwargs):
                self.completed = checkpoint_result

        class _Telemetry:
            def __init__(self, *_args, **_kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        with mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verify_current_checkpoint_envelope",
            return_value=(envelope, current_plan, tuple(range(10000))),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.SQLiteGateCCheckpointLedger",
            _Ledger,
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator._require_safe_checkpoint_start"
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.SQLiteShadowSearchTelemetryStore",
            _Telemetry,
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator._telemetry_binding",
            return_value=object(),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.run_gate_c_execute_from_cli",
            return_value=(SimpleNamespace(window_sequence=0),),
        ) as execute, mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verified_checkpoint_transition",
            return_value=_checkpoint_transition_fixture(),
        ):
            result = run_gate_c_checkpoint(operands, {"ignored": True})
        self.assertEqual(
            result["checkpoint_result_payload"]["processed_window_sequences"], [0]
        )
        self.assertEqual(
            result["checkpoint_result_payload"]["schema_version"],
            "exp012-scale-gate-c-checkpoint-result-v2",
        )
        self.assertEqual(
            result["checkpoint_result_payload"]["source_revision"], "revision"
        )
        self.assertEqual(
            result["checkpoint_result_payload"]["execution_source_revision"],
            "b" * 40,
        )
        self.assertFalse(result["checkpoint_result_payload"]["full_campaign_complete"])
        self.assertEqual(
            execute.call_args.kwargs["execution_bound"],
            GateCWindowExecutionBound(0, 1),
        )

    def test_checkpoint_preflight_derives_v2_execution_revision(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        plan, campaign, head, sources, _envelope = _operator_v2_fixture()
        identity = VerifiedGateCExecutionSource(Path("/repository"), "b" * 40)
        with mock.patch.object(
            gate_c_scale_operator, "build_gate_c_plan", return_value=plan
        ), mock.patch.object(
            gate_c_scale_operator, "_telemetry_binding", return_value=object()
        ), mock.patch.object(
            gate_c_scale_operator,
            "_verified_sources_and_head",
            return_value=(sources, head),
        ), mock.patch.object(
            gate_c_scale_operator, "_campaign_binding", return_value=campaign
        ), mock.patch.object(
            gate_c_scale_operator,
            "derive_gate_c_execution_source",
            return_value=identity,
        ) as derive:
            envelope = gate_c_scale_operator.build_gate_c_checkpoint_envelope(
                operands, GateCWindowExecutionBound(0, 1)
            )
        self.assertEqual(envelope.source_revision, "revision")
        self.assertEqual(envelope.execution_source_revision, "b" * 40)
        self.assertEqual(
            envelope.schema_version,
            "exp012-scale-gate-c-bounded-execution-envelope-v2",
        )
        derive.assert_called_once_with()

    def test_upstream_drift_refuses_before_execution_revision_check(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        plan, campaign, head, sources, envelope = _operator_v2_fixture()
        document = gate_c_bounded_execution_envelope_document_v2(envelope)
        document["envelope_payload"]["source_revision"] = "changed-upstream"
        document["envelope_sha256"] = strict_canonical_digest(
            b"VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V2\x00",
            document["envelope_payload"],
        )
        with mock.patch.object(
            gate_c_scale_operator, "build_gate_c_plan", return_value=plan
        ), mock.patch.object(
            gate_c_scale_operator, "_telemetry_binding", return_value=object()
        ), mock.patch.object(
            gate_c_scale_operator,
            "_verified_sources_and_head",
            return_value=(sources, head),
        ), mock.patch.object(
            gate_c_scale_operator, "_campaign_binding", return_value=campaign
        ), mock.patch.object(
            gate_c_scale_operator, "derive_gate_c_execution_source"
        ) as derive, self.assertRaises(GateCBoundedExecutionError):
            gate_c_scale_operator._verify_current_checkpoint_envelope(
                operands, document
            )
        derive.assert_not_called()

    def test_wrong_execution_revision_refuses_before_live_or_ledger_construction(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        plan, campaign, head, sources, envelope = _operator_v2_fixture()
        document = gate_c_bounded_execution_envelope_document_v2(envelope)
        with mock.patch.object(
            gate_c_scale_operator, "build_gate_c_plan", return_value=plan
        ), mock.patch.object(
            gate_c_scale_operator, "_telemetry_binding", return_value=object()
        ), mock.patch.object(
            gate_c_scale_operator,
            "_verified_sources_and_head",
            return_value=(sources, head),
        ), mock.patch.object(
            gate_c_scale_operator, "_campaign_binding", return_value=campaign
        ), mock.patch.object(
            gate_c_scale_operator,
            "derive_gate_c_execution_source",
            side_effect=GateCExecutionSourceError(
                "GATE_C_EXECUTION_SOURCE_REVISION_MISMATCH"
            ),
        ), mock.patch.object(
            gate_c_scale_operator, "SQLiteGateCCheckpointLedger"
        ) as ledger, mock.patch.object(
            gate_c_scale_operator, "run_gate_c_execute_from_cli"
        ) as execute, self.assertRaises(GateCExecutionSourceError):
            run_gate_c_checkpoint(operands, document)
        ledger.assert_not_called()
        execute.assert_not_called()

    def test_startup_hook_drift_refuses_before_live_or_ledger_construction(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        plan, campaign, head, sources, envelope = _operator_v2_fixture()
        document = gate_c_bounded_execution_envelope_document_v2(envelope)
        with mock.patch.object(
            gate_c_scale_operator, "build_gate_c_plan", return_value=plan
        ), mock.patch.object(
            gate_c_scale_operator, "_telemetry_binding", return_value=object()
        ), mock.patch.object(
            gate_c_scale_operator,
            "_verified_sources_and_head",
            return_value=(sources, head),
        ), mock.patch.object(
            gate_c_scale_operator, "_campaign_binding", return_value=campaign
        ), mock.patch.object(
            gate_c_scale_operator,
            "derive_gate_c_execution_source",
            side_effect=GateCExecutionSourceError(
                "GATE_C_EXECUTION_SOURCE_DRIFT"
            ),
        ), mock.patch.object(
            gate_c_scale_operator, "SQLiteGateCCheckpointLedger"
        ) as ledger, mock.patch.object(
            gate_c_scale_operator, "run_gate_c_execute_from_cli"
        ) as execute, self.assertRaises(GateCExecutionSourceError):
            run_gate_c_checkpoint(operands, document)
        ledger.assert_not_called()
        execute.assert_not_called()

    def test_execution_revision_is_not_a_cli_authority_field(self) -> None:
        with self.assertRaises(SystemExit):
            gate_c_scale_operator._parser().parse_args(
                [
                    "--operands",
                    "/tmp/operands.json",
                    "--mode",
                    "checkpoint-preflight",
                    "--execution-source-revision",
                    "b" * 40,
                ]
            )

    def test_checkpoint_reconciles_completed_canonical_state_without_search(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        _, _, _, _, envelope = _bounded_fixture(
            target_profile=Exp012ScaleProfile.SCALE_10000
        )

        class _Ledger:
            def __init__(self, *_args, **_kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def state(self, _envelope):
                return SimpleNamespace(completed_event_sha256=None)

            def complete(self, *_args, **_kwargs):
                return None

        with mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verify_current_checkpoint_envelope",
            return_value=(envelope, {"observed": {"next_window_sequence": 1}}, tuple(range(10000))),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.SQLiteGateCCheckpointLedger",
            _Ledger,
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.run_gate_c_execute_from_cli"
        ) as execute, mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verified_checkpoint_transition",
            return_value=_checkpoint_transition_fixture(),
        ):
            result = run_gate_c_checkpoint(operands, {"ignored": True})
        execute.assert_not_called()
        self.assertEqual(
            result["checkpoint_result_payload"]["post_state"]["state_payload"]
            ["next_window_sequence"],
            1,
        )

    def test_real_canonical_c1_reconstruction_binds_every_store_without_search(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            operands = _canonical_checkpoint_operands(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                result = harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                self.assertEqual(tuple(item.window_sequence for item in result), (0,))
                sources = harness.runner.composition.response_store.poll(
                    consumer_id="checkpoint-test", limit=WINDOW_QUERY_COUNT * 2
                )
            finally:
                harness.close()
            _append_checkpoint_telemetry(
                operands, sources, start_window_sequence=0, window_count=1
            )
            *_unused, envelope = _bounded_fixture(
                start=0,
                count=1,
                target_profile=Exp012ScaleProfile.SCALE_10000,
            )
            checkpoint = _reconstruct_checkpoint(
                operands,
                envelope,
                sources,
                durable_next_window_sequence=1,
            )
            payload = checkpoint["checkpoint_result_payload"]
            self.assertEqual(payload["processed_window_sequences"], [0])
            self.assertEqual(
                payload["checkpoint_counts"]["acknowledgement"],
                {"pre": 0, "post": 200, "delta": 200},
            )
            self.assertEqual(
                payload["checkpoint_counts"]["attempt"],
                {"pre": 0, "post": 4, "delta": 4},
            )
            self.assertEqual(
                payload["checkpoint_counts"]["telemetry_record"],
                {"pre": 0, "post": 400, "delta": 400},
            )
            effect = payload["checkpoint_effects"][0]["effect_payload"]
            self.assertEqual(effect["window_sequence"], 0)
            self.assertEqual(effect["attestation_disposition"], "NOT_REQUIRED")
            for field in (
                "attempt_event_head_sha256",
                "detector_event_sha256",
                "prepared_sha256",
                "acknowledgement_head_sha256",
                "finalization_event_head_sha256",
                "telemetry_record_head_sha256",
            ):
                self.assertIsNotNone(effect[field])

    def test_real_canonical_nonzero_checkpoint_reports_only_local_delta(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            operands = _canonical_checkpoint_operands(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                first = harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                second = harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(1, 1)
                )
                self.assertEqual(tuple(item.window_sequence for item in first), (0,))
                self.assertEqual(tuple(item.window_sequence for item in second), (1,))
                sources = harness.runner.composition.response_store.poll(
                    consumer_id="checkpoint-test", limit=WINDOW_QUERY_COUNT * 2
                )
            finally:
                harness.close()
            _append_checkpoint_telemetry(
                operands, sources, start_window_sequence=0, window_count=2
            )
            *_unused, envelope = _bounded_fixture(
                start=1,
                count=1,
                target_profile=Exp012ScaleProfile.SCALE_10000,
            )
            checkpoint = _reconstruct_checkpoint(
                operands,
                envelope,
                sources,
                durable_next_window_sequence=2,
            )
            payload = checkpoint["checkpoint_result_payload"]
            self.assertEqual(payload["processed_window_sequences"], [1])
            self.assertEqual(len(payload["checkpoint_effects"]), 1)
            self.assertEqual(
                payload["checkpoint_effects"][0]["effect_payload"]["window_sequence"],
                1,
            )
            self.assertEqual(
                payload["checkpoint_counts"]["acknowledgement"],
                {"pre": 200, "post": 400, "delta": 200},
            )
            self.assertEqual(
                payload["checkpoint_counts"]["attempt"],
                {"pre": 4, "post": 8, "delta": 4},
            )
            self.assertEqual(
                payload["checkpoint_counts"]["telemetry_record"],
                {"pre": 400, "post": 800, "delta": 400},
            )

    def test_real_future_orphaned_attempt_refuses_completed_c1_reconstruction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            operands = _canonical_checkpoint_operands(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                sources = harness.runner.composition.response_store.poll(
                    consumer_id="checkpoint-test", limit=WINDOW_QUERY_COUNT * 2
                )
                identity = build_shadow_attempt_identity(
                    tuple(sources[WINDOW_QUERY_COUNT : WINDOW_QUERY_COUNT + TRACE_QUERY_COUNT]),
                    trace_sequence_index=0,
                )
                harness.runner.composition.shadow_attempt_store.start_attempt(
                    identity, started_at_utc="2026-08-12T01:00:00Z"
                )
            finally:
                harness.close()
            _append_checkpoint_telemetry(
                operands, sources, start_window_sequence=0, window_count=1
            )
            *_unused, envelope = _bounded_fixture(
                start=0,
                count=1,
                target_profile=Exp012ScaleProfile.SCALE_10000,
            )
            with self.assertRaisesRegex(
                Exception, "EXP012_GATE_C_CHECKPOINT_ORPHANED_SUFFIX"
            ):
                _reconstruct_checkpoint(
                    operands,
                    envelope,
                    sources,
                    durable_next_window_sequence=1,
                )
            self.assertIsNone(_RecoveringCheckpointLedger.completed_result)

    def test_real_future_completed_attempt_refuses_completed_c1_reconstruction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            operands = _canonical_checkpoint_operands(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                sources = harness.runner.composition.response_store.poll(
                    consumer_id="checkpoint-test", limit=WINDOW_QUERY_COUNT * 2
                )
                harness.runner.composition.shadow_worker.build(
                    tuple(sources[WINDOW_QUERY_COUNT : WINDOW_QUERY_COUNT * 2])
                )
            finally:
                harness.close()
            _append_checkpoint_telemetry(
                operands, sources, start_window_sequence=0, window_count=1
            )
            *_unused, envelope = _bounded_fixture(
                start=0,
                count=1,
                target_profile=Exp012ScaleProfile.SCALE_10000,
            )
            with self.assertRaisesRegex(
                Exception, "EXP012_GATE_C_CHECKPOINT_ATTEMPT_SUFFIX_INVALID"
            ):
                _reconstruct_checkpoint(
                    operands,
                    envelope,
                    sources,
                    durable_next_window_sequence=1,
                )
            self.assertIsNone(_RecoveringCheckpointLedger.completed_result)

    def test_real_future_telemetry_refuses_completed_c1_reconstruction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            operands = _canonical_checkpoint_operands(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                sources = harness.runner.composition.response_store.poll(
                    consumer_id="checkpoint-test", limit=WINDOW_QUERY_COUNT * 2
                )
            finally:
                harness.close()
            _append_checkpoint_telemetry(
                operands, sources, start_window_sequence=0, window_count=2
            )
            *_unused, envelope = _bounded_fixture(
                start=0,
                count=1,
                target_profile=Exp012ScaleProfile.SCALE_10000,
            )
            with self.assertRaisesRegex(Exception, "TELEMETRY_PREFIX_INVALID"):
                _reconstruct_checkpoint(
                    operands,
                    envelope,
                    sources,
                    durable_next_window_sequence=1,
                )
            self.assertIsNone(_RecoveringCheckpointLedger.completed_result)

    def test_real_future_pending_finalization_refuses_completed_c1_reconstruction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            operands = _canonical_checkpoint_operands(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                sources = harness.runner.composition.response_store.poll(
                    consumer_id="checkpoint-test", limit=WINDOW_QUERY_COUNT * 2
                )
                harness.runner._prepare_window(
                    tuple(sources[WINDOW_QUERY_COUNT : WINDOW_QUERY_COUNT * 2])
                )
                self.assertIsNotNone(
                    harness.runner.composition.finalization_store.pending()
                )
            finally:
                harness.close()
            _append_checkpoint_telemetry(
                operands, sources, start_window_sequence=0, window_count=1
            )
            *_unused, envelope = _bounded_fixture(
                start=0,
                count=1,
                target_profile=Exp012ScaleProfile.SCALE_10000,
            )
            with self.assertRaisesRegex(
                Exception, "EXP012_GATE_C_CHECKPOINT_FINALIZATION_SUFFIX_INVALID"
            ):
                _reconstruct_checkpoint(
                    operands,
                    envelope,
                    sources,
                    durable_next_window_sequence=1,
                )
            self.assertIsNone(_RecoveringCheckpointLedger.completed_result)

    def test_real_future_finalized_detector_effect_refuses_c1_reconstruction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root)
            operands = _canonical_checkpoint_operands(root)
            try:
                harness.serve_many(WINDOW_QUERY_COUNT * 2)
                harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(0, 1)
                )
                harness.runner.process_ready_windows(
                    execution_bound=GateCWindowExecutionBound(1, 1)
                )
                sources = harness.runner.composition.response_store.poll(
                    consumer_id="checkpoint-test", limit=WINDOW_QUERY_COUNT * 2
                )
                self.assertEqual(
                    harness.runner.composition.detector_store.load_progression()
                    .next_window_sequence,
                    2,
                )
                self.assertEqual(
                    harness.runner.composition.finalization_store.next_window_sequence(),
                    2,
                )
            finally:
                harness.close()
            _append_checkpoint_telemetry(
                operands, sources, start_window_sequence=0, window_count=2
            )
            *_unused, envelope = _bounded_fixture(
                start=0,
                count=1,
                target_profile=Exp012ScaleProfile.SCALE_10000,
            )
            with self.assertRaisesRegex(
                Exception, "EXP012_GATE_C_CHECKPOINT_FINALIZATION_SUFFIX_INVALID"
            ):
                _reconstruct_checkpoint(
                    operands,
                    envelope,
                    sources,
                    durable_next_window_sequence=1,
                )
            self.assertIsNone(_RecoveringCheckpointLedger.completed_result)

    def test_checkpoint_replay_or_identity_drift_refuses_before_live_seam(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        _, _, _, _, envelope = _bounded_fixture(
            target_profile=Exp012ScaleProfile.SCALE_10000
        )

        class _CompletedLedger:
            def __init__(self, *_args, **_kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def state(self, _envelope):
                return SimpleNamespace(completed_event_sha256="a" * 64)

        with mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verify_current_checkpoint_envelope",
            return_value=(envelope, {"observed": {"next_window_sequence": 1}}, tuple(range(10000))),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.SQLiteGateCCheckpointLedger",
            _CompletedLedger,
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.run_gate_c_execute_from_cli"
        ) as execute, self.assertRaisesRegex(
            Exception, "EXP012_GATE_C_CHECKPOINT_ALREADY_COMPLETED"
        ):
            run_gate_c_checkpoint(operands, {"ignored": True})
        execute.assert_not_called()

    def test_started_zero_effect_checkpoint_resumes_and_started_precedes_execute(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        _, _, _, _, envelope = _bounded_fixture(
            target_profile=Exp012ScaleProfile.SCALE_10000
        )
        order = []

        class _Ledger:
            def __init__(self, *_args, **_kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def state(self, _envelope):
                return None

            def start(self, *_args, **_kwargs):
                order.append("checkpoint-start")
                return SimpleNamespace(completed_event_sha256=None)

            def complete(self, *_args, **_kwargs):
                order.append("complete")

        class _Telemetry:
            def __init__(self, *_args, **_kwargs):
                pass

            def __enter__(self):
                order.append("telemetry-open")
                return self

            def __exit__(self, *_args):
                return None

        def execute(*_args, **_kwargs):
            order.append("execute")
            return (SimpleNamespace(window_sequence=0),)

        with mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verify_current_checkpoint_envelope",
            return_value=(envelope, {"observed": {"next_window_sequence": 0}}, tuple(range(10000))),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.SQLiteGateCCheckpointLedger", _Ledger
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator._require_safe_checkpoint_start",
            side_effect=lambda *_args: order.append("safe-start"),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.SQLiteShadowSearchTelemetryStore", _Telemetry
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator._telemetry_binding", return_value=object()
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.run_gate_c_execute_from_cli",
            side_effect=execute,
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verified_checkpoint_transition",
            return_value=_checkpoint_transition_fixture(),
        ):
            run_gate_c_checkpoint(operands, {"ignored": True})
        self.assertLess(order.index("safe-start"), order.index("execute"))
        self.assertLess(order.index("checkpoint-start"), order.index("execute"))
        self.assertLess(order.index("telemetry-open"), order.index("execute"))

    def test_orphaned_checkpoint_attempt_refuses_before_live_seam(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        _, _, _, _, envelope = _bounded_fixture(
            target_profile=Exp012ScaleProfile.SCALE_10000
        )
        with mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verified_checkpoint_transition",
            side_effect=gate_c_scale_operator.Exp012ScaleGateCOperatorError(
                "EXP012_GATE_C_CHECKPOINT_ORPHANED_SUFFIX"
            ),
        ), self.assertRaisesRegex(Exception, "EXP012_GATE_C_CHECKPOINT_ORPHANED"):
            gate_c_scale_operator._require_safe_checkpoint_start(
                operands, envelope, tuple(range(10000))
            )

    def test_checkpoint_cli_requires_distinct_confirmation(self) -> None:
        operands = _base_c(Exp012ScaleProfile.SCALE_10000)
        with mock.patch.object(
            gate_c_scale_operator, "load_operands", return_value=operands
        ), mock.patch.object(
            gate_c_scale_operator, "build_gate_c_plan", return_value={"plan": True}
        ), mock.patch.object(
            gate_c_scale_operator, "run_gate_c_checkpoint"
        ) as checkpoint:
            refused = gate_c_scale_operator.main(
                [
                    "--operands", "/tmp/operands.json",
                    "--mode", "checkpoint-execute",
                    "--execution-envelope", "/tmp/envelope.json",
                ]
            )
        self.assertEqual(refused, 2)
        checkpoint.assert_not_called()

        with mock.patch(
            "vdbench.exp012_scale_gate_c_operator._verify_current_checkpoint_envelope",
            side_effect=GateCBoundedExecutionError("GATE_C_BOUNDED_ENVELOPE_MISMATCH"),
        ), mock.patch(
            "vdbench.exp012_scale_gate_c_operator.run_gate_c_execute_from_cli"
        ) as execute, self.assertRaises(GateCBoundedExecutionError):
            run_gate_c_checkpoint(operands, {"ignored": True})
        execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
