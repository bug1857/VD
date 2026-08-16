"""Coverage for the committed canonical Gate-C operator entrypoint.

The entrypoint's whole value is that it is auditable, so most of these tests
are about what it must *never* do: serve a request, generate a query, issue a
physical search during preflight, reach Milvus before the operand and store
bindings are proven, or paper over an ambiguous STARTED attempt.
"""

from __future__ import annotations

import ast
import contextlib
import inspect
import io
import json
import tempfile
import unittest
from datetime import UTC
from pathlib import Path
from unittest import mock

import numpy as np

import vdbench.exp010_gate_c_operator as gate_c_operator
from tests.test_exp010_live_runner import DATASET001, _trace_for
from vdbench.config import Metric
from vdbench.exp010_gate_c_operator import (
    GATE_C_PLAN_SCHEMA_VERSION,
    OPERAND_FIELDS,
    Exp010GateCOperatorError,
    MonotonicUtcClock,
    build_gate_c_plan,
    load_operands,
    main,
)
from vdbench.exp010_live_runner import Exp010LiveRunner, Exp010OperatorConfiguration
from vdbench.exp010_serving_configuration import (
    Exp010ServingConfiguration,
    derive_serving_configuration_identity,
)
from vdbench.host_observation import RangeQueryRequest, ServedQueryOutcome
from vdbench.shadow_window import WINDOW_QUERY_COUNT

_ENVIRONMENT = "e" * 64
_REVISION = "revision/exp010-gate-c"
_SERVING = Exp010ServingConfiguration(
    metric=Metric.L2,
    threshold_stratum="target-075",
    threshold_radius=2.0,
    range_filter=0.0,
    limit=100,
    served_ef=400,
    dimensions=128,
    consistency_level="Strong",
)
_CONFIGURATION_IDENTITY = derive_serving_configuration_identity(_SERVING)


def _operand_document(root: Path, **overrides: object) -> dict[str, object]:
    document: dict[str, object] = {
        "stream_id": "exp010-gate-c-test",
        "metric": "L2",
        "threshold_stratum": "target-075",
        "threshold_radius": 2.0,
        "range_filter": 0.0,
        "limit": 100,
        "served_ef": 400,
        "dimensions": 128,
        "consistency_level": "Strong",
        "configuration_identity": _CONFIGURATION_IDENTITY,
        "flat_binding_id": "flat-index-v1",
        "hnsw_binding_id": "hnsw-index-v1",
        "source_revision": _REVISION,
        "environment_manifest_sha256": _ENVIRONMENT,
        "detector_seed": 20260813,
        "milvus_uri": "http://milvus.invalid:19530",
        "flat_collection_name": "vd_test_l2_flat",
        "hnsw_collection_name": "vd_test_l2_hnsw",
        "store_root": str(root / "stores"),
        "dataset001_dir": str(DATASET001),
        "exp010_output_dir": str(root / "exp010"),
        "etcd_container": "milvus-etcd",
        "minio_container": "milvus-minio",
    }
    document.update(overrides)
    return document


def _write_operands(root: Path, **overrides: object) -> Path:
    path = root / "operands.json"
    path.write_text(json.dumps(_operand_document(root, **overrides)), encoding="utf-8")
    return path


class _Serving:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, request: RangeQueryRequest) -> ServedQueryOutcome:
        self.calls += 1
        return ServedQueryOutcome(True, False, 1, 1.0)


class _ShadowCapture:
    def __init__(self) -> None:
        self.calls = 0

    def capture(self, sources, *, trace_sequence_index: int):
        self.calls += 1
        return _trace_for(sources)


def _seed_campaign(root: Path, *, sources: int) -> None:
    """Create a real initialized campaign the operator can preflight against.

    Sources exist only because `serve()` was called, exactly as Gate B
    requires; the operator entrypoint under test never does this itself.
    """

    tick = 0

    def clock() -> str:
        nonlocal tick
        tick += 1
        return (
            f"2026-08-15T{tick // 3600:02d}:"
            f"{(tick // 60) % 60:02d}:{tick % 60:02d}Z"
        )

    runner = Exp010LiveRunner(
        configuration=Exp010OperatorConfiguration(
            milvus_uri="http://milvus.invalid:19530",
            flat_collection_name="vd_test_l2_flat",
            hnsw_collection_name="vd_test_l2_hnsw",
            metric=Metric.L2,
            threshold_stratum="target-075",
            threshold_radius=2.0,
            served_ef=400,
            detector_seed=20260813,
            stream_id="exp010-gate-c-test",
            configuration_identity=_CONFIGURATION_IDENTITY,
            flat_binding_id="flat-index-v1",
            hnsw_binding_id="hnsw-index-v1",
            source_revision=_REVISION,
            environment_manifest_sha256=_ENVIRONMENT,
            store_root=root / "stores",
            dataset001_dir=DATASET001,
            exp010_output_dir=root / "exp010",
        ),
        serving_executor=_Serving(),
        shadow_capture_executor=_ShadowCapture(),
        clock=lambda: "2026-08-15T00:00:00Z",
        shadow_captured_at_clock=clock,
    )
    try:
        generator = np.random.Generator(np.random.PCG64(11))
        for index in range(sources):
            vector = generator.standard_normal(2).astype("<f4")
            runner.serve(
                RangeQueryRequest(
                    index,
                    runner.composition.stream_key,
                    tuple(float(value) for value in vector),
                    2.0,
                    0.0,
                    100,
                    400,
                )
            )
    finally:
        runner.composition.close()


class OperandValidationTests(unittest.TestCase):
    def test_every_operand_is_required_and_never_defaulted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for field in OPERAND_FIELDS:
                with self.subTest(missing=field):
                    document = _operand_document(root)
                    del document[field]
                    path = root / "operands.json"
                    path.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaises(Exp010GateCOperatorError) as caught:
                        load_operands(path)
                    self.assertEqual(
                        caught.exception.code, "GATE_C_OPERANDS_INCOMPLETE"
                    )
                    self.assertIn(field, str(caught.exception))

    def test_detector_seed_must_be_explicit(self) -> None:
        """The seed is authority; it has no default anywhere in this path."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = _operand_document(root)
            del document["detector_seed"]
            path = root / "operands.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaises(Exp010GateCOperatorError) as caught:
                load_operands(path)
            self.assertEqual(caught.exception.code, "GATE_C_OPERANDS_INCOMPLETE")
            self.assertIn("detector_seed", str(caught.exception))

    def test_detector_seed_must_be_an_exact_integer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for value in ("20260813", 20260813.0, True, None):
                with self.subTest(value=value):
                    path = _write_operands(root, detector_seed=value)
                    with self.assertRaises(Exp010GateCOperatorError) as caught:
                        load_operands(path)
                    self.assertEqual(caught.exception.code, "GATE_C_OPERAND_INVALID")

    def test_unexpected_operand_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = _write_operands(root, unexpected_key="anything")
            with self.assertRaises(Exp010GateCOperatorError) as caught:
                load_operands(path)
            self.assertEqual(caught.exception.code, "GATE_C_OPERANDS_UNEXPECTED")

    def test_configuration_identity_must_be_rederivable(self) -> None:
        """A supplied identity is never trusted; it must match the derivation."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # Syntactically governed, but not what these operands produce.
            path = _write_operands(
                root, configuration_identity="exp010-serving-config-v1:sha256:" + "a" * 64
            )
            with self.assertRaises(Exp010GateCOperatorError) as caught:
                load_operands(path)
            self.assertEqual(
                caught.exception.code, "GATE_C_CONFIGURATION_IDENTITY_MISMATCH"
            )

    def test_configuration_identity_syntax_is_governed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for value in ("config-v1", "", "exp010-serving-config-v1:sha256:zz"):
                with self.subTest(value=value):
                    path = _write_operands(root, configuration_identity=value)
                    with self.assertRaises(Exception) as caught:
                        load_operands(path)
                    self.assertEqual(
                        getattr(caught.exception, "code", None),
                        "CONFIGURATION_IDENTITY_SYNTAX_INVALID",
                    )

    def test_serving_operand_drift_changes_the_derived_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = _write_operands(root, threshold_radius=2.5)
            with self.assertRaises(Exp010GateCOperatorError) as caught:
                load_operands(path)
            self.assertEqual(
                caught.exception.code, "GATE_C_CONFIGURATION_IDENTITY_MISMATCH"
            )

    def test_environment_digest_must_be_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = _write_operands(root, environment_manifest_sha256="e" * 63)
            with self.assertRaises(Exp010GateCOperatorError) as caught:
                load_operands(path)
            self.assertEqual(caught.exception.code, "GATE_C_OPERAND_INVALID")

    def test_malformed_operand_file_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "operands.json"
            path.write_text("not json", encoding="utf-8")
            with self.assertRaises(Exp010GateCOperatorError) as caught:
                load_operands(path)
            self.assertEqual(caught.exception.code, "GATE_C_OPERANDS_MALFORMED")


class PreflightTests(unittest.TestCase):
    def test_preflight_issues_zero_searches_and_zero_serves(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _seed_campaign(root, sources=WINDOW_QUERY_COUNT)
            path = _write_operands(root)
            with mock.patch.object(
                gate_c_operator,
                "run_gate_c_execute_from_cli",
                side_effect=AssertionError("preflight must not execute"),
            ), contextlib.redirect_stdout(io.StringIO()):
                exit_code = main(["--operands", str(path), "--mode", "preflight"])
            self.assertEqual(exit_code, 0)

    def test_preflight_plan_reports_the_canonical_path_and_zero_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _seed_campaign(root, sources=WINDOW_QUERY_COUNT)
            operands = load_operands(_write_operands(root))
            plan = build_gate_c_plan(operands)
            self.assertEqual(plan["schema_version"], GATE_C_PLAN_SCHEMA_VERSION)
            self.assertEqual(
                plan["canonical_entrypoint"],
                "vdbench.exp010_live_runner.Exp010LiveRunner.process_ready_windows",
            )
            self.assertEqual(plan["physical_searches_issued_by_preflight"], 0)
            self.assertEqual(plan["serve_calls_issued_by_gate_c"], 0)
            self.assertEqual(plan["observed"]["complete_source_windows"], 1)
            self.assertEqual(plan["observed"]["next_window_sequence"], 0)
            self.assertEqual(plan["observed"]["windows_pending"], 1)
            self.assertEqual(plan["observed"]["shadow_acknowledged_count"], 0)
            self.assertEqual(
                plan["projected_physical_work"]["flat_searches"], WINDOW_QUERY_COUNT
            )
            self.assertEqual(
                plan["projected_physical_work"]["hnsw_sentinel_searches"],
                WINDOW_QUERY_COUNT,
            )
            self.assertEqual(len(plan["plan_sha256"]), 64)

    def test_plan_digest_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _seed_campaign(root, sources=WINDOW_QUERY_COUNT)
            operands = load_operands(_write_operands(root))
            first = build_gate_c_plan(operands)
            second = build_gate_c_plan(operands)
            self.assertEqual(first["plan_sha256"], second["plan_sha256"])

    def test_preflight_refuses_an_uninitialized_campaign(self) -> None:
        """Gate C advances a campaign; it never creates one."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = _write_operands(root)
            operands = load_operands(path)
            with self.assertRaises(Exp010GateCOperatorError) as caught:
                build_gate_c_plan(operands)
            self.assertIn(
                caught.exception.code,
                {"GATE_C_STORE_ROOT_MISSING", "GATE_C_STORES_NOT_INITIALIZED"},
            )
            self.assertFalse((root / "stores").exists())

    def test_source_revision_mismatch_fails_before_any_capture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _seed_campaign(root, sources=WINDOW_QUERY_COUNT)
            operands = load_operands(
                _write_operands(root, source_revision="revision/wrong")
            )
            with mock.patch.object(
                gate_c_operator,
                "run_gate_c_execute_from_cli",
                side_effect=AssertionError("must not reach physical execution"),
            ), self.assertRaises(Exception) as caught:
                build_gate_c_plan(operands)
            self.assertIn("BINDING_MISMATCH", getattr(caught.exception, "code", ""))

    def test_environment_manifest_mismatch_fails_before_any_capture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _seed_campaign(root, sources=WINDOW_QUERY_COUNT)
            operands = load_operands(
                _write_operands(root, environment_manifest_sha256="f" * 64)
            )
            with self.assertRaises(Exception) as caught:
                build_gate_c_plan(operands)
            self.assertIn("BINDING_MISMATCH", getattr(caught.exception, "code", ""))

    def test_stream_id_mismatch_fails_before_any_capture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _seed_campaign(root, sources=WINDOW_QUERY_COUNT)
            operands = load_operands(_write_operands(root, stream_id="other-stream"))
            with self.assertRaises(Exception) as caught:
                build_gate_c_plan(operands)
            self.assertIn("BINDING_MISMATCH", getattr(caught.exception, "code", ""))

    def test_preflight_releases_its_store_locks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _seed_campaign(root, sources=WINDOW_QUERY_COUNT)
            operands = load_operands(_write_operands(root))
            build_gate_c_plan(operands)
            # A second preflight would raise ..._STORE_BUSY if locks leaked.
            build_gate_c_plan(operands)


class ExecuteModeTests(unittest.TestCase):
    def test_execute_requires_the_separate_confirmation_flag(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _seed_campaign(root, sources=WINDOW_QUERY_COUNT)
            path = _write_operands(root)
            with (
                mock.patch.object(
                    gate_c_operator,
                    "run_gate_c_execute_from_cli",
                    side_effect=AssertionError("must not execute without confirmation"),
                ),
                contextlib.redirect_stdout(io.StringIO()),
                self.assertRaises(Exp010GateCOperatorError) as caught,
            ):
                main(["--operands", str(path), "--mode", "execute"])
            self.assertEqual(
                caught.exception.code, "GATE_C_EXECUTION_NOT_CONFIRMED"
            )

    def test_confirmed_execute_reaches_the_single_seam(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _seed_campaign(root, sources=WINDOW_QUERY_COUNT)
            path = _write_operands(root)
            with mock.patch.object(
                gate_c_operator, "run_gate_c_execute_from_cli", return_value=()
            ) as seam, contextlib.redirect_stdout(io.StringIO()):
                exit_code = main(
                    [
                        "--operands",
                        str(path),
                        "--mode",
                        "execute",
                        "--confirm-physical-shadow-searches",
                    ]
                )
            self.assertEqual(exit_code, 0)
            seam.assert_called_once()

    def test_orphaned_attempt_propagates_and_is_never_retried(self) -> None:
        """Execution ambiguity is surfaced verbatim, never resolved here."""

        from vdbench.v2_shadow_worker import V2ShadowWorkerError

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _seed_campaign(root, sources=WINDOW_QUERY_COUNT)
            path = _write_operands(root)
            failure = V2ShadowWorkerError(
                "SHADOW_ATTEMPT_ORPHANED", "ORPHANED;EXECUTION_OUTCOME_UNKNOWN"
            )
            with (
                mock.patch.object(
                    gate_c_operator, "run_gate_c_execute_from_cli", side_effect=failure
                ) as seam,
                contextlib.redirect_stdout(io.StringIO()),
                self.assertRaises(V2ShadowWorkerError) as caught,
            ):
                main(
                    [
                        "--operands",
                        str(path),
                        "--mode",
                        "execute",
                        "--confirm-physical-shadow-searches",
                    ]
                )
            self.assertEqual(caught.exception.code, "SHADOW_ATTEMPT_ORPHANED")
            self.assertEqual(seam.call_count, 1)


class EntrypointDisciplineTests(unittest.TestCase):
    """Source-level guarantees that survive refactoring."""

    def _tree(self) -> ast.Module:
        return ast.parse(inspect.getsource(gate_c_operator))

    def test_module_calls_process_ready_windows(self) -> None:
        calls = {
            node.func.attr
            for node in ast.walk(self._tree())
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertIn("process_ready_windows", calls)

    def test_module_never_calls_serve_or_admit(self) -> None:
        calls = {
            node.func.attr
            for node in ast.walk(self._tree())
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertNotIn("serve", calls)
        self.assertNotIn("admit", calls)
        self.assertNotIn("capture", calls)
        self.assertNotIn("capture_exp010_population", calls)

    def test_module_never_generates_vectors_or_replays_workload(self) -> None:
        source = inspect.getsource(gate_c_operator)
        for forbidden in (
            "numpy",
            "np.",
            "random",
            "RangeQueryRequest",
            "Exp010RequestIngress",
            "CaptureObserver",
            "capture_exp010_population",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)

    def test_module_imports_no_capture_observer(self) -> None:
        tree = self._tree()
        imported = {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        imported.update(
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        )
        self.assertFalse(
            any("capture_observer" in name.lower() for name in imported), imported
        )

    def test_refusing_executors_raise_on_any_use(self) -> None:
        with self.assertRaises(Exp010GateCOperatorError) as caught:
            gate_c_operator._RefusingServingExecutor().execute(object())
        self.assertEqual(caught.exception.code, "GATE_C_SERVING_FORBIDDEN")
        with self.assertRaises(Exp010GateCOperatorError) as caught:
            gate_c_operator._RefusingShadowCaptureExecutor().capture(
                (), trace_sequence_index=0
            )
        self.assertEqual(
            caught.exception.code, "GATE_C_PREFLIGHT_CAPTURE_FORBIDDEN"
        )

    def test_live_seam_is_the_only_place_pymilvus_can_be_reached(self) -> None:
        seam = inspect.getsource(gate_c_operator.run_gate_c_execute_from_cli)
        self.assertIn("build_readonly_milvus_client", seam)
        module_source = inspect.getsource(gate_c_operator)
        # Import and call both live inside the seam; nothing outside it names
        # the client factory, so no other path can reach PyMilvus.
        self.assertEqual(
            module_source.count("build_readonly_milvus_client"),
            seam.count("build_readonly_milvus_client"),
        )
        self.assertNotIn("build_readonly_milvus_client", inspect.getsource(
            gate_c_operator.build_gate_c_plan
        ))
        self.assertNotIn(
            "build_readonly_milvus_client", inspect.getsource(gate_c_operator.main)
        )


class MonotonicUtcClockTests(unittest.TestCase):
    def test_backward_wall_clock_still_yields_increasing_timestamps(self) -> None:
        from datetime import datetime, timedelta

        base = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)
        moments = iter(
            [base, base - timedelta(hours=1), base - timedelta(days=1), base]
        )
        clock = MonotonicUtcClock(now=lambda: next(moments))
        values = [clock() for _ in range(4)]
        self.assertEqual(values, sorted(values))
        self.assertEqual(len(set(values)), 4)

    def test_repeated_wall_clock_still_strictly_increases(self) -> None:
        from datetime import datetime

        fixed = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)
        clock = MonotonicUtcClock(now=lambda: fixed)
        values = [clock() for _ in range(5)]
        self.assertEqual(len(set(values)), 5)
        self.assertEqual(values, sorted(values))

    def test_naive_datetime_is_refused(self) -> None:
        from datetime import datetime

        # DTZ001 suppressed deliberately and narrowly: supplying a naive
        # datetime IS the subject of this test.
        naive = datetime(2026, 8, 15, 12, 0, 0)  # supplying a naive datetime is the subject of this test  # noqa: DTZ001
        clock = MonotonicUtcClock(now=lambda: naive)
        with self.assertRaises(Exp010GateCOperatorError) as caught:
            clock()
        self.assertEqual(caught.exception.code, "GATE_C_CLOCK_INVALID")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
