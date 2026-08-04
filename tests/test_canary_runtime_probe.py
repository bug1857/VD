"""Fail-closed fake-port tests for the Stage-4 read-only runtime probe."""

from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path
import unittest

from vdbench.canary_route_state import RouteStateBinding
from vdbench.canary_runtime_probe import Stage4ServingRuntimeProbe
from vdbench.canary_runtime_types import Stage4RuntimeReadiness, Stage4SlotSafety
from vdbench.config import Metric
from vdbench.milvus_serving import ServingPreflightResult
from vdbench.shadow_event_types import MonitorStreamKey


class _PreflightPort:
    def __init__(self, *results: object) -> None:
        self._results = list(results)
        self.calls = 0

    def preflight(self) -> object:
        self.calls += 1
        result = self._results[min(self.calls - 1, len(self._results) - 1)]
        if isinstance(result, BaseException):
            raise result
        return result


class CanaryRuntimeProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.binding = RouteStateBinding(
            metric=Metric.L2,
            threshold_stratum="target-075",
            last_known_good_ef=400,
            configuration_identity="stage4-config",
            data_identity="stage4-data",
            flat_binding_id="stage4-flat",
            hnsw_binding_id="stage4-hnsw",
        )
        cls.stream = MonitorStreamKey(
            stream_id="stage4-runtime-probe",
            metric=Metric.L2,
            threshold_stratum="target-075",
            configuration_identity="stage4-config",
            data_identity="stage4-data",
            flat_binding_id="stage4-flat",
            hnsw_binding_id="stage4-hnsw",
        )

    def _probe(self, *results: object, clock=lambda: "2026-08-04T22:00:00Z"):
        port = _PreflightPort(*results)
        return (
            Stage4ServingRuntimeProbe(
                expected_binding=self.binding,
                expected_stream=self.stream,
                serving_preflight=port,
                utc_now=clock,
            ),
            port,
        )

    def test_complete_exact_single_stream_maps_to_readiness_and_slot_safety(self) -> None:
        complete = ServingPreflightResult(True, 1, ())
        probe, port = self._probe(complete, complete)

        readiness = probe.preflight(binding=self.binding)
        safety = probe.slot_safety(binding=self.binding)

        self.assertTrue(readiness.serving_preflight_complete)
        self.assertEqual(readiness.binding, self.binding)
        self.assertEqual(readiness.observed_at_utc, "2026-08-04T22:00:00Z")
        self.assertEqual(readiness.reason_codes, ())
        self.assertTrue(safety.health_ok)
        self.assertTrue(safety.identity_ok)
        self.assertIsNone(safety.reason_code)
        self.assertEqual(port.calls, 2)

    def test_requested_binding_mismatch_fails_without_reading_port(self) -> None:
        probe, port = self._probe(ServingPreflightResult(True, 1, ()))
        requested = replace(self.binding, data_identity="different-data")

        readiness = probe.preflight(binding=requested)
        safety = probe.slot_safety(binding=requested)

        self.assertFalse(readiness.serving_preflight_complete)
        self.assertEqual(readiness.reason_codes, ("RUNTIME_BINDING_MISMATCH",))
        self.assertFalse(safety.health_ok)
        self.assertFalse(safety.identity_ok)
        self.assertEqual(safety.reason_code, "RUNTIME_BINDING_MISMATCH")
        self.assertEqual(port.calls, 0)

    def test_health_load_and_identity_reasons_map_to_distinct_unsafe_facts(self) -> None:
        cases = (
            ("health", "STACK_HEALTH_UNHEALTHY", False, True, "STACK_HEALTH_UNHEALTHY"),
            ("load", "COLLECTION_NOT_LOADED:HNSW", False, True, "COLLECTION_NOT_LOADED_HNSW"),
            ("identity", "COLLECTION_IDENTITY_MISMATCH:FLAT", True, False, "COLLECTION_IDENTITY_MISMATCH_FLAT"),
        )
        for name, reason, health, identity, expected in cases:
            with self.subTest(name=name):
                probe, port = self._probe(ServingPreflightResult(False, 0, (reason,)))
                safety = probe.slot_safety(binding=self.binding)
                self.assertEqual((safety.health_ok, safety.identity_ok), (health, identity))
                self.assertEqual(safety.reason_code, expected)
                self.assertEqual(port.calls, 1)

    def test_scope_unknown_malformed_and_exceptional_preflight_fail_closed(self) -> None:
        cases = (
            ("complete_scope", ServingPreflightResult(True, 2, ()), "STAGE4_STREAM_SCOPE_AMBIGUOUS"),
            (
                "incomplete_scope",
                ServingPreflightResult(False, 1, ("STACK_HEALTH_UNHEALTHY",)),
                "STAGE4_STREAM_SCOPE_AMBIGUOUS",
            ),
            ("unknown", ServingPreflightResult(False, 0, ("UNEXPECTED reason",)), "SERVING_PREFLIGHT_REASON_UNKNOWN"),
            ("empty", ServingPreflightResult(False, 0, ()), "SERVING_PREFLIGHT_INCOMPLETE"),
            ("malformed", object(), "SERVING_PREFLIGHT_RESULT_INVALID"),
            ("exception", RuntimeError("unavailable"), "SERVING_PREFLIGHT_UNAVAILABLE"),
        )
        for name, result, expected in cases:
            with self.subTest(name=name):
                probe, port = self._probe(result)
                safety = probe.slot_safety(binding=self.binding)
                self.assertFalse(safety.health_ok)
                self.assertFalse(safety.identity_ok)
                self.assertEqual(safety.reason_code, expected)
                self.assertEqual(port.calls, 1)

    def test_invalid_clock_makes_readiness_incomplete_without_reading_port(self) -> None:
        probe, port = self._probe(
            ServingPreflightResult(True, 1, ()), clock=lambda: "not-a-timestamp"
        )

        readiness = probe.preflight(binding=self.binding)

        self.assertFalse(readiness.serving_preflight_complete)
        self.assertEqual(readiness.observed_at_utc, "")
        self.assertEqual(readiness.reason_codes, ("RUNTIME_PROBE_CLOCK_INVALID",))
        self.assertEqual(port.calls, 0)

    def test_constructor_rejects_stream_binding_mismatch(self) -> None:
        with self.assertRaises(ValueError):
            Stage4ServingRuntimeProbe(
                expected_binding=self.binding,
                expected_stream=replace(self.stream, data_identity="different-data"),
                serving_preflight=_PreflightPort(ServingPreflightResult(True, 1, ())),
                utc_now=lambda: "2026-08-04T22:00:00Z",
            )

    def test_slot_safety_value_rejects_implicit_or_malformed_failure(self) -> None:
        with self.assertRaises(ValueError):
            Stage4SlotSafety(False, True)
        with self.assertRaises(ValueError):
            Stage4SlotSafety(True, True, "not-a-code")

    def test_ast_rejects_live_approval_route_policy_and_database_imports(self) -> None:
        path = Path("src/vdbench/canary_runtime_probe.py")
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        }
        forbidden = {
            "pymilvus",
            "vdbench.milvus",
            "vdbench.milvus_serving",
            "vdbench.canary_live_runner",
            "vdbench.canary_approval",
            "vdbench.canary_route_authority",
            "vdbench.canary_rollback",
            "vdbench.policy",
        }
        self.assertFalse(forbidden & imports)

    def test_admission_module_preserves_runtime_readiness_public_reexport(self) -> None:
        from vdbench.canary_admission import Stage4RuntimeReadiness as AdmissionReadiness

        self.assertIs(AdmissionReadiness, Stage4RuntimeReadiness)


if __name__ == "__main__":
    unittest.main()
