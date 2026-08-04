"""Failure-first tests for the EXP-009 LKG-only route-state marker."""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import tempfile
import unittest

from vdbench.canary_route_state import (
    FileCanaryRouteStateStore,
    RouteState,
    RouteStateBinding,
)
from vdbench.config import Metric


_TIMESTAMP = "2026-08-04T07:00:00Z"
_RECOVERY_TIMESTAMP = "2026-08-04T07:01:00Z"
_PLAN_SHA256 = "a" * 64


class CanaryRouteStateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self._temporary.name)
        self.path = self.directory / "route-state.json"
        self.binding = RouteStateBinding(
            metric=Metric.L2,
            threshold_stratum="target-075",
            last_known_good_ef=400,
            configuration_identity="config-v1",
            data_identity="data-v1",
            flat_binding_id="flat-v1",
            hnsw_binding_id="hnsw-v1",
        )

    def tearDown(self) -> None:
        self._temporary.cleanup()

    def test_activation_marker_is_strict_and_contains_no_candidate_route_data(self) -> None:
        store = FileCanaryRouteStateStore(self.path)
        record = store.begin_activation(
            binding=self.binding,
            grant_id="grant-exp009-001",
            plan_sha256=_PLAN_SHA256,
            changed_at_utc=_TIMESTAMP,
        )
        document = json.loads(self.path.read_text(encoding="utf-8"))

        self.assertEqual(record.state, RouteState.ACTIVATING)
        self.assertEqual(document["state"], "ACTIVATING")
        self.assertEqual(document["grant_id"], "grant-exp009-001")
        self.assertEqual(document["plan_sha256"], _PLAN_SHA256)
        self.assertNotIn("candidate_ef", document)
        self.assertNotIn("occurrences", document)
        self.assertEqual(os.stat(self.path).st_mode & 0o077, 0)

    def test_restart_after_activation_always_persists_lkg_only_failback(self) -> None:
        store = FileCanaryRouteStateStore(self.path)
        store.begin_activation(
            binding=self.binding,
            grant_id="grant-exp009-001",
            plan_sha256=_PLAN_SHA256,
            changed_at_utc=_TIMESTAMP,
        )

        recovery = FileCanaryRouteStateStore(self.path).recover(
            expected_binding=self.binding,
            changed_at_utc=_RECOVERY_TIMESTAMP,
        )
        persisted = FileCanaryRouteStateStore(self.path).load()

        self.assertTrue(recovery.recovered)
        self.assertTrue(recovery.persisted)
        self.assertEqual(recovery.reason_code, "RECOVERY_FAILBACK")
        self.assertEqual(recovery.record.state, RouteState.LKG_ONLY)
        self.assertIsNone(recovery.record.grant_id)
        self.assertIsNone(recovery.record.plan_sha256)
        self.assertEqual(persisted, recovery.record)

    def test_identity_mismatch_and_malformed_marker_fail_back_without_candidate_state(self) -> None:
        store = FileCanaryRouteStateStore(self.path)
        mismatched = RouteStateBinding(
            metric=Metric.L2,
            threshold_stratum="target-075",
            last_known_good_ef=400,
            configuration_identity="config-v2",
            data_identity="data-v1",
            flat_binding_id="flat-v1",
            hnsw_binding_id="hnsw-v1",
        )
        store.clear_to_lkg(
            binding=mismatched,
            reason_code="EXPLICIT_REMOVAL",
            changed_at_utc=_TIMESTAMP,
        )

        identity_recovery = store.recover(
            expected_binding=self.binding,
            changed_at_utc=_RECOVERY_TIMESTAMP,
        )
        self.path.write_text("{not-json", encoding="utf-8")
        malformed_recovery = store.recover(
            expected_binding=self.binding,
            changed_at_utc=_RECOVERY_TIMESTAMP,
        )

        self.assertEqual(identity_recovery.reason_code, "RECOVERY_IDENTITY_MISMATCH")
        self.assertEqual(identity_recovery.record.state, RouteState.LKG_ONLY)
        self.assertEqual(malformed_recovery.reason_code, "RECOVERY_MARKER_INVALID")
        self.assertEqual(malformed_recovery.record.state, RouteState.LKG_ONLY)
        self.assertIsNone(malformed_recovery.record.plan_sha256)

    def test_missing_marker_and_failed_recovery_write_remain_lkg_only(self) -> None:
        store = FileCanaryRouteStateStore(self.path)
        missing = store.recover(
            expected_binding=self.binding,
            changed_at_utc=_TIMESTAMP,
        )

        self.assertEqual(missing.record.state, RouteState.LKG_ONLY)
        self.assertEqual(missing.reason_code, "RECOVERY_NO_MARKER")
        self.assertTrue(missing.persisted)

        failure_path = self.directory / "missing" / "route-state.json"
        failing = FileCanaryRouteStateStore(failure_path).recover(
            expected_binding=self.binding,
            changed_at_utc=_TIMESTAMP,
        )
        self.assertEqual(failing.record.state, RouteState.LKG_ONLY)
        self.assertEqual(failing.reason_code, "RECOVERY_STATE_WRITE_FAILED")
        self.assertFalse(failing.persisted)

    def test_symlink_marker_is_refused_and_recovery_never_follows_it(self) -> None:
        target = self.directory / "outside.json"
        target.write_text("{}\n", encoding="utf-8")
        self.path.symlink_to(target)

        recovery = FileCanaryRouteStateStore(self.path).recover(
            expected_binding=self.binding,
            changed_at_utc=_TIMESTAMP,
        )

        self.assertEqual(recovery.record.state, RouteState.LKG_ONLY)
        self.assertEqual(recovery.reason_code, "RECOVERY_STATE_WRITE_FAILED")
        self.assertFalse(recovery.persisted)
        self.assertEqual(target.read_text(encoding="utf-8"), "{}\n")

    def test_module_has_no_routing_policy_or_milvus_import(self) -> None:
        source = Path("src/vdbench/canary_route_state.py").read_text(encoding="utf-8")
        imported = {
            alias.name
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            node.module or ""
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.ImportFrom)
        )

        self.assertFalse({"pymilvus", "vdbench.policy", "vdbench.milvus"} & imported)


if __name__ == "__main__":
    unittest.main()
