"""Coverage for the EXP-011 offline structural scenario runner."""

from __future__ import annotations

import ast
import json
from pathlib import Path
import tempfile
import unittest

from vdbench.exp011_offline_acquisition import (
    EVIDENCE_STATUS,
    Exp011OfflineError,
    _build_fast_fixture,
    _scenario_atomic_monitor_state_head_append,
    _scenario_bare_profile_refusal,
    _scenario_bare_root_capability_refusal,
    _scenario_canonical_pre_result_control_binding,
    _scenario_concurrent_append_vs_refresh,
    _scenario_forged_head_refusal,
    _scenario_mismatch_matrix,
    _scenario_monitor_state_head_divergence_fails_closed,
    _scenario_restart_and_complete_hash_chain_replay,
    _scenario_rollback_available_without_profile_evidence,
    _scenario_stale_superseded_head_refusal,
    run_exp011_offline,
)

MODULE_PATH = Path(__file__).parents[1] / "src" / "vdbench" / "exp011_offline_acquisition.py"


class Exp011OfflineScenarioTests(unittest.TestCase):
    """One test per scenario, driven directly against a shared real fixture."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.directory = tempfile.TemporaryDirectory()
        cls.root = Path(cls.directory.name)
        cls.fixture = _build_fast_fixture(store_path=cls.root / "fixture.sqlite3")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.fixture.close()
        cls.directory.cleanup()

    def test_canonical_pre_result_control_binding(self) -> None:
        result = _scenario_canonical_pre_result_control_binding(self.fixture)
        self.assertTrue(result.passed)
        self.assertEqual(result.scenario_id, "canonical_pre_result_control_binding")
        self.assertIn("FRESH_BIND_OK", result.reason_codes)

    def test_atomic_monitor_state_head_append(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            result = _scenario_atomic_monitor_state_head_append(Path(scratch) / "store.sqlite3")
        self.assertTrue(result.passed)
        self.assertEqual(result.reason_codes, ("HEAD_AND_STATE_ATOMIC",))

    def test_stale_superseded_head_refusal(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            fixture = _build_fast_fixture(store_path=Path(scratch) / "fixture.sqlite3")
            try:
                result = _scenario_stale_superseded_head_refusal(fixture)
            finally:
                fixture.close()
        self.assertTrue(result.passed)
        self.assertIn("DETECTOR_HEAD_MISMATCH", result.reason_codes)
        self.assertIn("OLD_BIND_STILL_HISTORICALLY_VALID", result.reason_codes)

    def test_restart_and_complete_hash_chain_replay(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store_path = Path(scratch) / "fixture.sqlite3"
            fixture = _build_fast_fixture(store_path=store_path)
            try:
                result = _scenario_restart_and_complete_hash_chain_replay(fixture, store_path)
            finally:
                fixture.close()
        self.assertTrue(result.passed)
        self.assertEqual(result.reason_codes, ("RESTART_REPLAY_MATCHES",))

    def test_forged_head_refusal(self) -> None:
        result = _scenario_forged_head_refusal(self.fixture)
        self.assertTrue(result.passed)
        self.assertEqual(result.reason_codes, ("DETECTOR_HEAD_MISMATCH",))

    def test_concurrent_append_vs_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            result = _scenario_concurrent_append_vs_refresh(
                Path(scratch) / "store.sqlite3", self.fixture.stream_key
            )
        self.assertTrue(result.passed)
        self.assertEqual(result.reason_codes, ("STORE_ALREADY_OPEN",))

    def test_monitor_state_head_divergence_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            result = _scenario_monitor_state_head_divergence_fails_closed(
                Path(scratch) / "store.sqlite3", self.fixture.stream_key
            )
        self.assertTrue(result.passed)
        self.assertEqual(result.reason_codes, ("STORE_SCHEMA_INVALID",))

    def test_bare_profile_refusal(self) -> None:
        result = _scenario_bare_profile_refusal(self.fixture)
        self.assertTrue(result.passed)
        self.assertEqual(result.reason_codes, ("RESPONSE_PROFILE_INVALID",))

    def test_bare_root_capability_refusal(self) -> None:
        result = _scenario_bare_root_capability_refusal(self.fixture)
        self.assertTrue(result.passed)
        self.assertEqual(result.reason_codes, ("ROOT_PINNED_CAPABILITY_INVALID",))

    def test_rollback_available_without_profile_evidence(self) -> None:
        result = _scenario_rollback_available_without_profile_evidence()
        self.assertTrue(result.passed)
        self.assertEqual(
            result.reason_codes, ("ROLLBACK_HAS_NO_RESPONSE_PROFILE_DEPENDENCY",)
        )

    def test_mismatch_matrix_all_eleven_axes_refuse(self) -> None:
        result = _scenario_mismatch_matrix(self.fixture)
        self.assertTrue(result.passed, result.reason_codes)
        ok_axes = {code.split(":")[1] for code in result.reason_codes if code.startswith("AXIS_OK:")}
        self.assertEqual(
            ok_axes,
            {
                "window_sequence", "provenance_window_id", "provenance_manifest",
                "metric", "stratum", "configuration", "data", "flat", "hnsw",
                "environment", "source",
            },
        )
        self.assertFalse(any(code.startswith("AXIS_FAILED:") for code in result.reason_codes))


class Exp011OfflineEndToEndTests(unittest.TestCase):
    def test_run_exp011_offline_produces_labeled_structural_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            output_dir = Path(scratch) / "exp011-run"
            result = run_exp011_offline(output_dir=output_dir)

            self.assertEqual(result.evidence_status, EVIDENCE_STATUS)
            self.assertEqual(len(result.scenarios), 11)
            self.assertTrue(all(item.passed for item in result.scenarios))

            summary = json.loads((output_dir / "summary.json").read_text())
            self.assertEqual(summary["evidence_status"], EVIDENCE_STATUS)
            self.assertEqual(summary["scenario_count"], 11)
            self.assertTrue(summary["all_passed"])

            for scenario in result.scenarios:
                document = json.loads((output_dir / f"{scenario.scenario_id}.json").read_text())
                self.assertEqual(document["evidence_status"], EVIDENCE_STATUS)
                self.assertEqual(document["passed"], scenario.passed)
                self.assertEqual(document["evidence_digest"], scenario.evidence_digest)

    def test_refuses_to_overwrite_existing_output_directory(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            output_dir = Path(scratch) / "exp011-run"
            output_dir.mkdir()
            with self.assertRaises(Exp011OfflineError):
                run_exp011_offline(output_dir=output_dir)


class Exp011OfflineAdversarialTests(unittest.TestCase):
    def test_module_never_imports_policy(self) -> None:
        source = MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(MODULE_PATH))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        forbidden = {"policy", "vdbench.policy"}
        self.assertFalse(
            {
                item
                for item in imported
                if item in forbidden or item.endswith(".policy")
            }
        )


if __name__ == "__main__":
    unittest.main()
