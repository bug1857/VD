"""Contract tests for the offline, artifact-producing EXP-007 validator."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments.exp007_validate import Exp007ValidationError, run_validation


class Exp007ValidationTests(unittest.TestCase):
    def test_validation_handles_dangling_quarantined_symlink_with_relative_output_path(self) -> None:
        """The scanner must never dereference a rejected path during inspection."""

        with tempfile.TemporaryDirectory(dir=".") as directory:
            result = run_validation(
                output_dir=Path(directory) / "exp-007", detector_seed=20260804
            )

        self.assertEqual(result["status"], "COMPLETE")
        self.assertGreaterEqual(
            result["data_minimization"]["ignored_unsafe_symlink_paths"], 1
        )

    def test_validation_writes_complete_evidence_for_all_registered_scenarios(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "exp-007"
            result = run_validation(output_dir=output, detector_seed=20260804)

            manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
            raw_result = json.loads((output / "raw_result.json").read_text(encoding="utf-8"))
            receipt = json.loads((output / "execution_receipt.json").read_text(encoding="utf-8"))

            self.assertEqual(result["status"], "COMPLETE")
            self.assertEqual(manifest["validation_status"], "COMPLETE")
            self.assertEqual(
                set(raw_result["scenarios"]),
                {
                    "atomic_publication_order",
                    "restart_and_redelivery",
                    "duplicate_and_conflict_safety",
                    "backpressure_and_foreground_isolation",
                    "schema_permission_checksum_path_safety",
                    "data_minimization",
                    "dry_run_monitor_composition",
                },
            )
            self.assertTrue(all(raw_result["scenarios"].values()))
            self.assertEqual(
                raw_result["composition"]["evaluated_by_metric"],
                {"COSINE": 1, "L2": 1},
            )
            self.assertTrue(raw_result["composition"]["no_prohibited_source_dependencies"])
            self.assertTrue(raw_result["data_minimization"]["sentinels_only_in_trace_payload"])
            self.assertTrue(raw_result["backpressure"]["foreground_calls"] == 1)
            self.assertTrue(raw_result["backpressure"]["monitor_calls"] == 0)
            self.assertTrue(raw_result["backpressure"]["synchronous_persistence_calls"] == 0)
            self.assertTrue(raw_result["restart"]["acknowledgement_idempotent"])
            self.assertEqual(receipt["validation_status"], "COMPLETE")
            self.assertEqual(receipt["manifest_sha256"], manifest["self_sha256"])
            self.assertEqual(receipt["raw_result_sha256"], raw_result["self_sha256"])
            self.assertTrue(manifest["artifact_sha256"])
            self.assertEqual(
                set(manifest["symlink_target_sha256"]),
                {
                    "safety/symlinked/rejected/"
                    "c1fc6eb6c60ea528f299b7c21a140c46e6ce26ccc9792d048d6e141117b6f757.json"
                },
            )

    def test_validator_fails_closed_after_writing_incomplete_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "exp-007"
            with patch(
                "experiments.exp007_validate._scenario_backpressure",
                return_value=(False, {"forced": True}),
            ):
                with self.assertRaisesRegex(Exp007ValidationError, "backpressure"):
                    run_validation(output_dir=output, detector_seed=20260804)

            raw_result = json.loads((output / "raw_result.json").read_text(encoding="utf-8"))
            manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(raw_result["status"], "INCOMPLETE")
        self.assertFalse(raw_result["scenarios"]["backpressure_and_foreground_isolation"])
        self.assertEqual(manifest["validation_status"], "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
