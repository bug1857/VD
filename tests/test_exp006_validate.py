"""Contract test for the offline EXP-006 artifact validator."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments.exp006_validate import ValidationError, _write, run_validation


class Exp006ValidationTests(unittest.TestCase):
    def test_artifact_write_failure_leaves_no_partial_or_temporary_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "artifact.json"
            with patch(
                "experiments.exp006_validate.os.link",
                side_effect=OSError("synthetic publish failure"),
            ):
                with self.assertRaisesRegex(OSError, "synthetic publish failure"):
                    _write(target, {"status": "test"})

            self.assertFalse(target.exists())
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_validator_writes_reproducible_offline_evidence_for_all_scenarios(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "exp-006"
            result = run_validation(output_dir=output, detector_seed=20260804)

            manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
            summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
            raw_result = json.loads((output / "raw_result.json").read_text(encoding="utf-8"))
            integrity_cases = json.loads(
                (output / "integrity_case_results.json").read_text(encoding="utf-8")
            )
            persisted_fixture_paths = {
                str(path.relative_to(output))
                for path in output.rglob("*.json")
                if "fixtures" in path.relative_to(output).parts
            }
            persisted_state_paths = {
                str(path.relative_to(output))
                for path in output.rglob("*.json")
                if "state" in path.relative_to(output).parts
            }

        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(manifest["execution_mode"], "offline")
        self.assertEqual(manifest["validation_status"], "COMPLETE")
        self.assertEqual(summary["metrics"], ["COSINE", "L2"])
        self.assertEqual(
            set(summary["scenarios"]),
            {"restart_recovery", "event_integrity", "backpressure", "dry_run_noop"},
        )
        self.assertTrue(all(summary["scenarios"].values()))
        self.assertEqual(summary["actuation_trap_calls"], [])
        self.assertEqual(set(manifest["fixture_sha256"]), persisted_fixture_paths)
        self.assertEqual(set(manifest["state_sha256"]), persisted_state_paths)
        self.assertEqual(
            set(manifest["audit_sha256"]),
            {"audit-cosine.json", "audit-l2.json", "integrity_case_results.json"},
        )
        self.assertEqual(manifest["artifact_directory"], str(output.resolve()))
        self.assertEqual(
            set(integrity_cases),
            {
                "duplicate_event",
                "duplicate_envelope_reference",
                "replay_after_restart",
                "malformed_json",
                "invalid_schema",
                "invalid_timestamp",
                "checksum_mismatch",
                "count_mismatch",
                "identity_change",
            },
        )
        self.assertTrue(all(case["passed"] for case in integrity_cases.values()))
        self.assertEqual(
            {
                name: case["expected_reason_code"]
                for name, case in integrity_cases.items()
            },
            {
                "duplicate_event": "DUPLICATE_EVENT",
                "duplicate_envelope_reference": "DUPLICATE_ENVELOPE_REFERENCE",
                "replay_after_restart": "DUPLICATE_EVENT",
                "malformed_json": "ENVELOPE_LOAD_FAILED",
                "invalid_schema": "ENVELOPE_LOAD_FAILED",
                "invalid_timestamp": "TIMESTAMP_INVALID",
                "checksum_mismatch": "ENVELOPE_LOAD_FAILED",
                "count_mismatch": "DECLARED_OBSERVATION_COUNT_INVALID",
                "identity_change": "STREAM_IDENTITY_CHANGED",
            },
        )
        self.assertTrue(
            all(case["policy_input_calls"] == 0 for case in integrity_cases.values())
        )
        self.assertTrue(
            all(case["audit_records"] for case in integrity_cases.values())
        )
        self.assertEqual(raw_result["event_integrity_cases"], integrity_cases)
        self.assertEqual(
            raw_result["dry_run_no_actuation_proof"],
            {
                "boundary_executed": False,
                "boundary_audit_record_count": 1,
                "controller_calls": [],
                "monitor_imports_actuation": False,
                "monitor_imports_pymilvus": False,
                "monitor_references_canary_enabled": False,
                "trap_client_calls": [],
            },
        )

    def test_validator_fails_closed_after_persisting_incomplete_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "exp-006"
            with patch(
                "experiments.exp006_validate._integrity_and_backpressure",
                return_value=({}, False),
            ):
                with self.assertRaisesRegex(ValidationError, "backpressure"):
                    run_validation(output_dir=output, detector_seed=20260804)

            raw_result = json.loads(
                (output / "raw_result.json").read_text(encoding="utf-8")
            )
            manifest = json.loads(
                (output / "manifest.json").read_text(encoding="utf-8")
            )

        self.assertEqual(raw_result["status"], "INCOMPLETE")
        self.assertFalse(raw_result["scenarios"]["backpressure"])
        self.assertEqual(manifest["validation_status"], "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
