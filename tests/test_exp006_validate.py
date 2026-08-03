"""Contract test for the offline EXP-006 artifact validator."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from experiments.exp006_validate import run_validation


class Exp006ValidationTests(unittest.TestCase):
    def test_validator_writes_reproducible_offline_evidence_for_all_scenarios(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "exp-006"
            result = run_validation(output_dir=output, detector_seed=20260804)

            manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
            summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))

        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(manifest["execution_mode"], "offline")
        self.assertEqual(summary["metrics"], ["COSINE", "L2"])
        self.assertEqual(
            set(summary["scenarios"]),
            {"restart_recovery", "event_integrity", "backpressure", "dry_run_noop"},
        )
        self.assertTrue(all(summary["scenarios"].values()))
        self.assertEqual(summary["actuation_trap_calls"], [])


if __name__ == "__main__":
    unittest.main()
