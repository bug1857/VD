"""Contract tests for the sealed fake-only Stage-4 runtime-probe profile."""

from __future__ import annotations

import ast
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from experiments.exp009_stage4_runtime_probe_validate import (
    RUNTIME_PROBE_SUITE_FILENAMES,
    Exp009Stage4RuntimeProbeValidationError,
    run_validation,
    verify_validation_bundle,
)


class Exp009Stage4RuntimeProbeValidationTests(unittest.TestCase):
    @staticmethod
    def _clean_state(_repository: Path) -> dict[str, object]:
        return {"commit": "a" * 40, "dirty": False}

    def test_complete_profile_seals_a_distinct_runtime_probe_inventory(self) -> None:
        calls: list[tuple[str, ...]] = []

        def runner(
            command: tuple[str, ...], *, repository: Path
        ) -> subprocess.CompletedProcess[str]:
            del repository
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, "stdout\n", "stderr\n")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            raw = run_validation(
                output_dir=output,
                git_state_provider=self._clean_state,
                suite_runner=runner,
            )
            verified = verify_validation_bundle(output)
            manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(raw["status"], "COMPLETE")
        self.assertEqual(verified["status"], "COMPLETE")
        self.assertEqual(
            manifest["schema_version"], "exp009-stage4-runtime-probe-manifest-v1"
        )
        self.assertEqual(
            manifest["execution_mode"], "offline_runtime_probe_composition"
        )
        self.assertEqual(
            manifest["focused_suite_filenames"], list(RUNTIME_PROBE_SUITE_FILENAMES)
        )
        self.assertIn("test_canary_runtime_probe.py", RUNTIME_PROBE_SUITE_FILENAMES)
        self.assertEqual(len(calls), len(RUNTIME_PROBE_SUITE_FILENAMES) + 3)

    def test_profile_refuses_failed_or_tampered_evidence(self) -> None:
        def failed_runner(
            command: tuple[str, ...], *, repository: Path
        ) -> subprocess.CompletedProcess[str]:
            del repository
            failed = RUNTIME_PROBE_SUITE_FILENAMES[0] in command
            return subprocess.CompletedProcess(command, 9 if failed else 0, "stdout", "stderr")

        def passing_runner(
            command: tuple[str, ...], *, repository: Path
        ) -> subprocess.CompletedProcess[str]:
            del repository
            return subprocess.CompletedProcess(command, 0, "stdout", "stderr")

        with tempfile.TemporaryDirectory() as directory:
            failed_output = Path(directory) / "failed"
            with self.assertRaisesRegex(
                Exp009Stage4RuntimeProbeValidationError, "SUITE_FAILURE"
            ):
                run_validation(
                    output_dir=failed_output,
                    git_state_provider=self._clean_state,
                    suite_runner=failed_runner,
                )
            with self.assertRaisesRegex(
                Exp009Stage4RuntimeProbeValidationError, "VALIDATION_INCOMPLETE"
            ):
                verify_validation_bundle(failed_output)

            output = Path(directory) / "complete"
            run_validation(
                output_dir=output,
                git_state_provider=self._clean_state,
                suite_runner=passing_runner,
            )
            (output / "commands" / "pip_check.stdout.txt").write_text(
                "tampered", encoding="utf-8"
            )
            with self.assertRaisesRegex(
                Exp009Stage4RuntimeProbeValidationError, "ARTIFACT_HASH_MISMATCH"
            ):
                verify_validation_bundle(output)

    def test_wrapper_imports_no_database_or_live_authority_runtime(self) -> None:
        path = (
            Path(__file__).parents[1]
            / "experiments"
            / "exp009_stage4_runtime_probe_validate.py"
        )
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        forbidden = {
            "pymilvus",
            "vdbench.milvus",
            "vdbench.milvus_serving",
            "vdbench.canary_activation",
            "vdbench.canary_approval",
            "vdbench.canary_route_authority",
        }
        self.assertFalse(forbidden & imports)


if __name__ == "__main__":
    unittest.main()
