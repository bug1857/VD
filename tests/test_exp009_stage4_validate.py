"""Contract tests for the sealed offline EXP-009 Stage-4 composition bundle."""

from __future__ import annotations

import ast
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from experiments.exp009_stage4_validate import (
    STAGE4_SUITE_FILENAMES,
    Exp009Stage4ValidationError,
    run_validation,
    verify_validation_bundle,
)


class Exp009Stage4ValidationTests(unittest.TestCase):
    @staticmethod
    def _clean_state(_repository: Path) -> dict[str, object]:
        return {"commit": "a" * 40, "dirty": False}

    def test_dirty_revision_refuses_before_creating_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            with self.assertRaisesRegex(Exp009Stage4ValidationError, "WORKTREE_DIRTY"):
                run_validation(
                    output_dir=output,
                    git_state_provider=lambda _repository: {"commit": "a" * 40, "dirty": True},
                )
            self.assertFalse(output.exists())

    def test_complete_bundle_preserves_raw_output_and_verifies(self) -> None:
        calls: list[tuple[str, ...]] = []

        def runner(command: tuple[str, ...], *, repository: Path) -> subprocess.CompletedProcess[str]:
            del repository
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, "stdout\n", "stderr\n")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            result = run_validation(
                output_dir=output, git_state_provider=self._clean_state, suite_runner=runner
            )
            verified = verify_validation_bundle(output)
            first = STAGE4_SUITE_FILENAMES[0]
            self.assertEqual(
                (output / "commands" / f"{first}.stdout.txt").read_text(encoding="utf-8"),
                "stdout\n",
            )
            self.assertEqual(
                (output / "commands" / f"{first}.stderr.txt").read_text(encoding="utf-8"),
                "stderr\n",
            )

        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(verified["status"], "COMPLETE")
        self.assertEqual(len(calls), len(STAGE4_SUITE_FILENAMES) + 3)

    def test_failed_suite_is_sealed_incomplete_and_cannot_verify_complete(self) -> None:
        def runner(command: tuple[str, ...], *, repository: Path) -> subprocess.CompletedProcess[str]:
            del repository
            failed = STAGE4_SUITE_FILENAMES[1] in command
            return subprocess.CompletedProcess(command, 9 if failed else 0, "stdout", "stderr")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            with self.assertRaisesRegex(Exp009Stage4ValidationError, "SUITE_FAILURE"):
                run_validation(
                    output_dir=output, git_state_provider=self._clean_state, suite_runner=runner
                )
            raw = json.loads((output / "raw_result.json").read_text(encoding="utf-8"))
            with self.assertRaisesRegex(Exp009Stage4ValidationError, "VALIDATION_INCOMPLETE"):
                verify_validation_bundle(output)

        self.assertEqual(raw["status"], "INCOMPLETE")
        self.assertIn(STAGE4_SUITE_FILENAMES[1], raw["failed_commands"])

    def test_verifier_rejects_tampered_raw_command_output(self) -> None:
        def runner(command: tuple[str, ...], *, repository: Path) -> subprocess.CompletedProcess[str]:
            del repository
            return subprocess.CompletedProcess(command, 0, "stdout", "stderr")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            run_validation(output_dir=output, git_state_provider=self._clean_state, suite_runner=runner)
            (output / "commands" / "pip_check.stdout.txt").write_text("tampered\n", encoding="utf-8")
            with self.assertRaisesRegex(Exp009Stage4ValidationError, "ARTIFACT_HASH_MISMATCH"):
                verify_validation_bundle(output)

    def test_verifier_rejects_tampered_execution_receipt(self) -> None:
        def runner(command: tuple[str, ...], *, repository: Path) -> subprocess.CompletedProcess[str]:
            del repository
            return subprocess.CompletedProcess(command, 0, "stdout", "stderr")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            run_validation(output_dir=output, git_state_provider=self._clean_state, suite_runner=runner)
            receipt_path = output / "execution_receipt.json"
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt["command_count"] = 0
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(Exp009Stage4ValidationError, "SELF_HASH_MISMATCH"):
                verify_validation_bundle(output)

    def test_validator_has_no_live_client_grant_or_route_authority_import(self) -> None:
        path = Path(__file__).parents[1] / "experiments" / "exp009_stage4_validate.py"
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
            "vdbench.canary_activation",
            "vdbench.canary_approval",
            "vdbench.canary_route_authority",
        }
        self.assertFalse(forbidden & imports)


if __name__ == "__main__":
    unittest.main()
