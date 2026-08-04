"""Contract tests for the immutable offline EXP-009 Stage-2 verifier."""

from __future__ import annotations

import ast
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from experiments.exp009_stage2_validate import (
    STAGE2_SUITE_FILENAMES,
    Exp009Stage2ValidationError,
    run_validation,
    verify_validation_bundle,
)


class Exp009Stage2ValidationTests(unittest.TestCase):
    def test_dirty_revision_is_refused_before_any_evidence_path_exists(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "run"
            with self.assertRaisesRegex(Exp009Stage2ValidationError, "WORKTREE_DIRTY"):
                run_validation(
                    output_dir=output_dir,
                    git_state_provider=lambda _repository: {
                        "commit": "a" * 40,
                        "dirty": True,
                    },
                )
            self.assertFalse(output_dir.exists())

    def test_complete_suite_bundle_preserves_raw_output_and_hashes_inputs(self) -> None:
        calls: list[tuple[str, ...]] = []

        def runner(command: tuple[str, ...], *, repository: Path) -> subprocess.CompletedProcess[str]:
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, "suite stdout\n", "suite stderr\n")

        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "run"
            result = run_validation(
                output_dir=output_dir,
                git_state_provider=lambda _repository: {"commit": "b" * 40, "dirty": False},
                suite_runner=runner,
            )
            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            receipt = json.loads((output_dir / "execution_receipt.json").read_text(encoding="utf-8"))
            first = STAGE2_SUITE_FILENAMES[0]
            raw_stdout = (output_dir / "commands" / f"{first}.stdout.txt").read_text(encoding="utf-8")
            raw_stderr = (output_dir / "commands" / f"{first}.stderr.txt").read_text(encoding="utf-8")
            verified = verify_validation_bundle(output_dir)

        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(len(calls), len(STAGE2_SUITE_FILENAMES) + 3)
        self.assertEqual(raw_stdout, "suite stdout\n")
        self.assertEqual(raw_stderr, "suite stderr\n")
        self.assertEqual(manifest["validation_status"], "COMPLETE")
        self.assertEqual(manifest["execution_mode"], "offline")
        self.assertEqual(set(manifest["suite_source_sha256"]), set(STAGE2_SUITE_FILENAMES))
        self.assertIn("requirements.lock", manifest["input_sha256"])
        self.assertTrue(manifest["artifact_sha256"])
        self.assertEqual(receipt["validation_status"], "COMPLETE")
        self.assertEqual(receipt["manifest_sha256"], manifest["self_sha256"])
        self.assertEqual(verified["status"], "COMPLETE")

    def test_failed_suite_writes_incomplete_evidence_before_failing_closed(self) -> None:
        def runner(command: tuple[str, ...], *, repository: Path) -> subprocess.CompletedProcess[str]:
            status = 17 if STAGE2_SUITE_FILENAMES[1] in command else 0
            return subprocess.CompletedProcess(command, status, "stdout", "stderr")

        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "run"
            with self.assertRaisesRegex(Exp009Stage2ValidationError, "SUITE_FAILURE"):
                run_validation(
                    output_dir=output_dir,
                    git_state_provider=lambda _repository: {"commit": "c" * 40, "dirty": False},
                    suite_runner=runner,
                )
            raw_result = json.loads((output_dir / "raw_result.json").read_text(encoding="utf-8"))
            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            failed = raw_result["commands"][STAGE2_SUITE_FILENAMES[1]]
            with self.assertRaisesRegex(Exp009Stage2ValidationError, "VALIDATION_INCOMPLETE"):
                verify_validation_bundle(output_dir)

        self.assertEqual(raw_result["status"], "INCOMPLETE")
        self.assertEqual(manifest["validation_status"], "INCOMPLETE")
        self.assertEqual(failed["returncode"], 17)
        self.assertFalse(failed["passed"])

    def test_public_verifier_rejects_tampered_artifact_content(self) -> None:
        def runner(command: tuple[str, ...], *, repository: Path) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(command, 0, "stdout", "stderr")

        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "run"
            run_validation(
                output_dir=output_dir,
                git_state_provider=lambda _repository: {"commit": "d" * 40, "dirty": False},
                suite_runner=runner,
            )
            (output_dir / "commands" / "pip_check.stdout.txt").write_text(
                "tampered\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(Exp009Stage2ValidationError, "ARTIFACT_HASH_MISMATCH"):
                verify_validation_bundle(output_dir)

    def test_verifier_cannot_import_or_invoke_live_database_or_routing_paths(self) -> None:
        source = (
            Path(__file__).parents[1] / "experiments" / "exp009_stage2_validate.py"
        )
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        called_names = {
            node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, (ast.Attribute, ast.Name))
        }

        self.assertFalse(
            {
                "pymilvus",
                "vdbench.milvus",
                "vdbench.milvus_actuation",
                "vdbench.execute_live",
                "vdbench.canary_route_authority",
            }
            & imports
        )
        self.assertFalse({"resolve_and_claim", "activate", "start_canary"} & called_names)


if __name__ == "__main__":
    unittest.main()
