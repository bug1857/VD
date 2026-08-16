"""Contract tests for the immutable offline EXP-009 Stage-3 verifier."""

from __future__ import annotations

import ast
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from experiments.exp009_stage3_validate import (
    STAGE3_SUITE_FILENAMES,
    Exp009Stage3ValidationError,
    run_validation,
    verify_validation_bundle,
)


class Exp009Stage3ValidationTests(unittest.TestCase):
    @staticmethod
    def _write_canonical_json(path: Path, value: dict[str, object]) -> None:
        path.write_text(
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _self_hash(value: dict[str, object]) -> str:
        projection = dict(value)
        projection.pop("self_sha256", None)
        return hashlib.sha256(
            (
                json.dumps(projection, sort_keys=True, separators=(",", ":"))
                + "\n"
            ).encode("utf-8")
        ).hexdigest()

    def test_dirty_revision_is_refused_before_any_evidence_path_exists(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "run"

            with self.assertRaisesRegex(
                Exp009Stage3ValidationError, "WORKTREE_DIRTY"
            ):
                run_validation(
                    output_dir=output_dir,
                    git_state_provider=lambda _repository: {
                        "commit": "a" * 40,
                        "dirty": True,
                    },
                )

            self.assertFalse(output_dir.exists())

    def test_complete_bundle_hashes_inputs_and_preserves_raw_command_output(self) -> None:
        calls: list[tuple[str, ...]] = []

        def runner(
            command: tuple[str, ...], *, repository: Path
        ) -> subprocess.CompletedProcess[str]:
            calls.append(command)
            return subprocess.CompletedProcess(
                command, 0, "suite stdout\n", "suite stderr\n"
            )

        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "run"
            result = run_validation(
                output_dir=output_dir,
                git_state_provider=lambda _repository: {
                    "commit": "b" * 40,
                    "dirty": False,
                },
                suite_runner=runner,
            )
            manifest = json.loads(
                (output_dir / "manifest.json").read_text(encoding="utf-8")
            )
            receipt = json.loads(
                (output_dir / "execution_receipt.json").read_text(
                    encoding="utf-8"
                )
            )
            first_suite = STAGE3_SUITE_FILENAMES[0]
            raw_stdout = (
                output_dir / "commands" / f"{first_suite}.stdout.txt"
            ).read_text(encoding="utf-8")
            raw_stderr = (
                output_dir / "commands" / f"{first_suite}.stderr.txt"
            ).read_text(encoding="utf-8")
            verified = verify_validation_bundle(output_dir)

        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(len(calls), len(STAGE3_SUITE_FILENAMES) + 3)
        self.assertEqual(raw_stdout, "suite stdout\n")
        self.assertEqual(raw_stderr, "suite stderr\n")
        self.assertEqual(manifest["validation_status"], "COMPLETE")
        self.assertEqual(manifest["execution_mode"], "offline")
        self.assertEqual(
            set(manifest["suite_source_sha256"]), set(STAGE3_SUITE_FILENAMES)
        )
        self.assertIn("requirements.lock", manifest["input_sha256"])
        self.assertIn("src/vdbench/canary_rollback.py", manifest["input_sha256"])
        self.assertTrue(manifest["artifact_sha256"])
        self.assertEqual(receipt["validation_status"], "COMPLETE")
        self.assertEqual(receipt["manifest_sha256"], manifest["self_sha256"])
        self.assertEqual(verified["status"], "COMPLETE")

    def test_failed_suite_writes_incomplete_evidence_before_failing_closed(self) -> None:
        def runner(
            command: tuple[str, ...], *, repository: Path
        ) -> subprocess.CompletedProcess[str]:
            status = 17 if STAGE3_SUITE_FILENAMES[1] in command else 0
            return subprocess.CompletedProcess(command, status, "stdout", "stderr")

        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "run"

            with self.assertRaisesRegex(
                Exp009Stage3ValidationError, "SUITE_FAILURE"
            ):
                run_validation(
                    output_dir=output_dir,
                    git_state_provider=lambda _repository: {
                        "commit": "c" * 40,
                        "dirty": False,
                    },
                    suite_runner=runner,
                )

            raw_result = json.loads(
                (output_dir / "raw_result.json").read_text(encoding="utf-8")
            )
            manifest = json.loads(
                (output_dir / "manifest.json").read_text(encoding="utf-8")
            )
            failed = raw_result["commands"][STAGE3_SUITE_FILENAMES[1]]

            with self.assertRaisesRegex(
                Exp009Stage3ValidationError, "VALIDATION_INCOMPLETE"
            ):
                verify_validation_bundle(output_dir)

        self.assertEqual(raw_result["status"], "INCOMPLETE")
        self.assertEqual(manifest["validation_status"], "INCOMPLETE")
        self.assertEqual(failed["returncode"], 17)
        self.assertFalse(failed["passed"])

    def test_public_verifier_rejects_tampered_artifact_content(self) -> None:
        def runner(
            command: tuple[str, ...], *, repository: Path
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(command, 0, "stdout", "stderr")

        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "run"
            run_validation(
                output_dir=output_dir,
                git_state_provider=lambda _repository: {
                    "commit": "d" * 40,
                    "dirty": False,
                },
                suite_runner=runner,
            )
            (output_dir / "commands" / "pip_check.stdout.txt").write_text(
                "tampered\n", encoding="utf-8"
            )

            with self.assertRaisesRegex(
                Exp009Stage3ValidationError, "ARTIFACT_HASH_MISMATCH"
            ):
                verify_validation_bundle(output_dir)

    def test_public_verifier_rejects_rehashed_command_digest_substitution(self) -> None:
        def runner(
            command: tuple[str, ...], *, repository: Path
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(command, 0, "stdout", "stderr")

        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "run"
            run_validation(
                output_dir=output_dir,
                git_state_provider=lambda _repository: {
                    "commit": "e" * 40,
                    "dirty": False,
                },
                suite_runner=runner,
            )
            raw_path = output_dir / "raw_result.json"
            manifest_path = output_dir / "manifest.json"
            receipt_path = output_dir / "execution_receipt.json"
            raw_result = json.loads(raw_path.read_text(encoding="utf-8"))
            raw_result["commands"]["pip_check"]["stdout_sha256"] = "0" * 64
            raw_result["self_sha256"] = self._self_hash(raw_result)
            self._write_canonical_json(raw_path, raw_result)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["artifact_sha256"]["raw_result.json"] = hashlib.sha256(
                raw_path.read_bytes()
            ).hexdigest()
            manifest["self_sha256"] = self._self_hash(manifest)
            self._write_canonical_json(manifest_path, manifest)
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt["manifest_sha256"] = manifest["self_sha256"]
            receipt["raw_result_sha256"] = raw_result["self_sha256"]
            self._write_canonical_json(receipt_path, receipt)

            with self.assertRaisesRegex(
                Exp009Stage3ValidationError, "COMMAND_INVENTORY_INVALID"
            ):
                verify_validation_bundle(output_dir)

    def test_public_verifier_rejects_a_symlinked_bundle_root(self) -> None:
        def runner(
            command: tuple[str, ...], *, repository: Path
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(command, 0, "stdout", "stderr")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "run"
            run_validation(
                output_dir=output_dir,
                git_state_provider=lambda _repository: {
                    "commit": "f" * 40,
                    "dirty": False,
                },
                suite_runner=runner,
            )
            alias = root / "alias"
            alias.symlink_to(output_dir, target_is_directory=True)

            with self.assertRaisesRegex(
                Exp009Stage3ValidationError, "SYMLINK_ARTIFACT_REFUSED"
            ):
                verify_validation_bundle(alias)

    def test_verifier_cannot_import_or_invoke_live_database_or_route_paths(self) -> None:
        source = (
            Path(__file__).parents[1] / "experiments" / "exp009_stage3_validate.py"
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
                "vdbench.canary_rollback",
            }
            & imports
        )
        self.assertFalse(
            {"resolve_and_claim", "activate", "rollback", "start_canary"}
            & called_names
        )


if __name__ == "__main__":
    unittest.main()
