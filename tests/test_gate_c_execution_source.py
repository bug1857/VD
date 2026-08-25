from __future__ import annotations

import os
import subprocess
import tempfile
import types
import unittest
from pathlib import Path

from vdbench.gate_c_execution_source import (
    GateCExecutionSourceError,
    verify_gate_c_execution_source,
)


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def _repository(root: Path) -> tuple[str, types.ModuleType]:
    package = root / "src" / "vdbench"
    package.mkdir(parents=True)
    module_path = package / "runtime.py"
    module_path.write_text("VALUE = 1\n", encoding="utf-8")
    _git(root, "init", "-q")
    _git(root, "config", "user.name", "Gate C Test")
    _git(root, "config", "user.email", "gate-c@example.invalid")
    _git(root, "add", "--", "src/vdbench/runtime.py")
    _git(root, "commit", "-q", "-m", "runtime")
    module = types.ModuleType("vdbench.runtime")
    module.__file__ = str(module_path)
    return _git(root, "rev-parse", "HEAD"), module


def _subprocess_verification(root: Path, revision: str) -> subprocess.CompletedProcess[str]:
    project_source = Path(__file__).resolve().parents[1] / "src"
    script = f"""
import types
from vdbench.gate_c_execution_source import (
    GateCExecutionSourceError,
    verify_gate_c_execution_source,
)
module = types.ModuleType("vdbench.runtime")
module.__file__ = {str(root / "src/vdbench/runtime.py")!r}
try:
    verify_gate_c_execution_source(
        {str(root)!r},
        expected_revision={revision!r},
        imported_modules=(module,),
    )
except GateCExecutionSourceError as exc:
    print(exc.code)
else:
    print("ACCEPTED")
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(project_source)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        (os.sys.executable, "-c", script),
        cwd=root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


class GateCExecutionSourceTests(unittest.TestCase):
    def test_exact_committed_runtime_and_import_origin_verify(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            revision, module = _repository(root)
            verified = verify_gate_c_execution_source(
                root,
                expected_revision=revision,
                imported_modules=(module,),
            )
            self.assertEqual(verified.repository_root, root.resolve())
            self.assertEqual(verified.execution_source_revision, revision)

    def test_tracked_runtime_byte_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _revision, module = _repository(root)
            (root / "src/vdbench/runtime.py").write_text(
                "VALUE = 2\n", encoding="utf-8"
            )
            with self.assertRaises(GateCExecutionSourceError) as raised:
                verify_gate_c_execution_source(root, imported_modules=(module,))
            self.assertEqual(raised.exception.code, "GATE_C_EXECUTION_SOURCE_DRIFT")

    def test_untracked_executable_shadow_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _revision, module = _repository(root)
            (root / "src/vdbench/policy.py").write_text(
                "raise RuntimeError\n", encoding="utf-8"
            )
            with self.assertRaises(GateCExecutionSourceError) as raised:
                verify_gate_c_execution_source(root, imported_modules=(module,))
            self.assertEqual(raised.exception.code, "GATE_C_EXECUTION_SOURCE_DRIFT")

    def test_source_root_package_shadow_or_startup_hook_fails_closed(self) -> None:
        for relative in (
            "src/vdbench.py",
            "src/sitecustomize.py",
            "src/usercustomize.py",
        ):
            with self.subTest(relative=relative), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                _revision, module = _repository(root)
                (root / relative).write_text("VALUE = 2\n", encoding="utf-8")
                with self.assertRaises(GateCExecutionSourceError) as raised:
                    verify_gate_c_execution_source(root, imported_modules=(module,))
                self.assertEqual(
                    raised.exception.code, "GATE_C_EXECUTION_SOURCE_DRIFT"
                )

    def test_untracked_repository_root_startup_hooks_fail_in_fresh_process(self) -> None:
        for name in ("sitecustomize.py", "usercustomize.py"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                revision, _module = _repository(root)
                (root / name).write_text("PROBE = 'harmless'\n", encoding="utf-8")
                completed = _subprocess_verification(root, revision)
                self.assertEqual(
                    completed.stdout.strip(), "GATE_C_EXECUTION_SOURCE_DRIFT"
                )

    def test_exact_committed_repository_root_startup_hook_is_bound(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _revision, module = _repository(root)
            (root / "sitecustomize.py").write_text(
                "PROBE = 'committed'\n", encoding="utf-8"
            )
            _git(root, "add", "--", "sitecustomize.py")
            _git(root, "commit", "-q", "-m", "bound startup hook")
            revision = _git(root, "rev-parse", "HEAD")
            verified = verify_gate_c_execution_source(
                root,
                expected_revision=revision,
                imported_modules=(module,),
            )
            self.assertEqual(verified.execution_source_revision, revision)

    def test_unrelated_document_and_artifact_changes_are_out_of_scope(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            revision, module = _repository(root)
            (root / "ROADMAP.md").write_text("local notes\n", encoding="utf-8")
            (root / "README.local.txt").write_text("report\n", encoding="utf-8")
            artifact = root / "artifacts" / "run"
            artifact.mkdir(parents=True)
            (artifact / "evidence.json").write_text("{}\n", encoding="utf-8")
            scratch = root / "scratch" / "notes"
            scratch.mkdir(parents=True)
            (scratch / "observation.txt").write_text("safe\n", encoding="utf-8")
            verified = verify_gate_c_execution_source(
                root,
                expected_revision=revision,
                imported_modules=(module,),
            )
            self.assertEqual(verified.execution_source_revision, revision)

    def test_wrong_expected_revision_fails_independently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _revision, module = _repository(root)
            with self.assertRaises(GateCExecutionSourceError) as raised:
                verify_gate_c_execution_source(
                    root,
                    expected_revision="f" * 40,
                    imported_modules=(module,),
                )
            self.assertEqual(
                raised.exception.code,
                "GATE_C_EXECUTION_SOURCE_REVISION_MISMATCH",
            )

    def test_imported_vdbench_module_outside_repository_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _revision, _module = _repository(root)
            outside = root / "outside.py"
            outside.write_text("VALUE = 1\n", encoding="utf-8")
            module = types.ModuleType("vdbench.shadowed")
            module.__file__ = str(outside)
            with self.assertRaises(GateCExecutionSourceError) as raised:
                verify_gate_c_execution_source(root, imported_modules=(module,))
            self.assertEqual(
                raised.exception.code,
                "GATE_C_EXECUTION_IMPORT_IDENTITY_INVALID",
            )


if __name__ == "__main__":
    unittest.main()
