"""Immutable, offline verification bundle for EXP-009 Stage 2.

The verifier exercises the committed approval, partition, routing-authority,
durable lifecycle, activation, and expiry-failback tests.  It does not import a
Milvus client or claim an occurrence; evidence is accepted only from a clean,
already-committed repository revision.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
from typing import Any

from vdbench.artifacts import git_state, sha256_file


__all__ = [
    "STAGE2_SUITE_FILENAMES",
    "Exp009Stage2ValidationError",
    "run_validation",
]


STAGE2_SUITE_FILENAMES = (
    "test_canary_approval.py",
    "test_canary_workload.py",
    "test_canary_routing.py",
    "test_canary_calibration.py",
    "test_canary_grant_store.py",
    "test_canary_route_state.py",
    "test_canary_route_authority.py",
    "test_canary_lifecycle_audit.py",
    "test_canary_activation.py",
    "test_canary_expiry_reconciliation.py",
)
_SOURCE_FILENAMES = (
    "src/vdbench/canary_approval.py",
    "src/vdbench/canary_workload.py",
    "src/vdbench/canary_routing.py",
    "src/vdbench/canary_calibration.py",
    "src/vdbench/canary_grant_store.py",
    "src/vdbench/canary_route_state.py",
    "src/vdbench/canary_route_authority.py",
    "src/vdbench/canary_lifecycle_audit.py",
    "src/vdbench/canary_activation.py",
    "src/vdbench/canary_expiry_reconciliation.py",
    "experiments/exp009_stage2_validate.py",
)
_FULL_SUITE_NAME = "full_repository_suite"
_PIP_CHECK_NAME = "pip_check"


class Exp009Stage2ValidationError(RuntimeError):
    """Raised after an EXP-009 Stage-2 evidence run fails closed."""


SuiteRunner = Callable[..., subprocess.CompletedProcess[str]]
GitStateProvider = Callable[[Path], Mapping[str, object]]


def _canonical_json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _content_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _write_bytes(path: Path, payload: bytes) -> None:
    """Atomically publish an immutable evidence file with directory fsync."""

    target = Path(path)
    if target.exists() or target.is_symlink():
        raise FileExistsError(f"refusing to overwrite immutable evidence: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, target)
        directory_descriptor = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _write_json(path: Path, value: object) -> None:
    _write_bytes(path, _canonical_json_bytes(value))


def _artifact_inventory(root: Path) -> dict[str, str]:
    excluded = {"manifest.json", "execution_receipt.json"}
    return {
        str(path.relative_to(root)): sha256_file(path)
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.is_symlink() and path.name not in excluded
    }


def _run_command(command: tuple[str, ...], *, repository: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONPATH"] = "src"
    return subprocess.run(
        command,
        cwd=repository,
        env=environment,
        check=False,
        text=True,
        capture_output=True,
    )


def _record_command(
    *,
    output_dir: Path,
    name: str,
    command: tuple[str, ...],
    repository: Path,
    runner: SuiteRunner,
) -> dict[str, object]:
    """Run one test command and retain byte-for-byte decoded process output."""

    try:
        completed = runner(command, repository=repository)
    except OSError as error:
        completed = subprocess.CompletedProcess(command, 127, "", f"{type(error).__name__}: {error}\n")
    stdout_path = output_dir / "commands" / f"{name}.stdout.txt"
    stderr_path = output_dir / "commands" / f"{name}.stderr.txt"
    _write_bytes(stdout_path, (completed.stdout or "").encode("utf-8"))
    _write_bytes(stderr_path, (completed.stderr or "").encode("utf-8"))
    result = {
        "command": list(command),
        "returncode": completed.returncode,
        "passed": completed.returncode == 0,
        "stdout_file": str(stdout_path.relative_to(output_dir)),
        "stdout_sha256": sha256_file(stdout_path),
        "stderr_file": str(stderr_path.relative_to(output_dir)),
        "stderr_sha256": sha256_file(stderr_path),
    }
    _write_json(output_dir / "commands" / f"{name}.result.json", result)
    return result


def _source_hashes(repository: Path) -> tuple[dict[str, str], dict[str, str]]:
    source = {name: sha256_file(repository / name) for name in _SOURCE_FILENAMES}
    suites = {
        filename: sha256_file(repository / "tests" / filename)
        for filename in STAGE2_SUITE_FILENAMES
    }
    return source, suites


def run_validation(
    *,
    output_dir: Path,
    repository: Path | None = None,
    git_state_provider: GitStateProvider = git_state,
    suite_runner: SuiteRunner = _run_command,
) -> dict[str, object]:
    """Write one complete, immutable, offline EXP-009 Stage-2 evidence bundle.

    A dirty revision is rejected before creating the output directory, so no
    evidence can accidentally claim a commit that differs from executed code.
    Every focused contract suite and the full suite run independently.  If any
    command fails, the runner writes an ``INCOMPLETE`` receipt then raises.
    """

    resolved_repository = (repository or Path(__file__).parents[1]).resolve()
    resolved_output = Path(output_dir).resolve()
    if resolved_output.exists():
        raise Exp009Stage2ValidationError("OUTPUT_PATH_EXISTS")

    state = dict(git_state_provider(resolved_repository))
    if state.get("dirty") is not False:
        raise Exp009Stage2ValidationError("WORKTREE_DIRTY")
    commit = state.get("commit")
    if not isinstance(commit, str) or len(commit) != 40:
        raise Exp009Stage2ValidationError("GIT_COMMIT_INVALID")

    source_sha256, suite_source_sha256 = _source_hashes(resolved_repository)
    lock_path = resolved_repository / "requirements.lock"
    if not lock_path.is_file():
        raise Exp009Stage2ValidationError("REQUIREMENTS_LOCK_MISSING")
    input_sha256 = {"requirements.lock": sha256_file(lock_path), **source_sha256}

    resolved_output.mkdir(parents=True, mode=0o700)
    resolved_output.chmod(0o700)
    commands: dict[str, dict[str, object]] = {}
    commands["git_diff_check"] = _record_command(
        output_dir=resolved_output,
        name="git_diff_check",
        command=("git", "diff", "--check"),
        repository=resolved_repository,
        runner=suite_runner,
    )
    for filename in STAGE2_SUITE_FILENAMES:
        commands[filename] = _record_command(
            output_dir=resolved_output,
            name=filename,
            command=(
                sys.executable,
                "-m",
                "unittest",
                "discover",
                "tests",
                "-p",
                filename,
                "-v",
            ),
            repository=resolved_repository,
            runner=suite_runner,
        )
    commands[_FULL_SUITE_NAME] = _record_command(
        output_dir=resolved_output,
        name=_FULL_SUITE_NAME,
        command=(sys.executable, "-m", "unittest", "discover", "tests", "-q", "--failfast"),
        repository=resolved_repository,
        runner=suite_runner,
    )
    commands[_PIP_CHECK_NAME] = _record_command(
        output_dir=resolved_output,
        name=_PIP_CHECK_NAME,
        command=(sys.executable, "-m", "pip", "check"),
        repository=resolved_repository,
        runner=suite_runner,
    )

    failed_commands = tuple(name for name, result in commands.items() if not result["passed"])
    status = "COMPLETE" if not failed_commands else "INCOMPLETE"
    raw_result: dict[str, object] = {
        "schema_version": "exp009-stage2-raw-result-v1",
        "experiment_id": "EXP-009",
        "stage": 2,
        "execution_mode": "offline",
        "status": status,
        "git_commit": commit,
        "failed_commands": list(failed_commands),
        "commands": commands,
        "live_database_or_routing_activity": False,
    }
    raw_result["self_sha256"] = _content_sha256(raw_result)
    _write_json(resolved_output / "raw_result.json", raw_result)

    manifest: dict[str, object] = {
        "schema_version": "exp009-stage2-manifest-v1",
        "experiment_id": "EXP-009",
        "stage": 2,
        "validation_status": status,
        "execution_mode": "offline",
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "git": {"commit": commit, "dirty": False},
        "python": sys.version,
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "input_sha256": input_sha256,
        "suite_source_sha256": suite_source_sha256,
        "focused_suite_filenames": list(STAGE2_SUITE_FILENAMES),
        "required_full_suite": _FULL_SUITE_NAME,
        "artifact_sha256": _artifact_inventory(resolved_output),
        "offline_safety_assertion": {
            "constructs_milvus_client": False,
            "issues_search": False,
            "claims_or_enables_candidate_route": False,
        },
    }
    manifest["self_sha256"] = _content_sha256(manifest)
    _write_json(resolved_output / "manifest.json", manifest)
    receipt = {
        "schema_version": "exp009-stage2-execution-receipt-v1",
        "validation_status": status,
        "git_commit": commit,
        "manifest_sha256": manifest["self_sha256"],
        "raw_result_sha256": raw_result["self_sha256"],
        "command_count": len(commands),
        "failed_commands": list(failed_commands),
    }
    _write_json(resolved_output / "execution_receipt.json", receipt)

    if status != "COMPLETE":
        raise Exp009Stage2ValidationError(
            "SUITE_FAILURE:" + ",".join(failed_commands)
        )
    return raw_result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_validation(output_dir=args.output_dir), sort_keys=True))


if __name__ == "__main__":
    main()
