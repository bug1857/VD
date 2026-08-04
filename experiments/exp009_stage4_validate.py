"""Sealed fake-only validation bundle for EXP-009's Stage-4 composition seam.

This script records committed test evidence only. It imports no live database,
approval/grant, activation, or route-authority runtime and issues no query.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

from vdbench.artifacts import git_state, sha256_file


__all__ = [
    "LIVE_ROOT_VALIDATION_SPEC",
    "RUNTIME_PROBE_VALIDATION_SPEC",
    "STAGE4_SUITE_FILENAMES",
    "Stage4ValidationSpec",
    "Exp009Stage4ValidationError",
    "run_validation",
    "verify_validation_bundle",
]


STAGE4_SUITE_FILENAMES = (
    "test_canary_admission.py",
    "test_canary_schedule.py",
    "test_canary_execution_ledger.py",
    "test_canary_schedule_evaluation.py",
    "test_canary_serial_runner.py",
    "test_exp009_stage4_validate.py",
)
_SOURCE_FILENAMES = (
    "src/vdbench/canary_admission.py",
    "src/vdbench/canary_query_source.py",
    "src/vdbench/canary_schedule.py",
    "src/vdbench/canary_execution_ledger.py",
    "src/vdbench/canary_schedule_evaluation.py",
    "src/vdbench/canary_serial_runner.py",
    "src/vdbench/canary_workload.py",
    "experiments/exp009_stage4_validate.py",
)
_FULL_SUITE = "full_repository_suite"
_PIP_CHECK = "pip_check"
_SHA256 = frozenset("0123456789abcdef")
_COMMAND_FIELDS = frozenset(
    {
        "command", "returncode", "passed", "stdout_file", "stdout_sha256", "stderr_file", "stderr_sha256"
    }
)


@dataclass(frozen=True, slots=True)
class Stage4ValidationSpec:
    """Immutable inventory/schema profile for one independently sealed run."""

    focused_suite_filenames: tuple[str, ...]
    source_filenames: tuple[str, ...]
    manifest_schema_version: str
    raw_schema_version: str
    receipt_schema_version: str
    execution_mode: str
    offline_safety_assertion: tuple[tuple[str, bool], ...]

    def __post_init__(self) -> None:
        for values in (self.focused_suite_filenames, self.source_filenames):
            if not values or len(values) != len(set(values)) or not all(
                isinstance(value, str) and value for value in values
            ):
                raise ValueError("validation inventory must contain unique non-empty names")
        if not all(
            isinstance(value, str) and value
            for value in (
                self.manifest_schema_version,
                self.raw_schema_version,
                self.receipt_schema_version,
                self.execution_mode,
            )
        ):
            raise ValueError("validation profile strings must be non-empty")
        safety = dict(self.offline_safety_assertion)
        if len(safety) != len(self.offline_safety_assertion) or not safety or not all(
            isinstance(key, str) and key and value is False
            for key, value in self.offline_safety_assertion
        ):
            raise ValueError("offline safety assertion must be unique false facts")

    @property
    def safety_assertion(self) -> dict[str, bool]:
        """Return a fresh stable document for manifest/verification comparison."""

        return dict(self.offline_safety_assertion)


_OFFLINE_COMPOSITION_SPEC = Stage4ValidationSpec(
    focused_suite_filenames=STAGE4_SUITE_FILENAMES,
    source_filenames=_SOURCE_FILENAMES,
    manifest_schema_version="exp009-stage4-manifest-v1",
    raw_schema_version="exp009-stage4-raw-result-v1",
    receipt_schema_version="exp009-stage4-execution-receipt-v1",
    execution_mode="offline_composition",
    offline_safety_assertion=(
        ("constructs_milvus_client", False),
        ("issues_search", False),
        ("accepts_or_verifies_grant", False),
        ("claims_or_enables_candidate_route", False),
    ),
)

LIVE_ROOT_VALIDATION_SPEC = Stage4ValidationSpec(
    focused_suite_filenames=(
        "test_canary_admission.py",
        "test_canary_schedule.py",
        "test_canary_execution_ledger.py",
        "test_canary_schedule_evaluation.py",
        "test_canary_serial_runner.py",
        "test_canary_live_runner.py",
        "test_canary_route_authority.py",
        "test_canary_activation.py",
        "test_canary_rollback.py",
        "test_exp009_stage4_validate.py",
        "test_exp009_stage4_live_root_validate.py",
    ),
    source_filenames=(
        "src/vdbench/canary_admission.py",
        "src/vdbench/canary_activation.py",
        "src/vdbench/canary_execution_ledger.py",
        "src/vdbench/canary_live_runner.py",
        "src/vdbench/canary_query_source.py",
        "src/vdbench/canary_rollback.py",
        "src/vdbench/canary_route_authority.py",
        "src/vdbench/canary_route_state.py",
        "src/vdbench/canary_schedule.py",
        "src/vdbench/canary_schedule_evaluation.py",
        "src/vdbench/canary_serial_runner.py",
        "src/vdbench/canary_workload.py",
        "experiments/exp009_stage4_validate.py",
        "experiments/exp009_stage4_live_root_validate.py",
    ),
    manifest_schema_version="exp009-stage4-live-root-manifest-v1",
    raw_schema_version="exp009-stage4-live-root-raw-result-v1",
    receipt_schema_version="exp009-stage4-live-root-execution-receipt-v1",
    execution_mode="offline_live_root_composition",
    offline_safety_assertion=(
        ("constructs_milvus_client", False),
        ("issues_live_search", False),
        ("accepts_or_verifies_real_grant", False),
        ("claims_or_enables_real_candidate_route", False),
        ("mutates_milvus_configuration", False),
    ),
)


RUNTIME_PROBE_VALIDATION_SPEC = Stage4ValidationSpec(
    focused_suite_filenames=(
        "test_canary_runtime_probe.py",
        "test_canary_admission.py",
        "test_canary_live_runner.py",
        "test_exp009_stage4_validate.py",
        "test_exp009_stage4_runtime_probe_validate.py",
    ),
    source_filenames=(
        "src/vdbench/canary_admission.py",
        "src/vdbench/canary_live_runner.py",
        "src/vdbench/canary_route_state.py",
        "src/vdbench/canary_runtime_probe.py",
        "src/vdbench/canary_runtime_types.py",
        "src/vdbench/config.py",
        "src/vdbench/milvus_serving.py",
        "src/vdbench/shadow_event_types.py",
        "experiments/exp009_stage4_validate.py",
        "experiments/exp009_stage4_runtime_probe_validate.py",
    ),
    manifest_schema_version="exp009-stage4-runtime-probe-manifest-v1",
    raw_schema_version="exp009-stage4-runtime-probe-raw-result-v1",
    receipt_schema_version="exp009-stage4-runtime-probe-execution-receipt-v1",
    execution_mode="offline_runtime_probe_composition",
    offline_safety_assertion=(
        ("constructs_milvus_client", False),
        ("issues_live_serving_preflight", False),
        ("issues_live_search", False),
        ("accepts_or_verifies_real_grant", False),
        ("claims_or_enables_real_candidate_route", False),
        ("mutates_milvus_configuration", False),
    ),
)


class Exp009Stage4ValidationError(RuntimeError):
    """A fail-closed offline Stage-4 evidence condition."""


SuiteRunner = Callable[..., subprocess.CompletedProcess[str]]
GitStateProvider = Callable[[Path], Mapping[str, object]]


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _is_hash(value: object, *, length: int = 64) -> bool:
    return isinstance(value, str) and len(value) == length and all(char in _SHA256 for char in value)


def _write(path: Path, payload: bytes) -> None:
    if path.exists() or path.is_symlink():
        raise FileExistsError("OUTPUT_PATH_EXISTS")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _write_json(path: Path, value: object) -> None:
    _write(path, _canonical(value))


def _inventory(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): sha256_file(path)
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.is_symlink() and path.name not in {"manifest.json", "execution_receipt.json"}
    }


def _run(command: tuple[str, ...], *, repository: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONPATH"] = "src"
    environment["PYTHONWARNINGS"] = "error::ResourceWarning"
    return subprocess.run(command, cwd=repository, env=environment, text=True, capture_output=True, check=False)


def _record(
    *, output: Path, name: str, command: tuple[str, ...], repository: Path, runner: SuiteRunner
) -> dict[str, object]:
    try:
        completed = runner(command, repository=repository)
    except OSError as error:
        completed = subprocess.CompletedProcess(command, 127, "", f"{type(error).__name__}: {error}\n")
    stdout = output / "commands" / f"{name}.stdout.txt"
    stderr = output / "commands" / f"{name}.stderr.txt"
    _write(stdout, (completed.stdout or "").encode("utf-8"))
    _write(stderr, (completed.stderr or "").encode("utf-8"))
    result = {
        "command": list(command), "returncode": completed.returncode, "passed": completed.returncode == 0,
        "stdout_file": str(stdout.relative_to(output)), "stdout_sha256": sha256_file(stdout),
        "stderr_file": str(stderr.relative_to(output)), "stderr_sha256": sha256_file(stderr),
    }
    _write_json(output / "commands" / f"{name}.result.json", result)
    return result


def _source_hashes(
    repository: Path, *, spec: Stage4ValidationSpec
) -> tuple[dict[str, str], dict[str, str]]:
    return (
        {name: sha256_file(repository / name) for name in spec.source_filenames},
        {
            name: sha256_file(repository / "tests" / name)
            for name in spec.focused_suite_filenames
        },
    )


def _load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise Exp009Stage4ValidationError("BUNDLE_JSON_INVALID") from error
    if not isinstance(value, dict):
        raise Exp009Stage4ValidationError("BUNDLE_DOCUMENT_INVALID")
    return value


def _valid_command(value: object, *, root: Path) -> bool:
    if not isinstance(value, dict) or frozenset(value) != _COMMAND_FIELDS:
        return False
    if not isinstance(value["command"], list) or not all(isinstance(item, str) for item in value["command"]):
        return False
    if type(value["returncode"]) is not int or type(value["passed"]) is not bool:
        return False
    if value["passed"] is not (value["returncode"] == 0):
        return False
    for file_field, hash_field in (("stdout_file", "stdout_sha256"), ("stderr_file", "stderr_sha256")):
        relative = value[file_field]
        if not isinstance(relative, str) or not _is_hash(value[hash_field]):
            return False
        relative_path = Path(relative)
        if (
            relative_path.is_absolute()
            or not relative_path.parts
            or relative_path.parts[0] != "commands"
            or any(part in {"", ".", ".."} for part in relative_path.parts)
        ):
            return False
        path = root / relative_path
        # Byte-level integrity is checked against the sealed manifest inventory
        # below. Keeping this function structural makes a modified raw artifact
        # deterministically report ARTIFACT_HASH_MISMATCH rather than a generic
        # command-schema failure.
        if not path.is_file() or path.is_symlink():
            return False
    return True


def verify_validation_bundle(
    output_dir: Path,
    *,
    require_complete: bool = True,
    spec: Stage4ValidationSpec = _OFFLINE_COMPOSITION_SPEC,
) -> dict[str, object]:
    """Independently verify one closed Stage-4 offline evidence bundle."""

    declared = Path(output_dir)
    if declared.is_symlink():
        raise Exp009Stage4ValidationError("SYMLINK_ARTIFACT_REFUSED")
    root = declared.absolute()
    paths = {name: root / name for name in ("manifest.json", "raw_result.json", "execution_receipt.json")}
    if not root.is_dir() or any(not path.is_file() for path in paths.values()):
        raise Exp009Stage4ValidationError("BUNDLE_STRUCTURE_INVALID")
    if any(path.is_symlink() for path in root.rglob("*")):
        raise Exp009Stage4ValidationError("SYMLINK_ARTIFACT_REFUSED")
    manifest, raw, receipt = (_load(paths[name]) for name in ("manifest.json", "raw_result.json", "execution_receipt.json"))
    manifest_projection, raw_projection, receipt_projection = dict(manifest), dict(raw), dict(receipt)
    manifest_hash, raw_hash = manifest_projection.pop("self_sha256", None), raw_projection.pop("self_sha256", None)
    receipt_hash = receipt_projection.pop("self_sha256", None)
    if (
        not _is_hash(manifest_hash)
        or not _is_hash(raw_hash)
        or not _is_hash(receipt_hash)
        or _digest(manifest_projection) != manifest_hash
        or _digest(raw_projection) != raw_hash
        or _digest(receipt_projection) != receipt_hash
    ):
        raise Exp009Stage4ValidationError("SELF_HASH_MISMATCH")
    commands, artifact_hashes = raw.get("commands"), manifest.get("artifact_sha256")
    expected_names = {
        "git_diff_check",
        *spec.focused_suite_filenames,
        _FULL_SUITE,
        _PIP_CHECK,
    }
    if not isinstance(commands, dict) or set(commands) != expected_names or not all(_valid_command(item, root=root) for item in commands.values()):
        raise Exp009Stage4ValidationError("COMMAND_INVENTORY_INVALID")
    if not isinstance(artifact_hashes, dict) or _inventory(root) != artifact_hashes:
        raise Exp009Stage4ValidationError("ARTIFACT_HASH_MISMATCH")
    safety = spec.safety_assertion
    input_hashes, suite_hashes = manifest.get("input_sha256"), manifest.get("suite_source_sha256")
    if not (
        manifest.get("schema_version") == spec.manifest_schema_version
        and raw.get("schema_version") == spec.raw_schema_version
        and receipt.get("schema_version") == spec.receipt_schema_version
        and manifest.get("experiment_id") == raw.get("experiment_id") == "EXP-009"
        and manifest.get("stage") == raw.get("stage") == 4
        and manifest.get("execution_mode") == raw.get("execution_mode") == spec.execution_mode
        and manifest.get("offline_safety_assertion") == safety
        and manifest.get("focused_suite_filenames") == list(spec.focused_suite_filenames)
        and isinstance(input_hashes, dict)
        and set(input_hashes) == {"requirements.lock", *spec.source_filenames}
        and isinstance(suite_hashes, dict)
        and set(suite_hashes) == set(spec.focused_suite_filenames)
        and all(_is_hash(value) for value in input_hashes.values())
        and all(_is_hash(value) for value in suite_hashes.values())
        and _is_hash(receipt.get("git_commit"), length=40)
        and receipt.get("manifest_sha256") == manifest_hash
        and receipt.get("raw_result_sha256") == raw_hash
        and receipt.get("git_commit") == raw.get("git_commit") == manifest.get("git", {}).get("commit")
        and receipt.get("validation_status") == raw.get("status") == manifest.get("validation_status")
        and receipt.get("command_count") == len(commands)
        and receipt.get("failed_commands") == raw.get("failed_commands")
        and raw.get("live_database_or_routing_activity") is False
    ):
        raise Exp009Stage4ValidationError("BUNDLE_SEMANTICS_INVALID")
    if raw.get("status") == "COMPLETE" and not all(item["passed"] is True for item in commands.values()):
        raise Exp009Stage4ValidationError("COMPLETE_COMMAND_FAILURE")
    if require_complete and raw.get("status") != "COMPLETE":
        raise Exp009Stage4ValidationError("VALIDATION_INCOMPLETE")
    return {"status": raw["status"], "git_commit": raw["git_commit"], "manifest_sha256": manifest_hash, "raw_result_sha256": raw_hash, "command_count": len(commands)}


def run_validation(
    *, output_dir: Path, repository: Path | None = None,
    git_state_provider: GitStateProvider = git_state,
    suite_runner: SuiteRunner = _run,
    spec: Stage4ValidationSpec = _OFFLINE_COMPOSITION_SPEC,
) -> dict[str, object]:
    """Create a sealed Stage-4 composition evidence bundle from a clean revision."""

    repository_path = (repository or Path(__file__).parents[1]).resolve()
    output = Path(output_dir).absolute()
    if output.exists() or output.is_symlink():
        raise Exp009Stage4ValidationError("OUTPUT_PATH_EXISTS")
    state = dict(git_state_provider(repository_path))
    if state.get("dirty") is not False:
        raise Exp009Stage4ValidationError("WORKTREE_DIRTY")
    commit = state.get("commit")
    if not _is_hash(commit, length=40):
        raise Exp009Stage4ValidationError("GIT_COMMIT_INVALID")
    source_hashes, suite_hashes = _source_hashes(repository_path, spec=spec)
    lock = repository_path / "requirements.lock"
    if not lock.is_file():
        raise Exp009Stage4ValidationError("REQUIREMENTS_LOCK_MISSING")
    output.mkdir(parents=True, mode=0o700)
    output.chmod(0o700)
    commands: dict[str, dict[str, object]] = {
        "git_diff_check": _record(output=output, name="git_diff_check", command=("git", "diff", "--check"), repository=repository_path, runner=suite_runner)
    }
    for filename in spec.focused_suite_filenames:
        commands[filename] = _record(
            output=output, name=filename,
            command=(sys.executable, "-m", "unittest", "discover", "tests", "-p", filename, "-v"),
            repository=repository_path, runner=suite_runner,
        )
    commands[_FULL_SUITE] = _record(
        output=output, name=_FULL_SUITE,
        command=(sys.executable, "-m", "unittest", "discover", "tests", "-q", "--failfast"),
        repository=repository_path, runner=suite_runner,
    )
    commands[_PIP_CHECK] = _record(
        output=output, name=_PIP_CHECK, command=(sys.executable, "-m", "pip", "check"), repository=repository_path, runner=suite_runner,
    )
    failed = [name for name, result in commands.items() if result["passed"] is not True]
    status = "COMPLETE" if not failed else "INCOMPLETE"
    raw: dict[str, object] = {
        "schema_version": spec.raw_schema_version, "experiment_id": "EXP-009", "stage": 4,
        "execution_mode": spec.execution_mode, "status": status, "git_commit": commit,
        "failed_commands": failed, "commands": commands, "live_database_or_routing_activity": False,
    }
    raw["self_sha256"] = _digest(raw)
    _write_json(output / "raw_result.json", raw)
    manifest: dict[str, object] = {
        "schema_version": spec.manifest_schema_version, "experiment_id": "EXP-009", "stage": 4,
        "validation_status": status, "execution_mode": spec.execution_mode,
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "git": {"commit": commit, "dirty": False}, "python": sys.version,
        "platform": platform.platform(), "architecture": platform.machine(),
        "input_sha256": {"requirements.lock": sha256_file(lock), **source_hashes},
        "suite_source_sha256": suite_hashes, "focused_suite_filenames": list(spec.focused_suite_filenames),
        "required_full_suite": _FULL_SUITE, "artifact_sha256": _inventory(output),
        "offline_safety_assertion": spec.safety_assertion,
    }
    manifest["self_sha256"] = _digest(manifest)
    _write_json(output / "manifest.json", manifest)
    receipt = {
        "schema_version": spec.receipt_schema_version, "validation_status": status,
        "git_commit": commit, "manifest_sha256": manifest["self_sha256"],
        "raw_result_sha256": raw["self_sha256"], "command_count": len(commands), "failed_commands": failed,
    }
    receipt["self_sha256"] = _digest(receipt)
    _write_json(output / "execution_receipt.json", receipt)
    if status != "COMPLETE":
        raise Exp009Stage4ValidationError("SUITE_FAILURE:" + ",".join(failed))
    verify_validation_bundle(output, spec=spec)
    return raw


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    arguments = parser.parse_args()
    print(json.dumps(run_validation(output_dir=arguments.output_dir), sort_keys=True))


if __name__ == "__main__":
    main()
