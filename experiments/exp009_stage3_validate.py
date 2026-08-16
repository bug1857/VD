"""Immutable, offline verification bundle for EXP-009 Stage 3.

The verifier executes only committed rollback-containment contract suites and
the repository suite. It records byte-for-byte command output in a closed,
hash-verified evidence bundle. It neither imports a route/rollback runtime nor
creates a Milvus client, issues a search, or enables candidate routing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path

from vdbench.artifacts import git_state, sha256_file

__all__ = [
    "STAGE3_SUITE_FILENAMES",
    "Exp009Stage3ValidationError",
    "run_validation",
    "verify_validation_bundle",
]


STAGE3_SUITE_FILENAMES = (
    "test_actuation_persistence.py",
    "test_canary_grant_store.py",
    "test_canary_lifecycle_audit.py",
    "test_canary_route_state.py",
    "test_canary_route_authority.py",
    "test_canary_expiry_reconciliation.py",
    "test_canary_activation.py",
    "test_canary_rollback.py",
)
_SOURCE_FILENAMES = (
    "src/vdbench/actuation_persistence.py",
    "src/vdbench/canary_activation.py",
    "src/vdbench/canary_approval.py",
    "src/vdbench/canary_expiry_reconciliation.py",
    "src/vdbench/canary_grant_store.py",
    "src/vdbench/canary_lifecycle_audit.py",
    "src/vdbench/canary_rollback.py",
    "src/vdbench/canary_route_authority.py",
    "src/vdbench/canary_route_state.py",
    "src/vdbench/canary_routing.py",
    "src/vdbench/canary_workload.py",
    "src/vdbench/config.py",
    "src/vdbench/policy.py",
    "experiments/exp009_stage3_validate.py",
)
_FULL_SUITE_NAME = "full_repository_suite"
_PIP_CHECK_NAME = "pip_check"
_COMMAND_RESULT_FIELDS = frozenset(
    {
        "command",
        "returncode",
        "passed",
        "stdout_file",
        "stdout_sha256",
        "stderr_file",
        "stderr_sha256",
    }
)
_SHA256_HEX = frozenset("0123456789abcdef")


class Exp009Stage3ValidationError(RuntimeError):
    """Raised after an EXP-009 Stage-3 evidence run fails closed."""


SuiteRunner = Callable[..., subprocess.CompletedProcess[str]]
GitStateProvider = Callable[[Path], Mapping[str, object]]


def _canonical_json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def _content_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in _SHA256_HEX for character in value)
    )


def _is_commit(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 40
        and all(character in _SHA256_HEX for character in value)
    )


def _write_bytes(path: Path, payload: bytes) -> None:
    """Atomically publish an evidence file without overwriting an artifact."""

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


def _run_command(
    command: tuple[str, ...], *, repository: Path
) -> subprocess.CompletedProcess[str]:
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
    """Run one command and preserve its decoded stdout/stderr independently."""

    try:
        completed = runner(command, repository=repository)
    except OSError as error:
        completed = subprocess.CompletedProcess(
            command, 127, "", f"{type(error).__name__}: {error}\n"
        )
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
        for filename in STAGE3_SUITE_FILENAMES
    }
    return source, suites


def _load_json(path: Path, *, code: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise Exp009Stage3ValidationError(code) from error
    if not isinstance(value, dict):
        raise Exp009Stage3ValidationError("BUNDLE_DOCUMENT_INVALID")
    return value


def _valid_command_record(record: object, *, root: Path) -> bool:
    if not isinstance(record, dict) or frozenset(record) != _COMMAND_RESULT_FIELDS:
        return False
    command = record["command"]
    if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
        return False
    if type(record["returncode"]) is not int or type(record["passed"]) is not bool:
        return False
    if record["passed"] is not (record["returncode"] == 0):
        return False
    for file_name, digest_name in (
        ("stdout_file", "stdout_sha256"),
        ("stderr_file", "stderr_sha256"),
    ):
        relative = record[file_name]
        if not isinstance(relative, str) or not relative.startswith("commands/"):
            return False
        candidate = root / relative
        if not candidate.is_file() or candidate.is_symlink():
            return False
        if not _is_sha256(record[digest_name]):
            return False
        if sha256_file(candidate) != record[digest_name]:
            return False
    return True


def verify_validation_bundle(
    output_dir: Path, *, require_complete: bool = True
) -> dict[str, object]:
    """Independently verify one closed Stage-3 artifact set.

    The verifier rejects absent, symlinked, malformed, substituted, incomplete,
    and hash-inconsistent evidence. It performs no route or database action.
    """

    declared_root = Path(output_dir)
    if declared_root.is_symlink():
        raise Exp009Stage3ValidationError("SYMLINK_ARTIFACT_REFUSED")
    root = declared_root.absolute()
    manifest_path = root / "manifest.json"
    raw_result_path = root / "raw_result.json"
    receipt_path = root / "execution_receipt.json"
    if not root.is_dir() or not all(
        path.is_file() for path in (manifest_path, raw_result_path, receipt_path)
    ):
        raise Exp009Stage3ValidationError("BUNDLE_STRUCTURE_INVALID")
    if any(path.is_symlink() for path in root.rglob("*")):
        raise Exp009Stage3ValidationError("SYMLINK_ARTIFACT_REFUSED")
    manifest = _load_json(manifest_path, code="BUNDLE_JSON_INVALID")
    raw_result = _load_json(raw_result_path, code="BUNDLE_JSON_INVALID")
    receipt = _load_json(receipt_path, code="BUNDLE_JSON_INVALID")
    try:
        manifest_hash = manifest["self_sha256"]
        raw_result_hash = raw_result["self_sha256"]
        artifact_sha256 = manifest["artifact_sha256"]
        commands = raw_result["commands"]
    except KeyError as error:
        raise Exp009Stage3ValidationError("BUNDLE_FIELD_MISSING") from error
    if not (
        _is_sha256(manifest_hash)
        and _is_sha256(raw_result_hash)
        and isinstance(artifact_sha256, dict)
        and isinstance(commands, dict)
    ):
        raise Exp009Stage3ValidationError("BUNDLE_FIELD_INVALID")
    manifest_projection = dict(manifest)
    manifest_projection.pop("self_sha256", None)
    raw_result_projection = dict(raw_result)
    raw_result_projection.pop("self_sha256", None)
    if (
        _content_sha256(manifest_projection) != manifest_hash
        or _content_sha256(raw_result_projection) != raw_result_hash
    ):
        raise Exp009Stage3ValidationError("SELF_HASH_MISMATCH")
    expected_commands = {
        "git_diff_check",
        *STAGE3_SUITE_FILENAMES,
        _FULL_SUITE_NAME,
        _PIP_CHECK_NAME,
    }
    actual_inventory = _artifact_inventory(root)
    if actual_inventory != artifact_sha256:
        raise Exp009Stage3ValidationError("ARTIFACT_HASH_MISMATCH")
    if set(commands) != expected_commands or not all(
        _valid_command_record(record, root=root) for record in commands.values()
    ):
        raise Exp009Stage3ValidationError("COMMAND_INVENTORY_INVALID")
    if not (
        manifest.get("schema_version") == "exp009-stage3-manifest-v1"
        and raw_result.get("schema_version") == "exp009-stage3-raw-result-v1"
        and receipt.get("schema_version") == "exp009-stage3-execution-receipt-v1"
        and manifest.get("experiment_id") == raw_result.get("experiment_id") == "EXP-009"
        and manifest.get("stage") == raw_result.get("stage") == 3
        and manifest.get("execution_mode") == raw_result.get("execution_mode") == "offline"
        and manifest.get("focused_suite_filenames") == list(STAGE3_SUITE_FILENAMES)
        and manifest.get("offline_safety_assertion")
        == {
            "constructs_milvus_client": False,
            "issues_search": False,
            "claims_or_enables_candidate_route": False,
            "invokes_live_rollback": False,
        }
    ):
        raise Exp009Stage3ValidationError("BUNDLE_SEMANTICS_INVALID")
    input_sha256 = manifest.get("input_sha256")
    suite_source_sha256 = manifest.get("suite_source_sha256")
    if not (
        isinstance(input_sha256, dict)
        and set(input_sha256) == {"requirements.lock", *_SOURCE_FILENAMES}
        and isinstance(suite_source_sha256, dict)
        and set(suite_source_sha256) == set(STAGE3_SUITE_FILENAMES)
        and _is_commit(receipt.get("git_commit"))
    ):
        raise Exp009Stage3ValidationError("SOURCE_INVENTORY_INVALID")
    if not all(
        isinstance(value, str) and _is_sha256(value)
        for value in input_sha256.values()
    ) or not all(
        isinstance(value, str) and _is_sha256(value)
        for value in suite_source_sha256.values()
    ):
        raise Exp009Stage3ValidationError("SOURCE_HASH_INVALID")
    if not (
        receipt.get("manifest_sha256") == manifest_hash
        and receipt.get("raw_result_sha256") == raw_result_hash
        and receipt.get("git_commit")
        == raw_result.get("git_commit")
        == manifest.get("git", {}).get("commit")
        and receipt.get("validation_status")
        == raw_result.get("status")
        == manifest.get("validation_status")
        and receipt.get("command_count") == len(commands)
    ):
        raise Exp009Stage3ValidationError("RECEIPT_BINDING_INVALID")
    if raw_result.get("status") == "COMPLETE" and not all(
        record["passed"] is True for record in commands.values()
    ):
        raise Exp009Stage3ValidationError("COMPLETE_COMMAND_FAILURE")
    if require_complete and raw_result.get("status") != "COMPLETE":
        raise Exp009Stage3ValidationError("VALIDATION_INCOMPLETE")
    return {
        "status": raw_result["status"],
        "git_commit": raw_result["git_commit"],
        "manifest_sha256": manifest_hash,
        "raw_result_sha256": raw_result_hash,
        "command_count": len(commands),
    }


def run_validation(
    *,
    output_dir: Path,
    repository: Path | None = None,
    git_state_provider: GitStateProvider = git_state,
    suite_runner: SuiteRunner = _run_command,
) -> dict[str, object]:
    """Write one complete immutable Stage-3 evidence bundle from a clean commit."""

    resolved_repository = (repository or Path(__file__).parents[1]).resolve()
    requested_output = Path(output_dir)
    if requested_output.exists() or requested_output.is_symlink():
        raise Exp009Stage3ValidationError("OUTPUT_PATH_EXISTS")
    resolved_output = requested_output.absolute()
    state = dict(git_state_provider(resolved_repository))
    if state.get("dirty") is not False:
        raise Exp009Stage3ValidationError("WORKTREE_DIRTY")
    commit = state.get("commit")
    if not isinstance(commit, str) or len(commit) != 40:
        raise Exp009Stage3ValidationError("GIT_COMMIT_INVALID")
    source_sha256, suite_source_sha256 = _source_hashes(resolved_repository)
    lock_path = resolved_repository / "requirements.lock"
    if not lock_path.is_file():
        raise Exp009Stage3ValidationError("REQUIREMENTS_LOCK_MISSING")
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
    for filename in STAGE3_SUITE_FILENAMES:
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
        command=(
            sys.executable,
            "-m",
            "unittest",
            "discover",
            "tests",
            "-q",
            "--failfast",
        ),
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
    failed_commands = tuple(
        name for name, result in commands.items() if result["passed"] is not True
    )
    status = "COMPLETE" if not failed_commands else "INCOMPLETE"
    raw_result: dict[str, object] = {
        "schema_version": "exp009-stage3-raw-result-v1",
        "experiment_id": "EXP-009",
        "stage": 3,
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
        "schema_version": "exp009-stage3-manifest-v1",
        "experiment_id": "EXP-009",
        "stage": 3,
        "validation_status": status,
        "execution_mode": "offline",
        "timestamp_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "git": {"commit": commit, "dirty": False},
        "python": sys.version,
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "input_sha256": input_sha256,
        "suite_source_sha256": suite_source_sha256,
        "focused_suite_filenames": list(STAGE3_SUITE_FILENAMES),
        "required_full_suite": _FULL_SUITE_NAME,
        "artifact_sha256": _artifact_inventory(resolved_output),
        "offline_safety_assertion": {
            "constructs_milvus_client": False,
            "issues_search": False,
            "claims_or_enables_candidate_route": False,
            "invokes_live_rollback": False,
        },
    }
    manifest["self_sha256"] = _content_sha256(manifest)
    _write_json(resolved_output / "manifest.json", manifest)
    receipt = {
        "schema_version": "exp009-stage3-execution-receipt-v1",
        "validation_status": status,
        "git_commit": commit,
        "manifest_sha256": manifest["self_sha256"],
        "raw_result_sha256": raw_result["self_sha256"],
        "command_count": len(commands),
        "failed_commands": list(failed_commands),
    }
    _write_json(resolved_output / "execution_receipt.json", receipt)
    if status != "COMPLETE":
        raise Exp009Stage3ValidationError("SUITE_FAILURE:" + ",".join(failed_commands))
    return raw_result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    arguments = parser.parse_args()
    print(json.dumps(run_validation(output_dir=arguments.output_dir), sort_keys=True))


if __name__ == "__main__":
    main()
