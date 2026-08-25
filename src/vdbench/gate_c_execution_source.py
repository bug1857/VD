"""Fail-closed identity for code executing a bounded Gate-C checkpoint.

The upstream ``source_revision`` belongs to Gate-A/Gate-B evidence.  This
module independently proves the committed checkout whose ``src/vdbench``
runtime is about to execute a bounded checkpoint.  Unrelated documentation
and artifact changes are deliberately outside this identity boundary.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

__all__ = [
    "GateCExecutionSourceError",
    "VerifiedGateCExecutionSource",
    "derive_gate_c_execution_source",
    "verify_gate_c_execution_source",
]


_REVISION = re.compile(r"[0-9a-f]{40}")
_PACKAGE_RELATIVE = Path("src/vdbench")
_SOURCE_ROOT_RELATIVE = Path("src")
_STARTUP_HOOK_NAMES = ("sitecustomize.py", "usercustomize.py")


class GateCExecutionSourceError(RuntimeError):
    """Stable fail-closed execution-source identity failure."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> GateCExecutionSourceError:
    return GateCExecutionSourceError(code)


@dataclass(frozen=True, slots=True)
class VerifiedGateCExecutionSource:
    repository_root: Path
    execution_source_revision: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.repository_root, Path)
            or not self.repository_root.is_absolute()
            or type(self.execution_source_revision) is not str
            or _REVISION.fullmatch(self.execution_source_revision) is None
        ):
            raise _error("GATE_C_EXECUTION_SOURCE_INVALID")


def _git(root: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ("git", "-C", str(root), *arguments),
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env={**os.environ, "LC_ALL": "C"},
        )
    except (OSError, UnicodeError, subprocess.CalledProcessError) as exc:
        raise _error("GATE_C_EXECUTION_SOURCE_UNVERIFIABLE") from exc
    return completed.stdout.strip()


def _git_bytes(root: Path, *arguments: str) -> bytes:
    try:
        completed = subprocess.run(
            ("git", "-C", str(root), *arguments),
            check=True,
            capture_output=True,
            env={**os.environ, "LC_ALL": "C"},
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise _error("GATE_C_EXECUTION_SOURCE_UNVERIFIABLE") from exc
    return completed.stdout


def _verify_runtime_tree(root: Path, package_root: Path) -> set[Path]:
    """Compare every committed runtime blob with the exact filesystem bytes."""

    object_format = _git(root, "rev-parse", "--show-object-format")
    if object_format not in {"sha1", "sha256"}:
        raise _error("GATE_C_EXECUTION_SOURCE_UNVERIFIABLE")
    records = _git_bytes(
        root,
        "ls-tree",
        "-rz",
        "HEAD",
        "--",
        str(_PACKAGE_RELATIVE),
        "src/vdbench.py",
        "src/sitecustomize.py",
        "src/usercustomize.py",
        "sitecustomize.py",
        "usercustomize.py",
    )
    tracked: set[Path] = set()
    for record in records.split(b"\x00"):
        if not record:
            continue
        try:
            metadata, raw_path = record.split(b"\t", 1)
            mode, kind, expected_object = metadata.split(b" ", 2)
            relative = Path(raw_path.decode("utf-8"))
        except (UnicodeError, ValueError) as exc:
            raise _error("GATE_C_EXECUTION_SOURCE_UNVERIFIABLE") from exc
        if kind != b"blob" or mode not in {b"100644", b"100755"}:
            raise _error("GATE_C_EXECUTION_SOURCE_DRIFT")
        location = root / relative
        try:
            info = location.stat(follow_symlinks=False)
            raw = location.read_bytes()
        except OSError as exc:
            raise _error("GATE_C_EXECUTION_SOURCE_DRIFT") from exc
        if not stat.S_ISREG(info.st_mode):
            raise _error("GATE_C_EXECUTION_SOURCE_DRIFT")
        digest = hashlib.new(object_format)
        digest.update(f"blob {len(raw)}\0".encode("ascii"))
        digest.update(raw)
        if digest.hexdigest().encode("ascii") != expected_object:
            raise _error("GATE_C_EXECUTION_SOURCE_DRIFT")
        tracked.add(location.resolve())

    executable_suffixes = {".py", ".so", ".pyd", ".dylib"}
    for location in package_root.rglob("*"):
        if not location.is_file() or "__pycache__" in location.parts:
            continue
        if location.suffix in executable_suffixes and location.resolve() not in tracked:
            raise _error("GATE_C_EXECUTION_SOURCE_DRIFT")
    source_root = root / _SOURCE_ROOT_RELATIVE
    for location in source_root.iterdir():
        if not location.is_file():
            continue
        is_package_shadow = (
            location.stem == "vdbench" and location.suffix in executable_suffixes
        )
        if is_package_shadow and location.resolve() not in tracked:
            raise _error("GATE_C_EXECUTION_SOURCE_DRIFT")

    # The governed invocation exposes the repository root through the current
    # working-directory entry and ``src`` through PYTHONPATH.  Bind exact
    # committed hooks at either project-controlled import root and reject every
    # untracked, replaced, or non-regular hook without requiring unrelated
    # worktree files to be clean.
    for search_root in (root, source_root):
        for name in _STARTUP_HOOK_NAMES:
            location = search_root / name
            try:
                info = location.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise _error("GATE_C_EXECUTION_SOURCE_DRIFT") from exc
            if (
                not stat.S_ISREG(info.st_mode)
                or location.resolve() not in tracked
            ):
                raise _error("GATE_C_EXECUTION_SOURCE_DRIFT")
    return tracked


def _verify_loaded_repository_startup_hooks(
    modules: tuple[ModuleType, ...], root: Path, tracked: set[Path]
) -> None:
    """Reject loaded project-owned startup hooks outside the bound tree."""

    startup_module_names = {Path(name).stem for name in _STARTUP_HOOK_NAMES}
    for module in modules:
        if getattr(module, "__name__", None) not in startup_module_names:
            continue
        location = getattr(module, "__file__", None)
        if type(location) is not str or not location:
            continue
        unresolved = Path(location)
        if not unresolved.is_absolute():
            unresolved = Path.cwd() / unresolved
        resolved = unresolved.resolve(strict=False)
        try:
            resolved.relative_to(root)
        except ValueError:
            # Interpreter/environment startup hooks are outside the repository
            # execution-source identity and remain an environment trust input.
            continue
        try:
            existing = unresolved.resolve(strict=True)
        except OSError as exc:
            raise _error("GATE_C_EXECUTION_SOURCE_DRIFT") from exc
        if existing not in tracked:
            raise _error("GATE_C_EXECUTION_SOURCE_DRIFT")


def _module_paths(modules: tuple[ModuleType, ...]) -> tuple[Path, ...]:
    paths: list[Path] = []
    for module in modules:
        name = getattr(module, "__name__", None)
        location = getattr(module, "__file__", None)
        if type(name) is not str or not name.startswith("vdbench"):
            continue
        if type(location) is not str or not location:
            raise _error("GATE_C_EXECUTION_IMPORT_IDENTITY_INVALID")
        try:
            paths.append(Path(location).resolve(strict=True))
        except OSError as exc:
            raise _error("GATE_C_EXECUTION_IMPORT_IDENTITY_INVALID") from exc
    return tuple(paths)


def verify_gate_c_execution_source(
    repository_root: str | os.PathLike[str],
    *,
    expected_revision: str | None = None,
    imported_modules: tuple[ModuleType, ...] | None = None,
) -> VerifiedGateCExecutionSource:
    """Verify HEAD, the runtime package worktree, and imported package origin."""

    try:
        root = Path(repository_root).resolve(strict=True)
    except OSError as exc:
        raise _error("GATE_C_EXECUTION_SOURCE_UNVERIFIABLE") from exc
    if not root.is_dir():
        raise _error("GATE_C_EXECUTION_SOURCE_UNVERIFIABLE")
    top = _git(root, "rev-parse", "--show-toplevel")
    if Path(top).resolve() != root:
        raise _error("GATE_C_EXECUTION_REPOSITORY_MISMATCH")
    revision = _git(root, "rev-parse", "--verify", "HEAD^{commit}")
    if _REVISION.fullmatch(revision) is None:
        raise _error("GATE_C_EXECUTION_SOURCE_UNVERIFIABLE")
    if expected_revision is not None and (
        type(expected_revision) is not str
        or _REVISION.fullmatch(expected_revision) is None
        or revision != expected_revision
    ):
        raise _error("GATE_C_EXECUTION_SOURCE_REVISION_MISMATCH")

    unresolved_package_root = root / _PACKAGE_RELATIVE
    try:
        package_info = unresolved_package_root.stat(follow_symlinks=False)
        package_root = unresolved_package_root.resolve(strict=True)
        package_root.relative_to(root)
    except (OSError, ValueError) as exc:
        raise _error("GATE_C_EXECUTION_PACKAGE_MISSING") from exc
    if not stat.S_ISDIR(package_info.st_mode):
        raise _error("GATE_C_EXECUTION_PACKAGE_MISSING")
    tracked = _verify_runtime_tree(root, package_root)

    selected = (
        tuple(module for module in sys.modules.values() if isinstance(module, ModuleType))
        if imported_modules is None
        else imported_modules
    )
    _verify_loaded_repository_startup_hooks(selected, root, tracked)
    for location in _module_paths(selected):
        try:
            location.relative_to(package_root)
        except ValueError as exc:
            raise _error("GATE_C_EXECUTION_IMPORT_IDENTITY_INVALID") from exc
    return VerifiedGateCExecutionSource(root, revision)


def derive_gate_c_execution_source(
    *, expected_revision: str | None = None
) -> VerifiedGateCExecutionSource:
    """Derive executor provenance from this module's repository checkout."""

    return verify_gate_c_execution_source(
        Path(__file__).resolve().parents[2],
        expected_revision=expected_revision,
    )
