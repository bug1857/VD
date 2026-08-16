"""Offline, immutable evidence runner for EXP-009 Stage 1.

The runner constructs the exact pre-registered L2 ``target-075`` 400→800
workload evidence bundle.  It has no route, candidate search, policy, actuation,
or PyMilvus path.  A real execution requires clean committed source; output is
written into a temporary sibling directory and atomically published only after
independent workload, selection, and calibration verification succeeds.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from .artifacts import canonical_json_bytes, sha256_file
from .canary_calibration import run_exp009_calibration
from .canary_workload import (
    WorkloadIdentityBinding,
    build_eligible_workload_manifest,
    create_candidate_selection_record,
    persist_candidate_selection_record,
    persist_eligible_workload_manifest,
    verify_candidate_selection_record,
    verify_eligible_workload_manifest,
)
from .exp005_acquisition import load_identity_baseline

EXP009_STAGE1_SCHEMA_VERSION = "exp009-stage1-evidence-v1"
EXP009_STAGE1_COMPLETION_SCHEMA_VERSION = "exp009-stage1-completion-v1"
EXP009_STAGE1_ENVIRONMENT_SCHEMA_VERSION = "exp009-stage1-environment-v1"
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_TRANSITION = ("L2", "target-075", 400, 800)
_REQUIRED_SOURCE_PATHS = (
    Path("src/vdbench/exp009_stage1.py"),
    Path("src/vdbench/canary_workload.py"),
    Path("src/vdbench/canary_calibration.py"),
    Path("src/vdbench/canary_statistics.py"),
    Path("src/vdbench/dataset002.py"),
    Path("src/vdbench/exp005_acquisition.py"),
)
_BUNDLE_FILES = frozenset(
    {
        "eligible_workload.json",
        "candidate_selection.json",
        "calibration.json",
        "environment.json",
        "run_manifest.json",
        "completion.json",
    }
)


class Exp009Stage1Error(RuntimeError):
    """Raised when an EXP-009 Stage-1 artifact cannot be trusted."""


class _DuplicateJsonField(ValueError):
    """Internal marker used to fail closed on ambiguous JSON documents."""


def _no_duplicate_json_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(key)
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class Exp009Stage1Result:
    """Pointer-only result for one atomically published evidence bundle."""

    output_dir: Path
    manifest_path: Path
    completion_path: Path
    eligible_occurrence_count: int
    candidate_count: int


class _StrictUtcClock:
    """Normalize an injectable clock into strictly increasing UTC timestamps."""

    def __init__(self, source: Callable[[], str] | None = None) -> None:
        self._source = source or self._now
        self._last: datetime | None = None

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        )

    def __call__(self) -> str:
        raw = self._source()
        if not isinstance(raw, str) or not raw.endswith("Z"):
            raise Exp009Stage1Error("CLOCK_TIMESTAMP_INVALID")
        try:
            value = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise Exp009Stage1Error("CLOCK_TIMESTAMP_INVALID") from exc
        if value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise Exp009Stage1Error("CLOCK_TIMESTAMP_INVALID")
        if self._last is not None and value <= self._last:
            value = self._last + timedelta(microseconds=1)
        self._last = value
        return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _run(command: Iterable[str], *, repository: Path) -> str:
    try:
        completed = subprocess.run(
            tuple(command),
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise Exp009Stage1Error("GIT_COMMAND_FAILED") from exc
    return completed.stdout


def assert_clean_committed_source(
    repository: str | os.PathLike[str],
    *,
    required_paths: Iterable[Path] = _REQUIRED_SOURCE_PATHS,
) -> str:
    """Require a committed, tracked, tracked-clean source revision.

    Existing untracked evidence directories are intentionally tolerated: Stage-1
    cannot require an empty repository because its datasets and prior immutable
    experiment bundles may be intentionally untracked.  Tracked modifications
    and every untracked source dependency still fail closed.
    """

    root = Path(repository).resolve()
    commit = _run(("git", "rev-parse", "HEAD"), repository=root).strip()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise Exp009Stage1Error("GIT_COMMIT_INVALID")
    if _run(("git", "status", "--porcelain", "--untracked-files=no"), repository=root):
        raise Exp009Stage1Error("SOURCE_TRACKED_DIRTY")
    for path in required_paths:
        relative = Path(path)
        if relative.is_absolute():
            try:
                relative = relative.resolve().relative_to(root)
            except ValueError as exc:
                raise Exp009Stage1Error("SOURCE_PATH_OUTSIDE_REPOSITORY") from exc
        if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
            raise Exp009Stage1Error("SOURCE_PATH_INVALID")
        try:
            subprocess.run(
                ("git", "ls-files", "--error-unmatch", "--", str(relative)),
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise Exp009Stage1Error("SOURCE_PATH_UNTRACKED") from exc
    return commit


def _write_durable_json(path: Path, value: Mapping[str, object]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite immutable evidence artifact: {path}")
    payload = canonical_json_bytes(value)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
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


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_directory(temporary: Path, target: Path) -> None:
    if target.exists():
        raise FileExistsError(f"refusing to overwrite existing evidence directory: {target}")
    _fsync_directory(temporary)
    os.replace(temporary, target)
    _fsync_directory(target.parent)


def _artifact_entry(path: Path) -> dict[str, object]:
    return {"file": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)}


def _load_canonical_json(path: Path, *, error_code: str) -> Mapping[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_no_duplicate_json_fields,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")),
        )
    except (OSError, UnicodeError, json.JSONDecodeError, _DuplicateJsonField, ValueError) as exc:
        raise Exp009Stage1Error(error_code) from exc
    if not isinstance(value, Mapping) or raw != canonical_json_bytes(value):
        raise Exp009Stage1Error(error_code)
    return value


def _validated_identity(baseline_path: Path) -> WorkloadIdentityBinding:
    try:
        baseline = load_identity_baseline(baseline_path)
    except Exception as exc:
        raise Exp009Stage1Error("BASELINE_INVALID") from exc
    if (
        baseline.metric.value,
        baseline.threshold_stratum,
        baseline.last_known_good_ef,
        baseline.candidate_ef,
    ) != _TRANSITION:
        raise Exp009Stage1Error("BASELINE_TRANSITION_MISMATCH")
    return WorkloadIdentityBinding(
        configuration_identity=baseline.configuration_identity,
        data_identity=baseline.data_identity,
        flat_binding_id=baseline.flat_binding.identity_id,
        hnsw_binding_id=baseline.hnsw_binding.identity_id,
    )


def _run_manifest(
    *,
    created_at_utc: str,
    commit: str,
    dataset001_dir: Path,
    dataset002_dir: Path,
    baseline_path: Path,
    artifacts: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    return {
        "schema_version": EXP009_STAGE1_SCHEMA_VERSION,
        "created_at_utc": created_at_utc,
        "repository": {"commit": commit, "tracked_source_clean": True},
        "transition": {
            "metric": "L2",
            "threshold_stratum": "target-075",
            "last_known_good_ef": 400,
            "candidate_ef": 800,
            "candidate_fraction": 0.10,
        },
        "inputs": {
            "dataset001_dir": str(dataset001_dir),
            "dataset001_generation_manifest_sha256": sha256_file(
                dataset001_dir / "generation_manifest.json"
            ),
            "dataset002_dir": str(dataset002_dir),
            "dataset002_manifest_sha256": sha256_file(
                dataset002_dir / "dataset002_manifest.json"
            ),
            "reviewed_baseline_file_sha256": sha256_file(baseline_path),
        },
        "artifacts": dict(artifacts),
        "scope": {
            "offline_only": True,
            "candidate_route_implemented": False,
            "milvus_operation_performed": False,
            "automatic_actuation_authorized": False,
        },
    }


def _environment_document() -> dict[str, object]:
    """Capture reproducibility metadata without making a performance claim."""

    return {
        "schema_version": EXP009_STAGE1_ENVIRONMENT_SCHEMA_VERSION,
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
            "executable": sys.executable,
        },
        "numpy_version": np.__version__,
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor() or "UNAVAILABLE",
        },
        "performance_scope": "offline_nonperformance",
    }


def _exact_mapping(
    value: object, *, fields: frozenset[str], error_code: str
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or frozenset(value) != fields:
        raise Exp009Stage1Error(error_code)
    return value


def _require(value: bool, *, error_code: str) -> None:
    if not value:
        raise Exp009Stage1Error(error_code)


def _validate_environment_document(document: Mapping[str, object]) -> None:
    """Reject incomplete or noncanonical Stage-1 environment provenance."""

    root = _exact_mapping(
        document,
        fields=frozenset(
            {"schema_version", "python", "numpy_version", "platform", "performance_scope"}
        ),
        error_code="ENVIRONMENT_ARTIFACT_INVALID",
    )
    _require(
        root["schema_version"] == EXP009_STAGE1_ENVIRONMENT_SCHEMA_VERSION,
        error_code="ENVIRONMENT_ARTIFACT_INVALID",
    )
    _require(
        root["performance_scope"] == "offline_nonperformance",
        error_code="ENVIRONMENT_ARTIFACT_INVALID",
    )
    python = _exact_mapping(
        root["python"],
        fields=frozenset({"implementation", "version", "executable"}),
        error_code="ENVIRONMENT_ARTIFACT_INVALID",
    )
    operating_system = _exact_mapping(
        root["platform"],
        fields=frozenset({"system", "release", "machine", "processor"}),
        error_code="ENVIRONMENT_ARTIFACT_INVALID",
    )
    for value in (*python.values(), root["numpy_version"], *operating_system.values()):
        _require(
            isinstance(value, str) and bool(value.strip()),
            error_code="ENVIRONMENT_ARTIFACT_INVALID",
        )


def verify_stage1_bundle(
    *,
    output_dir: str | os.PathLike[str],
    dataset001_dir: str | os.PathLike[str],
    dataset002_dir: str | os.PathLike[str],
    baseline_path: str | os.PathLike[str],
) -> Exp009Stage1Result:
    """Independently verify one published offline EXP-009 Stage-1 bundle.

    This validates artifact integrity, input bindings, selection provenance, and
    deterministic calibration recomputation.  The recorded commit is treated
    as immutable provenance; source equivalence is intentionally not inferred
    merely because a caller runs this verifier from a later checkout.
    """

    target = Path(output_dir).resolve()
    dataset001_path = Path(dataset001_dir).resolve()
    dataset002_path = Path(dataset002_dir).resolve()
    baseline = Path(baseline_path).resolve()
    try:
        inventory = {path.name for path in target.iterdir()}
    except OSError as exc:
        raise Exp009Stage1Error("BUNDLE_UNREADABLE") from exc
    _require(inventory == _BUNDLE_FILES, error_code="BUNDLE_INVENTORY_INVALID")

    eligible_path = target / "eligible_workload.json"
    selection_path = target / "candidate_selection.json"
    calibration_path = target / "calibration.json"
    manifest_path = target / "run_manifest.json"
    completion_path = target / "completion.json"
    try:
        workload = verify_eligible_workload_manifest(
            eligible_path,
            dataset002_dir=dataset002_path,
            dataset001_dir=dataset001_path,
        )
    except Exception as exc:
        raise Exp009Stage1Error("ELIGIBLE_WORKLOAD_ARTIFACT_INVALID") from exc
    try:
        selection = verify_candidate_selection_record(selection_path, eligible_path)
    except Exception as exc:
        raise Exp009Stage1Error("CANDIDATE_SELECTION_ARTIFACT_INVALID") from exc
    calibration = _load_canonical_json(
        calibration_path, error_code="CALIBRATION_ARTIFACT_INVALID"
    )
    _require(
        calibration == run_exp009_calibration().to_document(),
        error_code="CALIBRATION_RECOMPUTATION_MISMATCH",
    )
    environment = _load_canonical_json(
        target / "environment.json", error_code="ENVIRONMENT_ARTIFACT_INVALID"
    )
    _validate_environment_document(environment)
    manifest = _load_canonical_json(manifest_path, error_code="RUN_MANIFEST_INVALID")
    root = _exact_mapping(
        manifest,
        fields=frozenset(
            {
                "schema_version",
                "created_at_utc",
                "repository",
                "transition",
                "inputs",
                "artifacts",
                "scope",
            }
        ),
        error_code="RUN_MANIFEST_INVALID",
    )
    _require(root["schema_version"] == EXP009_STAGE1_SCHEMA_VERSION, error_code="RUN_MANIFEST_INVALID")
    _StrictUtcClock(lambda: root["created_at_utc"])()
    repository = _exact_mapping(
        root["repository"],
        fields=frozenset({"commit", "tracked_source_clean"}),
        error_code="RUN_MANIFEST_INVALID",
    )
    commit = repository["commit"]
    _require(
        isinstance(commit, str)
        and len(commit) == 40
        and all(character in "0123456789abcdef" for character in commit),
        error_code="RUN_MANIFEST_INVALID",
    )
    _require(repository["tracked_source_clean"] is True, error_code="RUN_MANIFEST_INVALID")
    transition = _exact_mapping(
        root["transition"],
        fields=frozenset(
            {
                "metric",
                "threshold_stratum",
                "last_known_good_ef",
                "candidate_ef",
                "candidate_fraction",
            }
        ),
        error_code="RUN_MANIFEST_INVALID",
    )
    _require(
        transition
        == {
            "metric": "L2",
            "threshold_stratum": "target-075",
            "last_known_good_ef": 400,
            "candidate_ef": 800,
            "candidate_fraction": 0.10,
        },
        error_code="RUN_MANIFEST_INVALID",
    )
    _require(
        (
            workload.metric.value,
            workload.threshold_stratum,
            workload.last_known_good_ef,
            workload.candidate_ef,
        )
        == _TRANSITION,
        error_code="ELIGIBLE_WORKLOAD_ARTIFACT_INVALID",
    )
    _require(
        len(workload.occurrences) == 600
        and len(selection.candidate_occurrence_ids) == 60,
        error_code="CARDINALITY_INVALID",
    )
    inputs = _exact_mapping(
        root["inputs"],
        fields=frozenset(
            {
                "dataset001_dir",
                "dataset001_generation_manifest_sha256",
                "dataset002_dir",
                "dataset002_manifest_sha256",
                "reviewed_baseline_file_sha256",
            }
        ),
        error_code="RUN_MANIFEST_INVALID",
    )
    _validated_identity(baseline)
    _require(
        inputs
        == {
            "dataset001_dir": str(dataset001_path),
            "dataset001_generation_manifest_sha256": sha256_file(
                dataset001_path / "generation_manifest.json"
            ),
            "dataset002_dir": str(dataset002_path),
            "dataset002_manifest_sha256": sha256_file(
                dataset002_path / "dataset002_manifest.json"
            ),
            "reviewed_baseline_file_sha256": sha256_file(baseline),
        },
        error_code="INPUT_BINDING_MISMATCH",
    )
    artifacts = _exact_mapping(
        root["artifacts"],
        fields=frozenset(
            {
                "eligible_workload.json",
                "candidate_selection.json",
                "calibration.json",
                "environment.json",
            }
        ),
        error_code="RUN_MANIFEST_INVALID",
    )
    for artifact_name, artifact in artifacts.items():
        expected = _artifact_entry(target / artifact_name)
        _require(artifact == expected, error_code="ARTIFACT_HASH_MISMATCH")
    scope = _exact_mapping(
        root["scope"],
        fields=frozenset(
            {
                "offline_only",
                "candidate_route_implemented",
                "milvus_operation_performed",
                "automatic_actuation_authorized",
            }
        ),
        error_code="RUN_MANIFEST_INVALID",
    )
    _require(
        scope
        == {
            "offline_only": True,
            "candidate_route_implemented": False,
            "milvus_operation_performed": False,
            "automatic_actuation_authorized": False,
        },
        error_code="RUN_MANIFEST_INVALID",
    )
    completion = _load_canonical_json(completion_path, error_code="COMPLETION_INVALID")
    completed = _exact_mapping(
        completion,
        fields=frozenset(
            {
                "schema_version",
                "completed_at_utc",
                "status",
                "run_manifest_sha256",
                "eligible_occurrence_count",
                "candidate_count",
                "verification",
            }
        ),
        error_code="COMPLETION_INVALID",
    )
    _require(
        completed["schema_version"] == EXP009_STAGE1_COMPLETION_SCHEMA_VERSION,
        error_code="COMPLETION_INVALID",
    )
    _StrictUtcClock(lambda: completed["completed_at_utc"])()
    _require(completed["status"] == "COMPLETE", error_code="COMPLETION_INVALID")
    _require(
        completed["run_manifest_sha256"] == sha256_file(manifest_path),
        error_code="COMPLETION_MANIFEST_HASH_MISMATCH",
    )
    _require(
        completed["eligible_occurrence_count"] == 600 and completed["candidate_count"] == 60,
        error_code="CARDINALITY_INVALID",
    )
    verification = _exact_mapping(
        completed["verification"],
        fields=frozenset(
            {
                "eligible_workload_rebuilt",
                "candidate_selection_bound",
                "calibration_recomputed",
                "manifest_artifact_hashes_match",
                "exact_cardinality",
                "offline_scope",
            }
        ),
        error_code="COMPLETION_INVALID",
    )
    _require(all(value is True for value in verification.values()), error_code="COMPLETION_INVALID")
    return Exp009Stage1Result(
        output_dir=target,
        manifest_path=manifest_path,
        completion_path=completion_path,
        eligible_occurrence_count=600,
        candidate_count=60,
    )


def run_stage1(
    *,
    repository: str | os.PathLike[str] = _REPOSITORY_ROOT,
    dataset001_dir: str | os.PathLike[str],
    dataset002_dir: str | os.PathLike[str],
    baseline_path: str | os.PathLike[str],
    output_dir: str | os.PathLike[str],
    clock: Callable[[], str] | None = None,
) -> Exp009Stage1Result:
    """Create one complete offline Stage-1 artifact bundle.

    ``output_dir`` must not exist.  All normal execution paths verify that the
    source is committed and tracked-clean before they read datasets or draw the
    CSPRNG selection.  The transition is frozen to the pre-registered L2
    quality-recovery exception; generic routing belongs to a later stage.
    """

    root = Path(repository).resolve()
    commit = assert_clean_committed_source(root)
    dataset001_path = Path(dataset001_dir).resolve()
    dataset002_path = Path(dataset002_dir).resolve()
    baseline = Path(baseline_path).resolve()
    target = Path(output_dir).resolve()
    if target.exists():
        raise FileExistsError(f"refusing to overwrite existing evidence directory: {target}")
    identity = _validated_identity(baseline)
    strict_clock = _StrictUtcClock(clock)
    temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        eligible_path = temporary / "eligible_workload.json"
        selection_path = temporary / "candidate_selection.json"
        calibration_path = temporary / "calibration.json"
        environment_path = temporary / "environment.json"
        manifest_path = temporary / "run_manifest.json"
        completion_path = temporary / "completion.json"

        workload = build_eligible_workload_manifest(
            dataset002_dir=dataset002_path,
            dataset001_dir=dataset001_path,
            metric="L2",
            threshold_stratum="target-075",
            candidate_ef=800,
            last_known_good_ef=400,
            identity=identity,
            created_at_utc=strict_clock(),
        )
        persist_eligible_workload_manifest(eligible_path, workload)
        selection = create_candidate_selection_record(
            eligible_path,
            selected_at_utc=strict_clock(),
        )
        persist_candidate_selection_record(selection_path, selection, eligible_path)
        calibration = run_exp009_calibration()
        _write_durable_json(calibration_path, calibration.to_document())
        _write_durable_json(environment_path, _environment_document())

        artifacts = {
            path.name: _artifact_entry(path)
            for path in (eligible_path, selection_path, calibration_path, environment_path)
        }
        manifest = _run_manifest(
            created_at_utc=strict_clock(),
            commit=commit,
            dataset001_dir=dataset001_path,
            dataset002_dir=dataset002_path,
            baseline_path=baseline,
            artifacts=artifacts,
        )
        _write_durable_json(manifest_path, manifest)

        verified_workload = verify_eligible_workload_manifest(
            eligible_path,
            dataset002_dir=dataset002_path,
            dataset001_dir=dataset001_path,
        )
        verified_selection = verify_candidate_selection_record(selection_path, eligible_path)
        verification = {
            "eligible_workload_rebuilt": verified_workload == workload,
            "candidate_selection_bound": verified_selection == selection,
            "calibration_recomputed": _load_canonical_json(
                calibration_path,
                error_code="CALIBRATION_ARTIFACT_INVALID",
            )
            == run_exp009_calibration().to_document(),
            "manifest_artifact_hashes_match": all(
                _artifact_entry(temporary / name) == entry
                for name, entry in artifacts.items()
            ),
            "exact_cardinality": len(workload.occurrences) == 600
            and len(selection.candidate_occurrence_ids) == 60,
            "offline_scope": True,
        }
        if not all(verification.values()):
            raise Exp009Stage1Error("STAGE1_SELF_VERIFICATION_FAILED")
        completion = {
            "schema_version": EXP009_STAGE1_COMPLETION_SCHEMA_VERSION,
            "completed_at_utc": strict_clock(),
            "status": "COMPLETE",
            "run_manifest_sha256": sha256_file(manifest_path),
            "eligible_occurrence_count": len(workload.occurrences),
            "candidate_count": len(selection.candidate_occurrence_ids),
            "verification": verification,
        }
        _write_durable_json(completion_path, completion)
        _publish_directory(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return Exp009Stage1Result(
        output_dir=target,
        manifest_path=target / "run_manifest.json",
        completion_path=target / "completion.json",
        eligible_occurrence_count=600,
        candidate_count=60,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vd-exp009-stage1")
    parser.add_argument("--repository", type=Path, default=_REPOSITORY_ROOT)
    parser.add_argument("--dataset001-dir", type=Path, required=True)
    parser.add_argument("--dataset002-dir", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = run_stage1(
        repository=args.repository,
        dataset001_dir=args.dataset001_dir,
        dataset002_dir=args.dataset002_dir,
        baseline_path=args.baseline,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "output_dir": str(result.output_dir),
                "manifest_path": str(result.manifest_path),
                "completion_path": str(result.completion_path),
                "eligible_occurrence_count": result.eligible_occurrence_count,
                "candidate_count": result.candidate_count,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "EXP009_STAGE1_COMPLETION_SCHEMA_VERSION",
    "EXP009_STAGE1_ENVIRONMENT_SCHEMA_VERSION",
    "EXP009_STAGE1_SCHEMA_VERSION",
    "Exp009Stage1Error",
    "Exp009Stage1Result",
    "assert_clean_committed_source",
    "run_stage1",
    "verify_stage1_bundle",
]
