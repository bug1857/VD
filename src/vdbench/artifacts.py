"""Immutable DATASET-001 and EXP-002 artifact serialization.

Every artifact is written once, hashed from its on-disk bytes, and listed in a
canonical manifest plus ``SHA256SUMS``. Existing output directories are refused
so a recorded dataset or run cannot be silently replaced.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
import os
import platform
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Any, Iterable, Mapping

import numpy as np

from .config import ENV001_PINS, ContractViolation, derive_seed
from .dataset import BoundaryFixture, DatasetBundle, FrozenThreshold


def canonical_json_bytes(value: object) -> bytes:
    """Encode JSON deterministically for byte-stable checksums."""

    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def sha256_file(path: Path) -> str:
    """Return the lower-case SHA-256 digest of a file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _new_directory(path: Path) -> None:
    if path.exists():
        raise ContractViolation(f"refusing to overwrite existing artifact path: {path}")
    path.mkdir(parents=True)


def _write_json(path: Path, value: object) -> None:
    path.write_bytes(canonical_json_bytes(value))


def write_immutable_json(path: Path, value: object) -> None:
    """Write canonical JSON while refusing replacement of prior evidence."""

    path = Path(path)
    if path.exists():
        raise ContractViolation(f"refusing to overwrite immutable JSON: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(path, value)


def _artifact_entry(path: Path) -> dict[str, object]:
    return {
        "file": path.name,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def write_dataset_artifacts(
    output_dir: Path,
    bundle: DatasetBundle,
    thresholds: Mapping[object, tuple[FrozenThreshold, ...]],
    fixtures: Iterable[BoundaryFixture],
) -> dict[str, Any]:
    """Persist one immutable, fully checksummed dataset directory."""

    output_dir = Path(output_dir)
    _new_directory(output_dir)
    arrays = {
        "base_ids.npy": bundle.ids,
        "base_vectors.npy": bundle.base_vectors,
        "calibration_queries.npy": bundle.calibration_queries,
        "measured_queries.npy": bundle.measured_queries,
    }
    for filename, array in arrays.items():
        with (output_dir / filename).open("wb") as handle:
            np.save(handle, array, allow_pickle=False)

    threshold_payload = {
        str(getattr(metric, "value", metric)): [value.as_dict() for value in values]
        for metric, values in thresholds.items()
    }
    _write_json(output_dir / "thresholds.json", threshold_payload)
    _write_json(
        output_dir / "boundary_fixtures.json",
        [fixture.as_dict() for fixture in fixtures],
    )

    artifact_names = tuple(arrays) + ("thresholds.json", "boundary_fixtures.json")
    artifact_entries = {
        name: _artifact_entry(output_dir / name) for name in artifact_names
    }
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "dataset": bundle.spec.as_dict(),
        "numpy_version": np.__version__,
        "generation": {
            "draw_order": "base vectors, then all queries; first queries are calibration",
            "source_dtype": "float64 standard_normal output",
            "stored_dtype": "little-endian float32 (<f4)",
            "license": "project-generated data",
        },
        "artifacts": artifact_entries,
    }
    manifest_path = output_dir / "generation_manifest.json"
    _write_json(manifest_path, manifest)
    sums = [
        f"{sha256_file(output_dir / name)}  {name}"
        for name in (*artifact_names, manifest_path.name)
    ]
    (output_dir / "SHA256SUMS").write_text("\n".join(sums) + "\n", encoding="utf-8")
    return manifest


def verify_dataset_artifacts(output_dir: Path) -> dict[str, Any]:
    """Verify every manifest and checksum-list entry before ingestion."""

    output_dir = Path(output_dir)
    manifest_path = output_dir / "generation_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for entry in manifest["artifacts"].values():
        path = output_dir / entry["file"]
        if not path.is_file() or path.stat().st_size != entry["bytes"]:
            raise ContractViolation(f"artifact size mismatch: {path.name}")
        actual = sha256_file(path)
        if actual != entry["sha256"]:
            raise ContractViolation(f"artifact SHA-256 mismatch: {path.name}")

    for line in (output_dir / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        expected, filename = line.split("  ", maxsplit=1)
        if sha256_file(output_dir / filename) != expected:
            raise ContractViolation(f"SHA256SUMS mismatch: {filename}")
    return manifest


def git_state(repository: Path) -> dict[str, object]:
    """Capture the exact source revision and dirty state without changing Git."""

    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    return {"commit": commit, "dirty": dirty}


def build_run_manifest(
    *,
    repository: Path,
    dataset_manifest: Mapping[str, object],
    dataset_manifest_sha256: str,
    schedule: Mapping[str, object],
    collection_prefix: str,
    timestamp: datetime | None = None,
) -> dict[str, object]:
    """Build the reproducibility manifest written with every future live run."""

    when = timestamp or datetime.now(timezone.utc)
    if when.tzinfo is None:
        raise ContractViolation("run timestamp must be timezone-aware")
    return {
        "experiment_id": "EXP-002",
        "contract_id": "EXP-001",
        "timestamp_utc": when.astimezone(timezone.utc).isoformat(),
        "git": git_state(repository),
        "environment": ENV001_PINS.as_dict(),
        "software": {
            "python": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "numpy": np.__version__,
            "pymilvus": version("pymilvus"),
        },
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "logical_cpu_count": os.cpu_count(),
        },
        "invocation": {
            "argv": list(sys.argv),
            "shell_escaped": shlex.join(sys.argv),
            "working_directory": str(Path.cwd()),
        },
        "dataset_manifest_sha256": dataset_manifest_sha256,
        "dataset": dataset_manifest,
        "collection_prefix": collection_prefix,
        "seed_derivation": {
            "method": "numpy.random.SeedSequence([20260801, stream_id]) -> uint64",
            "examples": {str(index): derive_seed(index) for index in range(3)},
        },
        "schedule": schedule,
    }


class JsonlSink:
    """Append raw records only after each search timing boundary closes."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        if self.path.exists():
            raise ContractViolation(f"refusing to overwrite raw output: {self.path}")
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def __call__(self, record: Mapping[str, object]) -> None:
        with self.path.open("ab") as handle:
            handle.write(canonical_json_bytes(dict(record)))
