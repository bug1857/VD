"""Immutable DATASET-003 generation and verification for LKG qualification.

Purpose:
    Produce a dedicated, role-disjoint ``lkg_qualification`` query population
    for the realized-workload last-known-good qualification contract, without
    touching DATASET-001 or DATASET-002 in any way.
Inputs:
    A deterministic PCG64 query specification, a checksum-verified DATASET-001
    artifact directory, and a DATASET-002 artifact directory verified only
    through ``verify_dataset002_query_identity`` -- its manifest/hash/role-
    disjointness contract, never its oracle-record semantics. Neither parent
    directory is modified.
Outputs:
    An atomically published, checksummed directory containing exactly one
    query role (``lkg_qualification``) and inherited provenance for both
    parent datasets.
Dependencies:
    NumPy plus existing DATASET-001/DATASET-002 artifact modules; never
    Milvus, PyMilvus, routing, policy, or actuation code.
Failure modes:
    Existing output, inherited-artifact drift, non-finite or cross-dataset
    role-overlapping IDs, or noncanonical JSON fails closed before the
    dataset can be consumed.
Scope:
    No precomputed oracle-record role is included. The live LKG shadow-audit
    path computes its oracle result at audit time via ``exact_range_search``
    against DATASET-001's base vectors directly (see
    ``MilvusActuationClient._oracle`` in ``milvus_actuation.py``); it does not
    read any dataset's ``oracle_records.jsonl``. DATASET-003 therefore only
    needs to register query IDs and vectors, and it deliberately verifies
    DATASET-002 only through the narrow ``verify_dataset002_query_identity``
    scope (manifest, hashes, deterministic arrays, role disjointness). It
    never verifies and never claims that DATASET-002's ``oracle_records.jsonl``
    is semantically correct against a fresh oracle recomputation; that is a
    separate, unresolved evidence-portability question tracked outside this
    module (see RESEARCH_PLAN.md's DATASET-002 entry and ARCHITECTURE.md's
    ADR-002 LKG qualification amendment).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping

import numpy as np
import numpy.typing as npt

from .artifacts import canonical_json_bytes, sha256_file, verify_dataset_artifacts
from .config import ContractViolation, EXP001_DATASET_SPEC
from .dataset002 import DATASET002_QUERY_IDENTITY_SCOPE, verify_dataset002_query_identity


DATASET003_SCHEMA_VERSION = 1
LKG_QUALIFICATION_ROLE = "lkg_qualification"
LKG_QUALIFICATION_ID_OFFSET = EXP001_DATASET_SPEC.base_count
DATASET003_ARTIFACTS = (
    "lkg_qualification_ids.npy",
    "lkg_qualification_queries.npy",
    "inherited_dataset001.json",
    "inherited_dataset002.json",
)
_MANIFEST_NAME = "dataset003_manifest.json"
_SUMS_NAME = "SHA256SUMS"
_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "dataset",
        "query_role",
        "generation",
        "inherited_dataset001",
        "inherited_dataset002",
        "artifacts",
    }
)
_INHERITED_DATASET001_FIELDS = frozenset(
    {
        "dataset_id",
        "version",
        "generation_manifest_sha256",
        "thresholds_sha256",
        "base_ids_sha256",
        "base_vectors_sha256",
    }
)
_INHERITED_DATASET002_FIELDS = frozenset(
    {
        "dataset_id",
        "version",
        "manifest_sha256",
        "routing_ids_sha256",
        "recall_audit_ids_sha256",
        "verification_scope",
    }
)
_GENERATION_CONTRACT = {
    "draw_order": "lkg_qualification queries only",
    "source_dtype": "float64 standard_normal output",
    "stored_dtype": "little-endian float32 (<f4)",
    "license": "project-generated data",
}


@dataclass(frozen=True, slots=True)
class Dataset003Spec:
    """Frozen generator contract for the query-only DATASET-003 workload."""

    dataset_id: str
    version: str
    seed: int
    dimensions: int
    lkg_qualification_query_count: int
    dtype: str
    distribution: str
    generator: str

    @property
    def query_count(self) -> int:
        """Return the total number of generated query vectors."""

        return self.lkg_qualification_query_count

    def as_dict(self) -> dict[str, object]:
        """Return a canonical JSON-ready specification."""

        return asdict(self)


DATASET003_SPEC = Dataset003Spec(
    dataset_id="DATASET-003",
    version="DATASET-003-v1",
    seed=20260806,
    dimensions=128,
    lkg_qualification_query_count=2_400,
    dtype="<f4",
    distribution="independent standard normal",
    generator="numpy.random.Generator(numpy.random.PCG64(seed))",
)


@dataclass(frozen=True, slots=True)
class Dataset003Bundle:
    """Generated DATASET-003 vectors before immutable artifact serialization."""

    lkg_qualification_ids: npt.NDArray[np.int64]
    lkg_qualification_queries: npt.NDArray[np.float32]
    spec: Dataset003Spec


class _DuplicateJsonField(ValueError):
    """Internal marker for a JSON object containing duplicate keys."""


def _no_duplicate_json_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_no_duplicate_json_fields,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")),
        )
    except (OSError, UnicodeError, json.JSONDecodeError, _DuplicateJsonField, ValueError) as exc:
        raise ContractViolation(f"invalid JSON artifact: {path.name}") from exc
    if not isinstance(payload, dict):
        raise ContractViolation(f"JSON artifact must be an object: {path.name}")
    return payload


def _validate_spec(spec: Dataset003Spec) -> None:
    if not isinstance(spec, Dataset003Spec):
        raise TypeError("spec must be a Dataset003Spec")
    values = (spec.seed, spec.dimensions, spec.lkg_qualification_query_count)
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in values):
        raise ContractViolation("dataset003 seed, dimensions, and count must be positive integers")
    if spec.dtype != "<f4":
        raise ContractViolation("dataset003 dtype must be little-endian float32 (<f4)")
    if spec.dataset_id != "DATASET-003":
        raise ContractViolation("dataset003 dataset_id must equal DATASET-003")
    if not all(isinstance(value, str) and value for value in (spec.dataset_id, spec.version, spec.distribution, spec.generator)):
        raise ContractViolation("dataset003 textual specification fields must be non-empty")


def _spec_from_mapping(value: object) -> Dataset003Spec:
    if not isinstance(value, Mapping) or frozenset(value) != frozenset(Dataset003Spec.__dataclass_fields__):
        raise ContractViolation("dataset003 manifest specification fields are invalid")
    try:
        spec = Dataset003Spec(**dict(value))
    except TypeError as exc:
        raise ContractViolation("dataset003 manifest specification is invalid") from exc
    _validate_spec(spec)
    return spec


def generate_dataset003(spec: Dataset003Spec = DATASET003_SPEC) -> Dataset003Bundle:
    """Generate deterministic ``lkg_qualification`` query vectors and IDs."""

    _validate_spec(spec)
    generator = np.random.Generator(np.random.PCG64(spec.seed))
    queries = generator.standard_normal((spec.query_count, spec.dimensions)).astype(
        "<f4", copy=False
    )
    if not np.all(np.isfinite(queries)):
        raise ContractViolation("generated DATASET-003 vectors are non-finite")
    ids = np.arange(
        LKG_QUALIFICATION_ID_OFFSET,
        LKG_QUALIFICATION_ID_OFFSET + spec.query_count,
        dtype=np.int64,
    )
    return Dataset003Bundle(
        lkg_qualification_ids=ids,
        lkg_qualification_queries=np.ascontiguousarray(queries),
        spec=spec,
    )


def _artifact_entry(path: Path) -> dict[str, object]:
    return {"file": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)}


def _write_bytes_durable(path: Path, payload: bytes) -> None:
    with path.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _write_array_durable(path: Path, values: npt.NDArray[Any]) -> None:
    with path.open("wb") as handle:
        np.save(handle, values, allow_pickle=False)
        handle.flush()
        os.fsync(handle.fileno())


def _directory_fsync(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _new_atomic_directory(target: Path) -> Path:
    if target.exists():
        raise ContractViolation(f"refusing to overwrite existing artifact path: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))


def _publish_directory(temporary: Path, target: Path) -> None:
    if target.exists():
        raise ContractViolation(f"refusing to overwrite existing artifact path: {target}")
    _directory_fsync(temporary)
    os.replace(temporary, target)
    _directory_fsync(target.parent)


def _load_dataset001_base_ids(dataset001_dir: Path) -> tuple[dict[str, Any], npt.NDArray[np.int64]]:
    manifest = verify_dataset_artifacts(dataset001_dir)
    dataset = manifest.get("dataset")
    if not isinstance(dataset, Mapping) or dataset.get("dataset_id") != "DATASET-001":
        raise ContractViolation("inherited dataset must be DATASET-001")
    base_ids = np.load(dataset001_dir / "base_ids.npy", allow_pickle=False)
    if base_ids.ndim != 1 or base_ids.dtype.kind not in "iu" or len(np.unique(base_ids)) != base_ids.size:
        raise ContractViolation("inherited DATASET-001 base_ids are invalid")
    inherited = {
        "dataset_id": dataset["dataset_id"],
        "version": dataset["version"],
        "generation_manifest_sha256": sha256_file(dataset001_dir / "generation_manifest.json"),
        "thresholds_sha256": sha256_file(dataset001_dir / "thresholds.json"),
        "base_ids_sha256": sha256_file(dataset001_dir / "base_ids.npy"),
        "base_vectors_sha256": sha256_file(dataset001_dir / "base_vectors.npy"),
    }
    return inherited, np.asarray(base_ids, dtype=np.int64)


def _load_dataset002_ids(
    dataset002_dir: Path, *, dataset001_dir: Path
) -> tuple[dict[str, Any], npt.NDArray[np.int64], npt.NDArray[np.int64]]:
    """Bind DATASET-003 to DATASET-002's query identity only.

    Uses ``verify_dataset002_query_identity`` -- never the full, oracle-
    inclusive ``verify_dataset002_artifacts`` -- so DATASET-003 depends only
    on DATASET-002's manifest/hash/role-disjointness contract and never
    claims (or requires) that DATASET-002's ``oracle_records.jsonl`` is
    semantically correct. The result's ``verification_scope`` is asserted
    here as a defense-in-depth check: a caller cannot substitute a
    differently-scoped or hand-built result for a real verifier call, because
    this function always performs that call itself and always reads the
    scope tag straight from its return value.
    """

    result = verify_dataset002_query_identity(dataset002_dir, dataset001_dir=dataset001_dir)
    if result.verification_scope != DATASET002_QUERY_IDENTITY_SCOPE:
        raise ContractViolation("DATASET-002 query-identity verification scope is invalid")
    if result.dataset_id != "DATASET-002":
        raise ContractViolation("inherited dataset must be DATASET-002")
    inherited = {
        "dataset_id": result.dataset_id,
        "version": result.version,
        "manifest_sha256": result.manifest_sha256,
        "routing_ids_sha256": result.routing_ids_sha256,
        "recall_audit_ids_sha256": result.recall_audit_ids_sha256,
        "verification_scope": result.verification_scope,
    }
    return (
        inherited,
        np.asarray(result.routing_ids, dtype=np.int64),
        np.asarray(result.recall_audit_ids, dtype=np.int64),
    )


def _validate_no_cross_dataset_overlap(
    lkg_qualification_ids: npt.NDArray[np.int64],
    *,
    base_ids: npt.NDArray[np.int64],
    routing_ids: npt.NDArray[np.int64],
    recall_audit_ids: npt.NDArray[np.int64],
) -> None:
    lkg_set = set(int(value) for value in lkg_qualification_ids)
    if len(lkg_set) != lkg_qualification_ids.size:
        raise ContractViolation("DATASET-003 lkg_qualification_ids contains duplicates")
    if lkg_set.intersection(int(value) for value in base_ids):
        raise ContractViolation("DATASET-003 IDs overlap DATASET-001 base_ids")
    if lkg_set.intersection(int(value) for value in routing_ids):
        raise ContractViolation("DATASET-003 IDs overlap DATASET-002 routing role")
    if lkg_set.intersection(int(value) for value in recall_audit_ids):
        raise ContractViolation("DATASET-003 IDs overlap DATASET-002 recall_audit role")


def _validate_bundle(bundle: Dataset003Bundle) -> None:
    if not isinstance(bundle, Dataset003Bundle):
        raise TypeError("bundle must be a Dataset003Bundle")
    _validate_spec(bundle.spec)
    ids = bundle.lkg_qualification_ids
    queries = bundle.lkg_qualification_queries
    if (
        ids.ndim != 1
        or ids.dtype.kind not in "iu"
        or queries.shape != (bundle.spec.lkg_qualification_query_count, bundle.spec.dimensions)
        or queries.dtype.str != "<f4"
        or not np.all(np.isfinite(queries))
        or len(np.unique(ids)) != ids.size
    ):
        raise ContractViolation("DATASET-003 bundle arrays are invalid")
    expected = generate_dataset003(bundle.spec)
    if not (
        np.array_equal(bundle.lkg_qualification_ids, expected.lkg_qualification_ids)
        and np.array_equal(bundle.lkg_qualification_queries, expected.lkg_qualification_queries)
    ):
        raise ContractViolation("DATASET-003 bundle disagrees with the deterministic generator")


def write_dataset003_artifacts(
    output_dir: str | os.PathLike[str],
    bundle: Dataset003Bundle,
    *,
    dataset001_dir: str | os.PathLike[str],
    dataset002_dir: str | os.PathLike[str],
) -> dict[str, Any]:
    """Atomically write one immutable DATASET-003 directory.

    DATASET-001 and DATASET-002 are opened strictly read-only, through their
    own existing verifiers, and are never written to or regenerated.
    """

    _validate_bundle(bundle)
    dataset001_path = Path(dataset001_dir)
    dataset002_path = Path(dataset002_dir)
    inherited_dataset001, base_ids = _load_dataset001_base_ids(dataset001_path)
    inherited_dataset002, routing_ids, recall_audit_ids = _load_dataset002_ids(
        dataset002_path, dataset001_dir=dataset001_path
    )
    _validate_no_cross_dataset_overlap(
        bundle.lkg_qualification_ids,
        base_ids=base_ids,
        routing_ids=routing_ids,
        recall_audit_ids=recall_audit_ids,
    )
    target = Path(output_dir)
    temporary = _new_atomic_directory(target)
    try:
        arrays = {
            "lkg_qualification_ids.npy": np.asarray(bundle.lkg_qualification_ids, dtype="<i8"),
            "lkg_qualification_queries.npy": np.asarray(bundle.lkg_qualification_queries, dtype="<f4"),
        }
        for filename, values in arrays.items():
            _write_array_durable(temporary / filename, values)
        _write_bytes_durable(
            temporary / "inherited_dataset001.json", canonical_json_bytes(inherited_dataset001)
        )
        _write_bytes_durable(
            temporary / "inherited_dataset002.json", canonical_json_bytes(inherited_dataset002)
        )
        manifest: dict[str, Any] = {
            "schema_version": DATASET003_SCHEMA_VERSION,
            "dataset": bundle.spec.as_dict(),
            "query_role": LKG_QUALIFICATION_ROLE,
            "generation": dict(_GENERATION_CONTRACT),
            "inherited_dataset001": inherited_dataset001,
            "inherited_dataset002": inherited_dataset002,
            "artifacts": {
                filename: _artifact_entry(temporary / filename)
                for filename in DATASET003_ARTIFACTS
            },
        }
        _write_bytes_durable(temporary / _MANIFEST_NAME, canonical_json_bytes(manifest))
        sums = "".join(
            f"{sha256_file(temporary / filename)}  {filename}\n"
            for filename in (*DATASET003_ARTIFACTS, _MANIFEST_NAME)
        )
        _write_bytes_durable(temporary / _SUMS_NAME, sums.encode("utf-8"))
        _publish_directory(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return manifest


def _verify_sums(output_dir: Path) -> None:
    path = output_dir / _SUMS_NAME
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ContractViolation("DATASET-003 checksum inventory is unreadable") from exc
    expected_files = set((*DATASET003_ARTIFACTS, _MANIFEST_NAME))
    entries: dict[str, str] = {}
    for line in lines:
        try:
            digest, filename = line.split("  ", maxsplit=1)
        except ValueError as exc:
            raise ContractViolation("DATASET-003 checksum inventory is malformed") from exc
        if filename in entries or filename not in expected_files or len(digest) != 64:
            raise ContractViolation("DATASET-003 checksum inventory is invalid")
        entries[filename] = digest
    if set(entries) != expected_files:
        raise ContractViolation("DATASET-003 checksum inventory is incomplete")
    for filename, digest in entries.items():
        if sha256_file(output_dir / filename) != digest:
            raise ContractViolation(f"DATASET-003 SHA256SUMS mismatch: {filename}")


def _load_output_arrays(
    output_dir: Path, spec: Dataset003Spec
) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.float32]]:
    try:
        ids = np.load(output_dir / "lkg_qualification_ids.npy", allow_pickle=False)
        queries = np.load(output_dir / "lkg_qualification_queries.npy", allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise ContractViolation("DATASET-003 arrays are unreadable") from exc
    if (
        ids.ndim != 1
        or ids.shape != (spec.lkg_qualification_query_count,)
        or ids.dtype.str != "<i8"
        or queries.shape != (spec.lkg_qualification_query_count, spec.dimensions)
        or queries.dtype.str != "<f4"
        or not np.all(np.isfinite(queries))
        or len(np.unique(ids)) != len(ids)
    ):
        raise ContractViolation("DATASET-003 arrays violate the schema")
    return np.asarray(ids, dtype=np.int64), np.asarray(queries, dtype="<f4")


def verify_dataset003_artifacts(
    output_dir: str | os.PathLike[str],
    *,
    dataset001_dir: str | os.PathLike[str],
    dataset002_dir: str | os.PathLike[str],
) -> dict[str, Any]:
    """Independently revalidate every DATASET-003 byte and cross-dataset ID role."""

    output = Path(output_dir)
    manifest = _read_json(output / _MANIFEST_NAME)
    if frozenset(manifest) != _MANIFEST_FIELDS or manifest.get("schema_version") != DATASET003_SCHEMA_VERSION:
        raise ContractViolation("DATASET-003 manifest schema is invalid")
    if manifest.get("generation") != _GENERATION_CONTRACT:
        raise ContractViolation("DATASET-003 generation contract is invalid")
    if manifest.get("query_role") != LKG_QUALIFICATION_ROLE:
        raise ContractViolation("DATASET-003 query_role is invalid")
    spec = _spec_from_mapping(manifest["dataset"])
    artifacts = manifest["artifacts"]
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(DATASET003_ARTIFACTS):
        raise ContractViolation("DATASET-003 manifest artifacts are invalid")
    for filename in DATASET003_ARTIFACTS:
        entry = artifacts[filename]
        if not isinstance(entry, Mapping) or set(entry) != {"file", "bytes", "sha256"}:
            raise ContractViolation("DATASET-003 artifact entry schema is invalid")
        path = output / filename
        if entry["file"] != filename or not path.is_file() or entry["bytes"] != path.stat().st_size or entry["sha256"] != sha256_file(path):
            raise ContractViolation(f"DATASET-003 artifact checksum mismatch: {filename}")
    _verify_sums(output)
    expected_files = set((*DATASET003_ARTIFACTS, _MANIFEST_NAME, _SUMS_NAME))
    try:
        actual_files = {path.name for path in output.iterdir()}
    except OSError as exc:
        raise ContractViolation("DATASET-003 artifact directory is unreadable") from exc
    if actual_files != expected_files:
        raise ContractViolation("DATASET-003 artifact directory contains unexpected files")

    dataset001_path = Path(dataset001_dir)
    dataset002_path = Path(dataset002_dir)
    inherited_dataset001, base_ids = _load_dataset001_base_ids(dataset001_path)
    inherited_dataset002, routing_ids, recall_audit_ids = _load_dataset002_ids(
        dataset002_path, dataset001_dir=dataset001_path
    )
    if (
        manifest["inherited_dataset001"] != inherited_dataset001
        or _read_json(output / "inherited_dataset001.json") != inherited_dataset001
    ):
        raise ContractViolation("DATASET-003 inherited DATASET-001 identity mismatch")
    if (
        manifest["inherited_dataset002"] != inherited_dataset002
        or _read_json(output / "inherited_dataset002.json") != inherited_dataset002
    ):
        raise ContractViolation("DATASET-003 inherited DATASET-002 identity mismatch")
    if frozenset(inherited_dataset001) != _INHERITED_DATASET001_FIELDS:
        raise ContractViolation("DATASET-003 inherited DATASET-001 schema is invalid")
    if frozenset(inherited_dataset002) != _INHERITED_DATASET002_FIELDS:
        raise ContractViolation("DATASET-003 inherited DATASET-002 schema is invalid")

    ids, queries = _load_output_arrays(output, spec)
    expected = generate_dataset003(spec)
    if not (
        np.array_equal(ids, expected.lkg_qualification_ids)
        and np.array_equal(queries, expected.lkg_qualification_queries)
    ):
        raise ContractViolation("DATASET-003 arrays disagree with the deterministic generator")
    _validate_no_cross_dataset_overlap(
        ids, base_ids=base_ids, routing_ids=routing_ids, recall_audit_ids=recall_audit_ids
    )
    return manifest


__all__ = [
    "DATASET003_ARTIFACTS",
    "DATASET003_SCHEMA_VERSION",
    "DATASET003_SPEC",
    "Dataset003Bundle",
    "Dataset003Spec",
    "LKG_QUALIFICATION_ID_OFFSET",
    "LKG_QUALIFICATION_ROLE",
    "generate_dataset003",
    "verify_dataset003_artifacts",
    "write_dataset003_artifacts",
]
