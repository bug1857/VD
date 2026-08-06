"""Immutable DATASET-002 generation and verification for EXP-009 Stage 1.

Purpose:
    Produce role-disjoint routing and background recall-audit query vectors while
    preserving DATASET-001's base vectors and calibrated thresholds unchanged.
Inputs:
    A deterministic PCG64 query specification and a checksum-verified
    DATASET-001 artifact directory.
Outputs:
    An atomically published, checksummed directory containing both query roles,
    inherited provenance, and exact float64 oracle records for every frozen
    metric/threshold configuration.
Dependencies:
    NumPy plus existing DATASET-001 artifact and exact-oracle modules; never
    Milvus, PyMilvus, routing, policy, or actuation code.
Failure modes:
    Existing output, inherited-artifact drift, non-finite or role-overlapping
    data, noncanonical JSON, checksum mismatch, or oracle disagreement fails
    closed before the dataset can be consumed.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping

import numpy as np
import numpy.typing as npt

from .artifacts import canonical_json_bytes, sha256_file, verify_dataset_artifacts
from .config import ContractViolation, Metric, RESULT_LIMIT, THRESHOLD_LABELS
from .oracle import OracleResult, exact_range_search


DATASET002_SCHEMA_VERSION = 1
DATASET002_ARTIFACTS = (
    "routing_ids.npy",
    "routing_queries.npy",
    "recall_audit_ids.npy",
    "recall_audit_queries.npy",
    "inherited_dataset001.json",
    "oracle_records.jsonl",
)
_MANIFEST_NAME = "dataset002_manifest.json"
_SUMS_NAME = "SHA256SUMS"
_MANIFEST_FIELDS = frozenset(
    {"schema_version", "dataset", "generation", "inherited_dataset001", "artifacts"}
)
_INHERITED_FIELDS = frozenset(
    {
        "dataset_id",
        "version",
        "generation_manifest_sha256",
        "thresholds_sha256",
        "base_ids_sha256",
        "base_vectors_sha256",
    }
)
_ORACLE_FIELDS = frozenset(
    {
        "query_id",
        "role",
        "metric",
        "threshold_label",
        "radius",
        "range_filter",
        "limit",
        "full_count",
        "capped",
        "hits",
    }
)
_HIT_FIELDS = frozenset({"id", "score"})
_GENERATION_CONTRACT = {
    "draw_order": "routing queries, then recall-audit queries",
    "source_dtype": "float64 standard_normal output",
    "stored_dtype": "little-endian float32 (<f4)",
    "license": "project-generated data",
}


@dataclass(frozen=True, slots=True)
class Dataset002Spec:
    """Frozen generator contract for the query-only DATASET-002 workload."""

    dataset_id: str
    version: str
    seed: int
    dimensions: int
    routing_query_count: int
    recall_audit_query_count: int
    dtype: str
    distribution: str
    generator: str

    @property
    def query_count(self) -> int:
        """Return the total number of generated query vectors."""

        return self.routing_query_count + self.recall_audit_query_count

    def as_dict(self) -> dict[str, object]:
        """Return a canonical JSON-ready specification."""

        return asdict(self)


DATASET002_SPEC = Dataset002Spec(
    dataset_id="DATASET-002",
    version="DATASET-002-v1",
    seed=20260809,
    dimensions=128,
    routing_query_count=600,
    recall_audit_query_count=1_200,
    dtype="<f4",
    distribution="independent standard normal",
    generator="numpy.random.Generator(numpy.random.PCG64(seed))",
)


@dataclass(frozen=True, slots=True)
class Dataset002Bundle:
    """Generated DATASET-002 vectors before immutable artifact serialization."""

    routing_ids: npt.NDArray[np.int64]
    routing_queries: npt.NDArray[np.float32]
    recall_audit_ids: npt.NDArray[np.int64]
    recall_audit_queries: npt.NDArray[np.float32]
    spec: Dataset002Spec


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


def _validate_spec(spec: Dataset002Spec) -> None:
    if not isinstance(spec, Dataset002Spec):
        raise TypeError("spec must be a Dataset002Spec")
    values = (
        spec.seed,
        spec.dimensions,
        spec.routing_query_count,
        spec.recall_audit_query_count,
    )
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in values):
        raise ContractViolation("dataset002 seed, dimensions, and counts must be positive integers")
    if spec.dtype != "<f4":
        raise ContractViolation("dataset002 dtype must be little-endian float32 (<f4)")
    if spec.dataset_id != "DATASET-002":
        raise ContractViolation("dataset002 dataset_id must equal DATASET-002")
    if not all(isinstance(value, str) and value for value in (spec.dataset_id, spec.version, spec.distribution, spec.generator)):
        raise ContractViolation("dataset002 textual specification fields must be non-empty")


def _spec_from_mapping(value: object) -> Dataset002Spec:
    if not isinstance(value, Mapping) or frozenset(value) != frozenset(Dataset002Spec.__dataclass_fields__):
        raise ContractViolation("dataset002 manifest specification fields are invalid")
    try:
        spec = Dataset002Spec(**dict(value))
    except TypeError as exc:
        raise ContractViolation("dataset002 manifest specification is invalid") from exc
    _validate_spec(spec)
    return spec


def generate_dataset002(spec: Dataset002Spec = DATASET002_SPEC) -> Dataset002Bundle:
    """Generate deterministic, role-disjoint standard-normal query vectors."""

    _validate_spec(spec)
    generator = np.random.Generator(np.random.PCG64(spec.seed))
    queries = generator.standard_normal((spec.query_count, spec.dimensions)).astype(
        "<f4", copy=False
    )
    if not np.all(np.isfinite(queries)):
        raise ContractViolation("generated DATASET-002 vectors are non-finite")
    split = spec.routing_query_count
    return Dataset002Bundle(
        routing_ids=np.arange(split, dtype=np.int64),
        routing_queries=np.ascontiguousarray(queries[:split]),
        recall_audit_ids=np.arange(split, spec.query_count, dtype=np.int64),
        recall_audit_queries=np.ascontiguousarray(queries[split:]),
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


def _load_dataset001(dataset001_dir: Path) -> tuple[dict[str, Any], npt.NDArray[np.int64], npt.NDArray[np.float32], dict[Metric, tuple[tuple[str, float], ...]], dict[str, str]]:
    manifest = verify_dataset_artifacts(dataset001_dir)
    dataset = manifest.get("dataset")
    if not isinstance(dataset, Mapping) or dataset.get("dataset_id") != "DATASET-001":
        raise ContractViolation("inherited dataset must be DATASET-001")
    base_ids = np.load(dataset001_dir / "base_ids.npy", allow_pickle=False)
    base_vectors = np.load(dataset001_dir / "base_vectors.npy", allow_pickle=False)
    if (
        base_ids.ndim != 1
        or base_vectors.ndim != 2
        or base_ids.shape[0] != base_vectors.shape[0]
        or base_ids.dtype.kind not in "iu"
        or base_vectors.dtype.str != "<f4"
        or not np.all(np.isfinite(base_vectors))
        or len(np.unique(base_ids)) != base_ids.size
    ):
        raise ContractViolation("inherited DATASET-001 base arrays are invalid")
    thresholds_raw = _read_json(dataset001_dir / "thresholds.json")
    thresholds: dict[Metric, tuple[tuple[str, float], ...]] = {}
    for metric in Metric:
        values = thresholds_raw.get(metric.value)
        if not isinstance(values, list) or len(values) != len(THRESHOLD_LABELS):
            raise ContractViolation("inherited DATASET-001 thresholds are invalid")
        parsed: list[tuple[str, float]] = []
        for expected_label, entry in zip(THRESHOLD_LABELS, values, strict=True):
            if not isinstance(entry, Mapping) or entry.get("label") != expected_label:
                raise ContractViolation("inherited DATASET-001 threshold labels are invalid")
            radius = entry.get("radius")
            if isinstance(radius, bool) or not isinstance(radius, (int, float)) or not math.isfinite(float(radius)):
                raise ContractViolation("inherited DATASET-001 threshold radius is invalid")
            parsed.append((expected_label, float(radius)))
        thresholds[metric] = tuple(parsed)
    inherited = {
        "dataset_id": dataset["dataset_id"],
        "version": dataset["version"],
        "generation_manifest_sha256": sha256_file(dataset001_dir / "generation_manifest.json"),
        "thresholds_sha256": sha256_file(dataset001_dir / "thresholds.json"),
        "base_ids_sha256": sha256_file(dataset001_dir / "base_ids.npy"),
        "base_vectors_sha256": sha256_file(dataset001_dir / "base_vectors.npy"),
    }
    return (
        dict(manifest),
        np.asarray(base_ids, dtype=np.int64),
        np.asarray(base_vectors, dtype="<f4"),
        thresholds,
        inherited,
    )


def _oracle_record(
    *,
    query_id: int,
    role: str,
    query: npt.NDArray[np.float32],
    metric: Metric,
    threshold_label: str,
    radius: float,
    base_ids: npt.NDArray[np.int64],
    base_vectors: npt.NDArray[np.float32],
) -> dict[str, object]:
    range_filter = 0.0 if metric is Metric.L2 else 1.0
    result: OracleResult = exact_range_search(
        base_vectors,
        base_ids,
        query,
        metric,
        radius=radius,
        range_filter=range_filter,
        limit=RESULT_LIMIT,
    )
    return {
        "query_id": query_id,
        "role": role,
        "metric": metric.value,
        "threshold_label": threshold_label,
        "radius": radius,
        "range_filter": range_filter,
        "limit": RESULT_LIMIT,
        "full_count": result.full_count,
        "capped": result.capped,
        "hits": [{"id": hit.id, "score": hit.score} for hit in result.hits],
    }


def _oracle_records(
    bundle: Dataset002Bundle,
    *,
    base_ids: npt.NDArray[np.int64],
    base_vectors: npt.NDArray[np.float32],
    thresholds: Mapping[Metric, tuple[tuple[str, float], ...]],
) -> bytes:
    records: list[bytes] = []
    role_rows = (
        ("routing", bundle.routing_ids, bundle.routing_queries),
        ("recall_audit", bundle.recall_audit_ids, bundle.recall_audit_queries),
    )
    for role, ids, queries in role_rows:
        for query_id, query in zip(ids, queries, strict=True):
            for metric in Metric:
                for threshold_label, radius in thresholds[metric]:
                    records.append(
                        canonical_json_bytes(
                            _oracle_record(
                                query_id=int(query_id),
                                role=role,
                                query=query,
                                metric=metric,
                                threshold_label=threshold_label,
                                radius=radius,
                                base_ids=base_ids,
                                base_vectors=base_vectors,
                            )
                        )
                    )
    return b"".join(records)


def _validate_bundle(bundle: Dataset002Bundle) -> None:
    if not isinstance(bundle, Dataset002Bundle):
        raise TypeError("bundle must be a Dataset002Bundle")
    _validate_spec(bundle.spec)
    expected = (
        (bundle.routing_ids, bundle.routing_queries, bundle.spec.routing_query_count),
        (bundle.recall_audit_ids, bundle.recall_audit_queries, bundle.spec.recall_audit_query_count),
    )
    for ids, queries, count in expected:
        if (
            ids.ndim != 1
            or ids.dtype.kind not in "iu"
            or queries.shape != (count, bundle.spec.dimensions)
            or queries.dtype.str != "<f4"
            or not np.all(np.isfinite(queries))
            or len(np.unique(ids)) != ids.size
        ):
            raise ContractViolation("DATASET-002 bundle arrays are invalid")
    if set(int(value) for value in bundle.routing_ids).intersection(int(value) for value in bundle.recall_audit_ids):
        raise ContractViolation("DATASET-002 bundle has routing/recall-audit role overlap")
    expected = generate_dataset002(bundle.spec)
    if not (
        np.array_equal(bundle.routing_ids, expected.routing_ids)
        and np.array_equal(bundle.routing_queries, expected.routing_queries)
        and np.array_equal(bundle.recall_audit_ids, expected.recall_audit_ids)
        and np.array_equal(bundle.recall_audit_queries, expected.recall_audit_queries)
    ):
        raise ContractViolation("DATASET-002 bundle disagrees with the deterministic generator")


def write_dataset002_artifacts(
    output_dir: str | os.PathLike[str],
    bundle: Dataset002Bundle,
    *,
    dataset001_dir: str | os.PathLike[str],
) -> dict[str, Any]:
    """Atomically write one immutable, exact-oracle DATASET-002 directory."""

    _validate_bundle(bundle)
    dataset001_path = Path(dataset001_dir)
    _, base_ids, base_vectors, thresholds, inherited = _load_dataset001(dataset001_path)
    if base_vectors.shape[1] != bundle.spec.dimensions:
        raise ContractViolation("DATASET-002 dimensions do not match inherited DATASET-001")
    target = Path(output_dir)
    temporary = _new_atomic_directory(target)
    try:
        arrays = {
            "routing_ids.npy": np.asarray(bundle.routing_ids, dtype="<i8"),
            "routing_queries.npy": np.asarray(bundle.routing_queries, dtype="<f4"),
            "recall_audit_ids.npy": np.asarray(bundle.recall_audit_ids, dtype="<i8"),
            "recall_audit_queries.npy": np.asarray(bundle.recall_audit_queries, dtype="<f4"),
        }
        for filename, values in arrays.items():
            _write_array_durable(temporary / filename, values)
        _write_bytes_durable(
            temporary / "inherited_dataset001.json", canonical_json_bytes(inherited)
        )
        _write_bytes_durable(
            temporary / "oracle_records.jsonl",
            _oracle_records(
                bundle,
                base_ids=base_ids,
                base_vectors=base_vectors,
                thresholds=thresholds,
            ),
        )
        manifest: dict[str, Any] = {
            "schema_version": DATASET002_SCHEMA_VERSION,
            "dataset": bundle.spec.as_dict(),
            "generation": dict(_GENERATION_CONTRACT),
            "inherited_dataset001": inherited,
            "artifacts": {
                filename: _artifact_entry(temporary / filename)
                for filename in DATASET002_ARTIFACTS
            },
        }
        _write_bytes_durable(temporary / _MANIFEST_NAME, canonical_json_bytes(manifest))
        sums = "".join(
            f"{sha256_file(temporary / filename)}  {filename}\n"
            for filename in (*DATASET002_ARTIFACTS, _MANIFEST_NAME)
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
        raise ContractViolation("DATASET-002 checksum inventory is unreadable") from exc
    expected_files = set((*DATASET002_ARTIFACTS, _MANIFEST_NAME))
    entries: dict[str, str] = {}
    for line in lines:
        try:
            digest, filename = line.split("  ", maxsplit=1)
        except ValueError as exc:
            raise ContractViolation("DATASET-002 checksum inventory is malformed") from exc
        if filename in entries or filename not in expected_files or len(digest) != 64:
            raise ContractViolation("DATASET-002 checksum inventory is invalid")
        entries[filename] = digest
    if set(entries) != expected_files:
        raise ContractViolation("DATASET-002 checksum inventory is incomplete")
    for filename, digest in entries.items():
        if sha256_file(output_dir / filename) != digest:
            raise ContractViolation(f"DATASET-002 SHA256SUMS mismatch: {filename}")


def _load_output_arrays(output_dir: Path, spec: Dataset002Spec) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.float32], npt.NDArray[np.int64], npt.NDArray[np.float32]]:
    try:
        routing_ids = np.load(output_dir / "routing_ids.npy", allow_pickle=False)
        routing_queries = np.load(output_dir / "routing_queries.npy", allow_pickle=False)
        audit_ids = np.load(output_dir / "recall_audit_ids.npy", allow_pickle=False)
        audit_queries = np.load(output_dir / "recall_audit_queries.npy", allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise ContractViolation("DATASET-002 arrays are unreadable") from exc
    values = (
        (routing_ids, routing_queries, spec.routing_query_count),
        (audit_ids, audit_queries, spec.recall_audit_query_count),
    )
    for ids, queries, count in values:
        if (
            ids.ndim != 1
            or ids.shape != (count,)
            or ids.dtype.str != "<i8"
            or queries.shape != (count, spec.dimensions)
            or queries.dtype.str != "<f4"
            or not np.all(np.isfinite(queries))
            or len(np.unique(ids)) != len(ids)
        ):
            raise ContractViolation("DATASET-002 arrays violate the schema")
    routing_set = set(int(value) for value in routing_ids)
    audit_set = set(int(value) for value in audit_ids)
    if routing_set.intersection(audit_set):
        raise ContractViolation("DATASET-002 routing/recall-audit role overlap")
    return (
        np.asarray(routing_ids, dtype=np.int64),
        np.asarray(routing_queries, dtype="<f4"),
        np.asarray(audit_ids, dtype=np.int64),
        np.asarray(audit_queries, dtype="<f4"),
    )


def _verify_oracle_records(
    output_dir: Path,
    *,
    routing_ids: npt.NDArray[np.int64],
    routing_queries: npt.NDArray[np.float32],
    audit_ids: npt.NDArray[np.int64],
    audit_queries: npt.NDArray[np.float32],
    base_ids: npt.NDArray[np.int64],
    base_vectors: npt.NDArray[np.float32],
    thresholds: Mapping[Metric, tuple[tuple[str, float], ...]],
) -> None:
    query_map = {
        int(query_id): ("routing", query)
        for query_id, query in zip(routing_ids, routing_queries, strict=True)
    }
    query_map.update(
        {
            int(query_id): ("recall_audit", query)
            for query_id, query in zip(audit_ids, audit_queries, strict=True)
        }
    )
    expected_count = len(query_map) * len(Metric) * len(THRESHOLD_LABELS)
    seen: set[tuple[int, str, str]] = set()
    try:
        lines = (output_dir / "oracle_records.jsonl").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ContractViolation("DATASET-002 oracle records are unreadable") from exc
    if len(lines) != expected_count:
        raise ContractViolation("DATASET-002 oracle record count is invalid")
    for line in lines:
        try:
            record = json.loads(line, object_pairs_hook=_no_duplicate_json_fields)
        except (json.JSONDecodeError, _DuplicateJsonField) as exc:
            raise ContractViolation("DATASET-002 oracle record JSON is invalid") from exc
        if not isinstance(record, dict) or frozenset(record) != _ORACLE_FIELDS:
            raise ContractViolation("DATASET-002 oracle record schema is invalid")
        query_id = record["query_id"]
        role = record["role"]
        metric_value = record["metric"]
        label = record["threshold_label"]
        if isinstance(query_id, bool) or not isinstance(query_id, int) or query_id not in query_map:
            raise ContractViolation("DATASET-002 oracle record query ID is invalid")
        if role != query_map[query_id][0]:
            raise ContractViolation("DATASET-002 oracle record role is invalid")
        try:
            metric = Metric(metric_value)
        except (TypeError, ValueError) as exc:
            raise ContractViolation("DATASET-002 oracle record metric is invalid") from exc
        threshold_map = dict(thresholds[metric])
        if label not in threshold_map:
            raise ContractViolation("DATASET-002 oracle record threshold is invalid")
        key = (query_id, metric.value, label)
        if key in seen:
            raise ContractViolation("DATASET-002 oracle records contain duplicates")
        seen.add(key)
        expected = _oracle_record(
            query_id=query_id,
            role=role,
            query=query_map[query_id][1],
            metric=metric,
            threshold_label=label,
            radius=threshold_map[label],
            base_ids=base_ids,
            base_vectors=base_vectors,
        )
        if record != expected:
            raise ContractViolation("DATASET-002 oracle record disagrees with exact oracle")
    if len(seen) != expected_count:
        raise ContractViolation("DATASET-002 oracle records are incomplete")


DATASET002_QUERY_IDENTITY_SCOPE = "QUERY_IDENTITY_ONLY"


@dataclass(frozen=True, slots=True)
class Dataset002QueryIdentityResult:
    """Structural/query-identity verification only.

    This result does NOT verify, imply, or depend on ``oracle_records.jsonl``
    being semantically correct. Its ``verification_scope`` is always
    ``DATASET002_QUERY_IDENTITY_SCOPE`` so a caller can never mistake it for a
    full :func:`verify_dataset002_artifacts` result.
    """

    verification_scope: str
    dataset_id: str
    version: str
    manifest_sha256: str
    routing_ids_sha256: str
    recall_audit_ids_sha256: str
    routing_ids: tuple[int, ...]
    recall_audit_ids: tuple[int, ...]
    inherited_dataset001: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _Dataset002Structure:
    """Internal bundle shared by both public DATASET-002 verifiers."""

    manifest: dict[str, Any]
    spec: Dataset002Spec
    manifest_sha256: str
    thresholds: Mapping[Metric, tuple[tuple[str, float], ...]]
    base_ids: npt.NDArray[np.int64]
    base_vectors: npt.NDArray[np.float32]
    routing_ids: npt.NDArray[np.int64]
    routing_queries: npt.NDArray[np.float32]
    audit_ids: npt.NDArray[np.int64]
    audit_queries: npt.NDArray[np.float32]
    inherited_dataset001: dict[str, Any]


def _verify_dataset002_structure(
    output_dir: str | os.PathLike[str],
    *,
    dataset001_dir: str | os.PathLike[str],
) -> _Dataset002Structure:
    """Validate everything about DATASET-002 except oracle-record semantics.

    Covers manifest schema/generation contract, every artifact hash,
    SHA256SUMS, the exact closed file inventory, inherited DATASET-001
    identity/hashes, deterministic routing/recall-audit array regeneration,
    and role counts/schema/disjointness (via ``_load_output_arrays``). Never
    reads or judges ``oracle_records.jsonl`` content.
    """

    output = Path(output_dir)
    manifest = _read_json(output / _MANIFEST_NAME)
    if frozenset(manifest) != _MANIFEST_FIELDS or manifest.get("schema_version") != DATASET002_SCHEMA_VERSION:
        raise ContractViolation("DATASET-002 manifest schema is invalid")
    if manifest.get("generation") != _GENERATION_CONTRACT:
        raise ContractViolation("DATASET-002 generation contract is invalid")
    spec = _spec_from_mapping(manifest["dataset"])
    artifacts = manifest["artifacts"]
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(DATASET002_ARTIFACTS):
        raise ContractViolation("DATASET-002 manifest artifacts are invalid")
    for filename in DATASET002_ARTIFACTS:
        entry = artifacts[filename]
        if not isinstance(entry, Mapping) or set(entry) != {"file", "bytes", "sha256"}:
            raise ContractViolation("DATASET-002 artifact entry schema is invalid")
        path = output / filename
        if entry["file"] != filename or not path.is_file() or entry["bytes"] != path.stat().st_size or entry["sha256"] != sha256_file(path):
            raise ContractViolation(f"DATASET-002 artifact checksum mismatch: {filename}")
    _verify_sums(output)
    expected_files = set((*DATASET002_ARTIFACTS, _MANIFEST_NAME, _SUMS_NAME))
    try:
        actual_files = {path.name for path in output.iterdir()}
    except OSError as exc:
        raise ContractViolation("DATASET-002 artifact directory is unreadable") from exc
    if actual_files != expected_files:
        raise ContractViolation("DATASET-002 artifact directory contains unexpected files")
    _, base_ids, base_vectors, thresholds, inherited = _load_dataset001(Path(dataset001_dir))
    if manifest["inherited_dataset001"] != inherited or _read_json(output / "inherited_dataset001.json") != inherited:
        raise ContractViolation("DATASET-002 inherited DATASET-001 identity mismatch")
    if base_vectors.shape[1] != spec.dimensions:
        raise ContractViolation("DATASET-002/inherited DATASET-001 dimension mismatch")
    routing_ids, routing_queries, audit_ids, audit_queries = _load_output_arrays(output, spec)
    expected = generate_dataset002(spec)
    if not (
        np.array_equal(routing_ids, expected.routing_ids)
        and np.array_equal(routing_queries, expected.routing_queries)
        and np.array_equal(audit_ids, expected.recall_audit_ids)
        and np.array_equal(audit_queries, expected.recall_audit_queries)
    ):
        raise ContractViolation("DATASET-002 arrays disagree with the deterministic generator")
    return _Dataset002Structure(
        manifest=manifest,
        spec=spec,
        manifest_sha256=sha256_file(output / _MANIFEST_NAME),
        thresholds=thresholds,
        base_ids=base_ids,
        base_vectors=base_vectors,
        routing_ids=routing_ids,
        routing_queries=routing_queries,
        audit_ids=audit_ids,
        audit_queries=audit_queries,
        inherited_dataset001=inherited,
    )


def verify_dataset002_query_identity(
    output_dir: str | os.PathLike[str],
    *,
    dataset001_dir: str | os.PathLike[str],
) -> Dataset002QueryIdentityResult:
    """Verify DATASET-002 query identity and role-disjointness only.

    Deliberately does not read or judge ``oracle_records.jsonl``. Callers that
    need oracle-record correctness (e.g. EXP-009 Stage 1 recall-audit
    evidence) must use :func:`verify_dataset002_artifacts` instead; this
    narrower function exists for consumers -- such as DATASET-003 -- whose
    contract depends only on DATASET-002's query IDs and vectors, not its
    oracle semantics.
    """

    structure = _verify_dataset002_structure(output_dir, dataset001_dir=dataset001_dir)
    output = Path(output_dir)
    return Dataset002QueryIdentityResult(
        verification_scope=DATASET002_QUERY_IDENTITY_SCOPE,
        dataset_id=structure.spec.dataset_id,
        version=structure.spec.version,
        manifest_sha256=structure.manifest_sha256,
        routing_ids_sha256=sha256_file(output / "routing_ids.npy"),
        recall_audit_ids_sha256=sha256_file(output / "recall_audit_ids.npy"),
        routing_ids=tuple(int(value) for value in structure.routing_ids),
        recall_audit_ids=tuple(int(value) for value in structure.audit_ids),
        inherited_dataset001=structure.inherited_dataset001,
    )


def verify_dataset002_artifacts(
    output_dir: str | os.PathLike[str],
    *,
    dataset001_dir: str | os.PathLike[str],
) -> dict[str, Any]:
    """Independently revalidate every DATASET-002 byte, role, and oracle record.

    The complete, strict verifier: structural/query-identity checks plus the
    byte-exact oracle-record semantic recomputation. Never weakens, tolerates,
    or bypasses the oracle comparison.
    """

    structure = _verify_dataset002_structure(output_dir, dataset001_dir=dataset001_dir)
    _verify_oracle_records(
        Path(output_dir),
        routing_ids=structure.routing_ids,
        routing_queries=structure.routing_queries,
        audit_ids=structure.audit_ids,
        audit_queries=structure.audit_queries,
        base_ids=structure.base_ids,
        base_vectors=structure.base_vectors,
        thresholds=structure.thresholds,
    )
    return structure.manifest


def load_recall_audit_oracle_ids(
    output_dir: str | os.PathLike[str],
    *,
    metric: Metric,
    threshold_label: str,
) -> dict[int, tuple[int, ...]]:
    """Read already-verified ``oracle_records.jsonl`` for one recall-audit slice.

    Callers must have already run ``verify_dataset002_artifacts`` against
    ``output_dir`` -- this function trusts the file's bytes and re-derives
    nothing about the base vectors or generator; it only parses and filters
    the same schema ``_verify_oracle_records`` already validates byte-exact.
    Returns ``{query_id: oracle_hit_ids}`` for every ``role == "recall_audit"``
    record at the given ``(metric, threshold_label)``, ordered ascending by
    hit rank (Milvus-style: nearest/most-similar first), never deduplicated
    or reordered by this function.
    """

    if not isinstance(metric, Metric):
        raise ContractViolation("metric must be a Metric")
    if threshold_label not in THRESHOLD_LABELS:
        raise ContractViolation("threshold_label must be a registered THRESHOLD_LABELS value")
    path = Path(output_dir) / "oracle_records.jsonl"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ContractViolation("DATASET-002 oracle records are unreadable") from exc
    result: dict[int, tuple[int, ...]] = {}
    for line in lines:
        try:
            record = json.loads(line, object_pairs_hook=_no_duplicate_json_fields)
        except (json.JSONDecodeError, _DuplicateJsonField) as exc:
            raise ContractViolation("DATASET-002 oracle record JSON is invalid") from exc
        if not isinstance(record, dict) or frozenset(record) != _ORACLE_FIELDS:
            raise ContractViolation("DATASET-002 oracle record schema is invalid")
        if record["role"] != "recall_audit" or record["metric"] != metric.value or record["threshold_label"] != threshold_label:
            continue
        query_id = record["query_id"]
        hits = record["hits"]
        if isinstance(query_id, bool) or not isinstance(query_id, int):
            raise ContractViolation("DATASET-002 oracle record query ID is invalid")
        if query_id in result:
            raise ContractViolation("DATASET-002 oracle records contain duplicates")
        if not isinstance(hits, list) or not all(
            isinstance(hit, dict) and frozenset(hit) == _HIT_FIELDS and isinstance(hit["id"], int) and not isinstance(hit["id"], bool)
            for hit in hits
        ):
            raise ContractViolation("DATASET-002 oracle record hits are invalid")
        result[query_id] = tuple(hit["id"] for hit in hits)
    return result


__all__ = [
    "DATASET002_ARTIFACTS",
    "DATASET002_QUERY_IDENTITY_SCOPE",
    "DATASET002_SCHEMA_VERSION",
    "DATASET002_SPEC",
    "Dataset002Bundle",
    "Dataset002QueryIdentityResult",
    "Dataset002Spec",
    "generate_dataset002",
    "load_recall_audit_oracle_ids",
    "verify_dataset002_artifacts",
    "verify_dataset002_query_identity",
    "write_dataset002_artifacts",
]
