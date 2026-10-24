"""ADR-026 canonical construction and its private, non-resumable evidence.

No client, process, directory or governed store is opened at import. Native
execution is an explicit separately authorized API. Tests use isolated scopes.
The claim concerns canonical logical values, never native row multiplicity.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import sqlite3
import stat
import struct
import uuid
import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .artifacts import verify_dataset_artifacts
from .canonical_serialization import (
    decode_strict_canonical_json,
    strict_canonical_digest,
    strict_canonical_json_bytes,
)
from .deployment_governance import (
    DeploymentGovernanceScope,
    canonical_deployment_governance_scope,
    ensure_deployment_scope_directory,
)

DATASET001_MANIFEST_SHA256 = (
    "b6cb56a3eee60f6728be1d08a465e2a2500eec4089b4466da76fe2e886b51da9"
)
MILVUS_REVISION = "f46a0328558be155d11266a1a2b90602ccc9b366"
PYMILVUS_REVISION = "abf720abc64f57cea4cbc05292a802f9fd73c4c8"
DATA_IDENTITY = f"DATASET-001-v1:sha256:{DATASET001_MANIFEST_SHA256}"
RUN_PREFIX = "lkg-remat-v1-"
COLLECTION_PREFIX = "vd_remat_"
_ZERO = "0" * 64
_DOMAIN = b"vdbench.controlled-rematerialization.v1\0"
_MAX_JOURNAL_BYTES = 256 * 1024 * 1024


class RematerializationError(RuntimeError):
    """A refusal never implies that an unacknowledged native effect was absent."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def require(condition: bool, code: str) -> None:
    if not condition:
        raise RematerializationError(code)


def digest(kind: str, value: object) -> str:
    return strict_canonical_digest(_DOMAIN + kind.encode("ascii") + b"\0", value)


def _hex(value: object, size: int = 64) -> str:
    require(type(value) is str and re.fullmatch(f"[0-9a-f]{{{size}}}", value) is not None,
            "IDENTITY_DIGEST_INVALID")
    return value  # type: ignore[return-value]


def validate_input_arrays(ids: object, vectors: object) -> None:
    """Validate before conversion; accepting coerced input changes C(p)."""
    require(type(ids) is np.ndarray and ids.dtype.str == "<i8"
            and ids.shape == (10000,), "DATASET001_IDS_INVALID")
    require(type(vectors) is np.ndarray and vectors.dtype.str == "<f4"
            and vectors.shape == (10000, 128), "DATASET001_VECTORS_INVALID")
    require(np.array_equal(ids, np.arange(10000, dtype="<i8")),
            "DATASET001_PK_DOMAIN_OR_UNIQUENESS_INVALID")
    require(bool(np.isfinite(vectors).all()), "DATASET001_NONFINITE")


@dataclass(frozen=True, slots=True, init=False)
class CanonicalDataset001:
    """Loader-issued arrays backed by immutable bytes, not caller-owned memory."""

    ids: np.ndarray
    vectors: np.ndarray
    artifact_document_bytes: bytes

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("use CanonicalDataset001.load")

    @classmethod
    def load(cls, directory: str | os.PathLike[str]) -> CanonicalDataset001:
        root = Path(directory)
        raw = (root / "generation_manifest.json").read_bytes()
        require(hashlib.sha256(raw).hexdigest() == DATASET001_MANIFEST_SHA256,
                "DATASET001_MANIFEST_MISMATCH")
        manifest = json.loads(raw)
        # Reuse the historical inventory verifier, then hash and parse the SAME
        # separately captured bytes below; np.load(path) would introduce a race.
        verify_dataset_artifacts(root)
        captured: dict[str, bytes] = {}
        inventory = manifest["artifacts"]
        require(set(inventory) == {
            "base_ids.npy", "base_vectors.npy", "boundary_fixtures.json",
            "calibration_queries.npy", "measured_queries.npy", "thresholds.json",
        }, "DATASET001_INVENTORY_INVALID")
        for name, entry in inventory.items():
            require(entry["file"] == name, "DATASET001_ARTIFACT_PATH_INVALID")
            data = (root / name).read_bytes()
            require(len(data) == entry["bytes"]
                    and hashlib.sha256(data).hexdigest() == entry["sha256"],
                    "DATASET001_ARTIFACT_MISMATCH")
            captured[name] = data
        require((root / "generation_manifest.json").read_bytes() == raw,
                "DATASET001_MANIFEST_CHANGED")
        ids = np.load(io.BytesIO(captured["base_ids.npy"]), allow_pickle=False)
        vectors = np.load(io.BytesIO(captured["base_vectors.npy"]), allow_pickle=False)
        validate_input_arrays(ids, vectors)
        value = object.__new__(cls)
        # bytes backing prevents even setflags(write=True) from changing them.
        object.__setattr__(value, "ids", np.frombuffer(ids.tobytes(), dtype="<i8"))
        object.__setattr__(value, "vectors",
                           np.frombuffer(vectors.tobytes(), dtype="<f4").reshape(10000, 128))
        object.__setattr__(value, "artifact_document_bytes",
                           strict_canonical_json_bytes(inventory))
        return value

    def batch(self, ordinal: int) -> tuple[tuple[int, ...], np.ndarray, str]:
        require(type(ordinal) is int and 0 <= ordinal < 10, "BATCH_ORDINAL_INVALID")
        start = ordinal * 1000
        ids = tuple(int(p) for p in self.ids[start:start + 1000])
        vectors = self.vectors[start:start + 1000]
        payload = (b"vdbench.canonical-user-batch.v1\0" + struct.pack("<QI", 1000, 128)
                   + self.ids[start:start + 1000].tobytes() + vectors.tobytes())
        return ids, vectors, hashlib.sha256(payload).hexdigest()


def _private_directory(path: Path, *, create: bool = False) -> None:
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700, "EVIDENCE_DIRECTORY_UNSAFE")


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _private_file(path: Path) -> os.stat_result:
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
            and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o600,
            "EVIDENCE_FILE_UNSAFE")
    return info


def _database(path: Path, *, create: bool = False, readonly: bool = False,
              tables: Mapping[str, str]) -> sqlite3.Connection:
    _private_directory(path.parent)
    if create:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_WRONLY, 0o600)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        _sync_directory(path.parent)
    before = _private_file(path)
    if not create:
        # Even mode=ro SQLite may create WAL/SHM sidecars. Refuse the native
        # header's WAL format before SQLite sees an unsupported existing file.
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            opened = os.fstat(fd)
            require((opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino),
                    "EVIDENCE_FILE_REPLACED")
            header = os.read(fd, 100)
        finally:
            os.close(fd)
        require(len(header) == 100 and header[:16] == b"SQLite format 3\0",
                "EVIDENCE_SCHEMA_INVALID")
        require(header[18:20] == b"\x01\x01", "EVIDENCE_JOURNAL_MODE_INVALID")
        preflight = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True,
                                   isolation_level=None, timeout=1.0)
        try:
            require(preflight.execute("PRAGMA journal_mode").fetchone()[0] == "delete",
                    "EVIDENCE_JOURNAL_MODE_INVALID")
            require(preflight.execute("PRAGMA quick_check").fetchone()[0] == "ok",
                    "EVIDENCE_DATABASE_CORRUPT")
            _verify_schema(preflight, tables)
        finally:
            preflight.close()
    mode = "ro" if readonly else "rw"
    connection = sqlite3.connect(path.as_uri() + f"?mode={mode}", uri=True,
                                 isolation_level=None, timeout=1.0)
    after = _private_file(path)
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        connection.close()
        raise RematerializationError("EVIDENCE_FILE_REPLACED")
    if not readonly:
        if create:
            connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _install_schema(connection: sqlite3.Connection, tables: Mapping[str, str]) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        for sql in tables.values():
            connection.execute(sql)
        for table in tables:
            for operation in ("UPDATE", "DELETE"):
                connection.execute(
                    f"CREATE TRIGGER {table}_no_{operation.lower()} "
                    f"BEFORE {operation} ON {table} "
                    "BEGIN SELECT RAISE(ABORT,'append-only'); END"
                )
        connection.execute("PRAGMA user_version=1")
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def _verify_schema(connection: sqlite3.Connection, tables: Mapping[str, str]) -> None:
    require(connection.execute("PRAGMA user_version").fetchone()[0] == 1,
            "EVIDENCE_SCHEMA_INVALID")
    expected = {name: sql for name, sql in tables.items()}
    for table in tables:
        for operation in ("UPDATE", "DELETE"):
            expected[f"{table}_no_{operation.lower()}"] = (
                f"CREATE TRIGGER {table}_no_{operation.lower()} "
                f"BEFORE {operation} ON {table} "
                "BEGIN SELECT RAISE(ABORT,'append-only'); END"
            )
    actual = dict(connection.execute(
        "SELECT name,sql FROM sqlite_master WHERE type IN ('table','trigger')"
    ))
    require(actual == expected, "EVIDENCE_SCHEMA_INVALID")


_REGISTRY_TABLES = {
    "generations": "CREATE TABLE generations (generation TEXT PRIMARY KEY,"
                   "plan BLOB NOT NULL,plan_digest TEXT NOT NULL UNIQUE) STRICT",
    "origins": "CREATE TABLE origins (identity TEXT PRIMARY KEY,generation TEXT NOT NULL,"
               "FOREIGN KEY(generation) REFERENCES generations(generation)) STRICT",
}
_JOURNAL_TABLES = {
    "events": "CREATE TABLE events (sequence INTEGER PRIMARY KEY CHECK(sequence>=0),"
              "kind TEXT NOT NULL,document BLOB NOT NULL,digest TEXT NOT NULL UNIQUE) STRICT",
    "owners": "CREATE TABLE owners (target TEXT NOT NULL,pk INTEGER NOT NULL,"
              "request INTEGER NOT NULL,PRIMARY KEY(target,pk)) STRICT",
}


@dataclass(frozen=True, slots=True)
class GenerationPlan:
    """Immutable reservation. Native IDs are captured later, never patched in."""

    document_bytes: bytes
    scope: DeploymentGovernanceScope

    def validate(self, dataset: CanonicalDataset001 | None = None) -> None:
        from .controlled_lkg_control import validate_origin, validate_profile

        doc = self.document
        require(set(doc) == {
            "schema_version", "generation", "deployment_identity", "deployment_namespace_digest",
            "source_revision", "dataset_identity", "artifacts", "database", "dimensions", "metric",
            "user_schema", "indexes", "targets", "requests", "request_manifest_sha256", "effects",
            "profile", "profile_sha256", "origin", "sdk_version", "sdk_revision", "server_version",
            "server_revision",
        }, "GENERATION_PLAN_SHAPE_INVALID")
        _hex(doc["generation"], 32)
        _hex(doc["source_revision"], 40)
        require(doc["schema_version"] == "controlled-generation-plan-v1"
                and doc["deployment_identity"] == self.scope.deployment_identity
                and doc["deployment_namespace_digest"] == self.scope.namespace_digest
                and doc["dataset_identity"] == DATA_IDENTITY and doc["database"] == "default"
                and type(doc["dimensions"]) is int and doc["dimensions"] == 128 and doc["metric"] == "L2"
                and doc["sdk_version"] == "3.0.1" and doc["sdk_revision"] == PYMILVUS_REVISION
                and doc["server_version"] == "3.0.0" and doc["server_revision"] == MILVUS_REVISION,
                "GENERATION_IDENTITY_INVALID")
        require(doc["targets"] == {t: f"{COLLECTION_PREFIX}{doc['generation']}_{t.lower()}"
                                   for t in ("FLAT", "HNSW")}, "GENERATION_TARGET_INVALID")
        require(doc["user_schema"] == [
            {"name": "id", "type": 5, "is_primary": True, "auto_id": False},
            {"name": "vector", "type": 101, "dim": 128}], "GENERATION_SCHEMA_INVALID")
        require(doc["indexes"] == {
            "FLAT": {"index_type": "FLAT", "metric_type": "L2", "params": {}},
            "HNSW": {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 200}}},
            "GENERATION_INDEX_INVALID")
        validate_profile(doc["profile"])
        validate_origin(doc["origin"], doc["profile"])
        require(doc["profile_sha256"] == digest("profile", doc["profile"]), "GENERATION_PROFILE_INVALID")
        requests = doc["requests"]
        require(type(requests) is list and len(requests) == 20, "GENERATION_REQUESTS_INVALID")
        for index, request in enumerate(requests):
            ordinal, target = index % 10, "FLAT" if index < 10 else "HNSW"
            require(type(request) is dict and set(request) == {"target", "ordinal", "pks", "payload_sha256"}
                    and type(request["ordinal"]) is int and request["ordinal"] == ordinal
                    and request["target"] == target
                    and type(request["pks"]) is list and all(type(p) is int for p in request["pks"])
                    and request["pks"] == list(range(ordinal * 1000, (ordinal + 1) * 1000)),
                    "GENERATION_REQUESTS_INVALID")
            _hex(request["payload_sha256"])
            if dataset is not None:
                require(request["payload_sha256"] == dataset.batch(ordinal)[2], "CONSTRUCTION_PAYLOAD_CHANGED")
        require(doc["request_manifest_sha256"] == digest("requests", requests), "GENERATION_REQUESTS_INVALID")
        expected = ([{"action": "CREATE", "target": t} for t in ("FLAT", "HNSW")]
                    + [{"action": "INSERT", "target": r["target"], "ordinal": r["ordinal"]} for r in requests]
                    + [{"action": a, "target": t} for a in ("FLUSH", "INDEX", "LOAD") for t in ("FLAT", "HNSW")])
        require(doc["effects"] == expected, "GENERATION_EFFECTS_INVALID")
        if dataset is not None:
            require(type(dataset) is CanonicalDataset001
                    and doc["artifacts"] == decode_strict_canonical_json(dataset.artifact_document_bytes),
                    "GENERATION_DATASET_INVALID")

    @property
    def document(self) -> dict[str, Any]:
        return decode_strict_canonical_json(self.document_bytes)  # type: ignore[return-value]

    @property
    def sha256(self) -> str:
        return digest("generation-plan", self.document)

    @property
    def generation(self) -> str:
        return self.document["generation"]

    @property
    def root(self) -> Path:
        return Path(self.scope.scope_root) / "rematerializations"

    @property
    def journal_path(self) -> Path:
        return self.root / self.generation / "construction.sqlite3"

    @classmethod
    def prepare(
        cls, *, dataset: CanonicalDataset001, source_revision: str,
        profile: Mapping[str, Any], origin: Mapping[str, Any],
        scope: DeploymentGovernanceScope | None = None,
    ) -> GenerationPlan:
        from .controlled_lkg_control import validate_origin, validate_profile

        require(type(dataset) is CanonicalDataset001, "CANONICAL_INPUT_REQUIRED")
        _hex(source_revision, 40)
        scope = scope or canonical_deployment_governance_scope()
        profile_doc = validate_profile(dict(profile))
        origin_doc = validate_origin(dict(origin), profile_doc)
        generation = uuid.uuid4().hex
        targets = {
            "FLAT": f"{COLLECTION_PREFIX}{generation}_flat",
            "HNSW": f"{COLLECTION_PREFIX}{generation}_hnsw",
        }
        requests = [
            {"target": target, "ordinal": ordinal,
             "pks": list(dataset.batch(ordinal)[0]),
             "payload_sha256": dataset.batch(ordinal)[2]}
            for target in ("FLAT", "HNSW") for ordinal in range(10)
        ]
        effects = ([{"action": "CREATE", "target": target} for target in targets]
                   + [{"action": "INSERT", "target": r["target"],
                       "ordinal": r["ordinal"]} for r in requests]
                   + [{"action": action, "target": target}
                      for action in ("FLUSH", "INDEX", "LOAD") for target in targets])
        document = {
            "schema_version": "controlled-generation-plan-v1",
            "generation": generation,
            "deployment_identity": scope.deployment_identity,
            "deployment_namespace_digest": scope.namespace_digest,
            "source_revision": source_revision,
            "dataset_identity": DATA_IDENTITY,
            "artifacts": decode_strict_canonical_json(dataset.artifact_document_bytes),
            "database": "default", "dimensions": 128, "metric": "L2",
            "user_schema": [
                {"name": "id", "type": 5, "is_primary": True, "auto_id": False},
                {"name": "vector", "type": 101, "dim": 128},
            ],
            "indexes": {"FLAT": {"index_type": "FLAT", "metric_type": "L2", "params": {}},
                        "HNSW": {"index_type": "HNSW", "metric_type": "L2",
                                 "params": {"M": 16, "efConstruction": 200}}},
            "targets": targets, "requests": requests,
            "request_manifest_sha256": digest("requests", requests), "effects": effects,
            "profile": profile_doc, "profile_sha256": digest("profile", profile_doc),
            "origin": origin_doc,
            "sdk_version": "3.0.1", "sdk_revision": PYMILVUS_REVISION,
            "server_version": "3.0.0", "server_revision": MILVUS_REVISION,
        }
        plan = cls(strict_canonical_json_bytes(document), scope)
        plan.validate(dataset)
        return plan


def reserve_generation(plan: GenerationPlan, *, expected_plan_sha256: str) -> None:
    """Reserve before effects; uniqueness survives failure and process death."""
    require(type(plan) is GenerationPlan and expected_plan_sha256 == plan.sha256,
            "GENERATION_AUTHORITY_MISMATCH")
    plan.validate()
    ensure_deployment_scope_directory(plan.scope)
    _private_directory(plan.root, create=True)
    path = plan.root / "registry.sqlite3"
    try:
        connection = _database(path, create=True, tables=_REGISTRY_TABLES)
        created = True
    except FileExistsError:
        connection = _database(path, tables=_REGISTRY_TABLES)
        created = False
    try:
        if created:
            _install_schema(connection, _REGISTRY_TABLES)
        connection.execute("BEGIN IMMEDIATE")
        try:
            _verify_schema(connection, _REGISTRY_TABLES)
            doc = plan.document
            connection.execute("INSERT INTO generations VALUES (?,?,?)",
                               (plan.generation, plan.document_bytes, plan.sha256))
            identities = doc["origin"]["identities"]
            require(len(identities) == len(set(identities)), "ORIGIN_IDENTITIES_OVERLAP")
            connection.executemany("INSERT INTO origins VALUES (?,?)",
                                   [(p, plan.generation) for p in identities])
            connection.execute("COMMIT")
            _sync_directory(plan.root)
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
    except sqlite3.IntegrityError as exc:
        raise RematerializationError("GENERATION_OR_ORIGIN_ALREADY_CONSUMED") from exc
    finally:
        connection.close()
    # Registry commit deliberately precedes file creation. A partial directory
    # or schema is consumed, not a resumable allocation.
    root = plan.journal_path.parent
    root.mkdir(mode=0o700)
    _sync_directory(plan.root)
    connection = _database(plan.journal_path, create=True, tables=_JOURNAL_TABLES)
    try:
        _install_schema(connection, _JOURNAL_TABLES)
    finally:
        connection.close()
    with ConstructionJournal(plan) as journal:
        journal.append("RESERVED", {"plan": plan.document, "plan_sha256": plan.sha256})


class ConstructionJournal:
    """Private append-only journal. Opening evidence never resumes execution."""

    def __init__(self, plan: GenerationPlan, *, readonly: bool = False) -> None:
        self.plan = plan
        self.readonly = readonly
        self._connection = _database(plan.journal_path, readonly=readonly, tables=_JOURNAL_TABLES)
        self._inode = _private_file(plan.journal_path)
        self._fault = False
        _verify_schema(self._connection, _JOURNAL_TABLES)
        self.events()  # refuse corrupt/torn tails, never truncate

    def __enter__(self) -> ConstructionJournal:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        self._connection.close()

    def _harden(self) -> None:
        info = _private_file(self.plan.journal_path)
        require((info.st_dev, info.st_ino) == (self._inode.st_dev, self._inode.st_ino),
                "EVIDENCE_FILE_REPLACED")
        require(info.st_size <= _MAX_JOURNAL_BYTES, "JOURNAL_BOUND_EXCEEDED")
        _verify_schema(self._connection, _JOURNAL_TABLES)

    def events(self) -> tuple[dict[str, Any], ...]:
        self._harden()
        require(self._connection.execute("PRAGMA quick_check").fetchone()[0] == "ok",
                "JOURNAL_CORRUPT")
        rows: list[dict[str, Any]] = []
        previous = _ZERO
        size = 0
        for sequence, kind, raw, expected in self._connection.execute(
            "SELECT sequence,kind,document,digest FROM events ORDER BY sequence"
        ):
            size += len(raw)
            require(size <= _MAX_JOURNAL_BYTES, "JOURNAL_BOUND_EXCEEDED")
            doc = decode_strict_canonical_json(raw)
            require(type(doc) is dict and set(doc) == {
                "schema_version", "sequence", "kind", "previous", "payload",
            }, "JOURNAL_DOCUMENT_INVALID")
            require(doc["schema_version"] == "controlled-construction-event-v1"
                    and type(doc["sequence"]) is int and doc["sequence"] == sequence == len(rows)
                    and doc["kind"] == kind and doc["previous"] == previous
                    and digest("event", doc) == expected, "JOURNAL_CHAIN_INVALID")
            previous = expected
            rows.append({**doc, "digest": expected})
        if rows:
            require(rows[0]["kind"] == "RESERVED"
                    and rows[0]["payload"] == {"plan": self.plan.document,
                                                "plan_sha256": self.plan.sha256},
                    "JOURNAL_GENERATION_MISMATCH")
        return tuple(rows)

    def append(self, kind: str, payload: Mapping[str, Any], *,
               ownership: tuple[str, int, tuple[int, ...]] | None = None,
               expected_last_kind: str | None = None) -> str:
        require(not self.readonly and not self._fault, "JOURNAL_NOT_WRITABLE")
        try:
            self._harden()
            self._connection.execute("BEGIN IMMEDIATE")
            last = self._connection.execute(
                "SELECT sequence,kind,digest FROM events ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            require(last is None or last[1] != "INVALID", "GENERATION_INVALID")
            if expected_last_kind is not None:
                require(last is not None and last[1] == expected_last_kind,
                        "STATE_TRANSITION_OR_RESUME_REFUSED")
            if ownership is not None:
                target, ordinal, pks = ownership
                require(target in self.plan.document["targets"] and type(ordinal) is int,
                        "REQUEST_OWNER_INVALID")
                require(len(pks) == 1000 and all(type(p) is int for p in pks)
                        and len(set(pks)) == 1000, "REQUEST_PK_MEMBERSHIP_INVALID")
                self._connection.executemany("INSERT INTO owners VALUES (?,?,?)",
                                           [(target, p, ordinal) for p in pks])
            doc = {"schema_version": "controlled-construction-event-v1",
                   "sequence": 0 if last is None else last[0] + 1, "kind": kind,
                   "previous": _ZERO if last is None else last[2], "payload": dict(payload)}
            raw = strict_canonical_json_bytes(doc)
            require(self.plan.journal_path.stat().st_size + len(raw) < _MAX_JOURNAL_BYTES,
                    "JOURNAL_BOUND_EXCEEDED")
            result = digest("event", doc)
            self._connection.execute("INSERT INTO events VALUES (?,?,?,?)",
                                     (doc["sequence"], kind, raw, result))
            self._connection.execute("COMMIT")
            _sync_directory(self.plan.journal_path.parent)
            self._harden()
            return result
        except BaseException:
            if self._connection.in_transaction:
                self._connection.execute("ROLLBACK")
            self._fault = True
            raise

    def invalidate(self, code: str) -> None:
        """Failure to persist INVALID still forbids the current owner from work."""
        if self.readonly:
            raise RematerializationError("JOURNAL_NOT_WRITABLE")
        self._fault = False
        try:
            if self.events() and self.events()[-1]["kind"] == "INVALID":
                return
            self.append("INVALID", {"reason": code})
        finally:
            self._fault = True


def load_reserved_plan(scope: DeploymentGovernanceScope, generation: str) -> GenerationPlan:
    """Read-only exact registry membership; caller paths never select evidence."""
    _hex(generation, 32)
    path = Path(scope.scope_root) / "rematerializations" / "registry.sqlite3"
    connection = _database(path, readonly=True, tables=_REGISTRY_TABLES)
    try:
        _verify_schema(connection, _REGISTRY_TABLES)
        row = connection.execute(
            "SELECT plan,plan_digest FROM generations WHERE generation=?", (generation,)
        ).fetchone()
        require(row is not None, "GENERATION_NOT_REGISTERED")
        plan = GenerationPlan(row[0], scope)
        plan.validate()
        require(plan.generation == generation and plan.sha256 == row[1]
                and plan.document["deployment_namespace_digest"] == scope.namespace_digest,
                "GENERATION_REGISTRY_MISMATCH")
        actual = {r[0] for r in connection.execute(
            "SELECT identity FROM origins WHERE generation=?", (generation,)
        )}
        require(actual == set(plan.document["origin"]["identities"]),
                "GENERATION_ORIGIN_REGISTRY_MISMATCH")
        return plan
    finally:
        connection.close()


def _request(plan: GenerationPlan, target: str, ordinal: int) -> dict[str, Any]:
    require(target in {"FLAT", "HNSW"} and type(ordinal) is int and 0 <= ordinal < 10,
            "REQUEST_OWNER_INVALID")
    return plan.document["requests"][(0 if target == "FLAT" else 10) + ordinal]


def execute_construction(
    plan: GenerationPlan, dataset: CanonicalDataset001, writer: Any,
    *, revoke_writer: Callable[[], None], deadline_ns: int,
    clock: Callable[[], int] = time.monotonic_ns,
) -> None:
    """One application owner, no retry or repair; ports do not issue authority.

    A process supervisor must enforce the external deadline for a native RPC
    that blocks. Revocation belongs to the parent as well, including SIGKILL.
    A test port can exercise this executor but cannot establish native control.
    """
    try:
        require(type(dataset) is CanonicalDataset001 and type(deadline_ns) is int,
                "CONSTRUCTION_INPUT_INVALID")
        plan.validate(dataset)
        # The native path additionally needs a process supervisor and a source-
        # complete P1-B baseline. Neither a fake port nor a parsed profile supplies
        # that proof; this partial implementation must never dispatch it directly.
        from .controlled_lkg_milvus import PinnedColumnarMilvus
        require(not isinstance(writer, PinnedColumnarMilvus), "NATIVE_CONTROL_AND_SUPERVISOR_REQUIRED")
        journal = ConstructionJournal(plan)
    except BaseException:
        try:
            writer.close()
        finally:
            revoke_writer()
        raise
    with journal:
        try:
            journal.append("CONSTRUCTION_STARTED", {"deadline_ns": deadline_ns},
                           expected_last_kind="RESERVED")
            native_ids: set[int] = set()
            for number, effect in enumerate(plan.document["effects"]):
                require(clock() < deadline_ns, "CONSTRUCTION_DEADLINE")
                target, action = effect["target"], effect["action"]
                name = plan.document["targets"][target]
                ownership = None
                if action == "CREATE":
                    # Collection's constructor adopts existing collections.
                    # Negative existence and an explicit Create RPC precede it.
                    require(writer.has_collection(name) is False, "TARGET_ALREADY_EXISTS")
                if action == "INSERT":
                    pks, vectors, payload = dataset.batch(effect["ordinal"])
                    request = _request(plan, target, effect["ordinal"])
                    require(request == {"target": target, "ordinal": effect["ordinal"],
                                        "pks": list(pks), "payload_sha256": payload},
                            "CONSTRUCTION_PAYLOAD_CHANGED")
                    ownership = (target, effect["ordinal"], pks)
                journal.append("EFFECT_INTENT", {"effect": number, **effect},
                               ownership=ownership)
                if action == "CREATE":
                    collection_id = writer.create_collection(name)
                    require(type(collection_id) is int and collection_id > 0
                            and collection_id not in native_ids, "NATIVE_COLLECTION_ID_INVALID")
                    native_ids.add(collection_id)
                    ack = {"collection_id": collection_id}
                elif action == "INSERT":
                    result = writer.insert(name, pks, vectors)
                    require(type(result) is dict and set(result) == {"insert_count", "primary_keys"}
                            and type(result["insert_count"]) is int and result["insert_count"] == 1000
                            and type(result["primary_keys"]) is list
                            and all(type(p) is int for p in result["primary_keys"])
                            and result["primary_keys"] == list(pks), "INSERT_ACK_INVALID")
                    ack = result
                elif action == "FLUSH":
                    ack = writer.flush(name)
                    require(type(ack) is dict and set(ack) == {"flush_timestamp", "segment_ids"}
                            and type(ack["flush_timestamp"]) is int and ack["flush_timestamp"] > 0
                            and type(ack["segment_ids"]) is list
                            and all(type(s) is int and s > 0 for s in ack["segment_ids"])
                            and len(set(ack["segment_ids"])) == len(ack["segment_ids"]),
                            "FLUSH_ACK_INVALID")
                elif action == "INDEX":
                    writer.create_index(name, plan.document["indexes"][target])
                    ack = {"index": plan.document["indexes"][target]}
                elif action == "LOAD":
                    writer.load(name)
                    ack = {"loaded": True}
                else:
                    raise RematerializationError("CONSTRUCTION_EFFECT_INVALID")
                require(clock() < deadline_ns, "CONSTRUCTION_DEADLINE")
                # No subsequent mutation until this acknowledgment is durable.
                journal.append("EFFECT_ACK", {"effect": number, "ack": ack},
                               expected_last_kind="EFFECT_INTENT")
            journal.append("CONSTRUCTION_ACKNOWLEDGED", {}, expected_last_kind="EFFECT_ACK")
        except BaseException as exc:
            journal.invalidate(getattr(exc, "code", "AMBIGUOUS_APPLICATION_MUTATION"))
            raise
        finally:
            # Close even on decode/durability failures. The parent still revokes
            # when a killed child never reaches this finally block.
            try:
                writer.close()
            except BaseException:
                journal.invalidate("WRITER_SESSION_CLOSE_FAILED")
                raise
            finally:
                try:
                    revoke_writer()
                except BaseException:
                    journal.invalidate("WRITER_REVOCATION_FAILED")
                    raise


def verify_construction(journal: ConstructionJournal, dataset: CanonicalDataset001) -> dict[str, int]:
    """Replay intent/ACK order and SQL ownership, independent of worker claims."""
    connection = journal._connection
    require(not connection.in_transaction, "EVIDENCE_TRANSACTION_ALREADY_ACTIVE")
    connection.execute("BEGIN")
    try:
        identities = _verify_construction_locked(journal, dataset)
        connection.execute("COMMIT")
        return identities
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise


def _verify_construction_locked(journal: ConstructionJournal, dataset: CanonicalDataset001) -> dict[str, int]:
    """One coherent snapshot; it is never a live applicability capability."""
    events = journal.events()
    journal.plan.validate(dataset)
    require(len(events) >= 59 and events[1]["kind"] == "CONSTRUCTION_STARTED",
            "CONSTRUCTION_INCOMPLETE_OR_RESUME")
    identities: dict[str, int] = {}
    expected_owners = []
    for number, effect in enumerate(journal.plan.document["effects"]):
        intent, acknowledgment = events[2 + number * 2:4 + number * 2]
        require(intent["kind"] == "EFFECT_INTENT"
                and intent["payload"] == {"effect": number, **effect}
                and acknowledgment["kind"] == "EFFECT_ACK"
                and set(acknowledgment["payload"]) == {"effect", "ack"}
                and acknowledgment["payload"]["effect"] == number, "CONSTRUCTION_ORDER_INVALID")
        ack, action, target = acknowledgment["payload"]["ack"], effect["action"], effect["target"]
        if action == "CREATE":
            require(type(ack) is dict and set(ack) == {"collection_id"}
                    and type(ack["collection_id"]) is int and ack["collection_id"] > 0,
                    "NATIVE_COLLECTION_ID_INVALID")
            identities[target] = ack["collection_id"]
        elif action == "INSERT":
            pks, _vectors, payload = dataset.batch(effect["ordinal"])
            require(_request(journal.plan, target, effect["ordinal"]) == {
                "target": target, "ordinal": effect["ordinal"], "pks": list(pks),
                "payload_sha256": payload}, "CONSTRUCTION_PAYLOAD_CHANGED")
            require(ack == {"insert_count": 1000, "primary_keys": list(pks)}
                    and all(type(p) is int for p in ack["primary_keys"]), "INSERT_ACK_INVALID")
            expected_owners.extend((target, pk, effect["ordinal"]) for pk in pks)
        elif action == "FLUSH":
            require(type(ack) is dict and set(ack) == {"flush_timestamp", "segment_ids"}
                    and type(ack["flush_timestamp"]) is int and ack["flush_timestamp"] > 0
                    and type(ack["segment_ids"]) is list
                    and all(type(s) is int and s > 0 for s in ack["segment_ids"])
                    and len(ack["segment_ids"]) == len(set(ack["segment_ids"])), "FLUSH_ACK_INVALID")
        elif action == "INDEX":
            require(ack == {"index": journal.plan.document["indexes"][target]}, "INDEX_ACK_INVALID")
        elif action == "LOAD":
            require(ack == {"loaded": True}, "LOAD_ACK_INVALID")
    require(len(set(identities.values())) == 2
            and events[58]["kind"] == "CONSTRUCTION_ACKNOWLEDGED"
            and events[58]["payload"] == {}, "CONSTRUCTION_INCOMPLETE")
    actual = journal._connection.execute("SELECT target,pk,request FROM owners ORDER BY target,pk").fetchall()
    require(actual == sorted(expected_owners), "APPLICATION_OWNERSHIP_INVALID")
    require(all(e["kind"] != "INVALID" for e in events), "GENERATION_INVALID")
    return identities


def verify_logical_values(reader: Any, name: str, dataset: CanonicalDataset001,
                          *, check: Callable[[], None]) -> dict[str, Any]:
    """Strong domain union + exact PK reads, no iterator or count inference.

    A duplicate returned value is permitted when its exact user value equals
    C(p). Float inputs must already be finite and exactly representable as
    binary32; rounding an incorrect response into C(p) is prohibited.
    """
    domain: set[int] = set()
    for expression in ("id < 0", "id >= 0"):
        check()
        rows = reader.query(name, expression, ["id"])
        require(type(rows) is list, "LOGICAL_QUERY_RESPONSE_INVALID")
        for row in rows:
            require(type(row) is dict and set(row) == {"id"} and type(row["id"]) is int,
                    "LOGICAL_PK_INVALID")
            require(0 <= row["id"] < 10000, "LOGICAL_EXTRA_PK")
            domain.add(row["id"])
        check()
    require(domain == set(range(10000)), "LOGICAL_PK_DOMAIN_INCOMPLETE")
    for pk in range(10000):
        check()
        rows = reader.query(name, f"id == {pk}", ["id", "vector"])
        require(type(rows) is list and bool(rows), "LOGICAL_VALUE_MISSING")
        expected = dataset.vectors[pk].tobytes()
        for row in rows:
            require(type(row) is dict and set(row) == {"id", "vector"}
                    and type(row["id"]) is int and row["id"] == pk,
                    "LOGICAL_VALUE_PK_INVALID")
            vector = row["vector"]
            require(type(vector) in (list, tuple) and len(vector) == 128,
                    "LOGICAL_VECTOR_SHAPE_INVALID")
            require(all(type(v) is float and math.isfinite(v) for v in vector),
                    "LOGICAL_VECTOR_VALUE_INVALID")
            try:
                packed = struct.pack("<128f", *vector)
            except (OverflowError, struct.error) as exc:
                raise RematerializationError("LOGICAL_VECTOR_VALUE_INVALID") from exc
            require(tuple(vector) == struct.unpack("<128f", packed) and packed == expected,
                    "LOGICAL_CANONICAL_VALUE_MISMATCH")
        check()
    return {"collection": name, "dataset_identity": DATA_IDENTITY,
            "logical_pk_domain_sha256": hashlib.sha256(dataset.ids.tobytes()).hexdigest(),
            "canonical_values_sha256": hashlib.sha256(dataset.vectors.tobytes()).hexdigest(),
            "predicate": "nonempty-canonical-value-set", "query_consistency": "Strong",
            "query_limit": "omitted", "pk_count": 10000}
