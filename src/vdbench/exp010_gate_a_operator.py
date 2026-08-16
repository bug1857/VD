"""The committed canonical Gate-A operator entrypoint for EXP-010.

Why this module exists:
    ADR-016 item 9 requires a fresh Gate A before any V5 campaign, but the only
    committed Gate-A artifact was `build_environment_manifest_sha256` -- a pure
    offline helper that hashes an observation an operator has *already* made. It
    defines neither how that observation is obtained nor where the result is
    persisted, so no Gate A was provable from the repository and no boundary
    owned V5 creation. ADR-017 closes exactly that gap, and this module is the
    entrypoint it names.

What Gate A proves:
    That at one stated instant a live ENV-001 stack was observed to match a
    governed environment description, and that the observation was reduced to
    one canonical digest. Nothing about workload, sources, detectors, or drift.
    Gate A is an observation boundary, never a serving or capture boundary.

What it is NOT:
    Not a workload path. No `serve()`, no Gate-B ingest, no Gate-C capture, and
    no search. Milvus is reached only through `_ReadOnlyMetadataReader`, which
    exposes exactly four metadata calls and has no `search` attribute at all --
    issuing a search is structurally impossible rather than merely forbidden.

Two modes, never one:
    `--mode preflight` validates the closed operand set, verifies the frozen
    source revision, inspects live metadata read-only, observes container
    lifetimes, re-derives every derived field, and prints the resolved plan with
    its `plan_sha256`. It creates nothing. `--mode execute` re-runs that entire
    preflight and then, only after the separate explicit `--confirm-initialize-v5`
    flag, initializes the campaign.

Create-once, never rebind:
    Gate A is the only boundary that brings a campaign into being. An existing
    campaign root is refused outright, so re-execution can neither overwrite nor
    rebind evidence, and no V1-V4 path is reachable. The campaign root is
    published by renaming a fully-fsynced staging directory into place, so a
    crash leaves either no campaign root or a complete one -- a partial V5
    cannot exist and is never accepted as a successful Gate A.

Authority:
    None. No policy, admission, grant, routing, activation, actuation, or
    candidate authority is created or imported. Gate C is untouched.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import sys
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .canonical_serialization import (
    CANONICAL_JSON_SCHEMA_VERSION,
    strict_canonical_digest,
    strict_canonical_json_bytes,
)
from .config import INDEX_NAME, IndexTrack, Metric
from .exp010_live_runner import (
    ENVIRONMENT_IDENTITY_SCHEMA_VERSION,
    build_environment_manifest_sha256,
)
from .exp010_serving_configuration import (
    EXP010_SERVING_CONFIGURATION_SCHEMA_VERSION,
    Exp010ServingConfiguration,
    derive_serving_configuration_identity,
    validate_governed_configuration_identity,
)
from .exp010_v2_host import pin_dataset001_identity

__all__ = [
    "GATE_A_EVIDENCE_FILENAME",
    "GATE_A_EVIDENCE_SCHEMA_VERSION",
    "GATE_A_EVIDENCE_SUBDIRECTORY",
    "GATE_A_PLAN_SCHEMA_VERSION",
    "OPERAND_FIELDS",
    "Exp010GateAObservation",
    "Exp010GateAOperands",
    "Exp010GateAOperatorError",
    "build_gate_a_evidence",
    "build_gate_a_plan",
    "initialize_v5_campaign",
    "load_operands",
    "main",
    "observe_environment",
]


GATE_A_PLAN_SCHEMA_VERSION = "exp010-gate-a-plan-v1"
GATE_A_EVIDENCE_SCHEMA_VERSION = "exp010-gate-a-evidence-v1"
_PLAN_DOMAIN = b"VD::EXP010_GATE_A_PLAN::V1\x00"
_EVIDENCE_DOMAIN = b"VD::EXP010_GATE_A_EVIDENCE::V1\x00"

#: Gate A owns these two path components and nothing else beneath the campaign
#: root. The five Gate-C stores stay Gate B's to initialize through genuine
#: ingest, so Gate A must never create them.
GATE_A_EVIDENCE_SUBDIRECTORY = "gate_a"
GATE_A_EVIDENCE_FILENAME = "gate_a_environment_manifest.json"

#: The exact, closed operand set. Anything else is refused, never defaulted.
#: `deployment_identity` leads deliberately: ADR-017 item 3 makes it a governed
#: operator input with no default, because no committed authority assigns it a
#: value and this module must not invent one.
OPERAND_FIELDS = (
    "deployment_identity",
    "stream_id",
    "campaign_root",
    "milvus_uri",
    "flat_collection_name",
    "hnsw_collection_name",
    "metric",
    "threshold_stratum",
    "threshold_radius",
    "range_filter",
    "limit",
    "served_ef",
    "dimensions",
    "consistency_level",
    "configuration_identity",
    "flat_binding_id",
    "hnsw_binding_id",
    "source_revision",
    "expected_row_count",
    "hnsw_m",
    "hnsw_ef_construction",
    "dataset001_dir",
    "etcd_container",
    "minio_container",
    "milvus_container",
)

_SHA256_LENGTH = 64
_GIT_REVISION_LENGTH = 40
_HEX = frozenset("0123456789abcdef")

#: Reachable only through these four names. Deliberately no `search`.
_METADATA_CALLS = (
    "describe_collection",
    "describe_index",
    "get_collection_stats",
    "get_load_state",
)


class Exp010GateAOperatorError(RuntimeError):
    """Fail-closed operator error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> Exp010GateAOperatorError:
    return Exp010GateAOperatorError(code, message)


# --------------------------------------------------------------------------
# Operands
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Exp010GateAOperands:
    """One fully validated, self-consistent Gate-A operand set."""

    deployment_identity: str
    stream_id: str
    campaign_root: Path
    milvus_uri: str
    flat_collection_name: str
    hnsw_collection_name: str
    metric: Metric
    threshold_stratum: str
    threshold_radius: float
    range_filter: float
    limit: int
    served_ef: int
    dimensions: int
    consistency_level: str
    configuration_identity: str
    flat_binding_id: str
    hnsw_binding_id: str
    source_revision: str
    expected_row_count: int
    hnsw_m: int
    hnsw_ef_construction: int
    dataset001_dir: Path
    etcd_container: str
    minio_container: str
    milvus_container: str

    @property
    def serving_configuration(self) -> Exp010ServingConfiguration:
        return Exp010ServingConfiguration(
            metric=self.metric,
            threshold_stratum=self.threshold_stratum,
            threshold_radius=self.threshold_radius,
            range_filter=self.range_filter,
            limit=self.limit,
            served_ef=self.served_ef,
            dimensions=self.dimensions,
            consistency_level=self.consistency_level,
        )

    @property
    def evidence_directory(self) -> Path:
        return self.campaign_root / GATE_A_EVIDENCE_SUBDIRECTORY

    @property
    def evidence_path(self) -> Path:
        return self.evidence_directory / GATE_A_EVIDENCE_FILENAME


def _text(values: Mapping[str, Any], name: str) -> str:
    value = values[name]
    if type(value) is not str or not value or value != value.strip():
        raise _error("GATE_A_OPERAND_INVALID", name)
    return value


def _exact_int(values: Mapping[str, Any], name: str) -> int:
    value = values[name]
    if isinstance(value, bool) or type(value) is not int:
        raise _error("GATE_A_OPERAND_INVALID", name)
    return value


def _positive_int(values: Mapping[str, Any], name: str) -> int:
    value = _exact_int(values, name)
    if value <= 0:
        raise _error("GATE_A_OPERAND_INVALID", name)
    return value


def _real(values: Mapping[str, Any], name: str) -> float:
    value = values[name]
    if isinstance(value, bool) or type(value) not in (int, float):
        raise _error("GATE_A_OPERAND_INVALID", name)
    return float(value)


def _container_name(values: Mapping[str, Any], name: str) -> str:
    value = _text(values, name)
    if "/" in value:
        raise _error("GATE_A_OPERAND_INVALID", name)
    return value


def _git_revision(values: Mapping[str, Any], name: str) -> str:
    value = _text(values, name)
    if len(value) != _GIT_REVISION_LENGTH or any(char not in _HEX for char in value):
        raise _error("GATE_A_OPERAND_INVALID", name)
    return value


def load_operands(path: str | os.PathLike[str]) -> Exp010GateAOperands:
    """Load and fully cross-validate one operand file, contacting nothing.

    Every operand is required and no operand is defaulted. As in Gate C,
    `configuration_identity` is *re-derived* from the serving operands rather
    than trusted, so a campaign can never be initialized under an identity its
    own configuration does not actually produce.
    """

    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise _error("GATE_A_OPERANDS_UNREADABLE", str(exc)) from exc
    try:
        values = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _error("GATE_A_OPERANDS_MALFORMED", str(exc)) from exc
    if type(values) is not dict:
        raise _error("GATE_A_OPERANDS_MALFORMED", "operands must be a JSON object")
    missing = sorted(set(OPERAND_FIELDS) - set(values))
    if missing:
        raise _error("GATE_A_OPERANDS_INCOMPLETE", ",".join(missing))
    unexpected = sorted(set(values) - set(OPERAND_FIELDS))
    if unexpected:
        raise _error("GATE_A_OPERANDS_UNEXPECTED", ",".join(unexpected))

    try:
        metric = Metric(_text(values, "metric"))
    except ValueError as exc:
        raise _error("GATE_A_OPERAND_INVALID", "metric") from exc

    # Syntax first, derivation second -- exactly the Gate-C ordering.
    configuration_identity = validate_governed_configuration_identity(
        values["configuration_identity"]
    )

    operands = Exp010GateAOperands(
        deployment_identity=_text(values, "deployment_identity"),
        stream_id=_text(values, "stream_id"),
        campaign_root=Path(_text(values, "campaign_root")),
        milvus_uri=_text(values, "milvus_uri"),
        flat_collection_name=_text(values, "flat_collection_name"),
        hnsw_collection_name=_text(values, "hnsw_collection_name"),
        metric=metric,
        threshold_stratum=_text(values, "threshold_stratum"),
        threshold_radius=_real(values, "threshold_radius"),
        range_filter=_real(values, "range_filter"),
        limit=_positive_int(values, "limit"),
        served_ef=_positive_int(values, "served_ef"),
        dimensions=_positive_int(values, "dimensions"),
        consistency_level=_text(values, "consistency_level"),
        configuration_identity=configuration_identity,
        flat_binding_id=_text(values, "flat_binding_id"),
        hnsw_binding_id=_text(values, "hnsw_binding_id"),
        source_revision=_git_revision(values, "source_revision"),
        expected_row_count=_positive_int(values, "expected_row_count"),
        hnsw_m=_positive_int(values, "hnsw_m"),
        hnsw_ef_construction=_positive_int(values, "hnsw_ef_construction"),
        dataset001_dir=Path(_text(values, "dataset001_dir")),
        etcd_container=_container_name(values, "etcd_container"),
        minio_container=_container_name(values, "minio_container"),
        milvus_container=_container_name(values, "milvus_container"),
    )

    derived = derive_serving_configuration_identity(operands.serving_configuration)
    if derived != operands.configuration_identity:
        raise _error(
            "GATE_A_CONFIGURATION_IDENTITY_MISMATCH", f"operands derive {derived}"
        )
    if operands.flat_collection_name == operands.hnsw_collection_name:
        raise _error("GATE_A_OPERAND_INVALID", "collection names must differ")
    if operands.flat_binding_id == operands.hnsw_binding_id:
        raise _error("GATE_A_OPERAND_INVALID", "binding ids must differ")
    if not operands.campaign_root.is_absolute():
        raise _error("GATE_A_OPERAND_INVALID", "campaign_root must be absolute")
    if operands.campaign_root != Path(os.path.normpath(operands.campaign_root)):
        raise _error("GATE_A_CAMPAIGN_ROOT_UNSAFE", "campaign_root must be normalized")
    return operands


# --------------------------------------------------------------------------
# Live observation
# --------------------------------------------------------------------------


class _ReadOnlyMetadataReader:
    """The only path to Milvus, and it cannot search.

    ADR-017 item 10: this wrapper forwards exactly the four metadata calls and
    defines no `search` attribute, so a physical query is unreachable by
    construction rather than by convention.
    """

    def __init__(self, client: object) -> None:
        for name in _METADATA_CALLS:
            if not callable(getattr(client, name, None)):
                raise _error("GATE_A_METADATA_READER_INVALID", name)
        self._client = client

    def describe_collection(self, name: str) -> Mapping[str, Any]:
        return self._client.describe_collection(collection_name=name)

    def describe_index(self, name: str, index_name: str) -> Mapping[str, Any]:
        return self._client.describe_index(
            collection_name=name, index_name=index_name
        )

    def get_collection_stats(self, name: str) -> Mapping[str, Any]:
        return self._client.get_collection_stats(collection_name=name)

    def get_load_state(self, name: str) -> Mapping[str, Any]:
        return self._client.get_load_state(collection_name=name)


@dataclass(frozen=True, slots=True)
class Exp010GateAObservation:
    """One complete, freshly observed Gate-A environment observation."""

    observed_at_utc: str
    data_identity: str
    generation_manifest_sha256: str
    base_vectors_sha256: str
    dataset_version: str
    flat: Mapping[str, Any]
    hnsw: Mapping[str, Any]
    containers: Mapping[str, Any]
    environment_manifest_sha256: str
    environment_observation: Mapping[str, Any]


ContainerInspector = Callable[[str], object]


def _default_container_inspector(socket_path: str | os.PathLike[str] | None = None):
    """Reuse the committed Docker socket transport rather than add a second one.

    `DockerSocketHealthProbe` already owns the subprocess-free Unix-socket
    inspect used by the live query path. Gate A needs the whole inspect
    document (lifetime, not just health), so it borrows that transport instead
    of introducing a competing Docker client.
    """

    from .docker_health import DockerSocketHealthProbe

    probe = DockerSocketHealthProbe(
        etcd_container="placeholder",
        minio_container="placeholder",
        socket_path=socket_path,
    )
    # Deliberate single-transport reuse: borrowing the probe's bound socket
    # reader is preferable to standing up a competing Docker client here.
    return probe._inspect_via_socket


def _resolve_repository_revision(repository_root: Path) -> str:
    """Read the frozen revision from a git checkout without spawning a process."""

    git_dir = repository_root / ".git"
    try:
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise _error("GATE_A_SOURCE_REVISION_UNVERIFIABLE", str(exc)) from exc
    if not head.startswith("ref: "):
        candidate = head
    else:
        reference = head.removeprefix("ref: ").strip()
        try:
            candidate = (git_dir / reference).read_text(encoding="utf-8").strip()
        except OSError:
            candidate = ""
            try:
                packed = (git_dir / "packed-refs").read_text(encoding="utf-8")
            except OSError as exc:
                raise _error("GATE_A_SOURCE_REVISION_UNVERIFIABLE", str(exc)) from exc
            for line in packed.splitlines():
                if line.startswith("#") or " " not in line:
                    continue
                value, _, name = line.partition(" ")
                if name.strip() == reference:
                    candidate = value.strip()
                    break
    if len(candidate) != _GIT_REVISION_LENGTH or any(
        char not in _HEX for char in candidate
    ):
        raise _error("GATE_A_SOURCE_REVISION_UNVERIFIABLE", "HEAD is not a full sha")
    return candidate


def _require(condition: bool, code: str, detail: str) -> None:
    if not condition:
        raise _error(code, detail)


def _observe_collection(
    reader: _ReadOnlyMetadataReader,
    *,
    name: str,
    track: IndexTrack,
    operands: Exp010GateAOperands,
) -> dict[str, Any]:
    """Read one collection's live metadata and hold it to the operand contract."""

    try:
        description = reader.describe_collection(name)
        stats = reader.get_collection_stats(name)
        load_state = reader.get_load_state(name)
        index = reader.describe_index(name, INDEX_NAME)
    except Exp010GateAOperatorError:
        raise
    except Exception as exc:  # live boundary is deliberately fail-closed
        raise _error("GATE_A_LIVE_METADATA_UNAVAILABLE", f"{name}: {exc}") from exc

    if not isinstance(description, Mapping) or not isinstance(index, Mapping):
        raise _error("GATE_A_LIVE_METADATA_INVALID", name)

    fields = description.get("fields")
    dimensions: int | None = None
    if isinstance(fields, (list, tuple)):
        for field in fields:
            if not isinstance(field, Mapping):
                continue
            params = field.get("params")
            if isinstance(params, Mapping) and isinstance(params.get("dim"), int):
                dimensions = int(params["dim"])
                break

    row_count = stats.get("row_count") if isinstance(stats, Mapping) else None
    state = load_state.get("state") if isinstance(load_state, Mapping) else None
    load_text = state if isinstance(state, str) else str(state)

    _require(
        description.get("collection_name", name) == name,
        "GATE_A_COLLECTION_NAME_MISMATCH",
        name,
    )
    _require(
        isinstance(row_count, int) and not isinstance(row_count, bool),
        "GATE_A_LIVE_METADATA_INVALID",
        f"{name}: row_count",
    )
    _require(
        row_count == operands.expected_row_count,
        "GATE_A_ROW_COUNT_MISMATCH",
        f"{name}: {row_count} != {operands.expected_row_count}",
    )
    _require(
        dimensions == operands.dimensions,
        "GATE_A_DIMENSION_MISMATCH",
        f"{name}: {dimensions} != {operands.dimensions}",
    )
    _require(
        index.get("metric_type") == operands.metric.value,
        "GATE_A_METRIC_MISMATCH",
        f"{name}: {index.get('metric_type')}",
    )
    _require(
        index.get("index_type") == track.value,
        "GATE_A_INDEX_TYPE_MISMATCH",
        f"{name}: {index.get('index_type')} != {track.value}",
    )
    _require(
        index.get("state") == "Finished",
        "GATE_A_INDEX_NOT_FINISHED",
        f"{name}: {index.get('state')}",
    )
    _require(
        index.get("indexed_rows") == operands.expected_row_count,
        "GATE_A_INDEX_INCOMPLETE",
        f"{name}: indexed_rows={index.get('indexed_rows')}",
    )
    _require(
        index.get("pending_index_rows") == 0,
        "GATE_A_INDEX_INCOMPLETE",
        f"{name}: pending={index.get('pending_index_rows')}",
    )
    _require(
        load_text == "Loaded",
        "GATE_A_COLLECTION_NOT_LOADED",
        f"{name}: {load_text}",
    )

    observed: dict[str, Any] = {
        "collection_name": name,
        "index_name": INDEX_NAME,
        "index_type": track.value,
        "metric_type": operands.metric.value,
        "row_count": int(row_count),
        "dimensions": int(operands.dimensions),
        "indexed_rows": int(operands.expected_row_count),
        "pending_index_rows": 0,
        "index_state": "Finished",
        "load_state": "Loaded",
    }
    if track is IndexTrack.HNSW:
        _require(
            str(index.get("M")) == str(operands.hnsw_m),
            "GATE_A_HNSW_M_MISMATCH",
            f"{name}: M={index.get('M')} != {operands.hnsw_m}",
        )
        _require(
            str(index.get("efConstruction")) == str(operands.hnsw_ef_construction),
            "GATE_A_HNSW_EF_CONSTRUCTION_MISMATCH",
            f"{name}: efConstruction={index.get('efConstruction')}",
        )
        observed["M"] = int(operands.hnsw_m)
        observed["efConstruction"] = int(operands.hnsw_ef_construction)
    return observed


def _observe_container(inspector: ContainerInspector, name: str) -> dict[str, Any]:
    """Record one container's live lifetime identity and require it serviceable."""

    try:
        document = inspector(name)
    except Exception as exc:  # live boundary is deliberately fail-closed
        raise _error("GATE_A_CONTAINER_UNAVAILABLE", f"{name}: {exc}") from exc
    if not isinstance(document, Mapping):
        raise _error("GATE_A_CONTAINER_INVALID", name)
    state = document.get("State")
    if not isinstance(state, Mapping):
        raise _error("GATE_A_CONTAINER_INVALID", f"{name}: State")
    health = state.get("Health")
    health_status = (
        health.get("Status") if isinstance(health, Mapping) else None
    )
    container_id = document.get("Id")
    started_at = state.get("StartedAt")
    restart_count = document.get("RestartCount")
    oom_killed = state.get("OOMKilled")
    if (
        not isinstance(container_id, str)
        or not container_id
        or not isinstance(started_at, str)
        or not started_at
        or isinstance(restart_count, bool)
        or not isinstance(restart_count, int)
    ):
        raise _error("GATE_A_CONTAINER_INVALID", f"{name}: lifetime fields")
    _require(
        state.get("Status") == "running",
        "GATE_A_CONTAINER_NOT_RUNNING",
        f"{name}: {state.get('Status')}",
    )
    _require(oom_killed is False, "GATE_A_CONTAINER_OOM_KILLED", name)
    _require(
        health_status is None or health_status == "healthy",
        "GATE_A_CONTAINER_NOT_HEALTHY",
        f"{name}: {health_status}",
    )
    return {
        "container_name": name,
        "container_id": container_id,
        "status": "running",
        "health": health_status if isinstance(health_status, str) else "none",
        "started_at": started_at,
        "restart_count": int(restart_count),
        "oom_killed": False,
    }


def observe_environment(
    operands: Exp010GateAOperands,
    *,
    metadata_reader: object | None = None,
    container_inspector: ContainerInspector | None = None,
    revision_resolver: Callable[[], str] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> Exp010GateAObservation:
    """Freshly observe the live stack and reduce it to one environment digest.

    Ordering is the contract. The frozen source revision is verified first, then
    the corpus is pinned, then live metadata is read, then container lifetimes.
    Every derived field is recomputed here; none is accepted from the operator.
    """

    resolver = revision_resolver or (
        lambda: _resolve_repository_revision(Path(__file__).resolve().parents[2])
    )
    live_revision = resolver()
    if live_revision != operands.source_revision:
        raise _error(
            "GATE_A_SOURCE_REVISION_MISMATCH",
            f"live {live_revision} != operand {operands.source_revision}",
        )

    dataset = pin_dataset001_identity(operands.dataset001_dir)
    if dataset.dimensions != operands.dimensions:
        raise _error(
            "GATE_A_DIMENSION_MISMATCH",
            f"corpus {dataset.dimensions} != operand {operands.dimensions}",
        )

    client = metadata_reader
    if client is None:
        from .v2_milvus_shadow_capture import build_readonly_milvus_client

        client = build_readonly_milvus_client(operands.milvus_uri)
    reader = _ReadOnlyMetadataReader(client)

    flat = _observe_collection(
        reader,
        name=operands.flat_collection_name,
        track=IndexTrack.FLAT,
        operands=operands,
    )
    hnsw = _observe_collection(
        reader,
        name=operands.hnsw_collection_name,
        track=IndexTrack.HNSW,
        operands=operands,
    )

    inspector = container_inspector or _default_container_inspector()
    containers = {
        "etcd": _observe_container(inspector, operands.etcd_container),
        "minio": _observe_container(inspector, operands.minio_container),
        "milvus": _observe_container(inspector, operands.milvus_container),
    }

    now = (clock or (lambda: datetime.now(UTC)))()
    if now.tzinfo is None:
        raise _error("GATE_A_CLOCK_INVALID", "clock must be timezone-aware UTC")
    observed_at_utc = now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    # The exact thirteen `_ENVIRONMENT_FIELDS`. `flat_index_identity` and
    # `hnsw_index_identity` *are* the binding ids: `v2_milvus_shadow_capture`
    # passes `stream_key.flat_binding_id`/`hnsw_binding_id` under exactly those
    # names, so this is the committed equivalence, not a new convention.
    environment_observation: dict[str, Any] = {
        "milvus_uri": operands.milvus_uri,
        "deployment_identity": operands.deployment_identity,
        "flat_collection_name": operands.flat_collection_name,
        "hnsw_collection_name": operands.hnsw_collection_name,
        "metric": operands.metric,
        "threshold_stratum": operands.threshold_stratum,
        "dimensions": operands.dimensions,
        "flat_index_identity": operands.flat_binding_id,
        "hnsw_index_identity": operands.hnsw_binding_id,
        "data_identity": dataset.data_identity,
        "source_revision": operands.source_revision,
        "served_ef": operands.served_ef,
        "observed_at_utc": observed_at_utc,
    }
    environment_manifest_sha256 = build_environment_manifest_sha256(
        environment_observation
    )
    return Exp010GateAObservation(
        observed_at_utc=observed_at_utc,
        data_identity=dataset.data_identity,
        generation_manifest_sha256=dataset.generation_manifest_sha256,
        base_vectors_sha256=dataset.base_vectors_sha256,
        dataset_version=dataset.version,
        flat=flat,
        hnsw=hnsw,
        containers=containers,
        environment_manifest_sha256=environment_manifest_sha256,
        environment_observation={
            **environment_observation,
            "metric": operands.metric.value,
        },
    )


# --------------------------------------------------------------------------
# Plan and evidence
# --------------------------------------------------------------------------


def build_gate_a_evidence(
    operands: Exp010GateAOperands, observation: Exp010GateAObservation
) -> dict[str, Any]:
    """Assemble the immutable Gate-A evidence document.

    Self-sufficient by construction: everything needed to independently
    recompute `environment_manifest_sha256` is present, so a later auditor never
    has to trust this file's own summary.
    """

    evidence: dict[str, Any] = {
        "schema_version": GATE_A_EVIDENCE_SCHEMA_VERSION,
        "serialization_contract": CANONICAL_JSON_SCHEMA_VERSION,
        "environment_identity_schema_version": ENVIRONMENT_IDENTITY_SCHEMA_VERSION,
        "serving_configuration_schema_version": (
            EXP010_SERVING_CONFIGURATION_SCHEMA_VERSION
        ),
        "canonical_operator": "vdbench.exp010_gate_a_operator",
        "canonical_digest_helper": (
            "vdbench.exp010_live_runner.build_environment_manifest_sha256"
        ),
        "gate": "A",
        "campaign": {
            "stream_id": operands.stream_id,
            "campaign_root": str(operands.campaign_root),
            "deployment_identity": operands.deployment_identity,
        },
        "source_revision": operands.source_revision,
        "observed_at_utc": observation.observed_at_utc,
        "dataset": {
            "dataset001_dir": str(operands.dataset001_dir),
            "version": observation.dataset_version,
            "data_identity": observation.data_identity,
            "generation_manifest_sha256": observation.generation_manifest_sha256,
            "base_vectors_sha256": observation.base_vectors_sha256,
        },
        "serving": {
            "configuration_identity": operands.configuration_identity,
            "metric": operands.metric.value,
            "threshold_stratum": operands.threshold_stratum,
            "threshold_radius": operands.threshold_radius,
            "range_filter": operands.range_filter,
            "limit": operands.limit,
            "served_ef": operands.served_ef,
            "dimensions": operands.dimensions,
            "consistency_level": operands.consistency_level,
        },
        "flat": {
            "binding_id": operands.flat_binding_id,
            "index_identity": operands.flat_binding_id,
            "live": dict(observation.flat),
        },
        "hnsw": {
            "binding_id": operands.hnsw_binding_id,
            "index_identity": operands.hnsw_binding_id,
            "live": dict(observation.hnsw),
        },
        "milvus": {
            "uri": operands.milvus_uri,
            "flat_collection_name": operands.flat_collection_name,
            "hnsw_collection_name": operands.hnsw_collection_name,
        },
        "containers": {
            key: dict(value) for key, value in observation.containers.items()
        },
        "environment_observation": dict(observation.environment_observation),
        "environment_manifest_sha256": observation.environment_manifest_sha256,
        "physical_searches_issued_by_gate_a": 0,
        "serve_calls_issued_by_gate_a": 0,
    }
    evidence["evidence_sha256"] = strict_canonical_digest(_EVIDENCE_DOMAIN, evidence)
    return evidence


def build_gate_a_plan(
    operands: Exp010GateAOperands, observation: Exp010GateAObservation
) -> dict[str, Any]:
    """Resolve exactly what execute would write, having written nothing."""

    evidence = build_gate_a_evidence(operands, observation)
    plan: dict[str, Any] = {
        "schema_version": GATE_A_PLAN_SCHEMA_VERSION,
        "serialization_contract": CANONICAL_JSON_SCHEMA_VERSION,
        "canonical_entrypoint": "vdbench.exp010_gate_a_operator.initialize_v5_campaign",
        "gate": "A",
        "campaign_root": str(operands.campaign_root),
        "campaign_root_exists": operands.campaign_root.exists(),
        "would_create": [
            str(operands.campaign_root),
            str(operands.evidence_directory),
            str(operands.evidence_path),
        ],
        "would_not_create": {
            "gate_b_stores": True,
            "gate_c_evidence": True,
        },
        "environment_manifest_sha256": observation.environment_manifest_sha256,
        "evidence_sha256": evidence["evidence_sha256"],
        "observed_at_utc": observation.observed_at_utc,
        "source_revision": operands.source_revision,
        "physical_searches_issued_by_preflight": 0,
        "serve_calls_issued_by_preflight": 0,
        "evidence": evidence,
    }
    plan["plan_sha256"] = strict_canonical_digest(_PLAN_DOMAIN, plan)
    return plan


# --------------------------------------------------------------------------
# Atomic V5 initialization
# --------------------------------------------------------------------------


def _reject_unsafe_parent(parent: Path) -> None:
    if parent.is_symlink():
        raise _error("GATE_A_CAMPAIGN_ROOT_UNSAFE", "parent is a symlink")
    if not parent.is_dir():
        raise _error("GATE_A_CAMPAIGN_PARENT_MISSING", str(parent))
    details = parent.stat(follow_symlinks=False)
    if details.st_uid != os.getuid() or stat.S_IMODE(details.st_mode) & 0o022:
        raise _error("GATE_A_CAMPAIGN_ROOT_UNSAFE", "parent is group/world writable")


def initialize_v5_campaign(
    operands: Exp010GateAOperands,
    observation: Exp010GateAObservation,
    *,
    _publish: Callable[[Path, Path], None] | None = None,
) -> dict[str, Any]:
    """Create the campaign exactly once, whole or not at all.

    The evidence is written and fsynced inside a staging directory that lives
    beside the campaign root, and the staging directory is then renamed into
    place. A crash at any point leaves either no campaign root or a complete
    one -- there is no window in which a partial V5 is visible.

    Concurrency boundary, stated precisely because `os.rename` is subtler here
    than it looks: renaming onto an existing *non-empty* directory fails with
    ENOTEMPTY, but renaming onto an existing *empty* one succeeds and replaces
    it. Two racing Gate-A operators are therefore still safe, because every
    campaign root Gate A publishes contains `gate_a/` and is never empty, so the
    loser's rename always fails. The residual window is narrower than
    create-once: an unrelated process creating a bare empty directory at this
    exact path between the pre-check and the rename would be replaced silently.
    No Gate-A evidence can be lost that way -- an empty root holds none -- and a
    root that exists without complete evidence is an error state, never a PASS.
    """

    root = operands.campaign_root
    if root.exists() or root.is_symlink():
        raise _error("GATE_A_CAMPAIGN_ALREADY_INITIALIZED", str(root))
    _reject_unsafe_parent(root.parent)

    evidence = build_gate_a_evidence(operands, observation)
    payload = strict_canonical_json_bytes(evidence)

    staging = Path(
        tempfile.mkdtemp(prefix=f".{root.name}.", suffix=".partial", dir=root.parent)
    )
    try:
        evidence_directory = staging / GATE_A_EVIDENCE_SUBDIRECTORY
        evidence_directory.mkdir(mode=0o700)
        target = evidence_directory / GATE_A_EVIDENCE_FILENAME
        descriptor = os.open(
            target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        for directory in (evidence_directory, staging):
            handle_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(handle_fd)
            finally:
                os.close(handle_fd)
        publish = _publish or os.rename
        publish(staging, root)
    except Exp010GateAOperatorError:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    except OSError as exc:
        shutil.rmtree(staging, ignore_errors=True)
        raise _error("GATE_A_CAMPAIGN_WRITE_FAILED", str(exc)) from exc
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    else:
        parent_fd = os.open(root.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    return evidence


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operands",
        type=Path,
        required=True,
        help="Path to the exact-keyed Gate-A operand JSON file.",
    )
    parser.add_argument(
        "--mode",
        choices=("preflight", "execute"),
        required=True,
        help=(
            "preflight observes and prints the plan and creates nothing; "
            "execute additionally initializes the V5 campaign exactly once."
        ),
    )
    parser.add_argument(
        "--confirm-initialize-v5",
        action="store_true",
        help=(
            "Required with --mode execute. A second, explicit operator action, "
            "deliberately separate from choosing the mode, acknowledging that a "
            "new campaign root and immutable Gate-A evidence will be created."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Real operator entry point; never invoked by this repository's own code.

    Ordering is the contract: operands are validated, the live environment is
    observed, and the plan is printed -- all before anything can be created. A
    mismatch therefore always fails with zero filesystem effect.
    """

    args = _parser().parse_args(argv)
    operands = load_operands(args.operands)
    observation = observe_environment(operands)
    plan = build_gate_a_plan(operands, observation)
    sys.stdout.write(strict_canonical_json_bytes(plan).decode("utf-8"))

    if args.mode == "preflight":
        return 0
    if not args.confirm_initialize_v5:
        raise _error(
            "GATE_A_INITIALIZATION_NOT_CONFIRMED",
            "--mode execute requires --confirm-initialize-v5",
        )
    evidence = initialize_v5_campaign(operands, observation)
    summary = {
        "schema_version": GATE_A_EVIDENCE_SCHEMA_VERSION,
        "plan_sha256": plan["plan_sha256"],
        "campaign_root": str(operands.campaign_root),
        "evidence_path": str(operands.evidence_path),
        "environment_manifest_sha256": evidence["environment_manifest_sha256"],
        "evidence_sha256": evidence["evidence_sha256"],
    }
    sys.stdout.write(strict_canonical_json_bytes(summary).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
