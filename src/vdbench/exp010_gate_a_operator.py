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
    Gate A is the only boundary that brings a campaign into being. The campaign
    root is claimed with an exclusive `os.mkdir`, which fails against anything
    already at the path -- empty directory, non-empty directory, file, symlink,
    or dangling symlink alike. Exclusivity is therefore decided by the creating
    syscall rather than by an earlier `exists()` check, so nothing at the path
    can ever be replaced and no V1-V4 root is reachable. Evidence is then
    published into the reserved root by one atomic rename, and an incompleteness
    marker makes any interrupted reservation positively identifiable and never
    a PASS. See `initialize_v5_campaign` for the full protocol and its crash and
    recovery semantics.

Authority:
    None. No policy, admission, grant, routing, activation, actuation, or
    candidate authority is created or imported. Gate C is untouched.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import tempfile
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .canonical_serialization import (
    CANONICAL_JSON_SCHEMA_VERSION,
    CanonicalSerializationError,
    decode_strict_canonical_json,
    strict_canonical_digest,
    strict_canonical_json_bytes,
)
from .config import INDEX_NAME, IndexTrack, Metric
from .exp010_live_runner import (
    ENVIRONMENT_IDENTITY_SCHEMA_VERSION,
    Exp010LiveRunnerError,
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
    "CAMPAIGN_ABSENT",
    "CAMPAIGN_COMPLETE",
    "CAMPAIGN_FOREIGN",
    "CAMPAIGN_INCOMPLETE",
    "GATE_A_EVIDENCE_FILENAME",
    "GATE_A_EVIDENCE_SCHEMA_VERSION",
    "GATE_A_EVIDENCE_SUBDIRECTORY",
    "GATE_A_INCOMPLETE_MARKER",
    "GATE_A_PLAN_SCHEMA_VERSION",
    "GATE_A_RESERVATION_SCHEMA_VERSION",
    "OPERAND_FIELDS",
    "Exp010GateAObservation",
    "Exp010GateAOperands",
    "Exp010GateAOperatorError",
    "build_gate_a_evidence",
    "build_gate_a_plan",
    "derive_downstream_authority",
    "initialize_v5_campaign",
    "inspect_campaign_state",
    "load_operands",
    "load_verified_gate_a_evidence",
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

#: Present for exactly as long as a reserved campaign root is not yet complete.
#: Its removal is the commit point, so its presence always means "not a PASS".
GATE_A_INCOMPLETE_MARKER = ".gate_a_incomplete"
GATE_A_RESERVATION_SCHEMA_VERSION = "exp010-gate-a-reservation-v1"

#: The four states a campaign root can be in. Only ABSENT may be initialized,
#: and only COMPLETE is ever a successful Gate A.
CAMPAIGN_ABSENT = "ABSENT"
CAMPAIGN_INCOMPLETE = "INCOMPLETE"
CAMPAIGN_COMPLETE = "COMPLETE"
CAMPAIGN_FOREIGN = "FOREIGN"

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


#: Top-level keys a verified Gate-A evidence document must carry.
_EVIDENCE_REQUIRED_KEYS = frozenset(
    {
        "schema_version",
        "gate",
        "campaign",
        "source_revision",
        "observed_at_utc",
        "dataset",
        "serving",
        "flat",
        "hnsw",
        "milvus",
        "containers",
        "environment_observation",
        "environment_manifest_sha256",
        "evidence_sha256",
    }
)


def load_verified_gate_a_evidence(
    campaign_root: str | os.PathLike[str],
) -> dict[str, Any]:
    """Turn persisted Gate-A evidence into usable authority, or fail closed.

    `inspect_campaign_state` answers a *structural* question and deliberately
    validates nothing. This is the counterpart that must be used by anything
    treating the manifest as authority: it decodes under the strict canonical
    contract (whose round-trip equality check rejects reordered keys, added
    whitespace, duplicate keys, and any other non-canonical byte sequence),
    recomputes both digests, and re-derives the serving identity rather than
    trusting the stored one. A malformed, truncated, substituted, or internally
    inconsistent document is refused rather than believed.
    """

    root = Path(campaign_root)
    state = inspect_campaign_state(root)
    if state != CAMPAIGN_COMPLETE:
        raise _error("GATE_A_EVIDENCE_NOT_COMPLETE", f"{root}: {state}")

    path = root / GATE_A_EVIDENCE_SUBDIRECTORY / GATE_A_EVIDENCE_FILENAME
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise _error("GATE_A_EVIDENCE_UNREADABLE", str(exc)) from exc
    try:
        document = decode_strict_canonical_json(raw)
    except CanonicalSerializationError as exc:
        raise _error("GATE_A_EVIDENCE_NOT_CANONICAL", str(exc)) from exc
    if type(document) is not dict:
        raise _error("GATE_A_EVIDENCE_MALFORMED", "document is not an object")

    if document.get("schema_version") != GATE_A_EVIDENCE_SCHEMA_VERSION:
        raise _error("GATE_A_EVIDENCE_SCHEMA_UNKNOWN", str(document.get("schema_version")))
    if document.get("gate") != "A":
        raise _error("GATE_A_EVIDENCE_MALFORMED", "gate is not A")
    missing = sorted(_EVIDENCE_REQUIRED_KEYS - set(document))
    if missing:
        raise _error("GATE_A_EVIDENCE_INCOMPLETE", ",".join(missing))

    # The stored digest is never trusted: it is recomputed over the exact
    # remaining document, so any substituted field changes the result.
    stated_digest = document.get("evidence_sha256")
    body = {key: value for key, value in document.items() if key != "evidence_sha256"}
    if not isinstance(stated_digest, str) or strict_canonical_digest(
        _EVIDENCE_DOMAIN, body
    ) != stated_digest:
        raise _error("GATE_A_EVIDENCE_DIGEST_MISMATCH", str(path))

    observation = document.get("environment_observation")
    if type(observation) is not dict:
        raise _error("GATE_A_EVIDENCE_MALFORMED", "environment_observation")
    try:
        recomputed = build_environment_manifest_sha256(observation)
    except Exp010LiveRunnerError as exc:
        raise _error("GATE_A_EVIDENCE_ENVIRONMENT_INVALID", exc.code) from exc
    if recomputed != document.get("environment_manifest_sha256"):
        raise _error("GATE_A_EVIDENCE_ENVIRONMENT_MISMATCH", str(path))

    campaign = document.get("campaign")
    serving = document.get("serving")
    dataset = document.get("dataset")
    flat = document.get("flat")
    hnsw = document.get("hnsw")
    for name, section in (
        ("campaign", campaign), ("serving", serving), ("dataset", dataset),
        ("flat", flat), ("hnsw", hnsw), ("milvus", document.get("milvus")),
    ):
        if type(section) is not dict:
            raise _error("GATE_A_EVIDENCE_MALFORMED", name)

    if campaign.get("campaign_root") != str(root):
        raise _error(
            "GATE_A_EVIDENCE_CAMPAIGN_MISMATCH",
            f"evidence binds {campaign.get('campaign_root')}, loaded from {root}",
        )

    # Re-derive rather than trust, exactly as Gate C does with its operands.
    try:
        derived_identity = derive_serving_configuration_identity(
            Exp010ServingConfiguration(
                metric=Metric(serving["metric"]),
                threshold_stratum=serving["threshold_stratum"],
                threshold_radius=serving["threshold_radius"],
                range_filter=serving["range_filter"],
                limit=serving["limit"],
                served_ef=serving["served_ef"],
                dimensions=serving["dimensions"],
                consistency_level=serving["consistency_level"],
            )
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("GATE_A_EVIDENCE_MALFORMED", "serving") from exc
    if derived_identity != serving.get("configuration_identity"):
        raise _error("GATE_A_EVIDENCE_CONFIGURATION_MISMATCH", derived_identity)

    # The environment observation is the digested object, so every other section
    # must agree with it or the document is internally inconsistent.
    for field, expected in (
        ("deployment_identity", campaign.get("deployment_identity")),
        ("source_revision", document.get("source_revision")),
        ("observed_at_utc", document.get("observed_at_utc")),
        ("metric", serving.get("metric")),
        ("threshold_stratum", serving.get("threshold_stratum")),
        ("served_ef", serving.get("served_ef")),
        ("dimensions", serving.get("dimensions")),
        ("data_identity", dataset.get("data_identity")),
        ("flat_index_identity", flat.get("binding_id")),
        ("hnsw_index_identity", hnsw.get("binding_id")),
        ("flat_collection_name", document["milvus"].get("flat_collection_name")),
        ("hnsw_collection_name", document["milvus"].get("hnsw_collection_name")),
        ("milvus_uri", document["milvus"].get("uri")),
    ):
        if observation.get(field) != expected:
            raise _error("GATE_A_EVIDENCE_INCONSISTENT", field)
    return document


def derive_downstream_authority(
    campaign_root: str | os.PathLike[str],
) -> dict[str, Any]:
    """Every authority field a downstream operand set must inherit, verified.

    ADR-017 item 9 makes authority flow one way. A downstream operand set is
    meant to be *built from* this mapping rather than typed, so the environment
    digest a later gate binds is necessarily the one Gate A observed and
    persisted, not a free-form string.
    """

    evidence = load_verified_gate_a_evidence(campaign_root)
    serving = evidence["serving"]
    milvus = evidence["milvus"]
    return {
        "stream_id": evidence["campaign"]["stream_id"],
        "environment_manifest_sha256": evidence["environment_manifest_sha256"],
        "configuration_identity": serving["configuration_identity"],
        "source_revision": evidence["source_revision"],
        "metric": serving["metric"],
        "threshold_stratum": serving["threshold_stratum"],
        "threshold_radius": serving["threshold_radius"],
        "range_filter": serving["range_filter"],
        "limit": serving["limit"],
        "served_ef": serving["served_ef"],
        "dimensions": serving["dimensions"],
        "consistency_level": serving["consistency_level"],
        "flat_binding_id": evidence["flat"]["binding_id"],
        "hnsw_binding_id": evidence["hnsw"]["binding_id"],
        "milvus_uri": milvus["uri"],
        "flat_collection_name": milvus["flat_collection_name"],
        "hnsw_collection_name": milvus["hnsw_collection_name"],
        "dataset001_dir": evidence["dataset"]["dataset001_dir"],
    }


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
        "campaign_state": inspect_campaign_state(operands.campaign_root),
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


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _release_reservation(
    root: Path,
    *,
    marker: Path | None,
    evidence_directory: Path | None,
    manifest: Path | None,
    staging: Path | None,
) -> None:
    """Undo only what this invocation created, and never recursively.

    Deliberately not `shutil.rmtree`. A recursive delete of the reservation
    would remove whatever happens to be inside it, which is not the operator's
    to destroy: between the reservation and a failure, a same-UID process can
    add files, directories, symlinks, or hard links under the campaign root.
    Nothing here is removed merely because it exists -- every argument is a path
    this invocation is known to have created, and directories are removed with
    `os.rmdir`, which refuses a non-empty directory rather than descending.

    If anything foreign is present, the marker is deliberately left in place, so
    the root stays INCOMPLETE and enters the governed recovery path instead of
    being silently cleared. Preserving a stranger's data and reporting an
    honest INCOMPLETE is strictly safer than a tidy filesystem.
    """

    for path in (staging, manifest):
        if path is not None:
            with suppress(OSError):
                path.unlink()
    if evidence_directory is not None:
        with suppress(OSError):
            os.rmdir(evidence_directory)  # refuses if anything foreign is inside
    if marker is not None:
        try:
            remaining = [name for name in os.listdir(root) if name != marker.name]
        except OSError:
            return
        if remaining:
            return  # foreign content: keep the marker, stay INCOMPLETE
        with suppress(OSError):
            marker.unlink()
    with suppress(OSError):
        os.rmdir(root)  # refuses if non-empty; never recursive


def inspect_campaign_state(campaign_root: str | os.PathLike[str]) -> str:
    """Classify a campaign root without modifying anything.

    This is the single place that decides whether a path may be initialized and
    whether an existing root represents a successful Gate A. It is deliberately
    conservative: anything it does not positively recognize as COMPLETE is not a
    PASS, and anything other than ABSENT is refused for initialization.

    COMPLETE is a *structural* classification, not a validity attestation. It
    means only: the incompleteness marker is absent and a regular (non-symlink)
    file exists at the canonical manifest path. It does **not** decode the
    manifest, check it against the strict canonical contract, recompute
    `evidence_sha256`, or recompute `environment_manifest_sha256`. A malformed,
    truncated, substituted, or internally inconsistent manifest would therefore
    still classify as COMPLETE.

    That is sound today only because nothing consumes this evidence as
    authority: the sole callers are `initialize_v5_campaign`, which uses it to
    *refuse*, and `build_gate_a_plan`, which reports it. Gate C takes
    `environment_manifest_sha256` from its own operand file, never from this
    directory. **Any future consumer that treats the manifest as authority must
    re-verify it itself** -- decode with `decode_strict_canonical_json`,
    recompute the evidence digest under `_EVIDENCE_DOMAIN`, and recompute the
    environment digest with `build_environment_manifest_sha256` -- because a
    COMPLETE classification is not evidence that any of those hold.
    """

    root = Path(campaign_root)
    if root.is_symlink():
        return CAMPAIGN_FOREIGN
    if not root.exists():
        return CAMPAIGN_ABSENT
    if not root.is_dir():
        return CAMPAIGN_FOREIGN
    if (root / GATE_A_INCOMPLETE_MARKER).exists():
        return CAMPAIGN_INCOMPLETE
    evidence = root / GATE_A_EVIDENCE_SUBDIRECTORY / GATE_A_EVIDENCE_FILENAME
    if evidence.is_file() and not evidence.is_symlink():
        return CAMPAIGN_COMPLETE
    return CAMPAIGN_FOREIGN


#: Refusal reason for every non-ABSENT state. Distinct codes, one refusal.
_STATE_REFUSALS = {
    CAMPAIGN_COMPLETE: "GATE_A_CAMPAIGN_ALREADY_INITIALIZED",
    CAMPAIGN_INCOMPLETE: "GATE_A_CAMPAIGN_INCOMPLETE",
    CAMPAIGN_FOREIGN: "GATE_A_CAMPAIGN_PATH_OCCUPIED",
}


def initialize_v5_campaign(
    operands: Exp010GateAOperands,
    observation: Exp010GateAObservation,
    *,
    _publish: Callable[[Path, Path], None] | None = None,
    _race_hook: Callable[[], None] | None = None,
    _nested_race_hook: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Reserve the campaign root exclusively, then publish evidence into it.

    Why not a bare rename: `os.rename` has *replace* semantics. Renaming onto an
    existing non-empty directory fails with ENOTEMPTY, but renaming onto an
    existing *empty* one silently removes and replaces it. Any design whose
    exclusivity rests on an earlier `exists()` check therefore carries a real
    TOCTOU window -- a directory appearing between the check and the rename
    would be destroyed.

    The protocol closes that window with `os.mkdir`, which is unconditionally
    exclusive: it fails with EEXIST against an empty directory, a non-empty
    directory, a regular file, a symlink, and a dangling symlink alike. No
    platform-specific no-replace rename (`renameat2`/`renamex_np`) and no ctypes
    binding is needed.

        1. verify the parent is a safe, owned, non-world-writable directory
        2. `os.mkdir(root, 0o700)`         <- exclusive reservation of the root
        3. write the incompleteness marker inside the reserved root, fsync
        4. `os.mkdir(root/gate_a, 0o700)`  <- exclusive creation of the
                                              evidence directory
        5. write the manifest to a temporary name inside `gate_a/`, fsync, then
           `os.link` it onto the canonical name and unlink the temporary
        6. unlink the marker, fsync root   <- the commit point

    Every step that creates a name uses a primitive that refuses to replace an
    existing one, so no pre-existing directory, file, or symlink is ever
    silently destroyed anywhere in the tree.

    Same-UID threat model, stated because a mode bit is easy to over-trust:
    `0o700` on the reserved root keeps other *users* out, but grants full rights
    to any process sharing this UID. Directory permissions therefore provide no
    protection against a hostile or buggy same-UID process, and this protocol
    does not rely on them. Steps 2, 4, and 5 use `os.mkdir` and `os.link`, both
    of which are unconditionally exclusive -- EEXIST against an empty directory,
    a non-empty directory, a regular file, a symlink, and a dangling symlink
    alike -- so a same-UID process that plants any of those at `root`,
    `root/gate_a`, or the manifest path inside the window is refused rather than
    replaced. No platform-specific no-replace rename (`renameat2`/`renamex_np`)
    and no ctypes binding is needed.

    `gate_a/` therefore becomes visible while the campaign is still marked
    INCOMPLETE. That is deliberate and safe: the marker is the sole transition
    to COMPLETE, so a visible-but-uncommitted evidence directory is never a
    Gate-A PASS. `os.link` additionally makes the manifest appear at its
    canonical name exactly once and only fully written, so a torn manifest is
    never observable there.

    Crash semantics. A crash before step 2 leaves nothing. A crash between steps
    2 and 6 leaves a reserved root carrying the marker, which
    `inspect_campaign_state` reports as INCOMPLETE and which is therefore never
    a Gate-A PASS -- including the conservative case where the evidence is
    actually present but step 6 did not run. A crash after step 6 leaves a
    COMPLETE root. On a clean failure the reservation is released only while the
    evidence is still unpublished; once published, this function never deletes
    it, so a completed campaign cannot be destroyed by a late error.

    Recovery is governed, never automatic: an INCOMPLETE root is refused with
    `GATE_A_CAMPAIGN_INCOMPLETE` and is never repaired, reused, or removed by
    this operator. An operator must quarantine it explicitly and re-run.
    """

    root = operands.campaign_root
    _reject_unsafe_parent(root.parent)

    # Advisory: it yields the precise reason code. The refusal itself does not
    # depend on it -- step 2 below is what actually decides exclusivity.
    state = inspect_campaign_state(root)
    if state != CAMPAIGN_ABSENT:
        raise _error(_STATE_REFUSALS[state], str(root))

    if _race_hook is not None:  # test seam: open the window deliberately
        _race_hook()

    evidence = build_gate_a_evidence(operands, observation)
    payload = strict_canonical_json_bytes(evidence)

    try:
        os.mkdir(root, 0o700)
    except FileExistsError as exc:
        # Something claimed the path inside the window. Re-classify for the
        # reason code; the refusal is unconditional either way.
        raced = inspect_campaign_state(root)
        raise _error(
            _STATE_REFUSALS.get(raced, "GATE_A_CAMPAIGN_PATH_OCCUPIED"), str(root)
        ) from exc
    except OSError as exc:
        raise _error("GATE_A_CAMPAIGN_WRITE_FAILED", str(exc)) from exc

    marker = root / GATE_A_INCOMPLETE_MARKER
    evidence_directory = root / GATE_A_EVIDENCE_SUBDIRECTORY
    manifest_path = evidence_directory / GATE_A_EVIDENCE_FILENAME
    staging: Path | None = None
    created_marker = False
    created_evidence_directory = False
    linked_manifest = False
    published = False
    try:
        reservation = strict_canonical_json_bytes(
            {
                "schema_version": GATE_A_RESERVATION_SCHEMA_VERSION,
                "campaign_root": str(root),
                "stream_id": operands.stream_id,
                "reserved_for_observed_at_utc": observation.observed_at_utc,
                "state": CAMPAIGN_INCOMPLETE,
                "note": (
                    "This campaign root is reserved but not complete. It is not "
                    "a successful Gate A and must never be treated as one. "
                    "Recovery is an explicit governed operator action: "
                    "quarantine this root and re-run Gate A."
                ),
            }
        )
        descriptor = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(reservation)
            handle.flush()
            os.fsync(handle.fileno())
        created_marker = True
        _fsync_directory(root)

        # Nested exclusivity. `mode=0o700` keeps other *users* out but grants
        # everything to any process sharing this UID, so the evidence directory
        # is created exclusively rather than renamed into place: `os.mkdir`
        # fails EEXIST against a directory, file, or symlink alike, so a
        # same-UID process that plants `gate_a/` inside the reservation window
        # is refused instead of silently replaced.
        if _nested_race_hook is not None:  # test seam: open the nested window
            _nested_race_hook("before_evidence_directory")
        os.mkdir(evidence_directory, 0o700)
        created_evidence_directory = True

        # The manifest is written to a temporary name inside the evidence
        # directory and published with `os.link`, which is likewise
        # unconditionally exclusive -- EEXIST against a file, a directory, and
        # every symlink flavour, and it never writes through a symlink. The
        # final name therefore appears exactly once and only fully written, so
        # a torn manifest is never visible at the canonical path. This mirrors
        # the durability discipline already used by `shadow_event_source`.
        staging_descriptor, staging_name = tempfile.mkstemp(
            prefix=f".{GATE_A_EVIDENCE_FILENAME}.", suffix=".tmp",
            dir=evidence_directory,
        )
        staging = Path(staging_name)
        with os.fdopen(staging_descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(staging, 0o400)

        if _nested_race_hook is not None:  # test seam: open the manifest window
            _nested_race_hook("before_manifest_link")
        publish = _publish or os.link
        publish(staging, manifest_path)
        linked_manifest = True
        os.unlink(staging)
        staging = None
        _fsync_directory(evidence_directory)
        _fsync_directory(root)
        published = True

        os.unlink(marker)  # commit point
        created_marker = False
        _fsync_directory(root)
        _fsync_directory(root.parent)
    except BaseException as exc:
        if published:
            # The campaign is committed. Never undo any of it for a late error;
            # only this invocation's own temporary name may still need removing.
            if staging is not None:
                with suppress(OSError):
                    staging.unlink()
        else:
            _release_reservation(
                root,
                marker=marker if created_marker else None,
                evidence_directory=(
                    evidence_directory if created_evidence_directory else None
                ),
                manifest=manifest_path if linked_manifest else None,
                staging=staging,
            )
        if isinstance(exc, Exp010GateAOperatorError):
            raise
        if isinstance(exc, FileExistsError):
            raise _error("GATE_A_EVIDENCE_PATH_OCCUPIED", str(exc)) from exc
        if isinstance(exc, OSError):
            raise _error("GATE_A_CAMPAIGN_WRITE_FAILED", str(exc)) from exc
        raise
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
