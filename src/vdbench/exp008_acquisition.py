"""Preflight-gated live DRY_RUN composition runner for EXP-008.

The runner is a composition root, not a new query, detector, or policy
implementation.  It joins the reviewed foreground-serving, host-observation,
read-only shadow, durable-outbox, and DRY_RUN monitor boundaries for the two
pre-registered stationary streams.  It never constructs an actuation boundary
and exposes no configuration-mutation operation.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
from types import MappingProxyType
from typing import Any, Protocol

import numpy as np

from .actuation import ShadowResult
from .artifacts import canonical_json_bytes, git_state, sha256_file, write_immutable_json
from .config import ENV001_PINS, RESULT_LIMIT, IndexTrack, Metric
from .exp005_acquisition import (
    Exp005AcquisitionError,
    IdentityBaseline,
    derive_exp005_identities,
    load_identity_baseline,
)
from .docker_health import DockerSocketHealthProbe
from .host_observation import (
    BackgroundShadowWorker,
    BoundedHostObservationRecorder,
    FileHostWorkerStateStore,
    RangeQueryRequest,
    RangeServingExecutor,
    ReferenceRangeGateway,
    RegisteredTraceParameters,
    ShadowAuditExecutor,
    WorkerCycleResult,
)
from .milvus_actuation import (
    ActuationWorkload,
    CanaryBoundEstimatorLike,
    CanaryBounds,
    MilvusActuationClient,
)
from .milvus_host_executor import HostShadowPlan, MilvusHostShadowExecutor
from .milvus_serving import (
    HostServingPlan,
    MilvusRangeServingExecutor,
    ServingPreflightResult,
)
from .monitor_audit import FileMonitorAuditSink
from .policy import (
    PolicyMode,
    PreActionSafety,
    QualificationResult,
)
from .runner import load_dataset
from .shadow_artifacts import load_persisted_shadow_trace_envelope
from .shadow_event_source import FileShadowTraceEventSource
from .shadow_event_types import MonitorStreamKey
from .shadow_window import assemble_shadow_window
from .workload_monitor import (
    DryRunPolicyInputProvider,
    DryRunPolicyInputs,
    FileMonitorStateStore,
    WorkloadMonitor,
)


__all__ = [
    "EXP008_DETECTOR_SEED",
    "EXP008AcquisitionError",
    "Exp008Configuration",
    "Exp008CaptureResult",
    "Exp008Runtime",
    "Exp008RunResult",
    "Exp008Stream",
    "build_live_runtime",
    "capture_host_resource_snapshot",
    "capture_exp008",
    "finalize_exp008",
    "prepare_exp008_configuration",
    "run_exp008",
]


EXP008_DETECTOR_SEED = 20260805
_WINDOW_COUNT = 3
_WINDOW_QUERY_COUNT = 200
_TRACE_QUERY_COUNT = 50
_TRACE_COUNT_PER_WINDOW = 4
_HOST_QUEUE_CAPACITY = 50
_WORKER_DRAIN_LIMIT = 50
_MAX_PARTIAL_STREAMS = 2
_MAX_OBSERVATION_AGE_SECONDS = 60.0
_OUTBOX_PENDING_EVENT_CAPACITY = 28
_OUTBOX_PENDING_BYTE_CAPACITY = 16_777_216
_MONITOR_POLL_LIMIT = 28
_RUN_SCHEMA_VERSION = "exp008-live-dry-run-v1"
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class EXP008AcquisitionError(RuntimeError):
    """Non-sensitive failure raised when EXP-008 evidence is incomplete."""


@dataclass(frozen=True, slots=True)
class Exp008Stream:
    """One frozen lineage that must never share a configuration identity."""

    metric: Metric
    threshold_stratum: str
    candidate_ef: int
    last_known_good_ef: int
    served_ef: int
    sentinel_ef: int
    threshold_radius: float
    baseline: IdentityBaseline
    baseline_path: Path
    baseline_file_sha256: str
    stream_key: MonitorStreamKey


@dataclass(frozen=True, slots=True)
class Exp008Configuration:
    """Validated immutable dataset and reviewed stream-baseline inputs."""

    dataset_dir: Path
    dataset_manifest: Mapping[str, object]
    dataset_manifest_sha256: str
    base_ids: np.ndarray
    base_vectors: np.ndarray
    measured_queries: np.ndarray
    streams: tuple[Exp008Stream, ...]

    @property
    def measured_query_count(self) -> int:
        return int(self.measured_queries.shape[0])


@dataclass(frozen=True, slots=True)
class Exp008RunResult:
    """Compact pointer-only summary of a completed immutable run directory."""

    output_dir: Path
    manifest_path: Path
    completion_path: Path
    evaluated_stream_count: int
    trace_count: int


@dataclass(frozen=True, slots=True)
class Exp008CaptureResult:
    """Completed live-query phase, awaiting a fresh-process finalization."""

    output_dir: Path
    receipt_path: Path
    started_at_utc: str
    preflight: Mapping[MonitorStreamKey, ServingPreflightResult]
    evaluated_stream_count: int
    trace_count: int


class _NoCanaryBoundEstimator(CanaryBoundEstimatorLike):
    """Prevent accidental future canary use from fabricating an estimate."""

    def estimate(self, measurements: object) -> CanaryBounds:
        del measurements
        raise AssertionError("EXP-008 is DRY_RUN-only and has no canary estimator")


class _StrictUtcClock:
    """Return strictly increasing RFC3339 UTC timestamps for trace ordering."""

    def __init__(self) -> None:
        self._last: datetime | None = None

    def __call__(self) -> str:
        value = datetime.now(timezone.utc)
        if self._last is not None and value <= self._last:
            value = self._last + timedelta(microseconds=1)
        self._last = value
        return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


class _ReadOnlyShadowAdapter:
    """Expose only the adapter surface permitted to the shadow executor."""

    def __init__(self, adapter: MilvusActuationClient) -> None:
        self._adapter = adapter

    @property
    def workload(self) -> ActuationWorkload:
        return self._adapter.workload

    @property
    def client(self) -> object:
        return self._adapter.client

    @property
    def harness(self) -> object:
        return self._adapter.harness

    @property
    def stack_health_probe(self) -> object:
        return self._adapter.stack_health_probe

    @property
    def shadow_trace_sink(self) -> object:
        return self._adapter.shadow_trace_sink

    @shadow_trace_sink.setter
    def shadow_trace_sink(self, value: object) -> None:
        self._adapter.shadow_trace_sink = value  # type: ignore[assignment]

    def shadow_candidate(
        self,
        *,
        context: object,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult:
        return self._adapter.shadow_candidate(
            context=context,  # type: ignore[arg-type]
            candidate_ef=candidate_ef,
            last_known_good_ef=last_known_good_ef,
        )


class _StreamServingRouter(RangeServingExecutor):
    """Route a foreground request only to its isolated immutable stream plan."""

    def __init__(
        self, executors: Mapping[MonitorStreamKey, MilvusRangeServingExecutor]
    ) -> None:
        self._executors = MappingProxyType(dict(executors))

    def preflight(self) -> Mapping[MonitorStreamKey, ServingPreflightResult]:
        return MappingProxyType(
            {key: executor.preflight() for key, executor in self._executors.items()}
        )

    def execute(self, request: RangeQueryRequest) -> object:
        try:
            executor = self._executors[request.stream_key]
        except KeyError as exc:
            raise EXP008AcquisitionError("SERVING_STREAM_UNREGISTERED") from exc
        return executor.execute(request)


class _StreamShadowRouter(ShadowAuditExecutor):
    """Route each complete worker group to the matching isolated adapter."""

    def __init__(
        self, executors: Mapping[MonitorStreamKey, MilvusHostShadowExecutor]
    ) -> None:
        self._executors = MappingProxyType(dict(executors))

    def capture(self, observations: tuple[object, ...]) -> object:
        if not observations:
            raise EXP008AcquisitionError("SHADOW_OBSERVATIONS_EMPTY")
        stream_key = getattr(observations[0], "stream_key", None)
        try:
            executor = self._executors[stream_key]
        except (KeyError, TypeError) as exc:
            raise EXP008AcquisitionError("SHADOW_STREAM_UNREGISTERED") from exc
        return executor.capture(observations)  # type: ignore[arg-type]


class _Exp008PolicyInputs(DryRunPolicyInputProvider):
    """Supply only lineage-bound DRY_RUN inputs; never response estimates."""

    def __init__(self, streams: tuple[Exp008Stream, ...]) -> None:
        self._by_lineage = {
            (
                stream.metric,
                stream.threshold_stratum,
                stream.baseline.configuration_identity,
                stream.baseline.data_identity,
                stream.baseline.flat_binding.identity_id,
                stream.baseline.hnsw_binding.identity_id,
            ): stream
            for stream in streams
        }

    def resolve(self, *, decision: object, provenance: object) -> DryRunPolicyInputs:
        del decision
        key = (
            getattr(provenance, "metric", None),
            getattr(provenance, "threshold_stratum", None),
            getattr(provenance, "configuration_identity", None),
            getattr(provenance, "data_identity", None),
            getattr(provenance, "flat_binding_id", None),
            getattr(provenance, "hnsw_binding_id", None),
        )
        try:
            stream = self._by_lineage[key]
        except KeyError as exc:
            raise EXP008AcquisitionError("POLICY_PROVENANCE_UNREGISTERED") from exc
        return DryRunPolicyInputs(
            current_ef=stream.served_ef,
            response_estimates={},
            pre_action=PreActionSafety(
                metric=stream.metric,
                threshold_stratum=stream.threshold_stratum,
                configuration_identity=stream.baseline.configuration_identity,
                index_identity=stream.baseline.hnsw_binding.identity_id,
                flat_index_identity=stream.baseline.flat_binding.identity_id,
                data_identity=stream.baseline.data_identity,
                response_model_provenance="EXP008_DRY_RUN_NO_RESPONSE_MODEL",
            ),
            last_known_good=QualificationResult(
                qualified=False,
                ef=None,
                reasons=("EXP008_NO_LIVE_QUALIFICATION",),
            ),
            audit_id=(
                f"exp008:{stream.stream_key.stream_id}:"
                f"{getattr(provenance, 'current_window_id', 'unknown')}"
            ),
        )


def _expected_stream(
    *,
    baseline: IdentityBaseline,
    baseline_path: Path,
    metric: Metric,
    stratum: str,
    candidate_ef: int,
    lkg_ef: int,
    threshold_radius: float,
) -> Exp008Stream:
    if (
        baseline.metric is not metric
        or baseline.threshold_stratum != stratum
        or baseline.candidate_ef != candidate_ef
        or baseline.last_known_good_ef != lkg_ef
    ):
        raise EXP008AcquisitionError("BASELINE_DOES_NOT_MATCH_EXP008_REGISTRATION")
    stream_key = MonitorStreamKey(
        stream_id=f"exp008-{metric.value.lower()}-stationary",
        metric=metric,
        threshold_stratum=stratum,
        configuration_identity=baseline.configuration_identity,
        data_identity=baseline.data_identity,
        flat_binding_id=baseline.flat_binding.identity_id,
        hnsw_binding_id=baseline.hnsw_binding.identity_id,
    )
    return Exp008Stream(
        metric=metric,
        threshold_stratum=stratum,
        candidate_ef=candidate_ef,
        last_known_good_ef=lkg_ef,
        served_ef=lkg_ef,
        sentinel_ef=100,
        threshold_radius=threshold_radius,
        baseline=baseline,
        baseline_path=baseline_path,
        baseline_file_sha256=sha256_file(baseline_path),
        stream_key=stream_key,
    )
def prepare_exp008_configuration(
    *,
    dataset_dir: str | os.PathLike[str],
    l2_baseline_path: str | os.PathLike[str],
    cosine_baseline_path: str | os.PathLike[str],
) -> Exp008Configuration:
    """Load only DATASET-001 plus the two already-reviewed EXP-005 baselines."""

    dataset_path = Path(dataset_dir)
    l2_path = Path(l2_baseline_path)
    cosine_path = Path(cosine_baseline_path)
    try:
        bundle, _, manifest = load_dataset(dataset_path)
        l2 = load_identity_baseline(l2_path)
        cosine = load_identity_baseline(cosine_path)
    except (OSError, ValueError, Exp005AcquisitionError) as exc:
        raise EXP008AcquisitionError("EXP008_INPUT_LOAD_FAILED") from exc
    if bundle.measured_queries.shape[0] != _WINDOW_QUERY_COUNT:
        raise EXP008AcquisitionError("MEASURED_QUERY_COUNT_MUST_BE_200")
    manifest_sha256 = sha256_file(dataset_path / "generation_manifest.json")
    specifications = (
        (l2, l2_path, Metric.L2, "target-075", 800, 400),
        (cosine, cosine_path, Metric.COSINE, "target-025", 400, 200),
    )
    validated_streams: list[Exp008Stream] = []
    for baseline, baseline_path, metric, stratum, candidate_ef, lkg_ef in specifications:
        try:
            configuration_identity, data_identity, radius = derive_exp005_identities(
                dataset_dir=dataset_path,
                metric=metric,
                stratum=stratum,
                candidate_ef=candidate_ef,
                lkg_ef=lkg_ef,
            )
        except (OSError, ValueError, Exp005AcquisitionError) as exc:
            raise EXP008AcquisitionError("EXP008_IDENTITY_DERIVATION_FAILED") from exc
        if (
            baseline.configuration_identity != configuration_identity
            or baseline.data_identity != data_identity
        ):
            raise EXP008AcquisitionError("BASELINE_DATASET_CONFIGURATION_MISMATCH")
        validated_streams.append(
            _expected_stream(
                baseline=baseline,
                baseline_path=baseline_path,
                metric=metric,
                stratum=stratum,
                candidate_ef=candidate_ef,
                lkg_ef=lkg_ef,
                threshold_radius=radius,
            )
        )
    if len({stream.baseline.configuration_identity for stream in validated_streams}) != 2:
        raise EXP008AcquisitionError("EXP008_CONFIGURATION_IDENTITIES_MUST_BE_DISTINCT")
    return Exp008Configuration(
        dataset_dir=dataset_path,
        dataset_manifest=MappingProxyType(dict(manifest)),
        dataset_manifest_sha256=manifest_sha256,
        base_ids=bundle.ids,
        base_vectors=bundle.base_vectors,
        measured_queries=bundle.measured_queries,
        streams=tuple(validated_streams),
    )


def _workload_for_stream(
    configuration: Exp008Configuration, stream: Exp008Stream
) -> ActuationWorkload:
    queries = {
        index: vector
        for index, vector in enumerate(configuration.measured_queries)
    }
    return ActuationWorkload(
        query_vectors=queries,
        base_ids=configuration.base_ids,
        base_vectors=configuration.base_vectors,
        threshold_radii={(stream.metric, stream.threshold_stratum): stream.threshold_radius},
        collection_names={
            (stream.metric, IndexTrack.FLAT): stream.baseline.flat_binding.expected.collection_name,
            (stream.metric, IndexTrack.HNSW): stream.baseline.hnsw_binding.expected.collection_name,
        },
        identity_bindings={
            (stream.metric, IndexTrack.FLAT): stream.baseline.flat_binding,
            (stream.metric, IndexTrack.HNSW): stream.baseline.hnsw_binding,
        },
        configuration_identity=stream.baseline.configuration_identity,
        data_identity=stream.baseline.data_identity,
    )


class Exp008ServingExecutor(RangeServingExecutor, Protocol):
    """Foreground executor plus the explicit preflight required by EXP-008."""

    def preflight(self) -> Mapping[MonitorStreamKey, ServingPreflightResult]: ...


class Exp008ShadowExecutor(ShadowAuditExecutor, Protocol):
    """Background trace-capture executor selected by immutable stream key."""


@dataclass(frozen=True, slots=True)
class Exp008Runtime:
    """Injectable read-only runtime dependencies for a complete EXP-008 run."""

    serving: Exp008ServingExecutor
    shadow: Exp008ShadowExecutor
    close: Callable[[], None] = lambda: None

    def __post_init__(self) -> None:
        if not callable(getattr(self.serving, "preflight", None)) or not callable(
            getattr(self.serving, "execute", None)
        ):
            raise TypeError("serving must implement EXP-008 preflight and execute")
        if not callable(getattr(self.shadow, "capture", None)):
            raise TypeError("shadow must implement capture")
        if not callable(self.close):
            raise TypeError("close must be callable")


def build_live_runtime(
    *,
    configuration: Exp008Configuration,
    uri: str,
    etcd_container: str = "milvus-etcd",
    minio_container: str = "milvus-minio",
) -> Exp008Runtime:
    """Create isolated read-only adapters lazily; this is the only live factory."""

    serving: dict[MonitorStreamKey, MilvusRangeServingExecutor] = {}
    shadow: dict[MonitorStreamKey, MilvusHostShadowExecutor] = {}
    adapters: list[MilvusActuationClient] = []
    for stream in configuration.streams:
        adapter = MilvusActuationClient.from_uri(
            uri,
            workload=_workload_for_stream(configuration, stream),
            routing_seed=EXP008_DETECTOR_SEED,
            bound_estimator=_NoCanaryBoundEstimator(),
            stack_health_probe=DockerSocketHealthProbe(
                etcd_container=etcd_container,
                minio_container=minio_container,
            ),
            initial_ef=stream.served_ef,
        )
        adapters.append(adapter)
        read_only_adapter = _ReadOnlyShadowAdapter(adapter)
        serving[stream.stream_key] = MilvusRangeServingExecutor(
            client=adapter.client,
            plans={
                stream.stream_key: HostServingPlan(
                    flat_collection_name=stream.baseline.flat_binding.expected.collection_name,
                    hnsw_collection_name=stream.baseline.hnsw_binding.expected.collection_name,
                    flat_binding=stream.baseline.flat_binding,
                    hnsw_binding=stream.baseline.hnsw_binding,
                    threshold_radius=stream.threshold_radius,
                    dimensions=int(configuration.base_vectors.shape[1]),
                    allowed_served_efs=frozenset({stream.served_ef}),
                )
            },
            stack_health_probe=adapter.stack_health_probe,
        )
        shadow[stream.stream_key] = MilvusHostShadowExecutor(
            adapter=read_only_adapter,  # type: ignore[arg-type]
            plans={
                stream.stream_key: HostShadowPlan(
                    stream.candidate_ef,
                    stream.last_known_good_ef,
                    stream.served_ef,
                )
            },
            clock=_StrictUtcClock(),
        )
    closed = False

    def close() -> None:
        nonlocal closed
        if closed:
            return
        for adapter in adapters:
            callback = getattr(adapter.client, "close", None)
            if not callable(callback):
                raise EXP008AcquisitionError("MILVUS_CLIENT_CLOSE_UNAVAILABLE")
            callback()
        closed = True

    return Exp008Runtime(
        serving=_StreamServingRouter(serving),
        shadow=_StreamShadowRouter(shadow),
        close=close,
    )


def _request(stream: Exp008Stream, query_id: int, vector: np.ndarray) -> RangeQueryRequest:
    return RangeQueryRequest(
        request_id=query_id,
        stream_key=stream.stream_key,
        query_vector=tuple(float(value) for value in vector),
        threshold_radius=stream.threshold_radius,
        range_filter=0.0 if stream.metric is Metric.L2 else 1.0,
        limit=RESULT_LIMIT,
        served_ef=stream.served_ef,
    )


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return _json_value(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return [_json_value(item) for item in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    return value


def _command_snapshot(command: tuple[str, ...]) -> dict[str, object]:
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
    except OSError as exc:
        return {"command": list(command), "returncode": None, "stdout": "", "stderr": type(exc).__name__}
    return {
        "command": list(command),
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _resource_snapshot(*, timestamp_utc: str) -> dict[str, object]:
    memory: int | None = None
    try:
        memory = int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, OSError, ValueError):
        pass
    return {
        "timestamp_utc": timestamp_utc,
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "python": sys.version,
        "cpu_count": os.cpu_count(),
        "memory_bytes": memory,
        "docker_ps": _command_snapshot(("docker", "ps", "--format", "{{.Names}}\t{{.Status}}\t{{.Image}}")),
        "docker_stats": _command_snapshot(("docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}")),
        "processes": _command_snapshot(("ps", "-axo", "pid,ppid,%cpu,%mem,comm")),
    }


def capture_host_resource_snapshot(*, timestamp_utc: str) -> Mapping[str, object]:
    """Capture host evidence outside every live gRPC-owning process.

    The function is public solely for EXP-008's companion failure-probe
    capture.  Callers take a pre-run snapshot before opening a client and a
    post-run snapshot only from a freshly exec'd finalizer.
    """

    return MappingProxyType(_resource_snapshot(timestamp_utc=timestamp_utc))


def _artifact_hashes(root: Path) -> Mapping[str, str]:
    values: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name in {"run_manifest.json", "completion.json"}:
            continue
        if path.is_symlink():
            raise EXP008AcquisitionError("ARTIFACT_SYMLINK_REJECTED")
        values[str(path.relative_to(root))] = sha256_file(path)
    return MappingProxyType(values)


def _preflight_document(
    results: Mapping[MonitorStreamKey, ServingPreflightResult]
) -> dict[str, object]:
    return {
        key.stream_id: _json_value(result)
        for key, result in results.items()
    }


def _run_manifest(
    *,
    configuration: Exp008Configuration,
    output_dir: Path,
    git_metadata: Mapping[str, object],
    started_at_utc: str,
    preflight: Mapping[MonitorStreamKey, ServingPreflightResult],
) -> dict[str, object]:
    return {
        "schema_version": _RUN_SCHEMA_VERSION,
        "execution_mode": PolicyMode.DRY_RUN.value,
        "started_at_utc": started_at_utc,
        "git": dict(git_metadata),
        "environment_pins": _json_value(ENV001_PINS),
        "dataset": {
            "directory": str(configuration.dataset_dir),
            "generation_manifest_sha256": configuration.dataset_manifest_sha256,
        },
        "frozen_limits": {
            "host_observation_capacity": _HOST_QUEUE_CAPACITY,
            "worker_drain_limit": _WORKER_DRAIN_LIMIT,
            "max_partial_streams": _MAX_PARTIAL_STREAMS,
            "max_observation_age_seconds": _MAX_OBSERVATION_AGE_SECONDS,
            "outbox_pending_event_capacity": _OUTBOX_PENDING_EVENT_CAPACITY,
            "outbox_pending_byte_capacity": _OUTBOX_PENDING_BYTE_CAPACITY,
            "monitor_poll_limit": _MONITOR_POLL_LIMIT,
        },
        "detector_seed": EXP008_DETECTOR_SEED,
        "stream_configuration": [
            {
                "stream_id": stream.stream_key.stream_id,
                "metric": stream.metric.value,
                "threshold_stratum": stream.threshold_stratum,
                "candidate_ef": stream.candidate_ef,
                "last_known_good_ef": stream.last_known_good_ef,
                "served_ef": stream.served_ef,
                "sentinel_ef": stream.sentinel_ef,
                "baseline_path": str(stream.baseline_path),
                "baseline_file_sha256": stream.baseline_file_sha256,
                "baseline_sha256": stream.baseline.sha256,
                "stream_key": _json_value(stream.stream_key),
            }
            for stream in configuration.streams
        ],
        "serving_preflight": _preflight_document(preflight),
        "output_directory": str(output_dir),
    }


def _raise_if_incomplete_preflight(
    results: Mapping[MonitorStreamKey, ServingPreflightResult]
) -> None:
    if len(results) != 2 or not all(result.complete for result in results.values()):
        raise EXP008AcquisitionError("SERVING_PREFLIGHT_INCOMPLETE")


def capture_exp008(
    *,
    configuration: Exp008Configuration,
    runtime: Exp008Runtime,
    output_dir: str | os.PathLike[str],
    clock: Callable[[], str] | None = None,
    pre_run_resources: Mapping[str, object],
    capture_git: Mapping[str, object],
) -> Exp008CaptureResult:
    """Run only the gRPC-owning capture phase, then close its clients.

    The caller must obtain the resource snapshot and Git state *before*
    constructing live clients.  Finalization happens in a fresh process so
    host-snapshot subprocesses can never fork from a live gRPC runtime.
    """

    if not isinstance(configuration, Exp008Configuration):
        raise TypeError("configuration must be an Exp008Configuration")
    if not isinstance(runtime, Exp008Runtime):
        raise TypeError("runtime must be an Exp008Runtime")
    root = Path(output_dir)
    if root.exists():
        raise FileExistsError(f"refusing to overwrite EXP-008 evidence: {root}")
    root.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    utc_clock = clock or _StrictUtcClock()
    closed = False
    try:
        pre_snapshot = dict(pre_run_resources)
        if not isinstance(pre_snapshot.get("timestamp_utc"), str):
            raise EXP008AcquisitionError("PRE_RUN_RESOURCE_SNAPSHOT_INVALID")
        if (
            not isinstance(capture_git.get("commit"), str)
            or not isinstance(capture_git.get("dirty"), bool)
        ):
            raise EXP008AcquisitionError("CAPTURE_GIT_STATE_INVALID")
        write_immutable_json(root / "pre_run_resources.json", pre_snapshot)
        preflight = runtime.serving.preflight()
        write_immutable_json(root / "serving_preflight.json", _preflight_document(preflight))
        _raise_if_incomplete_preflight(preflight)

        recorder = BoundedHostObservationRecorder(
            max_pending_observations=_HOST_QUEUE_CAPACITY
        )
        source = FileShadowTraceEventSource(
            root / "outbox",
            max_pending_events=_OUTBOX_PENDING_EVENT_CAPACITY,
            max_pending_bytes=_OUTBOX_PENDING_BYTE_CAPACITY,
        )
        worker = BackgroundShadowWorker(
            recorder=recorder,
            executor=runtime.shadow,
            publisher=source,
            state_store=FileHostWorkerStateStore(root / "host-worker-state"),
            registered_trace_parameters=RegisteredTraceParameters(
                allowed_candidate_and_lkg_efs=frozenset({200, 400, 800}),
                sentinel_ef=100,
            ),
            max_partial_streams=_MAX_PARTIAL_STREAMS,
            max_observation_age_seconds=_MAX_OBSERVATION_AGE_SECONDS,
            clock=utc_clock,
        )
        gateway = ReferenceRangeGateway(
            serving_executor=runtime.serving, recorder=recorder, clock=utc_clock
        )
        receipts: list[dict[str, object]] = []
        worker_cycles: list[WorkerCycleResult] = []
        for stream in configuration.streams:
            for window_index in range(_WINDOW_COUNT):
                for group_start in range(0, _WINDOW_QUERY_COUNT, _TRACE_QUERY_COUNT):
                    for query_id in range(group_start, group_start + _TRACE_QUERY_COUNT):
                        response = gateway.execute(
                            _request(
                                stream,
                                query_id,
                                configuration.measured_queries[query_id],
                            )
                        )
                        if (
                            not response.served_outcome.success
                            or response.observation_receipt.status.value != "ACCEPTED"
                        ):
                            raise EXP008AcquisitionError("FOREGROUND_OR_RECORDER_FAILED")
                        receipts.append(
                            {
                                "stream_id": stream.stream_key.stream_id,
                                "window_index": window_index,
                                "query_id": query_id,
                                "served_success": response.served_outcome.success,
                                "served_timed_out": response.served_outcome.timed_out,
                                "result_count": response.served_outcome.result_count,
                                "latency_ms": response.served_outcome.latency_ms,
                                "observation_status": response.observation_receipt.status.value,
                                "observation_reason": response.observation_receipt.reason_code,
                            }
                        )
                    worker_result = worker.run_once(max_observations=_WORKER_DRAIN_LIMIT)
                    if (
                        worker_result.drained_observation_count != _TRACE_QUERY_COUNT
                        or worker_result.captured_trace_count != 1
                        or worker_result.published_trace_count != 1
                        or worker_result.rejected_observation_count != 0
                        or worker_result.blocked_stream_count != 0
                        or worker_result.reason_codes
                    ):
                        raise EXP008AcquisitionError("WORKER_CYCLE_INCOMPLETE")
                    worker_cycles.append(worker_result)

        write_immutable_json(root / "foreground_receipts.json", receipts)
        write_immutable_json(root / "worker_cycles.json", _json_value(worker_cycles))
        if len(tuple((root / "outbox" / "traces").glob("*.json"))) != 24:
            raise EXP008AcquisitionError("TRACE_COUNT_INVALID")
        audit_sink = FileMonitorAuditSink(root / "monitor-audit.jsonl")
        monitor = WorkloadMonitor(
            source=source,
            state_store=FileMonitorStateStore(root / "monitor-state"),
            policy_input_provider=_Exp008PolicyInputs(configuration.streams),
            audit_sink=audit_sink,
            detector_seed=EXP008_DETECTOR_SEED,
        )
        monitor_results = monitor.run_once(max_events=_MONITOR_POLL_LIMIT)
        evaluated = [
            result
            for result in monitor_results
            if result.policy_decision is not None
        ]
        if (
            len(monitor_results) != 24
            or len(evaluated) != 2
            or any(
                result.drift_decision is None
                or result.drift_decision.state.value != "NO_DRIFT"
                or result.policy_decision is None
                or result.policy_decision.action.value != "NO_CHANGE"
                or result.policy_decision.mode is not PolicyMode.DRY_RUN
                for result in evaluated
            )
            or source.poll(limit=1)
        ):
            raise EXP008AcquisitionError("MONITOR_COMPOSITION_INCOMPLETE")
        write_immutable_json(root / "monitor_results.json", _json_value(monitor_results))
        write_immutable_json(root / "monitor_audit_records.json", _json_value(audit_sink.read_records()))

        postflight = runtime.serving.preflight()
        write_immutable_json(root / "serving_postflight.json", _preflight_document(postflight))
        _raise_if_incomplete_preflight(postflight)
        runtime.close()
        closed = True
        receipt = {
            "schema_version": _RUN_SCHEMA_VERSION,
            "status": "CAPTURE_COMPLETE_AWAITING_FRESH_PROCESS_FINALIZATION",
            "capture_git": dict(capture_git),
            "started_at_utc": pre_snapshot["timestamp_utc"],
            "trace_count": 24,
            "foreground_request_count": len(receipts),
            "worker_cycle_count": len(worker_cycles),
            "evaluated_stream_count": len(evaluated),
            "detector_states": sorted(
                {result.drift_decision.state.value for result in evaluated if result.drift_decision is not None}
            ),
            "policy_actions": sorted(
                {result.policy_decision.action.value for result in evaluated if result.policy_decision is not None}
            ),
            "no_actuation": {
                "policy_mode_dry_run": True,
                "safe_actuation_boundary_constructed": False,
                "start_canary_called": False,
                "rollback_called": False,
                "milvus_configuration_mutation_called": False,
            },
        }
        receipt_path = root / "capture_receipt.json"
        write_immutable_json(receipt_path, receipt)
        return Exp008CaptureResult(
            output_dir=root,
            receipt_path=receipt_path,
            started_at_utc=pre_snapshot["timestamp_utc"],
            preflight=preflight,
            evaluated_stream_count=len(evaluated),
            trace_count=24,
        )
    except Exception as exc:
        if not closed:
            try:
                runtime.close()
            except Exception:
                pass
        failure = {
            "schema_version": _RUN_SCHEMA_VERSION,
            "status": "INCOMPLETE",
            "reason_code": (
                str(exc)
                if isinstance(exc, EXP008AcquisitionError)
                else type(exc).__name__
            ),
        }
        try:
            write_immutable_json(root / "failure.json", failure)
        except Exception:
            pass
        raise


def _load_capture_receipt(root: Path) -> Mapping[str, object]:
    try:
        receipt = json.loads((root / "capture_receipt.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EXP008AcquisitionError("CAPTURE_RECEIPT_UNAVAILABLE") from exc
    if not isinstance(receipt, dict) or receipt.get("schema_version") != _RUN_SCHEMA_VERSION:
        raise EXP008AcquisitionError("CAPTURE_RECEIPT_INVALID")
    if receipt.get("status") != "CAPTURE_COMPLETE_AWAITING_FRESH_PROCESS_FINALIZATION":
        raise EXP008AcquisitionError("CAPTURE_RECEIPT_NOT_FINALIZABLE")
    return receipt


def _preflight_from_document(
    configuration: Exp008Configuration, document: object
) -> Mapping[MonitorStreamKey, ServingPreflightResult]:
    if not isinstance(document, Mapping):
        raise EXP008AcquisitionError("PREFLIGHT_DOCUMENT_INVALID")
    results: dict[MonitorStreamKey, ServingPreflightResult] = {}
    for stream in configuration.streams:
        value = document.get(stream.stream_key.stream_id)
        if not isinstance(value, Mapping):
            raise EXP008AcquisitionError("PREFLIGHT_DOCUMENT_INVALID")
        complete = value.get("complete")
        checked = value.get("checked_stream_count")
        reasons = value.get("reason_codes")
        if (
            not isinstance(complete, bool)
            or isinstance(checked, bool)
            or not isinstance(checked, int)
            or not isinstance(reasons, list)
            or not all(isinstance(reason, str) for reason in reasons)
        ):
            raise EXP008AcquisitionError("PREFLIGHT_DOCUMENT_INVALID")
        results[stream.stream_key] = ServingPreflightResult(
            complete=complete,
            checked_stream_count=checked,
            reason_codes=tuple(reasons),
        )
    if len(document) != len(results):
        raise EXP008AcquisitionError("PREFLIGHT_DOCUMENT_INVALID")
    return MappingProxyType(results)


def _verify_capture_artifacts(
    *, root: Path, configuration: Exp008Configuration, receipt: Mapping[str, object]
) -> Mapping[MonitorStreamKey, ServingPreflightResult]:
    """Re-derive completion predicates before immutable finalization."""

    try:
        preflight_document = json.loads((root / "serving_preflight.json").read_text(encoding="utf-8"))
        postflight_document = json.loads((root / "serving_postflight.json").read_text(encoding="utf-8"))
        receipts = json.loads((root / "foreground_receipts.json").read_text(encoding="utf-8"))
        worker_cycles = json.loads((root / "worker_cycles.json").read_text(encoding="utf-8"))
        monitor_records = json.loads((root / "monitor_audit_records.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EXP008AcquisitionError("CAPTURE_ARTIFACT_LOAD_FAILED") from exc
    preflight = _preflight_from_document(configuration, preflight_document)
    _raise_if_incomplete_preflight(preflight)
    _raise_if_incomplete_preflight(_preflight_from_document(configuration, postflight_document))
    if (
        not isinstance(receipts, list)
        or len(receipts) != 1200
        or not all(
            isinstance(item, Mapping)
            and item.get("served_success") is True
            and item.get("observation_status") == "ACCEPTED"
            for item in receipts
        )
    ):
        raise EXP008AcquisitionError("CAPTURE_FOREGROUND_EVIDENCE_INVALID")
    counts = {stream.stream_key.stream_id: 0 for stream in configuration.streams}
    for item in receipts:
        stream_id = item.get("stream_id")
        if stream_id not in counts:
            raise EXP008AcquisitionError("CAPTURE_FOREGROUND_STREAM_INVALID")
        counts[stream_id] += 1
    if set(counts.values()) != {600}:
        raise EXP008AcquisitionError("CAPTURE_FOREGROUND_CARDINALITY_INVALID")
    if (
        not isinstance(worker_cycles, list)
        or len(worker_cycles) != 24
        or not all(
            isinstance(item, Mapping)
            and item.get("drained_observation_count") == 50
            and item.get("captured_trace_count") == 1
            and item.get("published_trace_count") == 1
            and item.get("rejected_observation_count") == 0
            and item.get("blocked_stream_count") == 0
            and item.get("reason_codes") == []
            for item in worker_cycles
        )
    ):
        raise EXP008AcquisitionError("CAPTURE_WORKER_EVIDENCE_INVALID")
    groups: dict[tuple[str, int, str], list[tuple[int, object]]] = {}
    outbox_root = (root / "outbox").resolve()
    for event_path in sorted((root / "outbox" / "acknowledged").glob("*.json")):
        try:
            event = json.loads(event_path.read_text(encoding="utf-8"))
            stream = event["stream_key"]["stream_id"]
            sequence = event["window_sequence"]
            window_id = event["window_id"]
            trace_index = event["trace_sequence_index"]
            relative_envelope_path = Path(event["envelope_path"])
            expected_trace_sha256 = event["expected_trace_sha256"]
        except (OSError, TypeError, KeyError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            raise EXP008AcquisitionError("CAPTURE_EVENT_OR_TRACE_INVALID") from exc
        if (
            relative_envelope_path.is_absolute()
            or relative_envelope_path.parts[:1] != ("traces",)
            or len(relative_envelope_path.parts) != 2
            or relative_envelope_path.suffix != ".json"
            or not isinstance(expected_trace_sha256, str)
        ):
            raise EXP008AcquisitionError("CAPTURE_EVENT_OR_TRACE_INVALID")
        envelope_path = (outbox_root / relative_envelope_path).resolve()
        if not envelope_path.is_relative_to(outbox_root):
            raise EXP008AcquisitionError("CAPTURE_EVENT_OR_TRACE_INVALID")
        try:
            envelope = load_persisted_shadow_trace_envelope(envelope_path)
        except (OSError, TypeError, UnicodeError, ValueError) as exc:
            raise EXP008AcquisitionError("CAPTURE_EVENT_OR_TRACE_INVALID") from exc
        if envelope.expected_trace_sha256 != expected_trace_sha256:
            raise EXP008AcquisitionError("CAPTURE_EVENT_OR_TRACE_INVALID")
        if not isinstance(stream, str) or not isinstance(sequence, int) or not isinstance(window_id, str) or not isinstance(trace_index, int):
            raise EXP008AcquisitionError("CAPTURE_EVENT_OR_TRACE_INVALID")
        groups.setdefault((stream, sequence, window_id), []).append((trace_index, envelope))
    if len(groups) != 6 or sum(len(values) for values in groups.values()) != 24:
        raise EXP008AcquisitionError("CAPTURE_TRACE_CARDINALITY_INVALID")
    for (stream_id, sequence, window_id), values in groups.items():
        if stream_id not in counts or sequence not in range(3) or len(values) != 4:
            raise EXP008AcquisitionError("CAPTURE_TRACE_GROUP_INVALID")
        window = assemble_shadow_window(
            window_id=window_id,
            envelopes=tuple(envelope for _, envelope in sorted(values)),
        )
        if not window.complete or len(window.query_records) != 200:
            raise EXP008AcquisitionError("CAPTURE_WINDOW_ASSEMBLY_INVALID")
    if any((root / "outbox" / "pending").glob("*.json")):
        raise EXP008AcquisitionError("CAPTURE_PENDING_EVENTS_PRESENT")
    evaluated = [
        item
        for item in monitor_records
        if isinstance(item, Mapping) and item.get("status") == "EVALUATED"
    ] if isinstance(monitor_records, list) else []
    if (
        len(evaluated) != 2
        or {item.get("detector_state") for item in evaluated} != {"NO_DRIFT"}
        or {item.get("policy_action") for item in evaluated} != {"NO_CHANGE"}
        or any(item.get("reason_codes") != [] for item in evaluated)
        or receipt.get("no_actuation", {}).get("policy_mode_dry_run") is not True
    ):
        raise EXP008AcquisitionError("CAPTURE_MONITOR_EVIDENCE_INVALID")
    return preflight


def finalize_exp008(
    *,
    configuration: Exp008Configuration,
    output_dir: str | os.PathLike[str],
    post_run_resources: Mapping[str, object],
    repository: str | os.PathLike[str] = _REPOSITORY_ROOT,
) -> Exp008RunResult:
    """Finalize a complete capture only from a fresh non-gRPC process."""

    root = Path(output_dir)
    repository_path = Path(repository)
    if (root / "run_manifest.json").exists() or (root / "completion.json").exists():
        raise EXP008AcquisitionError("EXP008_ALREADY_FINALIZED")
    if not isinstance(post_run_resources.get("timestamp_utc"), str):
        raise EXP008AcquisitionError("POST_RUN_RESOURCE_SNAPSHOT_INVALID")
    receipt = _load_capture_receipt(root)
    capture_git = receipt.get("capture_git")
    if not isinstance(capture_git, Mapping):
        raise EXP008AcquisitionError("CAPTURE_GIT_STATE_INVALID")
    current_git = git_state(repository_path)
    if current_git["commit"] != capture_git.get("commit"):
        raise EXP008AcquisitionError("CAPTURE_COMMIT_CHANGED_BEFORE_FINALIZATION")
    preflight = _verify_capture_artifacts(
        root=root, configuration=configuration, receipt=receipt
    )
    write_immutable_json(root / "post_run_resources.json", dict(post_run_resources))
    manifest = _run_manifest(
        configuration=configuration,
        output_dir=root,
        git_metadata=current_git,
        started_at_utc=receipt["started_at_utc"],
        preflight=preflight,
    )
    manifest["artifact_sha256"] = dict(_artifact_hashes(root))
    manifest["self_sha256"] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    manifest_path = root / "run_manifest.json"
    write_immutable_json(manifest_path, manifest)
    completion = {
        "schema_version": _RUN_SCHEMA_VERSION,
        "status": "COMPLETE",
        "manifest_sha256": sha256_file(manifest_path),
        "trace_count": receipt["trace_count"],
        "foreground_request_count": receipt["foreground_request_count"],
        "worker_cycle_count": receipt["worker_cycle_count"],
        "evaluated_stream_count": receipt["evaluated_stream_count"],
        "detector_states": receipt["detector_states"],
        "policy_actions": receipt["policy_actions"],
        "no_actuation": receipt["no_actuation"],
    }
    completion_path = root / "completion.json"
    write_immutable_json(completion_path, completion)
    return Exp008RunResult(
        output_dir=root,
        manifest_path=manifest_path,
        completion_path=completion_path,
        evaluated_stream_count=int(receipt["evaluated_stream_count"]),
        trace_count=int(receipt["trace_count"]),
    )


def run_exp008(
    *,
    configuration: Exp008Configuration,
    runtime: Exp008Runtime,
    output_dir: str | os.PathLike[str],
    clock: Callable[[], str] | None = None,
    resource_snapshot: Callable[[str], Mapping[str, object]] | None = None,
    repository: str | os.PathLike[str] = _REPOSITORY_ROOT,
) -> Exp008RunResult:
    """In-process helper for fake-runtime tests only.

    Production callers must use :func:`capture_exp008` followed by a fresh
    process invoking :func:`finalize_exp008`.  A caller has to inject both
    snapshots here so this convenience helper cannot accidentally launch a
    host subprocess after a real gRPC client has existed in the process.
    """

    utc_clock = clock or _StrictUtcClock()
    if resource_snapshot is None:
        raise TypeError("resource_snapshot is required for in-process fake-runtime use")
    snapshot = resource_snapshot
    repository_path = Path(repository)
    capture = capture_exp008(
        configuration=configuration,
        runtime=runtime,
        output_dir=output_dir,
        clock=utc_clock,
        pre_run_resources=dict(snapshot(utc_clock())),
        capture_git=git_state(repository_path),
    )
    return finalize_exp008(
        configuration=configuration,
        output_dir=capture.output_dir,
        post_run_resources=dict(snapshot(utc_clock())),
        repository=repository_path,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri")
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--l2-baseline", type=Path, required=True)
    parser.add_argument("--cosine-baseline", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--detector-seed", type=int, default=EXP008_DETECTOR_SEED)
    parser.add_argument("--etcd-container", default="milvus-etcd")
    parser.add_argument("--minio-container", default="milvus-minio")
    parser.add_argument(
        "--finalize-only",
        action="store_true",
        help=(
            "finalize a completed capture in a fresh non-gRPC process; "
            "never opens a Milvus client"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.detector_seed != EXP008_DETECTOR_SEED:
        raise EXP008AcquisitionError("EXP008_DETECTOR_SEED_IS_FROZEN")
    configuration = prepare_exp008_configuration(
        dataset_dir=args.dataset_dir,
        l2_baseline_path=args.l2_baseline,
        cosine_baseline_path=args.cosine_baseline,
    )
    clock = _StrictUtcClock()
    if args.finalize_only:
        result = finalize_exp008(
            configuration=configuration,
            output_dir=args.output_dir,
            post_run_resources=capture_host_resource_snapshot(timestamp_utc=clock()),
        )
        print(json.dumps(_json_value(result), sort_keys=True))
        return 0
    if not isinstance(args.uri, str) or not args.uri:
        raise EXP008AcquisitionError("URI_REQUIRED_FOR_CAPTURE")
    pre_run_resources = capture_host_resource_snapshot(timestamp_utc=clock())
    capture_git = git_state(_REPOSITORY_ROOT)
    runtime = build_live_runtime(
        configuration=configuration,
        uri=args.uri,
        etcd_container=args.etcd_container,
        minio_container=args.minio_container,
    )
    capture = capture_exp008(
        configuration=configuration,
        runtime=runtime,
        output_dir=args.output_dir,
        clock=clock,
        pre_run_resources=pre_run_resources,
        capture_git=capture_git,
    )
    del capture
    forwarded = list(sys.argv[1:] if argv is None else argv)
    if "--finalize-only" in forwarded:
        raise AssertionError("capture invocation unexpectedly includes --finalize-only")
    os.execv(
        sys.executable,
        [
            sys.executable,
            "-m",
            "vdbench.exp008_acquisition",
            *forwarded,
            "--finalize-only",
        ],
    )
    raise AssertionError("os.execv unexpectedly returned")


if __name__ == "__main__":
    raise SystemExit(main())
