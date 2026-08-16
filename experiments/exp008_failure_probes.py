"""EXP-008 H1/H4 read-only isolation and failure-containment capture.

This experiment deliberately injects post-response failures while retaining a
real foreground range-serving dependency in the live composition root.  It
never constructs a monitor, policy, or safe-actuation boundary; all faults are
contained before a trace can become monitor input.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Mapping
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType

import numpy as np

from vdbench.artifacts import (
    canonical_json_bytes,
    git_state,
    sha256_file,
    write_immutable_json,
)
from vdbench.config import RESULT_LIMIT, Metric
from vdbench.exp008_acquisition import (
    EXP008_DETECTOR_SEED,
    Exp008Configuration,
    Exp008Runtime,
    build_live_runtime,
    capture_host_resource_snapshot,
    prepare_exp008_configuration,
)
from vdbench.host_observation import (
    BackgroundShadowWorker,
    BoundedHostObservationRecorder,
    FileHostWorkerStateStore,
    RangeQueryRequest,
    ReferenceRangeGateway,
    RegisteredTraceParameters,
    TracePublicationReceipt,
    WorkerCycleResult,
)

__all__ = [
    "EXP008FailureProbeError",
    "FailureProbeCapture",
    "FailureProbeResult",
    "FailureProbeRunResult",
    "finalize_failure_probes",
    "run_failure_probes",
    "verify_failure_probe_bundle",
]


class EXP008FailureProbeError(RuntimeError):
    """Raised when an H4 probe cannot demonstrate its exact contract."""


@dataclass(frozen=True, slots=True)
class FailureProbeResult:
    """One probe's compact, non-sensitive fail-closed evidence."""

    name: str
    expected_reason_code: str
    foreground_success: bool
    fail_closed: bool
    foreground_request_count: int
    foreground_success_count: int
    foreground_observation_reason_codes: tuple[str, ...]
    worker_cycle: WorkerCycleResult | None
    publisher_call_count: int
    detail: str


@dataclass(frozen=True, slots=True)
class FailureProbeCapture:
    """Pointers and counters from one completed, non-actuating H4 capture."""

    output_dir: Path
    probes: tuple[FailureProbeResult, ...]
    published_trace_count: int
    monitor_call_count: int
    policy_call_count: int
    actuation_call_count: int


@dataclass(frozen=True, slots=True)
class FailureProbeRunResult:
    """Compact immutable pointers returned only after fresh-process finalization."""

    output_dir: Path
    manifest_path: Path
    completion_path: Path
    probe_count: int


class _StrictUtcClock:
    """Monotonic RFC3339 UTC values shared by gateway and worker validation."""

    def __init__(self) -> None:
        self._last: datetime | None = None

    def __call__(self) -> str:
        value = datetime.now(UTC)
        if self._last is not None and value <= self._last:
            value = self._last + timedelta(microseconds=1)
        self._last = value
        return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


class _RaisingPublisher:
    """Simulate an unavailable durable publisher after trace capture."""

    def __init__(self) -> None:
        self.calls = 0

    def publish(self, *, trace: object, context: object) -> TracePublicationReceipt:
        del trace, context
        self.calls += 1
        raise OSError("synthetic publisher unavailable")


class _RaisingRecorder:
    """Prove foreground success survives a post-response recorder fault."""

    def __init__(self) -> None:
        self.calls = 0

    def offer(self, observation: object) -> object:
        del observation
        self.calls += 1
        raise OSError("synthetic post-response recorder fault")


class _CountingPublisher:
    """Trap publisher: its call count proves invalid traces never publish."""

    def __init__(self) -> None:
        self.calls = 0

    def publish(self, *, trace: object, context: object) -> TracePublicationReceipt:
        del trace, context
        self.calls += 1
        raise AssertionError("invalid trace must not be published")


class _TimeoutExecutor:
    """Inject an executor timeout after real foreground requests finish."""

    def __init__(self) -> None:
        self.calls = 0

    def capture(self, observations: tuple[object, ...]) -> object:
        del observations
        self.calls += 1
        raise TimeoutError("synthetic shadow timeout")


class _IdentityMismatchExecutor:
    """Use a real trace then make only its binding evidence invalid."""

    def __init__(self, delegate: object) -> None:
        self._delegate = delegate
        self.calls = 0

    def capture(self, observations: tuple[object, ...]) -> object:
        self.calls += 1
        trace = self._delegate.capture(observations)
        return replace(
            trace,
            flat_identity=replace(trace.flat_identity, pre_binding_match=False),
        )


def _json_value(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_value(asdict(value))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if hasattr(value, "value"):
        return value.value
    return value


def _stream_request(
    *, configuration: Exp008Configuration, request_id: int
) -> RangeQueryRequest:
    stream = next(item for item in configuration.streams if item.metric is Metric.L2)
    return RangeQueryRequest(
        request_id=request_id,
        stream_key=stream.stream_key,
        query_vector=tuple(float(value) for value in configuration.measured_queries[request_id]),
        threshold_radius=stream.threshold_radius,
        range_filter=0.0,
        limit=RESULT_LIMIT,
        served_ef=stream.served_ef,
    )


def _worker(
    *,
    recorder: BoundedHostObservationRecorder,
    executor: object,
    publisher: object,
    state_directory: Path,
    clock: _StrictUtcClock,
) -> BackgroundShadowWorker:
    return BackgroundShadowWorker(
        recorder=recorder,
        executor=executor,
        publisher=publisher,
        state_store=FileHostWorkerStateStore(state_directory),
        registered_trace_parameters=RegisteredTraceParameters(
            allowed_candidate_and_lkg_efs=frozenset({200, 400, 800}),
            sentinel_ef=100,
        ),
        max_partial_streams=2,
        max_observation_age_seconds=60.0,
        clock=clock,
    )


def _gateway(runtime: Exp008Runtime, recorder: object, clock: _StrictUtcClock) -> ReferenceRangeGateway:
    return ReferenceRangeGateway(
        serving_executor=runtime.serving,
        recorder=recorder,
        clock=clock,
    )


def _serve(
    *, gateway: ReferenceRangeGateway, configuration: Exp008Configuration, count: int
) -> tuple[object, ...]:
    return tuple(
        gateway.execute(_stream_request(configuration=configuration, request_id=request_id))
        for request_id in range(count)
    )


def _observation_reason_codes(responses: tuple[object, ...]) -> tuple[str, ...]:
    """Return the exact post-response receipt codes emitted by the gateway."""

    codes: list[str] = []
    for response in responses:
        receipt = getattr(response, "observation_receipt", None)
        code = getattr(receipt, "reason_code", None)
        if code is None:
            status = getattr(receipt, "status", None)
            code = getattr(status, "value", status)
        if not isinstance(code, str) or not code:
            raise EXP008FailureProbeError("FOREGROUND_RECEIPT_EVIDENCE_INVALID")
        codes.append(code)
    return tuple(codes)


def _write_probe(root: Path, result: FailureProbeResult) -> None:
    write_immutable_json(root / "probes" / f"{result.name}.json", _json_value(result))


def run_failure_probes(
    *,
    configuration: Exp008Configuration,
    runtime: Exp008Runtime,
    output_dir: str | os.PathLike[str],
    pre_run_resources: Mapping[str, object],
    capture_git: Mapping[str, object],
) -> FailureProbeCapture:
    """Run all five registered H4 probes and close the injected live runtime.

    The only calls that may reach the supplied serving executor are foreground
    L2 threshold requests.  The source, executor, identity, and restart faults
    are introduced after those requests complete.  No monitor/policy/actuation
    object exists in this composition.
    """

    if not isinstance(configuration, Exp008Configuration):
        raise TypeError("configuration must be an Exp008Configuration")
    if not isinstance(runtime, Exp008Runtime):
        raise TypeError("runtime must be an Exp008Runtime")
    root = Path(output_dir)
    if root.exists():
        raise FileExistsError(f"refusing to overwrite immutable failure evidence: {root}")
    root.mkdir(mode=0o700, parents=True)
    root.chmod(0o700)
    if not isinstance(pre_run_resources.get("timestamp_utc"), str):
        raise EXP008FailureProbeError("PRE_RUN_RESOURCE_SNAPSHOT_INVALID")
    if (
        not isinstance(capture_git.get("commit"), str)
        or not isinstance(capture_git.get("dirty"), bool)
    ):
        raise EXP008FailureProbeError("CAPTURE_GIT_STATE_INVALID")
    clock = _StrictUtcClock()
    results: list[FailureProbeResult] = []
    closed = False
    try:
        preflight = runtime.serving.preflight()
        if len(preflight) != 2 or not all(item.complete for item in preflight.values()):
            raise EXP008FailureProbeError("SERVING_PREFLIGHT_INCOMPLETE")
        write_immutable_json(root / "pre_run_resources.json", dict(pre_run_resources))
        write_immutable_json(root / "serving_preflight.json", _preflight_document(preflight))

        isolation_recorder = _RaisingRecorder()
        isolation_responses = _serve(
            gateway=_gateway(runtime, isolation_recorder, clock),
            configuration=configuration,
            count=1,
        )
        isolation_result = FailureProbeResult(
            name="foreground_recorder_failure",
            expected_reason_code="RECORDER_FAILED",
            foreground_success=all(response.served_outcome.success for response in isolation_responses),
            fail_closed=(
                isolation_recorder.calls == 1
                and isolation_responses[0].observation_receipt.reason_code == "RECORDER_FAILED"
            ),
            foreground_request_count=len(isolation_responses),
            foreground_success_count=sum(
                response.served_outcome.success for response in isolation_responses
            ),
            foreground_observation_reason_codes=_observation_reason_codes(isolation_responses),
            worker_cycle=None,
            publisher_call_count=0,
            detail="real foreground query succeeded while its post-response recorder raised",
        )
        results.append(isolation_result)
        _write_probe(root, isolation_result)

        queue_recorder = BoundedHostObservationRecorder(max_pending_observations=1)
        queue_responses = _serve(
            gateway=_gateway(runtime, queue_recorder, clock),
            configuration=configuration,
            count=2,
        )
        queue_result = FailureProbeResult(
            name="queue_full",
            expected_reason_code="PENDING_OBSERVATION_CAPACITY_EXCEEDED",
            foreground_success=all(response.served_outcome.success for response in queue_responses),
            fail_closed=(
                queue_responses[0].observation_receipt.status.value == "ACCEPTED"
                and queue_responses[1].observation_receipt.reason_code
                == "PENDING_OBSERVATION_CAPACITY_EXCEEDED"
            ),
            foreground_request_count=len(queue_responses),
            foreground_success_count=sum(
                response.served_outcome.success for response in queue_responses
            ),
            foreground_observation_reason_codes=_observation_reason_codes(queue_responses),
            worker_cycle=None,
            publisher_call_count=0,
            detail="second post-response observation dropped; no worker invoked",
        )
        results.append(queue_result)
        _write_probe(root, queue_result)

        publisher_recorder = BoundedHostObservationRecorder(max_pending_observations=50)
        publisher_responses = _serve(
            gateway=_gateway(runtime, publisher_recorder, clock),
            configuration=configuration,
            count=50,
        )
        unavailable_publisher = _RaisingPublisher()
        publisher_cycle = _worker(
            recorder=publisher_recorder,
            executor=runtime.shadow,
            publisher=unavailable_publisher,
            state_directory=root / "state" / "publisher_unavailable",
            clock=clock,
        ).run_once(max_observations=50)
        publisher_result = FailureProbeResult(
            name="publisher_unavailable",
            expected_reason_code="PUBLISH_OUTCOME_UNKNOWN",
            foreground_success=all(response.served_outcome.success for response in publisher_responses),
            fail_closed=(
                "PUBLISH_OUTCOME_UNKNOWN" in publisher_cycle.reason_codes
                and publisher_cycle.published_trace_count == 0
                and unavailable_publisher.calls == 1
            ),
            foreground_request_count=len(publisher_responses),
            foreground_success_count=sum(
                response.served_outcome.success for response in publisher_responses
            ),
            foreground_observation_reason_codes=_observation_reason_codes(publisher_responses),
            worker_cycle=publisher_cycle,
            publisher_call_count=unavailable_publisher.calls,
            detail="real shadow trace captured, unavailable publisher blocks its stream",
        )
        results.append(publisher_result)
        _write_probe(root, publisher_result)

        timeout_recorder = BoundedHostObservationRecorder(max_pending_observations=50)
        timeout_responses = _serve(
            gateway=_gateway(runtime, timeout_recorder, clock),
            configuration=configuration,
            count=50,
        )
        timeout_executor = _TimeoutExecutor()
        timeout_publisher = _CountingPublisher()
        timeout_cycle = _worker(
            recorder=timeout_recorder,
            executor=timeout_executor,
            publisher=timeout_publisher,
            state_directory=root / "state" / "executor_timeout",
            clock=clock,
        ).run_once(max_observations=50)
        timeout_result = FailureProbeResult(
            name="executor_timeout",
            expected_reason_code="EXECUTOR_CAPTURE_FAILED",
            foreground_success=all(response.served_outcome.success for response in timeout_responses),
            fail_closed=(
                "EXECUTOR_CAPTURE_FAILED" in timeout_cycle.reason_codes
                and timeout_cycle.published_trace_count == 0
                and timeout_publisher.calls == 0
            ),
            foreground_request_count=len(timeout_responses),
            foreground_success_count=sum(
                response.served_outcome.success for response in timeout_responses
            ),
            foreground_observation_reason_codes=_observation_reason_codes(timeout_responses),
            worker_cycle=timeout_cycle,
            publisher_call_count=timeout_publisher.calls,
            detail="timeout after foreground completion rejects trace before publication",
        )
        results.append(timeout_result)
        _write_probe(root, timeout_result)

        identity_recorder = BoundedHostObservationRecorder(max_pending_observations=50)
        identity_responses = _serve(
            gateway=_gateway(runtime, identity_recorder, clock),
            configuration=configuration,
            count=50,
        )
        identity_executor = _IdentityMismatchExecutor(runtime.shadow)
        identity_publisher = _CountingPublisher()
        identity_cycle = _worker(
            recorder=identity_recorder,
            executor=identity_executor,
            publisher=identity_publisher,
            state_directory=root / "state" / "identity_mismatch",
            clock=clock,
        ).run_once(max_observations=50)
        identity_result = FailureProbeResult(
            name="identity_mismatch",
            expected_reason_code="TRACE_IDENTITY_MISMATCH",
            foreground_success=all(response.served_outcome.success for response in identity_responses),
            fail_closed=(
                "TRACE_IDENTITY_MISMATCH" in identity_cycle.reason_codes
                and identity_cycle.published_trace_count == 0
                and identity_publisher.calls == 0
            ),
            foreground_request_count=len(identity_responses),
            foreground_success_count=sum(
                response.served_outcome.success for response in identity_responses
            ),
            foreground_observation_reason_codes=_observation_reason_codes(identity_responses),
            worker_cycle=identity_cycle,
            publisher_call_count=identity_publisher.calls,
            detail="real shadow trace deliberately invalidated before publication",
        )
        results.append(identity_result)
        _write_probe(root, identity_result)

        restart_recorder = BoundedHostObservationRecorder(max_pending_observations=50)
        restart_responses = _serve(
            gateway=_gateway(runtime, restart_recorder, clock),
            configuration=configuration,
            count=1,
        )
        restart_publisher = _CountingPublisher()
        state_directory = root / "state" / "worker_restart_partial_loss"
        first_worker = _worker(
            recorder=restart_recorder,
            executor=_TimeoutExecutor(),
            publisher=restart_publisher,
            state_directory=state_directory,
            clock=clock,
        )
        restart_cycle = first_worker.run_once(max_observations=1)
        recovered_worker = _worker(
            recorder=BoundedHostObservationRecorder(max_pending_observations=50),
            executor=_TimeoutExecutor(),
            publisher=restart_publisher,
            state_directory=state_directory,
            clock=clock,
        )
        recovered_state = FileHostWorkerStateStore(state_directory).snapshot()
        stream_key = _stream_request(configuration=configuration, request_id=0).stream_key
        restart_loss = recovered_state.streams[stream_key].restart_loss_count
        del recovered_worker
        restart_result = FailureProbeResult(
            name="worker_restart_partial_loss",
            expected_reason_code="RESTART_LOSS_COUNT_EXACT",
            foreground_success=all(response.served_outcome.success for response in restart_responses),
            fail_closed=(
                restart_cycle.published_trace_count == 0
                and restart_publisher.calls == 0
                and restart_loss == 1
            ),
            foreground_request_count=len(restart_responses),
            foreground_success_count=sum(
                response.served_outcome.success for response in restart_responses
            ),
            foreground_observation_reason_codes=_observation_reason_codes(restart_responses),
            worker_cycle=restart_cycle,
            publisher_call_count=restart_publisher.calls,
            detail=f"volatile partial batch discarded exactly once on restart; restart_loss_count={restart_loss}",
        )
        results.append(restart_result)
        _write_probe(root, restart_result)

        if not all(result.foreground_success and result.fail_closed for result in results):
            raise EXP008FailureProbeError("H4_PROBE_ASSERTION_FAILED")
        postflight = runtime.serving.preflight()
        if len(postflight) != 2 or not all(item.complete for item in postflight.values()):
            raise EXP008FailureProbeError("SERVING_POSTFLIGHT_INCOMPLETE")
        write_immutable_json(root / "serving_postflight.json", _preflight_document(postflight))
        receipt = {
            "schema_version": "exp008-h4-failure-probes-v1",
            "status": "CAPTURE_COMPLETE_AWAITING_FRESH_PROCESS_FINALIZATION",
            "capture_git": dict(capture_git),
            "started_at_utc": pre_run_resources["timestamp_utc"],
            "probes": _json_value(results),
            "published_trace_count": 0,
            "monitor_call_count": 0,
            "policy_call_count": 0,
            "actuation_call_count": 0,
        }
        write_immutable_json(root / "capture_receipt.json", receipt)
        runtime.close()
        closed = True
        return FailureProbeCapture(
            output_dir=root,
            probes=tuple(results),
            published_trace_count=0,
            monitor_call_count=0,
            policy_call_count=0,
            actuation_call_count=0,
        )
    except Exception as exc:
        if not closed:
            try:
                runtime.close()
            except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001,S110
                pass
        try:
            write_immutable_json(
                root / "failure.json",
                {
                    "schema_version": _SCHEMA_VERSION,
                    "status": "INCOMPLETE",
                    "reason_code": (
                        str(exc)
                        if isinstance(exc, EXP008FailureProbeError)
                        else type(exc).__name__
                    ),
                },
            )
        except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001,S110
            pass
        raise


_SCHEMA_VERSION = "exp008-h4-failure-probes-v1"
_PROBE_EXPECTATIONS = MappingProxyType(
    {
        "foreground_recorder_failure": "RECORDER_FAILED",
        "queue_full": "PENDING_OBSERVATION_CAPACITY_EXCEEDED",
        "publisher_unavailable": "PUBLISH_OUTCOME_UNKNOWN",
        "executor_timeout": "EXECUTOR_CAPTURE_FAILED",
        "identity_mismatch": "TRACE_IDENTITY_MISMATCH",
        "worker_restart_partial_loss": "RESTART_LOSS_COUNT_EXACT",
    }
)
_STREAM_IDS = frozenset({"exp008-l2-stationary", "exp008-cosine-stationary"})
_PROBE_FOREGROUND_COUNTS = MappingProxyType(
    {
        "foreground_recorder_failure": 1,
        "queue_full": 2,
        "publisher_unavailable": 50,
        "executor_timeout": 50,
        "identity_mismatch": 50,
        "worker_restart_partial_loss": 1,
    }
)
_PROBE_RECEIPT_CODES = MappingProxyType(
    {
        "foreground_recorder_failure": ("RECORDER_FAILED",),
        "queue_full": ("ACCEPTED", "PENDING_OBSERVATION_CAPACITY_EXCEEDED"),
        "publisher_unavailable": ("ACCEPTED",) * 50,
        "executor_timeout": ("ACCEPTED",) * 50,
        "identity_mismatch": ("ACCEPTED",) * 50,
        "worker_restart_partial_loss": ("ACCEPTED",),
    }
)
_PROBE_WORKER_CYCLES = MappingProxyType(
    {
        "foreground_recorder_failure": None,
        "queue_full": None,
        "publisher_unavailable": {
            "drained_observation_count": 50,
            "captured_trace_count": 1,
            "published_trace_count": 0,
            "rejected_observation_count": 50,
            "blocked_stream_count": 1,
            "reason_codes": ["PUBLISH_OUTCOME_UNKNOWN"],
        },
        "executor_timeout": {
            "drained_observation_count": 50,
            "captured_trace_count": 0,
            "published_trace_count": 0,
            "rejected_observation_count": 50,
            "blocked_stream_count": 0,
            "reason_codes": ["EXECUTOR_CAPTURE_FAILED"],
        },
        "identity_mismatch": {
            "drained_observation_count": 50,
            "captured_trace_count": 0,
            "published_trace_count": 0,
            "rejected_observation_count": 50,
            "blocked_stream_count": 0,
            "reason_codes": ["TRACE_IDENTITY_MISMATCH"],
        },
        "worker_restart_partial_loss": {
            "drained_observation_count": 1,
            "captured_trace_count": 0,
            "published_trace_count": 0,
            "rejected_observation_count": 0,
            "blocked_stream_count": 0,
            "reason_codes": [],
        },
    }
)
_PROBE_PUBLISHER_CALL_COUNTS = MappingProxyType(
    {
        "foreground_recorder_failure": 0,
        "queue_full": 0,
        "publisher_unavailable": 1,
        "executor_timeout": 0,
        "identity_mismatch": 0,
        "worker_restart_partial_loss": 0,
    }
)
_WORKER_STATE_EXPECTATIONS = MappingProxyType(
    {
        "publisher_unavailable": {
            "blocked_reason_code": "PUBLISH_OUTCOME_UNKNOWN",
            "rejected_observation_count": 50,
            "restart_loss_count": 0,
        },
        "executor_timeout": {
            "blocked_reason_code": None,
            "rejected_observation_count": 50,
            "restart_loss_count": 0,
        },
        "identity_mismatch": {
            "blocked_reason_code": None,
            "rejected_observation_count": 50,
            "restart_loss_count": 0,
        },
        "worker_restart_partial_loss": {
            "blocked_reason_code": None,
            "rejected_observation_count": 0,
            "restart_loss_count": 1,
        },
    }
)


def _capture_artifact_paths() -> frozenset[str]:
    """The closed set produced before fresh-process finalization."""

    state_probe_names = {
        "publisher_unavailable",
        "executor_timeout",
        "identity_mismatch",
        "worker_restart_partial_loss",
    }
    return frozenset(
        {
            "capture_receipt.json",
            "pre_run_resources.json",
            "serving_preflight.json",
            "serving_postflight.json",
            *(f"probes/{name}.json" for name in _PROBE_EXPECTATIONS),
            *(
                f"state/{name}/host-worker-state.json" for name in state_probe_names
            ),
        }
    )


def _final_artifact_paths() -> frozenset[str]:
    return _capture_artifact_paths() | frozenset({"post_run_resources.json"})


def _artifact_file_paths(root: Path) -> frozenset[str]:
    paths: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise EXP008FailureProbeError("ARTIFACT_SYMLINK_REJECTED")
        if path.is_file():
            paths.add(str(path.relative_to(root)))
    return frozenset(paths)


def _verify_artifact_inventory(*, root: Path, finalized: bool) -> None:
    excluded = frozenset({"run_manifest.json", "completion.json"}) if finalized else frozenset()
    expected = (_final_artifact_paths() if finalized else _capture_artifact_paths()) | excluded
    if _artifact_file_paths(root) != expected:
        raise EXP008FailureProbeError("CAPTURE_ARTIFACT_INVENTORY_INVALID")


def _artifact_hashes(root: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name in {"run_manifest.json", "completion.json"}:
            continue
        if path.is_symlink():
            raise EXP008FailureProbeError("ARTIFACT_SYMLINK_REJECTED")
        values[str(path.relative_to(root))] = sha256_file(path)
    return values


def _load_json(path: Path, *, reason: str) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EXP008FailureProbeError(reason) from exc


def _preflight_document(preflight: Mapping[object, object]) -> dict[str, object]:
    """Serialize immutable runtime keys by their stable stream IDs only."""

    document: dict[str, object] = {}
    for key, value in preflight.items():
        stream_id = getattr(key, "stream_id", None)
        if not isinstance(stream_id, str) or stream_id in document:
            raise EXP008FailureProbeError("SERVING_PREFLIGHT_DOCUMENT_INVALID")
        document[stream_id] = _json_value(value)
    if frozenset(document) != _STREAM_IDS:
        raise EXP008FailureProbeError("SERVING_PREFLIGHT_DOCUMENT_INVALID")
    return document


def _verify_preflight_document(path: Path) -> None:
    document = _load_json(path, reason="SERVING_PREFLIGHT_ARTIFACT_UNAVAILABLE")
    if (
        not isinstance(document, dict)
        or frozenset(document) != _STREAM_IDS
        or not all(
            isinstance(value, dict)
            and value.get("complete") is True
            and value.get("checked_stream_count") == 1
            and value.get("reason_codes") == []
            for value in document.values()
        )
    ):
        raise EXP008FailureProbeError("SERVING_PREFLIGHT_EVIDENCE_INVALID")


def _verify_probe_document(*, name: str, document: object) -> Mapping[str, object]:
    """Validate one probe's observed facts, not its self-declared conclusion."""

    required_keys = {
        "name",
        "expected_reason_code",
        "foreground_success",
        "fail_closed",
        "foreground_request_count",
        "foreground_success_count",
        "foreground_observation_reason_codes",
        "worker_cycle",
        "publisher_call_count",
        "detail",
    }
    if not isinstance(document, dict) or set(document) != required_keys:
        raise EXP008FailureProbeError("PROBE_EVIDENCE_INVALID")
    request_count = _PROBE_FOREGROUND_COUNTS[name]
    if (
        document.get("name") != name
        or document.get("expected_reason_code") != _PROBE_EXPECTATIONS[name]
        or document.get("foreground_success") is not True
        or document.get("fail_closed") is not True
        or type(document.get("foreground_request_count")) is not int
        or document["foreground_request_count"] != request_count
        or type(document.get("foreground_success_count")) is not int
        or document["foreground_success_count"] != request_count
        or document.get("foreground_observation_reason_codes")
        != list(_PROBE_RECEIPT_CODES[name])
        or document.get("worker_cycle") != _PROBE_WORKER_CYCLES[name]
        or type(document.get("publisher_call_count")) is not int
        or document["publisher_call_count"] != _PROBE_PUBLISHER_CALL_COUNTS[name]
        or not isinstance(document.get("detail"), str)
        or not document["detail"]
    ):
        raise EXP008FailureProbeError("PROBE_EVIDENCE_INVALID")
    return document


def _verify_worker_state_evidence(root: Path) -> None:
    """Prove durable worker state agrees with each H4 worker-failure record."""

    stream_key_fields = {
        "stream_id",
        "metric",
        "threshold_stratum",
        "configuration_identity",
        "data_identity",
        "flat_binding_id",
        "hnsw_binding_id",
    }
    state_fields = {
        "stream_key",
        "next_trace_ordinal",
        "partial_observation_count",
        "inflight_observation_count",
        "restart_loss_count",
        "rejected_observation_count",
        "blocked_reason_code",
    }
    canonical_stream_key: Mapping[str, object] | None = None
    for name, expected in _WORKER_STATE_EXPECTATIONS.items():
        state_path = root / "state" / name / "host-worker-state.json"
        document = _load_json(state_path, reason="STATE_EVIDENCE_INVALID")
        if (
            not isinstance(document, dict)
            or set(document) != {"schema_version", "streams"}
            or document["schema_version"] != "host-worker-state-v1"
            or not isinstance(document["streams"], list)
            or len(document["streams"]) != 1
            or not isinstance(document["streams"][0], dict)
        ):
            raise EXP008FailureProbeError("STATE_EVIDENCE_INVALID")
        stream = document["streams"][0]
        stream_key = stream.get("stream_key")
        if (
            set(stream) != state_fields
            or not isinstance(stream_key, dict)
            or set(stream_key) != stream_key_fields
            or stream_key.get("stream_id") != "exp008-l2-stationary"
            or stream_key.get("metric") != "L2"
            or stream_key.get("threshold_stratum") != "target-075"
            or any(
                not isinstance(stream_key[field], str) or not stream_key[field]
                for field in stream_key_fields
            )
            or any(
                type(stream.get(field)) is not int or stream[field] < 0
                for field in (
                    "next_trace_ordinal",
                    "partial_observation_count",
                    "inflight_observation_count",
                    "restart_loss_count",
                    "rejected_observation_count",
                )
            )
            or stream["next_trace_ordinal"] != 0
            or stream["partial_observation_count"] != 0
            or stream["inflight_observation_count"] != 0
            or stream["blocked_reason_code"] != expected["blocked_reason_code"]
            or stream["rejected_observation_count"]
            != expected["rejected_observation_count"]
            or stream["restart_loss_count"] != expected["restart_loss_count"]
        ):
            raise EXP008FailureProbeError("STATE_EVIDENCE_INVALID")
        if canonical_stream_key is None:
            canonical_stream_key = stream_key
        elif stream_key != canonical_stream_key:
            raise EXP008FailureProbeError("STATE_EVIDENCE_INVALID")


def _verify_capture(root: Path, *, finalized: bool = False) -> Mapping[str, object]:
    _verify_artifact_inventory(root=root, finalized=finalized)
    receipt = _load_json(root / "capture_receipt.json", reason="CAPTURE_RECEIPT_UNAVAILABLE")
    if (
        not isinstance(receipt, dict)
        or set(receipt)
        != {
            "schema_version",
            "status",
            "capture_git",
            "started_at_utc",
            "probes",
            "published_trace_count",
            "monitor_call_count",
            "policy_call_count",
            "actuation_call_count",
        }
        or receipt["schema_version"] != _SCHEMA_VERSION
        or receipt["status"] != "CAPTURE_COMPLETE_AWAITING_FRESH_PROCESS_FINALIZATION"
        or not isinstance(receipt["capture_git"], dict)
        or set(receipt["capture_git"]) != {"commit", "dirty"}
        or not isinstance(receipt["capture_git"].get("commit"), str)
        or not isinstance(receipt["capture_git"].get("dirty"), bool)
        or not isinstance(receipt.get("started_at_utc"), str)
    ):
        raise EXP008FailureProbeError("CAPTURE_RECEIPT_INVALID")
    _verify_preflight_document(root / "serving_preflight.json")
    _verify_preflight_document(root / "serving_postflight.json")
    _verify_worker_state_evidence(root)
    probe_documents: dict[str, Mapping[str, object]] = {}
    for name in _PROBE_EXPECTATIONS:
        document = _load_json(root / "probes" / f"{name}.json", reason="PROBE_ARTIFACT_UNAVAILABLE")
        probe_documents[name] = _verify_probe_document(name=name, document=document)
    if receipt["probes"] != [probe_documents[name] for name in _PROBE_EXPECTATIONS]:
        raise EXP008FailureProbeError("CAPTURE_RECEIPT_INVALID")
    if (
        receipt.get("published_trace_count") != 0
        or receipt.get("monitor_call_count") != 0
        or receipt.get("policy_call_count") != 0
        or receipt.get("actuation_call_count") != 0
    ):
        raise EXP008FailureProbeError("CAPTURE_NON_ACTUATION_INVALID")
    return receipt


def _manifest_payload_sha256(manifest: Mapping[str, object]) -> str:
    """Hash the canonical manifest payload while excluding its self-reference."""

    payload = dict(manifest)
    payload.pop("self_sha256", None)
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def verify_failure_probe_bundle(
    output_dir: str | os.PathLike[str],
) -> Mapping[str, object]:
    """Independently validate a finalized EXP-008 H1/H4 evidence bundle.

    This verifier is safe to run in a fresh process: it opens no live clients
    and does not mutate evidence.  It fails closed on any inventory, raw probe,
    hash, receipt, or completion inconsistency.
    """

    root = Path(output_dir)
    _verify_capture(root, finalized=True)
    manifest_path = root / "run_manifest.json"
    completion_path = root / "completion.json"
    manifest = _load_json(manifest_path, reason="MANIFEST_UNAVAILABLE")
    completion = _load_json(completion_path, reason="COMPLETION_UNAVAILABLE")
    manifest_keys = {
        "schema_version",
        "execution_mode",
        "started_at_utc",
        "capture_git",
        "finalizer_git",
        "detector_seed",
        "probe_expectations",
        "no_monitor_policy_or_actuation",
        "artifact_sha256",
        "self_sha256",
    }
    if (
        not isinstance(manifest, dict)
        or set(manifest) != manifest_keys
        or manifest["schema_version"] != _SCHEMA_VERSION
        or manifest["execution_mode"] != "DRY_RUN"
        or not isinstance(manifest["started_at_utc"], str)
        or not manifest["started_at_utc"]
        or not isinstance(manifest["capture_git"], dict)
        or set(manifest["capture_git"]) != {"commit", "dirty"}
        or not isinstance(manifest["capture_git"].get("commit"), str)
        or manifest["capture_git"].get("dirty") is not False
        or not isinstance(manifest["finalizer_git"], dict)
        or set(manifest["finalizer_git"]) != {"commit", "dirty"}
        or not isinstance(manifest["finalizer_git"].get("commit"), str)
        or manifest["finalizer_git"].get("dirty") is not False
        or manifest["finalizer_git"] != manifest["capture_git"]
        or manifest["detector_seed"] != EXP008_DETECTOR_SEED
        or manifest["probe_expectations"] != dict(_PROBE_EXPECTATIONS)
        or manifest["no_monitor_policy_or_actuation"] is not True
        or not isinstance(manifest["artifact_sha256"], dict)
        or not isinstance(manifest["self_sha256"], str)
        or manifest["self_sha256"] != _manifest_payload_sha256(manifest)
    ):
        raise EXP008FailureProbeError("MANIFEST_INVALID")
    expected_hashes = {
        relative: sha256_file(root / relative)
        for relative in sorted(_final_artifact_paths())
    }
    if manifest["artifact_sha256"] != expected_hashes:
        raise EXP008FailureProbeError("ARTIFACT_HASH_MISMATCH")
    completion_keys = {
        "schema_version",
        "status",
        "manifest_sha256",
        "probe_count",
        "published_trace_count",
        "monitor_call_count",
        "policy_call_count",
        "actuation_call_count",
    }
    if (
        not isinstance(completion, dict)
        or set(completion) != completion_keys
        or completion["schema_version"] != _SCHEMA_VERSION
        or completion["status"] != "COMPLETE"
        or completion["manifest_sha256"] != sha256_file(manifest_path)
        or completion["probe_count"] != len(_PROBE_EXPECTATIONS)
        or any(
            completion[field] != 0
            for field in (
                "published_trace_count",
                "monitor_call_count",
                "policy_call_count",
                "actuation_call_count",
            )
        )
    ):
        raise EXP008FailureProbeError("COMPLETION_INVALID")
    return MappingProxyType(
        {
            "probe_count": completion["probe_count"],
            "capture_git": manifest["capture_git"],
            "finalizer_git": manifest["finalizer_git"],
            "manifest_sha256": completion["manifest_sha256"],
        }
    )


def finalize_failure_probes(
    *,
    output_dir: str | os.PathLike[str],
    post_run_resources: Mapping[str, object],
    repository: str | os.PathLike[str],
) -> FailureProbeRunResult:
    """Finalize H4 evidence only from a fresh, no-gRPC process."""

    root = Path(output_dir)
    if (root / "run_manifest.json").exists() or (root / "completion.json").exists():
        raise EXP008FailureProbeError("FAILURE_PROBES_ALREADY_FINALIZED")
    if not isinstance(post_run_resources.get("timestamp_utc"), str):
        raise EXP008FailureProbeError("POST_RUN_RESOURCE_SNAPSHOT_INVALID")
    receipt = _verify_capture(root)
    current_git = git_state(Path(repository))
    if current_git.get("commit") != receipt["capture_git"].get("commit"):
        raise EXP008FailureProbeError("CAPTURE_COMMIT_CHANGED_BEFORE_FINALIZATION")
    write_immutable_json(root / "post_run_resources.json", dict(post_run_resources))
    manifest: dict[str, object] = {
        "schema_version": _SCHEMA_VERSION,
        "execution_mode": "DRY_RUN",
        "started_at_utc": receipt["started_at_utc"],
        "capture_git": receipt["capture_git"],
        "finalizer_git": current_git,
        "detector_seed": EXP008_DETECTOR_SEED,
        "probe_expectations": dict(_PROBE_EXPECTATIONS),
        "no_monitor_policy_or_actuation": True,
    }
    manifest["artifact_sha256"] = _artifact_hashes(root)
    manifest["self_sha256"] = _manifest_payload_sha256(manifest)
    manifest_path = root / "run_manifest.json"
    write_immutable_json(manifest_path, manifest)
    completion_path = root / "completion.json"
    write_immutable_json(
        completion_path,
        {
            "schema_version": _SCHEMA_VERSION,
            "status": "COMPLETE",
            "manifest_sha256": sha256_file(manifest_path),
            "probe_count": len(_PROBE_EXPECTATIONS),
            "published_trace_count": 0,
            "monitor_call_count": 0,
            "policy_call_count": 0,
            "actuation_call_count": 0,
        },
    )
    return FailureProbeRunResult(
        output_dir=root,
        manifest_path=manifest_path,
        completion_path=completion_path,
        probe_count=len(_PROBE_EXPECTATIONS),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri")
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--l2-baseline", type=Path, required=True)
    parser.add_argument("--cosine-baseline", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--etcd-container", default="milvus-etcd")
    parser.add_argument("--minio-container", default="milvus-minio")
    parser.add_argument("--finalize-only", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.finalize_only:
        result = finalize_failure_probes(
            output_dir=args.output_dir,
            post_run_resources=capture_host_resource_snapshot(timestamp_utc=_StrictUtcClock()()),
            repository=Path(__file__).parents[1],
        )
        print(json.dumps(_json_value(result), sort_keys=True))
        return 0
    if not isinstance(args.uri, str) or not args.uri:
        raise EXP008FailureProbeError("URI_REQUIRED_FOR_CAPTURE")
    configuration = prepare_exp008_configuration(
        dataset_dir=args.dataset_dir,
        l2_baseline_path=args.l2_baseline,
        cosine_baseline_path=args.cosine_baseline,
    )
    clock = _StrictUtcClock()
    pre_run_resources = capture_host_resource_snapshot(timestamp_utc=clock())
    capture_git = git_state(Path(__file__).parents[1])
    runtime = build_live_runtime(
        configuration=configuration,
        uri=args.uri,
        etcd_container=args.etcd_container,
        minio_container=args.minio_container,
    )
    run_failure_probes(
        configuration=configuration,
        runtime=runtime,
        output_dir=args.output_dir,
        pre_run_resources=pre_run_resources,
        capture_git=capture_git,
    )
    forwarded = list(sys.argv[1:] if argv is None else argv)
    if "--finalize-only" in forwarded:
        raise AssertionError("capture invocation unexpectedly includes --finalize-only")
    os.execv(
        sys.executable,
        [sys.executable, "-m", "experiments.exp008_failure_probes", *forwarded, "--finalize-only"],
    )
    raise AssertionError("os.execv unexpectedly returned")


if __name__ == "__main__":
    raise SystemExit(main())
