"""EXP-008 H4 read-only failure-containment capture.

This experiment deliberately injects post-response failures while retaining a
real foreground range-serving dependency in the live composition root.  It
never constructs a monitor, policy, or safe-actuation boundary; all faults are
contained before a trace can become monitor input.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from types import MappingProxyType
from typing import Any, Mapping

import numpy as np

from vdbench.artifacts import canonical_json_bytes, git_state, sha256_file, write_immutable_json
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
        value = datetime.now(timezone.utc)
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
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if hasattr(value, "value"):
        return getattr(value, "value")
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


def _gateway(runtime: Exp008Runtime, recorder: BoundedHostObservationRecorder, clock: _StrictUtcClock) -> ReferenceRangeGateway:
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
        write_immutable_json(root / "serving_preflight.json", _json_value(preflight))

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
        write_immutable_json(root / "serving_postflight.json", _json_value(postflight))
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
            except Exception:
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
        except Exception:
            pass
        raise


_SCHEMA_VERSION = "exp008-h4-failure-probes-v1"
_PROBE_EXPECTATIONS = MappingProxyType(
    {
        "queue_full": "PENDING_OBSERVATION_CAPACITY_EXCEEDED",
        "publisher_unavailable": "PUBLISH_OUTCOME_UNKNOWN",
        "executor_timeout": "EXECUTOR_CAPTURE_FAILED",
        "identity_mismatch": "TRACE_IDENTITY_MISMATCH",
        "worker_restart_partial_loss": "RESTART_LOSS_COUNT_EXACT",
    }
)


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


def _verify_preflight_document(path: Path) -> None:
    document = _load_json(path, reason="SERVING_PREFLIGHT_ARTIFACT_UNAVAILABLE")
    if (
        not isinstance(document, dict)
        or len(document) != 2
        or not all(
            isinstance(value, dict)
            and value.get("complete") is True
            and value.get("checked_stream_count") == 1
            and value.get("reason_codes") == []
            for value in document.values()
        )
    ):
        raise EXP008FailureProbeError("SERVING_PREFLIGHT_EVIDENCE_INVALID")


def _verify_capture(root: Path) -> Mapping[str, object]:
    receipt = _load_json(root / "capture_receipt.json", reason="CAPTURE_RECEIPT_UNAVAILABLE")
    if (
        not isinstance(receipt, dict)
        or receipt.get("schema_version") != _SCHEMA_VERSION
        or receipt.get("status") != "CAPTURE_COMPLETE_AWAITING_FRESH_PROCESS_FINALIZATION"
        or not isinstance(receipt.get("capture_git"), dict)
        or not isinstance(receipt.get("started_at_utc"), str)
    ):
        raise EXP008FailureProbeError("CAPTURE_RECEIPT_INVALID")
    _verify_preflight_document(root / "serving_preflight.json")
    _verify_preflight_document(root / "serving_postflight.json")
    probe_documents: dict[str, Mapping[str, object]] = {}
    for name, expected_reason in _PROBE_EXPECTATIONS.items():
        document = _load_json(root / "probes" / f"{name}.json", reason="PROBE_ARTIFACT_UNAVAILABLE")
        if (
            not isinstance(document, dict)
            or document.get("name") != name
            or document.get("expected_reason_code") != expected_reason
            or document.get("foreground_success") is not True
            or document.get("fail_closed") is not True
            or document.get("publisher_call_count") not in (0, 1)
        ):
            raise EXP008FailureProbeError("PROBE_EVIDENCE_INVALID")
        probe_documents[name] = document
    expected_publisher_calls = {
        "queue_full": 0,
        "publisher_unavailable": 1,
        "executor_timeout": 0,
        "identity_mismatch": 0,
        "worker_restart_partial_loss": 0,
    }
    if any(
        document["publisher_call_count"] != expected_publisher_calls[name]
        for name, document in probe_documents.items()
    ):
        raise EXP008FailureProbeError("PROBE_EVIDENCE_INVALID")
    extras = {path.stem for path in (root / "probes").glob("*.json")} - set(_PROBE_EXPECTATIONS)
    if extras:
        raise EXP008FailureProbeError("PROBE_EVIDENCE_INVALID")
    if (
        receipt.get("published_trace_count") != 0
        or receipt.get("monitor_call_count") != 0
        or receipt.get("policy_call_count") != 0
        or receipt.get("actuation_call_count") != 0
    ):
        raise EXP008FailureProbeError("CAPTURE_NON_ACTUATION_INVALID")
    return receipt


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
    manifest["self_sha256"] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
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
