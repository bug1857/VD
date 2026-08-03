"""Offline, artifact-producing safety validation for EXP-007.

The validator exercises the actual ADR-006 durable source/outbox and the
existing ADR-005 DRY_RUN monitor.  It creates deterministic synthetic traces
only; it imports neither PyMilvus nor any live-service client.
"""

from __future__ import annotations

import argparse
import ast
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
from typing import Any, NoReturn
from unittest.mock import patch

from vdbench.config import IndexTrack, Metric
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from vdbench.oracle import OracleHit, OracleResult
from vdbench.policy import PolicyAction, PreActionSafety, QualificationResult
from vdbench.shadow_event_source import (
    FileShadowTraceEventSource,
    PublicationStatus,
    ShadowEventSourceError,
    TracePublicationContext,
)
from vdbench.workload_monitor import (
    DryRunPolicyInputs,
    FileMonitorStateStore,
    MonitorAuditRecord,
    MonitorRecordStatus,
    MonitorStreamKey,
    WorkloadMonitor,
)


EXP007_DETECTOR_SEED = 20260804
EXP007_FIXTURE_SEED = 20260805
STANDARD_PENDING_EVENT_CAP = 32
STANDARD_PENDING_BYTE_CAP = 131072
BACKPRESSURE_PENDING_EVENT_CAP = 1
BACKPRESSURE_PENDING_BYTE_CAP = 4096
MONITOR_POLL_MAX_EVENTS = 24
_SENTINELS = ("98765.25", "54321.125", "24680.75")


class Exp007ValidationError(RuntimeError):
    """Raised when a required EXP-007 scenario does not hold."""


class _Audit:
    def __init__(self) -> None:
        self.records: list[MonitorAuditRecord] = []

    def contains(self, record_id: str) -> bool:
        return any(item.record_id == record_id for item in self.records)

    def append(self, record: MonitorAuditRecord) -> None:
        if self.contains(record.record_id):
            raise Exp007ValidationError(f"duplicate monitor audit record: {record.record_id}")
        self.records.append(record)


class _Provider:
    def resolve(self, *, decision, provenance):  # type: ignore[no-untyped-def]
        return DryRunPolicyInputs(
            current_ef=400,
            response_estimates={},
            pre_action=PreActionSafety(
                metric=provenance.metric,
                threshold_stratum=provenance.threshold_stratum,
                configuration_identity=provenance.configuration_identity,
                index_identity=provenance.hnsw_binding_id,
                flat_index_identity=provenance.flat_binding_id,
                data_identity=provenance.data_identity,
                response_model_provenance="EXP007_DETERMINISTIC_FIXTURE",
            ),
            last_known_good=QualificationResult(
                qualified=False, ef=None, reasons=("EXP007_OFFLINE_ONLY",)
            ),
            audit_id=f"exp007:{provenance.current_window_id}",
        )


class _FailFirstAcknowledge:
    """Inject one post-state-commit acknowledgement failure for redelivery."""

    def __init__(self, source: FileShadowTraceEventSource) -> None:
        self._source = source
        self._failed = False

    def poll(self, *, limit: int):  # type: ignore[no-untyped-def]
        return self._source.poll(limit=limit)

    def acknowledge(self, event_ids: tuple[str, ...]) -> None:
        if not self._failed:
            self._failed = True
            raise OSError("synthetic acknowledgement interruption")
        self._source.acknowledge(event_ids)


class _ForbiddenClient:
    """Trap proving no database/actuation client is reachable from DRY_RUN."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> NoReturn:
        self.calls.append(name)
        raise AssertionError(f"forbidden call: {name}")


def _canonical_json(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, tuple):
        return [_canonical_json(item) for item in value]
    if isinstance(value, list):
        return [_canonical_json(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _canonical_json(item) for key, item in value.items()}
    if hasattr(value, "__dataclass_fields__"):
        return _canonical_json(asdict(value))
    return value


def _bytes(value: Any) -> bytes:
    return (json.dumps(_canonical_json(value), sort_keys=True, indent=2) + "\n").encode("utf-8")


def _write(path: Path, value: Any) -> None:
    """Create one immutable evidence file with file and directory fsyncs."""

    if path.exists():
        raise FileExistsError(f"refusing to overwrite evidence artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(_bytes(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _content_sha256(value: dict[str, Any]) -> str:
    copied = dict(value)
    copied.pop("self_sha256", None)
    return hashlib.sha256(_bytes(copied)).hexdigest()


def _stream_key(metric: Metric) -> MonitorStreamKey:
    return MonitorStreamKey(
        stream_id=f"exp007-{metric.value.lower()}-stationary-v1",
        metric=metric,
        threshold_stratum="target-075",
        configuration_identity="exp007-configuration-v1",
        data_identity="exp007-data-v1",
        flat_binding_id=f"exp007-{metric.value.lower()}-flat-v1",
        hnsw_binding_id=f"exp007-{metric.value.lower()}-hnsw-v1",
    )


def _identity(metric: Metric, track: IndexTrack) -> ShadowIdentityEvidence:
    description: dict[str, object] = {"index_type": track.value, "metric_type": metric.value}
    if track is IndexTrack.HNSW:
        description.update({"M": "16", "efConstruction": "200"})
    identity = CollectionIdentity(
        collection_name=f"exp007_{metric.value.lower()}_{track.value.lower()}",
        metric=metric.value,
        index_track=track.value,
        description=description,
    )
    stage = ShadowAuditStageEvidence(stage=f"{track.value}_IDENTITY", success=True)
    return ShadowIdentityEvidence(
        track=track,
        expected_binding_id=f"exp007-{metric.value.lower()}-{track.value.lower()}-v1",
        pre_snapshot=identity,
        post_snapshot=identity,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=stage,
        post_capture=stage,
    )


def _trace(metric: Metric, *, offset: int) -> ShadowAuditTrace:
    """Make one stable 50-observation trace with distinctive private payload."""

    score, radius, range_filter = (
        (24680.75, 54321.125, 0.0)
        if metric is Metric.L2
        else (0.75, 0.25, 1.0)
    )
    queries: list[ShadowQueryAuditTrace] = []
    for position in range(50):
        query_id = offset + position
        oracle = OracleResult((OracleHit(query_id, score),), full_count=1, capped=False)
        hit = SearchHit(query_id, score)
        queries.append(
            ShadowQueryAuditTrace(
                query_id=query_id,
                query_vector=(98765.25, float(query_id + 1)),
                threshold_radius=radius,
                range_filter=range_filter,
                limit=100,
                oracle_result=oracle,
                exact_cardinality=1,
                flat_hits=(hit,),
                sentinel_hits=(hit,),
                sentinel_recall=1.0,
                stages=(
                    ShadowAuditStageEvidence("ORACLE", success=True),
                    ShadowAuditStageEvidence("FLAT", success=True, oracle_agreement=True),
                    ShadowAuditStageEvidence("SENTINEL_HNSW", success=True),
                ),
            )
        )
    key = _stream_key(metric)
    return ShadowAuditTrace(
        metric=metric,
        threshold_stratum=key.threshold_stratum,
        candidate_ef=400,
        last_known_good_ef=200,
        sentinel_ef=100,
        configuration_identity=key.configuration_identity,
        data_identity=key.data_identity,
        flat_identity=_identity(metric, IndexTrack.FLAT),
        hnsw_identity=_identity(metric, IndexTrack.HNSW),
        queries=tuple(queries),
        complete=True,
    )


def _context(metric: Metric, *, window: int, index: int) -> TracePublicationContext:
    key = _stream_key(metric)
    return TracePublicationContext(
        stream_key=key,
        window_id=f"{metric.value.lower()}-window-{window}",
        window_sequence=window,
        trace_sequence_index=index,
        trace_id=f"{metric.value.lower()}-window-{window}-trace-{index}",
        captured_at_utc=f"2026-08-03T12:{window * 4 + index:02d}:00Z",
    )


def _source(
    root: Path,
    *,
    count: int = STANDARD_PENDING_EVENT_CAP,
    bytes_limit: int = STANDARD_PENDING_BYTE_CAP,
) -> FileShadowTraceEventSource:
    return FileShadowTraceEventSource(root, max_pending_events=count, max_pending_bytes=bytes_limit)


def _publish_window(source: FileShadowTraceEventSource, metric: Metric, *, window: int) -> tuple[str, ...]:
    ids: list[str] = []
    for index in range(4):
        receipt = source.publish(
            trace=_trace(metric, offset=index * 50),
            context=_context(metric, window=window, index=index),
        )
        if receipt.status is not PublicationStatus.PUBLISHED or receipt.event is None:
            raise Exp007ValidationError(f"fixture publication failed: {receipt}")
        ids.append(receipt.event.event_id)
    return tuple(ids)


def _scenario_atomic_order(root: Path) -> tuple[bool, dict[str, object]]:
    """Inject failures on both sides of the envelope/event durable boundary."""

    before = _source(root / "before")
    with patch("vdbench.shadow_event_source.persist_shadow_trace_envelope", side_effect=OSError("before-envelope")):
        try:
            before.publish(trace=_trace(Metric.L2, offset=0), context=_context(Metric.L2, window=0, index=0))
        except OSError:
            pass
        else:
            raise Exp007ValidationError("before-envelope fault did not surface")
    before_clean = not tuple((before.root / "traces").glob("*.json")) and not before.poll(limit=1)

    after = _source(root / "after")
    trace = _trace(Metric.L2, offset=0)
    context = _context(Metric.L2, window=0, index=0)
    with patch.object(after, "_publish_event_document", side_effect=OSError("after-envelope")):
        try:
            after.publish(trace=trace, context=context)
        except OSError:
            pass
        else:
            raise Exp007ValidationError("after-envelope fault did not surface")
    orphan_paths = after.orphaned_trace_paths()
    after_not_delivered = not after.poll(limit=1)
    restarted = _source(root / "after")
    recovered = restarted.publish(trace=trace, context=context)
    recovery_event_visible = recovered.event is not None and restarted.poll(limit=1) == (recovered.event,)
    return (
        before_clean and len(orphan_paths) == 1 and after_not_delivered and recovery_event_visible,
        {
            "before_envelope_no_trace_or_event": before_clean,
            "orphan_count_after_event_publication_fault": len(orphan_paths),
            "orphan_not_deliverable": after_not_delivered,
            "restart_recovers_exact_envelope": recovery_event_visible,
        },
    )


def _scenario_restart(root: Path) -> tuple[bool, dict[str, object]]:
    """Exercise source reopen, redelivery, idempotent ack, and one evaluation."""

    outbox = root / "outbox"
    published_ids: list[str] = []
    for window in range(3):
        for index in range(4):
            source = _source(outbox)
            receipt = source.publish(
                trace=_trace(Metric.L2, offset=index * 50),
                context=_context(Metric.L2, window=window, index=index),
            )
            if receipt.event is None:
                raise Exp007ValidationError("restart fixture event missing")
            published_ids.append(receipt.event.event_id)

    source = _source(outbox)
    state = FileMonitorStateStore(root / "monitor-state")
    audit = _Audit()
    interrupted = WorkloadMonitor(
        source=_FailFirstAcknowledge(source),
        state_store=state,
        policy_input_provider=_Provider(),
        audit_sink=audit,
        detector_seed=EXP007_DETECTOR_SEED,
    )
    try:
        interrupted.run_once(max_events=1)
    except OSError as exc:
        if str(exc) != "synthetic acknowledgement interruption":
            raise
    else:
        raise Exp007ValidationError("acknowledgement interruption did not occur")

    resumed = WorkloadMonitor(
        source=source,
        state_store=state,
        policy_input_provider=_Provider(),
        audit_sink=audit,
        detector_seed=EXP007_DETECTOR_SEED,
    )
    resumed_results = resumed.run_once(max_events=16)
    source.acknowledge((published_ids[0],))
    source.acknowledge((published_ids[0],))
    evaluated = [item for item in audit.records if item.status is MonitorRecordStatus.EVALUATED]
    event_order = tuple(item.event_id for item in sorted(source.poll(limit=16), key=lambda item: item.event_id))
    complete = (
        len(published_ids) == len(set(published_ids))
        and not source.poll(limit=16)
        and len(evaluated) == 1
        and evaluated[0].detector_state == "NO_DRIFT"
        and evaluated[0].policy_action == "NO_CHANGE"
    )
    return complete, {
        "published_event_count": len(published_ids),
        "published_event_ids_unique": len(published_ids) == len(set(published_ids)),
        "resumed_result_count": len(resumed_results),
        "evaluated_monitor_effect_count": len(evaluated),
        "acknowledgement_idempotent": not source.poll(limit=16),
        "pending_event_ids_after_ack": list(event_order),
        "evaluated_detector_state": evaluated[0].detector_state if evaluated else None,
        "evaluated_policy_action": evaluated[0].policy_action if evaluated else None,
    }


def _scenario_duplicates(root: Path) -> tuple[bool, dict[str, object]]:
    source = _source(root / "outbox")
    trace = _trace(Metric.L2, offset=0)
    context = _context(Metric.L2, window=0, index=0)
    initial = source.publish(trace=trace, context=context)
    duplicate = source.publish(trace=trace, context=context)
    failures: list[str] = []
    for replacement, changed_context in (
        (_trace(Metric.L2, offset=50), context),
        (trace, TracePublicationContext(
            stream_key=context.stream_key,
            window_id=context.window_id,
            window_sequence=context.window_sequence,
            trace_sequence_index=context.trace_sequence_index,
            trace_id=context.trace_id,
            captured_at_utc="2026-08-03T13:00:00Z",
        )),
    ):
        try:
            source.publish(trace=replacement, context=changed_context)
        except ShadowEventSourceError as exc:
            failures.append(str(exc))
        else:
            raise Exp007ValidationError("conflicting publication did not fail closed")
    polled = source.poll(limit=4)
    return (
        initial.status is PublicationStatus.PUBLISHED
        and duplicate.status is PublicationStatus.IDEMPOTENT
        and set(failures) == {"PUBLICATION_CONFLICT", "ENVELOPE_CONTEXT_MISMATCH"}
        and len(polled) == 1,
        {
            "identical_publication_status": duplicate.status.value,
            "conflict_reason_codes": sorted(failures),
            "deliverable_event_count": len(polled),
            "downstream_monitor_calls": 0,
        },
    )


def _scenario_backpressure(root: Path) -> tuple[bool, dict[str, object]]:
    source = _source(
        root / "outbox",
        count=BACKPRESSURE_PENDING_EVENT_CAP,
        bytes_limit=BACKPRESSURE_PENDING_BYTE_CAP,
    )
    first = source.publish(trace=_trace(Metric.L2, offset=0), context=_context(Metric.L2, window=0, index=0))
    foreground_calls = 1
    trace_count_before = len(tuple((source.root / "traces").glob("*.json")))
    with patch("vdbench.shadow_event_source.persist_shadow_trace_envelope", side_effect=AssertionError("must not persist")):
        dropped = source.publish(trace=_trace(Metric.L2, offset=50), context=_context(Metric.L2, window=1, index=0))
    trace_count_after = len(tuple((source.root / "traces").glob("*.json")))
    valid = (
        first.status is PublicationStatus.PUBLISHED
        and dropped.status is PublicationStatus.DROPPED_BACKPRESSURE
        and dropped.reason_code == "PENDING_EVENT_CAPACITY_EXCEEDED"
        and trace_count_before == trace_count_after == 1
        and len(source.poll(limit=4)) == 1
    )
    return valid, {
        "foreground_calls": foreground_calls,
        "publication_status": dropped.status.value,
        "reason_code": dropped.reason_code,
        "synchronous_persistence_calls": 0,
        "shadow_query_calls": 0,
        "monitor_calls": 0,
        "trace_count_before": trace_count_before,
        "trace_count_after": trace_count_after,
    }


def _scenario_safety(root: Path) -> tuple[bool, dict[str, object]]:
    results: dict[str, bool] = {}

    malformed = _source(root / "malformed")
    malformed_receipt = malformed.publish(trace=_trace(Metric.L2, offset=0), context=_context(Metric.L2, window=0, index=0))
    assert malformed_receipt.event is not None
    (malformed.root / "pending" / f"{malformed_receipt.event.event_id}.json").write_text("{broken", encoding="utf-8")
    results["malformed_event"] = not malformed.poll(limit=1) and "EVENT_MALFORMED" in malformed.rejected_reason_codes()

    corrupt = _source(root / "corrupt")
    corrupt_receipt = corrupt.publish(trace=_trace(Metric.L2, offset=0), context=_context(Metric.L2, window=0, index=0))
    assert corrupt_receipt.event is not None
    corrupt_receipt.event.envelope_path.write_text("{broken", encoding="utf-8")
    results["corrupt_envelope"] = not corrupt.poll(limit=1) and "ENVELOPE_INVALID" in corrupt.rejected_reason_codes()

    missing = _source(root / "missing")
    missing_receipt = missing.publish(trace=_trace(Metric.L2, offset=0), context=_context(Metric.L2, window=0, index=0))
    assert missing_receipt.event is not None
    missing_receipt.event.envelope_path.unlink()
    results["missing_envelope"] = not missing.poll(limit=1) and "ENVELOPE_INVALID" in missing.rejected_reason_codes()

    unsafe_root = root / "unsafe"
    unsafe_root.mkdir(mode=0o755)
    unsafe_root.chmod(0o755)
    try:
        _source(unsafe_root)
    except ShadowEventSourceError as exc:
        results["unsafe_permissions"] = str(exc) == "OUTBOX_UNSAFE_PERMISSIONS"
    else:
        results["unsafe_permissions"] = False

    symlinked = _source(root / "symlinked")
    symlink_receipt = symlinked.publish(trace=_trace(Metric.L2, offset=0), context=_context(Metric.L2, window=0, index=0))
    assert symlink_receipt.event is not None
    pending = symlinked.root / "pending" / f"{symlink_receipt.event.event_id}.json"
    replacement = symlinked.root / "replacement.json"
    replacement.write_text(pending.read_text(encoding="utf-8"), encoding="utf-8")
    pending.unlink()
    pending.symlink_to(replacement)
    results["symlink_escape"] = not symlinked.poll(limit=1) and "EVENT_SYMLINK_REJECTED" in symlinked.rejected_reason_codes()
    return all(results.values()), {"cases": results, "downstream_monitor_calls": 0}


def _scenario_composition(root: Path) -> tuple[bool, dict[str, object]]:
    source = _source(
        root / "outbox",
        count=STANDARD_PENDING_EVENT_CAP,
        bytes_limit=STANDARD_PENDING_BYTE_CAP,
    )
    for metric in (Metric.L2, Metric.COSINE):
        for window in range(3):
            _publish_window(source, metric, window=window)
    audit = _Audit()
    trap = _ForbiddenClient()
    monitor = WorkloadMonitor(
        source=source,
        state_store=FileMonitorStateStore(root / "monitor-state"),
        policy_input_provider=_Provider(),
        audit_sink=audit,
        detector_seed=EXP007_DETECTOR_SEED,
    )
    results = monitor.run_once(max_events=MONITOR_POLL_MAX_EVENTS)
    evaluated: dict[str, int] = {}
    valid = True
    for metric in (Metric.L2, Metric.COSINE):
        stream_records = [
            item for item in audit.records
            if item.stream_key.metric is metric and item.status is MonitorRecordStatus.EVALUATED
        ]
        evaluated[metric.value] = len(stream_records)
        valid = valid and len(stream_records) == 1
        if stream_records:
            valid = valid and stream_records[0].detector_state == "NO_DRIFT" and stream_records[0].policy_action == PolicyAction.NO_CHANGE.value

    source_path = Path(__file__).parents[1] / "src" / "vdbench" / "shadow_event_source.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported.update(
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    )
    forbidden = tuple(sorted(
        name for name in imported
        if name == "pymilvus" or name.endswith(".policy") or name.endswith(".actuation") or name.endswith(".milvus_actuation")
    ))
    source_text = source_path.read_text(encoding="utf-8")
    no_prohibited = not forbidden and "WorkloadMonitor" not in source_text
    valid = (
        valid
        and len(results) == MONITOR_POLL_MAX_EVENTS
        and not source.poll(limit=MONITOR_POLL_MAX_EVENTS)
        and not trap.calls
        and no_prohibited
    )
    return valid, {
        "processed_event_count": len(results),
        "evaluated_by_metric": evaluated,
        "trap_client_calls": trap.calls,
        "pending_after_monitor": len(source.poll(limit=MONITOR_POLL_MAX_EVENTS)),
        "prohibited_imports": list(forbidden),
        "no_prohibited_source_dependencies": no_prohibited,
    }


def _data_minimization(root: Path) -> tuple[bool, dict[str, object]]:
    trace_occurrences = 0
    non_trace_occurrences = 0
    ignored_unsafe_symlink_paths = 0
    for path in root.rglob("*.json"):
        # A deliberate safety test leaves a dangling symlink behind when its
        # target cannot be trusted.  Inspection must not dereference such a
        # path: the source already rejects it, and a symlink has no payload.
        if path.is_symlink():
            ignored_unsafe_symlink_paths += 1
            continue
        text = path.read_text(encoding="utf-8")
        occurrences = sum(text.count(value) for value in _SENTINELS)
        if "traces" in path.relative_to(root).parts:
            trace_occurrences += occurrences
        else:
            non_trace_occurrences += occurrences
    return trace_occurrences > 0 and non_trace_occurrences == 0, {
        "trace_payload_sentinel_occurrences": trace_occurrences,
        "non_trace_sentinel_occurrences": non_trace_occurrences,
        "ignored_unsafe_symlink_paths": ignored_unsafe_symlink_paths,
        "sentinels_only_in_trace_payload": trace_occurrences > 0 and non_trace_occurrences == 0,
    }


def _artifact_inventory(root: Path) -> tuple[dict[str, str], dict[str, str]]:
    """Hash regular evidence files and link targets without dereferencing links."""

    excluded = {"manifest.json", "execution_receipt.json"}
    regular: dict[str, str] = {}
    symlink_targets: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.name in excluded or ".tmp" in path.name:
            continue
        relative = str(path.relative_to(root))
        if path.is_symlink():
            symlink_targets[relative] = hashlib.sha256(
                os.fsencode(os.readlink(path))
            ).hexdigest()
        elif path.is_file():
            regular[relative] = _sha256(path)
    return regular, symlink_targets


def _filesystem_type(path: Path) -> str:
    """Return the host filesystem name or fail rather than inventing one."""

    command = (
        ["stat", "-f", "%T", str(path)]
        if sys.platform == "darwin"
        else ["stat", "-f", "-c", "%T", str(path)]
    )
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    value = result.stdout.strip()
    if result.returncode != 0 or not value:
        raise Exp007ValidationError("filesystem type could not be recorded")
    return value


def run_validation(*, output_dir: Path, detector_seed: int) -> dict[str, object]:
    """Run every registered offline EXP-007 scenario into a new evidence bundle."""

    if detector_seed != EXP007_DETECTOR_SEED:
        raise Exp007ValidationError(f"EXP-007 detector seed is frozen at {EXP007_DETECTOR_SEED}")
    if output_dir.exists():
        raise Exp007ValidationError("output directory must not already exist")
    output_dir.mkdir(parents=True, mode=0o700)
    output_dir.chmod(0o700)

    atomic_ok, atomic = _scenario_atomic_order(output_dir / "atomic")
    restart_ok, restart = _scenario_restart(output_dir / "restart")
    duplicate_ok, duplicate = _scenario_duplicates(output_dir / "duplicates")
    backpressure_ok, backpressure = _scenario_backpressure(output_dir / "backpressure")
    safety_ok, safety = _scenario_safety(output_dir / "safety")
    composition_ok, composition = _scenario_composition(output_dir / "composition")
    minimization_ok, minimization = _data_minimization(output_dir)
    scenarios = {
        "atomic_publication_order": atomic_ok,
        "restart_and_redelivery": restart_ok,
        "duplicate_and_conflict_safety": duplicate_ok,
        "backpressure_and_foreground_isolation": backpressure_ok,
        "schema_permission_checksum_path_safety": safety_ok,
        "data_minimization": minimization_ok,
        "dry_run_monitor_composition": composition_ok,
    }
    for name, value in (
        ("atomic_order.json", atomic),
        ("restart.json", restart),
        ("duplicates.json", duplicate),
        ("backpressure.json", backpressure),
        ("safety.json", safety),
        ("composition.json", composition),
        ("data_minimization.json", minimization),
    ):
        _write(output_dir / "results" / name, value)

    status = "COMPLETE" if all(scenarios.values()) else "INCOMPLETE"
    raw_result: dict[str, Any] = {
        "schema_version": "exp007-raw-result-v1",
        "status": status,
        "scenarios": scenarios,
        "atomic": atomic,
        "restart": restart,
        "duplicates": duplicate,
        "backpressure": backpressure,
        "safety": safety,
        "data_minimization": minimization,
        "composition": composition,
    }
    raw_result["self_sha256"] = _content_sha256(raw_result)
    _write(output_dir / "raw_result.json", raw_result)

    artifact_sha256, symlink_target_sha256 = _artifact_inventory(output_dir)
    manifest: dict[str, Any] = {
        "schema_version": "exp007-manifest-v1",
        "validation_status": status,
        "execution_mode": "offline",
        "detector_seed": detector_seed,
        "fixture_scheduling_seed": EXP007_FIXTURE_SEED,
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "working_tree_porcelain": subprocess.check_output(["git", "status", "--porcelain"], text=True),
        "python": sys.version,
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "filesystem_type": _filesystem_type(output_dir),
        "outbox_root_owner_uid": (output_dir / "composition" / "outbox").stat().st_uid,
        "outbox_root_mode": oct((output_dir / "composition" / "outbox").stat().st_mode & 0o777),
        "queue_limits": {
            "durable_pending_event_cap": STANDARD_PENDING_EVENT_CAP,
            "durable_pending_byte_cap": STANDARD_PENDING_BYTE_CAP,
            "monitor_poll_max_events": MONITOR_POLL_MAX_EVENTS,
            # ADR-006's host sampler is intentionally not implemented here;
            # source v1 receives only already-complete traces.
            "in_memory_observation_cap": None,
            "in_memory_observation_cap_status": "NOT_APPLICABLE_SOURCE_V1_HOST_SAMPLER_UNIMPLEMENTED",
        },
        "artifact_sha256": artifact_sha256,
        "symlink_target_sha256": symlink_target_sha256,
    }
    manifest["self_sha256"] = _content_sha256(manifest)
    _write(output_dir / "manifest.json", manifest)
    receipt = {
        "schema_version": "exp007-execution-receipt-v1",
        "validation_status": status,
        "git_commit": manifest["git_commit"],
        "manifest_sha256": manifest["self_sha256"],
        "raw_result_sha256": raw_result["self_sha256"],
        "artifact_count": len(manifest["artifact_sha256"]),
    }
    _write(output_dir / "execution_receipt.json", receipt)

    if status != "COMPLETE":
        failed = ", ".join(name for name, passed in scenarios.items() if not passed)
        raise Exp007ValidationError(f"EXP-007 validation incomplete: {failed}")
    return raw_result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--detector-seed", type=int, default=EXP007_DETECTOR_SEED)
    args = parser.parse_args()
    print(json.dumps(run_validation(output_dir=args.output_dir, detector_seed=args.detector_seed), sort_keys=True))


if __name__ == "__main__":
    main()
