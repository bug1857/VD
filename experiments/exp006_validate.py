"""Offline, artifact-producing validation harness for EXP-006.

This module generates deterministic persisted trace fixtures and exercises the
committed DRY_RUN monitor only.  It never imports PyMilvus or contacts Milvus.
"""

from __future__ import annotations

import argparse
import ast
from dataclasses import asdict, replace
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

from vdbench.actuation import ActuationContext, ActuationResult, SafeActuationBoundary
from vdbench.config import IndexTrack, Metric
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ShadowAuditStageEvidence, ShadowAuditTrace, ShadowIdentityEvidence, ShadowQueryAuditTrace,
)
from vdbench.oracle import OracleHit, OracleResult
from vdbench.policy import PolicyAction, PreActionSafety, QualificationResult
from vdbench.shadow_artifacts import persist_shadow_trace_envelope
from vdbench.shadow_window import PersistedShadowTraceEnvelope, hash_shadow_audit_trace
from vdbench.workload_monitor import (
    DryRunPolicyInputs, FileMonitorStateStore, MonitorAuditRecord, MonitorCycleResult,
    MonitorStreamKey, ShadowTraceEvent, WorkloadMonitor,
)


class ValidationError(RuntimeError):
    """Raised when an EXP-006 contract assertion is not met."""


class _Source:
    def __init__(self, events: list[ShadowTraceEvent]) -> None:
        self.events, self.acknowledged = list(events), []

    def poll(self, *, limit: int) -> tuple[ShadowTraceEvent, ...]:
        result = tuple(self.events[:limit]); del self.events[:limit]; return result

    def acknowledge(self, event_ids: tuple[str, ...]) -> None:
        self.acknowledged.extend(event_ids)


class _Audit:
    def __init__(self) -> None: self.records: list[MonitorAuditRecord] = []
    def contains(self, record_id: str) -> bool: return any(item.record_id == record_id for item in self.records)
    def append(self, record: MonitorAuditRecord) -> None:
        if self.contains(record.record_id): raise ValidationError(f"duplicate audit {record.record_id}")
        self.records.append(record)


class _Provider:
    def __init__(self) -> None: self.calls = []
    def resolve(self, *, decision, provenance):
        self.calls.append(decision)
        return DryRunPolicyInputs(
            current_ef=400, response_estimates={},
            pre_action=PreActionSafety(metric=provenance.metric, threshold_stratum=provenance.threshold_stratum,
                configuration_identity=provenance.configuration_identity, index_identity=provenance.hnsw_binding_id,
                flat_index_identity=provenance.flat_binding_id, data_identity=provenance.data_identity,
                response_model_provenance="EXP006_DETERMINISTIC_FIXTURE"),
            last_known_good=QualificationResult(False, None, ("EXP006_OFFLINE",)),
            audit_id=f"exp006:{provenance.current_window_id}")


class _TrapClient:
    def __init__(self) -> None: self.calls: list[str] = []
    def __getattr__(self, name: str) -> NoReturn:
        self.calls.append(name); raise AssertionError(f"forbidden client call: {name}")


class _BoundaryAudit:
    def __init__(self) -> None: self.records = {}
    def contains(self, audit_id: str) -> bool: return audit_id in self.records
    def append(self, record) -> None: self.records[record.audit_id] = record


class _Controller:
    def __init__(self) -> None: self.calls = []
    def disable_automatic_actions(self, *, audit_id: str, reason: str) -> None: self.calls.append((audit_id, reason))


def _key(metric: Metric, stream: str) -> MonitorStreamKey:
    return MonitorStreamKey(stream, metric, "target-075", "exp006-config-v1", "exp006-data-v1",
        f"{metric.value.lower()}-flat-v1", f"{metric.value.lower()}-hnsw-v1")


def _identity(metric: Metric, track: IndexTrack) -> ShadowIdentityEvidence:
    description: dict[str, object] = {"index_type": track.value, "metric_type": metric.value}
    if track is IndexTrack.HNSW: description.update({"M": "16", "efConstruction": "200"})
    identity = CollectionIdentity(f"exp006_{metric.value.lower()}_{track.value.lower()}", metric.value, track.value, description)
    stage = ShadowAuditStageEvidence(f"{track.value}_IDENTITY", success=True)
    return ShadowIdentityEvidence(track, f"{metric.value.lower()}-{track.value.lower()}-v1", identity, identity, True, True, stage, stage)


def _query(query_id: int, metric: Metric) -> ShadowQueryAuditTrace:
    score, radius, filt = (1.0, 2.0, 0.0) if metric is Metric.L2 else (0.5, 0.0, 1.0)
    oracle = OracleResult((OracleHit(query_id, score),), full_count=1, capped=False)
    hit = SearchHit(query_id, score)
    return ShadowQueryAuditTrace(query_id, (float(query_id + 1), 1.0), radius, filt, 100, oracle, 1,
        (hit,), (hit,), 1.0, (ShadowAuditStageEvidence("ORACLE", success=True),
        ShadowAuditStageEvidence("FLAT", success=True, oracle_agreement=True), ShadowAuditStageEvidence("SENTINEL_HNSW", success=True)))


def _events(root: Path, key: MonitorStreamKey, window: int) -> list[ShadowTraceEvent]:
    result = []
    for index in range(4):
        trace = ShadowAuditTrace(key.metric, key.threshold_stratum, 400, 200, 100, key.configuration_identity,
            key.data_identity, _identity(key.metric, IndexTrack.FLAT), _identity(key.metric, IndexTrack.HNSW),
            tuple(_query(query_id, key.metric) for query_id in range(index * 50, (index + 1) * 50)), True)
        envelope = PersistedShadowTraceEnvelope(f"{key.stream_id}-{window}-{index}", f"2026-08-03T12:{window:02d}:{index:02d}Z",
            index, 50, hash_shadow_audit_trace(trace), trace)
        path = root / "fixtures" / key.stream_id / f"{window}-{index}.json"; path.parent.mkdir(parents=True, exist_ok=True)
        persist_shadow_trace_envelope(path, envelope)
        result.append(ShadowTraceEvent(f"{key.stream_id}:{window}:{index}", key, f"{key.stream_id}:window:{window}", window, path, envelope.expected_trace_sha256))
    return result


def _json(value: Any) -> Any:
    if hasattr(value, "value"): return value.value
    if isinstance(value, Path): return str(value)
    if isinstance(value, tuple): return [_json(item) for item in value]
    if isinstance(value, list): return [_json(item) for item in value]
    if isinstance(value, dict): return {str(key): _json(item) for key, item in value.items()}
    if hasattr(value, "__dataclass_fields__"): return _json(asdict(value))
    return value


def _write_bytes(path: Path, payload: bytes) -> None:
    """Publish one new evidence artifact only after file and directory fsyncs."""

    target = Path(path)
    if target.exists():
        raise FileExistsError(f"refusing to overwrite immutable artifact: {target}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, target)
        directory_descriptor = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _write(path: Path, value: Any) -> None:
    _write_bytes(
        path,
        (json.dumps(_json(value), sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _dry_run_no_actuation_proof(
    *,
    client: _TrapClient,
    sink: _BoundaryAudit,
    controller: _Controller,
    noop: ActuationResult,
) -> dict[str, object]:
    """Record the structural and exercised proof for EXP-006 H4."""

    source_path = Path(__file__).parents[1] / "src" / "vdbench" / "workload_monitor.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    imported_modules = {
        module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for module in ((node.module or "").split(".")[0],)
    }
    imported_modules.update(
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    monitor_references_canary_enabled = any(
        (isinstance(node, ast.Name) and node.id == "CANARY_ENABLED")
        or (isinstance(node, ast.Constant) and node.value == "CANARY_ENABLED")
        for node in ast.walk(tree)
    )
    return {
        "monitor_imports_actuation": "actuation" in imported_modules,
        "monitor_imports_pymilvus": "pymilvus" in imported_modules,
        "monitor_references_canary_enabled": monitor_references_canary_enabled,
        "boundary_executed": noop.executed,
        "boundary_audit_record_count": len(sink.records),
        "trap_client_calls": client.calls,
        "controller_calls": controller.calls,
    }


def _rejection_case(
    *,
    expected_reason_code: str,
    results: tuple[MonitorCycleResult, ...],
    audit: _Audit,
    provider: _Provider,
) -> dict[str, object]:
    """Record fail-closed evidence for one named EXP-006 integrity case."""

    matching_results = [
        result
        for result in results
        if expected_reason_code in result.reason_codes
    ]
    matching_audits = [
        record
        for record in audit.records
        if expected_reason_code in record.reason_codes
    ]
    return {
        "expected_reason_code": expected_reason_code,
        "result_reason_codes": [list(result.reason_codes) for result in results],
        "result_accepted": [result.accepted for result in results],
        "policy_input_calls": len(provider.calls),
        "audit_records": _json(audit.records),
        "passed": bool(matching_results)
        and all(not result.accepted for result in matching_results)
        and not provider.calls
        and bool(matching_audits),
    }


def _run_stream(root: Path, metric: Metric, *, restart: bool) -> tuple[object, _Audit]:
    key = _key(metric, f"exp006-{metric.value.lower()}")
    events = [*_events(root, key, 0), *_events(root, key, 1), *_events(root, key, 2)]
    store = FileMonitorStateStore(root / "state" / key.stream_id); audit = _Audit(); provider = _Provider()
    chunks = (events[:6], events[6:8], events[8:]) if restart else (events,)
    results = []
    for chunk in chunks:
        monitor = WorkloadMonitor(source=_Source(chunk), state_store=store, policy_input_provider=provider, audit_sink=audit, detector_seed=20260804)
        results.extend(monitor.run_once(max_events=len(chunk)))
    policy = next(item.policy_decision for item in results if item.policy_decision is not None)
    if policy.action is not PolicyAction.NO_CHANGE: raise ValidationError("stationary policy was not NO_CHANGE")
    return policy, audit


def _integrity_and_backpressure(root: Path) -> tuple[dict[str, dict[str, object]], bool]:
    """Exercise every EXP-006 H2 case and return immutable case evidence."""

    key = _key(Metric.L2, "exp006-integrity")
    case_root = root / "integrity_cases"
    cases: dict[str, dict[str, object]] = {}

    duplicate_events = _events(case_root / "duplicate_event", key, 0)
    duplicate_audit, duplicate_provider = _Audit(), _Provider()
    duplicate_monitor = WorkloadMonitor(
        source=_Source([duplicate_events[0], duplicate_events[0]]),
        state_store=FileMonitorStateStore(root / "state" / "duplicate_event"),
        policy_input_provider=duplicate_provider,
        audit_sink=duplicate_audit,
        detector_seed=20260804,
    )
    cases["duplicate_event"] = _rejection_case(
        expected_reason_code="DUPLICATE_EVENT",
        results=duplicate_monitor.run_once(max_events=2),
        audit=duplicate_audit,
        provider=duplicate_provider,
    )

    envelope_events = _events(case_root / "duplicate_envelope", key, 0)
    conflicting_envelope = replace(
        envelope_events[1],
        event_id="exp006:duplicate-envelope-reference",
        envelope_path=envelope_events[0].envelope_path,
        expected_trace_sha256=envelope_events[0].expected_trace_sha256,
    )
    envelope_audit, envelope_provider = _Audit(), _Provider()
    envelope_monitor = WorkloadMonitor(
        source=_Source([envelope_events[0], conflicting_envelope]),
        state_store=FileMonitorStateStore(root / "state" / "duplicate_envelope"),
        policy_input_provider=envelope_provider,
        audit_sink=envelope_audit,
        detector_seed=20260804,
    )
    cases["duplicate_envelope_reference"] = _rejection_case(
        expected_reason_code="DUPLICATE_ENVELOPE_REFERENCE",
        results=envelope_monitor.run_once(max_events=2),
        audit=envelope_audit,
        provider=envelope_provider,
    )

    replay_events = _events(case_root / "replay_after_restart", key, 0)
    replay_audit, replay_provider = _Audit(), _Provider()
    replay_store = FileMonitorStateStore(root / "state" / "replay_after_restart")
    WorkloadMonitor(
        source=_Source([replay_events[0]]),
        state_store=replay_store,
        policy_input_provider=replay_provider,
        audit_sink=replay_audit,
        detector_seed=20260804,
    ).run_once(max_events=1)
    replay_monitor = WorkloadMonitor(
        source=_Source([replay_events[0]]),
        state_store=replay_store,
        policy_input_provider=replay_provider,
        audit_sink=replay_audit,
        detector_seed=20260804,
    )
    cases["replay_after_restart"] = _rejection_case(
        expected_reason_code="DUPLICATE_EVENT",
        results=replay_monitor.run_once(max_events=1),
        audit=replay_audit,
        provider=replay_provider,
    )

    malformed_root = case_root / "malformed"
    baseline = _events(malformed_root, key, 0)[0]
    malformed_documents: dict[str, str | dict[str, object]] = {
        "malformed_json": "{broken",
        "invalid_schema": {"schema_version": "persisted-shadow-trace-envelope-v1"},
        "invalid_timestamp": {"captured_at_utc": "not-a-timestamp"},
        "checksum_mismatch": {"expected_trace_sha256": "0" * 64},
        "count_mismatch": {"declared_observation_count": 49},
    }
    assembly_expected_reasons = {
        "invalid_timestamp": "TIMESTAMP_INVALID",
        "count_mismatch": "DECLARED_OBSERVATION_COUNT_INVALID",
    }
    for name, mutation in malformed_documents.items():
        case_events = [baseline]
        document = json.loads(baseline.envelope_path.read_text(encoding="utf-8"))
        if name in assembly_expected_reasons:
            case_events = _events(malformed_root / name, key, 0)
            document = json.loads(case_events[0].envelope_path.read_text(encoding="utf-8"))
        path = malformed_root / "fixtures" / f"{name}.json"
        if isinstance(mutation, str):
            _write_bytes(path, mutation.encode("utf-8"))
        else:
            if name == "invalid_schema":
                document = mutation
            else:
                document.update(mutation)
            _write(path, document)
        audit, provider = _Audit(), _Provider()
        event = replace(
            case_events[0],
            event_id=f"exp006:{name}",
            envelope_path=path,
        )
        source_events = [event, *case_events[1:]]
        monitor = WorkloadMonitor(
            source=_Source(source_events),
            state_store=FileMonitorStateStore(root / "state" / name),
            policy_input_provider=provider,
            audit_sink=audit,
            detector_seed=20260804,
        )
        cases[name] = _rejection_case(
            expected_reason_code=assembly_expected_reasons.get(
                name, "ENVELOPE_LOAD_FAILED"
            ),
            results=monitor.run_once(max_events=len(source_events)),
            audit=audit,
            provider=provider,
        )

    reference = _events(case_root / "identity", key, 0)
    changed = _events(case_root / "identity", key, 1)
    changed_key = MonitorStreamKey(
        key.stream_id,
        key.metric,
        key.threshold_stratum,
        "exp006-config-v2",
        key.data_identity,
        key.flat_binding_id,
        key.hnsw_binding_id,
    )
    changed_event = replace(changed[0], stream_key=changed_key)
    identity_audit, identity_provider = _Audit(), _Provider()
    identity_monitor = WorkloadMonitor(
        source=_Source([*reference, changed_event]),
        state_store=FileMonitorStateStore(root / "state" / "identity"),
        policy_input_provider=identity_provider,
        audit_sink=identity_audit,
        detector_seed=20260804,
    )
    cases["identity_change"] = _rejection_case(
        expected_reason_code="STREAM_IDENTITY_CHANGED",
        results=identity_monitor.run_once(max_events=5),
        audit=identity_audit,
        provider=identity_provider,
    )

    left, right = _events(root / "backpressure", _key(Metric.L2, "exp006-pressure-l2"), 0), _events(root / "backpressure", _key(Metric.COSINE, "exp006-pressure-cosine"), 0)
    source = _Source([*left, *right]); monitor = WorkloadMonitor(source=source, state_store=FileMonitorStateStore(root / "state" / "pressure"), policy_input_provider=_Provider(), audit_sink=_Audit(), detector_seed=20260804)
    bounded = len(monitor.run_once(max_events=3)) == 3 and len(source.events) == 5
    return cases, bounded


def run_validation(*, output_dir: Path, detector_seed: int) -> dict[str, object]:
    if detector_seed != 20260804: raise ValidationError("EXP006 detector seed is frozen at 20260804")
    if output_dir.exists(): raise ValidationError("output directory must not already exist")
    output_dir.mkdir(parents=True)
    policies, audits = {}, {}
    for metric in (Metric.L2, Metric.COSINE):
        policy, audit = _run_stream(output_dir / "resumed", metric, restart=True)
        baseline_policy, baseline_audit = _run_stream(output_dir / "uninterrupted", metric, restart=False)
        if policy != baseline_policy or audit.records != baseline_audit.records:
            raise ValidationError(f"restart replay mismatch for {metric.value}")
        policies[metric.value], audits[metric.value] = policy, audit
        _write(output_dir / f"audit-{metric.value.lower()}.json", audit.records)
    client, sink, controller = _TrapClient(), _BoundaryAudit(), _Controller()
    policy = policies[Metric.L2.value]
    noop = SafeActuationBoundary(client, sink, controller).execute(policy, ActuationContext(Metric.L2, "target-075", "exp006_l2_hnsw", "exp006-config-v1", "l2-hnsw-v1", "l2-flat-v1", "exp006-data-v1", tuple(range(50)), QualificationResult(False, None, ("EXP006",)), "2026-08-03T12:00:00Z"))
    dry_run_no_actuation_proof = _dry_run_no_actuation_proof(
        client=client,
        sink=sink,
        controller=controller,
        noop=noop,
    )
    integrity_cases, bounded = _integrity_and_backpressure(output_dir)
    integrity = all(case["passed"] for case in integrity_cases.values())
    scenarios = {"restart_recovery": True, "event_integrity": integrity, "backpressure": bounded, "dry_run_noop": bool(not client.calls and not controller.calls and not noop.executed)}
    summary = {"metrics": ["COSINE", "L2"], "scenarios": scenarios, "actuation_trap_calls": client.calls,
        "audit_record_counts": {metric: len(audit.records) for metric, audit in audits.items()},
        "event_integrity_case_status": {name: case["passed"] for name, case in integrity_cases.items()}}
    _write(output_dir / "integrity_case_results.json", integrity_cases)
    fixture_paths = sorted(
        path
        for path in output_dir.rglob("*.json")
        if "fixtures" in path.relative_to(output_dir).parts
    )
    state_paths = sorted(
        path
        for path in output_dir.rglob("*.json")
        if "state" in path.relative_to(output_dir).parts
    )
    audit_paths = [
        output_dir / "audit-cosine.json",
        output_dir / "audit-l2.json",
        output_dir / "integrity_case_results.json",
    ]
    fixture_checksums = {str(path.relative_to(output_dir)): _sha256(path) for path in fixture_paths}
    state_checksums = {str(path.relative_to(output_dir)): _sha256(path) for path in state_paths}
    audit_checksums = {str(path.relative_to(output_dir)): _sha256(path) for path in audit_paths}
    status = "COMPLETE" if all(scenarios.values()) else "INCOMPLETE"
    manifest = {"execution_mode": "offline", "detector_seed": detector_seed, "python": sys.version, "platform": platform.platform(),
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "invocation": list(sys.argv), "artifact_directory": str(output_dir.resolve()),
        "fixture_sha256": fixture_checksums, "state_sha256": state_checksums,
        "audit_sha256": audit_checksums, "validation_status": status}
    raw_result = {
        "status": status,
        **summary,
        "event_integrity_cases": integrity_cases,
        "dry_run_no_actuation_proof": dry_run_no_actuation_proof,
    }
    _write(output_dir / "summary.json", summary)
    _write(output_dir / "manifest.json", manifest)
    _write(output_dir / "raw_result.json", raw_result)
    if status != "COMPLETE":
        failed_scenarios = ", ".join(
            name for name, passed in scenarios.items() if not passed
        )
        raise ValidationError(
            f"EXP-006 validation incomplete: {failed_scenarios}"
        )
    return raw_result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--output-dir", type=Path, required=True); parser.add_argument("--detector-seed", type=int, default=20260804)
    args = parser.parse_args(); print(json.dumps(run_validation(output_dir=args.output_dir, detector_seed=args.detector_seed), sort_keys=True))


if __name__ == "__main__": main()
