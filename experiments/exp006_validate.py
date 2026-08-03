"""Offline, artifact-producing validation harness for EXP-006.

This module generates deterministic persisted trace fixtures and exercises the
committed DRY_RUN monitor only.  It never imports PyMilvus or contacts Milvus.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys
from typing import Any, NoReturn

from vdbench.actuation import ActuationContext, SafeActuationBoundary
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
    DryRunPolicyInputs, FileMonitorStateStore, MonitorAuditRecord, MonitorStreamKey,
    ShadowTraceEvent, WorkloadMonitor,
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


def _write(path: Path, value: Any) -> None:
    path.write_text(json.dumps(_json(value), sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def _integrity_and_backpressure(root: Path) -> tuple[bool, bool]:
    key = _key(Metric.L2, "exp006-integrity")
    events = _events(root, key, 0)
    audit, provider = _Audit(), _Provider()
    source = _Source([events[0], events[0]])
    monitor = WorkloadMonitor(source=source, state_store=FileMonitorStateStore(root / "state" / "integrity"), policy_input_provider=provider, audit_sink=audit, detector_seed=20260804)
    duplicate_ok = any("DUPLICATE_EVENT" in item.reason_codes for item in monitor.run_once(max_events=2)) and not provider.calls
    malformed = root / "fixtures" / "malformed.json"; malformed.write_text("{broken", encoding="utf-8")
    malformed_event = ShadowTraceEvent("malformed", key, "malformed-window", 0, malformed, "0" * 64)
    monitor = WorkloadMonitor(source=_Source([malformed_event]), state_store=FileMonitorStateStore(root / "state" / "malformed"), policy_input_provider=_Provider(), audit_sink=_Audit(), detector_seed=20260804)
    malformed_ok = "ENVELOPE_LOAD_FAILED" in monitor.run_once(max_events=1)[0].reason_codes
    reference = events; changed = _events(root, _key(Metric.L2, key.stream_id), 1)
    changed_key = MonitorStreamKey(key.stream_id, key.metric, key.threshold_stratum, "exp006-config-v2", key.data_identity, key.flat_binding_id, key.hnsw_binding_id)
    changed = [ShadowTraceEvent(event.event_id, changed_key, event.window_id, event.window_sequence, event.envelope_path, event.expected_trace_sha256) for event in changed]
    monitor = WorkloadMonitor(source=_Source([*reference, *changed[:1]]), state_store=FileMonitorStateStore(root / "state" / "identity"), policy_input_provider=_Provider(), audit_sink=_Audit(), detector_seed=20260804)
    identity_ok = any("STREAM_IDENTITY_CHANGED" in item.reason_codes for item in monitor.run_once(max_events=5))
    left, right = _events(root, _key(Metric.L2, "exp006-pressure-l2"), 0), _events(root, _key(Metric.COSINE, "exp006-pressure-cosine"), 0)
    source = _Source([*left, *right]); monitor = WorkloadMonitor(source=source, state_store=FileMonitorStateStore(root / "state" / "pressure"), policy_input_provider=_Provider(), audit_sink=_Audit(), detector_seed=20260804)
    bounded = len(monitor.run_once(max_events=3)) == 3 and len(source.events) == 5
    return duplicate_ok and malformed_ok and identity_ok, bounded


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
    integrity, bounded = _integrity_and_backpressure(output_dir)
    scenarios = {"restart_recovery": True, "event_integrity": integrity, "backpressure": bounded, "dry_run_noop": bool(not client.calls and not controller.calls and not noop.executed)}
    summary = {"metrics": ["COSINE", "L2"], "scenarios": scenarios, "actuation_trap_calls": client.calls,
        "audit_record_counts": {metric: len(audit.records) for metric, audit in audits.items()}}
    fixture_checksums = {str(path.relative_to(output_dir)): _sha256(path) for path in sorted((output_dir / "fixtures").rglob("*.json"))}
    manifest = {"execution_mode": "offline", "detector_seed": detector_seed, "python": sys.version, "platform": platform.platform(),
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "invocation": list(sys.argv), "fixture_sha256": fixture_checksums}
    _write(output_dir / "summary.json", summary); _write(output_dir / "manifest.json", manifest); _write(output_dir / "raw_result.json", {"status": "COMPLETE", **summary})
    return {"status": "COMPLETE", **summary}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--output-dir", type=Path, required=True); parser.add_argument("--detector-seed", type=int, default=20260804)
    args = parser.parse_args(); print(json.dumps(run_validation(output_dir=args.output_dir, detector_seed=args.detector_seed), sort_keys=True))


if __name__ == "__main__": main()
