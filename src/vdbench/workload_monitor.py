"""ADR-005 offline-safe workload monitor and DRY_RUN orchestration boundary.

Purpose:
    Consume immutable persisted shadow-trace events, assemble ordered windows,
    evaluate the existing detector and policy, and append durable audit records.
Inputs:
    Injected event source, state store, DRY_RUN policy-input provider, and audit
    sink.  This module never receives a database or actuation client.
Outputs:
    Immutable monitor-cycle results and append-only audit records.
Dependencies:
    Persisted EXP-005 trace artifacts plus the existing assembly, extraction,
    detector, and policy modules.  No PyMilvus or actuation executor is imported.
Failure modes:
    Event, state, identity, ordering, checksum, assembly, extraction, and policy
    input failures are fail-closed and audited.  Rebaselining is never automatic.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Protocol

from .config import Metric
from .drift import DriftDecision, EvidenceProvenance, WindowEvidence, evaluate_drift_decision
from .policy import (
    PolicyAction,
    PolicyDecision,
    PolicyMode,
    PreActionSafety,
    QualificationResult,
    ResponseEstimate,
    evaluate_tuning_policy,
)
from .shadow_artifacts import ShadowTraceArtifactError, load_persisted_shadow_trace_envelope
from .shadow_extraction import extract_window_evidence
from .monitor_evidence import (
    MonitorEvidenceCodecError,
    decode_persisted_window_evidence,
    encode_persisted_window_evidence,
)
from .shadow_window import AssembledShadowWindow, PersistedShadowTraceEnvelope, assemble_shadow_window
from .shadow_event_types import MonitorStreamKey, ShadowTraceEvent, ShadowTraceEventSource
from .response_profile_detector_head import (
    ResponseProfileDetectorHead,
    build_response_profile_detector_head,
)


_SCHEMA_VERSION = "workload-monitor-state-v2"
_SHA256_HEX = frozenset("0123456789abcdef")


class MonitorStateCorruptedError(ValueError):
    """Raised when persisted monitor state cannot be trusted."""


class MonitorRecordStatus(StrEnum):
    """Inspectable outcomes emitted by the monitor boundary."""

    EVENT_BUFFERED = "EVENT_BUFFERED"
    REFERENCE_ACCEPTED = "REFERENCE_ACCEPTED"
    CURRENT_WINDOW_READY = "CURRENT_WINDOW_READY"
    EVALUATED = "EVALUATED"
    REJECTED = "REJECTED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class DryRunPolicyInputs:
    """Externally owned policy material; the monitor never fabricates it."""

    current_ef: int
    response_estimates: Mapping[int, ResponseEstimate]
    pre_action: PreActionSafety
    last_known_good: QualificationResult
    audit_id: str


class DryRunPolicyInputProvider(Protocol):
    """Supplies validated policy inputs bound to detector provenance."""

    def resolve(
        self,
        *,
        decision: DriftDecision,
        provenance: EvidenceProvenance,
    ) -> DryRunPolicyInputs: ...


@dataclass(frozen=True, slots=True)
class MonitorAuditRecord:
    """Primitive-only append-only audit payload suitable for durable outbox use."""

    record_id: str
    stream_key: MonitorStreamKey
    window_id: int | str | None
    window_sequence: int | None
    event_ids: tuple[str, ...]
    event_trace_sha256: tuple[str, ...]
    status: MonitorRecordStatus
    reason_codes: tuple[str, ...] = ()
    manifest_sha256: str | None = None
    detector_state: str | None = None
    detector_classification: str | None = None
    policy_action: str | None = None
    policy_reason: str | None = None
    policy_audit_id: str | None = None


class MonitorAuditSink(Protocol):
    """Idempotent append-only destination for monitor audit records."""

    def contains(self, record_id: str) -> bool: ...

    def append(self, record: MonitorAuditRecord) -> None: ...


@dataclass(frozen=True, slots=True)
class _WindowEvents:
    """Persistable source references for one ordered four-trace window."""

    window_id: int | str
    window_sequence: int
    events: tuple[ShadowTraceEvent, ...]


@dataclass(frozen=True, slots=True)
class MonitorStreamState:
    """Restart-durable stream state plus an atomic append-only audit outbox."""

    stream_key: MonitorStreamKey
    next_window_sequence: int = 0
    reference: _WindowEvents | None = None
    previous_current: _WindowEvents | None = None
    previous_current_evidence: WindowEvidence | None = None
    pending_windows: tuple[_WindowEvents, ...] = ()
    processed_event_ids: tuple[str, ...] = ()
    blocked_reason_codes: tuple[str, ...] = ()
    outbox: tuple[MonitorAuditRecord, ...] = ()
    # The legacy file codec intentionally omits this field and therefore can
    # never issue latest-head authority.  The hardened SQLite store persists it
    # atomically with the state snapshot.
    latest_detector_head: ResponseProfileDetectorHead | None = None


class MonitorStateStore(Protocol):
    """Atomic state/outbox store keyed by stable monitor stream lineage."""

    def load(self, stream_key: MonitorStreamKey) -> MonitorStreamState | None: ...

    def save(self, state: MonitorStreamState) -> None: ...


class FileMonitorStateStore:
    """Strict JSON state store using write-fsync-replace-directory-fsync commits."""

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)

    def _path(self, stream_id: str) -> Path:
        digest = hashlib.sha256(stream_id.encode("utf-8")).hexdigest()
        return self._directory / f"{digest}.json"

    def load(self, stream_key: MonitorStreamKey) -> MonitorStreamState | None:
        path = self._path(stream_key.stream_id)
        if not path.exists():
            return None
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
            state = _state_from_document(document)
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise MonitorStateCorruptedError("MONITOR_STATE_CORRUPTED") from exc
        if state.stream_key.stream_id != stream_key.stream_id:
            raise MonitorStateCorruptedError("MONITOR_STATE_STREAM_ID_MISMATCH")
        return state

    def save(self, state: MonitorStreamState) -> None:
        document = _state_document(state)
        payload = (
            json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
            + "\n"
        ).encode("utf-8")
        target = self._path(state.stream_key.stream_id)
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
            os.replace(temporary, target)
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


@dataclass(frozen=True, slots=True)
class MonitorCycleResult:
    """One externally inspectable result for one consumed source event."""

    event_id: str
    accepted: bool
    processed_event_count: int
    reason_codes: tuple[str, ...] = ()
    assembled_window: AssembledShadowWindow | None = None
    drift_decision: DriftDecision | None = None
    policy_decision: PolicyDecision | None = None


class WorkloadMonitor:
    """One bounded ADR-005 monitoring cycle; this class is DRY_RUN-only."""

    def __init__(
        self,
        *,
        source: ShadowTraceEventSource,
        state_store: MonitorStateStore,
        policy_input_provider: DryRunPolicyInputProvider,
        audit_sink: MonitorAuditSink,
        detector_seed: int,
    ) -> None:
        if isinstance(detector_seed, bool) or not isinstance(detector_seed, int):
            raise ValueError("detector_seed must be an integer")
        self.source = source
        self.state_store = state_store
        self.policy_input_provider = policy_input_provider
        self.audit_sink = audit_sink
        self.detector_seed = detector_seed

    def run_once(self, *, max_events: int) -> tuple[MonitorCycleResult, ...]:
        """Process at most ``max_events`` source events without live side effects."""

        if isinstance(max_events, bool) or not isinstance(max_events, int) or max_events < 1:
            raise ValueError("max_events must be a positive integer")
        events = self.source.poll(limit=max_events)
        if len(events) > max_events:
            raise ValueError("event source violated max_events bound")
        results: list[MonitorCycleResult] = []
        for event in events:
            result = self._process_event(event)
            results.append(result)
            self.source.acknowledge((event.event_id,))
        return tuple(results)

    def _process_event(self, event: ShadowTraceEvent) -> MonitorCycleResult:
        try:
            state = self.state_store.load(event.stream_key)
        except MonitorStateCorruptedError as exc:
            record = _event_record(
                event, MonitorRecordStatus.REJECTED, (str(exc),)
            )
            _append_direct(self.audit_sink, record)
            return MonitorCycleResult(
                event_id=event.event_id,
                accepted=False,
                processed_event_count=1,
                reason_codes=(str(exc),),
            )

        if state is None:
            if event.window_sequence != 0:
                record = _event_record(
                    event,
                    MonitorRecordStatus.REJECTED,
                    ("STATE_MISSING_FOR_NONREFERENCE",),
                )
                _append_direct(self.audit_sink, record)
                return MonitorCycleResult(
                    event_id=event.event_id,
                    accepted=False,
                    processed_event_count=1,
                    reason_codes=("STATE_MISSING_FOR_NONREFERENCE",),
                )
            state = MonitorStreamState(stream_key=event.stream_key)
        else:
            # A replay can be a duplicate of an event whose state commit succeeded
            # but whose audit-sink append failed.  Drain the durable outbox before
            # the duplicate check so an at-least-once source cannot strand it.
            state = self._flush_outbox(state)
            if state.stream_key != event.stream_key:
                state = _reject_and_block(
                    state,
                    event,
                    "STREAM_IDENTITY_CHANGED",
                )
                self.state_store.save(state)
                self._flush_outbox(state)
                return MonitorCycleResult(
                    event_id=event.event_id,
                    accepted=False,
                    processed_event_count=1,
                    reason_codes=("STREAM_IDENTITY_CHANGED",),
                )

        if event.event_id in state.processed_event_ids:
            state = _enqueue(
                state,
                _event_record(
                    event,
                    MonitorRecordStatus.REJECTED,
                    ("DUPLICATE_EVENT",),
                    record_id=f"duplicate:{event.event_id}",
                ),
            )
            self.state_store.save(state)
            self._flush_outbox(state)
            return MonitorCycleResult(
                event_id=event.event_id,
                accepted=False,
                processed_event_count=1,
                reason_codes=("DUPLICATE_EVENT",),
            )
        if state.blocked_reason_codes:
            state = _reject_and_block(state, event, "STREAM_BLOCKED")
            self.state_store.save(state)
            self._flush_outbox(state)
            return MonitorCycleResult(
                event_id=event.event_id,
                accepted=False,
                processed_event_count=1,
                reason_codes=("STREAM_BLOCKED", *state.blocked_reason_codes),
            )

        try:
            envelope = load_persisted_shadow_trace_envelope(event.envelope_path)
        except (FileNotFoundError, ShadowTraceArtifactError, OSError) as exc:
            del exc
            state = _reject_and_block(state, event, "ENVELOPE_LOAD_FAILED")
            self.state_store.save(state)
            self._flush_outbox(state)
            return MonitorCycleResult(
                event_id=event.event_id,
                accepted=False,
                processed_event_count=1,
                reason_codes=("ENVELOPE_LOAD_FAILED",),
            )
        if envelope.expected_trace_sha256 != event.expected_trace_sha256:
            state = _reject_and_block(state, event, "EVENT_TRACE_SHA256_MISMATCH")
            self.state_store.save(state)
            self._flush_outbox(state)
            return MonitorCycleResult(
                event_id=event.event_id,
                accepted=False,
                processed_event_count=1,
                reason_codes=("EVENT_TRACE_SHA256_MISMATCH",),
            )
        if not _envelope_matches_stream(envelope, event.stream_key):
            state = _reject_and_block(state, event, "EVENT_STREAM_KEY_MISMATCH")
            self.state_store.save(state)
            self._flush_outbox(state)
            return MonitorCycleResult(
                event_id=event.event_id,
                accepted=False,
                processed_event_count=1,
                reason_codes=("EVENT_STREAM_KEY_MISMATCH",),
            )
        if event.window_sequence < state.next_window_sequence:
            state = _reject_and_block(state, event, "WINDOW_SEQUENCE_ALREADY_FINALIZED")
            self.state_store.save(state)
            self._flush_outbox(state)
            return MonitorCycleResult(
                event_id=event.event_id,
                accepted=False,
                processed_event_count=1,
                reason_codes=("WINDOW_SEQUENCE_ALREADY_FINALIZED",),
            )

        state, add_reason = _add_event(state, event)
        if add_reason is not None:
            state = _reject_and_block(state, event, add_reason)
            self.state_store.save(state)
            self._flush_outbox(state)
            return MonitorCycleResult(
                event_id=event.event_id,
                accepted=False,
                processed_event_count=1,
                reason_codes=(add_reason,),
            )

        state = _enqueue(
            state,
            _event_record(event, MonitorRecordStatus.EVENT_BUFFERED, ()),
        )
        advance = self._advance_ready_windows(state)
        state = advance.state
        self.state_store.save(state)
        self._flush_outbox(state)
        return MonitorCycleResult(
            event_id=event.event_id,
            accepted=not state.blocked_reason_codes,
            processed_event_count=1,
            reason_codes=advance.reason_codes,
            assembled_window=advance.assembled_window,
            drift_decision=advance.drift_decision,
            policy_decision=advance.policy_decision,
        )

    def _advance_ready_windows(self, state: MonitorStreamState) -> "_AdvanceResult":
        assembled: AssembledShadowWindow | None = None
        decision: DriftDecision | None = None
        policy: PolicyDecision | None = None
        reasons: tuple[str, ...] = ()
        while not state.blocked_reason_codes:
            window = _find_pending(state, state.next_window_sequence)
            if window is None or len(window.events) != 4:
                break
            try:
                envelopes = tuple(
                    load_persisted_shadow_trace_envelope(event.envelope_path)
                    for event in sorted(
                        window.events,
                        key=lambda item: _loaded_sequence_index(item.envelope_path),
                    )
                )
            except (FileNotFoundError, ShadowTraceArtifactError, OSError):
                state = _block_window(state, window, "ENVELOPE_LOAD_FAILED")
                reasons = ("ENVELOPE_LOAD_FAILED",)
                break
            assembled = assemble_shadow_window(
                window_id=window.window_id, envelopes=envelopes
            )
            if not assembled.complete:
                reasons = ("WINDOW_ASSEMBLY_INCOMPLETE", *assembled.reason_codes)
                state = _block_window(state, window, *reasons)
                break
            state = _remove_pending(state, window.window_sequence)
            if state.reference is None:
                state = replace(
                    state,
                    reference=window,
                    next_window_sequence=state.next_window_sequence + 1,
                )
                state = _enqueue(
                    state,
                    _window_record(
                        state,
                        window,
                        MonitorRecordStatus.REFERENCE_ACCEPTED,
                        assembled=assembled,
                    ),
                )
                continue

            try:
                reference_window = self._assemble_saved_window(state.reference)
            except MonitorStateCorruptedError:
                reasons = ("SAVED_WINDOW_LOAD_FAILED",)
                state = _block_window(state, window, *reasons)
                break
            try:
                current_evidence = extract_window_evidence(
                    reference_window=reference_window,
                    current_window=assembled,
                    metric=state.stream_key.metric,
                    detector_seed=self.detector_seed,
                )
            except Exception:
                reasons = ("EXTRACTION_RAISED",)
                state = _block_window(state, window, *reasons)
                break
            if not current_evidence.complete:
                reasons = ("EXTRACTION_INCOMPLETE", *current_evidence.reason_codes)
                state = _block_window(state, window, *reasons)
                break
            if state.previous_current is None:
                state = replace(
                    state,
                    previous_current=window,
                    previous_current_evidence=current_evidence,
                    next_window_sequence=state.next_window_sequence + 1,
                )
                state = _enqueue(
                    state,
                    _window_record(
                        state,
                        window,
                        MonitorRecordStatus.CURRENT_WINDOW_READY,
                        assembled=assembled,
                    ),
                )
                continue

            try:
                previous_window = self._assemble_saved_window(state.previous_current)
            except MonitorStateCorruptedError:
                reasons = ("SAVED_WINDOW_LOAD_FAILED",)
                state = _block_window(state, window, *reasons)
                break
            previous_evidence = state.previous_current_evidence
            if previous_evidence is None:
                reasons = ("STATE_PREVIOUS_EVIDENCE_MISSING",)
                state = _block_window(state, window, *reasons)
                break
            if not _evidence_matches_window(
                previous_evidence,
                previous_window,
                state.stream_key,
            ):
                reasons = ("STATE_PREVIOUS_EVIDENCE_MISMATCH",)
                state = _block_window(state, window, *reasons)
                break
            decision = evaluate_drift_decision(previous_evidence, current_evidence)
            if decision.evidence_provenance is None:
                reasons = ("DECISION_PROVENANCE_MISSING",)
                state = _block_window(state, window, *reasons)
                break
            try:
                inputs = self.policy_input_provider.resolve(
                    decision=decision, provenance=decision.evidence_provenance
                )
            except Exception:
                reasons = ("POLICY_INPUT_UNAVAILABLE",)
                state = _block_window(state, window, *reasons)
                break
            if not isinstance(inputs, DryRunPolicyInputs) or not inputs.audit_id:
                reasons = ("POLICY_INPUT_INVALID",)
                state = _block_window(state, window, *reasons)
                break
            policy = evaluate_tuning_policy(
                detector=decision,
                current_ef=inputs.current_ef,
                response_estimates=inputs.response_estimates,
                pre_action=inputs.pre_action,
                canary_observation=None,
                qualification_windows=None,
                last_known_good=inputs.last_known_good,
                mode=PolicyMode.DRY_RUN,
                threshold_stratum=state.stream_key.threshold_stratum,
                audit_id=inputs.audit_id,
            )
            if policy.action not in {PolicyAction.NO_CHANGE, PolicyAction.RECOMMEND_EF}:
                reasons = ("UNEXPECTED_POLICY_ACTION", policy.action.value)
                state = _block_window(state, window, *reasons)
                break
            state = replace(
                state,
                previous_current=window,
                previous_current_evidence=current_evidence,
                next_window_sequence=state.next_window_sequence + 1,
                latest_detector_head=build_response_profile_detector_head(
                    stream_key=state.stream_key,
                    window_sequence=window.window_sequence,
                    detector_state=decision.state,
                    detector_classification=decision.classification,
                    detector_provenance=decision.evidence_provenance,
                ),
            )
            state = _enqueue(
                state,
                _window_record(
                    state,
                    window,
                    MonitorRecordStatus.EVALUATED,
                    assembled=assembled,
                    decision=decision,
                    policy=policy,
                ),
            )
        return _AdvanceResult(state, reasons, assembled, decision, policy)

    def _assemble_saved_window(self, window: _WindowEvents) -> AssembledShadowWindow:
        try:
            envelopes = tuple(
                load_persisted_shadow_trace_envelope(event.envelope_path)
                for event in sorted(
                    window.events,
                    key=lambda item: _loaded_sequence_index(item.envelope_path),
                )
            )
        except (FileNotFoundError, ShadowTraceArtifactError, OSError) as exc:
            raise MonitorStateCorruptedError("SAVED_WINDOW_LOAD_FAILED") from exc
        assembled = assemble_shadow_window(window_id=window.window_id, envelopes=envelopes)
        if not assembled.complete:
            raise MonitorStateCorruptedError("SAVED_WINDOW_NO_LONGER_VALID")
        return assembled

    def _flush_outbox(self, state: MonitorStreamState) -> MonitorStreamState:
        remaining = list(state.outbox)
        while remaining:
            record = remaining[0]
            if not self.audit_sink.contains(record.record_id):
                self.audit_sink.append(record)
            remaining.pop(0)
            self.state_store.save(replace(state, outbox=tuple(remaining)))
            state = replace(state, outbox=tuple(remaining))
        return state


@dataclass(frozen=True, slots=True)
class _AdvanceResult:
    state: MonitorStreamState
    reason_codes: tuple[str, ...]
    assembled_window: AssembledShadowWindow | None
    drift_decision: DriftDecision | None
    policy_decision: PolicyDecision | None


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in _SHA256_HEX for character in value)
    )


def _envelope_matches_stream(
    envelope: PersistedShadowTraceEnvelope, stream_key: MonitorStreamKey
) -> bool:
    trace = envelope.trace
    if trace is None:
        return False
    return bool(
        trace.metric is stream_key.metric
        and trace.threshold_stratum == stream_key.threshold_stratum
        and trace.configuration_identity == stream_key.configuration_identity
        and trace.data_identity == stream_key.data_identity
        and trace.flat_identity.expected_binding_id == stream_key.flat_binding_id
        and trace.hnsw_identity.expected_binding_id == stream_key.hnsw_binding_id
    )


def _loaded_sequence_index(path: Path) -> int:
    return load_persisted_shadow_trace_envelope(path).sequence_index


def _find_pending(state: MonitorStreamState, sequence: int) -> _WindowEvents | None:
    return next(
        (window for window in state.pending_windows if window.window_sequence == sequence),
        None,
    )


def _evidence_matches_window(
    evidence: WindowEvidence,
    window: AssembledShadowWindow,
    stream_key: MonitorStreamKey,
) -> bool:
    """Bind restored evidence to the exact prior immutable window and stream."""

    provenance = evidence.provenance
    return bool(
        evidence.complete
        and evidence.metric is stream_key.metric
        and evidence.window_id == window.window_id
        and provenance is not None
        and provenance.metric is stream_key.metric
        and provenance.threshold_stratum == stream_key.threshold_stratum
        and provenance.current_window_id == window.window_id
        and provenance.current_manifest_sha256 == window.manifest_sha256
        and provenance.configuration_identity == stream_key.configuration_identity
        and provenance.data_identity == stream_key.data_identity
        and provenance.flat_binding_id == stream_key.flat_binding_id
        and provenance.hnsw_binding_id == stream_key.hnsw_binding_id
    )


def _add_event(
    state: MonitorStreamState, event: ShadowTraceEvent
) -> tuple[MonitorStreamState, str | None]:
    current = _find_pending(state, event.window_sequence)
    if current is None:
        current = _WindowEvents(event.window_id, event.window_sequence, ())
    elif current.window_id != event.window_id:
        return state, "WINDOW_ID_CONFLICT"
    if any(item.envelope_path == event.envelope_path for item in current.events):
        return state, "DUPLICATE_ENVELOPE_REFERENCE"
    if len(current.events) >= 4:
        return state, "WINDOW_EVENT_COUNT_EXCEEDED"
    updated = replace(current, events=(*current.events, event))
    pending = tuple(
        updated if window.window_sequence == event.window_sequence else window
        for window in state.pending_windows
    )
    if current not in state.pending_windows:
        pending = (*pending, updated)
    pending = tuple(sorted(pending, key=lambda item: item.window_sequence))
    return replace(
        state,
        pending_windows=pending,
        processed_event_ids=(*state.processed_event_ids, event.event_id),
    ), None


def _remove_pending(state: MonitorStreamState, sequence: int) -> MonitorStreamState:
    return replace(
        state,
        pending_windows=tuple(
            window for window in state.pending_windows if window.window_sequence != sequence
        ),
    )


def _enqueue(state: MonitorStreamState, record: MonitorAuditRecord) -> MonitorStreamState:
    if any(item.record_id == record.record_id for item in state.outbox):
        return state
    return replace(state, outbox=(*state.outbox, record))


def _reject_and_block(
    state: MonitorStreamState, event: ShadowTraceEvent, reason: str
) -> MonitorStreamState:
    state = replace(
        state,
        processed_event_ids=(*state.processed_event_ids, event.event_id),
        blocked_reason_codes=tuple(dict.fromkeys((*state.blocked_reason_codes, reason))),
    )
    return _enqueue(state, _event_record(event, MonitorRecordStatus.REJECTED, (reason,)))


def _block_window(
    state: MonitorStreamState, window: _WindowEvents, *reasons: str
) -> MonitorStreamState:
    state = replace(
        state,
        blocked_reason_codes=tuple(dict.fromkeys((*state.blocked_reason_codes, *reasons))),
    )
    return _enqueue(
        state,
        _window_record(state, window, MonitorRecordStatus.BLOCKED, reason_codes=reasons),
    )


def _event_record(
    event: ShadowTraceEvent,
    status: MonitorRecordStatus,
    reasons: tuple[str, ...],
    *,
    record_id: str | None = None,
) -> MonitorAuditRecord:
    return MonitorAuditRecord(
        record_id=record_id or f"event:{event.event_id}",
        stream_key=event.stream_key,
        window_id=event.window_id,
        window_sequence=event.window_sequence,
        event_ids=(event.event_id,),
        event_trace_sha256=(event.expected_trace_sha256,),
        status=status,
        reason_codes=reasons,
    )


def _window_record(
    state: MonitorStreamState,
    window: _WindowEvents,
    status: MonitorRecordStatus,
    *,
    assembled: AssembledShadowWindow | None = None,
    decision: DriftDecision | None = None,
    policy: PolicyDecision | None = None,
    reason_codes: Sequence[str] = (),
) -> MonitorAuditRecord:
    record_id = (
        policy.audit_id
        if policy is not None
        else f"window:{state.stream_key.stream_id}:{window.window_sequence}"
    )
    return MonitorAuditRecord(
        record_id=record_id,
        stream_key=state.stream_key,
        window_id=window.window_id,
        window_sequence=window.window_sequence,
        event_ids=tuple(event.event_id for event in window.events),
        event_trace_sha256=tuple(event.expected_trace_sha256 for event in window.events),
        status=status,
        reason_codes=tuple(reason_codes),
        manifest_sha256=assembled.manifest_sha256 if assembled is not None else None,
        detector_state=decision.state.value if decision is not None else None,
        detector_classification=(
            decision.classification.value if decision is not None else None
        ),
        policy_action=policy.action.value if policy is not None else None,
        policy_reason=policy.reason if policy is not None else None,
        policy_audit_id=policy.audit_id if policy is not None else None,
    )


def _append_direct(sink: MonitorAuditSink, record: MonitorAuditRecord) -> None:
    if not sink.contains(record.record_id):
        sink.append(record)


def _key_document(key: MonitorStreamKey) -> dict[str, object]:
    return {
        "stream_id": key.stream_id,
        "metric": key.metric.value,
        "threshold_stratum": key.threshold_stratum,
        "configuration_identity": key.configuration_identity,
        "data_identity": key.data_identity,
        "flat_binding_id": key.flat_binding_id,
        "hnsw_binding_id": key.hnsw_binding_id,
    }


def _key_from_document(value: object) -> MonitorStreamKey:
    if not isinstance(value, dict) or frozenset(value) != {
        "stream_id", "metric", "threshold_stratum", "configuration_identity",
        "data_identity", "flat_binding_id", "hnsw_binding_id",
    }:
        raise ValueError("stream key schema mismatch")
    return MonitorStreamKey(
        stream_id=value["stream_id"],
        metric=Metric(value["metric"]),
        threshold_stratum=value["threshold_stratum"],
        configuration_identity=value["configuration_identity"],
        data_identity=value["data_identity"],
        flat_binding_id=value["flat_binding_id"],
        hnsw_binding_id=value["hnsw_binding_id"],
    )


def _event_document(event: ShadowTraceEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "stream_key": _key_document(event.stream_key),
        "window_id": event.window_id,
        "window_sequence": event.window_sequence,
        "envelope_path": str(event.envelope_path),
        "expected_trace_sha256": event.expected_trace_sha256,
    }


def _event_from_document(value: object) -> ShadowTraceEvent:
    if not isinstance(value, dict) or frozenset(value) != {
        "event_id", "stream_key", "window_id", "window_sequence", "envelope_path",
        "expected_trace_sha256",
    }:
        raise ValueError("event schema mismatch")
    return ShadowTraceEvent(
        event_id=value["event_id"],
        stream_key=_key_from_document(value["stream_key"]),
        window_id=value["window_id"],
        window_sequence=value["window_sequence"],
        envelope_path=Path(value["envelope_path"]),
        expected_trace_sha256=value["expected_trace_sha256"],
    )


def _window_document(window: _WindowEvents | None) -> object:
    if window is None:
        return None
    return {
        "window_id": window.window_id,
        "window_sequence": window.window_sequence,
        "events": [_event_document(event) for event in window.events],
    }


def _window_from_document(value: object) -> _WindowEvents | None:
    if value is None:
        return None
    if not isinstance(value, dict) or frozenset(value) != {
        "window_id", "window_sequence", "events"
    } or not isinstance(value["events"], list):
        raise ValueError("window schema mismatch")
    return _WindowEvents(
        window_id=value["window_id"],
        window_sequence=value["window_sequence"],
        events=tuple(_event_from_document(item) for item in value["events"]),
    )


def _record_document(record: MonitorAuditRecord) -> dict[str, object]:
    return {
        "record_id": record.record_id,
        "stream_key": _key_document(record.stream_key),
        "window_id": record.window_id,
        "window_sequence": record.window_sequence,
        "event_ids": list(record.event_ids),
        "event_trace_sha256": list(record.event_trace_sha256),
        "status": record.status.value,
        "reason_codes": list(record.reason_codes),
        "manifest_sha256": record.manifest_sha256,
        "detector_state": record.detector_state,
        "detector_classification": record.detector_classification,
        "policy_action": record.policy_action,
        "policy_reason": record.policy_reason,
        "policy_audit_id": record.policy_audit_id,
    }


def _record_from_document(value: object) -> MonitorAuditRecord:
    required = {
        "record_id", "stream_key", "window_id", "window_sequence", "event_ids",
        "event_trace_sha256", "status",
        "reason_codes", "manifest_sha256", "detector_state", "detector_classification",
        "policy_action", "policy_reason", "policy_audit_id",
    }
    if not isinstance(value, dict) or frozenset(value) != required:
        raise ValueError("audit record schema mismatch")
    if (
        not isinstance(value["event_ids"], list)
        or not isinstance(value["event_trace_sha256"], list)
        or not isinstance(value["reason_codes"], list)
    ):
        raise ValueError("audit record array schema mismatch")
    return MonitorAuditRecord(
        record_id=value["record_id"], stream_key=_key_from_document(value["stream_key"]),
        window_id=value["window_id"], window_sequence=value["window_sequence"],
        event_ids=tuple(value["event_ids"]),
        event_trace_sha256=tuple(value["event_trace_sha256"]),
        status=MonitorRecordStatus(value["status"]),
        reason_codes=tuple(value["reason_codes"]), manifest_sha256=value["manifest_sha256"],
        detector_state=value["detector_state"], detector_classification=value["detector_classification"],
        policy_action=value["policy_action"], policy_reason=value["policy_reason"],
        policy_audit_id=value["policy_audit_id"],
    )


def _state_document(state: MonitorStreamState) -> dict[str, object]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "stream_key": _key_document(state.stream_key),
        "next_window_sequence": state.next_window_sequence,
        "reference": _window_document(state.reference),
        "previous_current": _window_document(state.previous_current),
        "previous_current_evidence": (
            None
            if state.previous_current_evidence is None
            else encode_persisted_window_evidence(state.previous_current_evidence)
        ),
        "pending_windows": [_window_document(window) for window in state.pending_windows],
        "processed_event_ids": list(state.processed_event_ids),
        "blocked_reason_codes": list(state.blocked_reason_codes),
        "outbox": [_record_document(record) for record in state.outbox],
    }


def _state_from_document(value: object) -> MonitorStreamState:
    required = {
        "schema_version", "stream_key", "next_window_sequence", "reference", "previous_current",
        "previous_current_evidence",
        "pending_windows", "processed_event_ids", "blocked_reason_codes", "outbox",
    }
    if not isinstance(value, dict) or frozenset(value) != required:
        raise ValueError("state schema mismatch")
    if value["schema_version"] != _SCHEMA_VERSION:
        raise ValueError("state schema version mismatch")
    if not all(isinstance(value[field], list) for field in (
        "pending_windows", "processed_event_ids", "blocked_reason_codes", "outbox"
    )):
        raise ValueError("state array schema mismatch")
    previous_current = _window_from_document(value["previous_current"])
    encoded_evidence = value["previous_current_evidence"]
    if (previous_current is None) != (encoded_evidence is None):
        raise ValueError("previous evidence/state mismatch")
    try:
        previous_current_evidence = (
            None
            if encoded_evidence is None
            else decode_persisted_window_evidence(encoded_evidence)
        )
    except MonitorEvidenceCodecError as exc:
        raise ValueError("previous evidence is untrusted") from exc
    if (
        previous_current is not None
        and previous_current_evidence is not None
        and previous_current_evidence.window_id != previous_current.window_id
    ):
        raise ValueError("previous evidence window mismatch")
    return MonitorStreamState(
        stream_key=_key_from_document(value["stream_key"]),
        next_window_sequence=value["next_window_sequence"],
        reference=_window_from_document(value["reference"]),
        previous_current=previous_current,
        previous_current_evidence=previous_current_evidence,
        pending_windows=tuple(_window_from_document(item) for item in value["pending_windows"]),
        processed_event_ids=tuple(value["processed_event_ids"]),
        blocked_reason_codes=tuple(value["blocked_reason_codes"]),
        outbox=tuple(_record_from_document(item) for item in value["outbox"]),
    )


__all__ = [
    "DryRunPolicyInputProvider",
    "DryRunPolicyInputs",
    "FileMonitorStateStore",
    "MonitorAuditRecord",
    "MonitorAuditSink",
    "MonitorCycleResult",
    "MonitorRecordStatus",
    "MonitorStateCorruptedError",
    "MonitorStateStore",
    "MonitorStreamKey",
    "MonitorStreamState",
    "ShadowTraceEvent",
    "ShadowTraceEventSource",
    "WorkloadMonitor",
]
