"""ADR-014 v2 shadow worker: committed source window -> source-bound evidence.

Purpose:
    Turn one complete 200-member committed v2 source window into the two
    artifacts the governed detector path needs, from a single pass:
      * one `AssembledShadowWindow` (the unchanged detector input), and
      * 200 `V2ShadowPositionEvidence` values binding each committed source
        position to the exact shadow record that answered it.
Shape:
    The repository's shadow format is four 50-query `ShadowAuditTrace`
    envelopes per 200-query window, so this module produces exactly
    `TRACE_COUNT` envelopes of `TRACE_QUERY_COUNT` queries and never invents a
    per-query envelope digest.
Durability:
    ADR-015 commits one governed STARTED event before each physical trace and
    one terminal event immediately afterward. Assembly reloads four verified
    COMPLETED envelopes from the injected SQLite attempt store; FAILED and
    orphaned attempts are terminal and non-retriable.
Boundaries:
    Shadow capture itself is an injected port (`V2ShadowCaptureExecutor`); this
    module performs no search and imports no Milvus, policy, canary, grant, or
    routing code. It duplicates no shadow semantics: window assembly and trace
    hashing are the existing `assemble_shadow_window` and
    `hash_shadow_audit_trace`.
Failure modes:
    A short/oversized window, a capture that returns the wrong trace shape, a
    query-id mismatch against the committed source, or an incomplete assembly
    fails closed; no partial window is emitted as if it were complete.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from .host_window_detector_v2 import (
    V2ShadowWindow,
    build_v2_shadow_position,
    build_v2_shadow_window,
)
from .host_window_lineage import CommittedHostObservation
from .milvus_actuation import ShadowAuditTrace
from .real_detector_attestation import position_evidence_sha256
from .shadow_attempt_store import (
    ShadowAttemptPermit,
    ShadowAttemptStatus,
    ShadowAttemptStoreError,
    SQLiteShadowAttemptStore,
    build_shadow_attempt_identity,
    expected_shadow_trace_id,
)
from .shadow_window import (
    TRACE_COUNT,
    TRACE_QUERY_COUNT,
    WINDOW_QUERY_COUNT,
    AssembledShadowWindow,
    PersistedShadowTraceEnvelope,
    assemble_shadow_window,
    hash_shadow_audit_trace,
    validate_persisted_shadow_trace_envelope,
)

__all__ = [
    "V2ShadowCaptureExecutor",
    "V2ShadowWindowBundle",
    "V2ShadowWorker",
    "V2ShadowWorkerError",
]


class V2ShadowWorkerError(RuntimeError):
    """Fail-closed shadow-worker error carrying one stable reason code."""

    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        reason_codes: tuple[str, ...] = (),
        failure_code: str | None = None,
        error_type: str | None = None,
    ) -> None:
        super().__init__(message or code)
        self.code = code
        self.reason_codes = reason_codes
        self.failure_code = failure_code
        self.error_type = error_type


def _error(
    code: str,
    message: str | None = None,
    *,
    reason_codes: tuple[str, ...] = (),
    failure_code: str | None = None,
    error_type: str | None = None,
) -> V2ShadowWorkerError:
    return V2ShadowWorkerError(
        code,
        message,
        reason_codes=reason_codes,
        failure_code=failure_code,
        error_type=error_type,
    )


class V2ShadowCaptureExecutor(Protocol):
    """Injected read-only shadow capture for one 50-query slice.

    A production implementation performs the real FLAT / HNSW / sentinel-ef
    captures; tests inject a deterministic fake. Either way this module never
    contacts a service itself.
    """

    def capture(
        self, sources: tuple[CommittedHostObservation, ...], *, trace_sequence_index: int
    ) -> ShadowAuditTrace: ...


@dataclass(frozen=True, slots=True)
class V2ShadowWindowBundle:
    """One window's paired detector input and source-bound position evidence."""

    window_sequence: int
    sources: tuple[CommittedHostObservation, ...]
    assembled: AssembledShadowWindow
    shadow_window: V2ShadowWindow
    envelopes: tuple[PersistedShadowTraceEnvelope, ...]


class V2ShadowWorker:
    """Produce one `V2ShadowWindowBundle` per complete committed source window."""

    def __init__(
        self,
        *,
        capture_executor: V2ShadowCaptureExecutor,
        captured_at_clock: Callable[[], str],
        attempt_store: SQLiteShadowAttemptStore,
    ) -> None:
        """`captured_at_clock` timestamps each durable lifecycle boundary.

        New physical traces read it for STARTED and terminal persistence. The
        terminal timestamp is the envelope capture timestamp. The window
        assembler requires strictly increasing envelope timestamps, which is
        also what real sequential capture produces. Persisted COMPLETED traces
        consume no new clock value after restart.
        """

        if not callable(getattr(capture_executor, "capture", None)):
            raise TypeError("capture_executor must provide capture")
        if not callable(captured_at_clock):
            raise TypeError("captured_at_clock must be callable")
        if type(attempt_store) is not SQLiteShadowAttemptStore:
            raise TypeError("attempt_store must be SQLiteShadowAttemptStore")
        self._executor = capture_executor
        self._clock = captured_at_clock
        self._attempt_store = attempt_store

    @staticmethod
    def _store_error(exc: ShadowAttemptStoreError) -> V2ShadowWorkerError:
        return _error(exc.code, str(exc), failure_code=exc.code)

    def _fail_started_attempt(
        self,
        *,
        identity,
        permit: ShadowAttemptPermit,
        failure_code: str,
        reason_codes: tuple[str, ...],
        error_type: str | None = None,
        envelope: PersistedShadowTraceEnvelope | None = None,
    ) -> None:
        """Persist terminal failure; a write failure deliberately leaves an orphan."""

        try:
            self._attempt_store.fail_attempt(
                identity,
                permit=permit,
                failed_at_utc=self._clock(),
                failure_code=failure_code,
                reason_codes=reason_codes,
                error_type=error_type,
                envelope=envelope,
            )
        except ShadowAttemptStoreError as exc:
            raise _error(
                "SHADOW_ATTEMPT_TERMINAL_PERSIST_FAILED",
                exc.code,
                reason_codes=("EXECUTION_OUTCOME_UNKNOWN",),
                failure_code=exc.code,
                error_type=type(exc).__name__,
            ) from exc

    def build(
        self, sources: tuple[CommittedHostObservation, ...]
    ) -> V2ShadowWindowBundle:
        if type(sources) is not tuple or len(sources) != WINDOW_QUERY_COUNT:
            raise _error("SHADOW_WINDOW_SOURCE_COUNT_INVALID")
        first = sources[0]
        if type(first) is not CommittedHostObservation:
            raise _error("SHADOW_WINDOW_SOURCE_INVALID")
        window_sequence = first.window_sequence

        envelopes: list[PersistedShadowTraceEnvelope] = []
        for trace_index in range(TRACE_COUNT):
            start = trace_index * TRACE_QUERY_COUNT
            slice_sources = sources[start : start + TRACE_QUERY_COUNT]
            try:
                identity = build_shadow_attempt_identity(
                    slice_sources, trace_sequence_index=trace_index
                )
                persisted = self._attempt_store.load_slot(
                    window_sequence=window_sequence,
                    trace_sequence_index=trace_index,
                )
            except ShadowAttemptStoreError as exc:
                raise self._store_error(exc) from exc
            if persisted is not None:
                if persisted.identity.attempt_sha256 != identity.attempt_sha256:
                    raise _error(
                        "SHADOW_ATTEMPT_BINDING_MISMATCH",
                        failure_code="SHADOW_ATTEMPT_BINDING_MISMATCH",
                    )
                if persisted.status is ShadowAttemptStatus.ORPHANED:
                    raise _error(
                        "SHADOW_ATTEMPT_ORPHANED",
                        "ORPHANED;EXECUTION_OUTCOME_UNKNOWN",
                        reason_codes=("EXECUTION_OUTCOME_UNKNOWN",),
                        failure_code="ORPHANED",
                    )
                if persisted.status is ShadowAttemptStatus.FAILED:
                    detail = ";".join(
                        item
                        for item in (persisted.failure_code, *persisted.reason_codes)
                        if item
                    )
                    raise _error(
                        "SHADOW_ATTEMPT_PREVIOUSLY_FAILED",
                        detail,
                        reason_codes=persisted.reason_codes,
                        failure_code=persisted.failure_code,
                        error_type=persisted.error_type,
                    )
                if (
                    persisted.status is not ShadowAttemptStatus.COMPLETED
                    or persisted.envelope is None
                ):
                    raise _error("SHADOW_ATTEMPT_STATE_INVALID")
                envelopes.append(persisted.envelope)
                continue

            try:
                permit = self._attempt_store.start_attempt(
                    identity, started_at_utc=self._clock()
                )
            except ShadowAttemptStoreError as exc:
                raise self._store_error(exc) from exc
            try:
                trace = self._executor.capture(
                    slice_sources, trace_sequence_index=trace_index
                )
            except Exception as exc:
                error_type = type(exc).__name__
                self._fail_started_attempt(
                    identity=identity,
                    permit=permit,
                    failure_code="SHADOW_CAPTURE_EXCEPTION",
                    reason_codes=("EXECUTION_OUTCOME_UNKNOWN",),
                    error_type=error_type,
                )
                raise _error(
                    "SHADOW_CAPTURE_EXCEPTION",
                    f"SHADOW_CAPTURE_EXCEPTION;EXECUTION_OUTCOME_UNKNOWN;{error_type}",
                    reason_codes=("EXECUTION_OUTCOME_UNKNOWN",),
                    failure_code="SHADOW_CAPTURE_EXCEPTION",
                    error_type=error_type,
                ) from exc
            if type(trace) is not ShadowAuditTrace:
                self._fail_started_attempt(
                    identity=identity,
                    permit=permit,
                    failure_code="SHADOW_CAPTURE_INVALID",
                    reason_codes=(),
                )
                raise _error(
                    "SHADOW_CAPTURE_INVALID",
                    failure_code="SHADOW_CAPTURE_INVALID",
                )
            try:
                terminal_at_utc = self._clock()
                envelope = PersistedShadowTraceEnvelope(
                    trace_id=expected_shadow_trace_id(
                        window_sequence=window_sequence,
                        trace_sequence_index=trace_index,
                    ),
                    captured_at_utc=terminal_at_utc,
                    sequence_index=trace_index,
                    declared_observation_count=TRACE_QUERY_COUNT,
                    expected_trace_sha256=hash_shadow_audit_trace(trace),
                    trace=trace,
                )
                trace_validation_reasons = validate_persisted_shadow_trace_envelope(
                    envelope
                )
            except Exception as exc:
                error_type = type(exc).__name__
                self._fail_started_attempt(
                    identity=identity,
                    permit=permit,
                    failure_code="SHADOW_TRACE_CANONICALIZATION_FAILED",
                    reason_codes=tuple(trace.reason_codes),
                    error_type=error_type,
                )
                raise _error(
                    "SHADOW_TRACE_CANONICALIZATION_FAILED",
                    error_type,
                    reason_codes=tuple(trace.reason_codes),
                    failure_code="SHADOW_TRACE_CANONICALIZATION_FAILED",
                    error_type=error_type,
                ) from exc

            expected_query_ids = tuple(item.query_id for item in slice_sources)
            actual_query_ids = tuple(item.query_id for item in trace.queries)
            membership_mismatch = actual_query_ids != expected_query_ids
            if trace_validation_reasons or membership_mismatch:
                worker_reasons = list(trace_validation_reasons)
                if membership_mismatch:
                    worker_reasons.append("SHADOW_POSITION_QUERY_ID_MISMATCH")
                # Preserve the trace's exact canonical reasons in the durable
                # terminal record; derived validation reasons stay in worker
                # context and are independently reconstructable from the blob.
                self._fail_started_attempt(
                    identity=identity,
                    permit=permit,
                    failure_code="SHADOW_TRACE_FAILED",
                    reason_codes=tuple(trace.reason_codes),
                    envelope=envelope,
                )
                raise _error(
                    "SHADOW_TRACE_FAILED",
                    ";".join(worker_reasons),
                    reason_codes=tuple(trace.reason_codes),
                    failure_code="SHADOW_TRACE_FAILED",
                )
            try:
                completed = self._attempt_store.complete_attempt(
                    identity,
                    permit=permit,
                    envelope=envelope,
                    completed_at_utc=terminal_at_utc,
                )
            except ShadowAttemptStoreError as exc:
                raise self._store_error(exc) from exc
            if completed.envelope is None:
                raise _error("SHADOW_ATTEMPT_STATE_INVALID")
            envelopes.append(completed.envelope)

        return self.load_completed(sources)

    def load_completed(
        self, sources: tuple[CommittedHostObservation, ...]
    ) -> V2ShadowWindowBundle:
        """Reconstruct one window exclusively from verified COMPLETED attempts.

        This restart/reference path never invokes the physical executor.  A
        missing, failed, orphaned, or differently-bound slot fails closed.
        """

        if type(sources) is not tuple or len(sources) != WINDOW_QUERY_COUNT:
            raise _error("SHADOW_WINDOW_SOURCE_COUNT_INVALID")
        first = sources[0]
        if type(first) is not CommittedHostObservation:
            raise _error("SHADOW_WINDOW_SOURCE_INVALID")
        window_sequence = first.window_sequence
        envelopes: list[PersistedShadowTraceEnvelope] = []
        for trace_index in range(TRACE_COUNT):
            start = trace_index * TRACE_QUERY_COUNT
            expected_identity = build_shadow_attempt_identity(
                sources[start : start + TRACE_QUERY_COUNT],
                trace_sequence_index=trace_index,
            )
            try:
                record = self._attempt_store.load_slot(
                    window_sequence=window_sequence,
                    trace_sequence_index=trace_index,
                )
            except ShadowAttemptStoreError as exc:
                raise self._store_error(exc) from exc
            if (
                record is None
                or record.status is not ShadowAttemptStatus.COMPLETED
                or record.envelope is None
                or record.identity.attempt_sha256
                != expected_identity.attempt_sha256
            ):
                raise _error("SHADOW_ATTEMPT_WINDOW_NOT_COMPLETED")
            envelopes.append(record.envelope)

        # ADR-014 item 5: collapse the window-identifier namespaces so the
        # detector's provenance window ids are the v2 window sequences.
        assembled = assemble_shadow_window(
            window_id=window_sequence, envelopes=tuple(envelopes)
        )
        if not assembled.complete:
            raise _error(
                "SHADOW_WINDOW_INCOMPLETE",
                ";".join(assembled.reason_codes),
                reason_codes=assembled.reason_codes,
                failure_code="SHADOW_WINDOW_INCOMPLETE",
            )
        if len(assembled.query_records) != WINDOW_QUERY_COUNT:
            raise _error("SHADOW_WINDOW_RECORD_COUNT_INVALID")

        positions = []
        for index in range(WINDOW_QUERY_COUNT):
            source = sources[index]
            record = assembled.query_records[index]
            if record.query_id != source.query_id:
                raise _error("SHADOW_POSITION_QUERY_ID_MISMATCH")
            envelope = assembled.envelopes[index // TRACE_QUERY_COUNT]
            positions.append(
                build_v2_shadow_position(
                    source=source,
                    evaluation_eligible=True,
                    evaluation_evidence_sha256=position_evidence_sha256(
                        source=source,
                        trace_envelope_sha256=envelope.expected_trace_sha256,
                        trace_sequence_index=index // TRACE_QUERY_COUNT,
                        within_trace_index=index % TRACE_QUERY_COUNT,
                        query_id=record.query_id,
                    ),
                )
            )

        shadow_window = build_v2_shadow_window(
            sources=sources, positions=tuple(positions)
        )
        return V2ShadowWindowBundle(
            window_sequence=window_sequence,
            sources=sources,
            assembled=assembled,
            shadow_window=shadow_window,
            envelopes=tuple(envelopes),
        )
