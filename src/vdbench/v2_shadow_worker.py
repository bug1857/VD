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

from dataclasses import dataclass
from collections.abc import Callable
from typing import Protocol

from .host_window_detector_v2 import (
    V2ShadowWindow,
    build_v2_shadow_position,
    build_v2_shadow_window,
)
from .host_window_lineage import CommittedHostObservation
from .milvus_actuation import ShadowAuditTrace
from .real_detector_attestation import position_evidence_sha256
from .shadow_window import (
    TRACE_COUNT,
    TRACE_QUERY_COUNT,
    WINDOW_QUERY_COUNT,
    AssembledShadowWindow,
    PersistedShadowTraceEnvelope,
    assemble_shadow_window,
    hash_shadow_audit_trace,
)


__all__ = [
    "V2ShadowWorkerError",
    "V2ShadowCaptureExecutor",
    "V2ShadowWindowBundle",
    "V2ShadowWorker",
]


class V2ShadowWorkerError(RuntimeError):
    """Fail-closed shadow-worker error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> V2ShadowWorkerError:
    return V2ShadowWorkerError(code, message)


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
        captured_at_clock: "Callable[[], str]",
    ) -> None:
        """`captured_at_clock` is read once per 50-query trace.

        The window assembler requires strictly increasing envelope capture
        timestamps, which is also what real sequential capture produces: each
        trace is captured after the one before it. A single shared timestamp
        would therefore be rejected, so this port is a clock, not a constant.
        """

        if not callable(getattr(capture_executor, "capture", None)):
            raise TypeError("capture_executor must provide capture")
        if not callable(captured_at_clock):
            raise TypeError("captured_at_clock must be callable")
        self._executor = capture_executor
        self._clock = captured_at_clock

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
        traces: list[ShadowAuditTrace] = []
        for trace_index in range(TRACE_COUNT):
            start = trace_index * TRACE_QUERY_COUNT
            slice_sources = sources[start : start + TRACE_QUERY_COUNT]
            trace = self._executor.capture(
                slice_sources, trace_sequence_index=trace_index
            )
            if type(trace) is not ShadowAuditTrace:
                raise _error("SHADOW_CAPTURE_INVALID")
            if len(trace.queries) != TRACE_QUERY_COUNT:
                raise _error("SHADOW_TRACE_QUERY_COUNT_INVALID")
            traces.append(trace)
            envelopes.append(
                PersistedShadowTraceEnvelope(
                    trace_id=f"v2-window-{window_sequence}-trace-{trace_index}",
                    captured_at_utc=self._clock(),
                    sequence_index=trace_index,
                    declared_observation_count=TRACE_QUERY_COUNT,
                    expected_trace_sha256=hash_shadow_audit_trace(trace),
                    trace=trace,
                )
            )

        # ADR-014 item 5: collapse the window-identifier namespaces so the
        # detector's provenance window ids are the v2 window sequences.
        assembled = assemble_shadow_window(
            window_id=window_sequence, envelopes=tuple(envelopes)
        )
        if not assembled.complete:
            raise _error("SHADOW_WINDOW_INCOMPLETE", ";".join(assembled.reason_codes))
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
