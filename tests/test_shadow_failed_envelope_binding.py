"""FINDING-007: a FAILED attempt's envelope must belong to that attempt.

`fail_attempt` accepts an optional trace envelope as forensic evidence for a
capture that did not succeed.  It previously validated only the envelope's
`sequence_index`, `declared_observation_count`, and reason codes -- none of
which distinguish window 0 trace 0 from window 1 trace 0.  An envelope captured
for a completely different window was therefore attachable to this attempt as
its failure evidence, and the durable forensic record would describe physical
work that never belonged to it.

Impact is forensic, not authority: a FAILED attempt never becomes a COMPLETED
one and never becomes window authority.  Those two properties are re-proven
here so the fix cannot be mistaken for a change in terminal semantics.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from vdbench.shadow_attempt_store import (
    ShadowAttemptStatus,
    ShadowAttemptStoreError,
    SQLiteShadowAttemptStore,
    build_shadow_attempt_identity,
    expected_shadow_trace_id,
)
from vdbench.shadow_window import (
    TRACE_QUERY_COUNT,
    WINDOW_QUERY_COUNT,
    PersistedShadowTraceEnvelope,
    hash_shadow_audit_trace,
)
from vdbench.v2_shadow_worker import V2ShadowWorker, V2ShadowWorkerError
import vdbench.shadow_attempt_store as shadow_attempt_store

from tests.test_real_detector_attestation import (
    _ENVIRONMENT,
    _REVISION,
    _commit_sources,
    _stream,
    _trace_for,
)


class _Clock:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self) -> str:
        self.value += 1
        return f"2026-08-15T00:00:{self.value:02d}Z"


def _store(path: Path) -> SQLiteShadowAttemptStore:
    return SQLiteShadowAttemptStore(
        path,
        stream_key=_stream(),
        source_revision=_REVISION,
        environment_manifest_sha256=_ENVIRONMENT,
    )


def _envelope_for(sources, *, window_sequence: int, trace_index: int, timestamp: str):
    """Build the envelope the worker would build for that exact slot."""

    trace = _trace_for(sources)
    return PersistedShadowTraceEnvelope(
        trace_id=expected_shadow_trace_id(
            window_sequence=window_sequence, trace_sequence_index=trace_index
        ),
        captured_at_utc=timestamp,
        sequence_index=trace_index,
        declared_observation_count=TRACE_QUERY_COUNT,
        expected_trace_sha256=hash_shadow_audit_trace(trace),
        trace=trace,
    )


class _TwoWindowFixture:
    """Two genuinely distinct attempt identities from real committed sources."""

    def __init__(self, root: Path) -> None:
        self.sources = tuple(
            _commit_sources(root / "source.sqlite3", WINDOW_QUERY_COUNT * 2)
        )
        assert len(self.sources) == WINDOW_QUERY_COUNT * 2
        self.window0 = self.sources[:TRACE_QUERY_COUNT]
        self.window1 = self.sources[
            WINDOW_QUERY_COUNT : WINDOW_QUERY_COUNT + TRACE_QUERY_COUNT
        ]
        self.identity0 = build_shadow_attempt_identity(
            self.window0, trace_sequence_index=0
        )
        self.identity1 = build_shadow_attempt_identity(
            self.window1, trace_sequence_index=0
        )
        assert self.identity0.window_sequence == 0
        assert self.identity1.window_sequence == 1
        assert self.identity0.attempt_sha256 != self.identity1.attempt_sha256
        # Same trace index in both windows: exactly the case the old check
        # could not tell apart.
        assert (
            self.identity0.trace_sequence_index
            == self.identity1.trace_sequence_index
            == 0
        )


class ForeignFailedEnvelopeTests(unittest.TestCase):
    def test_envelope_from_another_window_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = _TwoWindowFixture(root)
            clock = _Clock()
            foreign = _envelope_for(
                fixture.window1,
                window_sequence=1,
                trace_index=0,
                timestamp=clock(),
            )
            with _store(root / "attempts.sqlite3") as store:
                permit = store.start_attempt(
                    fixture.identity0, started_at_utc=clock()
                )
                with self.assertRaises(ShadowAttemptStoreError) as caught:
                    store.fail_attempt(
                        fixture.identity0,
                        permit=permit,
                        failed_at_utc=clock(),
                        failure_code="SHADOW_CAPTURE_EXCEPTION",
                        envelope=foreign,
                    )
                self.assertEqual(
                    caught.exception.code, "SHADOW_ATTEMPT_TRACE_BINDING_INVALID"
                )
                # The foreign envelope never became durable evidence.
                record = store.load_slot(window_sequence=0, trace_sequence_index=0)
                self.assertIsNotNone(record)
                self.assertIsNone(record.envelope)
                # The attempt is now execution-ambiguous, never retryable.
                self.assertIs(record.status, ShadowAttemptStatus.ORPHANED)
                self.assertEqual(
                    record.reason_codes, ("EXECUTION_OUTCOME_UNKNOWN",)
                )

    def test_matching_envelope_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = _TwoWindowFixture(root)
            clock = _Clock()
            own = _envelope_for(
                fixture.window0,
                window_sequence=0,
                trace_index=0,
                timestamp=clock(),
            )
            with _store(root / "attempts.sqlite3") as store:
                permit = store.start_attempt(
                    fixture.identity0, started_at_utc=clock()
                )
                failed = store.fail_attempt(
                    fixture.identity0,
                    permit=permit,
                    failed_at_utc=clock(),
                    failure_code="SHADOW_TRACE_FAILED",
                    envelope=own,
                )
                self.assertIs(failed.status, ShadowAttemptStatus.FAILED)
                self.assertIsNotNone(failed.envelope)
                self.assertEqual(failed.envelope.trace_id, "v2-window-0-trace-0")
                self.assertEqual(failed.failure_code, "SHADOW_TRACE_FAILED")

    def test_envelope_with_wrong_trace_index_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = _TwoWindowFixture(root)
            clock = _Clock()
            # Correct window, wrong slot: trace_id and sequence_index both move.
            mismatched = _envelope_for(
                fixture.window0,
                window_sequence=0,
                trace_index=1,
                timestamp=clock(),
            )
            with _store(root / "attempts.sqlite3") as store:
                permit = store.start_attempt(
                    fixture.identity0, started_at_utc=clock()
                )
                with self.assertRaises(ShadowAttemptStoreError):
                    store.fail_attempt(
                        fixture.identity0,
                        permit=permit,
                        failed_at_utc=clock(),
                        failure_code="SHADOW_CAPTURE_EXCEPTION",
                        envelope=mismatched,
                    )

    def test_read_path_rejects_a_cross_bound_envelope(self) -> None:
        """The same invariant is enforced when records are reconstructed."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = _TwoWindowFixture(root)
            foreign = _envelope_for(
                fixture.window1,
                window_sequence=1,
                trace_index=0,
                timestamp="2026-08-15T00:00:09Z",
            )
            with self.assertRaises(ShadowAttemptStoreError) as caught:
                shadow_attempt_store._verify_terminal_envelope_binding(
                    fixture.identity0, foreign
                )
            self.assertEqual(
                caught.exception.code, "SHADOW_ATTEMPT_TRACE_BINDING_INVALID"
            )
            # ... and accepts the attempt's own envelope.
            own = _envelope_for(
                fixture.window0,
                window_sequence=0,
                trace_index=0,
                timestamp="2026-08-15T00:00:09Z",
            )
            shadow_attempt_store._verify_terminal_envelope_binding(
                fixture.identity0, own
            )


class FailedTerminalSemanticsUnchangedTests(unittest.TestCase):
    def test_failed_never_becomes_completed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = _TwoWindowFixture(root)
            clock = _Clock()
            own = _envelope_for(
                fixture.window0, window_sequence=0, trace_index=0, timestamp=clock()
            )
            with _store(root / "attempts.sqlite3") as store:
                permit = store.start_attempt(
                    fixture.identity0, started_at_utc=clock()
                )
                store.fail_attempt(
                    fixture.identity0,
                    permit=permit,
                    failed_at_utc=clock(),
                    failure_code="SHADOW_TRACE_FAILED",
                    envelope=own,
                )
                with self.assertRaises(ShadowAttemptStoreError):
                    store.complete_attempt(
                        fixture.identity0,
                        permit=permit,
                        envelope=own,
                        completed_at_utc=clock(),
                    )
                self.assertIs(
                    store.load_slot(
                        window_sequence=0, trace_sequence_index=0
                    ).status,
                    ShadowAttemptStatus.FAILED,
                )

    def test_failed_attempt_never_becomes_window_authority(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = _TwoWindowFixture(root)
            clock = _Clock()
            own = _envelope_for(
                fixture.window0, window_sequence=0, trace_index=0, timestamp=clock()
            )
            window_sources = fixture.sources[:WINDOW_QUERY_COUNT]
            with _store(root / "attempts.sqlite3") as store:
                permit = store.start_attempt(
                    fixture.identity0, started_at_utc=clock()
                )
                store.fail_attempt(
                    fixture.identity0,
                    permit=permit,
                    failed_at_utc=clock(),
                    failure_code="SHADOW_TRACE_FAILED",
                    envelope=own,
                )

                class _NeverCalled:
                    def capture(self, sources, *, trace_sequence_index):
                        raise AssertionError("no physical capture may occur")

                worker = V2ShadowWorker(
                    capture_executor=_NeverCalled(),
                    captured_at_clock=clock,
                    attempt_store=store,
                )
                with self.assertRaises(V2ShadowWorkerError) as caught:
                    worker.build(window_sources)
                self.assertEqual(
                    caught.exception.code, "SHADOW_ATTEMPT_PREVIOUSLY_FAILED"
                )
                with self.assertRaises(V2ShadowWorkerError) as caught:
                    worker.load_completed(window_sources)
                self.assertEqual(
                    caught.exception.code, "SHADOW_ATTEMPT_WINDOW_NOT_COMPLETED"
                )


class TraceIdHelperTests(unittest.TestCase):
    def test_helper_is_the_single_definition_of_the_slot_id(self) -> None:
        self.assertEqual(
            expected_shadow_trace_id(window_sequence=7, trace_sequence_index=2),
            "v2-window-7-trace-2",
        )

    def test_distinct_slots_never_share_a_trace_id(self) -> None:
        seen = {
            expected_shadow_trace_id(
                window_sequence=window, trace_sequence_index=index
            )
            for window in range(4)
            for index in range(4)
        }
        self.assertEqual(len(seen), 16)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
