"""Offline adversarial coverage for ADR-015 Gate-C shadow hardening."""

from __future__ import annotations

import ast
import inspect
import os
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from tests.test_real_detector_attestation import (
    _ENVIRONMENT,
    _REVISION,
    _commit_sources,
    _stream,
    _trace_for,
)
from vdbench import milvus_actuation, shadow_attempt_store, shadow_window
from vdbench.config import Metric
from vdbench.flat_oracle_agreement import (
    FlatOracleAgreementKind,
    compare_flat_oracle_hits,
)
from vdbench.milvus import SearchHit
from vdbench.oracle import OracleHit, OracleResult
from vdbench.shadow_attempt_store import (
    ShadowAttemptStatus,
    ShadowAttemptStoreError,
    SQLiteShadowAttemptStore,
    build_shadow_attempt_identity,
)
from vdbench.shadow_window import (
    TRACE_QUERY_COUNT,
    WINDOW_QUERY_COUNT,
    PersistedShadowTraceEnvelope,
    assemble_shadow_window,
    hash_shadow_audit_trace,
)
from vdbench.v2_shadow_worker import V2ShadowWorker, V2ShadowWorkerError


class _Clock:
    def __init__(self, start: int = 0) -> None:
        self.value = start

    def __call__(self) -> str:
        self.value += 1
        return f"2026-08-14T00:00:{self.value:02d}Z"


class _Executor:
    def __init__(self, callback=None) -> None:
        self.calls: list[int] = []
        self.callback = callback

    def capture(self, sources, *, trace_sequence_index: int):
        self.calls.append(trace_sequence_index)
        if self.callback is not None:
            return self.callback(sources, trace_sequence_index)
        return _trace_for(sources)


def _store(path: Path) -> SQLiteShadowAttemptStore:
    return SQLiteShadowAttemptStore(
        path,
        stream_key=_stream(),
        source_revision=_REVISION,
        environment_manifest_sha256=_ENVIRONMENT,
    )


def _envelope(sources, trace_index: int, timestamp: str) -> PersistedShadowTraceEnvelope:
    trace = _trace_for(sources)
    return PersistedShadowTraceEnvelope(
        trace_id=f"v2-window-0-trace-{trace_index}",
        captured_at_utc=timestamp,
        sequence_index=trace_index,
        declared_observation_count=TRACE_QUERY_COUNT,
        expected_trace_sha256=hash_shadow_audit_trace(trace),
        trace=trace,
    )



# --- Objective 3: real fork, run in a fresh single-threaded interpreter -------
#
# These scenarios exercise genuine `os.fork()` semantics that `spawn` cannot
# reproduce: a child sharing the parent's open file description, and a child
# inheriting a live in-memory permit object. CPython emits a
# DeprecationWarning when `os.fork()` runs in a process its runtime considers
# multi-threaded, and the full suite reaches that state even though
# `threading.enumerate()` shows only MainThread. Running each scenario in a
# fresh interpreter keeps the fork real while making the process provably
# single-threaded, so the warning is eliminated by construction rather than
# filtered. Every assertion below is the original one, executed where the fork
# actually happens; a failure raises and the non-zero exit is asserted by the
# parent test.


def _scenario_forked_child_close() -> None:
    """A forked child's close() must not release the parent's flock."""

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "attempts.sqlite3"
        store = _store(path)
        pid = os.fork()
        if pid == 0:
            try:
                store.close()
            finally:
                os._exit(0)
        os.waitpid(pid, 0)
        script = (
            "from pathlib import Path; "
            "from tests.test_shadow_attempt_store import _store; "
            f"p=Path({str(path)!r}); "
            "\ntry:\n s=_store(p)\nexcept Exception as e:\n"
            " print(getattr(e,'code',type(e).__name__))\nelse:\n"
            " s.close(); print('ACQUIRED')"
        )
        blocked = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).parents[1],
            env={**os.environ, "PYTHONPATH": "src"},
            text=True,
            capture_output=True,
            check=True,
        )
        assert blocked.stdout.strip() == "SHADOW_ATTEMPT_STORE_BUSY", blocked.stdout
        store.close()
        acquired = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).parents[1],
            env={**os.environ, "PYTHONPATH": "src"},
            text=True,
            capture_output=True,
            check=True,
        )
        assert acquired.stdout.strip() == "ACQUIRED", acquired.stdout


def _scenario_inherited_permit_refused() -> None:
    """A forked child may never terminalize with an inherited permit."""

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        sources = tuple(_commit_sources(root / "source.sqlite3", WINDOW_QUERY_COUNT))
        identity = build_shadow_attempt_identity(
            sources[:TRACE_QUERY_COUNT], trace_sequence_index=0
        )
        with _store(root / "attempts.sqlite3") as store:
            permit = store.start_attempt(
                identity, started_at_utc="2026-08-14T00:00:01Z"
            )
            read_fd, write_fd = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(read_fd)
                try:
                    store.complete_attempt(
                        identity,
                        permit=permit,
                        envelope=_envelope(
                            sources[:TRACE_QUERY_COUNT], 0, "2026-08-14T00:00:02Z"
                        ),
                        completed_at_utc="2026-08-14T00:00:02Z",
                    )
                except Exception as exc:  # child reports exact refusal  # noqa: BLE001
                    outcome = getattr(exc, "code", type(exc).__name__)
                else:
                    outcome = "TERMINALIZED"
                os.write(write_fd, outcome.encode("ascii"))
                os.close(write_fd)
                os._exit(0)
            os.close(write_fd)
            outcome = os.read(read_fd, 256).decode("ascii")
            os.close(read_fd)
            _, status = os.waitpid(pid, 0)
            assert status == 0, status
            assert outcome == "SHADOW_ATTEMPT_PERMIT_INVALID", outcome
            store.complete_attempt(
                identity,
                permit=permit,
                envelope=_envelope(
                    sources[:TRACE_QUERY_COUNT], 0, "2026-08-14T00:00:03Z"
                ),
                completed_at_utc="2026-08-14T00:00:03Z",
            )


def _run_fork_scenario(case: unittest.TestCase, scenario: str) -> None:
    """Execute one fork scenario in a fresh interpreter and assert it passed."""

    completed = subprocess.run(
        [
            sys.executable,
            "-W",
            "error::DeprecationWarning",
            "-c",
            f"from tests.test_shadow_attempt_store import {scenario}; {scenario}()",
        ],
        cwd=Path(__file__).parents[1],
        env={**os.environ, "PYTHONPATH": "src", "PYTHONDONTWRITEBYTECODE": "1"},
        text=True,
        capture_output=True,
        check=False,
    )
    case.assertEqual(
        completed.returncode, 0, f"{scenario} failed:\n{completed.stderr}"
    )
    case.assertNotIn("DeprecationWarning", completed.stderr)

class ShadowAttemptLifecycleTests(unittest.TestCase):
    def _sources(self, root: Path):
        return tuple(
            _commit_sources(root / "source.sqlite3", WINDOW_QUERY_COUNT)
        )

    def test_store_has_no_service_detector_or_authority_dependency(self) -> None:
        tree = ast.parse(inspect.getsource(shadow_attempt_store))
        imported = {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        imported.update(
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        )
        forbidden = {
            "pymilvus", "policy", "actuation", "canary_admission",
            "host_window_detector_v2", "real_detector_attestation",
        }
        self.assertFalse(
            any(name.split(".")[-1] in forbidden for name in imported), imported
        )

    def test_incomplete_trace_zero_is_persisted_and_stops_later_capture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)
            exact_reasons = (
                "TRACE_INCOMPLETE",
                "STAGE_FAILED:query=7:FLAT",
                "TIMEOUT:query=7:FLAT",
            )

            def callback(slice_sources, trace_index):
                return replace(
                    _trace_for(slice_sources),
                    complete=False,
                    reason_codes=exact_reasons,
                )

            executor = _Executor(callback)
            with _store(root / "attempts.sqlite3") as store:
                worker = V2ShadowWorker(
                    capture_executor=executor,
                    captured_at_clock=_Clock(),
                    attempt_store=store,
                )
                with self.assertRaises(V2ShadowWorkerError) as raised:
                    worker.build(sources)
                record = store.load_slot(window_sequence=0, trace_sequence_index=0)
                self.assertEqual(raised.exception.code, "SHADOW_TRACE_FAILED")
                self.assertEqual(raised.exception.reason_codes, exact_reasons)
                self.assertEqual(record.status, ShadowAttemptStatus.FAILED)
                self.assertEqual(record.reason_codes, exact_reasons)
                self.assertEqual(record.envelope.trace.reason_codes, exact_reasons)
                self.assertEqual(executor.calls, [0])
                self.assertIsNone(store.load_slot(window_sequence=0, trace_sequence_index=1))

    def test_started_is_durable_before_executor_and_prior_trace_is_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)
            observed: list[tuple[int, ShadowAttemptStatus]] = []
            with _store(root / "attempts.sqlite3") as store:
                def callback(slice_sources, trace_index):
                    current = store.load_slot(
                        window_sequence=0, trace_sequence_index=trace_index
                    )
                    observed.append((trace_index, current.status))
                    if trace_index:
                        previous = store.load_slot(
                            window_sequence=0,
                            trace_sequence_index=trace_index - 1,
                        )
                        self.assertEqual(previous.status, ShadowAttemptStatus.COMPLETED)
                    return _trace_for(slice_sources)

                bundle = V2ShadowWorker(
                    capture_executor=_Executor(callback),
                    captured_at_clock=_Clock(),
                    attempt_store=store,
                ).build(sources)
                self.assertTrue(bundle.assembled.complete)
            self.assertEqual(
                observed,
                [(index, ShadowAttemptStatus.STARTED) for index in range(4)],
            )

    def test_incomplete_middle_trace_preserves_earlier_and_stops_later(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)

            def callback(slice_sources, trace_index):
                trace = _trace_for(slice_sources)
                return (
                    replace(trace, complete=False, reason_codes=("IDENTITY_FAILED",))
                    if trace_index == 2
                    else trace
                )

            executor = _Executor(callback)
            with _store(root / "attempts.sqlite3") as store:
                worker = V2ShadowWorker(
                    capture_executor=executor,
                    captured_at_clock=_Clock(),
                    attempt_store=store,
                )
                with self.assertRaises(V2ShadowWorkerError):
                    worker.build(sources)
                self.assertEqual(executor.calls, [0, 1, 2])
                self.assertEqual(
                    [item.status for item in store.records_for_window(0)],
                    [
                        ShadowAttemptStatus.COMPLETED,
                        ShadowAttemptStatus.COMPLETED,
                        ShadowAttemptStatus.FAILED,
                    ],
                )
                self.assertIsNone(store.load_slot(window_sequence=0, trace_sequence_index=3))

    def test_complete_trace_with_wrong_source_membership_is_failed_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)

            def callback(slice_sources, trace_index):
                if trace_index == 1:
                    return _trace_for(sources[:TRACE_QUERY_COUNT])
                return _trace_for(slice_sources)

            executor = _Executor(callback)
            with _store(root / "attempts.sqlite3") as store:
                worker = V2ShadowWorker(
                    capture_executor=executor,
                    captured_at_clock=_Clock(),
                    attempt_store=store,
                )
                with self.assertRaises(V2ShadowWorkerError) as raised:
                    worker.build(sources)
                self.assertEqual(raised.exception.code, "SHADOW_TRACE_FAILED")
                self.assertIn("SHADOW_POSITION_QUERY_ID_MISMATCH", str(raised.exception))
                self.assertEqual(executor.calls, [0, 1])
                self.assertEqual(
                    store.load_slot(window_sequence=0, trace_sequence_index=1).status,
                    ShadowAttemptStatus.FAILED,
                )
                self.assertIsNone(store.load_slot(window_sequence=0, trace_sequence_index=2))

    def test_capture_exception_is_terminal_without_fabricated_trace(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)

            def callback(_sources, _trace_index):
                raise TimeoutError("injected")

            executor = _Executor(callback)
            with _store(root / "attempts.sqlite3") as store:
                worker = V2ShadowWorker(
                    capture_executor=executor,
                    captured_at_clock=_Clock(),
                    attempt_store=store,
                )
                with self.assertRaises(V2ShadowWorkerError) as raised:
                    worker.build(sources)
                record = store.load_slot(window_sequence=0, trace_sequence_index=0)
                self.assertEqual(raised.exception.code, "SHADOW_CAPTURE_EXCEPTION")
                self.assertEqual(record.status, ShadowAttemptStatus.FAILED)
                self.assertEqual(record.reason_codes, ("EXECUTION_OUTCOME_UNKNOWN",))
                self.assertEqual(record.failure_code, "SHADOW_CAPTURE_EXCEPTION")
                self.assertEqual(record.error_type, "TimeoutError")
                self.assertIsNone(record.envelope)
                self.assertEqual(executor.calls, [0])

    def test_orphan_started_is_non_retriable_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)
            identity = build_shadow_attempt_identity(
                sources[:TRACE_QUERY_COUNT], trace_sequence_index=0
            )
            with _store(root / "attempts.sqlite3") as store:
                store.start_attempt(identity, started_at_utc="2026-08-14T00:00:01Z")
            executor = _Executor()
            with _store(root / "attempts.sqlite3") as reopened:
                worker = V2ShadowWorker(
                    capture_executor=executor,
                    captured_at_clock=_Clock(10),
                    attempt_store=reopened,
                )
                with self.assertRaises(V2ShadowWorkerError) as raised:
                    worker.build(sources)
                self.assertEqual(raised.exception.code, "SHADOW_ATTEMPT_ORPHANED")
                self.assertEqual(executor.calls, [])
                self.assertEqual(
                    reopened.load_slot(window_sequence=0, trace_sequence_index=0).status,
                    ShadowAttemptStatus.ORPHANED,
                )
                with self.assertRaises(ShadowAttemptStoreError) as terminal:
                    reopened.complete_attempt(
                        identity,
                        permit=object(),
                        envelope=_envelope(
                            sources[:TRACE_QUERY_COUNT],
                            0,
                            "2026-08-14T00:00:20Z",
                        ),
                        completed_at_utc="2026-08-14T00:00:20Z",
                    )
                self.assertEqual(terminal.exception.code, "SHADOW_ATTEMPT_ORPHANED")

    def test_completed_trace_is_reused_after_restart_without_search(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)
            first_slice = sources[:TRACE_QUERY_COUNT]
            identity = build_shadow_attempt_identity(first_slice, trace_sequence_index=0)
            with _store(root / "attempts.sqlite3") as store:
                permit = store.start_attempt(
                    identity, started_at_utc="2026-08-14T00:00:01Z"
                )
                store.complete_attempt(
                    identity,
                    permit=permit,
                    envelope=_envelope(first_slice, 0, "2026-08-14T00:00:02Z"),
                    completed_at_utc="2026-08-14T00:00:02Z",
                )
            executor = _Executor()
            with _store(root / "attempts.sqlite3") as reopened:
                bundle = V2ShadowWorker(
                    capture_executor=executor,
                    captured_at_clock=_Clock(10),
                    attempt_store=reopened,
                ).build(sources)
                self.assertTrue(bundle.assembled.complete)
                self.assertEqual(executor.calls, [1, 2, 3])

    def test_four_persisted_completed_traces_assemble_without_executor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)
            with _store(root / "attempts.sqlite3") as store:
                for trace_index in range(4):
                    start = trace_index * TRACE_QUERY_COUNT
                    slice_sources = sources[start : start + TRACE_QUERY_COUNT]
                    identity = build_shadow_attempt_identity(
                        slice_sources, trace_sequence_index=trace_index
                    )
                    permit = store.start_attempt(
                        identity,
                        started_at_utc=f"2026-08-14T00:00:{trace_index * 2 + 1:02d}Z",
                    )
                    store.complete_attempt(
                        identity,
                        permit=permit,
                        envelope=_envelope(
                            slice_sources,
                            trace_index,
                            f"2026-08-14T00:00:{trace_index * 2 + 2:02d}Z",
                        ),
                        completed_at_utc=f"2026-08-14T00:00:{trace_index * 2 + 2:02d}Z",
                    )
            executor = _Executor(lambda *_args: self.fail("executor must not run"))
            with _store(root / "attempts.sqlite3") as reopened:
                bundle = V2ShadowWorker(
                    capture_executor=executor,
                    captured_at_clock=_Clock(20),
                    attempt_store=reopened,
                ).build(sources)
                self.assertTrue(bundle.assembled.complete)
                self.assertEqual(len(bundle.envelopes), 4)
                self.assertEqual(executor.calls, [])

    def test_trace_blob_hash_corruption_fails_closed_on_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)
            identity = build_shadow_attempt_identity(
                sources[:TRACE_QUERY_COUNT], trace_sequence_index=0
            )
            path = root / "attempts.sqlite3"
            with _store(path) as store:
                permit = store.start_attempt(
                    identity, started_at_utc="2026-08-14T00:00:01Z"
                )
                store.complete_attempt(
                    identity,
                    permit=permit,
                    envelope=_envelope(sources[:TRACE_QUERY_COUNT], 0, "2026-08-14T00:00:02Z"),
                    completed_at_utc="2026-08-14T00:00:02Z",
                )
            connection = sqlite3.connect(path)
            trigger_sql = connection.execute(
                "SELECT sql FROM sqlite_schema WHERE name='attempt_events_no_update'"
            ).fetchone()[0]
            connection.execute("DROP TRIGGER attempt_events_no_update")
            connection.execute(
                "UPDATE attempt_events SET trace_envelope_json=? WHERE event_kind='COMPLETED'",
                (b"{}\n",),
            )
            connection.execute(trigger_sql)
            connection.commit()
            connection.close()
            with self.assertRaises(ShadowAttemptStoreError) as raised:
                _store(path)
            self.assertEqual(raised.exception.code, "SHADOW_ATTEMPT_TRACE_INVALID")

    def test_store_binding_mismatch_and_forged_identity_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)
            path = root / "attempts.sqlite3"
            with _store(path):
                pass
            with self.assertRaises(ShadowAttemptStoreError) as raised:
                SQLiteShadowAttemptStore(
                    path,
                    stream_key=_stream(),
                    source_revision=_REVISION,
                    environment_manifest_sha256="f" * 64,
                )
            self.assertEqual(raised.exception.code, "SHADOW_ATTEMPT_BINDING_MISMATCH")
            identity = build_shadow_attempt_identity(
                sources[:TRACE_QUERY_COUNT], trace_sequence_index=0
            )
            object.__setattr__(identity, "environment_manifest_sha256", "f" * 64)
            with (
                _store(path) as store,
                self.assertRaises(ShadowAttemptStoreError),
            ):
                store.start_attempt(identity, started_at_utc="2026-08-14T00:00:01Z")

    def test_second_writer_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "attempts.sqlite3"
            with _store(path):
                with self.assertRaises(ShadowAttemptStoreError) as raised:
                    _store(path)
                self.assertEqual(raised.exception.code, "SHADOW_ATTEMPT_STORE_BUSY")

    @unittest.skipUnless(hasattr(os, "fork"), "requires POSIX fork")
    def test_forked_child_close_cannot_release_parent_lock(self) -> None:
        _run_fork_scenario(self, "_scenario_forked_child_close")

    def test_terminal_permit_is_private_one_shot_and_thread_bound_by_possession(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)
            identity = build_shadow_attempt_identity(
                sources[:TRACE_QUERY_COUNT], trace_sequence_index=0
            )
            with _store(root / "attempts.sqlite3") as store:
                permit = store.start_attempt(
                    identity, started_at_utc="2026-08-14T00:00:01Z"
                )
                errors = []

                def terminalize_without_permit() -> None:
                    try:
                        store.complete_attempt(
                            identity,
                            permit=object(),
                            envelope=_envelope(
                                sources[:TRACE_QUERY_COUNT], 0,
                                "2026-08-14T00:00:02Z",
                            ),
                            completed_at_utc="2026-08-14T00:00:02Z",
                        )
                    except Exception as exc:  # test captures exact boundary  # noqa: BLE001
                        errors.append(exc)

                thread = threading.Thread(target=terminalize_without_permit)
                thread.start()
                thread.join()
                self.assertEqual(len(errors), 1)
                self.assertEqual(errors[0].code, "SHADOW_ATTEMPT_PERMIT_INVALID")
                store.complete_attempt(
                    identity,
                    permit=permit,
                    envelope=_envelope(
                        sources[:TRACE_QUERY_COUNT], 0, "2026-08-14T00:00:03Z"
                    ),
                    completed_at_utc="2026-08-14T00:00:03Z",
                )
                with self.assertRaises(ShadowAttemptStoreError) as reused:
                    store.fail_attempt(
                        identity,
                        permit=permit,
                        failed_at_utc="2026-08-14T00:00:04Z",
                        failure_code="REUSE",
                    )
                self.assertEqual(reused.exception.code, "SHADOW_ATTEMPT_PERMIT_INVALID")

    @unittest.skipUnless(hasattr(os, "fork"), "requires POSIX fork")
    def test_forked_child_cannot_use_inherited_terminal_permit(self) -> None:
        _run_fork_scenario(self, "_scenario_inherited_permit_refused")

    def test_configuration_and_data_store_binding_mismatches_fail_closed(self) -> None:
        from vdbench.shadow_event_types import MonitorStreamKey

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "attempts.sqlite3"
            with _store(path):
                pass
            original = _stream()
            for field, replacement in (
                ("configuration_identity", "other-config"),
                ("data_identity", "other-data"),
            ):
                values = {
                    name: getattr(original, name)
                    for name in original.__dataclass_fields__
                }
                values[field] = replacement
                with self.subTest(field=field), self.assertRaises(
                    ShadowAttemptStoreError
                ) as raised:
                    SQLiteShadowAttemptStore(
                        path,
                        stream_key=MonitorStreamKey(**values),
                        source_revision=_REVISION,
                        environment_manifest_sha256=_ENVIRONMENT,
                    )
                self.assertEqual(
                    raised.exception.code, "SHADOW_ATTEMPT_BINDING_MISMATCH"
                )

    def test_illegal_lifecycle_transitions_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)
            identity = build_shadow_attempt_identity(
                sources[:TRACE_QUERY_COUNT], trace_sequence_index=0
            )
            with _store(root / "attempts.sqlite3") as store:
                permit = store.start_attempt(
                    identity, started_at_utc="2026-08-14T00:00:01Z"
                )
                with self.assertRaises(ShadowAttemptStoreError):
                    store.start_attempt(identity, started_at_utc="2026-08-14T00:00:02Z")
                store.complete_attempt(
                    identity,
                    permit=permit,
                    envelope=_envelope(sources[:TRACE_QUERY_COUNT], 0, "2026-08-14T00:00:03Z"),
                    completed_at_utc="2026-08-14T00:00:03Z",
                )
                with self.assertRaises(ShadowAttemptStoreError):
                    store.fail_attempt(
                        identity,
                        permit=permit,
                        failed_at_utc="2026-08-14T00:00:04Z",
                        failure_code="LATE_FAILURE",
                    )

    def test_completed_transition_requires_exact_trace_attempt_binding(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = self._sources(root)
            identity = build_shadow_attempt_identity(
                sources[:TRACE_QUERY_COUNT], trace_sequence_index=0
            )
            wrong_trace = _trace_for(sources[TRACE_QUERY_COUNT : 2 * TRACE_QUERY_COUNT])
            wrong_envelope = PersistedShadowTraceEnvelope(
                trace_id="v2-window-0-trace-0",
                captured_at_utc="2026-08-14T00:00:02Z",
                sequence_index=0,
                declared_observation_count=TRACE_QUERY_COUNT,
                expected_trace_sha256=hash_shadow_audit_trace(wrong_trace),
                trace=wrong_trace,
            )
            with _store(root / "attempts.sqlite3") as store:
                permit = store.start_attempt(
                    identity, started_at_utc="2026-08-14T00:00:01Z"
                )
                with self.assertRaises(ShadowAttemptStoreError) as raised:
                    store.complete_attempt(
                        identity,
                        permit=permit,
                        envelope=wrong_envelope,
                        completed_at_utc="2026-08-14T00:00:02Z",
                    )
                self.assertEqual(
                    raised.exception.code, "SHADOW_ATTEMPT_TRACE_BINDING_INVALID"
                )
                self.assertEqual(
                    store.load_slot(window_sequence=0, trace_sequence_index=0).status,
                    ShadowAttemptStatus.ORPHANED,
                )


class FlatOracleAgreementTests(unittest.TestCase):
    @staticmethod
    def _compare(flat, oracle, *, radius=10.0, limit=100):
        return compare_flat_oracle_hits(
            flat_hits=flat,
            oracle_result=oracle,
            metric=Metric.L2,
            radius=radius,
            range_filter=0.0,
            limit=limit,
        )

    def test_exact_ordered_match_passes(self) -> None:
        oracle = OracleResult(
            (OracleHit(1, 1.0), OracleHit(2, 2.0)), 2, False
        )
        result = self._compare((SearchHit(1, 1.0), SearchHit(2, 2.0)), oracle)
        self.assertEqual(result.kind, FlatOracleAgreementKind.EXACT_ORDERED)
        self.assertTrue(result.agrees)

    def test_reorder_across_distinguishable_scores_fails(self) -> None:
        oracle = OracleResult(
            (OracleHit(1, 1.0), OracleHit(2, 2.0)), 2, False
        )
        result = self._compare((SearchHit(2, 1.0), SearchHit(1, 2.0)), oracle)
        self.assertEqual(result.kind, FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH)
        self.assertFalse(result.agrees)

    def test_reorder_inside_binary32_equivalent_tie_passes(self) -> None:
        first = 1.00000001
        second = 1.00000002
        self.assertEqual(struct.pack("<f", first), struct.pack("<f", second))
        oracle = OracleResult((OracleHit(1, first), OracleHit(2, second)), 2, False)
        result = self._compare((SearchHit(2, 1.0), SearchHit(1, 1.0)), oracle)
        self.assertEqual(result.kind, FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT)
        self.assertTrue(result.agrees)

    def test_source_372_precision_shape_regression(self) -> None:
        # Regression shape from the V3 forensic finding: distinct binary64 L2
        # distances collapse to one governed binary32 score.
        first = 17.523456497
        second = 17.523456512
        self.assertNotEqual(first, second)
        self.assertEqual(struct.pack("<f", first), struct.pack("<f", second))
        oracle = OracleResult((OracleHit(37201, first), OracleHit(37202, second)), 2, False)
        result = self._compare(
            (SearchHit(37202, float(second)), SearchHit(37201, float(first))),
            oracle,
            radius=20.0,
        )
        self.assertEqual(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)
        self.assertEqual(result.reason_codes, ("FLAT_SCORE_ORDER_INVALID",))

    def test_raw_l2_score_inversion_inside_tie_group_fails(self) -> None:
        first = 1.00000001
        second = 1.00000002
        self.assertEqual(struct.pack("<f", first), struct.pack("<f", second))
        oracle = OracleResult((OracleHit(1, first), OracleHit(2, second)), 2, False)
        result = self._compare(
            (SearchHit(2, 1.00000002), SearchHit(1, 1.00000001)), oracle
        )
        self.assertEqual(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)
        self.assertEqual(result.reason_codes, ("FLAT_SCORE_ORDER_INVALID",))

    def test_raw_cosine_score_inversion_inside_tie_group_fails(self) -> None:
        higher = 0.90000002
        lower = 0.90000001
        self.assertEqual(struct.pack("<f", higher), struct.pack("<f", lower))
        oracle = OracleResult((OracleHit(1, higher), OracleHit(2, lower)), 2, False)
        result = compare_flat_oracle_hits(
            flat_hits=(SearchHit(2, lower), SearchHit(1, higher)),
            oracle_result=oracle,
            metric=Metric.COSINE,
            radius=0.25,
            range_filter=1.0,
            limit=100,
        )
        self.assertEqual(result.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)
        self.assertEqual(result.reason_codes, ("FLAT_SCORE_ORDER_INVALID",))

    def test_tie_group_permutations_with_ordered_raw_scores_pass(self) -> None:
        l2_oracle = OracleResult(
            (OracleHit(1, 1.00000001), OracleHit(2, 1.00000002)), 2, False
        )
        l2 = self._compare(
            (SearchHit(2, 1.0), SearchHit(1, 1.0)), l2_oracle
        )
        cosine_oracle = OracleResult(
            (OracleHit(1, 0.90000002), OracleHit(2, 0.90000001)), 2, False
        )
        cosine = compare_flat_oracle_hits(
            flat_hits=(SearchHit(2, 0.9), SearchHit(1, 0.9)),
            oracle_result=cosine_oracle,
            metric=Metric.COSINE,
            radius=0.25,
            range_filter=1.0,
            limit=100,
        )
        self.assertEqual(l2.kind, FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT)
        self.assertEqual(cosine.kind, FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT)

    def test_threshold_violation_and_nonfinite_scores_fail(self) -> None:
        oracle = OracleResult((OracleHit(1, 1.0),), 1, False)
        violation = self._compare((SearchHit(1, 12.0),), oracle, radius=10.0)
        nonfinite = self._compare((SearchHit(1, float("nan")),), oracle)
        self.assertEqual(violation.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)
        self.assertIn("FLAT_THRESHOLD_VIOLATION", violation.reason_codes)
        self.assertEqual(nonfinite.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)

    def test_different_membership_fails(self) -> None:
        oracle = OracleResult((OracleHit(1, 1.0), OracleHit(2, 1.0)), 2, False)
        result = self._compare((SearchHit(1, 1.0), SearchHit(3, 1.0)), oracle)
        self.assertEqual(result.kind, FlatOracleAgreementKind.MEMBERSHIP_MISMATCH)

    def test_capped_tie_does_not_allow_member_substitution(self) -> None:
        oracle = OracleResult((OracleHit(1, 1.0), OracleHit(2, 1.0)), 3, True)
        result = self._compare(
            (SearchHit(1, 1.0), SearchHit(3, 1.0)), oracle, limit=2
        )
        self.assertEqual(result.kind, FlatOracleAgreementKind.MEMBERSHIP_MISMATCH)

    def test_capture_and_reconstructive_validation_share_one_comparator(self) -> None:
        for module in (milvus_actuation, shadow_window):
            tree = ast.parse(inspect.getsource(module))
            imported = {
                node.module
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom)
            }
            self.assertIn("flat_oracle_agreement", imported)

    def test_adversarial_order_membership_and_numeric_matrix(self) -> None:
        tied = (1.00000001, 1.00000002, 1.00000003)
        self.assertEqual(len({struct.pack("<f", item) for item in tied}), 1)
        cases = (
            (
                "three-way tie",
                tuple(SearchHit(i, 1.0) for i in (3, 1, 2)),
                OracleResult(tuple(OracleHit(i, score) for i, score in zip((1, 2, 3), tied)), 3, False),
                FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT,
            ),
            (
                "multiple groups",
                tuple(SearchHit(i, score) for i, score in ((2, 1.0), (1, 1.0), (4, 2.0), (3, 2.0))),
                OracleResult((OracleHit(1, tied[0]), OracleHit(2, tied[1]), OracleHit(3, 2.00000001), OracleHit(4, 2.00000002)), 4, False),
                FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT,
            ),
            (
                "nonadjacent group movement",
                (SearchHit(3, 1.0), SearchHit(1, 2.0), SearchHit(2, 3.0)),
                OracleResult((OracleHit(1, tied[0]), OracleHit(2, tied[1]), OracleHit(3, 2.0)), 3, False),
                FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH,
            ),
            (
                "complete reverse",
                (SearchHit(3, 1.0), SearchHit(2, 2.0), SearchHit(1, 3.0)),
                OracleResult((OracleHit(1, 1.0), OracleHit(2, 2.0), OracleHit(3, 3.0)), 3, False),
                FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH,
            ),
            (
                "duplicate flat",
                (SearchHit(1, 1.0), SearchHit(1, 1.0)),
                OracleResult((OracleHit(1, 1.0), OracleHit(2, 1.0)), 2, False),
                FlatOracleAgreementKind.INVALID_EVIDENCE,
            ),
            (
                "duplicate oracle",
                (SearchHit(1, 1.0), SearchHit(2, 1.0)),
                OracleResult((OracleHit(1, 1.0), OracleHit(1, 1.0)), 2, False),
                FlatOracleAgreementKind.INVALID_EVIDENCE,
            ),
            (
                "missing member",
                (SearchHit(1, 1.0),),
                OracleResult((OracleHit(1, 1.0), OracleHit(2, 1.0)), 2, False),
                FlatOracleAgreementKind.MEMBERSHIP_MISMATCH,
            ),
            (
                "additional member",
                (SearchHit(1, 1.0), SearchHit(2, 1.0)),
                OracleResult((OracleHit(1, 1.0),), 1, False),
                FlatOracleAgreementKind.MEMBERSHIP_MISMATCH,
            ),
            (
                "nonfinite infinity",
                (SearchHit(1, float("inf")),),
                OracleResult((OracleHit(1, 1.0),), 1, False),
                FlatOracleAgreementKind.INVALID_EVIDENCE,
            ),
            (
                "nonfinite negative infinity",
                (SearchHit(1, float("-inf")),),
                OracleResult((OracleHit(1, 1.0),), 1, False),
                FlatOracleAgreementKind.INVALID_EVIDENCE,
            ),
            (
                "binary32 overflow",
                (SearchHit(1, 1.0),),
                OracleResult((OracleHit(1, 3.5e38),), 1, False),
                FlatOracleAgreementKind.INVALID_EVIDENCE,
            ),
        )
        for label, flat, oracle, expected in cases:
            with self.subTest(label=label):
                self.assertEqual(self._compare(flat, oracle).kind, expected)

    def test_production_comparator_exposes_no_tolerance_override(self) -> None:
        self.assertNotIn(
            "threshold_tolerance",
            inspect.signature(compare_flat_oracle_hits).parameters,
        )

    def test_zero_subnormal_threshold_and_cosine_edges(self) -> None:
        zero_oracle = OracleResult((OracleHit(1, -0.0), OracleHit(2, 0.0)), 2, False)
        zero = self._compare((SearchHit(2, 0.0), SearchHit(1, -0.0)), zero_oracle)
        self.assertEqual(zero.kind, FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT)
        subnormal = float.fromhex("0x1p-150")
        tiny = self._compare(
            (SearchHit(2, 0.0), SearchHit(1, subnormal)),
            OracleResult((OracleHit(1, 0.0), OracleHit(2, subnormal)), 2, False),
        )
        self.assertIn(
            tiny.kind,
            {FlatOracleAgreementKind.EXACT_ORDERED, FlatOracleAgreementKind.PRECISION_TIE_EQUIVALENT},
        )
        threshold = self._compare(
            (SearchHit(1, 10.00001),),
            OracleResult((OracleHit(1, 9.9999999),), 1, False),
            radius=10.0,
        )
        self.assertEqual(threshold.kind, FlatOracleAgreementKind.INVALID_EVIDENCE)
        cosine = compare_flat_oracle_hits(
            flat_hits=(SearchHit(2, 0.9), SearchHit(1, 0.8)),
            oracle_result=OracleResult((OracleHit(1, 0.9), OracleHit(2, 0.8)), 2, False),
            metric=Metric.COSINE,
            radius=0.25,
            range_filter=1.0,
            limit=100,
        )
        self.assertEqual(cosine.kind, FlatOracleAgreementKind.NON_TIE_ORDER_MISMATCH)

    def test_shadow_window_accepts_tie_only_reordering_and_rejects_non_tie(self) -> None:
        from tests.test_shadow_window import _envelopes

        values = list(_envelopes())
        query = values[0].trace.queries[0]
        tie_oracle = OracleResult(
            (OracleHit(10_001, 1.00000001), OracleHit(10_002, 1.00000002)),
            2,
            False,
        )
        tied = replace(
            query,
            oracle_result=tie_oracle,
            exact_cardinality=2,
            flat_hits=(SearchHit(10_002, 1.0), SearchHit(10_001, 1.0)),
            sentinel_hits=(SearchHit(10_001, 1.0), SearchHit(10_002, 1.0)),
            sentinel_recall=1.0,
        )
        trace = replace(values[0].trace, queries=(tied, *values[0].trace.queries[1:]))
        values[0] = replace(
            values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace)
        )
        self.assertTrue(assemble_shadow_window(window_id=1, envelopes=tuple(values)).complete)

        non_tie = replace(
            tied,
            oracle_result=OracleResult(
                (OracleHit(10_001, 1.0), OracleHit(10_002, 1.5)), 2, False
            ),
        )
        trace = replace(values[0].trace, queries=(non_tie, *values[0].trace.queries[1:]))
        values[0] = replace(
            values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace)
        )
        result = assemble_shadow_window(window_id=1, envelopes=tuple(values))
        self.assertFalse(result.complete)
        self.assertIn("FLAT_ORACLE_ORDER_MISMATCH", result.reason_codes)


if __name__ == "__main__":
    unittest.main()
