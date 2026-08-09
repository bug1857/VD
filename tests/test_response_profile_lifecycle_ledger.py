from __future__ import annotations

import ast
from dataclasses import fields
import os
import pickle
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

import numpy as np
import vdbench.response_profile_lifecycle_ledger as lifecycle_ledger_module

from vdbench.artifacts import canonical_json_bytes
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    WARMUP_QUERY_COUNT,
    ResponseProfileRoleKind,
    build_artifact_source_namespace,
    build_calibration_population_manifest,
    build_canonical_query_identity,
    build_query_vector_identity,
    build_response_profile_cell,
    build_response_profile_query_payload,
    build_response_profile_replay_schedule,
    build_response_profile_role,
    build_response_profile_role_manifest,
    build_response_profile_role_member,
)
from vdbench.response_profile_lifecycle import (
    LifecycleEventKind,
    build_response_profile_lifecycle_event,
    build_response_profile_run_binding,
    response_profile_lifecycle_event_document,
)
from vdbench.response_profile_lifecycle_ledger import (
    MeasurementStartPermit,
    ResponseProfileLifecycleLedger,
    ResponseProfileLifecycleLedgerError,
)


def _digest(character: str) -> str:
    return character * 64


def _configuration() -> SearchConfiguration:
    return SearchConfiguration(
        metric=Metric.L2,
        threshold_label="target-075",
        radius=0.75,
        index_track=IndexTrack.FLAT,
        ef=None,
    )


def _member(index: int, *, namespace: object, offset: float = 0.0):
    vector = build_query_vector_identity(
        np.asarray([float(index + 1) + offset], dtype="<f4")
    )
    return build_response_profile_role_member(
        source_namespace=namespace,
        query_identity=build_canonical_query_identity(index),
        vector_identity=vector,
        query_payload_identity=build_response_profile_query_payload(
            vector_identity=vector,
            search_configuration=_configuration(),
        ),
    )


def _manifest(kind: ResponseProfileRoleKind, members: tuple[object, ...]):
    return build_response_profile_role_manifest(
        role=build_response_profile_role(kind=kind), members=members
    )


def _assert_error(
    case: unittest.TestCase,
    operation: object,
    code: str | None = None,
) -> None:
    with case.assertRaises(ResponseProfileLifecycleLedgerError) as raised:
        operation()  # type: ignore[operator]
    if code is not None:
        case.assertEqual(raised.exception.code, code)


class _ConnectionProxy:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    @property
    def in_transaction(self) -> bool:
        return self.connection.in_transaction

    def execute(self, sql: str, parameters: object = ()):
        return self.connection.execute(sql, parameters)  # type: ignore[arg-type]

    def close(self) -> None:
        self.connection.close()


class _AmbiguousCommitConnection(_ConnectionProxy):
    def execute(self, sql: str, parameters: object = ()):
        result = self.connection.execute(sql, parameters)  # type: ignore[arg-type]
        if sql.strip().upper() == "COMMIT":
            raise sqlite3.OperationalError("simulated ambiguous commit")
        return result


class _FailBlobInsertConnection(_ConnectionProxy):
    def execute(self, sql: str, parameters: object = ()):
        if sql.startswith("INSERT INTO response_profile_opaque_evidence"):
            raise sqlite3.OperationalError("simulated blob insert failure")
        return self.connection.execute(sql, parameters)  # type: ignore[arg-type]


class ResponseProfileLifecycleLedgerFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        namespace = build_artifact_source_namespace(
            dataset_id="DATASET-R2-B2",
            dataset_version="v1",
            generation_manifest_sha256=_digest("a"),
        )
        calibration = _manifest(
            ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION,
            tuple(
                _member(index, namespace=namespace)
                for index in range(CALIBRATION_QUERY_COUNT)
            ),
        )
        population = build_calibration_population_manifest(
            cell=build_response_profile_cell(
                metric=Metric.L2, threshold_stratum="target-075"
            ),
            calibration_role_manifest=calibration,
        )
        schedule = build_response_profile_replay_schedule(
            population=population,
            source_revision="revision/r2-b2",
        )
        warmup = _manifest(
            ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP,
            tuple(
                _member(index + 10_000, namespace=namespace, offset=20_000.0)
                for index in range(WARMUP_QUERY_COUNT)
            ),
        )
        cls.binding = build_response_profile_run_binding(
            run_id="exp010-r2-b2",
            created_at_utc="2026-08-10T00:00:00Z",
            population=population,
            replay_schedule=schedule,
            warmup_role_manifest=warmup,
            source_revision="revision/r2-b2",
        )

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "lifecycle.sqlite3"

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _ledger(self, path: Path | None = None) -> ResponseProfileLifecycleLedger:
        return ResponseProfileLifecycleLedger(
            self.path if path is None else path,
            expected_run_binding=self.binding,
        )

    @staticmethod
    def _ready(ledger: ResponseProfileLifecycleLedger, *, epoch: int = 0) -> None:
        ledger.begin_epoch(epoch_index=epoch, recorded_at_utc="2026-08-10T00:00:00Z")
        ledger.complete_warmup(
            evidence_bytes=f"warmup-{epoch}".encode(),
            recorded_at_utc="2026-08-10T00:00:01Z",
        )

    @staticmethod
    def _complete_block(
        ledger: ResponseProfileLifecycleLedger,
        *,
        block: int,
        monotonic_base: int | None = None,
    ) -> None:
        base = 1_000_000 + block * 1_000 if monotonic_base is None else monotonic_base
        ledger.start_block(
            evidence_bytes=f"pre-{block}".encode(),
            recorded_at_utc="2026-08-10T00:00:02Z",
        )
        for within in range(4):
            started = base + within * 100
            permit = ledger.start_measurement(
                started_monotonic_ns=started,
                recorded_at_utc="2026-08-10T00:00:03Z",
            )
            ledger.complete_measurement(
                permit=permit,
                evidence_bytes=f"result-{block}-{within}".encode(),
                completed_monotonic_ns=started + 10,
                recorded_at_utc="2026-08-10T00:00:04Z",
            )
        ledger.close_block(
            evidence_bytes=f"post-{block}".encode(),
            recorded_at_utc="2026-08-10T00:00:05Z",
        )


class CreationSchemaAndPathTests(ResponseProfileLifecycleLedgerFixture):
    def test_new_database_is_private_strict_and_append_only(self) -> None:
        with self._ledger() as ledger:
            view = ledger.current_view()
            self.assertFalse(view.opened_existing)
            self.assertEqual(view.event_count, 0)
            self.assertFalse(hasattr(view, "opaque_evidence"))
            self.assertNotIn("evidence_bytes", repr(view))

            connection = ledger._conn
            assert connection is not None
            tables = {
                row[1]: row[5]
                for row in connection.execute("PRAGMA table_list").fetchall()
                if row[1].startswith("response_profile_")
            }
            self.assertEqual(
                tables,
                {
                    "response_profile_run_binding": 1,
                    "response_profile_lifecycle_events": 1,
                    "response_profile_opaque_evidence": 1,
                },
            )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE response_profile_run_binding SET schema_version='x'"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM response_profile_run_binding")

        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(
            Path(f"{self.path}.lock").stat().st_mode & 0o777,
            0o600,
        )

    def test_preexisting_empty_unversioned_wal_and_unsafe_paths_refuse(self) -> None:
        self.path.touch(mode=0o600)
        _assert_error(self, self._ledger, "LEDGER_SCHEMA_INVALID")
        self.path.unlink()

        connection = sqlite3.connect(self.path)
        connection.execute("CREATE TABLE unrelated (value INTEGER)")
        connection.close()
        os.chmod(self.path, 0o600)
        _assert_error(self, self._ledger, "LEDGER_SCHEMA_INVALID")
        self.path.unlink()

        with self._ledger():
            pass
        connection = sqlite3.connect(self.path)
        self.assertEqual(
            str(connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower(),
            "wal",
        )
        connection.close()
        _assert_error(self, self._ledger, "LEDGER_PRAGMA_INVALID")
        connection = sqlite3.connect(self.path)
        self.assertEqual(
            str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
            "wal",
        )
        connection.close()

    def test_symlink_hardlink_and_unsafe_mode_refuse_without_repair(self) -> None:
        original = Path(self.directory.name) / "original"
        original.write_bytes(b"not-sqlite")
        os.chmod(original, 0o600)

        symlink = Path(self.directory.name) / "symlink.db"
        symlink.symlink_to(original)
        _assert_error(
            self,
            lambda: self._ledger(symlink),
            "LEDGER_PATH_HARDENING_FAILED",
        )

        hardlink = Path(self.directory.name) / "hardlink.db"
        hardlink.hardlink_to(original)
        before = original.stat().st_mode & 0o777
        _assert_error(
            self,
            lambda: self._ledger(hardlink),
            "LEDGER_PATH_HARDENING_FAILED",
        )
        self.assertEqual(original.stat().st_mode & 0o777, before)

        unsafe = Path(self.directory.name) / "unsafe.db"
        unsafe.write_bytes(b"not-sqlite")
        os.chmod(unsafe, 0o644)
        _assert_error(
            self,
            lambda: self._ledger(unsafe),
            "LEDGER_PATH_HARDENING_FAILED",
        )
        self.assertEqual(unsafe.stat().st_mode & 0o777, 0o644)

    def test_same_process_second_owner_refuses_then_restart_succeeds(self) -> None:
        first = self._ledger()
        try:
            _assert_error(self, self._ledger, "LEDGER_OWNERSHIP_CONFLICT")
        finally:
            first.close()
        with self._ledger() as reopened:
            self.assertTrue(reopened.current_view().opened_existing)

    def test_process_lifetime_lock_survives_forked_child_cleanup(self) -> None:
        ledger = self._ledger()
        if hasattr(os, "fork"):
            child = os.fork()
            if child == 0:
                ledger.close()
                os._exit(0)
            _, status = os.waitpid(child, 0)
            self.assertEqual(status, 0)

        binding_path = Path(self.directory.name) / "binding.pickle"
        binding_path.write_bytes(pickle.dumps(self.binding))
        operation = subprocess.run(
            (
                sys.executable,
                "-c",
                "import pickle,sys; "
                "from vdbench.response_profile_lifecycle_ledger import "
                "ResponseProfileLifecycleLedger,ResponseProfileLifecycleLedgerError; "
                "binding=pickle.load(open(sys.argv[2],'rb')); "
                "\ntry:\n ResponseProfileLifecycleLedger(sys.argv[1], "
                "expected_run_binding=binding)\nexcept ResponseProfileLifecycleLedgerError as exc:\n "
                "print(exc.code)\nelse:\n print('OPENED')",
                str(self.path),
                str(binding_path),
            ),
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self.assertEqual(operation.returncode, 0, operation.stderr)
        self.assertEqual(operation.stdout.strip(), "LEDGER_OWNERSHIP_CONFLICT")
        ledger.close()

    def test_simultaneous_first_creation_has_one_owner_and_valid_database(self) -> None:
        binding_path = Path(self.directory.name) / "race-binding.pickle"
        binding_path.write_bytes(pickle.dumps(self.binding))
        start_path = Path(self.directory.name) / "race-start"
        ready_paths = tuple(
            Path(self.directory.name) / f"race-ready-{index}" for index in range(2)
        )
        worker = (
            "import pathlib,pickle,sys,time; "
            "from vdbench.response_profile_lifecycle_ledger import "
            "ResponseProfileLifecycleLedger,ResponseProfileLifecycleLedgerError; "
            "binding=pickle.load(open(sys.argv[2],'rb')); "
            "ready=pathlib.Path(sys.argv[3]); start=pathlib.Path(sys.argv[4]); "
            "ready.write_text('ready'); "
            "\nwhile not start.exists(): time.sleep(0.005)\n"
            "try:\n ledger=ResponseProfileLifecycleLedger(sys.argv[1], "
            "expected_run_binding=binding)\n"
            "except ResponseProfileLifecycleLedgerError as exc:\n print(exc.code,flush=True)\n"
            "else:\n print('OPENED',flush=True); time.sleep(2); ledger.close()"
        )
        processes = tuple(
            subprocess.Popen(
                (
                    sys.executable,
                    "-c",
                    worker,
                    str(self.path),
                    str(binding_path),
                    str(ready_path),
                    str(start_path),
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            )
            for ready_path in ready_paths
        )
        deadline = time.monotonic() + 30.0
        while not all(path.exists() for path in ready_paths):
            if time.monotonic() >= deadline:
                for process in processes:
                    process.kill()
                self.fail("first-create workers did not reach the start barrier")
            time.sleep(0.01)
        start_path.touch()
        outputs = tuple(process.communicate(timeout=30.0) for process in processes)
        for process, (_stdout, stderr) in zip(processes, outputs, strict=True):
            self.assertEqual(process.returncode, 0, stderr)
        self.assertCountEqual(
            [stdout.strip() for stdout, _stderr in outputs],
            ["OPENED", "LEDGER_OWNERSHIP_CONFLICT"],
        )
        with self._ledger() as reopened:
            self.assertTrue(reopened.current_view().opened_existing)

    def test_prepare_path_claims_lock_before_database_open_or_create(self) -> None:
        calls: list[str] = []
        real_open = lifecycle_ledger_module._open_private_regular_file
        real_flock = lifecycle_ledger_module.fcntl.flock
        real_claim = lifecycle_ledger_module._claim_lock_ownership

        def traced_open(path: str, *, create: bool):
            if path == f"{self.path}.lock":
                calls.append("LOCK_OPENED")
            elif path == str(self.path):
                self.assertEqual(
                    calls[:3],
                    ["LOCK_OPENED", "FLOCK_SUCCEEDED", "OWNERSHIP_REGISTERED"],
                )
                calls.append("DATABASE_OPENED")
            return real_open(path, create=create)

        def traced_flock(descriptor: int, operation: int) -> None:
            real_flock(descriptor, operation)
            if operation == (
                lifecycle_ledger_module.fcntl.LOCK_EX
                | lifecycle_ledger_module.fcntl.LOCK_NB
            ):
                calls.append("FLOCK_SUCCEEDED")

        def traced_claim(
            lock_descriptor: int, lock_inode: tuple[int, int]
        ) -> tuple[int, int, int]:
            ownership_key = real_claim(lock_descriptor, lock_inode)
            calls.append("OWNERSHIP_REGISTERED")
            return ownership_key

        with (
            mock.patch.object(
                lifecycle_ledger_module,
                "_open_private_regular_file",
                side_effect=traced_open,
            ),
            mock.patch.object(
                lifecycle_ledger_module.fcntl,
                "flock",
                side_effect=traced_flock,
            ),
            mock.patch.object(
                lifecycle_ledger_module,
                "_claim_lock_ownership",
                side_effect=traced_claim,
            ),
        ):
            with self._ledger():
                pass

        self.assertEqual(
            calls[:4],
            [
                "LOCK_OPENED",
                "FLOCK_SUCCEEDED",
                "OWNERSHIP_REGISTERED",
                "DATABASE_OPENED",
            ],
        )


class PermitAndAtomicityTests(ResponseProfileLifecycleLedgerFixture):
    def test_started_is_durable_before_private_permit_and_completion_requires_it(self) -> None:
        with self._ledger() as ledger:
            self._ready(ledger)
            ledger.start_block(
                evidence_bytes=b"pre", recorded_at_utc="2026-08-10T00:00:02Z"
            )
            connection = ledger._conn
            assert connection is not None
            real_make_permit = lifecycle_ledger_module._make_permit
            observed_started_counts: list[int] = []

            def observing_make_permit(**values: object) -> MeasurementStartPermit:
                observed_started_counts.append(
                    connection.execute(
                        "SELECT COUNT(*) FROM response_profile_lifecycle_events "
                        "WHERE event_kind='MEASUREMENT_STARTED'"
                    ).fetchone()[0]
                )
                return real_make_permit(**values)

            with mock.patch.object(
                lifecycle_ledger_module,
                "_make_permit",
                side_effect=observing_make_permit,
            ):
                permit = ledger.start_measurement(
                    started_monotonic_ns=100,
                    recorded_at_utc="2026-08-10T00:00:03Z",
                )
            self.assertIs(type(permit), MeasurementStartPermit)
            self.assertEqual(observed_started_counts, [0])
            row = connection.execute(
                "SELECT event_kind, lifecycle_event_sha256 "
                "FROM response_profile_lifecycle_events ORDER BY event_seq DESC LIMIT 1"
            ).fetchone()
            self.assertEqual(row["event_kind"], "MEASUREMENT_STARTED")
            self.assertEqual(
                row["lifecycle_event_sha256"],
                permit.measurement_started_event_sha256,
            )

            _assert_error(
                self,
                lambda: ledger.complete_measurement(
                    permit=object(),  # type: ignore[arg-type]
                    evidence_bytes=b"result",
                    completed_monotonic_ns=110,
                    recorded_at_utc="2026-08-10T00:00:04Z",
                ),
                "MEASUREMENT_PERMIT_INVALID",
            )
            ledger.complete_measurement(
                permit=permit,
                evidence_bytes=b"result",
                completed_monotonic_ns=110,
                recorded_at_utc="2026-08-10T00:00:04Z",
            )
            _assert_error(
                self,
                lambda: ledger.complete_measurement(
                    permit=permit,
                    evidence_bytes=b"again",
                    completed_monotonic_ns=120,
                    recorded_at_utc="2026-08-10T00:00:05Z",
                ),
                "MEASUREMENT_PERMIT_INVALID",
            )

    def test_permit_construction_failure_precedes_durable_started(self) -> None:
        with self._ledger() as ledger:
            self._ready(ledger)
            ledger.start_block(
                evidence_bytes=b"pre", recorded_at_utc="2026-08-10T00:00:02Z"
            )
            connection = ledger._conn
            assert connection is not None
            before = connection.execute(
                "SELECT COUNT(*) FROM response_profile_lifecycle_events"
            ).fetchone()[0]
            with mock.patch.object(
                lifecycle_ledger_module,
                "_make_permit",
                side_effect=MemoryError("simulated permit construction failure"),
            ):
                _assert_error(
                    self,
                    lambda: ledger.start_measurement(
                        started_monotonic_ns=100,
                        recorded_at_utc="2026-08-10T00:00:03Z",
                    ),
                    "MEASUREMENT_PERMIT_CONSTRUCTION_FAILED",
                )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM response_profile_lifecycle_events"
                ).fetchone()[0],
                before,
            )
            self.assertIsNone(ledger._active_permit)
            self.assertFalse(ledger._poisoned)

    def test_post_commit_reconciliation_failure_poisons_and_reopen_recovers(self) -> None:
        ledger = self._ledger()
        with mock.patch.object(
            ledger,
            "_reconcile_committed_candidate",
            side_effect=MemoryError("simulated reconciliation failure"),
        ):
            _assert_error(
                self,
                lambda: ledger.begin_epoch(
                    epoch_index=0, recorded_at_utc="2026-08-10T00:00:00Z"
                ),
                "LEDGER_POST_COMMIT_RECONCILIATION_FAILED",
            )
        connection = ledger._conn
        assert connection is not None
        self.assertEqual(
            connection.execute(
                "SELECT event_kind FROM response_profile_lifecycle_events"
            ).fetchone()[0],
            "EPOCH_STARTED",
        )
        self.assertTrue(ledger._poisoned)
        self.assertIsNone(ledger._active_permit)
        _assert_error(self, ledger.current_view, "LEDGER_POISONED")
        ledger.close()

        with self._ledger() as reopened:
            view = reopened.current_view()
            self.assertEqual(view.event_count, 1)
            self.assertEqual(view.current_epoch_index, 0)
            self.assertTrue(view.requires_fresh_epoch_after_recovery)

    def test_cross_instance_forged_and_thread_without_permit_refuse(self) -> None:
        other_path = Path(self.directory.name) / "other.sqlite3"
        with self._ledger() as first, self._ledger(other_path) as second:
            self._ready(first)
            first.start_block(
                evidence_bytes=b"pre", recorded_at_utc="2026-08-10T00:00:02Z"
            )
            permit = first.start_measurement(
                started_monotonic_ns=100,
                recorded_at_utc="2026-08-10T00:00:03Z",
            )
            self._ready(second)
            second.start_block(
                evidence_bytes=b"pre", recorded_at_utc="2026-08-10T00:00:02Z"
            )
            second_permit = second.start_measurement(
                started_monotonic_ns=100,
                recorded_at_utc="2026-08-10T00:00:03Z",
            )
            _assert_error(
                self,
                lambda: first.complete_measurement(
                    permit=second_permit,
                    evidence_bytes=b"result",
                    completed_monotonic_ns=110,
                    recorded_at_utc="2026-08-10T00:00:04Z",
                ),
                "MEASUREMENT_PERMIT_INVALID",
            )

            forged = object.__new__(MeasurementStartPermit)
            for item in fields(permit):
                object.__setattr__(forged, item.name, getattr(permit, item.name))
            _assert_error(
                self,
                lambda: first.complete_measurement(
                    permit=forged,
                    evidence_bytes=b"result",
                    completed_monotonic_ns=110,
                    recorded_at_utc="2026-08-10T00:00:04Z",
                ),
                "MEASUREMENT_PERMIT_INVALID",
            )

            observed: list[str] = []
            thread = threading.Thread(
                target=lambda: observed.append(
                    self._completion_error_code(first, object())
                )
            )
            thread.start()
            thread.join()
            self.assertEqual(observed, ["MEASUREMENT_PERMIT_INVALID"])

    @staticmethod
    def _completion_error_code(
        ledger: ResponseProfileLifecycleLedger, permit: object
    ) -> str:
        try:
            ledger.complete_measurement(
                permit=permit,  # type: ignore[arg-type]
                evidence_bytes=b"result",
                completed_monotonic_ns=110,
                recorded_at_utc="2026-08-10T00:00:04Z",
            )
        except ResponseProfileLifecycleLedgerError as exc:
            return exc.code
        return "NO_ERROR"

    def test_deterministic_completion_rejection_preserves_permit(self) -> None:
        with self._ledger() as ledger:
            self._ready(ledger)
            ledger.start_block(
                evidence_bytes=b"pre", recorded_at_utc="2026-08-10T00:00:02Z"
            )
            permit = ledger.start_measurement(
                started_monotonic_ns=100,
                recorded_at_utc="2026-08-10T00:00:03Z",
            )
            _assert_error(
                self,
                lambda: ledger.complete_measurement(
                    permit=permit,
                    evidence_bytes=b"result",
                    completed_monotonic_ns=100,
                    recorded_at_utc="2026-08-10T00:00:04Z",
                ),
                "LIFECYCLE_TRANSITION_INVALID",
            )
            self.assertIs(ledger._active_permit, permit)
            ledger.complete_measurement(
                permit=permit,
                evidence_bytes=b"result",
                completed_monotonic_ns=101,
                recorded_at_utc="2026-08-10T00:00:04Z",
            )

    def test_event_blob_failure_is_atomic_and_poisons(self) -> None:
        ledger = self._ledger()
        self._ready(ledger)
        connection = ledger._conn
        assert connection is not None
        before = connection.execute(
            "SELECT COUNT(*) FROM response_profile_lifecycle_events"
        ).fetchone()[0]
        ledger._conn = _FailBlobInsertConnection(connection)  # type: ignore[assignment]
        _assert_error(
            self,
            lambda: ledger.start_block(
                evidence_bytes=b"pre", recorded_at_utc="2026-08-10T00:00:02Z"
            ),
            "LEDGER_TRANSACTION_FAILED",
        )
        self.assertTrue(ledger._poisoned)
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM response_profile_lifecycle_events"
            ).fetchone()[0],
            before,
        )
        ledger.close()

    def test_ambiguous_measurement_start_yields_no_permit_and_terminal_reopen(self) -> None:
        ledger = self._ledger()
        self._ready(ledger)
        ledger.start_block(
            evidence_bytes=b"pre", recorded_at_utc="2026-08-10T00:00:02Z"
        )
        connection = ledger._conn
        assert connection is not None
        ledger._conn = _AmbiguousCommitConnection(connection)  # type: ignore[assignment]
        _assert_error(
            self,
            lambda: ledger.start_measurement(
                started_monotonic_ns=100,
                recorded_at_utc="2026-08-10T00:00:03Z",
            ),
            "LEDGER_TRANSACTION_FAILED",
        )
        self.assertIsNone(ledger._active_permit)
        self.assertTrue(ledger._poisoned)
        ledger.close()
        with self._ledger() as reopened:
            self.assertTrue(reopened.current_view().terminal_recovery)
            self.assertEqual(
                reopened.current_view().terminal_reason_codes,
                ("ORPHAN_MEASUREMENT_STARTED",),
            )

    def test_fork_pid_mismatch_refuses_and_close_invalidates_permit(self) -> None:
        ledger = self._ledger()
        self._ready(ledger)
        ledger.start_block(
            evidence_bytes=b"pre", recorded_at_utc="2026-08-10T00:00:02Z"
        )
        permit = ledger.start_measurement(
            started_monotonic_ns=100,
            recorded_at_utc="2026-08-10T00:00:03Z",
        )
        with mock.patch(
            "vdbench.response_profile_lifecycle_ledger.os.getpid",
            return_value=ledger._owner_pid + 1,
        ):
            _assert_error(self, ledger.current_view, "LEDGER_FORK_INVALID")
        ledger.close()
        _assert_error(
            self,
            lambda: ledger.complete_measurement(
                permit=permit,
                evidence_bytes=b"result",
                completed_monotonic_ns=110,
                recorded_at_utc="2026-08-10T00:00:04Z",
            ),
            "LEDGER_CLOSED",
        )


class RecoveryInterlockTests(ResponseProfileLifecycleLedgerFixture):
    def test_closed_block_reopen_requires_fresh_epoch_and_warmup(self) -> None:
        with self._ledger() as ledger:
            self._ready(ledger)
            self._complete_block(ledger, block=0)

        with self._ledger() as reopened:
            self.assertTrue(
                reopened.current_view().requires_fresh_epoch_after_recovery
            )
            for operation in (
                lambda: reopened.start_block(
                    evidence_bytes=b"pre", recorded_at_utc="2026-08-10T01:00:00Z"
                ),
                lambda: reopened.complete_warmup(
                    evidence_bytes=b"old-warmup",
                    recorded_at_utc="2026-08-10T01:00:00Z",
                ),
                lambda: reopened.append_run_invalidated(
                    reason_code="OLD_EPOCH",
                    recorded_at_utc="2026-08-10T01:00:00Z",
                ),
            ):
                _assert_error(
                    self, operation, "RECOVERY_FRESH_EPOCH_REQUIRED"
                )
            reopened.begin_epoch(
                epoch_index=1, recorded_at_utc="2026-08-10T01:00:01Z"
            )
            self.assertFalse(
                reopened.current_view().requires_fresh_epoch_after_recovery
            )
            _assert_error(
                self,
                lambda: reopened.start_block(
                    evidence_bytes=b"pre", recorded_at_utc="2026-08-10T01:00:02Z"
                ),
                "LIFECYCLE_TRANSITION_INVALID",
            )
            reopened.complete_warmup(
                evidence_bytes=b"fresh-warmup",
                recorded_at_utc="2026-08-10T01:00:03Z",
            )
            reopened.start_block(
                evidence_bytes=b"pre", recorded_at_utc="2026-08-10T01:00:04Z"
            )

    def test_warmup_only_reopen_cannot_continue_old_epoch(self) -> None:
        with self._ledger() as ledger:
            self._ready(ledger, epoch=7)
        with self._ledger() as reopened:
            self.assertTrue(
                reopened.current_view().requires_fresh_epoch_after_recovery
            )
            _assert_error(
                self,
                lambda: reopened.start_block(
                    evidence_bytes=b"pre", recorded_at_utc="2026-08-10T01:00:00Z"
                ),
                "RECOVERY_FRESH_EPOCH_REQUIRED",
            )
            reopened.begin_epoch(
                epoch_index=8, recorded_at_utc="2026-08-10T01:00:01Z"
            )
            self.assertEqual(reopened.current_view().current_epoch_index, 8)

    def test_interlock_clears_only_after_durable_epoch_commit(self) -> None:
        with self._ledger() as ledger:
            ledger.begin_epoch(
                epoch_index=0, recorded_at_utc="2026-08-10T00:00:00Z"
            )
        with self._ledger() as reopened:
            self.assertTrue(reopened._recovery_interlock)
            _assert_error(
                self,
                lambda: reopened.begin_epoch(
                    epoch_index=0, recorded_at_utc="2026-08-10T01:00:00Z"
                ),
                "LIFECYCLE_TRANSITION_INVALID",
            )
            self.assertTrue(reopened._recovery_interlock)
            reopened.begin_epoch(
                epoch_index=1, recorded_at_utc="2026-08-10T01:00:01Z"
            )
            self.assertFalse(reopened._recovery_interlock)
            connection = reopened._conn
            assert connection is not None
            self.assertEqual(
                connection.execute(
                    "SELECT event_kind FROM response_profile_lifecycle_events "
                    "ORDER BY event_seq DESC LIMIT 1"
                ).fetchone()[0],
                "EPOCH_STARTED",
            )

    def test_ambiguous_epoch_commit_does_not_clear_interlock(self) -> None:
        with self._ledger() as ledger:
            ledger.begin_epoch(
                epoch_index=0, recorded_at_utc="2026-08-10T00:00:00Z"
            )
        reopened = self._ledger()
        self.assertTrue(reopened._recovery_interlock)
        connection = reopened._conn
        assert connection is not None
        reopened._conn = _AmbiguousCommitConnection(connection)  # type: ignore[assignment]
        _assert_error(
            self,
            lambda: reopened.begin_epoch(
                epoch_index=1, recorded_at_utc="2026-08-10T01:00:00Z"
            ),
            "LEDGER_TRANSACTION_FAILED",
        )
        self.assertTrue(reopened._recovery_interlock)
        self.assertTrue(reopened._poisoned)
        reopened.close()
        with self._ledger() as recovered_again:
            self.assertTrue(recovered_again._recovery_interlock)
            self.assertEqual(recovered_again.current_view().current_epoch_index, 1)

    def test_orphan_and_partial_block_open_terminal_read_only(self) -> None:
        orphan_path = self.path
        with self._ledger(orphan_path) as ledger:
            self._ready(ledger)
            ledger.start_block(
                evidence_bytes=b"pre", recorded_at_utc="2026-08-10T00:00:02Z"
            )
            ledger.start_measurement(
                started_monotonic_ns=100,
                recorded_at_utc="2026-08-10T00:00:03Z",
            )
        with self._ledger(orphan_path) as reopened:
            view = reopened.current_view()
            self.assertTrue(view.terminal_recovery)
            connection = reopened._conn
            assert connection is not None
            self.assertEqual(connection.execute("PRAGMA query_only").fetchone()[0], 1)
            self.assertEqual(
                view.terminal_reason_codes, ("ORPHAN_MEASUREMENT_STARTED",)
            )
            _assert_error(
                self,
                lambda: reopened.begin_epoch(
                    epoch_index=1, recorded_at_utc="2026-08-10T01:00:00Z"
                ),
                "LEDGER_TERMINAL",
            )

        partial_path = Path(self.directory.name) / "partial.sqlite3"
        with self._ledger(partial_path) as ledger:
            self._ready(ledger)
            ledger.start_block(
                evidence_bytes=b"pre", recorded_at_utc="2026-08-10T00:00:02Z"
            )
            permit = ledger.start_measurement(
                started_monotonic_ns=100,
                recorded_at_utc="2026-08-10T00:00:03Z",
            )
            ledger.complete_measurement(
                permit=permit,
                evidence_bytes=b"result",
                completed_monotonic_ns=110,
                recorded_at_utc="2026-08-10T00:00:04Z",
            )
        with self._ledger(partial_path) as reopened:
            self.assertTrue(reopened.current_view().terminal_recovery)
            self.assertEqual(
                reopened.current_view().terminal_reason_codes,
                ("PARTIAL_MEASURED_BLOCK",),
            )


class VerificationAndDependencyTests(ResponseProfileLifecycleLedgerFixture):
    def test_expected_run_binding_and_canonical_blob_tamper_fail_closed(self) -> None:
        with self._ledger() as ledger:
            self._ready(ledger)

        other_binding = build_response_profile_run_binding(
            run_id="different-r2-b2-run",
            created_at_utc=self.binding.created_at_utc,
            population=self.binding.population,
            replay_schedule=self.binding.replay_schedule,
            warmup_role_manifest=self.binding.warmup_role_manifest,
            source_revision=self.binding.source_revision,
        )
        _assert_error(
            self,
            lambda: ResponseProfileLifecycleLedger(
                self.path, expected_run_binding=other_binding
            ),
            "RUN_BINDING_MISMATCH",
        )

        connection = sqlite3.connect(self.path)
        trigger_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' "
            "AND name='response_profile_opaque_evidence_no_update'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER response_profile_opaque_evidence_no_update")
        connection.execute(
            "UPDATE response_profile_opaque_evidence "
            "SET evidence_bytes=? WHERE event_seq=1",
            (b"tampered",),
        )
        connection.execute(trigger_sql)
        connection.commit()
        connection.close()
        _assert_error(self, self._ledger, "OPAQUE_EVIDENCE_INVALID")

    def test_intrinsically_invalid_chain_refuses_before_recovery_classification(self) -> None:
        with self._ledger():
            pass
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        run_digest = self.binding.run_binding_sha256
        first = build_response_profile_lifecycle_event(
            run_binding_sha256=run_digest,
            event_seq=0,
            event_kind=LifecycleEventKind.EPOCH_STARTED,
            epoch_index=0,
            block_index=None,
            position_index=None,
            recorded_at_utc="2026-08-10T00:00:00Z",
            event_data={},
            previous_event_sha256=run_digest,
        )
        second = build_response_profile_lifecycle_event(
            run_binding_sha256=run_digest,
            event_seq=1,
            event_kind=LifecycleEventKind.EPOCH_STARTED,
            epoch_index=0,
            block_index=None,
            position_index=None,
            recorded_at_utc="2026-08-10T00:00:01Z",
            event_data={},
            previous_event_sha256=first.lifecycle_event_sha256,
        )
        for event in (first, second):
            connection.execute(
                "INSERT INTO response_profile_lifecycle_events "
                "(event_seq, schema_version, run_binding_sha256, event_kind, "
                "epoch_index, block_index, position_index, previous_event_sha256, "
                "lifecycle_event_sha256, referenced_blob_sha256, canonical_document) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)",
                (
                    event.event_seq,
                    event.schema_version,
                    event.run_binding_sha256,
                    event.event_kind.value,
                    event.epoch_index,
                    event.block_index,
                    event.position_index,
                    event.previous_event_sha256,
                    event.lifecycle_event_sha256,
                    canonical_json_bytes(response_profile_lifecycle_event_document(event)),
                ),
            )
        connection.commit()
        connection.close()
        _assert_error(self, self._ledger, "PERSISTED_LIFECYCLE_INVALID")

    def test_schema_and_head_drift_fail_closed(self) -> None:
        with self._ledger():
            pass
        connection = sqlite3.connect(self.path)
        connection.execute("DROP TRIGGER response_profile_run_binding_no_update")
        connection.commit()
        connection.close()
        _assert_error(self, self._ledger, "LEDGER_SCHEMA_INVALID")

        index_path = Path(self.directory.name) / "extra-index.sqlite3"
        with self._ledger(index_path):
            pass
        connection = sqlite3.connect(index_path)
        connection.execute(
            "CREATE INDEX unauthorized_index "
            "ON response_profile_lifecycle_events(event_kind)"
        )
        connection.close()
        _assert_error(
            self,
            lambda: self._ledger(index_path),
            "LEDGER_SCHEMA_INVALID",
        )

        literal_path = Path(self.directory.name) / "literal-case.sqlite3"
        with self._ledger(literal_path):
            pass
        connection = sqlite3.connect(literal_path)
        trigger_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' "
            "AND name='response_profile_opaque_evidence_binding'"
        ).fetchone()[0]
        self.assertIn("'WARMUP_COMPLETED'", trigger_sql)
        connection.execute("DROP TRIGGER response_profile_opaque_evidence_binding")
        connection.execute(
            trigger_sql.replace("'WARMUP_COMPLETED'", "'warmup_completed'", 1)
        )
        connection.commit()
        connection.close()
        _assert_error(
            self,
            lambda: self._ledger(literal_path),
            "LEDGER_SCHEMA_INVALID",
        )

    def test_live_head_drift_is_detected_and_permanently_poisons(self) -> None:
        ledger = self._ledger()
        event = build_response_profile_lifecycle_event(
            run_binding_sha256=self.binding.run_binding_sha256,
            event_seq=0,
            event_kind=LifecycleEventKind.EPOCH_STARTED,
            epoch_index=0,
            block_index=None,
            position_index=None,
            recorded_at_utc="2026-08-10T00:00:00Z",
            event_data={},
            previous_event_sha256=self.binding.run_binding_sha256,
        )
        external = sqlite3.connect(self.path)
        external.execute(
            "INSERT INTO response_profile_lifecycle_events "
            "(event_seq, schema_version, run_binding_sha256, event_kind, "
            "epoch_index, block_index, position_index, previous_event_sha256, "
            "lifecycle_event_sha256, referenced_blob_sha256, canonical_document) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)",
            (
                event.event_seq,
                event.schema_version,
                event.run_binding_sha256,
                event.event_kind.value,
                event.epoch_index,
                event.block_index,
                event.position_index,
                event.previous_event_sha256,
                event.lifecycle_event_sha256,
                canonical_json_bytes(response_profile_lifecycle_event_document(event)),
            ),
        )
        external.commit()
        external.close()
        _assert_error(
            self,
            lambda: ledger.begin_epoch(
                epoch_index=0, recorded_at_utc="2026-08-10T00:00:01Z"
            ),
            "LEDGER_HEAD_DRIFT",
        )
        self.assertTrue(ledger._poisoned)
        _assert_error(self, ledger.current_view, "LEDGER_POISONED")
        ledger.close()

    def test_path_drift_poisons_the_open_instance(self) -> None:
        ledger = self._ledger()
        lock_path = Path(f"{self.path}.lock")
        os.chmod(lock_path, 0o644)
        _assert_error(self, ledger.current_view, "LEDGER_FILE_DRIFT")
        os.chmod(lock_path, 0o600)
        _assert_error(self, ledger.current_view, "LEDGER_POISONED")
        ledger.close()

    def test_no_raw_evidence_or_forbidden_dependencies(self) -> None:
        path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "vdbench"
            / "response_profile_lifecycle_ledger.py"
        )
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        forbidden = {
            "pymilvus",
            "policy",
            "actuation",
            "canary_admission",
            "lkg_phase3_authority",
            "response_profile",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[-1])
        self.assertFalse(imported & forbidden)

        with self._ledger() as ledger:
            public = {name for name in dir(ledger) if not name.startswith("_")}
            self.assertNotIn("load_opaque_evidence", public)
            self.assertNotIn("raw_evidence", public)
            self.assertFalse(
                any("evidence_bytes" in item.name for item in fields(ledger.current_view()))
            )


if __name__ == "__main__":
    unittest.main()
