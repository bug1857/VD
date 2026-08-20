from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from vdbench.config import Metric
from vdbench.host_observation import (
    CompletedRangeQueryObservation,
    RangeQueryRequest,
    ServedQueryOutcome,
)
from vdbench.host_window_detector_v2 import source_window_sha256
from vdbench.host_window_lineage import (
    HostResponseCommitError,
    InjectedReadOnlyCaptureMetadataProvider,
    ReferenceV2Host,
    SQLiteHostResponseCommitStore,
    VerifiedHostSourceHead,
    V2GenuineWorkloadObservationSource,
)
from vdbench.shadow_event_types import MonitorStreamKey


def _stream() -> MonitorStreamKey:
    return MonitorStreamKey(
        "served-l2", Metric.L2, "target-075", "cfg", "data", "flat", "hnsw"
    )


def _outcome() -> ServedQueryOutcome:
    return ServedQueryOutcome(True, False, 3, 1.25)


def _observation(index: int) -> CompletedRangeQueryObservation:
    return CompletedRangeQueryObservation(
        request_id=index,
        captured_at_utc="2026-08-12T00:00:00Z",
        stream_key=_stream(),
        query_vector=(float(index), 1.0),
        threshold_radius=0.75,
        range_filter=0.0,
        limit=100,
        served_ef=400,
        served_outcome=_outcome(),
    )


class _Executor:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, _request: RangeQueryRequest) -> ServedQueryOutcome:
        self.calls += 1
        return _outcome()


class _OutboxFailingConnection:
    """Delegate every statement except the outbox insert, which fails.

    This injects a durability fault strictly *between* the ``source_records``
    insert and the ``source_outbox`` insert of one ``commit_response``
    transaction, which is the exact window ADR-013's atomicity claim covers.
    A schema-level trigger cannot be used for this: the store's exact-set
    schema verification would reject the extra trigger before the transaction
    ever began.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self.attempted_outbox_insert = False

    def execute(self, sql: str, *args: object) -> object:
        if sql.startswith("INSERT INTO source_outbox"):
            self.attempted_outbox_insert = True
            raise sqlite3.OperationalError("injected outbox durability failure")
        return self._connection.execute(sql, *args)

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)


class HostWindowLineageTests(unittest.TestCase):
    def _store(self, path: Path) -> SQLiteHostResponseCommitStore:
        return SQLiteHostResponseCommitStore(
            path,
            stream_key=_stream(),
            source_revision="revision",
            environment_manifest_sha256="a" * 64,
        )

    def test_commit_is_membership_and_restart_reconstructs_exact_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                records = tuple(
                    store.commit_response(
                        _observation(index), committed_at_utc="2026-08-12T00:00:00Z"
                    )
                    for index in range(201)
                )
                self.assertEqual(
                    (records[200].source_sequence, records[200].window_sequence,
                     records[200].within_window_index),
                    (200, 1, 0),
                )
                first_window = store.window_sha256(0)
                self.assertIsNotNone(first_window)
                self.assertIsNone(store.window_sha256(1))
            with self._store(path) as reopened:
                self.assertEqual(reopened.window_sha256(0), first_window)
                self.assertEqual(
                    reopened.poll(consumer_id="shadow", limit=1)[0].source_sequence,
                    0,
                )

    def test_verified_head_binds_count_to_source_and_outbox_heads(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                empty = store.verified_source_head()
                self.assertIs(type(empty), VerifiedHostSourceHead)
                self.assertEqual(empty.source_count, 0)
                self.assertIsNone(empty.source_head_sha256)
                for index in range(3):
                    latest = store.commit_response(
                        _observation(index), committed_at_utc="2026-08-12T00:00:00Z"
                    )
                head = store.verified_source_head()
                self.assertEqual((head.source_count, head.maximum_source_sequence), (3, 2))
                self.assertEqual(head.source_head_sha256, latest.source_sha256)
                self.assertIsNotNone(head.outbox_head_sha256)
            with self._store(path) as reopened:
                self.assertEqual(reopened.verified_source_head(), head)

    def test_hot_head_check_never_replays_full_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                for index in range(8):
                    store.commit_response(
                        _observation(index), committed_at_utc="2026-08-12T00:00:00Z"
                    )
                original = store._verify_all
                store._verify_all = lambda: (_ for _ in ()).throw(
                    AssertionError("hot head check replayed the full chain")
                )
                try:
                    self.assertEqual(store.verified_source_head().source_count, 8)
                finally:
                    store._verify_all = original

    def test_outbox_head_substitution_refused_by_hot_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                store.commit_response(
                    _observation(0), committed_at_utc="2026-08-12T00:00:00Z"
                )
                trigger = store._connection.execute(
                    "SELECT sql FROM sqlite_schema WHERE name='source_outbox_no_update'"
                ).fetchone()[0]
                store._connection.execute("DROP TRIGGER source_outbox_no_update")
                store._connection.execute(
                    "UPDATE source_outbox SET outbox_sha256=? WHERE source_sequence=0",
                    ("0" * 64,),
                )
                store._connection.execute(trigger)
                with self.assertRaises(HostResponseCommitError) as raised:
                    store.verified_source_head()
                self.assertEqual(raised.exception.code, "HOST_SOURCE_HEAD_DRIFT")

    def test_cached_count_substitution_is_refused_by_durable_head(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                for index in range(2):
                    store.commit_response(
                        _observation(index), committed_at_utc="2026-08-12T00:00:00Z"
                    )
                verified = store._sources
                store._sources = verified[:-1]
                try:
                    with self.assertRaises(HostResponseCommitError) as raised:
                        store.verified_source_head()
                    self.assertEqual(raised.exception.code, "HOST_SOURCE_HEAD_DRIFT")
                finally:
                    store._sources = verified

    def test_v2_host_never_returns_visible_response_when_commit_fails(self) -> None:
        executor = _Executor()
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory) / "source.sqlite3")
            host = ReferenceV2Host(
                serving_executor=executor,
                response_store=store,
                clock=lambda: "2026-08-12T00:00:00Z",
            )
            store.close()
            request = RangeQueryRequest(
                1, _stream(), (1.0, 2.0), 0.75, 0.0, 100, 400
            )
            with self.assertRaises(HostResponseCommitError):
                host.execute(request)
            self.assertEqual(executor.calls, 1)

    def test_independent_offset_cursors_redeliver_exactly_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                for index in range(605):
                    store.commit_response(
                        _observation(index), committed_at_utc="2026-08-12T00:00:00Z"
                    )
                clock = lambda: "2026-08-12T00:01:00Z"
                exp010 = V2GenuineWorkloadObservationSource(
                    store=store, consumer_id="exp010", clock=clock,
                    start_source_sequence=600,
                )
                shadow = V2GenuineWorkloadObservationSource(
                    store=store, consumer_id="shadow", clock=clock,
                )
                first = exp010.poll(limit=5)
                self.assertEqual(tuple(item.source_sequence for item in first), tuple(range(600, 605)))
                self.assertEqual(exp010.poll(limit=5), first)
                self.assertEqual(shadow.poll(limit=1)[0].source_sequence, 0)
                exp010.acknowledge(tuple(item.event_id for item in first))
                self.assertEqual(exp010.poll(limit=1), ())
            with self._store(path) as reopened:
                exp010 = V2GenuineWorkloadObservationSource(
                    store=reopened, consumer_id="exp010", clock=clock,
                    start_source_sequence=600,
                )
                self.assertEqual(exp010.poll(limit=1), ())
                with self.assertRaises(HostResponseCommitError) as raised:
                    reopened.poll(
                        consumer_id="exp010", limit=1, start_source_sequence=0
                    )
                self.assertEqual(raised.exception.code, "HOST_SOURCE_CONSUMER_OFFSET_MISMATCH")

    def test_source_tamper_fails_closed_on_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                store.commit_response(
                    _observation(0), committed_at_utc="2026-08-12T00:00:00Z"
                )
            connection = sqlite3.connect(path)
            connection.execute("DROP TRIGGER source_records_no_update")
            connection.execute(
                "UPDATE source_records SET source_sha256=? WHERE source_sequence=0",
                ("0" * 64,),
            )
            connection.commit()
            connection.close()
            with self.assertRaises(HostResponseCommitError):
                self._store(path)

    def test_outbox_tamper_fails_closed_independently_of_source_row(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                store.commit_response(
                    _observation(0), committed_at_utc="2026-08-12T00:00:00Z"
                )
            connection = sqlite3.connect(path)
            connection.execute("DROP TRIGGER source_outbox_no_update")
            connection.execute(
                "UPDATE source_outbox SET outbox_sha256=? WHERE source_sequence=0",
                ("0" * 64,),
            )
            connection.commit()
            connection.close()
            with self.assertRaises(HostResponseCommitError):
                self._store(path)

    def test_store_window_digest_equals_shadow_side_recomputation(self) -> None:
        """Cross-module conservation: the host store's window digest and the
        detector-side recomputation are two independent implementations of the
        same ADR-012 formula. They must stay byte-identical, or shadow lineage
        silently stops binding committed source membership."""

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                records = tuple(
                    store.commit_response(
                        _observation(index), committed_at_utc="2026-08-12T00:00:00Z"
                    )
                    for index in range(200)
                )
                stored = store.window_sha256(0)
                self.assertIsNotNone(stored)
                self.assertEqual(stored, source_window_sha256(records))

    def test_outbox_insert_failure_rolls_back_source_and_leaves_no_gap(self) -> None:
        """ADR-013 atomicity: a failure after the source insert but before the
        outbox insert must leave neither row, must not return a visible v2
        response, and must not consume the source sequence."""

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                store.commit_response(
                    _observation(0), committed_at_utc="2026-08-12T00:00:00Z"
                )
                executor = _Executor()
                host = ReferenceV2Host(
                    serving_executor=executor,
                    response_store=store,
                    clock=lambda: "2026-08-12T00:00:01Z",
                )
                real = store._connection
                failing = _OutboxFailingConnection(real)
                store._connection = failing
                try:
                    with self.assertRaises(HostResponseCommitError) as raised:
                        host.execute(
                            RangeQueryRequest(1, _stream(), (1.0, 2.0), 0.75, 0.0, 100, 400)
                        )
                finally:
                    store._connection = real
                self.assertEqual(raised.exception.code, "HOST_RESPONSE_DURABILITY_FAILED")
                self.assertTrue(failing.attempted_outbox_insert)
                # The search really ran; no visible response was produced.
                self.assertEqual(executor.calls, 1)
                self.assertIsNone(
                    real.execute(
                        "SELECT source_sequence FROM source_records WHERE source_sequence=1"
                    ).fetchone()
                )
                self.assertIsNone(
                    real.execute(
                        "SELECT source_sequence FROM source_outbox WHERE source_sequence=1"
                    ).fetchone()
                )
            # Clean reopen, and the next successful commit reuses sequence 1.
            with self._store(path) as reopened:
                record = reopened.commit_response(
                    _observation(1), committed_at_utc="2026-08-12T00:00:02Z"
                )
                self.assertEqual(record.source_sequence, 1)
                self.assertEqual(
                    tuple(
                        item.source_sequence
                        for item in reopened.poll(consumer_id="shadow", limit=10)
                    ),
                    (0, 1),
                )

    def test_non_contiguous_acknowledgement_fails_and_stays_redeliverable(self) -> None:
        """A consumer may never skip an unacknowledged source member."""

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                records = tuple(
                    store.commit_response(
                        _observation(index), committed_at_utc="2026-08-12T00:00:00Z"
                    )
                    for index in range(3)
                )
                for label, event_ids in (
                    ("skip_first", (records[1].event_id,)),
                    ("out_of_order", (records[1].event_id, records[0].event_id)),
                    ("gap_inside_run", (records[0].event_id, records[2].event_id)),
                ):
                    with self.subTest(case=label):
                        with self.assertRaises(HostResponseCommitError) as raised:
                            store.acknowledge(
                                consumer_id="shadow",
                                event_ids=event_ids,
                                acknowledged_at_utc="2026-08-12T00:01:00Z",
                            )
                        self.assertEqual(raised.exception.code, "HOST_SOURCE_ACK_INVALID")
                # Every record remains redeliverable from the original cursor.
                self.assertEqual(
                    tuple(
                        item.source_sequence
                        for item in store.poll(consumer_id="shadow", limit=10)
                    ),
                    (0, 1, 2),
                )
                store.acknowledge(
                    consumer_id="shadow",
                    event_ids=(records[0].event_id,),
                    acknowledged_at_utc="2026-08-12T00:01:00Z",
                )
                self.assertEqual(
                    tuple(
                        item.source_sequence
                        for item in store.poll(consumer_id="shadow", limit=10)
                    ),
                    (1, 2),
                )

    def test_second_store_on_the_same_path_is_refused_while_first_is_live(self) -> None:
        """Exclusive-writer ownership is deterministic, not best effort."""

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with self._store(path) as store:
                store.commit_response(
                    _observation(0), committed_at_utc="2026-08-12T00:00:00Z"
                )
                with self.assertRaises(HostResponseCommitError) as raised:
                    self._store(path)
                self.assertEqual(raised.exception.code, "HOST_SOURCE_STORE_BUSY")
                # The live store is unaffected by the refused open.
                self.assertEqual(
                    store.commit_response(
                        _observation(1), committed_at_utc="2026-08-12T00:00:01Z"
                    ).source_sequence,
                    1,
                )
            # Once released, a fresh exclusive open succeeds.
            with self._store(path) as reopened:
                self.assertEqual(
                    reopened.poll(consumer_id="shadow", limit=10)[-1].source_sequence, 1
                )

    def test_read_only_metadata_adapter_reconstructs_canonical_identity(self) -> None:
        provider = InjectedReadOnlyCaptureMetadataProvider(
            lambda: {
                "milvus_uri": "http://localhost:19530",
                "deployment_identity": "offline-deployment",
                "collection_name": "collection",
                "dimensions": 2,
                "metric": Metric.L2,
                "hnsw_index_identity": "hnsw",
                "data_identity": "data",
                "source_revision": "revision",
                "observed_at_utc": "2026-08-12T00:00:00Z",
                "environment_manifest": {"mode": "offline"},
            }
        )
        metadata = provider.capture()
        self.assertEqual(metadata.dimensions, 2)
        self.assertEqual(metadata.metric, Metric.L2)


if __name__ == "__main__":
    unittest.main()
