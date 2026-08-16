"""TDD coverage for the end-to-end LKG-qualification producer.

Exercises DATASET-003 -> LkgQualificationRunner (LkgMilvusAdapter) ->
LkgQualificationLedger through LkgQualificationProducer, using a
deterministic fake Milvus client only -- the fake is test-only and is never
substituted for the production LkgMilvusAdapter/LkgQualificationRunner.from_uri
adapter anywhere in this file.
"""

from __future__ import annotations

import ast
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np

from vdbench.artifacts import write_dataset_artifacts
from vdbench.config import (
    EXP001_DATASET_SPEC,
    IndexTrack,
    Metric,
    SearchConfiguration,
)
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import (
    DATASET002_SPEC,
    generate_dataset002,
    write_dataset002_artifacts,
)
from vdbench.dataset003 import (
    Dataset003Spec,
    generate_dataset003,
    write_dataset003_artifacts,
)
from vdbench.lkg_dataset003_loader import (
    LkgDataset003Workload,
    load_dataset003_workload,
)
from vdbench.lkg_milvus_adapter import LkgMilvusAdapter
from vdbench.lkg_qualification_evidence import LkgAttemptStatus
from vdbench.lkg_qualification_ledger import (
    LkgQualificationLedger,
)
from vdbench.lkg_qualification_producer import (
    LkgQualificationProducer,
    LkgQualificationProducerResult,
)
from vdbench.lkg_qualification_runner import LkgQualificationRunner
from vdbench.lkg_run_binding import LkgRunBinding, lkg_ordered_query_ids_sha256
from vdbench.oracle import exact_range_search

REPOSITORY = Path(__file__).parents[1]
PRODUCER_MODULE_PATH = REPOSITORY / "src" / "vdbench" / "lkg_qualification_producer.py"
HNSW_NAME = "lkg_l2_hnsw"


def _small_dataset001(path: Path) -> np.ndarray:
    spec = replace(
        EXP001_DATASET_SPEC,
        version="dataset001-fixture-v1",
        dimensions=4,
        base_count=100,
        calibration_query_count=5,
        measured_query_count=7,
    )
    bundle = generate_dataset(spec)
    write_dataset_artifacts(
        path,
        bundle,
        calibrate_thresholds(bundle.base_vectors, bundle.calibration_queries),
        boundary_fixtures(),
    )
    return bundle


def _small_dataset002(path: Path, *, dataset001_dir: Path) -> None:
    spec = replace(
        DATASET002_SPEC,
        version="dataset002-fixture-v1",
        dimensions=4,
        seed=20260809,
        routing_query_count=6,
        recall_audit_query_count=12,
    )
    write_dataset002_artifacts(path, generate_dataset002(spec), dataset001_dir=dataset001_dir)


_DATASET003_SPEC = Dataset003Spec(
    dataset_id="DATASET-003",
    version="dataset003-fixture-v1",
    seed=20260806,
    dimensions=4,
    lkg_qualification_query_count=9,
    dtype="<f4",
    distribution="independent standard normal",
    generator="numpy.random.Generator(numpy.random.PCG64(seed))",
)


class FakeLkgMilvusClient:
    """Deterministic in-memory search: same oracle math the runner itself uses."""

    def __init__(self, *, base_ids: np.ndarray, base_vectors: np.ndarray) -> None:
        self.base_ids = base_ids
        self.base_vectors = base_vectors
        self.search_calls: list[dict[str, object]] = []
        self.injected_exception: Exception | None = None
        self.duplicate_ids = False
        self.empty_batch = False
        self.fail_for_query_ids: set[int] = set()

    def search(self, **kwargs):
        self.search_calls.append(kwargs)
        if self.injected_exception is not None:
            raise self.injected_exception
        if self.empty_batch:
            return []
        vector = np.asarray(kwargs["data"][0], dtype="<f4")
        parameters = kwargs["search_params"]["params"]
        metric = Metric(kwargs["search_params"]["metric_type"])
        reference = exact_range_search(
            self.base_vectors,
            self.base_ids,
            vector,
            metric,
            radius=float(parameters["radius"]),
            range_filter=float(parameters["range_filter"]),
            limit=int(kwargs["limit"]),
        )
        hits = [{"id": hit.id, "distance": hit.score} for hit in reference.hits]
        if self.duplicate_ids and hits:
            hits.append(dict(hits[0]))
        return [hits]


class LkgQualificationProducerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self.dataset001_dir = root / "dataset001"
        self.dataset002_dir = root / "dataset002"
        self.dataset003_dir = root / "dataset003"
        self.bundle001 = _small_dataset001(self.dataset001_dir)
        _small_dataset002(self.dataset002_dir, dataset001_dir=self.dataset001_dir)
        write_dataset003_artifacts(
            self.dataset003_dir,
            generate_dataset003(_DATASET003_SPEC),
            dataset001_dir=self.dataset001_dir,
            dataset002_dir=self.dataset002_dir,
        )
        self.workload = load_dataset003_workload(
            self.dataset003_dir,
            dataset001_dir=self.dataset001_dir,
            dataset002_dir=self.dataset002_dir,
        )
        self.search_configuration = SearchConfiguration(
            metric=Metric.L2,
            threshold_label="target-075",
            radius=5.0,
            index_track=IndexTrack.HNSW,
            ef=400,
        )
        self.run_binding = self._binding()
        self.client = FakeLkgMilvusClient(
            base_ids=self.bundle001.ids, base_vectors=self.bundle001.base_vectors
        )
        self.runner = self._runner(self.client)
        self.db_path = root / "lkg.sqlite3"
        self.ledger = LkgQualificationLedger(
            self.db_path,
            run_binding=self.run_binding,
            ordered_query_ids=self.workload.query_ids,
        )

    def _binding(self, **overrides) -> LkgRunBinding:
        fields = {
            "run_id": "run-1",
            "producer_identity": "producer-v1",
            "search_configuration": self.search_configuration,
            "collection_name": HNSW_NAME,
            "base_data_identity": "data-v1",
            "index_identity": "index-v1",
            "qualification_dataset_id": self.workload.dataset_id,
            "qualification_dataset_version": self.workload.dataset_version,
            "qualification_manifest_sha256": self.workload.manifest_sha256,
            "qualification_query_role": self.workload.query_role,
            "qualification_query_id_array_sha256": self.workload.query_id_array_sha256,
            "qualification_ordered_query_ids_sha256": lkg_ordered_query_ids_sha256(
                self.workload.query_ids
            ),
            "qualification_query_array_sha256": self.workload.query_array_sha256,
            "qualification_expected_query_count": len(self.workload.query_ids),
            "environment_identity": "env-v1",
            "source_revision": "deadbeef",
        }
        fields.update(overrides)
        return LkgRunBinding(**fields)

    def _runner(self, client, clock_ns=None) -> LkgQualificationRunner:
        kwargs = {"dimensions": 4, "hnsw_collection_name": HNSW_NAME}
        if clock_ns is not None:
            kwargs["clock_ns"] = clock_ns
        adapter = LkgMilvusAdapter(client, **kwargs)
        return LkgQualificationRunner(
            adapter, base_vectors=self.bundle001.base_vectors, base_ids=self.bundle001.ids
        )

    def _producer(self, *, ledger=None, runner=None, workload=None, run_binding=None) -> LkgQualificationProducer:
        return LkgQualificationProducer(
            run_binding=run_binding or self.run_binding,
            workload=workload or self.workload,
            runner=runner or self.runner,
            ledger=ledger or self.ledger,
        )

    def _reopen_ledger(self, run_binding=None) -> LkgQualificationLedger:
        return LkgQualificationLedger(
            self.db_path,
            run_binding=run_binding or self.run_binding,
            ordered_query_ids=self.workload.query_ids,
        )

    # -- end-to-end happy path ---------------------------------------------------

    def test_full_run_dispatches_every_query_and_completes(self) -> None:
        producer = self._producer()
        result = producer.run()
        self.assertIsInstance(result, LkgQualificationProducerResult)
        self.assertTrue(result.completed)
        self.assertEqual(result.dispatched_query_count, 9)
        self.assertEqual(result.already_present_query_count, 0)
        self.assertEqual(result.reason_codes, ())
        records = self.ledger.records()
        self.assertEqual(len(records), 9)
        self.assertTrue(all(r.status is LkgAttemptStatus.SUCCESS for r in records))

    # -- restart and partial-run continuation ------------------------------------

    def test_restart_resumes_from_already_succeeded_queries(self) -> None:
        first = self._producer()
        first.run(max_queries=4)
        self.assertEqual(len(self.ledger.records()), 4)

        resumed_ledger = self._reopen_ledger()
        resumed = self._producer(ledger=resumed_ledger)
        result = resumed.run()
        self.assertTrue(result.completed)
        self.assertEqual(result.already_present_query_count, 4)
        self.assertEqual(result.dispatched_query_count, 5)
        self.assertEqual(len(resumed_ledger.records()), 9)

    def test_completed_run_resumed_again_dispatches_nothing(self) -> None:
        self._producer().run()
        resumed = self._producer(ledger=self._reopen_ledger())
        result = resumed.run()
        self.assertTrue(result.completed)
        self.assertEqual(result.dispatched_query_count, 0)
        self.assertEqual(result.already_present_query_count, 9)

    def test_restart_after_a_failed_attempt_retries_as_attempt_two(self) -> None:
        self.client.injected_exception = TimeoutError("first attempt fails")
        first = self._producer()
        result = first.run(max_queries=1)
        self.assertFalse(result.completed)
        self.assertEqual(result.reason_codes, ("TIMEOUT",))
        records = self.ledger.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].attempt_number, 1)
        self.assertEqual(records[0].status, LkgAttemptStatus.TIMEOUT)

        self.client.injected_exception = None
        second = self._producer(ledger=self._reopen_ledger())
        result2 = second.run()
        self.assertTrue(result2.completed)
        records2 = self.ledger.records()
        first_query_records = [r for r in records2 if r.query_id == self.workload.query_ids[0]]
        self.assertEqual(len(first_query_records), 2)
        self.assertEqual([r.attempt_number for r in first_query_records], [1, 2])
        self.assertEqual(first_query_records[0].status, LkgAttemptStatus.TIMEOUT)
        self.assertEqual(first_query_records[1].status, LkgAttemptStatus.SUCCESS)

    # -- identical-retry idempotency ----------------------------------------------

    def test_identical_retry_after_partial_ledger_write_is_idempotent(self) -> None:
        deterministic_runner = self._runner(self.client, clock_ns=lambda: 1_000)
        first = self._producer(runner=deterministic_runner)
        first.run(max_queries=1)
        state_before = self.ledger.chain_state()

        third = self._producer(
            ledger=self._reopen_ledger(), runner=deterministic_runner
        )
        query_id = self.workload.query_ids[0]
        reason = third._process_one(
            query_id=query_id, attempt_sequence=0, attempt_number=1
        )
        self.assertIsNone(reason)
        # Idempotent replay: byte-identical content under the same
        # (query_id, attempt_number) key does not add a new row.
        self.assertEqual(self.ledger.chain_state().record_count, state_before.record_count)

    # -- conflicting-retry rejection (a real ledger append refusal) ---------------

    def test_conflicting_retry_halts_with_the_ledger_conflict_reason(self) -> None:
        """A query_id already present with different content (e.g. a
        concurrent writer's result) must halt the run, not silently
        overwrite or duplicate the original evidence. This is a real
        LkgQualificationLedger.append() soft refusal
        (accepted=False), never conflated with a genuine
        LkgQualificationLedgerError append failure."""

        query_id = self.workload.query_ids[0]
        conflicting_client = FakeLkgMilvusClient(
            base_ids=self.bundle001.ids, base_vectors=self.bundle001.base_vectors
        )
        conflicting_runner = self._runner(
            conflicting_client, clock_ns=lambda counter=iter((5_000, 6_000)): next(counter)  # the iterator default is the fixture under test  # noqa: B008
        )
        seeding_producer = self._producer(runner=conflicting_runner)
        seeding_producer._process_one(query_id=query_id, attempt_sequence=0, attempt_number=1)
        self.assertEqual(len(self.ledger.records()), 1)

        differing_runner = self._runner(
            self.client, clock_ns=lambda counter=iter((7_000, 9_000)): next(counter)  # the iterator default is the fixture under test  # noqa: B008
        )
        racing_producer = self._producer(ledger=self._reopen_ledger(), runner=differing_runner)
        reason = racing_producer._process_one(
            query_id=query_id, attempt_sequence=0, attempt_number=1
        )
        self.assertEqual(reason, "QUERY_ID_CONFLICTING_DUPLICATE")
        self.assertEqual(len(self.ledger.records()), 1)

    # -- blocker 2: genuine append failure, distinct from conflicting retry ------

    def test_genuine_append_failure_is_a_distinct_halt_reason(self) -> None:
        """The lock is held only around the append itself (not the whole
        run(), which also needs to read the ledger first) -- isolating
        exactly the append-failure code path from a broader ledger
        unavailability."""

        locked_ledger = LkgQualificationLedger(
            Path(self._tmp.name) / "locked.sqlite3",
            run_binding=self.run_binding,
            ordered_query_ids=self.workload.query_ids,
            lock_timeout_seconds=0.05,
        )
        producer = self._producer(ledger=locked_ledger)
        query_id = self.workload.query_ids[0]

        lock_connection = sqlite3.connect(locked_ledger.path)
        lock_connection.execute("BEGIN EXCLUSIVE")
        try:
            reason = producer._process_one(
                query_id=query_id, attempt_sequence=0, attempt_number=1
            )
        finally:
            lock_connection.rollback()
            lock_connection.close()

        self.assertIsNotNone(reason)
        self.assertTrue(reason.startswith("APPEND_FAILURE:"))
        self.assertNotEqual(reason, "QUERY_ID_CONFLICTING_DUPLICATE")

    def test_append_failure_chain_state_is_unchanged_and_restart_retries(self) -> None:
        locked_ledger = LkgQualificationLedger(
            Path(self._tmp.name) / "locked2.sqlite3",
            run_binding=self.run_binding,
            ordered_query_ids=self.workload.query_ids,
            lock_timeout_seconds=0.05,
        )
        state_before = locked_ledger.chain_state()
        producer = self._producer(ledger=locked_ledger)
        query_id = self.workload.query_ids[0]

        lock_connection = sqlite3.connect(locked_ledger.path)
        lock_connection.execute("BEGIN EXCLUSIVE")
        try:
            reason = producer._process_one(
                query_id=query_id, attempt_sequence=0, attempt_number=1
            )
        finally:
            lock_connection.rollback()
            lock_connection.close()

        self.assertIsNotNone(reason)
        self.assertEqual(locked_ledger.chain_state(), state_before)
        # Restart (lock released): the exact same attempt now succeeds.
        result = producer.run(max_queries=1)
        self.assertTrue(result.completed or result.dispatched_query_count == 1)
        self.assertEqual(len(locked_ledger.records()), 1)

    # -- client failure / timeout / malformed response / oracle failure ----------

    def test_client_exception_persists_a_failed_attempt_and_halts(self) -> None:
        self.client.injected_exception = RuntimeError("injected client failure")
        producer = self._producer()
        result = producer.run()
        self.assertFalse(result.completed)
        self.assertTrue(result.reason_codes[0].startswith("CLIENT_ERROR:"))
        self.assertEqual(result.dispatched_query_count, 0)
        records = self.ledger.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].status, LkgAttemptStatus.CLIENT_ERROR)
        self.assertIsNone(records[0].observation)

    def test_client_timeout_persists_a_typed_timeout_attempt(self) -> None:
        self.client.injected_exception = TimeoutError("injected timeout")
        producer = self._producer()
        result = producer.run()
        self.assertEqual(result.reason_codes, ("TIMEOUT",))
        records = self.ledger.records()
        self.assertEqual(records[0].status, LkgAttemptStatus.TIMEOUT)

    def test_malformed_response_persists_a_typed_malformed_response_attempt(self) -> None:
        self.client.empty_batch = True
        producer = self._producer()
        result = producer.run()
        self.assertFalse(result.completed)
        self.assertTrue(result.reason_codes[0].startswith("MALFORMED_RESPONSE:"))
        records = self.ledger.records()
        self.assertEqual(records[0].status, LkgAttemptStatus.MALFORMED_RESPONSE)

    def test_duplicate_returned_entity_ids_persist_a_malformed_response_attempt(self) -> None:
        self.client.duplicate_ids = True
        producer = self._producer()
        result = producer.run()
        self.assertFalse(result.completed)
        self.assertTrue(result.reason_codes[0].startswith("MALFORMED_RESPONSE:"))
        records = self.ledger.records()
        self.assertEqual(records[0].status, LkgAttemptStatus.MALFORMED_RESPONSE)

    def test_oracle_failure_persists_a_typed_oracle_error_attempt(self) -> None:
        broken_runner = LkgQualificationRunner(
            LkgMilvusAdapter(self.client, dimensions=4, hnsw_collection_name=HNSW_NAME),
            base_vectors=self.bundle001.base_vectors,
            base_ids=self.bundle001.ids[:-1],  # length mismatch -> oracle raises
        )
        producer = self._producer(runner=broken_runner)
        result = producer.run()
        self.assertFalse(result.completed)
        self.assertTrue(result.reason_codes[0].startswith("ORACLE_ERROR:"))
        records = self.ledger.records()
        self.assertEqual(records[0].status, LkgAttemptStatus.ORACLE_ERROR)
        self.assertIsNone(records[0].observation)

    # -- blocker 6: crash after search, before append -----------------------------

    def test_crash_before_append_leaves_no_evidence_and_restart_is_a_fresh_attempt(self) -> None:
        """Simulates: the search succeeds, but the process crashes before
        ledger.append() ever runs. Nothing is durably recorded, so a
        restarted producer must dispatch this query as attempt_number=1
        again -- never claim the unpersisted first result was "recovered"."""

        query_id = self.workload.query_ids[0]
        query_vector = self.workload.query_vectors[query_id]

        # The "crash": attempt_query succeeds, but we deliberately never
        # call ledger.append() for it -- exactly what a process crash
        # between search and append would leave behind (nothing).
        attempt = self.runner.attempt_query(
            query_id=query_id,
            query_vector=query_vector,
            metric=self.search_configuration.metric,
            threshold_stratum=self.search_configuration.threshold_label,
            ef=self.search_configuration.ef,
            radius=self.search_configuration.radius,
            attempt_sequence=0,
            attempt_number=1,
            run_binding_sha256=self.run_binding.sha256,
        )
        self.assertEqual(attempt.status, LkgAttemptStatus.SUCCESS)
        self.assertEqual(self.ledger.records(), ())  # nothing persisted -- the "crash"

        # A fresh producer instance ("restart") sees zero records for this
        # query and dispatches it as a brand-new attempt_number=1.
        restarted = self._producer(ledger=self._reopen_ledger())
        result = restarted.run(max_queries=1)
        self.assertTrue(result.completed or result.dispatched_query_count == 1)
        records = self.ledger.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].query_id, query_id)
        self.assertEqual(records[0].attempt_number, 1)  # a fresh attempt, not "recovered"
        self.assertEqual(records[0].status, LkgAttemptStatus.SUCCESS)

    def test_crash_before_append_does_not_double_count_search_calls_as_evidence(self) -> None:
        """The uncommitted first search happened against the live client,
        but since nothing was appended, it must never be counted among the
        durable evidence -- only the restart's own dispatch is."""

        query_id = self.workload.query_ids[0]
        query_vector = self.workload.query_vectors[query_id]
        self.runner.attempt_query(
            query_id=query_id,
            query_vector=query_vector,
            metric=self.search_configuration.metric,
            threshold_stratum=self.search_configuration.threshold_label,
            ef=self.search_configuration.ef,
            radius=self.search_configuration.radius,
            attempt_sequence=0,
            attempt_number=1,
            run_binding_sha256=self.run_binding.sha256,
        )
        calls_before_restart = len(self.client.search_calls)
        self.assertGreaterEqual(calls_before_restart, 1)

        restarted = self._producer(ledger=self._reopen_ledger())
        restarted.run(max_queries=1)
        # The restart issues its own, separate search call -- the crashed
        # attempt's call is not "replayed" from any cache.
        self.assertEqual(len(self.client.search_calls), calls_before_restart + 1)
        self.assertEqual(len(self.ledger.records()), 1)

    # -- duplicate query IDs (workload-level defensive check) -----------------------

    def test_duplicate_query_ids_in_workload_are_rejected_at_construction(self) -> None:
        tampered = LkgDataset003Workload(
            query_ids=(self.workload.query_ids[0], self.workload.query_ids[0]),
            query_vectors=self.workload.query_vectors,
            dataset_id=self.workload.dataset_id,
            dataset_version=self.workload.dataset_version,
            manifest_sha256=self.workload.manifest_sha256,
            query_role=self.workload.query_role,
            query_id_array_sha256=self.workload.query_id_array_sha256,
            query_array_sha256=self.workload.query_array_sha256,
        )
        with self.assertRaises(ValueError):
            self._producer(workload=tampered)
        self.assertEqual(self.client.search_calls, [])

    # -- blocker 4: complete DATASET-003 workload-binding mismatches, zero dispatch --

    def _tampered_workload(self, **overrides) -> LkgDataset003Workload:
        fields = {
            "query_ids": self.workload.query_ids,
            "query_vectors": self.workload.query_vectors,
            "dataset_id": self.workload.dataset_id,
            "dataset_version": self.workload.dataset_version,
            "manifest_sha256": self.workload.manifest_sha256,
            "query_role": self.workload.query_role,
            "query_id_array_sha256": self.workload.query_id_array_sha256,
            "query_array_sha256": self.workload.query_array_sha256,
        }
        fields.update(overrides)
        return LkgDataset003Workload(**fields)

    def test_altered_query_array_hash_is_rejected_with_zero_dispatch(self) -> None:
        tampered = self._tampered_workload(query_array_sha256="f" * 64)
        with self.assertRaises(ValueError):
            self._producer(workload=tampered)
        self.assertEqual(self.client.search_calls, [])

    def test_altered_vector_array_hash_binding_mismatch_is_rejected(self) -> None:
        """Binding-side variant: the run binding itself claims a different
        query-vector array hash than the (unmodified) workload it is
        checked against."""

        wrong_binding = self._binding(qualification_query_array_sha256="f" * 64)
        wrong_ledger = LkgQualificationLedger(
            Path(self._tmp.name) / "wrong_vec.sqlite3",
            run_binding=wrong_binding,
            ordered_query_ids=self.workload.query_ids,
        )
        with self.assertRaises(ValueError):
            self._producer(ledger=wrong_ledger, run_binding=wrong_binding)
        self.assertEqual(self.client.search_calls, [])

    def test_altered_query_id_array_hash_is_rejected_with_zero_dispatch(self) -> None:
        tampered = self._tampered_workload(query_id_array_sha256="f" * 64)
        with self.assertRaises(ValueError):
            self._producer(workload=tampered)
        self.assertEqual(self.client.search_calls, [])

    def test_altered_order_changes_the_id_array_hash_and_is_rejected(self) -> None:
        """Reordering the same set of IDs changes query_id_array_sha256
        (computed over the exact on-disk byte order), so the binding check
        (bound to the original order) rejects it -- zero dispatch."""

        reordered_ids = tuple(reversed(self.workload.query_ids))
        tampered = self._tampered_workload(
            query_ids=reordered_ids,
            query_id_array_sha256="f" * 64,  # stands in for "a different order's hash"
        )
        with self.assertRaises(ValueError):
            self._producer(workload=tampered)
        self.assertEqual(self.client.search_calls, [])

    def test_altered_count_is_rejected_with_zero_dispatch(self) -> None:
        truncated_ids = self.workload.query_ids[:-1]
        tampered = self._tampered_workload(query_ids=truncated_ids)
        with self.assertRaises(ValueError):
            self._producer(workload=tampered)
        self.assertEqual(self.client.search_calls, [])

    def test_altered_role_is_rejected_with_zero_dispatch(self) -> None:
        tampered = self._tampered_workload(query_role="routing")
        with self.assertRaises(ValueError):
            self._producer(workload=tampered)
        self.assertEqual(self.client.search_calls, [])

    def test_altered_manifest_is_rejected_with_zero_dispatch(self) -> None:
        tampered = self._tampered_workload(manifest_sha256="f" * 64)
        with self.assertRaises(ValueError):
            self._producer(workload=tampered)
        self.assertEqual(self.client.search_calls, [])

    def test_same_ids_different_vectors_is_rejected_with_zero_dispatch(self) -> None:
        """Same query-ID set (same query_id_array_sha256) but a different
        query-vector array -- the vector-array hash alone must catch this,
        proving the two hashes are checked independently."""

        altered_vectors = {
            query_id: (vector + 0.01) for query_id, vector in self.workload.query_vectors.items()
        }
        tampered = self._tampered_workload(
            query_vectors=altered_vectors,
            query_array_sha256="f" * 64,  # stands in for "the altered array's real hash"
        )
        with self.assertRaises(ValueError):
            self._producer(workload=tampered)
        self.assertEqual(self.client.search_calls, [])

    def test_altered_expected_count_binding_is_rejected_with_zero_dispatch(self) -> None:
        """A binding whose declared count no longer matches its own
        committed query-ID-array digest is now rejected by the ledger's
        own cryptographic check before it can even be opened -- an even
        earlier zero-dispatch failure than the producer's own count check.
        Either layer catching it satisfies "zero dispatch"."""

        wrong_binding = self._binding(qualification_expected_query_count=9_999)
        with self.assertRaises(ValueError):
            wrong_ledger = LkgQualificationLedger(
                Path(self._tmp.name) / "wrong_count.sqlite3",
                run_binding=wrong_binding,
                ordered_query_ids=tuple(range(9_999)),
            )
            self._producer(ledger=wrong_ledger, run_binding=wrong_binding)
        self.assertEqual(self.client.search_calls, [])

    # -- binding mismatch with zero dispatch (pre-existing categories) --------------

    def test_ledger_binding_mismatch_dispatches_nothing(self) -> None:
        other_binding = self._binding(producer_identity="other-producer")
        other_ledger = LkgQualificationLedger(
            Path(self._tmp.name) / "other.sqlite3",
            run_binding=other_binding,
            ordered_query_ids=self.workload.query_ids,
        )
        with self.assertRaises(ValueError):
            self._producer(ledger=other_ledger)
        self.assertEqual(self.client.search_calls, [])

    def test_flat_index_track_binding_is_rejected_at_construction(self) -> None:
        flat_configuration = replace(self.search_configuration, index_track=IndexTrack.FLAT, ef=None)
        flat_binding = self._binding(search_configuration=flat_configuration)
        flat_ledger = LkgQualificationLedger(
            Path(self._tmp.name) / "flat.sqlite3",
            run_binding=flat_binding,
            ordered_query_ids=self.workload.query_ids,
        )
        with self.assertRaises(ValueError):
            self._producer(ledger=flat_ledger, run_binding=flat_binding)
        self.assertEqual(self.client.search_calls, [])

    # -- max_queries validation -------------------------------------------------------

    def test_zero_max_queries_is_rejected(self) -> None:
        producer = self._producer()
        with self.assertRaises(ValueError):
            producer.run(max_queries=0)

    def test_negative_max_queries_is_rejected(self) -> None:
        producer = self._producer()
        with self.assertRaises(ValueError):
            producer.run(max_queries=-1)

    # -- structural: same-call provenance --------------------------------------------

    def test_producer_never_calls_low_level_constructors_directly(self) -> None:
        """The producer must derive every attempt exclusively through
        runner.attempt_query (bound to one real search call), never by
        calling build_lkg_query_observation/build_lkg_query_attempt with
        hand-supplied recall/latency/status values of its own."""

        source = PRODUCER_MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        called_names = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        self.assertNotIn("build_lkg_query_observation", called_names)
        self.assertNotIn("build_lkg_query_attempt", called_names)
        self.assertNotIn("build_lkg_query_observation", source)
        self.assertNotIn("build_lkg_query_attempt", source)


if __name__ == "__main__":
    unittest.main()
