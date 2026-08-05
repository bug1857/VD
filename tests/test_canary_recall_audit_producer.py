"""TDD coverage for the EXP-009 deterministic-fake-client recall-audit producer.

This is the first real caller the recall-audit ledger/evaluator/binding
machinery has ever had: it executes the frozen 1,200-query DATASET-002
recall-audit population against an injected client and durably populates
``CanaryRecallAuditLedger`` under one verified ``Stage4EvidenceBinding``. The
query source and oracle mapping used here are lightweight fakes satisfying
the same narrow protocols the real, already-verified DATASET-002 objects
satisfy (``Dataset002CanaryQuerySource.recall_audit_vector`` and
``dataset002.load_recall_audit_oracle_ids``); those objects have their own
dedicated test coverage and are not re-verified here.
"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from vdbench.canary_recall_audit_evaluation import EvaluationStatus, evaluate_recall_audit_evidence
from vdbench.canary_recall_audit_ledger import CanaryRecallAuditLedger, RecallAuditObservation
from vdbench.canary_recall_audit_producer import (
    FakeDeterministicRecallAuditClient,
    RecallAuditClientOutcome,
    Stage4RecallAuditProducer,
)
from vdbench.canary_stage4_evidence_binding import Stage4EvidenceBinding
from vdbench.canary_statistics import EXP009_RECALL_AUDIT_COUNT, EXP009_ROUTING_POPULATION_COUNT
from vdbench.canary_workload import WorkloadIdentityBinding
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.dataset002 import DATASET002_SCHEMA_VERSION


def _sha(value: str | bytes) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(data).hexdigest()


_FROZEN_IDS = frozenset(
    range(EXP009_ROUTING_POPULATION_COUNT, EXP009_ROUTING_POPULATION_COUNT + EXP009_RECALL_AUDIT_COUNT)
)
_IDENTITY = WorkloadIdentityBinding(
    configuration_identity="a" * 16,
    data_identity="DATASET-001-v1:sha256:" + "b" * 64,
    flat_binding_id="c" * 16,
    hnsw_binding_id="d" * 16,
)
_SEARCH_CONFIGURATION = SearchConfiguration(
    metric=Metric.L2,
    threshold_label="target-075",
    radius=0.6,
    index_track=IndexTrack.HNSW,
    ef=800,
    limit=100,
    consistency_level="Strong",
)


def _oracle_map(*, per_query_count: int = 3) -> dict[int, tuple[int, ...]]:
    """A small, deterministic, distinct-per-query oracle ground truth."""

    return {query_id: tuple(range(query_id, query_id + per_query_count)) for query_id in _FROZEN_IDS}


def _build_binding(**overrides: object) -> Stage4EvidenceBinding:
    fields: dict[str, object] = dict(
        run_id="fake-producer-run",
        source_revision="0" * 40,
        metric=_SEARCH_CONFIGURATION.metric,
        threshold_stratum=_SEARCH_CONFIGURATION.threshold_label,
        current_ef=400,
        candidate_ef=_SEARCH_CONFIGURATION.ef,
        last_known_good_ef=400,
        identity=_IDENTITY,
        dataset002_manifest_sha256=_sha("dataset002"),
        frozen_recall_audit_ids_sha256=_sha(",".join(str(i) for i in sorted(_FROZEN_IDS))),
        eligible_workload_sha256=_sha("eligible-workload"),
        candidate_selection_sha256=_sha("candidate-selection"),
        execution_schedule_sha256=_sha("execution-schedule"),
        recall_evidence_schema_version="recall-audit-hoeffding-1200-v1",
        latency_evidence_schema_version="exp009-stage4-execution-schedule-v1",
    )
    fields.update(overrides)
    return Stage4EvidenceBinding(**fields)


class _FakeVectorSource:
    """A trivial, deterministic RecallAuditVectorSourceLike -- not DATASET-002."""

    def recall_audit_vector(self, *, query_id: int) -> tuple[float, ...]:
        if query_id not in _FROZEN_IDS:
            raise KeyError(query_id)
        return (float(query_id), 0.0, 0.0)


class _ExplodingVectorSource:
    def recall_audit_vector(self, *, query_id: int) -> tuple[float, ...]:
        raise RuntimeError("vector source is unavailable")


class _NeverCalled:
    """Fails the test immediately if any method on it is ever invoked."""

    def __getattr__(self, name: str) -> object:
        def _fail(*args: object, **kwargs: object) -> object:
            raise AssertionError(f"{name} must never be called")

        return _fail


class _FailAfterNClient:
    """Succeeds for the first ``n`` calls, then reports an explicit failure."""

    def __init__(self, oracle_map: dict[int, tuple[int, ...]], *, n: int) -> None:
        self._oracle_map = oracle_map
        self._n = n
        self._calls = 0

    def execute(self, *, query_id: int, query_vector: tuple[float, ...]) -> RecallAuditClientOutcome:
        self._calls += 1
        if self._calls > self._n:
            return RecallAuditClientOutcome(
                success=False, candidate_result_ids=None, reason_code="SIMULATED_CLIENT_FAILURE"
            )
        return RecallAuditClientOutcome(
            success=True, candidate_result_ids=self._oracle_map[query_id], reason_code=None
        )


class _RaisingClient:
    def execute(self, *, query_id: int, query_vector: tuple[float, ...]) -> RecallAuditClientOutcome:
        raise RuntimeError("client is unavailable")


class _MalformedOutcomeClient:
    def execute(self, *, query_id: int, query_vector: tuple[float, ...]) -> object:
        return {"not": "an outcome"}


class _DuplicateCandidateIdsClient:
    def execute(self, *, query_id: int, query_vector: tuple[float, ...]) -> RecallAuditClientOutcome:
        return RecallAuditClientOutcome(
            success=True, candidate_result_ids=(1, 1, 2), reason_code=None
        )


def _clock(value: str = "2026-08-05T00:00:00Z"):
    return lambda: value


class Stage4RecallAuditProducerConstructionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)
        self.ledger_path = Path(self._tempdir.name) / "recall_audit.sqlite3"
        self.binding = _build_binding()
        self.oracle_map = _oracle_map()

    def _ledger(self, *, binding: Stage4EvidenceBinding | None = None) -> CanaryRecallAuditLedger:
        bound = binding or self.binding
        return CanaryRecallAuditLedger(
            self.ledger_path, run_id=bound.run_id, binding_sha256=bound.sha256
        )

    def test_ledger_binding_mismatch_is_rejected_before_any_client_call(self) -> None:
        # A different binding => different run_id => a distinct ledger path,
        # so the ledger itself must be constructed against the *other*
        # binding to prove cross-run evidence reuse cannot slip through the
        # producer's own construction-time check.
        other_binding = _build_binding(run_id="a-completely-different-run")
        ledger = self._ledger(binding=other_binding)
        with self.assertRaises(ValueError) as ctx:
            Stage4RecallAuditProducer(
                binding=self.binding,
                search_configuration=_SEARCH_CONFIGURATION,
                dataset002_schema_version=DATASET002_SCHEMA_VERSION,
                query_source=_NeverCalled(),
                oracle_result_ids_by_query_id=self.oracle_map,
                client=_NeverCalled(),
                ledger=ledger,
                utc_now=_clock(),
            )
        self.assertEqual(str(ctx.exception), "LEDGER_BINDING_MISMATCH")

    def test_search_configuration_ef_mismatch_is_rejected(self) -> None:
        mismatched = SearchConfiguration(
            metric=_SEARCH_CONFIGURATION.metric,
            threshold_label=_SEARCH_CONFIGURATION.threshold_label,
            radius=_SEARCH_CONFIGURATION.radius,
            index_track=IndexTrack.HNSW,
            ef=400,  # binding.candidate_ef is 800
            limit=100,
            consistency_level="Strong",
        )
        with self.assertRaises(ValueError) as ctx:
            Stage4RecallAuditProducer(
                binding=self.binding,
                search_configuration=mismatched,
                dataset002_schema_version=DATASET002_SCHEMA_VERSION,
                query_source=_NeverCalled(),
                oracle_result_ids_by_query_id=self.oracle_map,
                client=_NeverCalled(),
                ledger=self._ledger(),
                utc_now=_clock(),
            )
        self.assertEqual(str(ctx.exception), "SEARCH_CONFIGURATION_BINDING_MISMATCH")

    def test_wrong_dataset002_schema_version_is_rejected(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            Stage4RecallAuditProducer(
                binding=self.binding,
                search_configuration=_SEARCH_CONFIGURATION,
                dataset002_schema_version=DATASET002_SCHEMA_VERSION + 1,
                query_source=_NeverCalled(),
                oracle_result_ids_by_query_id=self.oracle_map,
                client=_NeverCalled(),
                ledger=self._ledger(),
                utc_now=_clock(),
            )
        self.assertEqual(str(ctx.exception), "DATASET002_SCHEMA_VERSION_MISMATCH")

    def test_incomplete_oracle_population_is_rejected(self) -> None:
        incomplete = dict(self.oracle_map)
        del incomplete[next(iter(_FROZEN_IDS))]
        with self.assertRaises(ValueError) as ctx:
            Stage4RecallAuditProducer(
                binding=self.binding,
                search_configuration=_SEARCH_CONFIGURATION,
                dataset002_schema_version=DATASET002_SCHEMA_VERSION,
                query_source=_NeverCalled(),
                oracle_result_ids_by_query_id=incomplete,
                client=_NeverCalled(),
                ledger=self._ledger(),
                utc_now=_clock(),
            )
        self.assertEqual(str(ctx.exception), "ORACLE_RESULT_POPULATION_INVALID")

    def test_oracle_population_with_a_foreign_extra_id_is_rejected(self) -> None:
        wrong = dict(self.oracle_map)
        wrong[999999] = (1, 2, 3)
        with self.assertRaises(ValueError) as ctx:
            Stage4RecallAuditProducer(
                binding=self.binding,
                search_configuration=_SEARCH_CONFIGURATION,
                dataset002_schema_version=DATASET002_SCHEMA_VERSION,
                query_source=_NeverCalled(),
                oracle_result_ids_by_query_id=wrong,
                client=_NeverCalled(),
                ledger=self._ledger(),
                utc_now=_clock(),
            )
        self.assertEqual(str(ctx.exception), "ORACLE_RESULT_POPULATION_INVALID")


class Stage4RecallAuditProducerRunTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)
        self.ledger_path = Path(self._tempdir.name) / "recall_audit.sqlite3"
        self.binding = _build_binding()
        self.oracle_map = _oracle_map()

    def _ledger(self) -> CanaryRecallAuditLedger:
        return CanaryRecallAuditLedger(
            self.ledger_path, run_id=self.binding.run_id, binding_sha256=self.binding.sha256
        )

    def _producer(
        self, *, client, ledger=None, oracle_map=None, query_source=None, utc_now=None
    ) -> Stage4RecallAuditProducer:
        return Stage4RecallAuditProducer(
            binding=self.binding,
            search_configuration=_SEARCH_CONFIGURATION,
            dataset002_schema_version=DATASET002_SCHEMA_VERSION,
            query_source=query_source or _FakeVectorSource(),
            oracle_result_ids_by_query_id=oracle_map if oracle_map is not None else self.oracle_map,
            client=client,
            ledger=ledger or self._ledger(),
            utc_now=utc_now or _clock(),
        )

    def test_full_production_is_complete_bound_and_evaluator_compatible(self) -> None:
        ledger = self._ledger()
        client = FakeDeterministicRecallAuditClient(self.oracle_map)
        producer = self._producer(client=client, ledger=ledger)

        result = producer.run()

        self.assertTrue(result.completed, result.reason_codes)
        self.assertEqual(result.dispatched_query_count, EXP009_RECALL_AUDIT_COUNT)
        self.assertEqual(result.already_present_query_count, 0)
        self.assertEqual(result.reason_codes, ())
        self.assertIsNone(result.failed_query_id)

        chain_state = ledger.chain_state()
        self.assertEqual(chain_state.record_count, EXP009_RECALL_AUDIT_COUNT)

        observations = ledger.records()
        self.assertEqual(len(observations), EXP009_RECALL_AUDIT_COUNT)
        self.assertTrue(all(observation.producer_run_id == self.binding.run_id for observation in observations))
        # The fake client returns the oracle set verbatim -> perfect recall.
        self.assertTrue(all(observation.capped_recall == 1.0 for observation in observations))

        evaluation = evaluate_recall_audit_evidence(
            expected_query_ids=_FROZEN_IDS,
            search_configuration=_SEARCH_CONFIGURATION,
            identity=_IDENTITY,
            dataset002_manifest_sha256=self.binding.dataset002_manifest_sha256,
            dataset002_schema_version=DATASET002_SCHEMA_VERSION,
            observations=observations,
            binding=self.binding,
            frozen_query_ids_sha256=self.binding.frozen_recall_audit_ids_sha256,
        )
        self.assertEqual(evaluation.status, EvaluationStatus.PASSING)
        self.assertTrue(evaluation.recall_audit_complete_and_passing)
        self.assertIsNotNone(evaluation.evidence_digest)
        self.assertEqual(evaluation.evidence_binding_sha256, self.binding.sha256)

    def test_deterministic_repeated_runs_produce_identical_scoring_content(self) -> None:
        bounded = 25

        def _run_once(*, clock_value: str) -> tuple[tuple[int, str, str], ...]:
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "ledger.sqlite3"
                ledger = CanaryRecallAuditLedger(
                    path, run_id=self.binding.run_id, binding_sha256=self.binding.sha256
                )
                producer = Stage4RecallAuditProducer(
                    binding=self.binding,
                    search_configuration=_SEARCH_CONFIGURATION,
                    dataset002_schema_version=DATASET002_SCHEMA_VERSION,
                    query_source=_FakeVectorSource(),
                    oracle_result_ids_by_query_id=self.oracle_map,
                    client=FakeDeterministicRecallAuditClient(self.oracle_map),
                    ledger=ledger,
                    utc_now=_clock(clock_value),
                )
                result = producer.run(max_queries=bounded)
                self.assertEqual(result.dispatched_query_count, bounded)
                observations = ledger.records()
                return tuple(
                    sorted(
                        (o.query_id, o.oracle_result_sha256, o.candidate_result_sha256)
                        for o in observations
                    )
                )

        first = _run_once(clock_value="2026-08-05T00:00:00Z")
        second = _run_once(clock_value="2026-08-06T12:30:00Z")

        self.assertEqual(first, second)

    def test_partial_client_failure_halts_at_exact_query_with_durable_partial_evidence(self) -> None:
        ledger = self._ledger()
        client = _FailAfterNClient(self.oracle_map, n=5)
        producer = self._producer(client=client, ledger=ledger)

        result = producer.run()

        self.assertFalse(result.completed)
        self.assertEqual(result.dispatched_query_count, 5)
        self.assertEqual(result.reason_codes, ("SIMULATED_CLIENT_FAILURE",))
        expected_failed_id = sorted(_FROZEN_IDS)[5]
        self.assertEqual(result.failed_query_id, expected_failed_id)
        self.assertEqual(ledger.chain_state().record_count, 5)

    def test_client_exception_halts_before_any_ledger_write(self) -> None:
        ledger = self._ledger()
        producer = self._producer(client=_RaisingClient(), ledger=ledger)

        result = producer.run(max_queries=1)

        self.assertFalse(result.completed)
        self.assertEqual(result.reason_codes, ("CLIENT_EXCEPTION",))
        self.assertEqual(result.dispatched_query_count, 0)
        self.assertEqual(ledger.chain_state().record_count, 0)

    def test_malformed_non_outcome_client_response_halts_before_any_ledger_write(self) -> None:
        ledger = self._ledger()
        producer = self._producer(client=_MalformedOutcomeClient(), ledger=ledger)

        result = producer.run(max_queries=1)

        self.assertFalse(result.completed)
        self.assertEqual(result.reason_codes, ("CLIENT_OUTCOME_INVALID",))
        self.assertEqual(ledger.chain_state().record_count, 0)

    def test_duplicate_candidate_ids_from_client_are_rejected_not_silently_accepted(self) -> None:
        ledger = self._ledger()
        producer = self._producer(client=_DuplicateCandidateIdsClient(), ledger=ledger)

        result = producer.run(max_queries=1)

        self.assertFalse(result.completed)
        self.assertEqual(result.reason_codes, ("OBSERVATION_CONSTRUCTION_FAILED",))
        self.assertEqual(ledger.chain_state().record_count, 0)

    def test_vector_source_failure_halts_before_any_client_call(self) -> None:
        ledger = self._ledger()
        producer = self._producer(
            client=_NeverCalled(), ledger=ledger, query_source=_ExplodingVectorSource()
        )

        result = producer.run(max_queries=1)

        self.assertFalse(result.completed)
        self.assertEqual(result.reason_codes, ("VECTOR_SOURCE_FAILURE",))
        self.assertEqual(ledger.chain_state().record_count, 0)

    def test_restart_resumes_from_already_present_queries_without_reprocessing_them(self) -> None:
        ledger = self._ledger()
        client = FakeDeterministicRecallAuditClient(self.oracle_map)

        first_producer = self._producer(client=client, ledger=ledger)
        first_result = first_producer.run(max_queries=3)
        self.assertEqual(first_result.dispatched_query_count, 3)
        self.assertEqual(first_result.already_present_query_count, 0)

        second_producer = self._producer(client=client, ledger=ledger)
        second_result = second_producer.run(max_queries=5)

        self.assertEqual(second_result.already_present_query_count, 3)
        self.assertEqual(second_result.dispatched_query_count, 5)
        self.assertEqual(ledger.chain_state().record_count, 8)

        processed_ids = sorted(observation.query_id for observation in ledger.records())
        self.assertEqual(processed_ids, sorted(_FROZEN_IDS)[:8])

    def test_reprocessing_an_already_present_query_with_identical_content_is_idempotent(self) -> None:
        # Exercises the per-query step directly: run()'s own resume logic
        # never revisits an already-present query_id, so this simulates the
        # narrow race window where two producer invocations could otherwise
        # both attempt the same not-yet-seen query.
        ledger = self._ledger()
        client = FakeDeterministicRecallAuditClient(self.oracle_map)
        producer = self._producer(client=client, ledger=ledger)
        first_id = sorted(_FROZEN_IDS)[0]

        self.assertIsNone(producer._process_one(first_id))
        self.assertIsNone(producer._process_one(first_id))

        self.assertEqual(ledger.chain_state().record_count, 1)

    def test_reprocessing_an_already_present_query_with_conflicting_content_is_refused(self) -> None:
        ledger = self._ledger()
        first_id = sorted(_FROZEN_IDS)[0]
        client_a = FakeDeterministicRecallAuditClient(self.oracle_map)
        client_b = FakeDeterministicRecallAuditClient(self.oracle_map, max_candidates_per_query=1)
        producer_a = self._producer(client=client_a, ledger=ledger)
        producer_b = self._producer(client=client_b, ledger=ledger)

        self.assertIsNone(producer_a._process_one(first_id))
        reason = producer_b._process_one(first_id)

        self.assertEqual(reason, "QUERY_ID_CONFLICTING_DUPLICATE")
        self.assertEqual(ledger.chain_state().record_count, 1)

    def test_final_run_reports_exact_frozen_population_count(self) -> None:
        ledger = self._ledger()
        client = FakeDeterministicRecallAuditClient(self.oracle_map)
        producer = self._producer(client=client, ledger=ledger)

        result = producer.run()

        self.assertEqual(
            result.dispatched_query_count + result.already_present_query_count,
            EXP009_RECALL_AUDIT_COUNT,
        )
        self.assertEqual(ledger.chain_state().record_count, EXP009_RECALL_AUDIT_COUNT)


if __name__ == "__main__":
    unittest.main()
