"""Deterministic-fake-client producer for the EXP-009 1,200-query recall audit.

Purpose:
    Execute the frozen DATASET-002 recall-audit query population against an
    injected query client and populate a ``CanaryRecallAuditLedger`` under one
    already-verified ``Stage4EvidenceBinding``. This is the missing caller the
    ledger/evaluator/binding machinery has always required: those modules can
    accept and evaluate observations, but nothing before this module actually
    produced them.
Inputs:
    An immutable evidence binding, the matching full ``SearchConfiguration``,
    an already-verified DATASET-002 vector source, an already-verified oracle
    ground-truth mapping for this run's exact (metric, threshold) slice, an
    injected query client (fake or, in a future change, real), and a durable
    ledger already constructed against the same binding digest.
Outputs:
    Durable ``RecallAuditObservation`` rows in the ledger, or an explicit
    halted result naming exactly which query failed and why. Never a partial,
    silently-incomplete population presented as if it were complete.
Dependencies:
    ``canary_recall_audit_ledger`` and ``canary_stage4_evidence_binding``
    only. No PyMilvus, network, routing, policy, or actuation import -- the
    injected ``client`` is the only extension point a future real search
    integration would replace.
Failure modes:
    A binding/ledger/search-configuration/schema-version mismatch is rejected
    at construction, before any client call. A vector-source failure, client
    exception, client-reported failure, malformed client outcome, or a
    ledger-append refusal halts the run immediately at that exact query;
    nothing after it is attempted, and nothing already durable is lost. Restart is
    idempotent: a new producer instance against the same ledger resumes from
    exactly the queries not yet present, never re-appending them.
Scope:
    This module never scores recall or decides PASSING/FAILING -- that
    remains exclusively ``canary_recall_audit_evaluation``. It never imports
    PyMilvus and never claims a candidate route, approval, or actuation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Callable, Protocol

from .canary_recall_audit_ledger import CanaryRecallAuditLedger, RecallAuditObservation
from .canary_stage4_evidence_binding import Stage4EvidenceBinding
from .canary_statistics import EXP009_RECALL_AUDIT_COUNT, EXP009_ROUTING_POPULATION_COUNT
from .config import IndexTrack, SearchConfiguration
from .dataset002 import DATASET002_SCHEMA_VERSION


__all__ = [
    "FakeDeterministicRecallAuditClient",
    "RecallAuditClientOutcome",
    "RecallAuditQueryClientLike",
    "RecallAuditVectorSourceLike",
    "Stage4RecallAuditProducer",
    "Stage4RecallAuditProducerResult",
]


_FROZEN_RECALL_AUDIT_QUERY_IDS = frozenset(
    range(
        EXP009_ROUTING_POPULATION_COUNT,
        EXP009_ROUTING_POPULATION_COUNT + EXP009_RECALL_AUDIT_COUNT,
    )
)


class RecallAuditVectorSourceLike(Protocol):
    """The minimal existing DATASET-002 source shape required by this producer."""

    def recall_audit_vector(self, *, query_id: int) -> tuple[float, ...]: ...


class RecallAuditQueryClientLike(Protocol):
    """Offline query-client seam; callers must supply a fake or a future real
    implementation for this boundary. The producer never talks to Milvus
    itself."""

    def execute(
        self, *, query_id: int, query_vector: tuple[float, ...]
    ) -> "RecallAuditClientOutcome": ...


@dataclass(frozen=True, slots=True)
class RecallAuditClientOutcome:
    """Non-sensitive facts returned by one injected query-client attempt."""

    success: bool
    candidate_result_ids: tuple[int, ...] | None
    reason_code: str | None


class FakeDeterministicRecallAuditClient:
    """A deterministic, fully offline stand-in for a real HNSW search client.

    Given the same injected oracle mapping and ``max_candidates_per_query``,
    every call for a given ``query_id`` returns exactly the same result --
    no randomness, no I/O, no real vector search, and ``query_vector`` is
    accepted only to match the shape a future real client would need, never
    inspected. This is a harness component, not a production Milvus client:
    it already knows the oracle answer because its purpose is to exercise the
    recall-audit ledger/evaluator/binding pipeline under full experimenter
    control, not to measure a real index's approximate behavior.
    """

    def __init__(
        self,
        oracle_result_ids_by_query_id: Mapping[int, tuple[int, ...]],
        *,
        max_candidates_per_query: int | None = None,
    ) -> None:
        if not isinstance(oracle_result_ids_by_query_id, Mapping):
            raise TypeError("oracle_result_ids_by_query_id must be a Mapping")
        if max_candidates_per_query is not None and (
            isinstance(max_candidates_per_query, bool)
            or not isinstance(max_candidates_per_query, int)
            or max_candidates_per_query < 0
        ):
            raise ValueError("max_candidates_per_query must be a non-negative integer or None")
        self._oracle_result_ids_by_query_id = dict(oracle_result_ids_by_query_id)
        self._max_candidates_per_query = max_candidates_per_query

    def execute(
        self, *, query_id: int, query_vector: tuple[float, ...]
    ) -> RecallAuditClientOutcome:
        oracle_ids = self._oracle_result_ids_by_query_id.get(query_id)
        if oracle_ids is None:
            return RecallAuditClientOutcome(
                success=False, candidate_result_ids=None, reason_code="QUERY_ID_UNKNOWN_TO_CLIENT"
            )
        candidate = (
            oracle_ids
            if self._max_candidates_per_query is None
            else oracle_ids[: self._max_candidates_per_query]
        )
        return RecallAuditClientOutcome(
            success=True, candidate_result_ids=candidate, reason_code=None
        )


@dataclass(frozen=True, slots=True)
class Stage4RecallAuditProducerResult:
    """Inspectable outcome of one bounded producer invocation.

    ``dispatched_query_count + already_present_query_count`` may be less than
    the frozen population size only when ``completed`` is ``False``; the
    caller can always compute exactly how much evidence remains outstanding.
    """

    dispatched_query_count: int
    already_present_query_count: int
    completed: bool
    reason_codes: tuple[str, ...]
    failed_query_id: int | None


class Stage4RecallAuditProducer:
    """Populate one recall-audit ledger run from an injected query client.

    A matching ``Stage4EvidenceBinding``/ledger pair gates this composition
    but is not itself an authorization. This class never accepts approval
    material, claims a route, or creates a live Milvus client.
    """

    def __init__(
        self,
        *,
        binding: Stage4EvidenceBinding,
        search_configuration: SearchConfiguration,
        dataset002_schema_version: int,
        query_source: RecallAuditVectorSourceLike,
        oracle_result_ids_by_query_id: Mapping[int, tuple[int, ...]],
        client: RecallAuditQueryClientLike,
        ledger: CanaryRecallAuditLedger,
        utc_now: Callable[[], str],
    ) -> None:
        if not isinstance(binding, Stage4EvidenceBinding):
            raise TypeError("binding must be a Stage4EvidenceBinding")
        if not isinstance(search_configuration, SearchConfiguration):
            raise TypeError("search_configuration must be a SearchConfiguration")
        search_configuration.validate()
        if not isinstance(ledger, CanaryRecallAuditLedger):
            raise TypeError("ledger must be a CanaryRecallAuditLedger")
        if not callable(getattr(query_source, "recall_audit_vector", None)):
            raise TypeError("query_source must satisfy RecallAuditVectorSourceLike")
        if not callable(getattr(client, "execute", None)):
            raise TypeError("client must satisfy RecallAuditQueryClientLike")
        if not callable(utc_now):
            raise TypeError("utc_now must be callable")
        if not isinstance(oracle_result_ids_by_query_id, Mapping):
            raise TypeError("oracle_result_ids_by_query_id must be a Mapping")

        if ledger.binding_sha256 != binding.sha256:
            raise ValueError("LEDGER_BINDING_MISMATCH")
        if (
            search_configuration.index_track is not IndexTrack.HNSW
            or search_configuration.metric is not binding.metric
            or search_configuration.threshold_label != binding.threshold_stratum
            or search_configuration.ef != binding.candidate_ef
        ):
            raise ValueError("SEARCH_CONFIGURATION_BINDING_MISMATCH")
        if dataset002_schema_version != DATASET002_SCHEMA_VERSION:
            raise ValueError("DATASET002_SCHEMA_VERSION_MISMATCH")
        if frozenset(oracle_result_ids_by_query_id) != _FROZEN_RECALL_AUDIT_QUERY_IDS:
            raise ValueError("ORACLE_RESULT_POPULATION_INVALID")

        self._binding = binding
        self._search_configuration = search_configuration
        self._dataset002_schema_version = dataset002_schema_version
        self._query_source = query_source
        self._oracle_result_ids_by_query_id = dict(oracle_result_ids_by_query_id)
        self._client = client
        self._ledger = ledger
        self._utc_now = utc_now

    def run(self, *, max_queries: int | None = None) -> Stage4RecallAuditProducerResult:
        """Dispatch a bounded suffix of not-yet-present queries, in order.

        Every call re-reads the ledger's own verified state first, so this is
        safe to call repeatedly (restart, retry, or incremental batches) from
        a fresh process with no other coordination.
        """

        limit = _query_limit(max_queries)
        already_present = {
            observation.query_id for observation in self._ledger.records()
        }
        remaining = sorted(_FROZEN_RECALL_AUDIT_QUERY_IDS - already_present)

        dispatched = 0
        for query_id in remaining[:limit]:
            reason = self._process_one(query_id)
            if reason is not None:
                return Stage4RecallAuditProducerResult(
                    dispatched_query_count=dispatched,
                    already_present_query_count=len(already_present),
                    completed=False,
                    reason_codes=(reason,),
                    failed_query_id=query_id,
                )
            dispatched += 1

        total_present = len(already_present) + dispatched
        return Stage4RecallAuditProducerResult(
            dispatched_query_count=dispatched,
            already_present_query_count=len(already_present),
            completed=total_present == EXP009_RECALL_AUDIT_COUNT,
            reason_codes=(),
            failed_query_id=None,
        )

    def _process_one(self, query_id: int) -> str | None:
        try:
            query_vector = self._query_source.recall_audit_vector(query_id=query_id)
        except Exception:
            return "VECTOR_SOURCE_FAILURE"

        try:
            outcome = self._client.execute(query_id=query_id, query_vector=query_vector)
        except Exception:
            return "CLIENT_EXCEPTION"
        if not isinstance(outcome, RecallAuditClientOutcome):
            return "CLIENT_OUTCOME_INVALID"
        if not outcome.success:
            return outcome.reason_code or "CLIENT_REPORTED_FAILURE"
        if outcome.candidate_result_ids is None:
            return "CLIENT_OUTCOME_INVALID"

        try:
            recorded_at_utc = self._utc_now()
            observation = RecallAuditObservation(
                query_id=query_id,
                search_configuration=self._search_configuration,
                identity=self._binding.identity,
                dataset002_manifest_sha256=self._binding.dataset002_manifest_sha256,
                dataset002_schema_version=self._dataset002_schema_version,
                oracle_result_ids=self._oracle_result_ids_by_query_id[query_id],
                candidate_result_ids=outcome.candidate_result_ids,
                producer_run_id=self._binding.run_id,
                recorded_at_utc=recorded_at_utc,
            )
        except (ValueError, TypeError):
            return "OBSERVATION_CONSTRUCTION_FAILED"

        append = self._ledger.append(observation)
        if not append.accepted:
            return append.reason_code or "LEDGER_APPEND_REFUSED"
        return None


def _query_limit(value: int | None) -> int:
    if value is None:
        return EXP009_RECALL_AUDIT_COUNT
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("max_queries must be a positive integer or None")
    return value
