"""End-to-end LKG-qualification producer: DATASET-003 -> runner -> ledger.

Purpose:
    Dispatch an already-validated DATASET-003 ``lkg_qualification`` workload
    through a dedicated ``LkgQualificationRunner`` and durably persist the
    resulting typed ``LkgQueryAttempt`` -- success or a specific failure --
    into a matching ``LkgQualificationLedger``, one row per attempt. This is
    the missing caller the runner/ledger machinery has always required:
    those modules can execute one search and store one attempt, but
    nothing before this module actually drove a complete population.
Inputs:
    An immutable ``LkgRunBinding`` (the run/producer/complete
    ``SearchConfiguration``/collection/base-data/index/environment/
    source-revision identity, plus the complete DATASET-003 workload
    commitment -- dataset ID/version/manifest hash/query role, query-ID and
    query-vector array hashes, and expected query count -- all bound before
    any dispatch), a matching validated ``LkgDataset003Workload``, an
    injected ``LkgQualificationRunner`` (fake-client for tests, or
    ``from_uri`` for a real Milvus collection), and a durable ledger
    already constructed against the same binding digest.
Outputs:
    Durable ``LkgQueryAttempt`` rows in the ledger -- success or a typed,
    non-silent failure -- or an explicit halted result naming exactly which
    query failed and why. Never a partial, silently-incomplete population
    presented as if it were complete, and never a failure that simply
    disappears without a persisted row.
Attempt identity and retry semantics:
    Every query has a fixed ``attempt_sequence`` (its 0-based position in
    the workload's ordered query IDs) and, per attempt, a 1-based
    ``attempt_number``. A query already recorded with a ``SUCCESS`` attempt
    is never re-dispatched. A query with only prior failed attempts (or no
    attempts at all) is dispatched again as a new, distinctly numbered
    attempt -- this is also exactly how a crash between a successful search
    and its ledger append is handled: nothing was durably committed, so the
    next run simply attempts attempt_number 1 again, fresh. No code ever
    claims an unpersisted result was "recovered".
Dependencies:
    ``lkg_dataset003_loader``, ``lkg_qualification_evidence``,
    ``lkg_qualification_ledger``, ``lkg_qualification_runner``, and
    ``lkg_run_binding`` only. No PyMilvus import of its own -- the injected
    ``runner`` (composed around ``lkg_milvus_adapter.LkgMilvusAdapter``) is
    the only extension point that ever talks to Milvus.
Failure modes:
    A binding/ledger/workload-identity/search-configuration mismatch is
    rejected at construction, before any query is dispatched
    (``client.search_calls`` stays empty in every such test). A dispatched
    query that does not succeed (client error, timeout, malformed response,
    oracle error) still produces and durably persists a typed
    ``LkgQueryAttempt`` -- it is never silently dropped -- and then halts
    the run, reporting that attempt's ``error_code``. A genuine ledger
    append failure (SQLite write/transaction/commit/connection fault)
    raises ``LkgQualificationLedgerError``, caught here and reported as a
    distinct ``APPEND_FAILURE:...`` halt reason -- never conflated with a
    conflicting-duplicate refusal, which is a different, soft
    ``LkgAppendResult(accepted=False)`` outcome. Restart is idempotent: a
    new producer instance against the same ledger resumes from exactly the
    queries without a durable ``SUCCESS`` attempt, retrying each as a new,
    distinctly numbered attempt.
Scope:
    This module never scores recall or decides PASSING/FAILING, and it
    never assembles constituent windows or epochs -- that is
    LKG-qualification Phase 2/3, not implemented here. It never imports
    PyMilvus directly and never claims a qualification result, LKG
    promotion, or policy/admission decision.
"""

from __future__ import annotations

from dataclasses import dataclass

from .actuation import QueryId
from .config import IndexTrack
from .lkg_dataset003_loader import LkgDataset003Workload
from .lkg_qualification_evidence import LkgAttemptStatus, LkgQueryAttempt
from .lkg_qualification_ledger import LkgQualificationLedger, LkgQualificationLedgerError
from .lkg_qualification_runner import LkgQualificationRunner
from .lkg_run_binding import LkgRunBinding


__all__ = [
    "LkgQualificationProducer",
    "LkgQualificationProducerResult",
]


@dataclass(frozen=True, slots=True)
class LkgQualificationProducerResult:
    """Inspectable outcome of one bounded producer invocation.

    ``dispatched_query_count + already_present_query_count`` may be less
    than the workload's population size only when ``completed`` is
    ``False``; the caller can always compute exactly how much evidence
    remains outstanding.
    """

    dispatched_query_count: int
    already_present_query_count: int
    completed: bool
    reason_codes: tuple[str, ...]
    failed_query_id: QueryId | None


class LkgQualificationProducer:
    """Populate one LKG-qualification ledger run from a validated workload.

    A matching ``LkgRunBinding``/ledger/workload triple gates this
    composition but is not itself an authorization for a live Milvus run --
    that remains a separate, explicit decision of which ``runner`` (fake or
    real) the caller injects.
    """

    def __init__(
        self,
        *,
        run_binding: LkgRunBinding,
        workload: LkgDataset003Workload,
        runner: LkgQualificationRunner,
        ledger: LkgQualificationLedger,
    ) -> None:
        if not isinstance(run_binding, LkgRunBinding):
            raise TypeError("run_binding must be an LkgRunBinding")
        if not isinstance(workload, LkgDataset003Workload):
            raise TypeError("workload must be an LkgDataset003Workload")
        if not isinstance(runner, LkgQualificationRunner):
            raise TypeError("runner must be an LkgQualificationRunner")
        if not isinstance(ledger, LkgQualificationLedger):
            raise TypeError("ledger must be an LkgQualificationLedger")

        search_configuration = run_binding.search_configuration
        if search_configuration.index_track is not IndexTrack.HNSW:
            raise ValueError("SEARCH_CONFIGURATION_INDEX_TRACK_INVALID")
        if ledger.run_binding_sha256 != run_binding.sha256:
            raise ValueError("LEDGER_BINDING_MISMATCH")
        if ledger.run_id != run_binding.run_id:
            raise ValueError("LEDGER_RUN_ID_MISMATCH")
        if run_binding.qualification_dataset_id != workload.dataset_id:
            raise ValueError("WORKLOAD_DATASET_ID_MISMATCH")
        if run_binding.qualification_dataset_version != workload.dataset_version:
            raise ValueError("WORKLOAD_DATASET_VERSION_MISMATCH")
        if run_binding.qualification_manifest_sha256 != workload.manifest_sha256:
            raise ValueError("WORKLOAD_MANIFEST_MISMATCH")
        if run_binding.qualification_query_role != workload.query_role:
            raise ValueError("WORKLOAD_QUERY_ROLE_MISMATCH")
        if run_binding.qualification_query_id_array_sha256 != workload.query_id_array_sha256:
            raise ValueError("WORKLOAD_QUERY_ID_ARRAY_MISMATCH")
        if run_binding.qualification_query_array_sha256 != workload.query_array_sha256:
            raise ValueError("WORKLOAD_QUERY_ARRAY_MISMATCH")
        if run_binding.qualification_expected_query_count != len(workload.query_ids):
            raise ValueError("WORKLOAD_QUERY_COUNT_MISMATCH")
        if not workload.query_ids:
            raise ValueError("WORKLOAD_POPULATION_EMPTY")
        if len(set(workload.query_ids)) != len(workload.query_ids):
            raise ValueError("WORKLOAD_POPULATION_INVALID")

        self._run_binding = run_binding
        self._search_configuration = search_configuration
        self._workload = workload
        self._runner = runner
        self._ledger = ledger

    def run(self, *, max_queries: int | None = None) -> LkgQualificationProducerResult:
        """Dispatch a bounded suffix of not-yet-succeeded queries, in order.

        Every call re-reads the ledger's own verified state first, so this
        is safe to call repeatedly (restart, retry, or incremental batches)
        from a fresh process with no other coordination.
        """

        limit = _query_limit(max_queries, len(self._workload.query_ids))
        existing_records = self._ledger.records()
        succeeded_query_ids = {
            record.query_id
            for record in existing_records
            if record.status is LkgAttemptStatus.SUCCESS
        }
        attempt_counts: dict[QueryId, int] = {}
        for record in existing_records:
            attempt_counts[record.query_id] = max(
                attempt_counts.get(record.query_id, 0), record.attempt_number
            )

        remaining = [
            (sequence, query_id)
            for sequence, query_id in enumerate(self._workload.query_ids)
            if query_id not in succeeded_query_ids
        ]

        dispatched = 0
        for attempt_sequence, query_id in remaining[:limit]:
            attempt_number = attempt_counts.get(query_id, 0) + 1
            reason = self._process_one(
                query_id=query_id,
                attempt_sequence=attempt_sequence,
                attempt_number=attempt_number,
            )
            if reason is not None:
                return LkgQualificationProducerResult(
                    dispatched_query_count=dispatched,
                    already_present_query_count=len(succeeded_query_ids),
                    completed=False,
                    reason_codes=(reason,),
                    failed_query_id=query_id,
                )
            dispatched += 1
            attempt_counts[query_id] = attempt_number

        total_present = len(succeeded_query_ids) + dispatched
        return LkgQualificationProducerResult(
            dispatched_query_count=dispatched,
            already_present_query_count=len(succeeded_query_ids),
            completed=total_present == len(self._workload.query_ids),
            reason_codes=(),
            failed_query_id=None,
        )

    def _process_one(
        self, *, query_id: QueryId, attempt_sequence: int, attempt_number: int
    ) -> str | None:
        try:
            query_vector = self._workload.query_vectors[query_id]
        except KeyError:
            return "VECTOR_SOURCE_FAILURE"

        attempt: LkgQueryAttempt = self._runner.attempt_query(
            query_id=query_id,
            query_vector=query_vector,
            metric=self._search_configuration.metric,
            threshold_stratum=self._search_configuration.threshold_label,
            ef=self._search_configuration.ef,
            radius=self._search_configuration.radius,
            attempt_sequence=attempt_sequence,
            attempt_number=attempt_number,
            run_binding_sha256=self._run_binding.sha256,
        )

        try:
            append = self._ledger.append(attempt)
        except LkgQualificationLedgerError as exc:
            # A genuine SQLite write/transaction/commit/connection failure
            # -- distinct from a conflicting-duplicate refusal below. The
            # attempt (success or failure) was never durably recorded; the
            # chain head and row count are unchanged (the failed
            # transaction rolled back), so a restart safely retries this
            # exact attempt_number again.
            return f"APPEND_FAILURE:{exc}"
        if not append.accepted:
            # The ledger is available and reachable; a row already exists
            # under this exact (query_id, attempt_number) with genuinely
            # different content -- a conflicting retry, not an append
            # failure.
            return append.conflict_reason or "LEDGER_APPEND_REFUSED"

        if attempt.status is not LkgAttemptStatus.SUCCESS:
            # The failed attempt IS durably persisted above (it never
            # disappears silently) -- but this bounded run() call still
            # halts here, exactly like every other halt reason, so a
            # caller always sees precisely which query failed and why.
            return attempt.error_code or attempt.status.value
        return None


def _query_limit(value: int | None, population_size: int) -> int:
    if value is None:
        return population_size
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("max_queries must be a positive integer or None")
    return value
