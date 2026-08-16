"""Dedicated DATASET-003 LKG-qualification query pipeline.

Purpose:
    Turn one already-validated DATASET-003 query into exactly one typed
    ``LkgQueryAttempt`` -- success or a specific, non-silent failure -- by
    composing the dedicated ``LkgMilvusAdapter`` (one live search call) with
    an exact oracle recomputation, with no second live query anywhere in
    the path. This is a wholly independent execution path from the
    DATASET-002 canary/shadow-audit machinery: it never imports, calls, or
    reuses ``MilvusActuationClient``, ``_run_audit``, ``shadow_candidate``,
    ``ShadowQueryAuditTrace``, or any persisted canary/Stage-4 schema.
Inputs:
    An injected ``LkgMilvusAdapter`` (fake-client for tests, or
    ``from_uri`` for a real Milvus collection), the DATASET-001 base
    vectors/IDs an oracle result is computed against, and one query's
    id/vector/metric/threshold/ef/radius plus its intended sequence
    position and retry-attempt number.
Outputs:
    One ``LkgQueryAttempt`` per call to ``attempt_query`` -- this is the
    single pipeline entry point: validated run binding -> exact DATASET-003
    query -> one adapter call -> immutable same-call result -> response
    validation -> exact oracle computation -> typed success/failure
    attempt. There is no public path that constructs an
    ``LkgQueryObservation``/``LkgQueryAttempt`` from independently supplied
    recall/latency/IDs/distances/timestamps.
Dependencies:
    ``lkg_milvus_adapter.py`` for the live search call, ``oracle.py`` for
    ``exact_range_search``/``capped_threshold_recall``/``threshold_violations``,
    and ``lkg_qualification_evidence.py`` for the typed attempt/observation
    contract. Nothing from ``milvus_actuation.py`` is imported.
Failure modes:
    A failed or malformed search call becomes a ``CLIENT_ERROR``,
    ``TIMEOUT``, or ``MALFORMED_RESPONSE`` attempt; a failed oracle
    recomputation becomes an ``ORACLE_ERROR`` attempt. ``attempt_query``
    itself never raises for any of these -- only a genuinely invalid
    caller input (a non-``LkgMilvusAdapter`` instance, an invalid
    ``attempt_sequence``/``attempt_number``/``run_binding_sha256``) raises.
"""

from __future__ import annotations

from time import perf_counter_ns
from typing import Self

import numpy as np
import numpy.typing as npt

from .actuation import QueryId
from .config import NUMERIC_TOLERANCE, ContractViolation, Metric
from .lkg_milvus_adapter import ClockNs, LkgMilvusAdapter, LkgSearchCall
from .lkg_qualification_evidence import (
    LkgAttemptStatus,
    LkgQueryAttempt,
    build_lkg_query_attempt,
    build_lkg_query_observation,
)
from .oracle import capped_threshold_recall, exact_range_search, threshold_violations

__all__ = ["LkgQualificationRunner"]


class LkgQualificationRunner:
    """Composes an ``LkgMilvusAdapter`` with an exact oracle recomputation.

    Deliberately thin: all PyMilvus-facing request/response handling lives
    in ``lkg_milvus_adapter.py``; this class only adds the oracle
    computation and typed-attempt classification on top of one already-
    bound search call.
    """

    def __init__(
        self,
        adapter: LkgMilvusAdapter,
        *,
        base_vectors: npt.NDArray[np.float32],
        base_ids: npt.NDArray[np.int64],
    ) -> None:
        if not isinstance(adapter, LkgMilvusAdapter):
            raise TypeError("adapter must be an LkgMilvusAdapter")
        self._adapter = adapter
        self._base_vectors = base_vectors
        self._base_ids = base_ids

    @classmethod
    def from_uri(
        cls,
        uri: str,
        *,
        dimensions: int,
        hnsw_collection_name: str,
        base_vectors: npt.NDArray[np.float32],
        base_ids: npt.NDArray[np.int64],
        clock_ns: ClockNs = perf_counter_ns,
    ) -> Self:
        """Construct with a real PyMilvus-backed adapter; calling this may open a client.

        No query is dispatched until a caller explicitly calls
        ``attempt_query`` (and the caller, not this constructor, is
        responsible for the separate explicit authorization a live Milvus
        run requires).
        """

        adapter = LkgMilvusAdapter.from_uri(
            uri,
            dimensions=dimensions,
            hnsw_collection_name=hnsw_collection_name,
            clock_ns=clock_ns,
        )
        return cls(adapter, base_vectors=base_vectors, base_ids=base_ids)

    def attempt_query(
        self,
        *,
        query_id: QueryId,
        query_vector: npt.NDArray[np.float32],
        metric: Metric,
        threshold_stratum: str,
        ef: int,
        radius: float,
        attempt_sequence: int,
        attempt_number: int,
        run_binding_sha256: str,
    ) -> LkgQueryAttempt:
        """Run the complete same-call pipeline for one query and classify it.

        validated run binding -> exact DATASET-003 query -> one adapter
        call -> immutable same-call result -> response validation -> exact
        oracle computation -> typed success/failure attempt. Never issues a
        second live query: the oracle recomputation below uses only the
        ``query_vector``/``hits`` already bound on the one ``LkgSearchCall``
        this method obtains.
        """

        call = self._adapter.search(
            query_id=query_id,
            query_vector=query_vector,
            metric=metric,
            threshold_stratum=threshold_stratum,
            ef=ef,
            radius=radius,
        )
        if not isinstance(call, LkgSearchCall):
            raise ContractViolation("adapter.search must return an LkgSearchCall")

        if not call.succeeded or call.hits is None:
            status, error_code = _classify_failed_call(call)
            return build_lkg_query_attempt(
                query_id=query_id,
                attempt_sequence=attempt_sequence,
                attempt_number=attempt_number,
                status=status,
                error_code=error_code,
                run_binding_sha256=run_binding_sha256,
            )

        try:
            oracle_query_vector = np.asarray(call.query_vector, dtype="<f4")
            oracle = exact_range_search(
                self._base_vectors,
                self._base_ids,
                oracle_query_vector,
                call.metric,
                radius=call.radius,
                range_filter=call.range_filter,
                limit=call.limit,
            )
            recall = capped_threshold_recall(call.hit_ids, oracle.ids)
            violation_count = len(
                threshold_violations(
                    (hit.score for hit in call.hits),
                    call.metric,
                    radius=call.radius,
                    range_filter=call.range_filter,
                    tolerance=NUMERIC_TOLERANCE,
                )
            )
        except Exception as exc:  # oracle computation boundary  # noqa: BLE001
            return build_lkg_query_attempt(
                query_id=query_id,
                attempt_sequence=attempt_sequence,
                attempt_number=attempt_number,
                status=LkgAttemptStatus.ORACLE_ERROR,
                error_code=f"ORACLE_ERROR:{type(exc).__name__}",
                run_binding_sha256=run_binding_sha256,
            )

        observation = build_lkg_query_observation(
            query_id=call.query_id,
            metric=call.metric,
            threshold_stratum=call.threshold_stratum,
            ef=call.ef,
            recall=recall,
            latency_ms=call.latency_ms,
            start_ns=call.start_ns,
            end_ns=call.end_ns,
            exact_cardinality=oracle.full_count,
            threshold_violation_count=violation_count,
        )
        return build_lkg_query_attempt(
            query_id=query_id,
            attempt_sequence=attempt_sequence,
            attempt_number=attempt_number,
            status=LkgAttemptStatus.SUCCESS,
            run_binding_sha256=run_binding_sha256,
            observation=observation,
        )


def _classify_failed_call(call: LkgSearchCall) -> tuple[LkgAttemptStatus, str]:
    if call.timed_out:
        return LkgAttemptStatus.TIMEOUT, "TIMEOUT"
    if isinstance(call.exception, ContractViolation):
        return (
            LkgAttemptStatus.MALFORMED_RESPONSE,
            f"MALFORMED_RESPONSE:{call.exception}",
        )
    exception_name = type(call.exception).__name__ if call.exception is not None else "UNKNOWN"
    return LkgAttemptStatus.CLIENT_ERROR, f"CLIENT_ERROR:{exception_name}"
