"""Production Milvus adapter for the DATASET-003 LKG-qualification runner.

Purpose:
    Issue exactly one live HNSW range-search request per call and bind its
    request configuration, returned IDs/distances, and monotonic
    start/end timestamps into one immutable ``LkgSearchCall`` -- the
    neutral same-call result contract later evidence derivation depends on.
    Kept as its own module, separate from ``lkg_qualification_runner.py``,
    so every PyMilvus-facing construction/decoding detail (collection name,
    search params, ``limit=100``, output fields, consistency level, ID/
    distance decoding, duplicate/non-finite/shape validation) has one
    place, independently testable against an injected mock client without
    pulling in oracle computation or ledger/producer concerns.
Inputs:
    A live or fake ``ClientLike`` (``milvus.py``), the target HNSW
    collection name, and one query id/vector/metric/threshold/ef/radius per
    call.
Outputs:
    One ``LkgSearchCall`` per call -- never raises for an ordinary search
    failure (client exception, timeout, malformed response); those are
    captured on the returned call's ``exception`` field instead, so a
    caller can always turn a call into a typed attempt without a
    try/except of its own.
Dependencies:
    Only ``milvus.py`` (``MilvusHarness``/``ClientLike``/``SearchHit``) and
    ``config.py``/``actuation.py``. Never ``milvus_actuation.py`` or any
    canary/actuation module. PyMilvus itself is imported lazily, only
    inside ``from_uri``.
Failure modes:
    A raised exception from the underlying client (network failure,
    timeout, or a ``ContractViolation`` raised by ``MilvusHarness``'s own
    response-shape/duplicate-ID validation) is captured on the returned
    ``LkgSearchCall.exception``, never re-raised here. Only a genuinely
    invalid caller input (e.g. a non-callable ``clock_ns``, an empty
    collection name) raises at construction time.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from time import perf_counter_ns
from typing import Callable, Self

import numpy as np
import numpy.typing as npt

from .actuation import QueryId
from .config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from .milvus import ClientLike, MilvusHarness, SearchHit


__all__ = ["LkgSearchCall", "LkgMilvusAdapter"]


ClockNs = Callable[[], int]


@dataclass(frozen=True, slots=True)
class LkgSearchCall:
    """One exact live search call; request and result bound as a single unit.

    Every fact about this call -- what configuration was asked for, what
    came back, and exactly when -- lives on this one immutable record, so a
    caller can derive recall/threshold-violation evidence from it later
    without a second live query, and without risk of pairing one call's
    request against a different call's result. ``query_vector`` is stored
    here (not just referenced by ``query_id``) for the same reason: oracle
    recomputation must use the exact vector this call already searched.
    """

    query_id: QueryId
    metric: Metric
    threshold_stratum: str
    ef: int
    radius: float
    range_filter: float
    limit: int
    query_vector: tuple[float, ...]
    hits: tuple[SearchHit, ...] | None
    start_ns: int
    end_ns: int
    exception: Exception | None = None

    @property
    def latency_ms(self) -> float:
        return max(0.0, float(self.end_ns - self.start_ns) / 1_000_000.0)

    @property
    def timed_out(self) -> bool:
        return isinstance(self.exception, TimeoutError)

    @property
    def succeeded(self) -> bool:
        return self.exception is None and self.hits is not None

    @property
    def hit_ids(self) -> tuple[int, ...]:
        if self.hits is None:
            return ()
        return tuple(hit.id for hit in self.hits)


def _validate_nonempty_str(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ContractViolation(f"{field_name} must be a non-empty string")
    return value


def _validate_hits(hits: tuple[SearchHit, ...], *, limit: int) -> None:
    """Defense-in-depth response validation beyond ``MilvusHarness.search``'s
    own duplicate-ID/batch-shape checks: a response with more hits than the
    configured limit, or any non-finite distance, is malformed and must
    fail closed here rather than silently become bogus recall evidence."""

    if len(hits) > limit:
        raise ContractViolation(
            f"Milvus returned {len(hits)} hits, more than the configured limit {limit}"
        )
    for hit in hits:
        if not math.isfinite(hit.score):
            raise ContractViolation(f"Milvus returned a non-finite distance: {hit.score!r}")


class LkgMilvusAdapter:
    """Issues one live HNSW search per call and returns its bound result.

    Composed directly around ``MilvusHarness`` -- the same generic adapter
    ``MilvusActuationClient`` itself is built on -- never around
    ``MilvusActuationClient`` or any canary/actuation execution flow.
    """

    def __init__(
        self,
        client: ClientLike,
        *,
        dimensions: int,
        hnsw_collection_name: str,
        clock_ns: ClockNs = perf_counter_ns,
    ) -> None:
        if not callable(clock_ns):
            raise TypeError("clock_ns must be callable")
        self._harness = MilvusHarness(client, dimensions=dimensions)
        self._hnsw_collection_name = _validate_nonempty_str(
            hnsw_collection_name, field_name="hnsw_collection_name"
        )
        self._clock_ns = clock_ns

    @classmethod
    def from_uri(cls, uri: str, **kwargs: object) -> Self:
        """Construct with a real PyMilvus client lazily; calling this may open a client.

        Constructing this object alone opens a client connection but issues
        no search -- no query is dispatched until a caller explicitly calls
        ``search`` (and the caller, not this constructor, is responsible
        for the separate explicit authorization a live Milvus run
        requires).
        """

        from pymilvus import MilvusClient

        return cls(MilvusClient(uri=uri), **kwargs)  # type: ignore[arg-type]

    @property
    def hnsw_collection_name(self) -> str:
        return self._hnsw_collection_name

    def search(
        self,
        *,
        query_id: QueryId,
        query_vector: npt.NDArray[np.float32],
        metric: Metric,
        threshold_stratum: str,
        ef: int,
        radius: float,
    ) -> LkgSearchCall:
        """Execute exactly one live HNSW search and return its bound result.

        The request always targets ``self.hnsw_collection_name`` with
        ``index_track=HNSW``, the caller-supplied ``metric``/
        ``threshold_stratum`` (-> ``threshold_label``)/``ef``/``radius``,
        and ``SearchConfiguration``'s fixed ``limit=100``/
        ``consistency_level="Strong"`` contract -- ``configuration.validate()``
        runs before any request is sent. Exactly one ``client.search`` call
        is timed by this method's own ``clock_ns()`` pair; the returned
        ``LkgSearchCall.hits``/``hit_ids`` are decoded from that single
        response only.
        """

        configuration = SearchConfiguration(
            metric=metric,
            threshold_label=threshold_stratum,
            radius=radius,
            index_track=IndexTrack.HNSW,
            ef=ef,
        )
        configuration.validate()
        vector_tuple = tuple(float(value) for value in query_vector)
        start = self._clock_ns()
        try:
            hits = self._harness.search(
                name=self._hnsw_collection_name,
                query=query_vector,
                configuration=configuration,
            )
            _validate_hits(hits, limit=configuration.limit)
        except Exception as exc:  # noqa: BLE001 - injected client boundary
            end = self._clock_ns()
            return LkgSearchCall(
                query_id=query_id,
                metric=metric,
                threshold_stratum=threshold_stratum,
                ef=ef,
                radius=radius,
                range_filter=configuration.range_filter,
                limit=configuration.limit,
                query_vector=vector_tuple,
                hits=None,
                start_ns=start,
                end_ns=end,
                exception=exc,
            )
        end = self._clock_ns()
        if isinstance(start, bool) or isinstance(end, bool) or end < start:
            raise ContractViolation("clock_ns must return monotonic integer nanoseconds")
        return LkgSearchCall(
            query_id=query_id,
            metric=metric,
            threshold_stratum=threshold_stratum,
            ef=ef,
            radius=radius,
            range_filter=configuration.range_filter,
            limit=configuration.limit,
            query_vector=vector_tuple,
            hits=hits,
            start_ns=start,
            end_ns=end,
        )
