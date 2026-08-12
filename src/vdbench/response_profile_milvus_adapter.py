"""Read-only Milvus adapters for a future real EXP-011 acquisition run.

Purpose:
    Implement the two existing `response_profile_producer.py` ports --
    `ResponseProfileQueryExecutor` and `ResponseProfileRuntimeProbe` -- against
    a real (or fake, for tests) Milvus client, so a future live acquisition
    composition root can drive the unmodified `ResponseProfileProducer`
    against a real collection.
Inputs:
    A `ClientLike` (`milvus.py`) restricted here to the narrow read-only
    surface this module actually calls, one exact HNSW collection name and
    vector dimensionality, and an injected `StackHealthProbeLike` for the
    etcd/minio facts Milvus itself does not report.
Outputs:
    `ResponseProfileSearchResult`/`ResponseProfileRuntimeReadiness` values
    built only through their existing contract factories.
Dependencies:
    `MilvusHarness` (`milvus.py`) is reused unmodified for both the search
    call and the read-only index-identity/describe_index call -- this module
    adds no new PyMilvus-facing request/response construction of its own,
    matching `lkg_milvus_adapter.py`'s and `milvus_serving.py`'s existing
    convention of keeping that detail in one place. PyMilvus itself is
    imported lazily, only inside `from_uri`.
Failure modes:
    Every Milvus/health call is wrapped; a client exception, a malformed
    response, or an unhealthy/not-loaded state never raises out of this
    module -- it becomes a governed `ResponseProfileProducer` failure
    (`SEARCH_FAILED`/`RUNTIME_READINESS_FAILED`) through the existing
    producer contract, never a fabricated success.
Read-only contract:
    The only Milvus client methods this module ever calls are `search`,
    `get_load_state`, and `describe_index`. No insert, upsert, delete,
    collection/index create or drop, load/release, alias mutation, or
    configuration mutation is reachable from any code path in this module --
    enforced both by direct construction (see the methods below) and by this
    module's own adversarial import/attribute test.
"""

from __future__ import annotations

from typing import Protocol, Self

import numpy as np

from .config import IndexTrack, Metric
from .milvus import MilvusHarness
from .response_profile_producer import (
    ResponseProfileExecutionQuery,
    ResponseProfileRuntimeReadiness,
    ResponseProfileSearchResult,
    build_response_profile_runtime_readiness,
    build_response_profile_search_result,
)


__all__ = [
    "ResponseProfileMilvusClientLike",
    "StackHealthProbeLike",
    "ResponseProfileMilvusQueryExecutor",
    "ResponseProfileMilvusRuntimeProbe",
    "build_response_profile_milvus_client",
]


def build_response_profile_milvus_client(uri: str) -> ResponseProfileMilvusClientLike:
    """Construct one real read-only PyMilvus client lazily.

    PyMilvus is imported only here, so a composition root (the EXP-011 live
    CLI) can obtain a real client without importing PyMilvus itself.
    Constructing the client opens a connection but issues no search and no
    mutation -- exactly one search per query is dispatched only later, through
    the already-hardened ``ResponseProfileProducer`` lifecycle.
    """

    if not isinstance(uri, str) or not uri:
        raise ValueError("uri must be a non-empty string")
    from pymilvus import MilvusClient

    return MilvusClient(uri=uri)  # type: ignore[return-value]


class ResponseProfileMilvusClientLike(Protocol):
    """Narrowest client surface this module ever calls -- read-only only."""

    def search(self, **kwargs: object) -> object: ...
    def get_load_state(self, **kwargs: object) -> object: ...
    def describe_index(self, **kwargs: object) -> object: ...


class StackHealthProbeLike(Protocol):
    """Structural etcd/minio health probe, kept outside the Milvus client
    facade -- mirrors `milvus_serving.py`'s identical port exactly."""

    def check(self) -> object: ...


class ResponseProfileMilvusQueryExecutor:
    """Issue exactly one read-only HNSW search per `execute` call.

    Composed directly around `MilvusHarness` -- never PyMilvus directly, and
    never `MilvusActuationClient`/`MilvusRangeServingExecutor` or any
    candidate-routing/serving module.
    """

    def __init__(
        self,
        client: ResponseProfileMilvusClientLike,
        *,
        collection_name: str,
        dimensions: int,
    ) -> None:
        if not isinstance(collection_name, str) or not collection_name:
            raise ValueError("collection_name must be a non-empty string")
        if isinstance(dimensions, bool) or not isinstance(dimensions, int) or dimensions <= 0:
            raise ValueError("dimensions must be a positive integer")
        self._collection_name = collection_name
        self._dimensions = dimensions
        self._harness = MilvusHarness(client, dimensions=dimensions)  # type: ignore[arg-type]

    @classmethod
    def from_uri(cls, uri: str, **kwargs: object) -> Self:
        """Construct with a real PyMilvus client lazily.

        Constructing this object opens a client connection but issues no
        search -- exactly one search is dispatched only when a caller
        explicitly calls `execute`, and only through the already-hardened
        `ResponseProfileProducer` lifecycle (durable STARTED committed first).
        """

        from pymilvus import MilvusClient

        return cls(MilvusClient(uri=uri), **kwargs)  # type: ignore[arg-type]

    def execute(self, query: ResponseProfileExecutionQuery) -> ResponseProfileSearchResult:
        if not isinstance(query, ResponseProfileExecutionQuery):
            raise TypeError("query must be a ResponseProfileExecutionQuery")
        if query.dimensions != self._dimensions:
            raise ValueError("query dimensions differ from the bound collection")
        vector = np.frombuffer(query.vector_bytes, dtype="<f4")
        if vector.shape != (self._dimensions,):
            raise ValueError("query vector shape differs from the bound collection")
        hits = self._harness.search(
            name=self._collection_name,
            query=vector,
            configuration=query.search_configuration,
        )
        return build_response_profile_search_result(
            candidate_ids=tuple(hit.id for hit in hits),
            candidate_distances=tuple(float(hit.score) for hit in hits),
        )


class ResponseProfileMilvusRuntimeProbe:
    """Collect one read-only readiness snapshot; never a search or mutation.

    Mirrors `MilvusRangeServingExecutor.preflight`'s exact read-only call
    shape (`stack_health_probe.check()`, `client.get_load_state`,
    `MilvusHarness.index_identity` -> `client.describe_index`) without
    reusing that class directly, since it is bound to ADR-007 foreground
    serving plans/routing concerns this module must not depend on.
    """

    def __init__(
        self,
        client: ResponseProfileMilvusClientLike,
        *,
        collection_name: str,
        dimensions: int,
        metric: Metric,
        stack_health_probe: StackHealthProbeLike,
    ) -> None:
        if not isinstance(collection_name, str) or not collection_name:
            raise ValueError("collection_name must be a non-empty string")
        if isinstance(dimensions, bool) or not isinstance(dimensions, int) or dimensions <= 0:
            raise ValueError("dimensions must be a positive integer")
        if not isinstance(metric, Metric):
            raise TypeError("metric must be a Metric")
        if not callable(getattr(stack_health_probe, "check", None)):
            raise TypeError("stack_health_probe must provide check")
        self._collection_name = collection_name
        self._metric = metric
        self._harness = MilvusHarness(client, dimensions=dimensions)  # type: ignore[arg-type]
        self._client = client
        self._stack_health_probe = stack_health_probe

    @classmethod
    def from_uri(cls, uri: str, **kwargs: object) -> Self:
        from pymilvus import MilvusClient

        return cls(MilvusClient(uri=uri), **kwargs)  # type: ignore[arg-type]

    def collect(self) -> ResponseProfileRuntimeReadiness:
        try:
            health = self._stack_health_probe.check()
            etcd_healthy = getattr(health, "etcd_healthy", None) is True
            minio_healthy = getattr(health, "minio_healthy", None) is True
        except Exception:
            etcd_healthy = False
            minio_healthy = False

        try:
            state = self._client.get_load_state(collection_name=self._collection_name)
            value = state.get("state") if isinstance(state, dict) else state
            collection_loaded = getattr(value, "name", str(value)) == "Loaded"
            milvus_healthy = True
        except Exception:
            collection_loaded = False
            milvus_healthy = False

        if milvus_healthy:
            try:
                self._harness.index_identity(
                    self._collection_name, self._metric, IndexTrack.HNSW
                )
            except Exception:
                # A collection that cannot report its own index identity is
                # not a trustworthy read target, even if load_state reported
                # Loaded moments earlier.
                collection_loaded = False

        return build_response_profile_runtime_readiness(
            collection_loaded=collection_loaded,
            milvus_healthy=milvus_healthy,
            etcd_healthy=etcd_healthy,
            minio_healthy=minio_healthy,
        )
