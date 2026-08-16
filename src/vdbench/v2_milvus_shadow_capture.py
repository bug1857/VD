"""ADR-014 production-capable v2 Milvus shadow capture.

Purpose:
    Satisfy `v2_shadow_worker.V2ShadowCaptureExecutor` against real Milvus, so
    a 50-observation slice of committed v2 sources becomes one real
    `ShadowAuditTrace` (exact local oracle + FLAT + sentinel `ef=100`).
The gap this closes:
    `MilvusActuationClient._query()` resolves a query vector as
    `self.workload.query_vectors[query_id]`, i.e. from a *pre-registered*
    workload. Genuine live traffic has no pre-registered id, and its vector is
    the one committed in the observation. This adapter therefore builds an
    **ephemeral** `ActuationWorkload` per 50-source slice whose `query_vectors`
    mapping is constructed from those exact committed observations, so the
    vector used by the real capture path is provably the committed one. No
    query-id lookup into any historical or synthetic workload occurs.
Reuse:
    All metric, range, ordering, oracle, FLAT/sentinel search, identity
    capture, recall, and trace-assembly semantics are the accepted, unchanged
    implementations in `milvus_actuation.py`
    (`capture_readonly_shadow_trace` -> `_run_audit` / `_build_shadow_trace`).
    Nothing statistical is reimplemented here. `ef_values=()` means no
    candidate and no last-known-good search is ever issued.
Base material:
    Oracle base vectors/ids come only from a `verify_dataset_artifacts`
    -verified DATASET-001 directory, and the derived
    `<version>:sha256:<generation manifest digest>` must equal the stream's
    `data_identity`. A caller-supplied base-data identity is never accepted.
Milvus boundary:
    The client is injected. This module constructs no PyMilvus client itself
    and is fully exercisable with fakes; `build_readonly_milvus_client` exists
    as a lazy factory for a future authorized operator run and is not invoked
    by any test.
Authority:
    None. No policy, admission, grant, routing, activation, actuation, or
    candidate authority is created or imported.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter_ns
from typing import Any

import numpy as np

from .actuation import ShadowActuationContext
from .artifacts import sha256_file, verify_dataset_artifacts
from .config import ContractViolation, IndexTrack
from .host_window_lineage import CommittedHostObservation
from .milvus_actuation import (
    ActuationWorkload,
    CollectionIdentityBinding,
    MilvusActuationClient,
    ShadowAuditTrace,
)
from .shadow_event_types import MonitorStreamKey
from .shadow_window import TRACE_QUERY_COUNT

__all__ = [
    "V2MilvusShadowCaptureError",
    "V2MilvusShadowCaptureExecutor",
    "V2ShadowCaptureIdentityBinding",
    "build_readonly_milvus_client",
]


class V2MilvusShadowCaptureError(RuntimeError):
    """Fail-closed capture error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> V2MilvusShadowCaptureError:
    return V2MilvusShadowCaptureError(code, message)


def build_readonly_milvus_client(uri: str) -> Any:
    """Lazily construct one real read-only PyMilvus client.

    Defined for a future authorized operator run; never called by this module
    or by any offline test. PyMilvus is imported only inside this function.
    """

    if not isinstance(uri, str) or not uri:
        raise ValueError("uri must be a non-empty string")
    from pymilvus import MilvusClient

    return MilvusClient(uri=uri)


@dataclass(frozen=True, slots=True)
class V2ShadowCaptureIdentityBinding:
    """The exact collection/index identities this capture is bound to."""

    flat_collection_name: str
    hnsw_collection_name: str
    flat_binding: CollectionIdentityBinding
    hnsw_binding: CollectionIdentityBinding


class V2MilvusShadowCaptureExecutor:
    """Concrete `V2ShadowCaptureExecutor` over the accepted real capture path."""

    def __init__(
        self,
        *,
        client: Any,
        stream_key: MonitorStreamKey,
        dataset001_dir: Path,
        identity_binding: V2ShadowCaptureIdentityBinding,
        threshold_radius: float,
        served_ef: int,
        source_revision: str,
        environment_manifest_sha256: str,
        stack_health_probe: Any,
        occurred_at_clock: Callable[[], str],
        clock_ns: Callable[[], int] = perf_counter_ns,
    ) -> None:
        if type(stream_key) is not MonitorStreamKey:
            raise _error("CAPTURE_STREAM_INVALID")
        if type(identity_binding) is not V2ShadowCaptureIdentityBinding:
            raise _error("CAPTURE_IDENTITY_BINDING_INVALID")
        if isinstance(threshold_radius, bool) or not isinstance(
            threshold_radius, (int, float)
        ) or not math.isfinite(float(threshold_radius)):
            raise _error("CAPTURE_RADIUS_INVALID")
        if not callable(occurred_at_clock):
            raise TypeError("occurred_at_clock must be callable")
        if not isinstance(source_revision, str) or not source_revision:
            raise _error("CAPTURE_SOURCE_REVISION_INVALID")
        if (
            not isinstance(environment_manifest_sha256, str)
            or len(environment_manifest_sha256) != 64
        ):
            raise _error("CAPTURE_ENVIRONMENT_INVALID")

        self.stream_key = stream_key
        self.identity_binding = identity_binding
        self.threshold_radius = float(threshold_radius)
        self.served_ef = served_ef
        self.source_revision = source_revision
        self.environment_manifest_sha256 = environment_manifest_sha256
        self._client = client
        self._stack_health_probe = stack_health_probe
        self._occurred_at_clock = occurred_at_clock
        self._clock_ns = clock_ns

        base_ids, base_vectors, data_identity, dimensions = _load_verified_corpus(
            dataset001_dir
        )
        # ADR-014 item 10: the corpus must be the one the stream is pinned to.
        if data_identity != stream_key.data_identity:
            raise _error(
                "CAPTURE_DATA_IDENTITY_MISMATCH",
                "verified DATASET-001 identity differs from the stream data_identity",
            )
        self._base_ids = base_ids
        self._base_vectors = base_vectors
        self.data_identity = data_identity
        self.dimensions = dimensions

        # Index/collection identities the capture must observe unchanged.
        self._collection_names = {
            (stream_key.metric, IndexTrack.FLAT): identity_binding.flat_collection_name,
            (stream_key.metric, IndexTrack.HNSW): identity_binding.hnsw_collection_name,
        }
        self._identity_bindings = {
            (stream_key.metric, IndexTrack.FLAT): identity_binding.flat_binding,
            (stream_key.metric, IndexTrack.HNSW): identity_binding.hnsw_binding,
        }
        self._threshold_radii = {
            (stream_key.metric, stream_key.threshold_stratum): self.threshold_radius
        }

    # -- V2ShadowCaptureExecutor -----------------------------------------

    def capture(
        self,
        sources: tuple[CommittedHostObservation, ...],
        *,
        trace_sequence_index: int,
    ) -> ShadowAuditTrace:
        """Capture one real 50-query trace for this exact committed slice."""

        validated = self._validated_slice(sources)
        workload = self._ephemeral_workload(validated)
        client = MilvusActuationClient(
            self._client,
            workload=workload,
            routing_seed=0,
            stack_health_probe=self._stack_health_probe,
            initial_ef=self.served_ef,
            clock_ns=self._clock_ns,
        )
        context = ShadowActuationContext(
            metric=self.stream_key.metric,
            threshold_stratum=self.stream_key.threshold_stratum,
            collection_name=self.identity_binding.hnsw_collection_name,
            configuration_identity=self.stream_key.configuration_identity,
            index_identity=self.stream_key.hnsw_binding_id,
            flat_index_identity=self.stream_key.flat_binding_id,
            data_identity=self.stream_key.data_identity,
            occurred_at_utc=self._occurred_at_clock(),
            audited_query_ids=tuple(item.query_id for item in validated),
        )
        try:
            trace = client.capture_readonly_shadow_trace(
                context=context, served_ef=self.served_ef
            )
        except (ContractViolation, ValueError, TypeError) as exc:
            raise _error("CAPTURE_FAILED", str(exc)) from exc
        # Conservation: the trace must answer exactly these committed queries,
        # in this exact order.
        if tuple(item.query_id for item in trace.queries) != tuple(
            item.query_id for item in validated
        ):
            raise _error("CAPTURE_QUERY_ID_CONSERVATION_FAILED")
        return trace

    # -- validation ------------------------------------------------------

    def _validated_slice(
        self, sources: tuple[CommittedHostObservation, ...]
    ) -> tuple[CommittedHostObservation, ...]:
        if type(sources) is not tuple or len(sources) != TRACE_QUERY_COUNT:
            raise _error("CAPTURE_SLICE_COUNT_INVALID")
        seen: set[object] = set()
        for item in sources:
            if type(item) is not CommittedHostObservation:
                raise _error("CAPTURE_SOURCE_INVALID")
            if item.stream_key != self.stream_key:
                raise _error("CAPTURE_STREAM_MISMATCH")
            if item.source_revision != self.source_revision:
                raise _error("CAPTURE_SOURCE_REVISION_MISMATCH")
            if item.environment_manifest_sha256 != self.environment_manifest_sha256:
                raise _error("CAPTURE_ENVIRONMENT_MISMATCH")
            if item.query_id in seen:
                raise _error("CAPTURE_DUPLICATE_QUERY_ID")
            seen.add(item.query_id)
            vector = item.query_vector
            if type(vector) is not tuple or len(vector) != self.dimensions:
                raise _error("CAPTURE_DIMENSIONS_MISMATCH")
            for value in vector:
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise _error("CAPTURE_VECTOR_INVALID")
                if not math.isfinite(float(value)):
                    raise _error("CAPTURE_VECTOR_NONFINITE")
        return sources

    def _ephemeral_workload(
        self, sources: tuple[CommittedHostObservation, ...]
    ) -> ActuationWorkload:
        """Build the per-slice workload from the committed observations.

        This mapping exists only for the duration of one capture and is built
        from `source.query_vector` values, so the vector the real capture path
        resolves for `source.query_id` is provably the committed one. No frozen
        or historical workload is consulted or mutated.
        """

        query_vectors = {
            item.query_id: np.ascontiguousarray(
                np.asarray(item.query_vector, dtype="<f4")
            )
            for item in sources
        }
        return ActuationWorkload(
            query_vectors=query_vectors,
            base_ids=self._base_ids,
            base_vectors=self._base_vectors,
            threshold_radii=self._threshold_radii,
            collection_names=self._collection_names,
            identity_bindings=self._identity_bindings,
            configuration_identity=self.stream_key.configuration_identity,
            data_identity=self.stream_key.data_identity,
        )


def _load_verified_corpus(
    dataset001_dir: Path,
) -> tuple[np.ndarray, np.ndarray, str, int]:
    """Verify DATASET-001 and derive its governed identity mechanically."""

    directory = Path(dataset001_dir)
    try:
        manifest = verify_dataset_artifacts(directory)
    except (OSError, ValueError, KeyError, ContractViolation) as exc:
        raise _error("CAPTURE_CORPUS_UNVERIFIED") from exc
    dataset: Mapping[str, Any] | None = (
        manifest.get("dataset") if isinstance(manifest, Mapping) else None
    )
    if not isinstance(dataset, Mapping) or dataset.get("dataset_id") != "DATASET-001":
        raise _error("CAPTURE_CORPUS_INVALID")
    version = dataset.get("version")
    if not isinstance(version, str) or not version:
        raise _error("CAPTURE_CORPUS_INVALID")
    try:
        base_ids = np.load(directory / "base_ids.npy", allow_pickle=False)
        base_vectors = np.load(directory / "base_vectors.npy", allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise _error("CAPTURE_CORPUS_INVALID") from exc
    if (
        base_ids.ndim != 1
        or base_vectors.ndim != 2
        or base_ids.shape[0] != base_vectors.shape[0]
        or base_vectors.dtype.str != "<f4"
        or not bool(np.all(np.isfinite(base_vectors)))
        or len(np.unique(base_ids)) != base_ids.size
    ):
        raise _error("CAPTURE_CORPUS_INVALID")
    identity = f"{version}:sha256:{sha256_file(directory / 'generation_manifest.json')}"
    return (
        np.asarray(base_ids, dtype=np.int64),
        np.asarray(base_vectors, dtype="<f4"),
        identity,
        int(base_vectors.shape[1]),
    )
