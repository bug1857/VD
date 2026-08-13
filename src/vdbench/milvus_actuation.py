"""Milvus-backed read-only shadow and rollback adapter.

Purpose:
    Execute read-only shadow audits and restoration verification through
    ``MilvusHarness`` without embedding drift or policy decisions. Candidate
    serving is owned exclusively by the Stage-4 live composition root.
Inputs:
    An immutable workload, a PyMilvus-client-like object, expected collection
    identities and an external etcd/MinIO health probe. Historical constructor
    inputs for routing/bounds remain inert for read-only acquisition callers.
Outputs:
    ``ShadowResult`` and ``RollbackVerification`` values consumed by the safe
    boundary.
Dependencies:
    PyMilvus is imported only by ``from_uri``. Unit tests inject a fake client
    and never contact a database. Confidence-bound estimation remains external.
Failure modes:
    Incomplete workload data, invalid routing, query failures, FLAT/oracle
    disagreement, unhealthy services, or collection-identity drift fail closed.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from time import perf_counter_ns
from types import MappingProxyType
from typing import Protocol, Self, TypeAlias

import numpy as np
import numpy.typing as npt

from .actuation import (
    ActuationIdentityContext,
    QueryId,
    RollbackActuationContext,
    RollbackVerification,
    ShadowActuationContext,
    ShadowResult,
    validate_rollback_actuation_context,
    validate_shadow_actuation_context,
)
from .config import (
    NUMERIC_TOLERANCE,
    THRESHOLD_LABELS,
    ContractViolation,
    IndexTrack,
    Metric,
    SearchConfiguration,
)
from .drift import AUDIT_QUERY_COUNT, SENTINEL_EF, canonical_serialize_tuple
from .milvus import ClientLike, CollectionIdentity, MilvusHarness, SearchHit
from .oracle import (
    OracleResult,
    capped_threshold_recall,
    exact_range_search,
    threshold_violations,
)
from .policy import ACTUATION_LADDER

CANARY_BATCH_SIZE = 500

ClockNs: TypeAlias = Callable[[], int]


class _FrozenDict(dict[object, object]):
    """JSON-serializable dictionary that rejects all mutation."""

    @staticmethod
    def _immutable(*args: object, **kwargs: object) -> None:
        raise TypeError("shadow identity snapshots are immutable")

    __delitem__ = _immutable
    __ior__ = _immutable
    __setitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable

    def __copy__(self) -> _FrozenDict:
        return self

    def __deepcopy__(self, memo: dict[int, object]) -> _FrozenDict:
        return self


def _freeze_json_value(value: object) -> object:
    if isinstance(value, Mapping):
        return _FrozenDict(
            {
                _freeze_json_value(key): _freeze_json_value(item)
                for key, item in value.items()
            }
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json_value(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class CollectionIdentityBinding:
    """Bind one opaque project identity to expected Milvus index metadata."""

    identity_id: str
    expected: CollectionIdentity
    _fingerprint: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.identity_id, str) or not self.identity_id.strip():
            raise ValueError("identity_id must be non-empty")
        if not isinstance(self.expected, CollectionIdentity):
            raise TypeError("expected must be a CollectionIdentity")
        object.__setattr__(self, "_fingerprint", _identity_fingerprint(self.expected))

    def matches(self, actual: CollectionIdentity) -> bool:
        """Compare against the immutable canonical identity captured at creation."""

        return (
            isinstance(actual, CollectionIdentity)
            and _identity_fingerprint(actual) == self._fingerprint
        )


@dataclass(frozen=True, slots=True)
class StackHealth:
    """Externally observed etcd/MinIO health evidence."""

    etcd_healthy: bool
    minio_healthy: bool
    detail: str = ""


class StackHealthProbeLike(Protocol):
    """Health probe kept outside the PyMilvus query adapter."""

    def check(self) -> StackHealth: ...


@dataclass(frozen=True, slots=True)
class CanaryPairedMeasurements:
    """The same 50 query IDs measured at candidate and last-known-good ef."""

    query_ids: tuple[QueryId, ...]
    candidate_recalls: tuple[float, ...]
    last_known_good_recalls: tuple[float, ...]
    candidate_latencies_ms: tuple[float, ...]
    last_known_good_latencies_ms: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class CanaryBounds:
    """Externally estimated one-sided 95% conservative bounds."""

    recall_lower_bound_95: float
    latency_upper_bound_95_ms: float
    confidence_level: float = 0.95
    provenance: str = ""


class CanaryBoundEstimatorLike(Protocol):
    """Future statistics contract injected into, never defined by, this adapter."""

    def estimate(self, measurements: CanaryPairedMeasurements) -> CanaryBounds: ...


@dataclass(frozen=True, slots=True)
class ShadowAuditStageEvidence:
    """Outcome of one query or identity-capture stage in a shadow trace."""

    stage: str
    success: bool
    timed_out: bool = False
    threshold_violation_count: int = 0
    oracle_agreement: bool | None = None
    error_type: str | None = None


@dataclass(frozen=True, slots=True)
class ShadowQueryAuditTrace:
    """Immutable evidence for one of the 50 audited shadow queries."""

    query_id: QueryId
    query_vector: tuple[float, ...]
    threshold_radius: float
    range_filter: float
    limit: int
    oracle_result: OracleResult | None
    exact_cardinality: int | None
    flat_hits: tuple[SearchHit, ...] | None
    sentinel_hits: tuple[SearchHit, ...] | None
    sentinel_recall: float | None
    stages: tuple[ShadowAuditStageEvidence, ...]


@dataclass(frozen=True, slots=True)
class ShadowIdentityEvidence:
    """Expected binding plus pre/post live identity snapshots for one track."""

    track: IndexTrack
    expected_binding_id: str
    pre_snapshot: CollectionIdentity | None
    post_snapshot: CollectionIdentity | None
    pre_binding_match: bool
    post_binding_match: bool
    pre_capture: ShadowAuditStageEvidence
    post_capture: ShadowAuditStageEvidence


@dataclass(frozen=True, slots=True)
class ShadowAuditTrace:
    """Read-only 50-query trace; not a complete 200-query drift window."""

    metric: Metric
    threshold_stratum: str
    candidate_ef: int
    last_known_good_ef: int
    sentinel_ef: int
    configuration_identity: str
    data_identity: str
    flat_identity: ShadowIdentityEvidence
    hnsw_identity: ShadowIdentityEvidence
    queries: tuple[ShadowQueryAuditTrace, ...]
    complete: bool
    reason_codes: tuple[str, ...] = ()


class ShadowAuditTraceSinkLike(Protocol):
    """Injected destination for exactly one immutable trace per shadow call."""

    def append(self, trace: ShadowAuditTrace) -> None: ...


@dataclass(frozen=True, slots=True)
class ActuationWorkload:
    """Immutable query, oracle, threshold, collection, and identity source."""

    query_vectors: Mapping[QueryId, npt.NDArray[np.float32]]
    canary_query_ids: tuple[QueryId, ...] = field(
        default_factory=tuple,
        kw_only=True,
    )
    base_ids: npt.NDArray[np.int64]
    base_vectors: npt.NDArray[np.float32]
    threshold_radii: Mapping[tuple[Metric | str, str], float]
    collection_names: Mapping[tuple[Metric | str, IndexTrack | str], str]
    identity_bindings: Mapping[
        tuple[Metric | str, IndexTrack | str], CollectionIdentityBinding
    ]
    configuration_identity: str
    data_identity: str

    def __post_init__(self) -> None:
        if not _nonempty(self.configuration_identity) or not _nonempty(
            self.data_identity
        ):
            raise ValueError("configuration_identity and data_identity are required")

        base_ids = np.asarray(self.base_ids, dtype=np.int64).copy()
        base_vectors = np.asarray(self.base_vectors, dtype="<f4").copy()
        if (
            base_ids.ndim != 1
            or base_vectors.ndim != 2
            or base_ids.shape[0] != base_vectors.shape[0]
            or base_ids.size == 0
            or len(np.unique(base_ids)) != base_ids.size
            or not np.all(np.isfinite(base_vectors))
        ):
            raise ValueError("base IDs/vectors must be finite, unique, and aligned")
        base_ids.setflags(write=False)
        base_vectors.setflags(write=False)

        normalized_queries: dict[QueryId, npt.NDArray[np.float32]] = {}
        for query_id, vector in self.query_vectors.items():
            _validate_query_id(query_id)
            if query_id in normalized_queries:
                raise ValueError("query IDs must be unique")
            value = np.asarray(vector, dtype="<f4").copy()
            if value.shape != (base_vectors.shape[1],) or not np.all(
                np.isfinite(value)
            ):
                raise ValueError("query vectors must be finite and match dimensions")
            value.setflags(write=False)
            normalized_queries[query_id] = value

        canary_query_ids = tuple(self.canary_query_ids)
        if canary_query_ids:
            _validate_canary_query_batch(canary_query_ids, normalized_queries)

        normalized_thresholds: dict[tuple[Metric, str], float] = {}
        for (metric, stratum), radius in self.threshold_radii.items():
            normalized_metric = Metric(metric)
            if stratum not in THRESHOLD_LABELS:
                raise ValueError("threshold stratum must be canonical")
            configuration = SearchConfiguration(
                metric=normalized_metric,
                threshold_label=stratum,
                radius=float(radius),
                index_track=IndexTrack.FLAT,
            )
            configuration.validate()
            normalized_thresholds[(normalized_metric, stratum)] = float(radius)

        normalized_names: dict[tuple[Metric, IndexTrack], str] = {}
        for (metric, track), name in self.collection_names.items():
            key = (Metric(metric), IndexTrack(track))
            if not _nonempty(name):
                raise ValueError("collection names must be non-empty")
            normalized_names[key] = name

        normalized_bindings: dict[
            tuple[Metric, IndexTrack], CollectionIdentityBinding
        ] = {}
        for (metric, track), binding in self.identity_bindings.items():
            key = (Metric(metric), IndexTrack(track))
            if not isinstance(binding, CollectionIdentityBinding):
                raise TypeError("identity bindings must be CollectionIdentityBinding")
            expected = binding.expected
            if (
                expected.collection_name != normalized_names.get(key)
                or expected.metric != key[0].value
                or expected.index_track != key[1].value
            ):
                raise ValueError("identity binding does not match its collection key")
            normalized_bindings[key] = binding

        for metric, _ in normalized_thresholds:
            for track in IndexTrack:
                key = (metric, track)
                if key not in normalized_names or key not in normalized_bindings:
                    raise ValueError(
                        "every configured metric requires FLAT/HNSW names and identities"
                    )

        object.__setattr__(self, "base_ids", base_ids)
        object.__setattr__(self, "base_vectors", base_vectors)
        object.__setattr__(self, "query_vectors", MappingProxyType(normalized_queries))
        object.__setattr__(self, "canary_query_ids", canary_query_ids)
        object.__setattr__(
            self, "threshold_radii", MappingProxyType(normalized_thresholds)
        )
        object.__setattr__(self, "collection_names", MappingProxyType(normalized_names))
        object.__setattr__(
            self, "identity_bindings", MappingProxyType(normalized_bindings)
        )

    def validate_for_canary(self) -> None:
        """Fail closed unless a complete 500-query canary batch is configured."""

        _validate_canary_query_batch(self.canary_query_ids, self.query_vectors)


@dataclass(frozen=True, slots=True)
class _SearchOutcome:
    hits: tuple[SearchHit, ...] | None
    latency_ms: float
    exception: Exception | None = None

    @property
    def timed_out(self) -> bool:
        return isinstance(self.exception, TimeoutError)


@dataclass(frozen=True, slots=True)
class _AuditEvidence:
    failed_query_count: int
    timeout_query_count: int
    threshold_violation_count: int
    flat_oracle_agreement: bool

    @property
    def passed(self) -> bool:
        return bool(
            self.failed_query_count == 0
            and self.timeout_query_count == 0
            and self.threshold_violation_count == 0
            and self.flat_oracle_agreement
        )


@dataclass(frozen=True, slots=True)
class _AuditRun:
    evidence: _AuditEvidence
    query_traces: tuple[ShadowQueryAuditTrace, ...] = ()


@dataclass(frozen=True, slots=True)
class _IdentityCapture:
    snapshot: CollectionIdentity | None
    binding_match: bool
    stage: ShadowAuditStageEvidence


@dataclass(frozen=True, slots=True)
class _RuntimeStatus:
    milvus_healthy: bool
    etcd_healthy: bool
    minio_healthy: bool
    collection_loaded: bool
    configuration_valid: bool
    index_identity_unchanged: bool
    detail: str


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_actuation_ef(ef: object, *, name: str) -> int:
    if isinstance(ef, bool) or not isinstance(ef, int) or ef not in ACTUATION_LADDER:
        raise ValueError(f"{name} must be in the ADR-002 actuation ladder")
    return ef


def _validate_routing_seed(seed: object) -> int:
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("routing_seed must be a non-negative integer")
    return seed


def _validate_query_id(query_id: object) -> None:
    if isinstance(query_id, bool) or not isinstance(query_id, (int, str)):
        raise TypeError("query IDs must be canonical integers or strings")
    if isinstance(query_id, str) and not query_id:
        raise ValueError("string query IDs must be non-empty")
    canonical_serialize_tuple((query_id,))


def _validate_canary_query_batch(
    query_ids: Sequence[QueryId],
    query_vectors: Mapping[QueryId, npt.NDArray[np.float32]],
) -> None:
    values = tuple(query_ids)
    if len(values) != CANARY_BATCH_SIZE:
        raise ValueError("canary batch must contain exactly 500 query IDs")
    for query_id in values:
        _validate_query_id(query_id)
    if len(set(values)) != CANARY_BATCH_SIZE:
        raise ValueError("canary query IDs must be unique")
    if any(query_id not in query_vectors for query_id in values):
        raise ValueError("every canary query ID must resolve to a query vector")


def _identity_fingerprint(identity: CollectionIdentity) -> bytes:
    try:
        payload = json.dumps(
            {
                "collection_name": identity.collection_name,
                "metric": identity.metric,
                "index_track": identity.index_track,
                "description": identity.description,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("collection identity must be canonical JSON data") from exc
    return hashlib.sha256(payload.encode("utf-8")).digest()


class MilvusActuationClient:
    """Read-only shadow/rollback implementation composed around MilvusHarness."""

    def __init__(
        self,
        client: ClientLike,
        *,
        workload: ActuationWorkload,
        routing_seed: int,
        # ADR-014: optional so a read-only shadow capture need not fabricate
        # canary state. It is stored and never used by any capture path; only
        # a candidate-bound caller supplies one.
        bound_estimator: CanaryBoundEstimatorLike | None = None,
        stack_health_probe: StackHealthProbeLike,
        initial_ef: int,
        clock_ns: ClockNs = perf_counter_ns,
        shadow_trace_sink: ShadowAuditTraceSinkLike | None = None,
    ) -> None:
        _validate_actuation_ef(initial_ef, name="initial_ef")
        _validate_routing_seed(routing_seed)
        if not callable(clock_ns):
            raise TypeError("clock_ns must be callable")
        self.client = client
        self.workload = workload
        self.routing_seed = routing_seed
        self.bound_estimator = bound_estimator
        self.stack_health_probe = stack_health_probe
        self.clock_ns = clock_ns
        self.shadow_trace_sink = shadow_trace_sink
        self.harness = MilvusHarness(
            client,
            dimensions=int(workload.base_vectors.shape[1]),
        )
        self._default_ef = initial_ef
        self._candidate_ef: int | None = None

    @classmethod
    def from_uri(
        cls,
        uri: str,
        **kwargs: object,
    ) -> Self:
        """Construct with real PyMilvus lazily; calling this may open a client."""

        from pymilvus import MilvusClient

        return cls(MilvusClient(uri=uri), **kwargs)  # type: ignore[arg-type]

    @property
    def default_ef(self) -> int:
        return self._default_ef

    @property
    def candidate_ef(self) -> int | None:
        return self._candidate_ef

    def _metric(self, context: ActuationIdentityContext) -> Metric:
        try:
            return Metric(context.metric)
        except (TypeError, ValueError) as exc:
            raise ContractViolation("actuation context metric is invalid") from exc

    def _configuration(
        self,
        *,
        metric: Metric,
        threshold_stratum: str,
        track: IndexTrack,
        ef: int | None = None,
    ) -> SearchConfiguration:
        try:
            radius = self.workload.threshold_radii[(metric, threshold_stratum)]
        except KeyError as exc:
            raise ContractViolation(
                "workload has no matching threshold radius"
            ) from exc
        configuration = SearchConfiguration(
            metric=metric,
            threshold_label=threshold_stratum,
            radius=radius,
            index_track=track,
            ef=ef,
        )
        configuration.validate()
        return configuration

    def _query(self, query_id: QueryId) -> npt.NDArray[np.float32]:
        try:
            return self.workload.query_vectors[query_id]
        except KeyError as exc:
            raise ContractViolation(f"unknown query ID: {query_id!r}") from exc

    def _oracle(
        self,
        query: npt.NDArray[np.float32],
        configuration: SearchConfiguration,
    ) -> OracleResult:
        return exact_range_search(
            self.workload.base_vectors,
            self.workload.base_ids,
            query,
            configuration.metric,
            radius=configuration.radius,
            range_filter=configuration.range_filter,
            limit=configuration.limit,
        )

    def _timed_search(
        self,
        *,
        name: str,
        query: npt.NDArray[np.float32],
        configuration: SearchConfiguration,
    ) -> _SearchOutcome:
        start = self.clock_ns()
        try:
            hits = self.harness.search(
                name=name,
                query=query,
                configuration=configuration,
            )
        except Exception as exc:  # noqa: BLE001 - injected client boundary
            end = self.clock_ns()
            return _SearchOutcome(
                hits=None,
                latency_ms=max(0.0, float(end - start) / 1_000_000.0),
                exception=exc,
            )
        end = self.clock_ns()
        if isinstance(start, bool) or isinstance(end, bool) or end < start:
            raise ContractViolation(
                "clock_ns must return monotonic integer nanoseconds"
            )
        return _SearchOutcome(
            hits=hits,
            latency_ms=float(end - start) / 1_000_000.0,
        )

    @staticmethod
    def _violation_count(
        hits: Sequence[SearchHit], configuration: SearchConfiguration
    ) -> int:
        return len(
            threshold_violations(
                (hit.score for hit in hits),
                configuration.metric,
                radius=configuration.radius,
                range_filter=configuration.range_filter,
                tolerance=NUMERIC_TOLERANCE,
            )
        )

    @staticmethod
    def _flat_agrees(
        outcome: _SearchOutcome,
        oracle: OracleResult,
        configuration: SearchConfiguration,
    ) -> bool:
        return bool(
            outcome.hits is not None
            and tuple(hit.id for hit in outcome.hits) == oracle.ids
            and MilvusActuationClient._violation_count(outcome.hits, configuration) == 0
        )

    def _run_audit(
        self,
        *,
        context: ActuationIdentityContext,
        query_ids: Sequence[QueryId],
        ef_values: Sequence[int],
        collect_trace: bool,
    ) -> _AuditRun:
        metric = self._metric(context)
        flat_configuration = self._configuration(
            metric=metric,
            threshold_stratum=context.threshold_stratum,
            track=IndexTrack.FLAT,
        )
        hnsw_configurations = tuple(
            self._configuration(
                metric=metric,
                threshold_stratum=context.threshold_stratum,
                track=IndexTrack.HNSW,
                ef=ef,
            )
            for ef in ef_values
        )
        sentinel_configuration = (
            self._configuration(
                metric=metric,
                threshold_stratum=context.threshold_stratum,
                track=IndexTrack.HNSW,
                ef=SENTINEL_EF,
            )
            if collect_trace
            else None
        )
        flat_name = self.workload.collection_names[(metric, IndexTrack.FLAT)]
        hnsw_name = self.workload.collection_names[(metric, IndexTrack.HNSW)]
        failures = 0
        timeouts = 0
        violations = 0
        flat_agreement = True
        query_traces: list[ShadowQueryAuditTrace] = []
        for query_id in query_ids:
            query_vector: tuple[float, ...] = ()
            oracle: OracleResult | None = None
            flat_hits: tuple[SearchHit, ...] | None = None
            sentinel_hits: tuple[SearchHit, ...] | None = None
            sentinel_recall: float | None = None
            stages: list[ShadowAuditStageEvidence] = []
            try:
                query = self._query(query_id)
                if collect_trace:
                    query_vector = tuple(float(value) for value in query)
                oracle = self._oracle(query, flat_configuration)
                if collect_trace:
                    stages.append(
                        ShadowAuditStageEvidence(stage="ORACLE", success=True)
                    )
            except (ContractViolation, KeyError, TypeError, ValueError) as exc:
                failures += 1
                flat_agreement = False
                if collect_trace:
                    stages.append(
                        ShadowAuditStageEvidence(
                            stage=("QUERY_INPUT" if not query_vector else "ORACLE"),
                            success=False,
                            error_type=type(exc).__name__,
                        )
                    )
                    query_traces.append(
                        ShadowQueryAuditTrace(
                            query_id=query_id,
                            query_vector=query_vector,
                            threshold_radius=flat_configuration.radius,
                            range_filter=flat_configuration.range_filter,
                            limit=flat_configuration.limit,
                            oracle_result=None,
                            exact_cardinality=None,
                            flat_hits=None,
                            sentinel_hits=None,
                            sentinel_recall=None,
                            stages=tuple(stages),
                        )
                    )
                continue
            flat = self._timed_search(
                name=flat_name,
                query=query,
                configuration=flat_configuration,
            )
            if flat.exception is not None:
                failures += 1
                timeouts += int(flat.timed_out)
                flat_agreement = False
                if collect_trace:
                    stages.append(
                        ShadowAuditStageEvidence(
                            stage="FLAT",
                            success=False,
                            timed_out=flat.timed_out,
                            oracle_agreement=False,
                            error_type=type(flat.exception).__name__,
                        )
                    )
            else:
                assert flat.hits is not None
                flat_hits = flat.hits
                flat_violations = self._violation_count(
                    flat.hits, flat_configuration
                )
                agrees = self._flat_agrees(flat, oracle, flat_configuration)
                violations += flat_violations
                flat_agreement &= agrees
                if collect_trace:
                    stages.append(
                        ShadowAuditStageEvidence(
                            stage="FLAT",
                            success=flat_violations == 0 and agrees,
                            threshold_violation_count=flat_violations,
                            oracle_agreement=agrees,
                        )
                    )
            for configuration_index, configuration in enumerate(
                hnsw_configurations
            ):
                result = self._timed_search(
                    name=hnsw_name,
                    query=query,
                    configuration=configuration,
                )
                if result.exception is not None:
                    failures += 1
                    timeouts += int(result.timed_out)
                    if collect_trace:
                        stages.append(
                            ShadowAuditStageEvidence(
                                stage=(
                                    "CANDIDATE_HNSW"
                                    if configuration_index == 0
                                    else "LAST_KNOWN_GOOD_HNSW"
                                ),
                                success=False,
                                timed_out=result.timed_out,
                                error_type=type(result.exception).__name__,
                            )
                        )
                else:
                    assert result.hits is not None
                    result_violations = self._violation_count(
                        result.hits, configuration
                    )
                    violations += result_violations
                    if collect_trace:
                        stages.append(
                            ShadowAuditStageEvidence(
                                stage=(
                                    "CANDIDATE_HNSW"
                                    if configuration_index == 0
                                    else "LAST_KNOWN_GOOD_HNSW"
                                ),
                                success=result_violations == 0,
                                threshold_violation_count=result_violations,
                            )
                        )
            if collect_trace:
                assert sentinel_configuration is not None
                sentinel = self._timed_search(
                    name=hnsw_name,
                    query=query,
                    configuration=sentinel_configuration,
                )
                if sentinel.exception is not None:
                    stages.append(
                        ShadowAuditStageEvidence(
                            stage="SENTINEL_HNSW",
                            success=False,
                            timed_out=sentinel.timed_out,
                            error_type=type(sentinel.exception).__name__,
                        )
                    )
                else:
                    assert sentinel.hits is not None
                    sentinel_hits = sentinel.hits
                    sentinel_violations = self._violation_count(
                        sentinel.hits, sentinel_configuration
                    )
                    sentinel_recall = capped_threshold_recall(
                        (hit.id for hit in sentinel.hits),
                        oracle.ids,
                    )
                    stages.append(
                        ShadowAuditStageEvidence(
                            stage="SENTINEL_HNSW",
                            success=sentinel_violations == 0,
                            threshold_violation_count=sentinel_violations,
                        )
                    )
                query_traces.append(
                    ShadowQueryAuditTrace(
                        query_id=query_id,
                        query_vector=query_vector,
                        threshold_radius=flat_configuration.radius,
                        range_filter=flat_configuration.range_filter,
                        limit=flat_configuration.limit,
                        oracle_result=oracle,
                        exact_cardinality=oracle.full_count,
                        flat_hits=flat_hits,
                        sentinel_hits=sentinel_hits,
                        sentinel_recall=sentinel_recall,
                        stages=tuple(stages),
                    )
                )
        return _AuditRun(
            evidence=_AuditEvidence(
                failed_query_count=failures,
                timeout_query_count=timeouts,
                threshold_violation_count=violations,
                flat_oracle_agreement=flat_agreement,
            ),
            query_traces=tuple(query_traces),
        )

    def _audit(
        self,
        *,
        context: ActuationIdentityContext,
        query_ids: Sequence[QueryId],
        ef_values: Sequence[int],
    ) -> _AuditEvidence:
        return self._run_audit(
            context=context,
            query_ids=query_ids,
            ef_values=ef_values,
            collect_trace=False,
        ).evidence

    def _capture_identity(
        self,
        *,
        metric: Metric,
        track: IndexTrack,
        phase: str,
    ) -> _IdentityCapture:
        binding = self.workload.identity_bindings[(metric, track)]
        name = self.workload.collection_names[(metric, track)]
        try:
            actual = self.harness.index_identity(name, metric, track)
            matches = binding.matches(actual)
            snapshot = CollectionIdentity(
                collection_name=actual.collection_name,
                metric=actual.metric,
                index_track=actual.index_track,
                description=_freeze_json_value(actual.description),
            )
            return _IdentityCapture(
                snapshot=snapshot,
                binding_match=matches,
                stage=ShadowAuditStageEvidence(
                    stage=f"{phase}_{track.value}_IDENTITY",
                    success=matches,
                ),
            )
        except Exception as exc:  # noqa: BLE001 - injected client boundary
            return _IdentityCapture(
                snapshot=None,
                binding_match=False,
                stage=ShadowAuditStageEvidence(
                    stage=f"{phase}_{track.value}_IDENTITY",
                    success=False,
                    error_type=type(exc).__name__,
                ),
            )

    @staticmethod
    def _identity_evidence(
        *,
        track: IndexTrack,
        expected_binding_id: str,
        pre: _IdentityCapture,
        post: _IdentityCapture,
    ) -> ShadowIdentityEvidence:
        return ShadowIdentityEvidence(
            track=track,
            expected_binding_id=expected_binding_id,
            pre_snapshot=pre.snapshot,
            post_snapshot=post.snapshot,
            pre_binding_match=pre.binding_match,
            post_binding_match=post.binding_match,
            pre_capture=pre.stage,
            post_capture=post.stage,
        )

    def _build_shadow_trace(
        self,
        *,
        context: ShadowActuationContext,
        metric: Metric,
        candidate_ef: int,
        last_known_good_ef: int,
        audit_run: _AuditRun,
        pre_flat: _IdentityCapture,
        pre_hnsw: _IdentityCapture,
        post_flat: _IdentityCapture,
        post_hnsw: _IdentityCapture,
    ) -> ShadowAuditTrace:
        flat_binding = self.workload.identity_bindings[(metric, IndexTrack.FLAT)]
        hnsw_binding = self.workload.identity_bindings[(metric, IndexTrack.HNSW)]
        flat_identity = self._identity_evidence(
            track=IndexTrack.FLAT,
            expected_binding_id=flat_binding.identity_id,
            pre=pre_flat,
            post=post_flat,
        )
        hnsw_identity = self._identity_evidence(
            track=IndexTrack.HNSW,
            expected_binding_id=hnsw_binding.identity_id,
            pre=pre_hnsw,
            post=post_hnsw,
        )
        reasons: list[str] = []
        if len(context.audited_query_ids) != AUDIT_QUERY_COUNT:
            reasons.append("INVALID_AUDIT_QUERY_COUNT")
        if len(set(context.audited_query_ids)) != len(context.audited_query_ids):
            reasons.append("DUPLICATE_AUDIT_QUERY_ID")
        if not self._context_configuration_valid(context):
            reasons.append("CONTEXT_IDENTITY_MISMATCH")
        for identity in (flat_identity, hnsw_identity):
            for capture in (identity.pre_capture, identity.post_capture):
                if not capture.success:
                    reasons.append(f"STAGE_FAILED:{capture.stage}")
        for query_trace in audit_run.query_traces:
            if query_trace.oracle_result is None:
                reasons.append(f"ORACLE_MISSING:{query_trace.query_id}")
            if query_trace.flat_hits is None:
                reasons.append(f"FLAT_HITS_MISSING:{query_trace.query_id}")
            if query_trace.sentinel_hits is None or query_trace.sentinel_recall is None:
                reasons.append(f"SENTINEL_EVIDENCE_MISSING:{query_trace.query_id}")
            for stage in query_trace.stages:
                if not stage.success:
                    reasons.append(
                        f"STAGE_FAILED:{query_trace.query_id}:{stage.stage}"
                    )
                if stage.timed_out:
                    reasons.append(f"TIMEOUT:{query_trace.query_id}:{stage.stage}")
                if stage.threshold_violation_count:
                    reasons.append(
                        f"THRESHOLD_VIOLATION:{query_trace.query_id}:{stage.stage}"
                    )
        if len(audit_run.query_traces) != len(context.audited_query_ids):
            reasons.append("TRACE_QUERY_COUNT_MISMATCH")
        return ShadowAuditTrace(
            metric=metric,
            threshold_stratum=context.threshold_stratum,
            candidate_ef=candidate_ef,
            last_known_good_ef=last_known_good_ef,
            sentinel_ef=SENTINEL_EF,
            configuration_identity=self.workload.configuration_identity,
            data_identity=self.workload.data_identity,
            flat_identity=flat_identity,
            hnsw_identity=hnsw_identity,
            queries=audit_run.query_traces,
            complete=not reasons,
            reason_codes=tuple(dict.fromkeys(reasons)),
        )

    def capture_readonly_shadow_trace(
        self,
        *,
        context: ShadowActuationContext,
        served_ef: int,
    ) -> ShadowAuditTrace:
        """ADR-014 read-only 50-query capture: exact oracle + FLAT + sentinel.

        This is the already-tested `_run_audit`/`_build_shadow_trace` machinery
        with `ef_values=()`, so **no candidate and no last-known-good search is
        issued** -- those concepts are not semantically required by the ADR-002
        detector, which needs only the exact local oracle, the FLAT result, and
        the sentinel `ef=100` result per query.

        `ShadowAuditTrace` nevertheless has required `candidate_ef` and
        `last_known_good_ef` fields that are covered by its canonical digest and
        must agree across the four traces of a window. Both are set to the
        stream's real `served_ef`: a true fact about the serving breadth, never
        a fabricated candidate. No routing, policy, grant, activation, or
        actuation authority is created or consulted here.
        """

        try:
            context = validate_shadow_actuation_context(context)
        except ValueError as exc:
            raise ContractViolation(str(exc)) from exc
        _validate_actuation_ef(served_ef, name="served_ef")
        metric = self._metric(context)
        pre_flat = self._capture_identity(
            metric=metric, track=IndexTrack.FLAT, phase="PRE"
        )
        pre_hnsw = self._capture_identity(
            metric=metric, track=IndexTrack.HNSW, phase="PRE"
        )
        audit_run = self._run_audit(
            context=context,
            query_ids=context.audited_query_ids,
            ef_values=(),
            collect_trace=True,
        )
        post_flat = self._capture_identity(
            metric=metric, track=IndexTrack.FLAT, phase="POST"
        )
        post_hnsw = self._capture_identity(
            metric=metric, track=IndexTrack.HNSW, phase="POST"
        )
        return self._build_shadow_trace(
            context=context,
            metric=metric,
            candidate_ef=served_ef,
            last_known_good_ef=served_ef,
            audit_run=audit_run,
            pre_flat=pre_flat,
            pre_hnsw=pre_hnsw,
            post_flat=post_flat,
            post_hnsw=post_hnsw,
        )

    def shadow_candidate(
        self,
        *,
        context: ShadowActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult:
        """Audit 50 queries at both ef values against one exact FLAT/oracle baseline."""

        try:
            context = validate_shadow_actuation_context(context)
        except ValueError as exc:
            raise ContractViolation(str(exc)) from exc
        _validate_actuation_ef(candidate_ef, name="candidate_ef")
        _validate_actuation_ef(last_known_good_ef, name="last_known_good_ef")
        trace_sink = self.shadow_trace_sink
        if trace_sink is None:
            evidence = self._audit(
                context=context,
                query_ids=context.audited_query_ids,
                ef_values=(candidate_ef, last_known_good_ef),
            )
        else:
            metric = self._metric(context)
            pre_flat = self._capture_identity(
                metric=metric, track=IndexTrack.FLAT, phase="PRE"
            )
            pre_hnsw = self._capture_identity(
                metric=metric, track=IndexTrack.HNSW, phase="PRE"
            )
            audit_run = self._run_audit(
                context=context,
                query_ids=context.audited_query_ids,
                ef_values=(candidate_ef, last_known_good_ef),
                collect_trace=True,
            )
            post_flat = self._capture_identity(
                metric=metric, track=IndexTrack.FLAT, phase="POST"
            )
            post_hnsw = self._capture_identity(
                metric=metric, track=IndexTrack.HNSW, phase="POST"
            )
            trace = self._build_shadow_trace(
                context=context,
                metric=metric,
                candidate_ef=candidate_ef,
                last_known_good_ef=last_known_good_ef,
                audit_run=audit_run,
                pre_flat=pre_flat,
                pre_hnsw=pre_hnsw,
                post_flat=post_flat,
                post_hnsw=post_hnsw,
            )
            trace_sink.append(trace)
            evidence = audit_run.evidence
        context_valid = self._context_configuration_valid(context)
        passed = evidence.passed and context_valid
        return ShadowResult(
            success=passed,
            audited_query_count=len(context.audited_query_ids),
            failed_query_count=evidence.failed_query_count,
            timeout_query_count=evidence.timeout_query_count,
            threshold_violation_count=evidence.threshold_violation_count,
            candidate_flat_oracle_agreement=evidence.flat_oracle_agreement,
            last_known_good_flat_oracle_agreement=evidence.flat_oracle_agreement,
            detail=(
                "FLAT/oracle exact baseline agreed; HNSW was evaluated as recall"
                if passed
                else "shadow audit or workload/context identity validation failed"
            ),
        )

    def stop_candidate(self) -> None:
        """Clear adapter routing state; ef has no persistent Milvus server state."""

        self._candidate_ef = None

    def restore_last_known_good(self, ef: int) -> None:
        """Reset adapter default query-time ef; perform no server-side mutation."""

        self._default_ef = _validate_actuation_ef(ef, name="restored ef")

    def verify_restoration(
        self,
        *,
        context: RollbackActuationContext,
        expected_ef: int,
    ) -> RollbackVerification:
        """Verify routing state, health/identity, and a fresh 50-query restored audit."""

        try:
            context = validate_rollback_actuation_context(context)
        except ValueError as exc:
            raise ContractViolation(str(exc)) from exc
        _validate_actuation_ef(expected_ef, name="expected_ef")
        if context.expected_last_known_good_ef != expected_ef:
            raise ContractViolation("ROLLBACK_LAST_KNOWN_GOOD_EF_MISMATCH")
        evidence = self._audit(
            context=context,
            query_ids=context.audited_query_ids,
            ef_values=(expected_ef,),
        )
        status = self._runtime_status(context)
        metric = self._metric(context)
        routing_restored = bool(
            self._candidate_ef is None and self._default_ef == expected_ef
        )
        health_passed = bool(
            status.milvus_healthy
            and status.etcd_healthy
            and status.minio_healthy
            and status.collection_loaded
        )
        success = bool(
            routing_restored
            and health_passed
            and status.configuration_valid
            and status.index_identity_unchanged
            and evidence.passed
        )
        return RollbackVerification(
            success=success,
            restored_ef=self._default_ef,
            health_passed=health_passed,
            audit_passed=evidence.passed,
            configuration_identity=self.workload.configuration_identity,
            index_identity=self._hnsw_binding(metric).identity_id,
            data_identity=self.workload.data_identity,
            detail=(
                "adapter routing restored; no server-side ef mutation exists"
                if success
                else f"restoration verification failed: {status.detail}"
            ),
        )

    def _hnsw_binding(self, metric: Metric) -> CollectionIdentityBinding:
        return self.workload.identity_bindings[(metric, IndexTrack.HNSW)]

    def _context_configuration_valid(
        self, context: ActuationIdentityContext
    ) -> bool:
        try:
            metric = self._metric(context)
            hnsw_name = self.workload.collection_names[(metric, IndexTrack.HNSW)]
            hnsw_binding = self._hnsw_binding(metric)
            flat_binding = self.workload.identity_bindings[(metric, IndexTrack.FLAT)]
            return bool(
                context.threshold_stratum in THRESHOLD_LABELS
                and (metric, context.threshold_stratum) in self.workload.threshold_radii
                and context.collection_name == hnsw_name
                and context.configuration_identity
                == self.workload.configuration_identity
                and context.data_identity == self.workload.data_identity
                and context.index_identity == hnsw_binding.identity_id
                and context.flat_index_identity == flat_binding.identity_id
            )
        except (KeyError, TypeError, ValueError):
            return False

    def _runtime_status(self, context: ActuationIdentityContext) -> _RuntimeStatus:
        metric = self._metric(context)
        loaded = True
        identities_match = True
        details: list[str] = []
        for track in IndexTrack:
            key = (metric, track)
            name = self.workload.collection_names[key]
            try:
                load_state = self.client.get_load_state(collection_name=name)
                state = (
                    load_state.get("state")
                    if isinstance(load_state, dict)
                    else load_state
                )
                track_loaded = getattr(state, "name", str(state)) == "Loaded"
            except Exception as exc:  # noqa: BLE001 - injected client boundary
                track_loaded = False
                details.append(f"{track.value} load check failed: {type(exc).__name__}")
            loaded &= track_loaded
            try:
                actual = self.harness.index_identity(name, metric, track)
                track_identity = self.workload.identity_bindings[key].matches(actual)
            except Exception as exc:  # noqa: BLE001 - injected client boundary
                track_identity = False
                details.append(
                    f"{track.value} identity check failed: {type(exc).__name__}"
                )
            identities_match &= track_identity
        try:
            stack_health = self.stack_health_probe.check()
            if not isinstance(stack_health, StackHealth):
                raise TypeError("health probe returned the wrong type")
            etcd_healthy = stack_health.etcd_healthy is True
            minio_healthy = stack_health.minio_healthy is True
            if stack_health.detail:
                details.append(stack_health.detail)
        except Exception as exc:  # noqa: BLE001 - injected health-probe boundary
            etcd_healthy = False
            minio_healthy = False
            details.append(f"stack health probe failed: {type(exc).__name__}")
        configuration_valid = self._context_configuration_valid(context)
        if not configuration_valid:
            details.append("workload/context configuration identity mismatch")
        if not loaded:
            details.append("one or more collections are not Loaded")
        if not identities_match:
            details.append("one or more collection identities changed")
        return _RuntimeStatus(
            milvus_healthy=loaded,
            etcd_healthy=etcd_healthy,
            minio_healthy=minio_healthy,
            collection_loaded=loaded,
            configuration_valid=configuration_valid,
            index_identity_unchanged=identities_match,
            detail="; ".join(details) or "all runtime checks passed",
        )


__all__ = [
    "CANARY_BATCH_SIZE",
    "ActuationWorkload",
    "CanaryBoundEstimatorLike",
    "CanaryBounds",
    "CanaryPairedMeasurements",
    "CollectionIdentityBinding",
    "MilvusActuationClient",
    "ShadowAuditStageEvidence",
    "ShadowAuditTrace",
    "ShadowAuditTraceSinkLike",
    "ShadowIdentityEvidence",
    "ShadowQueryAuditTrace",
    "StackHealth",
    "StackHealthProbeLike",
]
