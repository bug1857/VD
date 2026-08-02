"""Milvus-backed ADR-002 actuation adapter with dependency-injected evidence.

Purpose:
    Execute shadow, deterministic canary-routing, and restoration verification
    through ``MilvusHarness`` without embedding drift or policy decisions.
Inputs:
    An immutable workload, a PyMilvus-client-like object, expected collection
    identities, deterministic routing seed, external bounds estimator, and
    external etcd/MinIO health probe.
Outputs:
    ``ShadowResult``, ``CanaryObservation``, and ``RollbackVerification`` values
    consumed by the existing safe-actuation boundary.
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
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from statistics import mean
from time import perf_counter_ns
from types import MappingProxyType
from typing import Protocol, Self, TypeAlias

import numpy as np
import numpy.typing as npt

from .actuation import (
    MAX_CANARY_TRAFFIC_FRACTION,
    ActuationContext,
    QueryId,
    RollbackVerification,
    ShadowResult,
)
from .config import (
    NUMERIC_TOLERANCE,
    THRESHOLD_LABELS,
    ContractViolation,
    IndexTrack,
    Metric,
    SearchConfiguration,
)
from .drift import canonical_serialize_tuple
from .milvus import ClientLike, CollectionIdentity, MilvusHarness, SearchHit
from .oracle import (
    OracleResult,
    capped_threshold_recall,
    exact_range_search,
    threshold_violations,
)
from .policy import ACTUATION_LADDER, CanaryObservation

CANARY_BATCH_SIZE = 500
CANARY_CANDIDATE_COUNT = 50
CANARY_ROUTING_DOMAIN = "ADR-002-CANARY-ROUTING-v1"

ClockNs: TypeAlias = Callable[[], int]


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
class CanaryRouteSelection:
    """Deterministic 50/450 routing result for one 500-query batch."""

    candidate_query_ids: tuple[QueryId, ...]
    last_known_good_query_ids: tuple[QueryId, ...]
    candidate_digest_hex: tuple[str, ...]


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


def select_canary_routes(
    query_ids: Sequence[QueryId],
    *,
    routing_seed: int,
    metric: Metric | str,
    threshold_stratum: str,
    traffic_fraction: float,
) -> CanaryRouteSelection:
    """Select exactly 50 of 500 IDs by keyed-BLAKE2b rank.

    The SHA-256 key is derived from the canonical tuple ``(domain, seed)``. Each
    rank hashes ``(metric, threshold_stratum, query_id)`` and uses the canonical
    query-ID bytes as the deterministic collision tie-break.
    """

    _validate_routing_seed(routing_seed)
    if (
        isinstance(traffic_fraction, bool)
        or not isinstance(traffic_fraction, (int, float))
        or not math.isfinite(traffic_fraction)
        or float(traffic_fraction) != MAX_CANARY_TRAFFIC_FRACTION
    ):
        raise ValueError("canary traffic_fraction must be exactly 0.10")
    normalized_metric = Metric(metric)
    if threshold_stratum not in THRESHOLD_LABELS:
        raise ValueError("threshold stratum must be canonical")
    values = tuple(query_ids)
    if len(values) != CANARY_BATCH_SIZE:
        raise ValueError("canary routing requires exactly 500 query IDs")
    for query_id in values:
        _validate_query_id(query_id)
    if len(set(values)) != CANARY_BATCH_SIZE:
        raise ValueError("canary routing requires 500 unique query IDs")

    key = hashlib.sha256(
        canonical_serialize_tuple((CANARY_ROUTING_DOMAIN, routing_seed))
    ).digest()
    ranked: list[tuple[bytes, bytes, QueryId]] = []
    for query_id in values:
        encoded_id = canonical_serialize_tuple((query_id,))
        message = canonical_serialize_tuple(
            (normalized_metric.value, threshold_stratum, query_id)
        )
        digest = hashlib.blake2b(message, key=key, digest_size=32).digest()
        ranked.append((digest, encoded_id, query_id))
    ranked.sort(key=lambda item: (item[0], item[1]))
    candidate = ranked[:CANARY_CANDIDATE_COUNT]
    candidate_keys = {item[1] for item in candidate}
    last_known_good = tuple(
        query_id
        for query_id in values
        if canonical_serialize_tuple((query_id,)) not in candidate_keys
    )
    return CanaryRouteSelection(
        candidate_query_ids=tuple(item[2] for item in candidate),
        last_known_good_query_ids=last_known_good,
        candidate_digest_hex=tuple(item[0].hex() for item in candidate),
    )


class MilvusActuationClient:
    """ActuationClientLike implementation composed around ``MilvusHarness``."""

    def __init__(
        self,
        client: ClientLike,
        *,
        workload: ActuationWorkload,
        routing_seed: int,
        bound_estimator: CanaryBoundEstimatorLike,
        stack_health_probe: StackHealthProbeLike,
        initial_ef: int,
        clock_ns: ClockNs = perf_counter_ns,
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

    def _metric(self, context: ActuationContext) -> Metric:
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

    def _audit(
        self,
        *,
        context: ActuationContext,
        query_ids: Sequence[QueryId],
        ef_values: Sequence[int],
    ) -> _AuditEvidence:
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
        flat_name = self.workload.collection_names[(metric, IndexTrack.FLAT)]
        hnsw_name = self.workload.collection_names[(metric, IndexTrack.HNSW)]
        failures = 0
        timeouts = 0
        violations = 0
        flat_agreement = True
        for query_id in query_ids:
            try:
                query = self._query(query_id)
                oracle = self._oracle(query, flat_configuration)
            except (ContractViolation, KeyError, TypeError, ValueError):
                failures += 1
                flat_agreement = False
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
            else:
                assert flat.hits is not None
                violations += self._violation_count(flat.hits, flat_configuration)
                flat_agreement &= self._flat_agrees(flat, oracle, flat_configuration)
            for configuration in hnsw_configurations:
                result = self._timed_search(
                    name=hnsw_name,
                    query=query,
                    configuration=configuration,
                )
                if result.exception is not None:
                    failures += 1
                    timeouts += int(result.timed_out)
                else:
                    assert result.hits is not None
                    violations += self._violation_count(result.hits, configuration)
        return _AuditEvidence(
            failed_query_count=failures,
            timeout_query_count=timeouts,
            threshold_violation_count=violations,
            flat_oracle_agreement=flat_agreement,
        )

    def shadow_candidate(
        self,
        *,
        context: ActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult:
        """Audit 50 queries at both ef values against one exact FLAT/oracle baseline."""

        _validate_actuation_ef(candidate_ef, name="candidate_ef")
        _validate_actuation_ef(last_known_good_ef, name="last_known_good_ef")
        evidence = self._audit(
            context=context,
            query_ids=context.audited_query_ids,
            ef_values=(candidate_ef, last_known_good_ef),
        )
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

    def start_canary(
        self,
        *,
        context: ActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
        traffic_fraction: float,
    ) -> CanaryObservation:
        """Route 50/500 requests to candidate ef and shadow those same 50 at LKG."""

        _validate_actuation_ef(candidate_ef, name="candidate_ef")
        _validate_actuation_ef(last_known_good_ef, name="last_known_good_ef")
        self.workload.validate_for_canary()
        if not self._context_configuration_valid(context):
            raise ContractViolation("workload/context configuration identity mismatch")
        metric = self._metric(context)
        routes = select_canary_routes(
            self.workload.canary_query_ids,
            routing_seed=self.routing_seed,
            metric=metric,
            threshold_stratum=context.threshold_stratum,
            traffic_fraction=traffic_fraction,
        )
        candidate_ids = set(routes.candidate_query_ids)
        flat_configuration = self._configuration(
            metric=metric,
            threshold_stratum=context.threshold_stratum,
            track=IndexTrack.FLAT,
        )
        candidate_configuration = self._configuration(
            metric=metric,
            threshold_stratum=context.threshold_stratum,
            track=IndexTrack.HNSW,
            ef=candidate_ef,
        )
        last_known_good_configuration = self._configuration(
            metric=metric,
            threshold_stratum=context.threshold_stratum,
            track=IndexTrack.HNSW,
            ef=last_known_good_ef,
        )
        flat_name = self.workload.collection_names[(metric, IndexTrack.FLAT)]
        hnsw_name = self.workload.collection_names[(metric, IndexTrack.HNSW)]

        self._candidate_ef = candidate_ef
        self._default_ef = last_known_good_ef
        failures = 0
        timeouts = 0
        violations = 0
        flat_agreement = True
        candidate_query_ids: list[QueryId] = []
        candidate_recalls: list[float] = []
        paired_recalls: list[float] = []
        candidate_latencies: list[float] = []
        paired_latencies: list[float] = []

        for query_id in self.workload.canary_query_ids:
            query = self._query(query_id)
            if query_id not in candidate_ids:
                served = self._timed_search(
                    name=hnsw_name,
                    query=query,
                    configuration=last_known_good_configuration,
                )
                if served.exception is not None:
                    failures += 1
                    timeouts += int(served.timed_out)
                else:
                    assert served.hits is not None
                    violations += self._violation_count(
                        served.hits, last_known_good_configuration
                    )
                continue

            oracle = self._oracle(query, flat_configuration)
            flat = self._timed_search(
                name=flat_name,
                query=query,
                configuration=flat_configuration,
            )
            candidate = self._timed_search(
                name=hnsw_name,
                query=query,
                configuration=candidate_configuration,
            )
            paired = self._timed_search(
                name=hnsw_name,
                query=query,
                configuration=last_known_good_configuration,
            )
            candidate_query_ids.append(query_id)

            for result, configuration in (
                (flat, flat_configuration),
                (candidate, candidate_configuration),
                (paired, last_known_good_configuration),
            ):
                if result.exception is not None:
                    failures += 1
                    timeouts += int(result.timed_out)
                else:
                    assert result.hits is not None
                    violations += self._violation_count(result.hits, configuration)
            flat_agreement &= self._flat_agrees(flat, oracle, flat_configuration)
            if candidate.hits is not None:
                candidate_recalls.append(
                    capped_threshold_recall(
                        (hit.id for hit in candidate.hits), oracle.ids
                    )
                )
                candidate_latencies.append(candidate.latency_ms)
            if paired.hits is not None:
                paired_recalls.append(
                    capped_threshold_recall((hit.id for hit in paired.hits), oracle.ids)
                )
                paired_latencies.append(paired.latency_ms)

        complete_measurements = bool(
            len(candidate_query_ids) == CANARY_CANDIDATE_COUNT
            and len(candidate_recalls) == CANARY_CANDIDATE_COUNT
            and len(paired_recalls) == CANARY_CANDIDATE_COUNT
            and len(candidate_latencies) == CANARY_CANDIDATE_COUNT
            and len(paired_latencies) == CANARY_CANDIDATE_COUNT
        )
        candidate_mean = mean(candidate_recalls) if candidate_recalls else 0.0
        paired_mean = mean(paired_recalls) if paired_recalls else 0.0
        candidate_p95 = _p95(candidate_latencies)
        paired_p95 = _p95(paired_latencies)
        recall_lower_bound = 0.0
        latency_upper_bound = candidate_p95
        if complete_measurements:
            measurements = CanaryPairedMeasurements(
                query_ids=tuple(candidate_query_ids),
                candidate_recalls=tuple(candidate_recalls),
                last_known_good_recalls=tuple(paired_recalls),
                candidate_latencies_ms=tuple(candidate_latencies),
                last_known_good_latencies_ms=tuple(paired_latencies),
            )
            bounds = self.bound_estimator.estimate(measurements)
            _validate_bounds(bounds, mean_recall=candidate_mean, p95=candidate_p95)
            recall_lower_bound = bounds.recall_lower_bound_95
            latency_upper_bound = bounds.latency_upper_bound_95_ms

        status = self._runtime_status(context)
        return CanaryObservation(
            metric=metric,
            threshold_stratum=context.threshold_stratum,
            candidate_ef=candidate_ef,
            last_known_good_ef=last_known_good_ef,
            completed_query_count=len(candidate_query_ids),
            candidate_mean_recall=candidate_mean,
            candidate_recall_lower_bound_95=recall_lower_bound,
            last_known_good_mean_recall=paired_mean,
            candidate_p95_latency_ms=candidate_p95,
            candidate_latency_upper_bound_95_ms=latency_upper_bound,
            last_known_good_p95_latency_ms=paired_p95,
            configuration_identity=self.workload.configuration_identity,
            index_identity=self._hnsw_binding(metric).identity_id,
            data_identity=self.workload.data_identity,
            failed_query_count=failures,
            timeout_query_count=timeouts,
            threshold_violation_count=violations,
            flat_oracle_agreement=flat_agreement,
            milvus_healthy=status.milvus_healthy,
            etcd_healthy=status.etcd_healthy,
            minio_healthy=status.minio_healthy,
            collection_loaded=status.collection_loaded,
            configuration_valid=status.configuration_valid,
            index_identity_unchanged=status.index_identity_unchanged,
            audit_record_present=True,
            actuation_exception=False,
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
        context: ActuationContext,
        expected_ef: int,
    ) -> RollbackVerification:
        """Verify routing state, health/identity, and a fresh 50-query restored audit."""

        _validate_actuation_ef(expected_ef, name="expected_ef")
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

    def _context_configuration_valid(self, context: ActuationContext) -> bool:
        try:
            metric = self._metric(context)
            hnsw_name = self.workload.collection_names[(metric, IndexTrack.HNSW)]
            binding = self._hnsw_binding(metric)
            return bool(
                context.threshold_stratum in THRESHOLD_LABELS
                and (metric, context.threshold_stratum) in self.workload.threshold_radii
                and context.collection_name == hnsw_name
                and context.configuration_identity
                == self.workload.configuration_identity
                and context.data_identity == self.workload.data_identity
                and context.index_identity == binding.identity_id
            )
        except (KeyError, TypeError, ValueError):
            return False

    def _runtime_status(self, context: ActuationContext) -> _RuntimeStatus:
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


def _p95(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(
        np.percentile(np.asarray(values, dtype=np.float64), 95, method="linear")
    )


def _validate_bounds(
    bounds: object,
    *,
    mean_recall: float,
    p95: float,
) -> None:
    if not isinstance(bounds, CanaryBounds):
        raise TypeError("bound estimator must return CanaryBounds")
    if (
        not math.isfinite(bounds.recall_lower_bound_95)
        or not 0.0 <= bounds.recall_lower_bound_95 <= mean_recall <= 1.0
        or not math.isfinite(bounds.latency_upper_bound_95_ms)
        or bounds.latency_upper_bound_95_ms < p95
        or bounds.confidence_level != 0.95
        or not _nonempty(bounds.provenance)
    ):
        raise ValueError("bound estimator returned invalid one-sided 95% bounds")


__all__ = [
    "CANARY_BATCH_SIZE",
    "CANARY_CANDIDATE_COUNT",
    "CANARY_ROUTING_DOMAIN",
    "ActuationWorkload",
    "CanaryBoundEstimatorLike",
    "CanaryBounds",
    "CanaryPairedMeasurements",
    "CanaryRouteSelection",
    "CollectionIdentityBinding",
    "MilvusActuationClient",
    "StackHealth",
    "StackHealthProbeLike",
    "select_canary_routes",
]
