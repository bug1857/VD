"""Immutable offline 60-of-600 route-plan construction for EXP-009 Stage 2.

This module turns already-verified Stage-1 workload/selection values into one
immutable candidate/LKG partition.  It has no filesystem, network, policy,
approval, state-installation, or Milvus dependency.  A later route authority
owns one-shot occurrence claims, atomic installation/removal, and failback.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

from .artifacts import canonical_json_bytes
from .canary_workload import (
    CandidateSelectionRecord,
    EligibleWorkloadManifest,
    WorkloadIdentityBinding,
)
from .config import (
    HNSW_EF_SWEEP,
    RESULT_LIMIT,
    THRESHOLD_LABELS,
    ContractViolation,
    Metric,
)

__all__ = [
    "CanaryRouteKind",
    "CanaryRoutePlan",
    "RouteOccurrence",
    "RouteResolution",
    "build_canary_route_plan",
]


CANARY_ROUTE_PLAN_SCHEMA_VERSION = "exp009-canary-route-plan-v1"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_OCCURRENCE_RE = re.compile(r"exp009-routing-[0-9]{6}\Z")


class CanaryRouteKind(StrEnum):
    """The only two query-time route assignments an immutable plan can emit."""

    CANDIDATE = "CANDIDATE"
    LAST_KNOWN_GOOD = "LAST_KNOWN_GOOD"


@dataclass(frozen=True, slots=True)
class RouteOccurrence:
    """One manifest-bound non-sensitive occurrence available to a route plan."""

    sequence_index: int
    occurrence_id: str
    dataset_query_id: int
    vector_sha256: str
    threshold_radius: float
    range_filter: float
    limit: int


@dataclass(frozen=True, slots=True)
class RouteResolution:
    """A no-I/O foreground lookup result; refusal never names a candidate ef."""

    accepted: bool
    occurrence_id: str | None
    dataset_query_id: int | None
    ef: int | None
    kind: CanaryRouteKind | None
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class CanaryRoutePlan:
    """Immutable exact partition derived solely from frozen Stage-1 evidence."""

    eligible_workload_sha256: str
    candidate_selection_sha256: str
    metric: Metric
    threshold_stratum: str
    candidate_ef: int
    last_known_good_ef: int
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str
    occurrences: tuple[RouteOccurrence, ...]
    candidate_occurrence_ids: tuple[str, ...]
    plan_sha256: str
    _by_occurrence_id: Mapping[str, RouteOccurrence] = field(
        init=False,
        repr=False,
        compare=False,
    )
    _candidate_occurrence_id_set: frozenset[str] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        by_occurrence_id, candidate_occurrence_id_set = _validate_route_plan(self)
        object.__setattr__(
            self,
            "_by_occurrence_id",
            MappingProxyType(by_occurrence_id),
        )
        object.__setattr__(self, "_candidate_occurrence_id_set", candidate_occurrence_id_set)

    @property
    def population_count(self) -> int:
        """The fixed eligible population cardinality."""

        return len(self.occurrences)

    @property
    def candidate_count(self) -> int:
        """The fixed candidate partition cardinality."""

        return len(self._candidate_occurrence_id_set)

    @property
    def last_known_good_occurrence_ids(self) -> tuple[str, ...]:
        """Manifest-ordered complement of the candidate partition."""

        return tuple(
            item.occurrence_id
            for item in self.occurrences
            if item.occurrence_id not in self._candidate_occurrence_id_set
        )

    def resolve(self, occurrence_id: object) -> RouteResolution:
        """Resolve one valid occurrence without I/O, mutation, or dispatch."""

        if not isinstance(occurrence_id, str) or _OCCURRENCE_RE.fullmatch(occurrence_id) is None:
            return RouteResolution(
                accepted=False,
                occurrence_id=None,
                dataset_query_id=None,
                ef=None,
                kind=None,
                reason_code="OCCURRENCE_ID_INVALID",
            )
        occurrence = self._by_occurrence_id.get(occurrence_id)
        if occurrence is None:
            return RouteResolution(
                accepted=False,
                occurrence_id=occurrence_id,
                dataset_query_id=None,
                ef=None,
                kind=None,
                reason_code="OCCURRENCE_UNKNOWN",
            )
        if occurrence_id in self._candidate_occurrence_id_set:
            return RouteResolution(
                accepted=True,
                occurrence_id=occurrence_id,
                dataset_query_id=occurrence.dataset_query_id,
                ef=self.candidate_ef,
                kind=CanaryRouteKind.CANDIDATE,
            )
        return RouteResolution(
            accepted=True,
            occurrence_id=occurrence_id,
            dataset_query_id=occurrence.dataset_query_id,
            ef=self.last_known_good_ef,
            kind=CanaryRouteKind.LAST_KNOWN_GOOD,
        )


def _digest(document: object) -> str:
    return hashlib.sha256(canonical_json_bytes(document)).hexdigest()


def _route_occurrence_document(item: RouteOccurrence) -> dict[str, object]:
    return {
        "sequence_index": item.sequence_index,
        "occurrence_id": item.occurrence_id,
        "dataset_query_id": item.dataset_query_id,
        "vector_sha256": item.vector_sha256,
        "threshold_radius": item.threshold_radius,
        "range_filter": item.range_filter,
        "limit": item.limit,
    }


def _plan_document(
    *,
    eligible_workload_sha256: str,
    candidate_selection_sha256: str,
    metric: Metric,
    threshold_stratum: str,
    candidate_ef: int,
    last_known_good_ef: int,
    configuration_identity: str,
    data_identity: str,
    flat_binding_id: str,
    hnsw_binding_id: str,
    occurrences: tuple[RouteOccurrence, ...],
    candidate_occurrence_ids: tuple[str, ...],
) -> dict[str, object]:
    return {
        "schema_version": CANARY_ROUTE_PLAN_SCHEMA_VERSION,
        "eligible_workload_sha256": eligible_workload_sha256,
        "candidate_selection_sha256": candidate_selection_sha256,
        "metric": metric.value,
        "threshold_stratum": threshold_stratum,
        "candidate_ef": candidate_ef,
        "last_known_good_ef": last_known_good_ef,
        "identity": {
            "configuration_identity": configuration_identity,
            "data_identity": data_identity,
            "flat_binding_id": flat_binding_id,
            "hnsw_binding_id": hnsw_binding_id,
        },
        "occurrences": [_route_occurrence_document(item) for item in occurrences],
        "candidate_occurrence_ids": list(candidate_occurrence_ids),
    }


def _route_plan_document(plan: CanaryRoutePlan) -> dict[str, object]:
    return _plan_document(
        eligible_workload_sha256=plan.eligible_workload_sha256,
        candidate_selection_sha256=plan.candidate_selection_sha256,
        metric=plan.metric,
        threshold_stratum=plan.threshold_stratum,
        candidate_ef=plan.candidate_ef,
        last_known_good_ef=plan.last_known_good_ef,
        configuration_identity=plan.configuration_identity,
        data_identity=plan.data_identity,
        flat_binding_id=plan.flat_binding_id,
        hnsw_binding_id=plan.hnsw_binding_id,
        occurrences=plan.occurrences,
        candidate_occurrence_ids=plan.candidate_occurrence_ids,
    )


def _route_plan_invalid() -> ValueError:
    return ValueError("ROUTE_PLAN_INVALID")


def _validate_route_plan(
    plan: CanaryRoutePlan,
) -> tuple[dict[str, RouteOccurrence], frozenset[str]]:
    """Validate direct construction too, so frozen values cannot be forged."""

    if (
        not isinstance(plan.eligible_workload_sha256, str)
        or _SHA256_RE.fullmatch(plan.eligible_workload_sha256) is None
        or not isinstance(plan.candidate_selection_sha256, str)
        or _SHA256_RE.fullmatch(plan.candidate_selection_sha256) is None
        or not isinstance(plan.plan_sha256, str)
        or _SHA256_RE.fullmatch(plan.plan_sha256) is None
        or not isinstance(plan.metric, Metric)
        or plan.threshold_stratum not in THRESHOLD_LABELS
    ):
        raise _route_plan_invalid()
    ladder = tuple(value for value in HNSW_EF_SWEEP if value != 100)
    if (
        type(plan.candidate_ef) is not int
        or type(plan.last_known_good_ef) is not int
        or plan.candidate_ef not in ladder
        or plan.last_known_good_ef not in ladder
        or ladder.index(plan.candidate_ef) != ladder.index(plan.last_known_good_ef) + 1
    ):
        raise _route_plan_invalid()
    try:
        WorkloadIdentityBinding(
            configuration_identity=plan.configuration_identity,
            data_identity=plan.data_identity,
            flat_binding_id=plan.flat_binding_id,
            hnsw_binding_id=plan.hnsw_binding_id,
        ).validate()
    except (ContractViolation, TypeError, ValueError) as exc:
        raise _route_plan_invalid() from exc
    if (
        not isinstance(plan.occurrences, tuple)
        or len(plan.occurrences) != 600
        or not isinstance(plan.candidate_occurrence_ids, tuple)
        or len(plan.candidate_occurrence_ids) != 60
    ):
        raise _route_plan_invalid()
    by_occurrence_id: dict[str, RouteOccurrence] = {}
    vector_hashes: set[str] = set()
    common_search: tuple[float, float, int] | None = None
    for expected_index, occurrence in enumerate(plan.occurrences):
        if (
            not isinstance(occurrence, RouteOccurrence)
            or type(occurrence.sequence_index) is not int
            or occurrence.sequence_index != expected_index
            or not isinstance(occurrence.occurrence_id, str)
            or _OCCURRENCE_RE.fullmatch(occurrence.occurrence_id) is None
            or type(occurrence.dataset_query_id) is not int
            or occurrence.dataset_query_id < 0
            or not isinstance(occurrence.vector_sha256, str)
            or _SHA256_RE.fullmatch(occurrence.vector_sha256) is None
            or type(occurrence.limit) is not int
            or occurrence.limit != RESULT_LIMIT
            or not isinstance(occurrence.threshold_radius, float)
            or not isinstance(occurrence.range_filter, float)
            or not math.isfinite(occurrence.threshold_radius)
            or not math.isfinite(occurrence.range_filter)
        ):
            raise _route_plan_invalid()
        if occurrence.occurrence_id in by_occurrence_id or occurrence.vector_sha256 in vector_hashes:
            raise _route_plan_invalid()
        if plan.metric is Metric.L2:
            search_valid = occurrence.threshold_radius > 0.0 and occurrence.range_filter == 0.0
        else:
            search_valid = -1.0 <= occurrence.threshold_radius < 1.0 and occurrence.range_filter == 1.0
        search_binding = (
            occurrence.threshold_radius,
            occurrence.range_filter,
            occurrence.limit,
        )
        if not search_valid or (common_search is not None and search_binding != common_search):
            raise _route_plan_invalid()
        common_search = search_binding
        by_occurrence_id[occurrence.occurrence_id] = occurrence
        vector_hashes.add(occurrence.vector_sha256)
    selected_ids = plan.candidate_occurrence_ids
    selected_set = frozenset(selected_ids)
    occurrence_ids = tuple(by_occurrence_id)
    if (
        len(selected_set) != 60
        or not selected_set.issubset(by_occurrence_id)
        or selected_ids
        != tuple(occurrence_id for occurrence_id in occurrence_ids if occurrence_id in selected_set)
    ):
        raise _route_plan_invalid()
    if _digest(_route_plan_document(plan)) != plan.plan_sha256:
        raise _route_plan_invalid()
    return by_occurrence_id, selected_set


def _validated_manifest(manifest: object) -> EligibleWorkloadManifest:
    if not isinstance(manifest, EligibleWorkloadManifest):
        raise ValueError("ELIGIBLE_WORKLOAD_INVALID")  # domain error type carries the governed reason code  # noqa: TRY004
    try:
        manifest.validate()
    except (ContractViolation, TypeError, ValueError) as exc:
        raise ValueError("ELIGIBLE_WORKLOAD_INVALID") from exc
    return manifest


def _validated_selection(selection: object) -> CandidateSelectionRecord:
    if not isinstance(selection, CandidateSelectionRecord):
        raise ValueError("CANDIDATE_SELECTION_INVALID")  # domain error type carries the governed reason code  # noqa: TRY004
    try:
        selection.validate()
    except (ContractViolation, TypeError, ValueError) as exc:
        raise ValueError("CANDIDATE_SELECTION_INVALID") from exc
    return selection


def build_canary_route_plan(
    manifest: EligibleWorkloadManifest,
    selection: CandidateSelectionRecord,
) -> CanaryRoutePlan:
    """Build the only valid static 60/540 route partition, or fail closed."""

    workload = _validated_manifest(manifest)
    candidate_selection = _validated_selection(selection)
    workload_sha256 = _digest(workload.to_document())
    if candidate_selection.eligible_manifest_sha256 != workload_sha256:
        raise ValueError("SELECTION_MANIFEST_MISMATCH")

    occurrence_ids = tuple(item.occurrence_id for item in workload.occurrences)
    selected_ids = candidate_selection.candidate_occurrence_ids
    selected_set = frozenset(selected_ids)
    if not selected_set.issubset(set(occurrence_ids)):
        raise ValueError("CANDIDATE_SELECTION_OUTSIDE_WORKLOAD")
    if selected_ids != tuple(
        occurrence_id for occurrence_id in occurrence_ids if occurrence_id in selected_set
    ):
        raise ValueError("CANDIDATE_SELECTION_ORDER_INVALID")
    occurrences = tuple(
        RouteOccurrence(
            sequence_index=item.sequence_index,
            occurrence_id=item.occurrence_id,
            dataset_query_id=item.dataset_query_id,
            vector_sha256=item.vector_sha256,
            threshold_radius=item.threshold_radius,
            range_filter=item.range_filter,
            limit=item.limit,
        )
        for item in workload.occurrences
    )
    selection_sha256 = _digest(candidate_selection.to_document())
    plan_sha256 = _digest(
        _plan_document(
            eligible_workload_sha256=workload_sha256,
            candidate_selection_sha256=selection_sha256,
            metric=workload.metric,
            threshold_stratum=workload.threshold_stratum,
            candidate_ef=workload.candidate_ef,
            last_known_good_ef=workload.last_known_good_ef,
            configuration_identity=workload.identity.configuration_identity,
            data_identity=workload.identity.data_identity,
            flat_binding_id=workload.identity.flat_binding_id,
            hnsw_binding_id=workload.identity.hnsw_binding_id,
            occurrences=occurrences,
            candidate_occurrence_ids=selected_ids,
        )
    )
    return CanaryRoutePlan(
        eligible_workload_sha256=workload_sha256,
        candidate_selection_sha256=selection_sha256,
        metric=workload.metric,
        threshold_stratum=workload.threshold_stratum,
        candidate_ef=workload.candidate_ef,
        last_known_good_ef=workload.last_known_good_ef,
        configuration_identity=workload.identity.configuration_identity,
        data_identity=workload.identity.data_identity,
        flat_binding_id=workload.identity.flat_binding_id,
        hnsw_binding_id=workload.identity.hnsw_binding_id,
        occurrences=occurrences,
        candidate_occurrence_ids=selected_ids,
        plan_sha256=plan_sha256,
    )
