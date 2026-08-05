"""Pure, immutable Stage-4 serial schedule construction for EXP-009.

Purpose:
    Derive the frozen 1,200-slot Stage-4 cadence from the verified eligible
    workload and already-partitioned route plan before a future composition
    root can dispatch any request.
Inputs:
    An ``EligibleWorkloadManifest`` and matching ``CanaryRoutePlan``.
Outputs:
    A canonical, schema-versioned ``Stage4ExecutionSchedule`` containing only
    non-sensitive query bindings, expected route kind/``ef``, and a digest.
Dependencies:
    Immutable workload/routing value objects and canonical JSON hashing only.
    This module has no I/O, client, approval, route-authority, or activation
    dependency.
Complexity:
    O(1,200) time and space.
Failure modes:
    Any malformed schedule, digest mismatch, or manifest/plan binding mismatch
    raises a stable ``ValueError`` before a future runner can dispatch work.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import hashlib
import math
import re

from .artifacts import canonical_json_bytes
from .canary_routing import CanaryRouteKind, CanaryRoutePlan, RouteOccurrence
from .canary_statistics import EXP009_CANDIDATE_COUNT, EXP009_ROUTING_POPULATION_COUNT
from .canary_workload import (
    SCHEDULE_CONTROL_COUNT,
    SCHEDULE_INTERLEAVED_SWEEP_COUNT,
    SCHEDULE_POST_SWEEP_COUNT,
    SCHEDULE_PRE_SWEEP_COUNT,
    SCHEDULE_ROUTING_BLOCK_SIZE,
    EligibleWorkloadManifest,
    ScheduleControl,
)
from .config import ContractViolation, HNSW_EF_SWEEP, Metric, RESULT_LIMIT, THRESHOLD_LABELS


__all__ = [
    "CanaryRouteKind",
    "Stage4ExecutionSchedule",
    "Stage4ScheduleStep",
    "Stage4ScheduleStepKind",
    "build_stage4_execution_schedule",
]


STAGE4_EXECUTION_SCHEDULE_SCHEMA_VERSION = "exp009-stage4-execution-schedule-v1"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_OCCURRENCE_RE = re.compile(r"exp009-routing-([0-9]{6})\Z")
_ACTUATION_LADDER = tuple(value for value in HNSW_EF_SWEEP if value != 100)
_CONTROL_SWEEP_COUNT = (
    SCHEDULE_PRE_SWEEP_COUNT
    + SCHEDULE_INTERLEAVED_SWEEP_COUNT
    + SCHEDULE_POST_SWEEP_COUNT
)
_STEP_COUNT = (
    EXP009_ROUTING_POPULATION_COUNT
    + _CONTROL_SWEEP_COUNT * SCHEDULE_CONTROL_COUNT
)


class Stage4ScheduleStepKind(StrEnum):
    """The two query categories permitted by the frozen Stage-4 cadence."""

    CONTROL = "CONTROL"
    ROUTING = "ROUTING"


@dataclass(frozen=True, slots=True)
class Stage4ScheduleStep:
    """One non-sensitive, pre-dispatch schedule slot.

    A control step binds one LKG-only schedule-control ID and digest.  A
    routing step binds one immutable plan occurrence and its already-selected
    route kind.  Neither form contains a raw query value or mutable outcome.
    """

    execution_index: int
    kind: Stage4ScheduleStepKind
    expected_ef: int
    sweep_index: int | None
    control_query_id: int | None
    control_vector_sha256: str | None
    routing_sequence_index: int | None
    occurrence_id: str | None
    dataset_query_id: int | None
    vector_sha256: str | None
    threshold_radius: float | None
    range_filter: float | None
    limit: int | None
    route_kind: CanaryRouteKind | None


@dataclass(frozen=True, slots=True)
class Stage4ExecutionSchedule:
    """Canonical 1,200-slot Stage-4 execution order and immutable digest."""

    schema_version: str
    plan_sha256: str
    metric: Metric
    threshold_stratum: str
    candidate_ef: int
    last_known_good_ef: int
    control_ef: int
    steps: tuple[Stage4ScheduleStep, ...]
    schedule_sha256: str

    def __post_init__(self) -> None:
        _validate_schedule(self)


def build_stage4_execution_schedule(
    manifest: EligibleWorkloadManifest,
    plan: CanaryRoutePlan,
) -> Stage4ExecutionSchedule:
    """Build the only serial schedule permitted by the frozen Stage-4 contract.

    This is intentionally a preparation-only operation.  It does not claim an
    occurrence, read a query value, persist the result, contact a service, or
    validate an approval grant.
    """

    _validate_manifest_and_plan(manifest, plan)
    control_contract = manifest.schedule_stability
    steps: list[Stage4ScheduleStep] = []

    for sweep_index in range(SCHEDULE_PRE_SWEEP_COUNT):
        _append_control_sweep(
            steps,
            sweep_index=sweep_index,
            controls=control_contract.controls,
            control_ef=control_contract.control_ef,
        )
    for block_index in range(SCHEDULE_INTERLEAVED_SWEEP_COUNT):
        start = block_index * SCHEDULE_ROUTING_BLOCK_SIZE
        for occurrence in plan.occurrences[start : start + SCHEDULE_ROUTING_BLOCK_SIZE]:
            _append_routing_step(steps, plan=plan, occurrence=occurrence)
        _append_control_sweep(
            steps,
            sweep_index=SCHEDULE_PRE_SWEEP_COUNT + block_index,
            controls=control_contract.controls,
            control_ef=control_contract.control_ef,
        )
    for offset in range(SCHEDULE_POST_SWEEP_COUNT):
        _append_control_sweep(
            steps,
            sweep_index=(
                SCHEDULE_PRE_SWEEP_COUNT + SCHEDULE_INTERLEAVED_SWEEP_COUNT + offset
            ),
            controls=control_contract.controls,
            control_ef=control_contract.control_ef,
        )

    frozen_steps = tuple(steps)
    schedule_sha256 = _digest(
        _schedule_document(
            plan_sha256=plan.plan_sha256,
            metric=plan.metric,
            threshold_stratum=plan.threshold_stratum,
            candidate_ef=plan.candidate_ef,
            last_known_good_ef=plan.last_known_good_ef,
            control_ef=control_contract.control_ef,
            steps=frozen_steps,
        )
    )
    return Stage4ExecutionSchedule(
        schema_version=STAGE4_EXECUTION_SCHEDULE_SCHEMA_VERSION,
        plan_sha256=plan.plan_sha256,
        metric=plan.metric,
        threshold_stratum=plan.threshold_stratum,
        candidate_ef=plan.candidate_ef,
        last_known_good_ef=plan.last_known_good_ef,
        control_ef=control_contract.control_ef,
        steps=frozen_steps,
        schedule_sha256=schedule_sha256,
    )


def _append_control_sweep(
    steps: list[Stage4ScheduleStep],
    *,
    sweep_index: int,
    controls: tuple[ScheduleControl, ...],
    control_ef: int,
) -> None:
    for control in controls:
        steps.append(
            Stage4ScheduleStep(
                execution_index=len(steps),
                kind=Stage4ScheduleStepKind.CONTROL,
                expected_ef=control_ef,
                sweep_index=sweep_index,
                control_query_id=control.query_id,
                control_vector_sha256=control.vector_sha256,
                routing_sequence_index=None,
                occurrence_id=None,
                dataset_query_id=None,
                vector_sha256=None,
                threshold_radius=None,
                range_filter=None,
                limit=None,
                route_kind=None,
            )
        )


def _append_routing_step(
    steps: list[Stage4ScheduleStep],
    *,
    plan: CanaryRoutePlan,
    occurrence: RouteOccurrence,
) -> None:
    resolution = plan.resolve(occurrence.occurrence_id)
    if (
        not resolution.accepted
        or resolution.occurrence_id != occurrence.occurrence_id
        or resolution.dataset_query_id != occurrence.dataset_query_id
        or resolution.ef is None
        or resolution.kind is None
    ):
        raise ValueError("SCHEDULE_PLAN_RESOLUTION_INVALID")
    steps.append(
        Stage4ScheduleStep(
            execution_index=len(steps),
            kind=Stage4ScheduleStepKind.ROUTING,
            expected_ef=resolution.ef,
            sweep_index=None,
            control_query_id=None,
            control_vector_sha256=None,
            routing_sequence_index=occurrence.sequence_index,
            occurrence_id=occurrence.occurrence_id,
            dataset_query_id=occurrence.dataset_query_id,
            vector_sha256=occurrence.vector_sha256,
            threshold_radius=occurrence.threshold_radius,
            range_filter=occurrence.range_filter,
            limit=occurrence.limit,
            route_kind=resolution.kind,
        )
    )


def _validate_manifest_and_plan(
    manifest: object,
    plan: object,
) -> None:
    if not isinstance(manifest, EligibleWorkloadManifest) or not isinstance(plan, CanaryRoutePlan):
        raise ValueError("SCHEDULE_INPUT_INVALID")
    try:
        manifest.validate()
    except (ContractViolation, TypeError, ValueError) as exc:
        raise ValueError("SCHEDULE_MANIFEST_INVALID") from exc
    identity = manifest.identity
    same_header = (
        plan.metric is manifest.metric
        and plan.threshold_stratum == manifest.threshold_stratum
        and plan.candidate_ef == manifest.candidate_ef
        and plan.last_known_good_ef == manifest.last_known_good_ef
        and plan.configuration_identity == identity.configuration_identity
        and plan.data_identity == identity.data_identity
        and plan.flat_binding_id == identity.flat_binding_id
        and plan.hnsw_binding_id == identity.hnsw_binding_id
        and plan.population_count == EXP009_ROUTING_POPULATION_COUNT
        and plan.candidate_count == EXP009_CANDIDATE_COUNT
        and manifest.schedule_stability.control_ef == plan.last_known_good_ef
    )
    same_occurrences = (
        len(plan.occurrences) == len(manifest.occurrences)
        and all(
            _route_occurrence_matches_manifest(route, source)
            for route, source in zip(plan.occurrences, manifest.occurrences, strict=True)
        )
    )
    if not same_header or not same_occurrences:
        raise ValueError("SCHEDULE_PLAN_MANIFEST_MISMATCH")


def _route_occurrence_matches_manifest(route: object, source: object) -> bool:
    return all(
        getattr(route, field, object()) == getattr(source, field, object())
        for field in (
            "sequence_index",
            "occurrence_id",
            "dataset_query_id",
            "vector_sha256",
            "threshold_radius",
            "range_filter",
            "limit",
        )
    )


def _validate_schedule(schedule: Stage4ExecutionSchedule) -> None:
    if (
        schedule.schema_version != STAGE4_EXECUTION_SCHEDULE_SCHEMA_VERSION
        or not _sha256(schedule.plan_sha256)
        or not isinstance(schedule.metric, Metric)
        or schedule.threshold_stratum not in THRESHOLD_LABELS
        or not _valid_transition(schedule.candidate_ef, schedule.last_known_good_ef)
        or schedule.control_ef != schedule.last_known_good_ef
        or not isinstance(schedule.steps, tuple)
        or len(schedule.steps) != _STEP_COUNT
        or not _sha256(schedule.schedule_sha256)
    ):
        raise _schedule_invalid()
    controls_by_sweep: dict[int, list[Stage4ScheduleStep]] = {
        index: [] for index in range(_CONTROL_SWEEP_COUNT)
    }
    routing_steps: list[Stage4ScheduleStep] = []
    for expected_index, step in enumerate(schedule.steps):
        if not isinstance(step, Stage4ScheduleStep) or step.execution_index != expected_index:
            raise _schedule_invalid()
        if step.kind is Stage4ScheduleStepKind.CONTROL:
            _validate_control_step(step, schedule, controls_by_sweep)
        elif step.kind is Stage4ScheduleStepKind.ROUTING:
            _validate_routing_step(step, schedule, routing_steps)
        else:
            raise _schedule_invalid()
    if len(routing_steps) != EXP009_ROUTING_POPULATION_COUNT:
        raise _schedule_invalid()
    if [step.routing_sequence_index for step in routing_steps] != list(
        range(EXP009_ROUTING_POPULATION_COUNT)
    ):
        raise _schedule_invalid()
    candidate_count = sum(
        step.route_kind is CanaryRouteKind.CANDIDATE for step in routing_steps
    )
    if candidate_count != EXP009_CANDIDATE_COUNT:
        raise _schedule_invalid()
    for sweep_index, sweep in controls_by_sweep.items():
        if len(sweep) != SCHEDULE_CONTROL_COUNT:
            raise _schedule_invalid()
        if [step.control_query_id for step in sweep] != list(_control_query_ids()):
            raise _schedule_invalid()
        if len({step.control_vector_sha256 for step in sweep}) != SCHEDULE_CONTROL_COUNT:
            raise _schedule_invalid()
        if sweep_index < SCHEDULE_PRE_SWEEP_COUNT:
            expected_before = sweep_index * SCHEDULE_CONTROL_COUNT
            if [step.execution_index for step in sweep] != list(
                range(expected_before, expected_before + SCHEDULE_CONTROL_COUNT)
            ):
                raise _schedule_invalid()
    _validate_cadence(schedule.steps, routing_steps)
    if _digest(_schedule_document_from_value(schedule)) != schedule.schedule_sha256:
        raise _schedule_invalid()


def _validate_control_step(
    step: Stage4ScheduleStep,
    schedule: Stage4ExecutionSchedule,
    controls_by_sweep: dict[int, list[Stage4ScheduleStep]],
) -> None:
    if (
        type(step.sweep_index) is not int
        or step.sweep_index not in controls_by_sweep
        or type(step.control_query_id) is not int
        or step.control_query_id not in _control_query_ids()
        or not _sha256(step.control_vector_sha256)
        or step.expected_ef != schedule.control_ef
        or any(
            value is not None
            for value in (
                step.routing_sequence_index,
                step.occurrence_id,
                step.dataset_query_id,
                step.vector_sha256,
                step.threshold_radius,
                step.range_filter,
                step.limit,
                step.route_kind,
            )
        )
    ):
        raise _schedule_invalid()
    controls_by_sweep[step.sweep_index].append(step)


def _validate_routing_step(
    step: Stage4ScheduleStep,
    schedule: Stage4ExecutionSchedule,
    routing_steps: list[Stage4ScheduleStep],
) -> None:
    sequence_index = step.routing_sequence_index
    expected_occurrence_id = (
        None
        if type(sequence_index) is not int
        else f"exp009-routing-{sequence_index:06d}"
    )
    route_valid = (
        type(sequence_index) is int
        and sequence_index in range(EXP009_ROUTING_POPULATION_COUNT)
        and step.occurrence_id == expected_occurrence_id
        and isinstance(step.occurrence_id, str)
        and _OCCURRENCE_RE.fullmatch(step.occurrence_id) is not None
        and step.dataset_query_id == sequence_index
        and _sha256(step.vector_sha256)
        and _finite_float(step.threshold_radius)
        and _finite_float(step.range_filter)
        and step.limit == RESULT_LIMIT
        and step.sweep_index is None
        and step.control_query_id is None
        and step.control_vector_sha256 is None
    )
    if not route_valid:
        raise _schedule_invalid()
    search_valid = (
        step.threshold_radius is not None
        and step.range_filter is not None
        and (
            (schedule.metric is Metric.L2 and step.threshold_radius > 0.0 and step.range_filter == 0.0)
            or (
                schedule.metric is Metric.COSINE
                and -1.0 <= step.threshold_radius < 1.0
                and step.range_filter == 1.0
            )
        )
    )
    expected_ef = (
        schedule.candidate_ef
        if step.route_kind is CanaryRouteKind.CANDIDATE
        else schedule.last_known_good_ef
    )
    if (
        not search_valid
        or step.route_kind not in (CanaryRouteKind.CANDIDATE, CanaryRouteKind.LAST_KNOWN_GOOD)
        or step.expected_ef != expected_ef
    ):
        raise _schedule_invalid()
    routing_steps.append(step)


def _validate_cadence(
    steps: tuple[Stage4ScheduleStep, ...],
    routing_steps: list[Stage4ScheduleStep],
) -> None:
    expected_routing_positions: list[int] = []
    index = SCHEDULE_PRE_SWEEP_COUNT * SCHEDULE_CONTROL_COUNT
    for _ in range(SCHEDULE_INTERLEAVED_SWEEP_COUNT):
        expected_routing_positions.extend(range(index, index + SCHEDULE_ROUTING_BLOCK_SIZE))
        index += SCHEDULE_ROUTING_BLOCK_SIZE + SCHEDULE_CONTROL_COUNT
    if [step.execution_index for step in routing_steps] != expected_routing_positions:
        raise _schedule_invalid()
    expected_control_sweeps = list(range(_CONTROL_SWEEP_COUNT))
    actual_control_sweeps = [
        step.sweep_index
        for step in steps
        if step.kind is Stage4ScheduleStepKind.CONTROL
        and step.control_query_id == 600
    ]
    if actual_control_sweeps != expected_control_sweeps:
        raise _schedule_invalid()


def _schedule_document(
    *,
    plan_sha256: str,
    metric: Metric,
    threshold_stratum: str,
    candidate_ef: int,
    last_known_good_ef: int,
    control_ef: int,
    steps: tuple[Stage4ScheduleStep, ...],
) -> dict[str, object]:
    return {
        "schema_version": STAGE4_EXECUTION_SCHEDULE_SCHEMA_VERSION,
        "plan_sha256": plan_sha256,
        "metric": metric.value,
        "threshold_stratum": threshold_stratum,
        "candidate_ef": candidate_ef,
        "last_known_good_ef": last_known_good_ef,
        "control_ef": control_ef,
        "steps": [_step_document(step) for step in steps],
    }


def _schedule_document_from_value(schedule: Stage4ExecutionSchedule) -> dict[str, object]:
    return _schedule_document(
        plan_sha256=schedule.plan_sha256,
        metric=schedule.metric,
        threshold_stratum=schedule.threshold_stratum,
        candidate_ef=schedule.candidate_ef,
        last_known_good_ef=schedule.last_known_good_ef,
        control_ef=schedule.control_ef,
        steps=schedule.steps,
    )


def _step_document(step: Stage4ScheduleStep) -> dict[str, object]:
    return {
        "execution_index": step.execution_index,
        "kind": step.kind.value,
        "expected_ef": step.expected_ef,
        "sweep_index": step.sweep_index,
        "control_query_id": step.control_query_id,
        "control_vector_sha256": step.control_vector_sha256,
        "routing_sequence_index": step.routing_sequence_index,
        "occurrence_id": step.occurrence_id,
        "dataset_query_id": step.dataset_query_id,
        "vector_sha256": step.vector_sha256,
        "threshold_radius": step.threshold_radius,
        "range_filter": step.range_filter,
        "limit": step.limit,
        "route_kind": None if step.route_kind is None else step.route_kind.value,
    }


def _digest(document: object) -> str:
    return hashlib.sha256(canonical_json_bytes(document)).hexdigest()


def _valid_transition(candidate: object, last_known_good: object) -> bool:
    if type(candidate) is not int or type(last_known_good) is not int:
        return False
    if candidate not in _ACTUATION_LADDER or last_known_good not in _ACTUATION_LADDER:
        return False
    return _ACTUATION_LADDER.index(candidate) == _ACTUATION_LADDER.index(last_known_good) + 1


def _sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _finite_float(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def _control_query_ids() -> range:
    """Return the frozen first 50 DATASET-002 recall-audit identifiers."""

    return range(
        EXP009_ROUTING_POPULATION_COUNT,
        EXP009_ROUTING_POPULATION_COUNT + SCHEDULE_CONTROL_COUNT,
    )


def _schedule_invalid() -> ValueError:
    return ValueError("STAGE4_SCHEDULE_INVALID")
