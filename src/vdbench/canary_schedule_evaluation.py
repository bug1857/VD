"""Pure post-run evaluation of EXP-009's frozen Stage-4 schedule.

Purpose:
    Evaluate the pre-registered control-sweep stability diagnostic and the
    conditional finite-manifest latency result from durable schedule evidence.
Inputs:
    A rebuilt immutable schedule and records already verified by the Stage-4
    execution ledger; a narrow wrapper can read both from that ledger.
Outputs:
    Explicit applicability, reason codes, per-sweep medians/p95s, baseline,
    candidate-route maximum, and finite-population coverage metadata.
Dependencies:
    Pure schedule/ledger value objects and frozen statistics only.  It has no
    Milvus, serving, approval, route-authority, policy, or activation import.
Complexity:
    O(1,200) time and O(12) additional memory for the frozen reference run.
Failure modes:
    Incomplete, unsafe, mismatched, or ceiling-breaching evidence yields an
    explicit NOT-APPLICABLE result; no benign value is fabricated.
Scope:
    This module does not estimate recall.  The disjoint 1,200-query recall
    audit remains a separately pre-registered Stage-4 evidence stream.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil, isfinite

from .canary_execution_ledger import (
    Stage4ExecutionLedger,
    Stage4ExecutionRecord,
    Stage4LedgerError,
    Stage4LedgerProgress,
    Stage4LedgerStatus,
)
from .canary_routing import CanaryRouteKind
from .canary_schedule import Stage4ExecutionSchedule, Stage4ScheduleStepKind
from .canary_statistics import exp009_latency_bound_contract
from .canary_workload import (
    SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING,
    SCHEDULE_CONTROL_COUNT,
    SCHEDULE_MEDIAN_RELATIVE_CEILING,
    SCHEDULE_P95_RELATIVE_CEILING,
    SCHEDULE_PRE_SWEEP_COUNT,
)


__all__ = [
    "Stage4ControlSweepResult",
    "Stage4ScheduleEvaluation",
    "evaluate_stage4_execution_ledger",
    "evaluate_stage4_schedule_evidence",
]


@dataclass(frozen=True, slots=True)
class Stage4ControlSweepResult:
    """One 50-query control sweep's measured latency and validity outcome."""

    sweep_index: int
    median_latency_ms: float | None
    p95_latency_ms: float | None
    complete: bool
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Stage4ScheduleEvaluation:
    """A non-IID, finite-manifest schedule-stability result.

    ``finite_manifest_latency_applicable`` is never a production-latency or
    recall claim.  It is true only when the frozen schedule controls and exact
    60-of-600 route evidence are complete and satisfy their declared checks.
    """

    finite_manifest_latency_applicable: bool
    reason_codes: tuple[str, ...]
    control_sweeps: tuple[Stage4ControlSweepResult, ...]
    baseline_median_ms: float | None
    baseline_p95_ms: float | None
    candidate_latency_count: int
    candidate_latency_max_ms: float | None
    finite_population_coverage_probability: float
    recall_bound_evaluated: bool


def evaluate_stage4_execution_ledger(
    *,
    schedule: Stage4ExecutionSchedule,
    ledger: object,
) -> Stage4ScheduleEvaluation:
    """Read verified durable evidence, then evaluate it without side effects."""

    if not isinstance(ledger, Stage4ExecutionLedger):
        return _not_applicable("LEDGER_INPUT_INVALID")
    try:
        return evaluate_stage4_schedule_evidence(
            schedule=schedule,
            progress=ledger.progress(),
            records=ledger.records(),
        )
    except Stage4LedgerError:
        return _not_applicable("LEDGER_EVIDENCE_UNAVAILABLE")


def evaluate_stage4_schedule_evidence(
    *,
    schedule: object,
    progress: object,
    records: object,
) -> Stage4ScheduleEvaluation:
    """Evaluate pre-verified ledger records against the rebuilt schedule.

    The pure function deliberately does not verify SQLite persistence or the
    record hash chain; callers requiring durable evidence use
    ``evaluate_stage4_execution_ledger``.  This separation makes arithmetic
    and fail-closed schedule rules independently testable.
    """

    coverage = exp009_latency_bound_contract().coverage_probability
    if not isinstance(schedule, Stage4ExecutionSchedule):
        return _not_applicable("SCHEDULE_INPUT_INVALID", coverage=coverage)
    if not isinstance(progress, Stage4LedgerProgress) or not isinstance(records, tuple):
        return _not_applicable("LEDGER_EVIDENCE_INVALID", coverage=coverage)
    if progress.status is not Stage4LedgerStatus.COMPLETE:
        return _not_applicable("LEDGER_NOT_COMPLETE", coverage=coverage)
    if progress.record_count != len(schedule.steps) or len(records) != len(schedule.steps):
        return _not_applicable("LEDGER_RECORD_COUNT_INVALID", coverage=coverage)

    reasons: list[str] = []
    control_latencies: dict[int, list[float]] = {}
    candidate_latencies: list[float] = []
    for expected_step, record in zip(schedule.steps, records, strict=True):
        observation = getattr(record, "observation", None)
        if (
            not isinstance(record, Stage4ExecutionRecord)
            or observation is None
            or observation.execution_index != expected_step.execution_index
            or observation.observed_ef != expected_step.expected_ef
        ):
            _append_once(reasons, "RECORD_SCHEDULE_MISMATCH")
            continue
        if not _observation_safe(observation):
            _append_once(reasons, "LEDGER_RECORD_UNSAFE")
            continue
        latency = observation.latency_ms
        if not isinstance(latency, float) or not isfinite(latency) or latency < 0.0:
            _append_once(reasons, "LEDGER_LATENCY_INVALID")
            continue
        if expected_step.kind is Stage4ScheduleStepKind.CONTROL:
            if expected_step.sweep_index is None:
                _append_once(reasons, "RECORD_SCHEDULE_MISMATCH")
                continue
            control_latencies.setdefault(expected_step.sweep_index, []).append(latency)
        elif expected_step.kind is Stage4ScheduleStepKind.ROUTING:
            if expected_step.route_kind is CanaryRouteKind.CANDIDATE:
                candidate_latencies.append(latency)
        else:
            _append_once(reasons, "RECORD_SCHEDULE_MISMATCH")

    sweep_results = _sweep_results(control_latencies)
    if any(not item.complete for item in sweep_results):
        _append_once(reasons, "SCHEDULE_CONTROL_INCOMPLETE")
    baseline_median, baseline_p95 = _baselines(sweep_results, reasons)
    if len(candidate_latencies) != 60:
        _append_once(reasons, "CANDIDATE_ROUTE_PARTITION_INVALID")
    candidate_max = max(candidate_latencies) if len(candidate_latencies) == 60 else None
    if candidate_max is not None and (not isfinite(candidate_max) or candidate_max < 0.0):
        _append_once(reasons, "CANDIDATE_LATENCY_INVALID")

    if baseline_median is not None and baseline_p95 is not None:
        for sweep in sweep_results:
            if not sweep.complete or sweep.median_latency_ms is None or sweep.p95_latency_ms is None:
                continue
            if sweep.p95_latency_ms > SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING:
                _append_once(reasons, "CONTROL_ABSOLUTE_P95_CEILING_BREACH")
            if sweep.p95_latency_ms > SCHEDULE_P95_RELATIVE_CEILING * baseline_p95:
                _append_once(reasons, "CONTROL_RELATIVE_P95_CEILING_BREACH")
            if sweep.median_latency_ms > SCHEDULE_MEDIAN_RELATIVE_CEILING * baseline_median:
                _append_once(reasons, "CONTROL_RELATIVE_MEDIAN_CEILING_BREACH")

    return Stage4ScheduleEvaluation(
        finite_manifest_latency_applicable=not reasons,
        reason_codes=tuple(reasons),
        control_sweeps=sweep_results,
        baseline_median_ms=baseline_median,
        baseline_p95_ms=baseline_p95,
        candidate_latency_count=len(candidate_latencies),
        candidate_latency_max_ms=candidate_max,
        finite_population_coverage_probability=coverage,
        recall_bound_evaluated=False,
    )


def _sweep_results(
    control_latencies: dict[int, list[float]],
) -> tuple[Stage4ControlSweepResult, ...]:
    results: list[Stage4ControlSweepResult] = []
    for sweep_index in range(12):
        values = control_latencies.get(sweep_index, [])
        if len(values) != SCHEDULE_CONTROL_COUNT or not all(
            isfinite(value) and value >= 0.0 for value in values
        ):
            results.append(
                Stage4ControlSweepResult(
                    sweep_index, None, None, False, ("CONTROL_SWEEP_INVALID",)
                )
            )
            continue
        results.append(
            Stage4ControlSweepResult(
                sweep_index,
                _median(values),
                _nearest_rank(values, 0.95),
                True,
                (),
            )
        )
    return tuple(results)


def _baselines(
    sweeps: tuple[Stage4ControlSweepResult, ...], reasons: list[str]
) -> tuple[float | None, float | None]:
    pre = sweeps[:SCHEDULE_PRE_SWEEP_COUNT]
    if len(pre) != SCHEDULE_PRE_SWEEP_COUNT or any(
        not item.complete
        or item.median_latency_ms is None
        or item.p95_latency_ms is None
        for item in pre
    ):
        _append_once(reasons, "CONTROL_BASELINE_INVALID")
        return None, None
    median = _median([item.median_latency_ms for item in pre if item.median_latency_ms is not None])
    p95 = _nearest_rank([item.p95_latency_ms for item in pre if item.p95_latency_ms is not None], 0.95)
    if not isfinite(median) or not isfinite(p95) or median <= 0.0 or p95 <= 0.0:
        _append_once(reasons, "CONTROL_BASELINE_INVALID")
        return None, None
    return median, p95


def _not_applicable(
    reason: str, *, coverage: float | None = None
) -> Stage4ScheduleEvaluation:
    probability = (
        exp009_latency_bound_contract().coverage_probability if coverage is None else coverage
    )
    return Stage4ScheduleEvaluation(
        finite_manifest_latency_applicable=False,
        reason_codes=(reason,),
        control_sweeps=(),
        baseline_median_ms=None,
        baseline_p95_ms=None,
        candidate_latency_count=0,
        candidate_latency_max_ms=None,
        finite_population_coverage_probability=probability,
        recall_bound_evaluated=False,
    )


def _observation_safe(observation: object) -> bool:
    return (
        getattr(observation, "success", None) is True
        and getattr(observation, "timed_out", None) is False
        and getattr(observation, "threshold_semantics_valid", None) is True
        and getattr(observation, "health_before_ok", None) is True
        and getattr(observation, "health_after_ok", None) is True
        and getattr(observation, "identity_before_ok", None) is True
        and getattr(observation, "identity_after_ok", None) is True
    )


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    count = len(ordered)
    if not count:
        raise ValueError("median requires at least one value")
    midpoint = count // 2
    if count % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def _nearest_rank(values: list[float], percentile: float) -> float:
    if not values:
        raise ValueError("nearest rank requires at least one value")
    return sorted(values)[ceil(percentile * len(values)) - 1]


def _append_once(reasons: list[str], code: str) -> None:
    if code not in reasons:
        reasons.append(code)
