"""Offline-only serial composition boundary for EXP-009 Stage 4.

Purpose:
    Compose an already-admitted immutable schedule, verified DATASET-002 vector
    lookup, injected slot executor, durable ledger, and pure evaluator using
    fakes. This is a test/preflight seam, not a live runner or grant boundary.
Inputs:
    An immutable admission receipt, schedule, source adapter, executor protocol,
    ledger, and injected monotonic/UTC clocks.
Outputs:
    Immutable ledger/evaluation evidence and explicit non-sensitive refusal
    reasons; raw vectors and executor payloads are never retained.
Dependencies:
    Value-only admission/schedule/workload contracts plus the ledger/evaluator.
    It intentionally imports no Milvus, serving, activation, approval, grant,
    or route-authority module.
Complexity:
    O(number of newly executed slots) time and O(1) runner memory, aside from
    the ledger's existing durable history.
Failure modes:
    Refused/mismatched admission causes zero dispatch. Source/executor failures
    become one terminal ledger record where a valid clock/timestamp is available;
    no retry, reordering, route selection, or alternate ef is possible.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
import re
from typing import Callable, Protocol

from .canary_admission import Stage4AdmissionReceipt
from .canary_execution_ledger import (
    Stage4ExecutionLedger,
    Stage4LedgerError,
    Stage4LedgerProgress,
    Stage4LedgerStartResult,
    Stage4LedgerStatus,
    Stage4SlotObservation,
)
from .canary_schedule import Stage4ExecutionSchedule, Stage4ScheduleStep, Stage4ScheduleStepKind
from .canary_schedule_evaluation import (
    Stage4ScheduleEvaluation,
    evaluate_stage4_execution_ledger,
)
from .canary_workload import ScheduleControl


__all__ = [
    "Dataset002ScheduleVectorSource",
    "Stage4SerialRunResult",
    "Stage4SerialRunner",
    "Stage4SlotExecutorLike",
    "Stage4SlotExecutorOutcome",
]


_REASON_RE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


class _Dataset002VectorSourceLike(Protocol):
    """The minimal existing DATASET-002 source shape required by this adapter."""

    def control_vector(self, *, control: ScheduleControl) -> tuple[float, ...]: ...

    def routing_vector(
        self,
        *,
        occurrence_id: str,
        dataset_query_id: int,
        vector_sha256: str,
    ) -> tuple[float, ...]: ...


class Stage4SlotExecutorLike(Protocol):
    """Offline executor seam; callers must supply a fake for this boundary."""

    def execute(
        self, *, step: Stage4ScheduleStep, query_vector: tuple[float, ...]
    ) -> "Stage4SlotExecutorOutcome": ...


@dataclass(frozen=True, slots=True)
class Stage4SlotExecutorOutcome:
    """Non-sensitive facts returned by one injected slot executor attempt."""

    success: bool
    timed_out: bool
    threshold_semantics_valid: bool
    health_before_ok: bool
    health_after_ok: bool
    identity_before_ok: bool
    identity_after_ok: bool
    result_count: int
    reason_code: str | None


@dataclass(frozen=True, slots=True)
class Stage4SerialRunResult:
    """Inspectable outcome of a bounded offline serial-runner invocation."""

    dispatched_slot_count: int
    ledger_progress: Stage4LedgerProgress
    evaluation: Stage4ScheduleEvaluation
    reason_codes: tuple[str, ...]


class Dataset002ScheduleVectorSource:
    """Adapt frozen schedule bindings to the existing verified DATASET-002 API."""

    def __init__(self, source: _Dataset002VectorSourceLike) -> None:
        if (
            not callable(getattr(source, "control_vector", None))
            or not callable(getattr(source, "routing_vector", None))
        ):
            raise TypeError("source must satisfy the DATASET-002 vector source contract")
        self._source = source

    def vector_for_step(self, step: object) -> tuple[float, ...]:
        """Resolve exactly one vector using only a validated schedule binding."""

        if not isinstance(step, Stage4ScheduleStep):
            raise ValueError("SCHEDULE_STEP_INVALID")
        if step.kind is Stage4ScheduleStepKind.CONTROL:
            if step.control_query_id is None or step.control_vector_sha256 is None:
                raise ValueError("SCHEDULE_CONTROL_BINDING_INVALID")
            vector = self._source.control_vector(
                control=ScheduleControl(step.control_query_id, step.control_vector_sha256)
            )
        elif step.kind is Stage4ScheduleStepKind.ROUTING:
            if (
                step.occurrence_id is None
                or step.dataset_query_id is None
                or step.vector_sha256 is None
            ):
                raise ValueError("SCHEDULE_ROUTING_BINDING_INVALID")
            vector = self._source.routing_vector(
                occurrence_id=step.occurrence_id,
                dataset_query_id=step.dataset_query_id,
                vector_sha256=step.vector_sha256,
            )
        else:
            raise ValueError("SCHEDULE_STEP_KIND_INVALID")
        return _validated_vector(vector)


class Stage4SerialRunner:
    """Run only injected offline slots in durable schedule order.

    A passing admission receipt gates this composition test but is not a grant.
    This class never accepts approval material, claims a route, or creates a
    live client. A later live composition root must be separately designed and
    human-authorized; it must not treat this runner as authorization.
    """

    def __init__(
        self,
        *,
        admission_receipt: Stage4AdmissionReceipt,
        schedule: Stage4ExecutionSchedule,
        vector_source: Dataset002ScheduleVectorSource,
        executor: Stage4SlotExecutorLike,
        ledger: Stage4ExecutionLedger,
        monotonic_ns: Callable[[], int],
        utc_now: Callable[[], str],
    ) -> None:
        if type(admission_receipt) is not Stage4AdmissionReceipt:
            raise TypeError("admission_receipt must be a Stage4AdmissionReceipt")
        if not isinstance(schedule, Stage4ExecutionSchedule):
            raise TypeError("schedule must be a Stage4ExecutionSchedule")
        if not isinstance(vector_source, Dataset002ScheduleVectorSource):
            raise TypeError("vector_source must be a Dataset002ScheduleVectorSource")
        if not isinstance(ledger, Stage4ExecutionLedger):
            raise TypeError("ledger must be a Stage4ExecutionLedger")
        if not callable(getattr(executor, "execute", None)):
            raise TypeError("executor must satisfy Stage4SlotExecutorLike")
        if not callable(monotonic_ns) or not callable(utc_now):
            raise TypeError("clocks must be callable")
        self._admission_receipt = admission_receipt
        self._schedule = schedule
        self._vector_source = vector_source
        self._executor = executor
        self._ledger = ledger
        self._monotonic_ns = monotonic_ns
        self._utc_now = utc_now

    def run(self, *, max_slots: int | None = None) -> Stage4SerialRunResult:
        """Run a bounded suffix once, returning fail-closed durable evidence."""

        limit = len(self._schedule.steps) if max_slots is None else _slot_limit(max_slots)
        admission_reason = _admission_refusal(
            self._admission_receipt, self._schedule
        )
        if admission_reason is not None:
            return self._result(0, (admission_reason,))
        if self._ledger.schedule_sha256 != self._schedule.schedule_sha256:
            return self._result(0, ("LEDGER_SCHEDULE_MISMATCH",))
        progress = self._ledger.progress()
        if progress.status is Stage4LedgerStatus.FAILED:
            return self._result(0, ("LEDGER_NOT_ACTIVE",))
        if progress.status is Stage4LedgerStatus.COMPLETE:
            return self._result(0, ())
        if progress.status is Stage4LedgerStatus.AMBIGUOUS:
            # A durable STARTED marker with no matching terminal record --
            # governed recovery is required; this boundary never resolves it
            # automatically.
            return self._result(0, ("LEDGER_AMBIGUOUS",))

        dispatched = 0
        end_index = min(len(self._schedule.steps), progress.record_count + limit)
        for execution_index in range(progress.record_count, end_index):
            step = self._schedule.steps[execution_index]
            observation, was_dispatched, started_record_sha256 = self._observe(step)
            if observation is None or started_record_sha256 is None:
                return self._result(dispatched, ("LEDGER_START_REFUSED",))
            dispatched += int(was_dispatched)
            append = self._ledger.complete_slot(
                observation, started_record_sha256=started_record_sha256
            )
            if not append.accepted:
                return self._result(dispatched, (append.reason_code or "LEDGER_APPEND_REFUSED",))
            if not _observation_is_safe(observation):
                return self._result(dispatched, (observation.reason_code or "SLOT_UNSAFE",))
        return self._result(dispatched, ())

    def _observe(
        self, step: Stage4ScheduleStep
    ) -> tuple[Stage4SlotObservation | None, bool, str | None]:
        start = _monotonic(self._monotonic_ns, "CLOCK_START_INVALID")
        start_recorded_at = _timestamp(self._utc_now())
        try:
            start_result = self._ledger.start_slot(
                step.execution_index,
                started_monotonic_ns=start,
                recorded_at_utc=start_recorded_at,
            )
        except Stage4LedgerError:
            return None, False, None
        if (
            not isinstance(start_result, Stage4LedgerStartResult)
            or not start_result.accepted
            or not isinstance(start_result.start_sha256, str)
        ):
            # No durable STARTED marker -- the injected executor must not be
            # invoked, matching the STARTED-before-dispatch contract shared
            # with Stage4LiveRunner.
            return None, False, None
        started_record_sha256 = start_result.start_sha256

        try:
            query_vector = self._vector_source.vector_for_step(step)
        except Exception:
            finish = _monotonic(self._monotonic_ns, "CLOCK_FINISH_INVALID")
            return (
                self._failure_observation(step, start, finish, "QUERY_SOURCE_FAILURE"),
                False,
                started_record_sha256,
            )
        try:
            outcome = self._executor.execute(step=step, query_vector=query_vector)
        except Exception:
            finish = _monotonic(self._monotonic_ns, "CLOCK_FINISH_INVALID")
            return (
                self._failure_observation(step, start, finish, "EXECUTOR_EXCEPTION"),
                True,
                started_record_sha256,
            )
        finish = _monotonic(self._monotonic_ns, "CLOCK_FINISH_INVALID")
        return (
            self._observation_from_outcome(step, start, finish, outcome),
            True,
            started_record_sha256,
        )

    def _observation_from_outcome(
        self,
        step: Stage4ScheduleStep,
        start: int,
        finish: int,
        outcome: object,
    ) -> Stage4SlotObservation:
        if not isinstance(outcome, Stage4SlotExecutorOutcome):
            return self._failure_observation(step, start, finish, "EXECUTOR_OUTCOME_INVALID")
        booleans = (
            outcome.success,
            outcome.timed_out,
            outcome.threshold_semantics_valid,
            outcome.health_before_ok,
            outcome.health_after_ok,
            outcome.identity_before_ok,
            outcome.identity_after_ok,
        )
        safe = _outcome_is_safe(outcome)
        if (
            not all(isinstance(value, bool) for value in booleans)
            or outcome.success is True and outcome.timed_out is True
            or isinstance(outcome.result_count, bool)
            or not isinstance(outcome.result_count, int)
            or outcome.result_count < 0
            or safe and outcome.reason_code is not None
            or not safe and _reason_code(outcome.reason_code) is None
        ):
            return self._failure_observation(step, start, finish, "EXECUTOR_OUTCOME_INVALID")
        return Stage4SlotObservation(
            execution_index=step.execution_index,
            observed_ef=step.expected_ef,
            started_monotonic_ns=start,
            finished_monotonic_ns=finish,
            recorded_at_utc=_timestamp(self._utc_now()),
            success=outcome.success,
            timed_out=outcome.timed_out,
            threshold_semantics_valid=outcome.threshold_semantics_valid,
            health_before_ok=outcome.health_before_ok,
            health_after_ok=outcome.health_after_ok,
            identity_before_ok=outcome.identity_before_ok,
            identity_after_ok=outcome.identity_after_ok,
            result_count=outcome.result_count,
            latency_ms=(finish - start) / 1_000_000.0,
            reason_code=outcome.reason_code,
        )

    def _failure_observation(
        self,
        step: Stage4ScheduleStep,
        start: int,
        finish: int,
        reason: str,
    ) -> Stage4SlotObservation:
        return Stage4SlotObservation(
            execution_index=step.execution_index,
            observed_ef=step.expected_ef,
            started_monotonic_ns=start,
            finished_monotonic_ns=finish,
            recorded_at_utc=_timestamp(self._utc_now()),
            success=False,
            timed_out=False,
            threshold_semantics_valid=False,
            health_before_ok=True,
            health_after_ok=True,
            identity_before_ok=True,
            identity_after_ok=True,
            result_count=0,
            latency_ms=(finish - start) / 1_000_000.0,
            reason_code=reason,
        )

    def _result(self, dispatched: int, reasons: tuple[str, ...]) -> Stage4SerialRunResult:
        try:
            progress = self._ledger.progress()
            evaluation = evaluate_stage4_execution_ledger(
                schedule=self._schedule, ledger=self._ledger
            )
        except Stage4LedgerError as exc:
            raise RuntimeError("LEDGER_EVIDENCE_UNAVAILABLE") from exc
        return Stage4SerialRunResult(
            dispatched_slot_count=dispatched,
            ledger_progress=progress,
            evaluation=evaluation,
            reason_codes=reasons,
        )


def _admission_refusal(
    receipt: Stage4AdmissionReceipt, schedule: Stage4ExecutionSchedule
) -> str | None:
    if type(receipt) is not Stage4AdmissionReceipt or not receipt.matches_canonical_digest():
        return "ADMISSION_RECEIPT_INVALID"
    if receipt.route_plan_sha256 != schedule.plan_sha256:
        return "ADMISSION_SCHEDULE_BINDING_MISMATCH"
    if receipt.execution_schedule_sha256 != schedule.schedule_sha256:
        return "ADMISSION_EXECUTION_SCHEDULE_MISMATCH"
    return None


def _slot_limit(value: int | None) -> int:
    if value is None:
        return 1_200
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("max_slots must be a positive integer or None")
    return value


def _validated_vector(value: object) -> tuple[float, ...]:
    if not isinstance(value, tuple) or not value:
        raise ValueError("SOURCE_VECTOR_INVALID")
    if not all(
        isinstance(item, (int, float))
        and not isinstance(item, bool)
        and isfinite(float(item))
        for item in value
    ):
        raise ValueError("SOURCE_VECTOR_INVALID")
    return tuple(float(item) for item in value)


def _monotonic(clock: Callable[[], int], reason: str) -> int:
    try:
        value = clock()
    except Exception as exc:
        raise RuntimeError(reason) from exc
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(reason)
    return value


def _timestamp(value: object) -> str:
    if not isinstance(value, str):
        raise RuntimeError("UTC_CLOCK_INVALID")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00") if value.endswith("Z") else None
    except ValueError as exc:
        raise RuntimeError("UTC_CLOCK_INVALID") from exc
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise RuntimeError("UTC_CLOCK_INVALID")
    return value


def _reason_code(value: object) -> str | None:
    return value if isinstance(value, str) and _REASON_RE.fullmatch(value) is not None else None


def _observation_is_safe(value: Stage4SlotObservation) -> bool:
    return (
        value.success
        and not value.timed_out
        and value.threshold_semantics_valid
        and value.health_before_ok
        and value.health_after_ok
        and value.identity_before_ok
        and value.identity_after_ok
    )


def _outcome_is_safe(value: Stage4SlotExecutorOutcome) -> bool:
    return (
        value.success is True
        and value.timed_out is False
        and value.threshold_semantics_valid is True
        and value.health_before_ok is True
        and value.health_after_ok is True
        and value.identity_before_ok is True
        and value.identity_after_ok is True
    )
