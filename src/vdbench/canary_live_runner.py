"""Human-gated Stage-4 serial composition root for EXP-009.

Purpose:
    Coordinate a separately verified approval activation, immutable serial
    schedule, one-shot route authority, read-only serving port, durable ledger,
    and rollback port.  It intentionally owns no database client, private key,
    configuration mutation API, route-selection algorithm, or retry loop.
Inputs:
    An immutable ``Stage4LiveRunRequest`` and dependency-injected ports.  A
    caller must supply a real, exact human grant before a real activation port
    can publish a candidate plan; tests use only fakes.
Outputs:
    Non-sensitive immutable run evidence.  Every activated invocation attempts
    containment before return, including successful 1,200-slot completion.
Failure modes:
    Preflight/admission, binding, source, claim, serving, health, identity,
    ledger, and clock failures stop the serial run.  A terminal ledger record
    is appended when a validated clock/timestamp permits it; otherwise the
    root fails closed and contains the active route immediately.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
import unicodedata
from typing import Callable, Protocol

from .canary_activation import (
    ActivationAttempt,
    ActivationTimestamps,
    ActiveCanaryContext,
)
from .canary_admission import (
    Stage4AdmissionRequest,
    Stage4AdmissionResult,
    Stage4RepositoryEvidence,
    evaluate_stage4_admission,
)
from .canary_execution_ledger import (
    Stage4ExecutionLedger,
    Stage4LedgerAppendResult,
    Stage4LedgerError,
    Stage4LedgerProgress,
    Stage4LedgerStatus,
    Stage4SlotObservation,
)
from .canary_rollback import (
    RollbackContext,
    RollbackRequest,
    RollbackResult,
    RollbackTrigger,
)
from .canary_route_authority import RouteClaim
from .canary_route_state import RouteStateBinding
from .canary_runtime_types import Stage4RuntimeReadiness, Stage4SlotSafety
from .canary_routing import CanaryRoutePlan
from .canary_schedule import (
    Stage4ExecutionSchedule,
    Stage4ScheduleStep,
    Stage4ScheduleStepKind,
)
from .canary_stage4_evidence_binding import Stage4EvidenceBinding
from .canary_serial_runner import Dataset002ScheduleVectorSource
from .canary_workload import CandidateSelectionRecord, EligibleWorkloadManifest
from .host_observation import RangeQueryRequest, ServedQueryOutcome
from .lkg_phase3_binding import LkgPhase3AuthorityPair
from .policy import PolicyDecision
from .shadow_event_types import MonitorStreamKey


__all__ = [
    "Stage4LkgAuthorityProvider",
    "Stage4LiveRunRequest",
    "Stage4LiveRunResult",
    "Stage4LiveRunner",
    "Stage4SlotSafety",
]


_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_UTC = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)


class _ActivationPort(Protocol):
    def activate(self, **kwargs: object) -> ActivationAttempt: ...


class _AuthorityPort(Protocol):
    def resolve_and_claim(self, occurrence_id: object) -> RouteClaim: ...


class _RollbackPort(Protocol):
    def rollback(self, request: RollbackRequest) -> RollbackResult: ...


class _ServingPort(Protocol):
    def execute(self, request: RangeQueryRequest) -> ServedQueryOutcome: ...


class _RuntimeProbePort(Protocol):
    def preflight(self, *, binding: object) -> Stage4RuntimeReadiness: ...

    def slot_safety(self, *, binding: object) -> "Stage4SlotSafety": ...


class Stage4LkgAuthorityProvider(Protocol):
    """Return one already-validated fresh D1/D2 pair per independent refresh."""

    def refresh(self) -> LkgPhase3AuthorityPair: ...


@dataclass(frozen=True, slots=True)
class Stage4LiveRunRequest:
    """Static human-gated inputs; no long-lived D1/D2 authority is retained."""

    manifest: EligibleWorkloadManifest
    selection: CandidateSelectionRecord
    plan: CanaryRoutePlan
    schedule: Stage4ExecutionSchedule
    policy_decision: PolicyDecision
    evidence_binding: Stage4EvidenceBinding
    repository: Stage4RepositoryEvidence
    runtime_binding: RouteStateBinding
    grant: object
    trust_store: object
    approval_context: object
    activation_timestamps: ActivationTimestamps
    run_id: str

    def __post_init__(self) -> None:
        for value, expected, field in (
            (self.manifest, EligibleWorkloadManifest, "manifest"),
            (self.selection, CandidateSelectionRecord, "selection"),
            (self.plan, CanaryRoutePlan, "plan"),
            (self.policy_decision, PolicyDecision, "policy_decision"),
            (self.evidence_binding, Stage4EvidenceBinding, "evidence_binding"),
            (self.repository, Stage4RepositoryEvidence, "repository"),
            (self.runtime_binding, RouteStateBinding, "runtime_binding"),
        ):
            if not isinstance(value, expected):
                raise TypeError(f"{field} must be a {expected.__name__}")
        if not isinstance(self.schedule, Stage4ExecutionSchedule):
            raise TypeError("schedule must be a Stage4ExecutionSchedule")
        if not isinstance(self.activation_timestamps, ActivationTimestamps):
            raise TypeError("activation_timestamps must be ActivationTimestamps")
        if not all(
            _valid_utc(value)
            for value in (
                self.activation_timestamps.reserved_at_utc,
                self.activation_timestamps.authorized_at_utc,
                self.activation_timestamps.marker_at_utc,
                self.activation_timestamps.failure_at_utc,
            )
        ):
            raise ValueError("activation timestamps must be RFC3339 UTC")
        if not _canonical_run_id(self.run_id):
            raise ValueError("run_id must be canonical non-empty text")
        if self.run_id != self.evidence_binding.run_id:
            raise ValueError("run_id must equal evidence_binding.run_id")


@dataclass(frozen=True, slots=True)
class Stage4LiveRunResult:
    """Non-sensitive outcome; success requires verified LKG restoration."""

    dispatched_slot_count: int
    activation: ActivationAttempt | None
    first_admission: Stage4AdmissionResult | None
    post_activation_admission: Stage4AdmissionResult | None
    ledger_progress: Stage4LedgerProgress | None
    rollback: RollbackResult | None
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _SlotExecution:
    """Private result retaining a refusal code when no ledger record is safe."""

    observation: Stage4SlotObservation | None
    dispatched: bool
    reason_code: str | None


class Stage4LiveRunner:
    """Strict serial root that contains every activated plan before returning.

    This class is candidate-capable only through its injected activation,
    authority, serving, and rollback ports.  It cannot create a grant, choose a
    route, mutate a Milvus configuration, retry a slot, batch work, or resume a
    partially executed candidate run.  Restart recovery is deliberately
    LKG-only and handled by the existing route-state/ledger recovery boundary.
    """

    def __init__(
        self,
        *,
        request: Stage4LiveRunRequest,
        activation: _ActivationPort,
        authority: _AuthorityPort,
        runtime_probe: _RuntimeProbePort,
        serving: _ServingPort,
        vector_source: Dataset002ScheduleVectorSource,
        ledger: Stage4ExecutionLedger,
        rollback: _RollbackPort,
        lkg_authority_provider: Stage4LkgAuthorityProvider,
        admission_evaluator: Callable[[object], Stage4AdmissionResult] = evaluate_stage4_admission,
        monotonic_ns: Callable[[], int],
        utc_now: Callable[[], str],
    ) -> None:
        if not isinstance(request, Stage4LiveRunRequest):
            raise TypeError("request must be a Stage4LiveRunRequest")
        if not isinstance(vector_source, Dataset002ScheduleVectorSource):
            raise TypeError("vector_source must be a Dataset002ScheduleVectorSource")
        if not isinstance(ledger, Stage4ExecutionLedger):
            raise TypeError("ledger must be a Stage4ExecutionLedger")
        for port, method in (
            (activation, "activate"),
            (authority, "resolve_and_claim"),
            (runtime_probe, "preflight"),
            (runtime_probe, "slot_safety"),
            (serving, "execute"),
            (rollback, "rollback"),
            (lkg_authority_provider, "refresh"),
        ):
            if not callable(getattr(port, method, None)):
                raise TypeError(f"dependency must provide {method}")
        if not callable(admission_evaluator) or not callable(monotonic_ns) or not callable(utc_now):
            raise TypeError("evaluator and clocks must be callable")
        self._request = request
        self._activation = activation
        self._authority = authority
        self._runtime_probe = runtime_probe
        self._serving = serving
        self._source = vector_source
        self._ledger = ledger
        self._rollback = rollback
        self._lkg_authority_provider = lkg_authority_provider
        self._admission_evaluator = admission_evaluator
        self._monotonic_ns = monotonic_ns
        self._utc_now = utc_now

    def run(self) -> Stage4LiveRunResult:
        """Perform one fresh 1,200-slot serial run or fail closed before it."""

        if self._ledger.schedule_sha256 != self._request.schedule.schedule_sha256:
            return self._result(0, None, None, None, None, ("LEDGER_SCHEDULE_MISMATCH",))
        try:
            progress = self._ledger.progress()
        except Stage4LedgerError:
            return self._result(0, None, None, None, None, ("LEDGER_UNAVAILABLE",))
        if progress.status is not Stage4LedgerStatus.IN_PROGRESS or progress.record_count != 0:
            return self._result(0, None, None, None, None, ("LEDGER_NOT_FRESH",))

        first = self._admit()
        if not self._admission_matches_schedule(first):
            return self._result(0, None, first, None, None, ("INITIAL_ADMISSION_REFUSED",))

        activation = self._activate()
        if activation is None or not activation.activated or activation.active_context is None:
            return self._result(0, activation, first, None, None, ("ACTIVATION_REFUSED",))
        context = activation.active_context
        if not self._active_context_matches(context, first):
            rollback = self._contain(context, RollbackTrigger.RUNTIME_PREFLIGHT_FAILURE)
            return self._result(
                0,
                activation,
                first,
                None,
                rollback,
                ("ACTIVATION_CONTEXT_MISMATCH",),
            )

        second = self._admit()
        if not self._admission_matches_schedule(second):
            rollback = self._contain(context, RollbackTrigger.RUNTIME_PREFLIGHT_FAILURE)
            return self._result(
                0,
                activation,
                first,
                second,
                rollback,
                ("POST_ACTIVATION_ADMISSION_REFUSED",),
            )
        assert first is not None and first.receipt is not None
        assert second is not None and second.receipt is not None
        if not first.receipt.stable_lineage_matches(second.receipt):
            rollback = self._contain(context, RollbackTrigger.RUNTIME_PREFLIGHT_FAILURE)
            return self._result(
                0,
                activation,
                first,
                second,
                rollback,
                ("POST_ACTIVATION_STABLE_LINEAGE_MISMATCH",),
            )

        dispatched = 0
        for step in self._request.schedule.steps:
            execution = self._execute_slot(step)
            dispatched += int(execution.dispatched)
            if execution.observation is None:
                rollback = self._contain(context, RollbackTrigger.SLOT_SAFETY_FAILURE)
                return self._result(
                    dispatched,
                    activation,
                    first,
                    second,
                    rollback,
                    (execution.reason_code or "SLOT_OBSERVATION_UNAVAILABLE",),
                )
            try:
                appended = self._ledger.append(execution.observation)
            except Stage4LedgerError:
                rollback = self._contain(context, RollbackTrigger.SLOT_SAFETY_FAILURE)
                return self._result(
                    dispatched, activation, first, second, rollback, ("LEDGER_UNAVAILABLE",)
                )
            if not isinstance(appended, Stage4LedgerAppendResult):
                rollback = self._contain(context, RollbackTrigger.SLOT_SAFETY_FAILURE)
                return self._result(
                    dispatched,
                    activation,
                    first,
                    second,
                    rollback,
                    ("LEDGER_APPEND_INVALID",),
                )
            if not appended.accepted or not _safe(execution.observation):
                rollback = self._contain(context, RollbackTrigger.SLOT_SAFETY_FAILURE)
                return self._result(
                    dispatched,
                    activation,
                    first,
                    second,
                    rollback,
                    (
                        execution.observation.reason_code
                        or appended.reason_code
                        or "SLOT_UNSAFE",
                    ),
                )

        rollback = self._contain(context, RollbackTrigger.COMPLETED_CANARY)
        reasons = () if rollback.contained and rollback.restoration_verified else ("FINAL_RESTORATION_UNVERIFIED",)
        return self._result(dispatched, activation, first, second, rollback, reasons)

    def _admit(self) -> Stage4AdmissionResult | None:
        try:
            runtime = self._runtime_probe.preflight(
                binding=self._request.runtime_binding
            )
            if not isinstance(runtime, Stage4RuntimeReadiness):
                return None
            pair = self._lkg_authority_provider.refresh()
            if type(pair) is not LkgPhase3AuthorityPair:
                return None
            value = self._admission_evaluator(
                Stage4AdmissionRequest(
                    manifest=self._request.manifest,
                    selection=self._request.selection,
                    plan=self._request.plan,
                    schedule=self._request.schedule,
                    policy_decision=self._request.policy_decision,
                    lkg_authority=pair,
                    evidence_binding=self._request.evidence_binding,
                    repository=self._request.repository,
                    runtime=runtime,
                )
            )
        except Exception:
            return None
        return value if isinstance(value, Stage4AdmissionResult) else None

    def _activate(self) -> ActivationAttempt | None:
        try:
            value = self._activation.activate(
                grant=self._request.grant,
                trust_store=self._request.trust_store,
                approval_context=self._request.approval_context,
                plan=self._request.plan,
                binding=self._request.runtime_binding,
                timestamps=self._request.activation_timestamps,
            )
        except Exception:
            return None
        return value if isinstance(value, ActivationAttempt) else None

    def _execute_slot(self, step: Stage4ScheduleStep) -> _SlotExecution:
        try:
            start = self._clock()
        except Exception:
            return _SlotExecution(None, False, "CLOCK_START_INVALID")
        try:
            vector = self._source.vector_for_step(step)
        except Exception:
            return self._failure(step, start, "QUERY_SOURCE_FAILURE", dispatched=False)

        before = self._safety()
        if before is None or not before.health_ok or not before.identity_ok:
            return self._failure(
                step,
                start,
                before.reason_code if before is not None else "SLOT_PREFLIGHT_UNAVAILABLE",
                before=before,
                dispatched=False,
            )
        if step.kind is Stage4ScheduleStepKind.ROUTING and not self._claim_matches(step):
            return self._failure(
                step, start, "ROUTE_CLAIM_REFUSED", before=before, dispatched=False
            )

        try:
            request = self._range_request(step, vector)
        except Exception:
            return self._failure(
                step, start, "SERVING_REQUEST_INVALID", before=before, dispatched=False
            )
        try:
            started = self._clock()
        except Exception:
            return self._failure(
                step, start, "CLOCK_START_INVALID", before=before, dispatched=False
            )
        try:
            outcome = self._serving.execute(request)
        except Exception:
            return self._failure(
                step, started, "SERVING_EXCEPTION", before=before, dispatched=True
            )
        try:
            finished = self._clock()
        except Exception:
            return self._failure(
                step, started, "CLOCK_FINISH_INVALID", before=before, dispatched=True
            )
        after = self._safety()
        if not isinstance(outcome, ServedQueryOutcome):
            return self._failure(
                step,
                started,
                "SERVING_OUTCOME_INVALID",
                before=before,
                after=after,
                dispatched=True,
            )
        if not outcome.success or outcome.timed_out:
            return self._failure(
                step,
                started,
                self._failure_code(outcome.error_code)
                or ("SERVING_TIMEOUT" if outcome.timed_out else "SERVING_FAILED"),
                before=before,
                after=after,
                dispatched=True,
                finished=finished,
                result_count=outcome.result_count,
                timed_out=outcome.timed_out,
            )
        if after is None or not after.health_ok or not after.identity_ok:
            return self._failure(
                step,
                started,
                after.reason_code if after is not None else "SLOT_POSTFLIGHT_UNAVAILABLE",
                before=before,
                after=after,
                dispatched=True,
                finished=finished,
                result_count=outcome.result_count,
            )
        try:
            observation = Stage4SlotObservation(
                execution_index=step.execution_index,
                observed_ef=step.expected_ef,
                started_monotonic_ns=started,
                finished_monotonic_ns=finished,
                recorded_at_utc=self._timestamp(),
                success=True,
                timed_out=False,
                threshold_semantics_valid=True,
                health_before_ok=True,
                health_after_ok=True,
                identity_before_ok=True,
                identity_after_ok=True,
                result_count=outcome.result_count,
                latency_ms=(finished - started) / 1_000_000.0,
                reason_code=None,
            )
        except Exception:
            return _SlotExecution(None, True, "UTC_CLOCK_INVALID")
        return _SlotExecution(observation, True, None)

    @staticmethod
    def _failure_code(value: str | None) -> str | None:
        """Accept only the ledger's non-sensitive stable reason vocabulary."""

        return value if isinstance(value, str) and _CODE.fullmatch(value) else None

    def _claim_matches(self, step: Stage4ScheduleStep) -> bool:
        try:
            claim = self._authority.resolve_and_claim(step.occurrence_id)
        except Exception:
            return False
        return bool(
            isinstance(claim, RouteClaim)
            and claim.accepted
            and claim.occurrence_id == step.occurrence_id
            and claim.dataset_query_id == step.dataset_query_id
            and claim.ef == step.expected_ef
            and claim.kind == step.route_kind
        )

    def _range_request(
        self, step: Stage4ScheduleStep, vector: tuple[float, ...]
    ) -> RangeQueryRequest:
        manifest = self._request.manifest
        plan = self._request.plan
        stream = MonitorStreamKey(
            f"exp009:{self._request.run_id}",
            plan.metric,
            plan.threshold_stratum,
            plan.configuration_identity,
            plan.data_identity,
            plan.flat_binding_id,
            plan.hnsw_binding_id,
        )
        return RangeQueryRequest(
            step.execution_index,
            stream,
            vector,
            manifest.radius,
            manifest.range_filter,
            manifest.limit,
            step.expected_ef,
        )

    def _safety(self) -> Stage4SlotSafety | None:
        try:
            value = self._runtime_probe.slot_safety(
                binding=self._request.runtime_binding
            )
        except Exception:
            return None
        return value if isinstance(value, Stage4SlotSafety) else None

    def _contain(
        self, context: ActiveCanaryContext, trigger: RollbackTrigger
    ) -> RollbackResult:
        try:
            occurred_at_utc = self._timestamp()
        except Exception:
            # This prevalidated timestamp is solely a fail-safe fallback when
            # the runtime UTC clock is unavailable; it avoids stranding a route.
            occurred_at_utc = self._request.activation_timestamps.failure_at_utc
        try:
            result = self._rollback.rollback(
                RollbackRequest(
                    trigger,
                    RollbackContext(
                        context.grant_id,
                        context.signed_payload_sha256,
                        context.policy_audit_id,
                        context.plan_sha256,
                        context.binding,
                        occurred_at_utc,
                    ),
                    None,
                )
            )
        except Exception:
            return RollbackResult(False, False, "ROLLBACK_UNAVAILABLE", None, None, False)
        return (
            result
            if isinstance(result, RollbackResult)
            else RollbackResult(False, False, "ROLLBACK_RESULT_INVALID", None, None, False)
        )

    def _failure(
        self,
        step: Stage4ScheduleStep,
        start: int,
        reason: str,
        *,
        before: Stage4SlotSafety | None = None,
        after: Stage4SlotSafety | None = None,
        dispatched: bool,
        finished: int | None = None,
        result_count: int = 0,
        timed_out: bool = False,
    ) -> _SlotExecution:
        try:
            end = self._clock() if finished is None else finished
            observation = Stage4SlotObservation(
                execution_index=step.execution_index,
                observed_ef=step.expected_ef,
                started_monotonic_ns=start,
                finished_monotonic_ns=end,
                recorded_at_utc=self._timestamp(),
                success=False,
                timed_out=timed_out,
                threshold_semantics_valid=False,
                health_before_ok=before.health_ok if before is not None else False,
                health_after_ok=after.health_ok if after is not None else False,
                identity_before_ok=before.identity_ok if before is not None else False,
                identity_after_ok=after.identity_ok if after is not None else False,
                result_count=result_count,
                latency_ms=(end - start) / 1_000_000.0,
                reason_code=reason,
            )
        except Exception:
            return _SlotExecution(None, dispatched, "CLOCK_OR_TIMESTAMP_INVALID")
        return _SlotExecution(observation, dispatched, reason)

    def _clock(self) -> int:
        value = self._monotonic_ns()
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RuntimeError("CLOCK_INVALID")
        return value

    def _timestamp(self) -> str:
        value = self._utc_now()
        try:
            parsed = (
                datetime.fromisoformat(value[:-1] + "+00:00")
                if isinstance(value, str) and value.endswith("Z")
                else None
            )
        except ValueError:
            parsed = None
        if parsed is None or parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
            raise RuntimeError("UTC_CLOCK_INVALID")
        return value

    def _active_context_matches(
        self, context: ActiveCanaryContext, admission: Stage4AdmissionResult
    ) -> bool:
        return bool(
            context.plan_sha256 == self._request.schedule.plan_sha256
            and context.binding == self._request.runtime_binding
            and context.policy_audit_id == admission.policy_audit_id
        )

    def _admission_matches_schedule(
        self, admission: Stage4AdmissionResult | None
    ) -> bool:
        if type(admission) is not Stage4AdmissionResult or admission.receipt is None:
            return False
        receipt = admission.receipt
        return bool(
            receipt.matches_canonical_digest()
            and receipt.route_plan_sha256 == self._request.schedule.plan_sha256
            and receipt.execution_schedule_sha256
            == self._request.schedule.schedule_sha256
        )

    def _result(
        self,
        dispatched: int,
        activation: ActivationAttempt | None,
        first: Stage4AdmissionResult | None,
        second: Stage4AdmissionResult | None,
        rollback: RollbackResult | None,
        reasons: tuple[str, ...],
    ) -> Stage4LiveRunResult:
        try:
            progress = self._ledger.progress()
        except Stage4LedgerError:
            progress = None
        return Stage4LiveRunResult(
            dispatched, activation, first, second, progress, rollback, reasons
        )


def _safe(observation: Stage4SlotObservation) -> bool:
    return bool(
        observation.success
        and not observation.timed_out
        and observation.threshold_semantics_valid
        and observation.health_before_ok
        and observation.health_after_ok
        and observation.identity_before_ok
        and observation.identity_after_ok
    )


def _valid_utc(value: object) -> bool:
    if not isinstance(value, str) or _UTC.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    return bool(
        parsed.tzinfo is not None
        and parsed.utcoffset() == timezone.utc.utcoffset(parsed)
    )


def _canonical_run_id(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and value
        and value == unicodedata.normalize("NFC", value)
        and value.strip() == value
        and all(ord(character) >= 0x20 and ord(character) != 0x7F for character in value)
    )
