"""Thin, dependency-injected safe-actuation boundary for ADR-002.

Purpose:
    Execute only policy-authorized START_CANARY and ROLLBACK operations, record
    immutable outcomes, and leave detector/policy decisions outside this module.
Inputs:
    A frozen PolicyDecision, ActuationContext, client-like executor, append-only
    audit sink, and automatic-action controller.
Outputs:
    Immutable ActuationResult and ActuationAuditRecord values.
Dependencies:
    Protocols and offline policy value objects only; never PyMilvus.
Failure modes:
    Missing/duplicate audit identity, unsafe START_CANARY gates, invalid context,
    excess exposure, client failures, and failed rollback verification fail closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import math
import re
from typing import Protocol, TypeAlias

from .config import Metric, THRESHOLD_LABELS
from .policy import (
    CanaryObservation,
    PolicyAction,
    PolicyDecision,
    QualificationResult,
    SafetyGateResult,
)

MAX_CANARY_TRAFFIC_FRACTION = 0.10
AUDIT_QUERY_COUNT = 50

QueryId: TypeAlias = int | str

_RFC3339_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")


class ActuationOutcome(StrEnum):
    """Observable result categories from the execution boundary."""

    NO_OP = "NO_OP"
    BLOCKED = "BLOCKED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class ShadowResult:
    """Exact pre-canary shadow evidence returned by the client-like adapter."""

    success: bool
    audited_query_count: int
    failed_query_count: int
    timeout_query_count: int
    threshold_violation_count: int
    candidate_flat_oracle_agreement: bool
    last_known_good_flat_oracle_agreement: bool
    detail: str = ""


@dataclass(frozen=True, slots=True)
class RollbackVerification:
    """Post-rollback health, audit, identity, and restored-ef evidence."""

    success: bool
    restored_ef: int | None
    health_passed: bool
    audit_passed: bool
    configuration_identity: str
    index_identity: str
    data_identity: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class ActuationContext:
    """Immutable execution identity and audit-query context."""

    metric: Metric | str
    threshold_stratum: str
    collection_name: str
    configuration_identity: str
    index_identity: str
    data_identity: str
    audited_query_ids: tuple[QueryId, ...]
    last_known_good: QualificationResult
    occurred_at_utc: str


@dataclass(frozen=True, slots=True)
class ActuationAuditRecord:
    """Immutable append-only record of one boundary invocation."""

    audit_id: str
    action: PolicyAction
    outcome: ActuationOutcome
    attempted: bool
    success: bool
    reason: str
    context: ActuationContext
    current_ef: int
    candidate_ef: int | None
    last_known_good_ef: int | None
    traffic_fraction: float | None
    policy_reason: str
    safety_gate_results: tuple[SafetyGateResult, ...]
    shadow_result: ShadowResult | None = None
    canary_observation: CanaryObservation | None = None
    rollback_verification: RollbackVerification | None = None
    automatic_actions_disabled: bool = False


@dataclass(frozen=True, slots=True)
class ActuationResult:
    """Immutable return value from the safe-actuation boundary."""

    outcome: ActuationOutcome
    executed: bool
    success: bool
    reason: str
    audit_record: ActuationAuditRecord
    canary_observation: CanaryObservation | None = None
    automatic_actions_disabled: bool = False


class ActuationClientLike(Protocol):
    """High-level adapter implemented by fakes now and Milvus/routing later."""

    def shadow_candidate(
        self,
        *,
        context: ActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult: ...

    def start_canary(
        self,
        *,
        context: ActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
        traffic_fraction: float,
    ) -> CanaryObservation: ...

    def stop_candidate(self) -> None: ...

    def restore_last_known_good(self, ef: int) -> None: ...

    def verify_restoration(
        self,
        *,
        context: ActuationContext,
        expected_ef: int,
    ) -> RollbackVerification: ...


class AuditSinkLike(Protocol):
    """Append-only audit sink with duplicate-ID lookup."""

    def contains(self, audit_id: str) -> bool: ...

    def append(self, record: ActuationAuditRecord) -> None: ...


class AutomaticActionControllerLike(Protocol):
    """Owner of the fail-closed automatic-action disable switch."""

    def disable_automatic_actions(self, *, audit_id: str, reason: str) -> None: ...


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _valid_rfc3339_utc(value: object) -> bool:
    if not isinstance(value, str) or _RFC3339_UTC.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    offset = parsed.utcoffset()
    return offset is not None and offset.total_seconds() == 0


def _metric(value: Metric | str) -> Metric | None:
    try:
        return Metric(value)
    except (TypeError, ValueError):
        return None


def _valid_query_ids(values: object) -> bool:
    if not isinstance(values, tuple) or len(values) != AUDIT_QUERY_COUNT:
        return False
    canonical: set[tuple[type, QueryId]] = set()
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            return False
        if isinstance(value, str) and not value:
            return False
        key = (type(value), value)
        if key in canonical:
            return False
        canonical.add(key)
    return True


def _context_failure(
    decision: PolicyDecision,
    context: ActuationContext,
) -> str | None:
    if not isinstance(context, ActuationContext):
        return "ACTUATION_CONTEXT_INVALID"
    metric = _metric(context.metric)
    if metric is None:
        return "ACTUATION_METRIC_INVALID"
    if context.threshold_stratum not in THRESHOLD_LABELS:
        return "ACTUATION_THRESHOLD_STRATUM_INVALID"
    if not all(
        _nonempty(value)
        for value in (
            context.collection_name,
            context.configuration_identity,
            context.index_identity,
            context.data_identity,
        )
    ):
        return "ACTUATION_IDENTITY_MISSING"
    if not _valid_query_ids(context.audited_query_ids):
        return "ACTUATION_AUDIT_QUERY_IDS_INVALID"
    if not _valid_rfc3339_utc(context.occurred_at_utc):
        return "ACTUATION_TIMESTAMP_INVALID"
    qualification = context.last_known_good
    if (
        not isinstance(qualification, QualificationResult)
        or not qualification.qualified
    ):
        return "LAST_KNOWN_GOOD_NOT_QUALIFIED"
    if (
        qualification.ef is None
        or decision.last_known_good_ef != qualification.ef
        or qualification.metric is not metric
        or qualification.threshold_stratum != context.threshold_stratum
        or qualification.configuration_identity != context.configuration_identity
        or qualification.index_identity != context.index_identity
        or qualification.data_identity != context.data_identity
    ):
        return "LAST_KNOWN_GOOD_IDENTITY_MISMATCH"
    if decision.action is PolicyAction.START_CANARY and decision.candidate_ef is None:
        return "CANDIDATE_EF_MISSING"
    return None


def _shadow_failure(result: object) -> str | None:
    if not isinstance(result, ShadowResult):
        return "SHADOW_RESULT_INVALID"
    checks = (
        result.success is True,
        isinstance(result.audited_query_count, int)
        and not isinstance(result.audited_query_count, bool)
        and result.audited_query_count == AUDIT_QUERY_COUNT,
        result.failed_query_count == 0,
        result.timeout_query_count == 0,
        result.threshold_violation_count == 0,
        result.candidate_flat_oracle_agreement is True,
        result.last_known_good_flat_oracle_agreement is True,
    )
    return None if all(checks) else "SHADOW_AUDIT_FAILED"


def _observation_matches(
    observation: object,
    *,
    decision: PolicyDecision,
    context: ActuationContext,
) -> bool:
    if not isinstance(observation, CanaryObservation):
        return False
    return bool(
        observation.candidate_ef == decision.candidate_ef
        and observation.last_known_good_ef == decision.last_known_good_ef
        and _metric(observation.metric) is _metric(context.metric)
        and observation.threshold_stratum == context.threshold_stratum
        and observation.configuration_identity == context.configuration_identity
        and observation.index_identity == context.index_identity
        and observation.data_identity == context.data_identity
    )


def _verification_succeeded(
    verification: object,
    *,
    expected_ef: int,
    context: ActuationContext,
) -> bool:
    return bool(
        isinstance(verification, RollbackVerification)
        and verification.success is True
        and verification.restored_ef == expected_ef
        and verification.health_passed is True
        and verification.audit_passed is True
        and verification.configuration_identity == context.configuration_identity
        and verification.index_identity == context.index_identity
        and verification.data_identity == context.data_identity
    )


class SafeActuationBoundary:
    """Execute policy output without deriving detector or policy decisions."""

    def __init__(
        self,
        client: ActuationClientLike,
        audit_sink: AuditSinkLike,
        controller: AutomaticActionControllerLike,
    ) -> None:
        self.client = client
        self.audit_sink = audit_sink
        self.controller = controller

    def _record(
        self,
        decision: PolicyDecision,
        context: ActuationContext,
        *,
        outcome: ActuationOutcome,
        attempted: bool,
        success: bool,
        reason: str,
        traffic_fraction: float | None,
        shadow_result: ShadowResult | None = None,
        canary_observation: CanaryObservation | None = None,
        rollback_verification: RollbackVerification | None = None,
        automatic_actions_disabled: bool = False,
    ) -> ActuationAuditRecord:
        return ActuationAuditRecord(
            audit_id=decision.audit_id,
            action=decision.action,
            outcome=outcome,
            attempted=attempted,
            success=success,
            reason=reason,
            context=context,
            current_ef=decision.current_ef,
            candidate_ef=decision.candidate_ef,
            last_known_good_ef=decision.last_known_good_ef,
            traffic_fraction=traffic_fraction,
            policy_reason=decision.reason,
            safety_gate_results=decision.safety_gate_results,
            shadow_result=shadow_result,
            canary_observation=canary_observation,
            rollback_verification=rollback_verification,
            automatic_actions_disabled=automatic_actions_disabled,
        )

    def _result(
        self,
        record: ActuationAuditRecord,
        *,
        append: bool,
    ) -> ActuationResult:
        if append:
            self.audit_sink.append(record)
        return ActuationResult(
            outcome=record.outcome,
            executed=record.attempted,
            success=record.success,
            reason=record.reason,
            audit_record=record,
            canary_observation=record.canary_observation,
            automatic_actions_disabled=record.automatic_actions_disabled,
        )

    def _blocked(
        self,
        decision: PolicyDecision,
        context: ActuationContext,
        *,
        reason: str,
        traffic_fraction: float | None,
        append: bool,
    ) -> ActuationResult:
        return self._result(
            self._record(
                decision,
                context,
                outcome=ActuationOutcome.BLOCKED,
                attempted=False,
                success=False,
                reason=reason,
                traffic_fraction=traffic_fraction,
            ),
            append=append,
        )

    def execute(
        self,
        decision: PolicyDecision,
        context: ActuationContext,
        *,
        traffic_fraction: float = MAX_CANARY_TRAFFIC_FRACTION,
    ) -> ActuationResult:
        """Execute or record one policy decision with no policy recomputation."""

        if not isinstance(decision, PolicyDecision):
            raise TypeError("decision must be a PolicyDecision")
        if not _nonempty(decision.audit_id):
            return self._blocked(
                decision,
                context,
                reason="AUDIT_ID_MISSING",
                traffic_fraction=None,
                append=False,
            )
        if self.audit_sink.contains(decision.audit_id):
            return self._blocked(
                decision,
                context,
                reason="AUDIT_ID_DUPLICATE",
                traffic_fraction=None,
                append=False,
            )

        if decision.action in {PolicyAction.NO_CHANGE, PolicyAction.RECOMMEND_EF}:
            return self._result(
                self._record(
                    decision,
                    context,
                    outcome=ActuationOutcome.NO_OP,
                    attempted=False,
                    success=True,
                    reason="NON_ACTIONABLE_POLICY_DECISION",
                    traffic_fraction=None,
                ),
                append=True,
            )
        if decision.action not in {PolicyAction.START_CANARY, PolicyAction.ROLLBACK}:
            return self._blocked(
                decision,
                context,
                reason="POLICY_ACTION_UNSUPPORTED",
                traffic_fraction=None,
                append=True,
            )

        context_failure = _context_failure(decision, context)
        if context_failure is not None:
            return self._blocked(
                decision,
                context,
                reason=context_failure,
                traffic_fraction=None,
                append=True,
            )

        if decision.action is PolicyAction.START_CANARY:
            failed_gate = next(
                (gate for gate in decision.safety_gate_results if not gate.passed),
                None,
            )
            if not decision.safety_gate_results:
                return self._blocked(
                    decision,
                    context,
                    reason="SAFETY_GATES_MISSING",
                    traffic_fraction=traffic_fraction,
                    append=True,
                )
            if failed_gate is not None:
                return self._blocked(
                    decision,
                    context,
                    reason=f"SAFETY_GATE_FAILED:{failed_gate.name}",
                    traffic_fraction=traffic_fraction,
                    append=True,
                )
            if (
                isinstance(traffic_fraction, bool)
                or not isinstance(traffic_fraction, (int, float))
                or not math.isfinite(traffic_fraction)
                or not 0.0 < traffic_fraction <= MAX_CANARY_TRAFFIC_FRACTION
            ):
                return self._blocked(
                    decision,
                    context,
                    reason="CANARY_TRAFFIC_FRACTION_INVALID",
                    traffic_fraction=(
                        float(traffic_fraction)
                        if isinstance(traffic_fraction, (int, float))
                        and not isinstance(traffic_fraction, bool)
                        else None
                    ),
                    append=True,
                )
            return self._start_canary(
                decision,
                context,
                traffic_fraction=float(traffic_fraction),
            )
        return self._rollback(decision, context)

    def _start_canary(
        self,
        decision: PolicyDecision,
        context: ActuationContext,
        *,
        traffic_fraction: float,
    ) -> ActuationResult:
        candidate_ef = int(decision.candidate_ef)
        last_known_good_ef = int(decision.last_known_good_ef)
        try:
            shadow = self.client.shadow_candidate(
                context=context,
                candidate_ef=candidate_ef,
                last_known_good_ef=last_known_good_ef,
            )
        except Exception as exc:  # client boundary must convert failures to evidence
            record = self._record(
                decision,
                context,
                outcome=ActuationOutcome.FAILED,
                attempted=True,
                success=False,
                reason=f"SHADOW_CLIENT_EXCEPTION:{type(exc).__name__}",
                traffic_fraction=traffic_fraction,
            )
            return self._result(record, append=True)

        shadow_failure = _shadow_failure(shadow)
        if shadow_failure is not None:
            return self._result(
                self._record(
                    decision,
                    context,
                    outcome=ActuationOutcome.FAILED,
                    attempted=True,
                    success=False,
                    reason=shadow_failure,
                    traffic_fraction=traffic_fraction,
                    shadow_result=(
                        shadow if isinstance(shadow, ShadowResult) else None
                    ),
                ),
                append=True,
            )
        try:
            observation = self.client.start_canary(
                context=context,
                candidate_ef=candidate_ef,
                last_known_good_ef=last_known_good_ef,
                traffic_fraction=traffic_fraction,
            )
        except Exception as exc:  # candidate state may be unknown; disable automation
            reason = f"CANARY_CLIENT_EXCEPTION:{type(exc).__name__}"
            self.controller.disable_automatic_actions(
                audit_id=decision.audit_id,
                reason=reason,
            )
            return self._result(
                self._record(
                    decision,
                    context,
                    outcome=ActuationOutcome.FAILED,
                    attempted=True,
                    success=False,
                    reason=reason,
                    traffic_fraction=traffic_fraction,
                    shadow_result=shadow,
                    automatic_actions_disabled=True,
                ),
                append=True,
            )
        if not _observation_matches(
            observation,
            decision=decision,
            context=context,
        ):
            reason = "CANARY_OBSERVATION_IDENTITY_MISMATCH"
            self.controller.disable_automatic_actions(
                audit_id=decision.audit_id,
                reason=reason,
            )
            return self._result(
                self._record(
                    decision,
                    context,
                    outcome=ActuationOutcome.FAILED,
                    attempted=True,
                    success=False,
                    reason=reason,
                    traffic_fraction=traffic_fraction,
                    shadow_result=shadow,
                    canary_observation=(
                        observation
                        if isinstance(observation, CanaryObservation)
                        else None
                    ),
                    automatic_actions_disabled=True,
                ),
                append=True,
            )
        return self._result(
            self._record(
                decision,
                context,
                outcome=ActuationOutcome.SUCCEEDED,
                attempted=True,
                success=True,
                reason="CANARY_STARTED",
                traffic_fraction=traffic_fraction,
                shadow_result=shadow,
                canary_observation=observation,
            ),
            append=True,
        )

    def _rollback(
        self,
        decision: PolicyDecision,
        context: ActuationContext,
    ) -> ActuationResult:
        last_known_good_ef = int(decision.last_known_good_ef)
        verification: RollbackVerification | None = None
        failure_reason: str | None = None
        try:
            self.client.stop_candidate()
            self.client.restore_last_known_good(last_known_good_ef)
            returned = self.client.verify_restoration(
                context=context,
                expected_ef=last_known_good_ef,
            )
            if isinstance(returned, RollbackVerification):
                verification = returned
            if not _verification_succeeded(
                returned,
                expected_ef=last_known_good_ef,
                context=context,
            ):
                failure_reason = "ROLLBACK_VERIFICATION_FAILED"
        except Exception as exc:  # rollback exception is always fail-closed
            failure_reason = f"ROLLBACK_CLIENT_EXCEPTION:{type(exc).__name__}"

        if failure_reason is not None:
            self.controller.disable_automatic_actions(
                audit_id=decision.audit_id,
                reason=failure_reason,
            )
            return self._result(
                self._record(
                    decision,
                    context,
                    outcome=ActuationOutcome.FAILED,
                    attempted=True,
                    success=False,
                    reason=failure_reason,
                    traffic_fraction=None,
                    rollback_verification=verification,
                    automatic_actions_disabled=True,
                ),
                append=True,
            )
        return self._result(
            self._record(
                decision,
                context,
                outcome=ActuationOutcome.SUCCEEDED,
                attempted=True,
                success=True,
                reason="ROLLBACK_VERIFIED",
                traffic_fraction=None,
                rollback_verification=verification,
            ),
            append=True,
        )


__all__ = [
    "AUDIT_QUERY_COUNT",
    "MAX_CANARY_TRAFFIC_FRACTION",
    "ActuationAuditRecord",
    "ActuationClientLike",
    "ActuationContext",
    "ActuationOutcome",
    "ActuationResult",
    "AuditSinkLike",
    "AutomaticActionControllerLike",
    "RollbackVerification",
    "SafeActuationBoundary",
    "ShadowResult",
]
