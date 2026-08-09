"""Thin, dependency-injected rollback and audit boundary for ADR-002.

Purpose:
    Permanently refuse generic START_CANARY, execute policy-authorized ROLLBACK,
    record immutable outcomes, and leave detector/policy decisions outside this
    module. Candidate serving is owned exclusively by Stage4LiveRunner.
Inputs:
    A frozen PolicyDecision, ActuationContext, client-like executor, append-only
    audit sink, and automatic-action controller.
Outputs:
    Immutable ActuationResult and ActuationAuditRecord values.
Dependencies:
    Protocols and offline policy value objects only; never PyMilvus.
Failure modes:
    Missing/duplicate audit identity, retired generic candidate starts, invalid
    rollback context/ef, client failures, and failed verification fail closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import re
from typing import Protocol, TypeAlias

from .config import Metric, THRESHOLD_LABELS
from .drift import EvidenceProvenance
from .policy import (
    ACTUATION_LADDER,
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
    flat_index_identity: str
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
    evidence_provenance: EvidenceProvenance | None = None


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
    """Read-only shadow and rollback adapter; never candidate-start authority."""

    def shadow_candidate(
        self,
        *,
        context: ActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult: ...

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


def _rollback_context_failure(context: ActuationContext) -> str | None:
    """Validate explicit runtime rollback context, never legacy qualification."""

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
            context.flat_index_identity,
            context.data_identity,
        )
    ):
        return "ACTUATION_IDENTITY_MISSING"
    if not _valid_query_ids(context.audited_query_ids):
        return "ACTUATION_AUDIT_QUERY_IDS_INVALID"
    if not _valid_rfc3339_utc(context.occurred_at_utc):
        return "ACTUATION_TIMESTAMP_INVALID"
    return None


def _rollback_ef_failure(value: object) -> str | None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value not in ACTUATION_LADDER
    ):
        return "ROLLBACK_LAST_KNOWN_GOOD_EF_INVALID"
    return None


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
            evidence_provenance=decision.evidence_provenance,
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

        if decision.action is PolicyAction.START_CANARY:
            return self._blocked(
                decision,
                context,
                reason="GENERIC_START_CANARY_RETIRED",
                traffic_fraction=None,
                append=True,
            )

        context_failure = _rollback_context_failure(context)
        if context_failure is not None:
            return self._blocked(
                decision,
                context,
                reason=context_failure,
                traffic_fraction=None,
                append=True,
            )

        ef_failure = _rollback_ef_failure(decision.last_known_good_ef)
        if ef_failure is not None:
            return self._blocked(
                decision,
                context,
                reason=ef_failure,
                traffic_fraction=None,
                append=True,
            )
        return self._rollback(decision, context)

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
