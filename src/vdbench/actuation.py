"""Thin, dependency-injected rollback and audit boundary for ADR-002.

Purpose:
    Permanently refuse generic START_CANARY, execute policy-authorized ROLLBACK,
    record immutable outcomes, and leave detector/policy decisions outside this
    module. Candidate serving is owned exclusively by Stage4LiveRunner.
Inputs:
    A frozen PolicyDecision, qualification-free identity/rollback context,
    client-like executor, append-only audit sink, and automatic-action controller.
Outputs:
    Immutable ActuationResult and ActuationAuditRecord values.
Dependencies:
    Protocols and offline policy value objects only; never PyMilvus.
Failure modes:
    Missing/duplicate audit identity, retired generic candidate starts, invalid
    rollback context/ef, client failures, and failed verification fail closed.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol, TypeAlias

from .config import THRESHOLD_LABELS, Metric
from .drift import EvidenceProvenance
from .policy import (
    ACTUATION_LADDER,
    CanaryObservation,
    PolicyAction,
    PolicyDecision,
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
class ActuationIdentityContext:
    """Immutable common identity projection with no qualification authority."""

    metric: Metric | str
    threshold_stratum: str
    collection_name: str
    configuration_identity: str
    index_identity: str
    flat_index_identity: str
    data_identity: str
    occurred_at_utc: str


@dataclass(frozen=True, slots=True)
class ShadowActuationContext(ActuationIdentityContext):
    """Read-only shadow identity plus exactly 50 canonical audit query IDs."""

    audited_query_ids: tuple[QueryId, ...]


@dataclass(frozen=True, slots=True)
class RollbackActuationContext(ActuationIdentityContext):
    """Rollback identity, expected ef, and 50 restoration-audit query IDs."""

    expected_last_known_good_ef: int
    audited_query_ids: tuple[QueryId, ...]


@dataclass(frozen=True, slots=True)
class ActuationAuditRecord:
    """Immutable append-only record of one boundary invocation."""

    audit_id: str
    action: PolicyAction
    outcome: ActuationOutcome
    attempted: bool
    success: bool
    reason: str
    context: ActuationIdentityContext
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
        context: ShadowActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult: ...

    def stop_candidate(self) -> None: ...

    def restore_last_known_good(self, ef: int) -> None: ...

    def verify_restoration(
        self,
        *,
        context: RollbackActuationContext,
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
        parsed = datetime.fromisoformat(value)
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
        if isinstance(value, bool) or type(value) not in {int, str}:
            return False
        if isinstance(value, str):
            normalized = unicodedata.normalize("NFC", value)
            if not normalized or normalized != value:
                return False
        key = (type(value), value)
        if key in canonical:
            return False
        canonical.add(key)
    return True


def _identity_context_failure(context: object) -> str | None:
    if not isinstance(context, ActuationIdentityContext):
        return "ACTUATION_IDENTITY_CONTEXT_INVALID"
    try:
        metric_value = context.metric
        threshold_stratum = context.threshold_stratum
        identity_values = (
            context.collection_name,
            context.configuration_identity,
            context.index_identity,
            context.flat_index_identity,
            context.data_identity,
        )
        occurred_at_utc = context.occurred_at_utc
    except AttributeError:
        return "ACTUATION_IDENTITY_CONTEXT_INVALID"
    metric = _metric(metric_value)
    if metric is None:
        return "ACTUATION_METRIC_INVALID"
    if threshold_stratum not in THRESHOLD_LABELS:
        return "ACTUATION_THRESHOLD_STRATUM_INVALID"
    if not all(_nonempty(value) for value in identity_values):
        return "ACTUATION_IDENTITY_MISSING"
    if not _valid_rfc3339_utc(occurred_at_utc):
        return "ACTUATION_TIMESTAMP_INVALID"
    return None


def _shadow_context_failure(context: object) -> str | None:
    if type(context) is not ShadowActuationContext:
        return "SHADOW_ACTUATION_CONTEXT_INVALID"
    identity_failure = _identity_context_failure(context)
    if identity_failure is not None:
        return identity_failure
    try:
        audited_query_ids = context.audited_query_ids
    except AttributeError:
        return "SHADOW_ACTUATION_CONTEXT_INVALID"
    if not _valid_query_ids(audited_query_ids):
        return "SHADOW_AUDIT_QUERY_IDS_INVALID"
    return None


def validate_shadow_actuation_context(
    context: object,
) -> ShadowActuationContext:
    """Return one complete shadow context or raise its stable refusal reason."""

    failure = _shadow_context_failure(context)
    if failure is not None:
        raise ValueError(failure)
    assert type(context) is ShadowActuationContext
    return context


def _rollback_context_failure(context: object) -> str | None:
    """Validate explicit runtime rollback context, never qualification evidence."""

    if type(context) is not RollbackActuationContext:
        return "ROLLBACK_ACTUATION_CONTEXT_INVALID"
    identity_failure = _identity_context_failure(context)
    if identity_failure is not None:
        return identity_failure
    try:
        audited_query_ids = context.audited_query_ids
        expected_ef = context.expected_last_known_good_ef
    except AttributeError:
        return "ROLLBACK_ACTUATION_CONTEXT_INVALID"
    if not _valid_query_ids(audited_query_ids):
        return "ROLLBACK_AUDIT_QUERY_IDS_INVALID"
    if _rollback_ef_failure(expected_ef) is not None:
        return "ROLLBACK_CONTEXT_LAST_KNOWN_GOOD_EF_INVALID"
    return None


def validate_rollback_actuation_context(
    context: object,
) -> RollbackActuationContext:
    """Return one complete rollback context or raise its stable refusal reason."""

    failure = _rollback_context_failure(context)
    if failure is not None:
        raise ValueError(failure)
    assert type(context) is RollbackActuationContext
    return context


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
    context: RollbackActuationContext,
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
        context: ActuationIdentityContext,
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
        context: ActuationIdentityContext,
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
        context: ActuationIdentityContext,
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
        assert type(context) is RollbackActuationContext
        if context.expected_last_known_good_ef != decision.last_known_good_ef:
            return self._blocked(
                decision,
                context,
                reason="ROLLBACK_LAST_KNOWN_GOOD_EF_MISMATCH",
                traffic_fraction=None,
                append=True,
            )
        return self._rollback(decision, context)

    def _rollback(
        self,
        decision: PolicyDecision,
        context: RollbackActuationContext,
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
        except Exception as exc:  # rollback exception is always fail-closed  # noqa: BLE001
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
    "ActuationIdentityContext",
    "ActuationOutcome",
    "ActuationResult",
    "AuditSinkLike",
    "AutomaticActionControllerLike",
    "RollbackActuationContext",
    "RollbackVerification",
    "SafeActuationBoundary",
    "ShadowActuationContext",
    "ShadowResult",
    "validate_rollback_actuation_context",
    "validate_shadow_actuation_context",
]
