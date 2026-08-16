"""Offline signed-approval verification for EXP-009 Stage 2.

Purpose:
    Verify one canonical, externally signed human approval grant against the
    exact immutable policy decision and Stage-1 workload bindings it permits.
Inputs:
    An untrusted strict JSON grant document, injected Ed25519 public-key trust
    store, current UTC instant, policy decision, and expected artifact digests.
Outputs:
    An immutable approval/refusal result.  This module cannot install a route,
    persist state, open a network connection, or issue a Milvus operation.
Dependencies:
    ``cryptography``'s Ed25519 public-key API, plus local immutable values.
Failure modes:
    Every malformed, unsigned, expired, revoked, mismatched, or invalid grant
    returns a stable non-sensitive refusal code and no approval object.

The private signing key is intentionally outside this module and this repository.
Tests generate an ephemeral in-memory key only to exercise public verification.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timedelta
from enum import Enum
from types import MappingProxyType
from typing import Any, Protocol

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from .config import HNSW_EF_SWEEP, THRESHOLD_LABELS, Metric
from .drift import evidence_provenance_valid
from .policy import PolicyAction, PolicyDecision, PolicyMode

__all__ = [
    "APPROVAL_GRANT_SCHEMA_VERSION",
    "APPROVAL_SIGNATURE_ALGORITHM",
    "ApprovalVerificationContext",
    "ApprovalVerificationResult",
    "CanaryApprovalGrant",
    "CanaryApprovalTrustStore",
    "StaticCanaryApprovalTrustStore",
    "approval_grant_from_bytes",
    "approval_grant_signing_bytes",
    "approval_grant_to_bytes",
    "policy_decision_sha256",
    "verify_canary_approval_grant",
]


APPROVAL_GRANT_SCHEMA_VERSION = "canary-approval-grant-v1"
APPROVAL_SIGNATURE_ALGORITHM = "Ed25519"
_SIGNING_DOMAIN = b"vdbench.canary-approval/v1\0"
_POLICY_BINDING_SCHEMA_VERSION = "policy-decision-binding-v1"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_BASE64URL_RE = re.compile(r"[A-Za-z0-9_-]+\Z")
_ACTUATION_LADDER = tuple(value for value in HNSW_EF_SWEEP if value != 100)
_FLOAT_TAG = "__vdbench_float64_hex__"
_GRANT_UNSIGNED_FIELDS = (
    "grant_id",
    "key_id",
    "issued_at_utc",
    "expires_at_utc",
    "experiment_id",
    "policy_decision_sha256",
    "policy_audit_id",
    "metric",
    "threshold_stratum",
    "current_ef",
    "candidate_ef",
    "last_known_good_ef",
    "configuration_identity",
    "data_identity",
    "flat_binding_id",
    "hnsw_binding_id",
    "eligible_workload_sha256",
    "candidate_selection_sha256",
    "routing_population_count",
    "candidate_count",
    "maximum_fraction",
    "rollback_pre_authorized",
)
_GRANT_DOCUMENT_FIELDS = frozenset(
    {
        "schema_version",
        *_GRANT_UNSIGNED_FIELDS,
        "signature_algorithm",
        "signature",
    }
)
_POLICY_DECISION_FIELDS = (
    "action",
    "current_ef",
    "candidate_ef",
    "last_known_good_ef",
    "expected_mean_recall",
    "expected_recall_lower_bound_95",
    "expected_p95_latency_ms",
    "expected_latency_upper_bound_95_ms",
    "predicted_recall_improvement",
    "predicted_latency_reduction_fraction",
    "reason",
    "detector_confidence",
    "detector_magnitude",
    "safety_gate_results",
    "mode",
    "audit_id",
    "alert_required",
    "evidence_provenance",
)


class _ApprovalValidationError(ValueError):
    """Private carrier for stable public refusal codes."""


class _DuplicateJsonField(ValueError):
    """Raised only while rejecting non-canonical JSON input."""


@dataclass(frozen=True, slots=True)
class CanaryApprovalGrant:
    """One externally signed, exact-scope candidate-route authorization.

    ``signature`` is absent only while an external operator serializes the
    signing bytes.  It must be present for persistence or verification.
    """

    grant_id: str
    key_id: str
    issued_at_utc: str
    expires_at_utc: str
    experiment_id: str
    policy_decision_sha256: str
    policy_audit_id: str
    metric: Metric | str
    threshold_stratum: str
    current_ef: int
    candidate_ef: int
    last_known_good_ef: int
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str
    eligible_workload_sha256: str
    candidate_selection_sha256: str
    routing_population_count: int
    candidate_count: int
    maximum_fraction: float
    rollback_pre_authorized: bool
    signature_algorithm: str = APPROVAL_SIGNATURE_ALGORITHM
    signature: str | None = None


@dataclass(frozen=True, slots=True)
class ApprovalVerificationContext:
    """Runtime values the signed grant must match exactly.

    The future coordinator must obtain both artifact digests by independently
    verifying the persisted Stage-1 manifest and selection record; callers may
    not substitute unverified values after this boundary.
    """

    decision: PolicyDecision
    expected_experiment_id: str
    eligible_workload_sha256: str
    candidate_selection_sha256: str
    now_utc: str


@dataclass(frozen=True, slots=True)
class ApprovalVerificationResult:
    """Fail-closed verification result suitable for a route coordinator."""

    approved: bool
    reason_code: str | None
    grant: CanaryApprovalGrant | None = None
    grant_sha256: str | None = None


class CanaryApprovalTrustStore(Protocol):
    """Injected public-key and revocation authority; never holds private keys."""

    def public_key_for(self, key_id: str) -> Ed25519PublicKey | None: ...

    def is_key_revoked(self, key_id: str) -> bool: ...

    def is_grant_revoked(self, grant_id: str) -> bool: ...


class StaticCanaryApprovalTrustStore:
    """Immutable in-memory trust store for tests and offline composition."""

    def __init__(
        self,
        *,
        public_keys: Mapping[str, Ed25519PublicKey],
        revoked_key_ids: frozenset[str] = frozenset(),
        revoked_grant_ids: frozenset[str] = frozenset(),
    ) -> None:
        normalized: dict[str, Ed25519PublicKey] = {}
        for key_id, public_key in dict(public_keys).items():
            normalized_key_id = _canonical_text(key_id, field="key_id")
            if not isinstance(public_key, Ed25519PublicKey):
                raise TypeError("public_keys values must be Ed25519PublicKey")
            if normalized_key_id in normalized:
                raise ValueError("public_keys contains duplicate canonical key_id")
            normalized[normalized_key_id] = public_key
        self._public_keys = MappingProxyType(normalized)
        self._revoked_key_ids = frozenset(
            _canonical_text(value, field="revoked_key_id")
            for value in revoked_key_ids
        )
        self._revoked_grant_ids = frozenset(
            _canonical_text(value, field="revoked_grant_id")
            for value in revoked_grant_ids
        )

    def public_key_for(self, key_id: str) -> Ed25519PublicKey | None:
        return self._public_keys.get(key_id)

    def is_key_revoked(self, key_id: str) -> bool:
        return key_id in self._revoked_key_ids

    def is_grant_revoked(self, grant_id: str) -> bool:
        return grant_id in self._revoked_grant_ids


def _canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise _ApprovalValidationError(f"{field.upper()}_INVALID")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or normalized != value
        or value.strip() != value
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise _ApprovalValidationError(f"{field.upper()}_INVALID")
    return normalized


def _canonical_sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise _ApprovalValidationError(f"{field.upper()}_INVALID")
    return value


def _parse_utc(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or _RFC3339_UTC_RE.fullmatch(value) is None:
        raise _ApprovalValidationError(f"{field.upper()}_INVALID")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise _ApprovalValidationError(f"{field.upper()}_INVALID") from exc
    if parsed.utcoffset() != timedelta(0):
        raise _ApprovalValidationError(f"{field.upper()}_INVALID")
    return parsed


def _canonical_metric(value: object, *, field: str) -> Metric:
    try:
        return Metric(value)
    except (TypeError, ValueError) as exc:
        raise _ApprovalValidationError(f"{field.upper()}_INVALID") from exc


def _canonical_int(value: object, *, field: str, allowed: tuple[int, ...]) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value not in allowed:
        raise _ApprovalValidationError(f"{field.upper()}_INVALID")
    return value


def _signature_bytes(value: object) -> bytes:
    if not isinstance(value, str) or _BASE64URL_RE.fullmatch(value) is None:
        raise _ApprovalValidationError("GRANT_SIGNATURE_INVALID")
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (ValueError, UnicodeError) as exc:
        raise _ApprovalValidationError("GRANT_SIGNATURE_INVALID") from exc
    if (
        len(decoded) != 64
        or base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=") != value
    ):
        raise _ApprovalValidationError("GRANT_SIGNATURE_INVALID")
    return decoded


def _validate_grant(grant: object, *, require_signature: bool) -> CanaryApprovalGrant:
    if not isinstance(grant, CanaryApprovalGrant):
        raise _ApprovalValidationError("GRANT_TYPE_INVALID")
    _canonical_text(grant.grant_id, field="grant_id")
    _canonical_text(grant.key_id, field="key_id")
    issued = _parse_utc(grant.issued_at_utc, field="issued_at_utc")
    expires = _parse_utc(grant.expires_at_utc, field="expires_at_utc")
    if expires <= issued:
        raise _ApprovalValidationError("GRANT_TIME_RANGE_INVALID")
    if grant.experiment_id != "EXP-009":
        raise _ApprovalValidationError("EXPERIMENT_ID_INVALID")
    _canonical_sha256(grant.policy_decision_sha256, field="policy_decision_sha256")
    _canonical_text(grant.policy_audit_id, field="policy_audit_id")
    metric = _canonical_metric(grant.metric, field="metric")
    if grant.threshold_stratum not in THRESHOLD_LABELS:
        raise _ApprovalValidationError("THRESHOLD_STRATUM_INVALID")
    current_ef = _canonical_int(
        grant.current_ef, field="current_ef", allowed=_ACTUATION_LADDER
    )
    candidate_ef = _canonical_int(
        grant.candidate_ef, field="candidate_ef", allowed=_ACTUATION_LADDER
    )
    _canonical_int(
        grant.last_known_good_ef,
        field="last_known_good_ef",
        allowed=_ACTUATION_LADDER,
    )
    if abs(_ACTUATION_LADDER.index(current_ef) - _ACTUATION_LADDER.index(candidate_ef)) != 1:
        raise _ApprovalValidationError("TRANSITION_INVALID")
    for field in (
        "configuration_identity",
        "data_identity",
        "flat_binding_id",
        "hnsw_binding_id",
    ):
        _canonical_text(getattr(grant, field), field=field)
    _canonical_sha256(grant.eligible_workload_sha256, field="eligible_workload_sha256")
    _canonical_sha256(grant.candidate_selection_sha256, field="candidate_selection_sha256")
    if grant.routing_population_count != 600:
        raise _ApprovalValidationError("ROUTING_POPULATION_COUNT_INVALID")
    if grant.candidate_count != 60:
        raise _ApprovalValidationError("CANDIDATE_COUNT_INVALID")
    if (
        isinstance(grant.maximum_fraction, bool)
        or not isinstance(grant.maximum_fraction, (int, float))
        or not math.isfinite(float(grant.maximum_fraction))
        or float(grant.maximum_fraction) != 0.10
    ):
        raise _ApprovalValidationError("MAXIMUM_FRACTION_INVALID")
    if grant.rollback_pre_authorized is not True:
        raise _ApprovalValidationError("ROLLBACK_PREAUTHORIZATION_REQUIRED")
    if grant.signature_algorithm != APPROVAL_SIGNATURE_ALGORITHM:
        raise _ApprovalValidationError("SIGNATURE_ALGORITHM_UNSUPPORTED")
    if require_signature or grant.signature is not None:
        _signature_bytes(grant.signature)
    return CanaryApprovalGrant(
        grant_id=grant.grant_id,
        key_id=grant.key_id,
        issued_at_utc=grant.issued_at_utc,
        expires_at_utc=grant.expires_at_utc,
        experiment_id=grant.experiment_id,
        policy_decision_sha256=grant.policy_decision_sha256,
        policy_audit_id=grant.policy_audit_id,
        metric=metric,
        threshold_stratum=grant.threshold_stratum,
        current_ef=current_ef,
        candidate_ef=candidate_ef,
        last_known_good_ef=grant.last_known_good_ef,
        configuration_identity=grant.configuration_identity,
        data_identity=grant.data_identity,
        flat_binding_id=grant.flat_binding_id,
        hnsw_binding_id=grant.hnsw_binding_id,
        eligible_workload_sha256=grant.eligible_workload_sha256,
        candidate_selection_sha256=grant.candidate_selection_sha256,
        routing_population_count=grant.routing_population_count,
        candidate_count=grant.candidate_count,
        maximum_fraction=float(grant.maximum_fraction),
        rollback_pre_authorized=True,
        signature_algorithm=grant.signature_algorithm,
        signature=grant.signature,
    )


def _canonicalize(value: object) -> object:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, Enum):
        return _canonicalize(value.value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite value cannot be signed")
        return {_FLOAT_TAG: value.hex()}
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _canonicalize(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("signed mappings must use string keys")
            normalized_key = unicodedata.normalize("NFC", key)
            if normalized_key in normalized:
                raise ValueError("signed mapping has duplicate normalized keys")
            normalized[normalized_key] = _canonicalize(item)
        return normalized
    if isinstance(value, (tuple, list)):
        return [_canonicalize(item) for item in value]
    raise TypeError(f"unsupported signed value type: {type(value).__name__}")


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            _canonicalize(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _strict_document_json_bytes(value: object) -> bytes:
    """Encode a persisted document without changing its public field types."""

    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _policy_decision_document(decision: PolicyDecision) -> dict[str, object]:
    if not isinstance(decision, PolicyDecision):
        raise TypeError("decision must be a PolicyDecision")
    actual_fields = tuple(field.name for field in fields(PolicyDecision))
    if actual_fields != _POLICY_DECISION_FIELDS:
        raise RuntimeError("PolicyDecision schema changed; update binding schema")
    return {
        "schema_version": _POLICY_BINDING_SCHEMA_VERSION,
        "policy_decision": {
            field: _canonicalize(getattr(decision, field))
            for field in _POLICY_DECISION_FIELDS
        },
    }


def policy_decision_sha256(decision: PolicyDecision) -> str:
    """Return the strict canonical digest that a grant must bind."""

    return hashlib.sha256(_canonical_json_bytes(_policy_decision_document(decision))).hexdigest()


def _raw_unsigned_grant_document(grant: CanaryApprovalGrant) -> dict[str, object]:
    validated = _validate_grant(grant, require_signature=False)
    return {
        field: (
            getattr(validated, field).value
            if field == "metric"
            else getattr(validated, field)
        )
        for field in _GRANT_UNSIGNED_FIELDS
    }


def _unsigned_grant_document(grant: CanaryApprovalGrant) -> dict[str, object]:
    document = _canonicalize(_raw_unsigned_grant_document(grant))
    if not isinstance(document, dict):  # invariant: the input is a mapping
        raise RuntimeError("canonical grant document is not an object")  # domain error type carries the governed reason code  # noqa: TRY004
    return document


def approval_grant_signing_bytes(grant: CanaryApprovalGrant) -> bytes:
    """Return the sole versioned byte sequence an external operator signs."""

    return _SIGNING_DOMAIN + _canonical_json_bytes(_unsigned_grant_document(grant))


def approval_grant_to_bytes(grant: CanaryApprovalGrant) -> bytes:
    """Encode one signed grant as strict canonical JSON for immutable storage."""

    validated = _validate_grant(grant, require_signature=True)
    document = {
        "schema_version": APPROVAL_GRANT_SCHEMA_VERSION,
        **_raw_unsigned_grant_document(validated),
        "signature_algorithm": validated.signature_algorithm,
        "signature": validated.signature,
    }
    return _strict_document_json_bytes(document)


def _no_duplicate_json_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(key)
        result[key] = value
    return result


def approval_grant_from_bytes(payload: bytes) -> CanaryApprovalGrant:
    """Decode one strict signed-grant document, rejecting duplicate/extra fields."""

    if not isinstance(payload, bytes):
        raise ValueError("approval grant payload must be bytes")  # domain error type carries the governed reason code  # noqa: TRY004
    try:
        parsed = json.loads(
            payload.decode("utf-8"), object_pairs_hook=_no_duplicate_json_fields
        )
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonField) as exc:
        raise ValueError("approval grant document is malformed") from exc
    if not isinstance(parsed, Mapping) or frozenset(parsed) != _GRANT_DOCUMENT_FIELDS:
        raise ValueError("approval grant document has invalid schema")
    if parsed["schema_version"] != APPROVAL_GRANT_SCHEMA_VERSION:
        raise ValueError("approval grant document has unsupported schema")
    try:
        grant = CanaryApprovalGrant(
            grant_id=parsed["grant_id"],
            key_id=parsed["key_id"],
            issued_at_utc=parsed["issued_at_utc"],
            expires_at_utc=parsed["expires_at_utc"],
            experiment_id=parsed["experiment_id"],
            policy_decision_sha256=parsed["policy_decision_sha256"],
            policy_audit_id=parsed["policy_audit_id"],
            metric=Metric(parsed["metric"]),
            threshold_stratum=parsed["threshold_stratum"],
            current_ef=parsed["current_ef"],
            candidate_ef=parsed["candidate_ef"],
            last_known_good_ef=parsed["last_known_good_ef"],
            configuration_identity=parsed["configuration_identity"],
            data_identity=parsed["data_identity"],
            flat_binding_id=parsed["flat_binding_id"],
            hnsw_binding_id=parsed["hnsw_binding_id"],
            eligible_workload_sha256=parsed["eligible_workload_sha256"],
            candidate_selection_sha256=parsed["candidate_selection_sha256"],
            routing_population_count=parsed["routing_population_count"],
            candidate_count=parsed["candidate_count"],
            maximum_fraction=parsed["maximum_fraction"],
            rollback_pre_authorized=parsed["rollback_pre_authorized"],
            signature_algorithm=parsed["signature_algorithm"],
            signature=parsed["signature"],
        )
        validated = _validate_grant(grant, require_signature=True)
    except (KeyError, TypeError, ValueError, _ApprovalValidationError) as exc:
        raise ValueError("approval grant document is invalid") from exc
    if payload != approval_grant_to_bytes(validated):
        raise ValueError("approval grant document is noncanonical")
    return validated


def _refused(code: str) -> ApprovalVerificationResult:
    return ApprovalVerificationResult(approved=False, reason_code=code)


def _validate_context(context: object) -> ApprovalVerificationContext:
    if not isinstance(context, ApprovalVerificationContext):
        raise _ApprovalValidationError("VERIFICATION_CONTEXT_INVALID")
    if not isinstance(context.decision, PolicyDecision):
        raise _ApprovalValidationError("POLICY_DECISION_INVALID")
    if context.expected_experiment_id != "EXP-009":
        raise _ApprovalValidationError("EXPECTED_EXPERIMENT_INVALID")
    _canonical_sha256(context.eligible_workload_sha256, field="eligible_workload_sha256")
    _canonical_sha256(context.candidate_selection_sha256, field="candidate_selection_sha256")
    _parse_utc(context.now_utc, field="verification_time")
    return context


def _verify_policy_binding(
    grant: CanaryApprovalGrant,
    context: ApprovalVerificationContext,
) -> str | None:
    decision = context.decision
    if (
        decision.action is not PolicyAction.START_CANARY
        or decision.mode is not PolicyMode.CANARY_ENABLED
    ):
        return "POLICY_DECISION_NOT_CANARY"
    try:
        decision_digest = policy_decision_sha256(decision)
    except (RuntimeError, TypeError, ValueError):
        return "POLICY_DECISION_INVALID"
    if grant.policy_decision_sha256 != decision_digest:
        return "POLICY_DECISION_MISMATCH"
    if grant.policy_audit_id != decision.audit_id:
        return "POLICY_AUDIT_ID_MISMATCH"
    if (
        decision.candidate_ef is None
        or decision.last_known_good_ef is None
        or grant.current_ef != decision.current_ef
        or grant.candidate_ef != decision.candidate_ef
        or grant.last_known_good_ef != decision.last_known_good_ef
    ):
        return "POLICY_TRANSITION_MISMATCH"
    if not decision.safety_gate_results or any(
        getattr(gate, "passed", None) is not True
        for gate in decision.safety_gate_results
    ):
        return "POLICY_SAFETY_GATES_FAILED"
    provenance = decision.evidence_provenance
    if provenance is None or not evidence_provenance_valid(provenance):
        return "POLICY_PROVENANCE_INVALID"
    if grant.metric is not provenance.metric:
        return "METRIC_MISMATCH"
    if grant.threshold_stratum != provenance.threshold_stratum:
        return "THRESHOLD_STRATUM_MISMATCH"
    identity_fields = (
        ("CONFIGURATION", grant.configuration_identity, provenance.configuration_identity),
        ("DATA", grant.data_identity, provenance.data_identity),
        ("FLAT", grant.flat_binding_id, provenance.flat_binding_id),
        ("HNSW", grant.hnsw_binding_id, provenance.hnsw_binding_id),
    )
    for name, actual, expected in identity_fields:
        if actual != expected:
            return f"{name}_IDENTITY_MISMATCH"
    return None


def verify_canary_approval_grant(
    grant: CanaryApprovalGrant | None,
    *,
    trust_store: CanaryApprovalTrustStore,
    context: ApprovalVerificationContext,
) -> ApprovalVerificationResult:
    """Verify a signed exact-scope approval without any route side effect."""

    if grant is None:
        return _refused("GRANT_MISSING")
    try:
        validated_context = _validate_context(context)
        validated = _validate_grant(grant, require_signature=True)
    except _ApprovalValidationError as exc:
        return _refused(str(exc))
    try:
        if trust_store.is_key_revoked(validated.key_id):
            return _refused("SIGNING_KEY_REVOKED")
        if trust_store.is_grant_revoked(validated.grant_id):
            return _refused("GRANT_REVOKED")
        public_key = trust_store.public_key_for(validated.key_id)
    except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
        return _refused("TRUST_STORE_UNAVAILABLE")
    if not isinstance(public_key, Ed25519PublicKey):
        return _refused("SIGNING_KEY_UNKNOWN")
    try:
        public_key.verify(
            _signature_bytes(validated.signature),
            approval_grant_signing_bytes(validated),
        )
    except (InvalidSignature, _ApprovalValidationError, ValueError):
        return _refused("GRANT_SIGNATURE_INVALID")

    now = _parse_utc(validated_context.now_utc, field="verification_time")
    issued = _parse_utc(validated.issued_at_utc, field="issued_at_utc")
    expires = _parse_utc(validated.expires_at_utc, field="expires_at_utc")
    if now < issued:
        return _refused("GRANT_NOT_YET_VALID")
    if now >= expires:
        return _refused("GRANT_EXPIRED")
    if validated.experiment_id != validated_context.expected_experiment_id:
        return _refused("EXPERIMENT_ID_MISMATCH")
    if validated.eligible_workload_sha256 != validated_context.eligible_workload_sha256:
        return _refused("ELIGIBLE_WORKLOAD_MISMATCH")
    if validated.candidate_selection_sha256 != validated_context.candidate_selection_sha256:
        return _refused("CANDIDATE_SELECTION_MISMATCH")
    policy_failure = _verify_policy_binding(validated, validated_context)
    if policy_failure is not None:
        return _refused(policy_failure)
    return ApprovalVerificationResult(
        approved=True,
        reason_code=None,
        grant=validated,
        grant_sha256=hashlib.sha256(approval_grant_to_bytes(validated)).hexdigest(),
    )
