"""R2-D independent-root-pinned response-profile evidence capability.

Issuance always reruns the complete R2-C verifier. A caller-supplied semantic
report cannot substitute for the raw bundle, and the expected root is a
separate input. The capability is predictive integrity evidence only.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
import hashlib
import hmac
import re

from .artifacts import canonical_json_bytes
from .response_profile import (
    OBSERVATION_COUNT,
    ResponseProfileCalibrationEvidence,
    ResponseProfileContractError,
    ResponseProfileIdentity,
    ResponseProfileQueryObservation,
    compute_response_profile_estimates,
)
from .response_profile_semantic import (
    ResponseProfileSemanticError,
    ResponseProfileSemanticBundle,
    ResponseProfileSemanticExpectation,
    response_profile_semantic_identity_payload,
    verify_response_profile_semantic_bundle,
)


__all__ = [
    "ROOT_PINNED_EVIDENCE_SCHEMA_VERSION",
    "ROOT_PINNED_EVIDENCE_HASH_DOMAIN",
    "ResponseProfileRootPinError",
    "RootPinnedResponseProfileEvidence",
    "issue_root_pinned_response_profile_evidence",
    "verify_root_pinned_response_profile_evidence",
    "root_pinned_response_profile_evidence_payload",
]


ROOT_PINNED_EVIDENCE_SCHEMA_VERSION = "root-pinned-response-profile-evidence-v1"
ROOT_PINNED_EVIDENCE_HASH_DOMAIN = b"VD::ROOT_PINNED_RESPONSE_PROFILE_EVIDENCE::V1\x00"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class ResponseProfileRootPinError(ValueError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileRootPinError:
    return ResponseProfileRootPinError(message, code=code)


@dataclass(frozen=True, slots=True, init=False)
class RootPinnedResponseProfileEvidence:
    schema_version: str
    semantic_report_sha256: str
    raw_evidence_sha256: str
    profile_identity_sha256: str
    profile_identity: ResponseProfileIdentity
    observations: tuple[ResponseProfileQueryObservation, ...]
    capability_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("root-pinned evidence is issued only after complete R2-C replay")


def _new(**values: object) -> RootPinnedResponseProfileEvidence:
    value = object.__new__(RootPinnedResponseProfileEvidence)
    for name, item in values.items():
        object.__setattr__(value, name, item)
    return value


def _same_fields(actual: object, expected: object) -> bool:
    if type(actual) is not type(expected):
        return False
    if hasattr(actual, "__dataclass_fields__"):
        return all(
            _same_fields(getattr(actual, item.name), getattr(expected, item.name))
            for item in fields(actual)
        )
    if type(actual) is tuple:
        return len(actual) == len(expected) and all(  # type: ignore[arg-type]
            _same_fields(left, right)
            for left, right in zip(actual, expected, strict=True)  # type: ignore[arg-type]
        )
    return bool(actual == expected)


def _sha256(value: object, *, field: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise _error("SHA256_INVALID", f"{field} must be lowercase SHA-256")
    return value


def _observation_payload(value: ResponseProfileQueryObservation) -> dict[str, object]:
    if type(value) is not ResponseProfileQueryObservation:
        raise _error("CAPABILITY_INVALID", "observation must be concrete")
    return {
        "query_id": value.query_id,
        "responses": [
            {
                "ef": item.ef,
                "capped_recall": item.capped_recall,
                "latency_ms": item.latency_ms,
            }
            for item in value.responses
        ],
    }


def _payload_from_parts(
    *,
    semantic_report_sha256: str,
    raw_evidence_sha256: str,
    identity: ResponseProfileIdentity,
    observations: tuple[ResponseProfileQueryObservation, ...],
) -> dict[str, object]:
    identity_payload = response_profile_semantic_identity_payload(identity)
    identity_sha256 = hashlib.sha256(canonical_json_bytes(identity_payload)).hexdigest()
    evidence = ResponseProfileCalibrationEvidence(
        raw_evidence_sha256=raw_evidence_sha256, observations=observations
    )
    compute_response_profile_estimates(evidence)
    return {
        "schema_version": ROOT_PINNED_EVIDENCE_SCHEMA_VERSION,
        "semantic_report_sha256": _sha256(
            semantic_report_sha256, field="semantic_report_sha256"
        ),
        "raw_evidence_sha256": _sha256(
            raw_evidence_sha256, field="raw_evidence_sha256"
        ),
        "profile_identity_sha256": identity_sha256,
        "profile_identity": identity_payload,
        "observation_count": len(observations),
        "observations": [_observation_payload(item) for item in observations],
    }


def _issue(
    *,
    semantic_report_sha256: str,
    raw_evidence_sha256: str,
    identity: ResponseProfileIdentity,
    observations: tuple[ResponseProfileQueryObservation, ...],
) -> RootPinnedResponseProfileEvidence:
    payload = _payload_from_parts(
        semantic_report_sha256=semantic_report_sha256,
        raw_evidence_sha256=raw_evidence_sha256,
        identity=identity,
        observations=observations,
    )
    if payload["observation_count"] != OBSERVATION_COUNT:
        raise _error("CAPABILITY_INVALID", "capability requires 1200 observations")
    return _new(
        schema_version=ROOT_PINNED_EVIDENCE_SCHEMA_VERSION,
        semantic_report_sha256=semantic_report_sha256,
        raw_evidence_sha256=raw_evidence_sha256,
        profile_identity_sha256=payload["profile_identity_sha256"],
        profile_identity=identity,
        observations=observations,
        capability_sha256=hashlib.sha256(
            ROOT_PINNED_EVIDENCE_HASH_DOMAIN + canonical_json_bytes(payload)
        ).hexdigest(),
    )


def issue_root_pinned_response_profile_evidence(
    *,
    bundle: ResponseProfileSemanticBundle,
    expectation: ResponseProfileSemanticExpectation,
    expected_raw_evidence_sha256: str,
) -> RootPinnedResponseProfileEvidence:
    """Rerun R2-C and issue only when the independent root pin matches."""

    expected_root = _sha256(
        expected_raw_evidence_sha256, field="expected_raw_evidence_sha256"
    )
    verification = verify_response_profile_semantic_bundle(
        bundle=bundle, expectation=expectation
    )
    if not hmac.compare_digest(expected_root, verification.raw_evidence_sha256):
        raise _error("RAW_EVIDENCE_ROOT_MISMATCH", "independent raw-evidence root mismatch")
    return _issue(
        semantic_report_sha256=verification.report.semantic_report_sha256,
        raw_evidence_sha256=verification.raw_evidence_sha256,
        identity=expectation.profile_identity,
        observations=verification.report.observations,
    )


def verify_root_pinned_response_profile_evidence(
    value: object,
    *,
    expected_raw_evidence_sha256: str,
    expected_identity: ResponseProfileIdentity,
) -> RootPinnedResponseProfileEvidence:
    if type(value) is not RootPinnedResponseProfileEvidence:
        raise _error("CAPABILITY_INVALID", "root-pinned capability must be concrete")
    expected_root = _sha256(
        expected_raw_evidence_sha256, field="expected_raw_evidence_sha256"
    )
    try:
        rebuilt = _issue(
            semantic_report_sha256=value.semantic_report_sha256,
            raw_evidence_sha256=value.raw_evidence_sha256,
            identity=value.profile_identity,
            observations=value.observations,
        )
    except (
        AttributeError,
        ResponseProfileContractError,
        ResponseProfileSemanticError,
        TypeError,
        ValueError,
    ) as exc:
        raise _error("CAPABILITY_INVALID", "root-pinned capability is malformed") from exc
    if not hmac.compare_digest(expected_root, rebuilt.raw_evidence_sha256):
        raise _error("RAW_EVIDENCE_ROOT_MISMATCH", "capability root does not match expectation")
    if response_profile_semantic_identity_payload(rebuilt.profile_identity) != response_profile_semantic_identity_payload(expected_identity):
        raise _error("PROFILE_IDENTITY_MISMATCH", "capability identity mismatch")
    if not _same_fields(value, rebuilt):
        raise _error("CAPABILITY_INVALID", "root-pinned capability failed reconstruction")
    return rebuilt


def root_pinned_response_profile_evidence_payload(
    value: RootPinnedResponseProfileEvidence,
) -> dict[str, object]:
    if type(value) is not RootPinnedResponseProfileEvidence:
        raise _error("CAPABILITY_INVALID", "root-pinned capability must be concrete")
    payload = _payload_from_parts(
        semantic_report_sha256=value.semantic_report_sha256,
        raw_evidence_sha256=value.raw_evidence_sha256,
        identity=value.profile_identity,
        observations=value.observations,
    )
    expected = hashlib.sha256(
        ROOT_PINNED_EVIDENCE_HASH_DOMAIN + canonical_json_bytes(payload)
    ).hexdigest()
    if not hmac.compare_digest(value.capability_sha256, expected):
        raise _error("CAPABILITY_INVALID", "capability digest mismatch")
    return payload
