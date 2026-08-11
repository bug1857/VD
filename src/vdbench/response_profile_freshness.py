"""ADR-010 response-profile binding to one verified latest detector-head snapshot.

This pure boundary binds predictive evidence to the detector head observed as
latest during one store refresh.  It neither proves that the snapshot remains
latest after return nor authorizes policy, admission, routing, or actuation.
Candidate-capable composition must reacquire the store-issued head immediately
before each governed use and remains disabled pending reviewed EXP-010 evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
import hashlib
import hmac
import re

from .artifacts import canonical_json_bytes
from .response_profile import (
    CalibratedResponseProfile,
    ResponseProfileCalibrationEvidence,
    verify_calibrated_response_profile,
)
from .response_profile_control import (
    ResponseProfileControl,
    response_profile_control_payload,
    verify_response_profile_control,
)
from .response_profile_detector_head import verify_response_profile_detector_head
from .response_profile_monitor_store import VerifiedLatestResponseProfileDetectorHead
from .response_profile_root_pin import (
    RootPinnedResponseProfileEvidence,
    verify_root_pinned_response_profile_evidence,
)
from .lkg_window_readiness import parse_rfc3339_utc_instant, validate_rfc3339_utc


FRESH_EVIDENCE_SCHEMA_VERSION = "fresh-response-profile-evidence-v1"
FRESH_EVIDENCE_HASH_DOMAIN = b"VD::FRESH_RESPONSE_PROFILE_EVIDENCE::V1\x00"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

__all__ = [
    "FRESH_EVIDENCE_SCHEMA_VERSION",
    "FRESH_EVIDENCE_HASH_DOMAIN",
    "ResponseProfileFreshnessError",
    "FreshResponseProfileEvidence",
    "bind_fresh_response_profile_evidence",
    "verify_fresh_response_profile_evidence",
    "fresh_response_profile_evidence_payload",
]


class ResponseProfileFreshnessError(ValueError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileFreshnessError:
    return ResponseProfileFreshnessError(message, code=code)


def _sha(value: object, *, field: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _error("FRESH_EVIDENCE_INVALID", f"{field} must be lowercase SHA-256")
    return value


@dataclass(frozen=True, slots=True, init=False)
class FreshResponseProfileEvidence:
    """Historical proof of one verified refresh instant; never authority.

    The value does not remain fresh after return and cannot authorize policy,
    qualification, admission, grants, activation, routing, or execution.
    """
    schema_version: str
    root_pinned_capability: RootPinnedResponseProfileEvidence
    profile: CalibratedResponseProfile
    control: ResponseProfileControl
    verified_latest_detector_head: VerifiedLatestResponseProfileDetectorHead
    fresh_evidence_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("fresh response-profile evidence must be built by the binder")


def _new(**values: object) -> FreshResponseProfileEvidence:
    result = object.__new__(FreshResponseProfileEvidence)
    for name, value in values.items():
        object.__setattr__(result, name, value)
    return result


def _validated_parts(
    *,
    capability: object,
    profile: object,
    control: object,
    verified_latest_detector_head: object,
) -> tuple[
    RootPinnedResponseProfileEvidence,
    CalibratedResponseProfile,
    ResponseProfileControl,
    VerifiedLatestResponseProfileDetectorHead,
]:
    if type(capability) is not RootPinnedResponseProfileEvidence:
        raise _error("ROOT_PINNED_CAPABILITY_INVALID", "root capability must be concrete")
    if type(profile) is not CalibratedResponseProfile:
        raise _error("RESPONSE_PROFILE_INVALID", "response profile must be concrete")
    try:
        verified_control = verify_response_profile_control(control)
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("CONTROL_PROFILE_INVALID", "control is invalid") from exc
    if type(verified_latest_detector_head) is not VerifiedLatestResponseProfileDetectorHead:
        raise _error("LATEST_DETECTOR_HEAD_REQUIRED", "store-issued latest detector head is required")
    try:
        latest = verified_latest_detector_head
        head = verify_response_profile_detector_head(latest.head)
        if type(latest.head_record_sequence) is not int or latest.head_record_sequence < 0:
            raise ValueError("head record sequence invalid")
        _sha(latest.head_record_sha256, field="head_record_sha256")
        validate_rfc3339_utc(
            latest.head_record_persisted_at_utc,
            field="head_record_persisted_at_utc",
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("LATEST_DETECTOR_HEAD_INVALID", "latest detector head is malformed") from exc
    try:
        verified_capability = verify_root_pinned_response_profile_evidence(
            capability,
            expected_raw_evidence_sha256=capability.raw_evidence_sha256,
            expected_identity=capability.profile_identity,
        )
        evidence = ResponseProfileCalibrationEvidence(
            raw_evidence_sha256=verified_capability.raw_evidence_sha256,
            observations=verified_capability.observations,
        )
        verified_profile = verify_calibrated_response_profile(
            profile=profile,
            identity=verified_capability.profile_identity,
            evidence=evidence,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("RESPONSE_PROFILE_INVALID", "profile/capability verification failed") from exc
    provenance = head.detector_provenance
    if (
        head.stream_key != verified_control.stream_key
        or head.window_sequence != verified_control.trigger_window_sequence
        or provenance != verified_control.detector_provenance
        or head.detector_head_sha256 != verified_control.detector_head_sha256
        or latest.head_record_sequence
        != verified_control.detector_head_record_sequence
        or latest.head_record_sha256
        != verified_control.detector_head_record_sha256
        or latest.head_record_persisted_at_utc
        != verified_control.detector_head_persisted_at_utc
    ):
        raise _error("DETECTOR_HEAD_MISMATCH", "control does not bind the verified latest head")
    if (
        verified_profile.control_profile_sha256 != verified_control.control_profile_sha256
        or verified_profile.metric is not verified_control.stream_key.metric
        or verified_profile.threshold_stratum != verified_control.stream_key.threshold_stratum
        or verified_profile.hnsw_index_identity != verified_control.stream_key.hnsw_binding_id
        or verified_profile.data_identity != verified_control.stream_key.data_identity
        or verified_profile.workload_manifest_sha256
        != verified_control.calibration_population_sha256
        or verified_profile.ordered_query_payload_sha256
        != verified_control.ordered_query_payload_sha256
        or verified_profile.replay_schedule_sha256
        != verified_control.replay_schedule_sha256
        or verified_profile.environment_manifest_sha256
        != verified_control.environment_manifest_sha256
        or verified_profile.source_revision != verified_control.source_revision
        or verified_profile.raw_evidence_sha256 != verified_capability.raw_evidence_sha256
    ):
        raise _error("PROFILE_CONTROL_MISMATCH", "profile differs from frozen control")
    if not (
        parse_rfc3339_utc_instant(latest.head_record_persisted_at_utc)
        < parse_rfc3339_utc_instant(verified_control.frozen_at_utc)
        < parse_rfc3339_utc_instant(verified_profile.calibration_started_at_utc)
    ):
        raise _error(
            "PROFILE_TRIGGER_ORDER_INVALID",
            "required head/control/calibration ordering is not satisfied",
        )
    return verified_capability, verified_profile, verified_control, latest


def _payload(
    *,
    capability: RootPinnedResponseProfileEvidence,
    profile: CalibratedResponseProfile,
    control: ResponseProfileControl,
    latest: VerifiedLatestResponseProfileDetectorHead,
) -> dict[str, object]:
    return {
        "schema_version": FRESH_EVIDENCE_SCHEMA_VERSION,
        "root_pinned_capability_sha256": _sha(
            capability.capability_sha256, field="capability_sha256"
        ),
        "raw_evidence_sha256": _sha(
            capability.raw_evidence_sha256, field="raw_evidence_sha256"
        ),
        "profile_sha256": _sha(profile.profile_sha256, field="profile_sha256"),
        "control_profile_sha256": _sha(
            control.control_profile_sha256, field="control_profile_sha256"
        ),
        "detector_head_sha256": _sha(
            latest.head.detector_head_sha256, field="detector_head_sha256"
        ),
        "detector_head_record_sequence": latest.head_record_sequence,
        "detector_head_record_sha256": _sha(
            latest.head_record_sha256, field="head_record_sha256"
        ),
        "detector_head_record_persisted_at_utc": latest.head_record_persisted_at_utc,
    }


def bind_fresh_response_profile_evidence(
    *,
    capability: RootPinnedResponseProfileEvidence,
    profile: CalibratedResponseProfile,
    control: ResponseProfileControl,
    verified_latest_detector_head: VerifiedLatestResponseProfileDetectorHead,
) -> FreshResponseProfileEvidence:
    verified_capability, verified_profile, verified_control, latest = _validated_parts(
        capability=capability,
        profile=profile,
        control=control,
        verified_latest_detector_head=verified_latest_detector_head,
    )
    payload = _payload(
        capability=verified_capability,
        profile=verified_profile,
        control=verified_control,
        latest=latest,
    )
    return _new(
        schema_version=FRESH_EVIDENCE_SCHEMA_VERSION,
        root_pinned_capability=verified_capability,
        profile=verified_profile,
        control=verified_control,
        verified_latest_detector_head=latest,
        fresh_evidence_sha256=hashlib.sha256(
            FRESH_EVIDENCE_HASH_DOMAIN + canonical_json_bytes(payload)
        ).hexdigest(),
    )


def verify_fresh_response_profile_evidence(value: object) -> FreshResponseProfileEvidence:
    if type(value) is not FreshResponseProfileEvidence:
        raise _error("FRESH_EVIDENCE_INVALID", "fresh evidence must be concrete")
    try:
        rebuilt = bind_fresh_response_profile_evidence(
            capability=value.root_pinned_capability,
            profile=value.profile,
            control=value.control,
            verified_latest_detector_head=value.verified_latest_detector_head,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("FRESH_EVIDENCE_INVALID", "fresh evidence reconstruction failed") from exc
    if value.schema_version != rebuilt.schema_version or not hmac.compare_digest(
        value.fresh_evidence_sha256, rebuilt.fresh_evidence_sha256
    ):
        raise _error("FRESH_EVIDENCE_INVALID", "fresh evidence digest differs")
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(value)
    ):
        raise _error("FRESH_EVIDENCE_INVALID", "fresh evidence is noncanonical")
    return rebuilt


def fresh_response_profile_evidence_payload(
    value: FreshResponseProfileEvidence,
) -> dict[str, object]:
    verified = verify_fresh_response_profile_evidence(value)
    return _payload(
        capability=verified.root_pinned_capability,
        profile=verified.profile,
        control=verified.control,
        latest=verified.verified_latest_detector_head,
    )
