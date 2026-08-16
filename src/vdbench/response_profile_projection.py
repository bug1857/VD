"""R2-E deterministic projection from root-pinned evidence into R1."""

from __future__ import annotations

from .response_profile import (
    CalibratedResponseProfile,
    ResponseProfileCalibrationEvidence,
    ResponseProfileIdentity,
    build_calibrated_response_profile,
)
from .response_profile_root_pin import (
    RootPinnedResponseProfileEvidence,
    verify_root_pinned_response_profile_evidence,
)

__all__ = ["project_root_pinned_response_profile"]


def project_root_pinned_response_profile(
    *,
    capability: RootPinnedResponseProfileEvidence,
    expected_raw_evidence_sha256: str,
    expected_identity: ResponseProfileIdentity,
) -> CalibratedResponseProfile:
    """Verify R2-D and invoke the unchanged R1 builder without reimplementation."""

    verified = verify_root_pinned_response_profile_evidence(
        capability,
        expected_raw_evidence_sha256=expected_raw_evidence_sha256,
        expected_identity=expected_identity,
    )
    evidence = ResponseProfileCalibrationEvidence(
        raw_evidence_sha256=verified.raw_evidence_sha256,
        observations=verified.observations,
    )
    profile = build_calibrated_response_profile(
        identity=expected_identity, evidence=evidence
    )
    if profile.raw_evidence_sha256 != verified.raw_evidence_sha256:
        raise RuntimeError("R1 projection did not preserve the independently pinned root")
    return profile
