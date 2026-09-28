"""Verified descendant evidence for the post-LKG policy boundary."""

from __future__ import annotations

from dataclasses import dataclass

from .drift import DriftDecision
from .exp010_live_runner import Exp010LiveRunner
from .lkg_phase3_binding import LkgPhase3AuthorityPair, bind_lkg_phase3_authority
from .post_lkg_collection_authority import VerifiedPostLkgCollectionAuthority
from .real_detector_attestation_store import VerifiedRealDetectorHead
from .window_finalization import restore_prepared_evaluation

__all__ = [
    "PostLkgCollectionEvidenceError",
    "VerifiedPostLkgDetectorEvidence",
    "load_verified_post_lkg_detector_evidence",
]


_ISSUE_TOKEN = object()
_REQUIRED_STORES = (
    "v2_source.sqlite3",
    "v2_shadow_attempts.sqlite3",
    "v2_detector.sqlite3",
    "v2_attestation.sqlite3",
    "v2_window_finalization.sqlite3",
)


class PostLkgCollectionEvidenceError(RuntimeError):
    """Fail-closed verified-evidence error with a stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(code if message is None else f"{code}: {message}")
        self.code = code


def _error(code: str, message: str | None = None) -> PostLkgCollectionEvidenceError:
    return PostLkgCollectionEvidenceError(code, message)


class _RefusingServingExecutor:
    def execute(self, _request: object) -> object:
        raise _error("POST_LKG_EVIDENCE_SERVING_FORBIDDEN")


class _RefusingShadowCaptureExecutor:
    def capture(self, _sources: object, *, trace_sequence_index: int) -> object:
        del trace_sequence_index
        raise _error("POST_LKG_EVIDENCE_SEARCH_FORBIDDEN")


@dataclass(frozen=True, slots=True, init=False)
class VerifiedPostLkgDetectorEvidence:
    """Store-issued authority, D1/D2 pair, detector head and attestation."""

    collection_authority: VerifiedPostLkgCollectionAuthority
    lkg_authority_pair: LkgPhase3AuthorityPair
    gate_a_evidence_sha256: str
    verified_real_detector_head: VerifiedRealDetectorHead
    drift_decision: DriftDecision

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("verified post-LKG detector evidence is loader-issued")

    @classmethod
    def _from_verified(
        cls,
        *,
        collection_authority: VerifiedPostLkgCollectionAuthority,
        lkg_authority_pair: LkgPhase3AuthorityPair,
        gate_a_evidence_sha256: str,
        verified_real_detector_head: VerifiedRealDetectorHead,
        drift_decision: DriftDecision,
        token: object,
    ) -> VerifiedPostLkgDetectorEvidence:
        if token is not _ISSUE_TOKEN:
            raise TypeError("verified post-LKG evidence construction token is invalid")
        value = object.__new__(cls)
        for name, item in (
            ("collection_authority", collection_authority),
            ("lkg_authority_pair", lkg_authority_pair),
            ("gate_a_evidence_sha256", gate_a_evidence_sha256),
            ("verified_real_detector_head", verified_real_detector_head),
            ("drift_decision", drift_decision),
        ):
            object.__setattr__(value, name, item)
        return value


def load_verified_post_lkg_detector_evidence(
    *,
    authority_sha256: str,
    lkg_authority_pair: LkgPhase3AuthorityPair,
) -> VerifiedPostLkgDetectorEvidence:
    """Verify the complete authority/store chain before issuing policy input."""

    if type(lkg_authority_pair) is not LkgPhase3AuthorityPair:
        raise _error("POST_LKG_D1_D2_PAIR_REQUIRED")

    from .deployment_governance import canonical_deployment_governance_scope
    from .exp010_gate_a_operator import load_verified_gate_a_evidence
    from .exp010_gate_c_operator import _load_post_lkg_operands
    from .post_lkg_collection_authority import (
        load_verified_post_lkg_collection_authority,
        verify_post_lkg_collection_authority_d2,
    )

    scope = canonical_deployment_governance_scope()
    try:
        collection_authority = load_verified_post_lkg_collection_authority(
            authority_sha256=authority_sha256,
            governance_scope=scope,
        )
        latest = verify_post_lkg_collection_authority_d2(
            collection_authority, governance_scope=scope
        )
        if lkg_authority_pair.verified_latest_reference != latest:
            raise _error("POST_LKG_EVIDENCE_D2_PAIR_MISMATCH")
        rebound_pair = bind_lkg_phase3_authority(
            authority=lkg_authority_pair.authority,
            verified_latest_reference=latest,
        )
        operands = _load_post_lkg_operands(authority_sha256)
        gate_a = load_verified_gate_a_evidence(operands.store_root.parent)
    except Exception as exc:
        if isinstance(exc, PostLkgCollectionEvidenceError):
            raise
        raise _error("POST_LKG_EVIDENCE_AUTHORITY_UNVERIFIED", str(exc)) from exc

    if gate_a.get("post_lkg_collection_authority_sha256") != authority_sha256:
        raise _error("POST_LKG_EVIDENCE_GATE_A_AUTHORITY_MISMATCH")
    if any(not (operands.store_root / name).is_file() for name in _REQUIRED_STORES):
        raise _error("POST_LKG_EVIDENCE_STORE_GROUP_INCOMPLETE")

    try:
        runner = Exp010LiveRunner(
            configuration=operands.runner_configuration(),
            serving_executor=_RefusingServingExecutor(),
            shadow_capture_executor=_RefusingShadowCaptureExecutor(),
            clock=lambda: "9999-12-31T23:59:59.999999Z",
            shadow_captured_at_clock=lambda: "9999-12-31T23:59:59.999999Z",
        )
    except Exception as exc:
        raise _error("POST_LKG_EVIDENCE_STORE_CHAIN_UNVERIFIED", str(exc)) from exc
    try:
        composition = runner.composition
        source_count = composition.response_store.verified_source_head().source_count
        window_plan = collection_authority.document["window_plan"]
        expected_source_count = window_plan["target_source_records"]
        expected_windows = window_plan["complete_windows"]
        if (
            source_count != expected_source_count
            or composition.finalization_store.pending() is not None
            or composition.finalization_store.next_window_sequence()
            != expected_windows
        ):
            raise _error("POST_LKG_EVIDENCE_CAMPAIGN_INCOMPLETE")
        verified_head = composition.attestation_store.load_verified_real_latest(
            composition.detector_store
        )
        if verified_head is None:
            raise _error("POST_LKG_EVIDENCE_REAL_DETECTOR_HEAD_MISSING")
        head = verified_head.head
        if head.stream_key != composition.stream_key:
            raise _error("POST_LKG_EVIDENCE_STREAM_MISMATCH")
        if (
            head.reference_window_sequence
            != window_plan["reference_window_sequence"]
            or head.current_window_sequence
            != window_plan["first_eligible_decision_sequence"]
        ):
            raise _error("POST_LKG_EVIDENCE_DETECTOR_HEAD_INELIGIBLE")
        finalization = composition.finalization_store.states()[
            head.current_window_sequence
        ]
        restored = restore_prepared_evaluation(finalization.prepared)
        if restored is None:
            raise _error("POST_LKG_EVIDENCE_DECISION_MISSING")
        drift_decision, _pending = restored
        if (
            drift_decision.state != head.detector_state
            or drift_decision.classification != head.detector_classification
            or drift_decision.evidence_provenance != head.detector_provenance
        ):
            raise _error("POST_LKG_EVIDENCE_DECISION_HEAD_MISMATCH")
    except Exception as exc:
        if isinstance(exc, PostLkgCollectionEvidenceError):
            raise
        raise _error("POST_LKG_EVIDENCE_STORE_CHAIN_UNVERIFIED", str(exc)) from exc
    finally:
        runner.close()

    return VerifiedPostLkgDetectorEvidence._from_verified(
        collection_authority=collection_authority,
        lkg_authority_pair=rebound_pair,
        gate_a_evidence_sha256=str(gate_a["evidence_sha256"]),
        verified_real_detector_head=verified_head,
        drift_decision=drift_decision,
        token=_ISSUE_TOKEN,
    )
