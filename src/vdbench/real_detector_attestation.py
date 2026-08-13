"""ADR-014 governed real-detector attestation for the v2 host-window path.

Purpose:
    Prove that one exact `V2DetectorHead` was produced by the accepted
    ADR-002/ADR-003 statistical detector over exactly the committed source
    windows it names -- a claim an ADR-012 head alone cannot make, because
    `process_window` accepts a caller-supplied evaluator.
Mechanism:
    `RealDetectorAttestation` is private-construction. The only issuer is the
    concrete `GovernedV2DetectorEvaluator`, which emits one solely as a
    by-product of calling the unchanged `shadow_extraction.extract_window_evidence`
    and `drift.evaluate_drift_decision`. No MMD, KS, recall, Holm, audit
    sampling, or two-window statistic is reimplemented here.
Position binding (ADR-014 item 5):
    A 200-query drift window is assembled from exactly four 50-query
    `ShadowAuditTrace` envelopes, so four envelope digests exist per window --
    not two hundred, and no per-query canonical digest is exported by the
    repository. Each source position therefore binds its containing envelope
    digest plus its trace/within-trace indices plus its query id, and the
    evaluator additionally requires
    `assembled.query_records[i].query_id == sources[i].query_id`.
Previous evidence (ADR-014 item 3):
    Never a parameter. The evaluator fetches it from an injected attestation
    store under exact reference-epoch and adjacency checks, so a caller cannot
    inject a trusted previous `WindowEvidence`.
Authority:
    None. This module creates no qualification, policy, grant, routing,
    admission, activation, actuation, or candidate authority, performs no
    search, and imports no Milvus, policy, canary, or grant module.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Protocol

from .artifacts import canonical_json_bytes
from .config import Metric
from .drift import (
    AUDIT_QUERY_COUNT,
    ELIGIBLE_QUERY_COUNT,
    FAMILY_WISE_ALPHA,
    PERMUTATION_COUNT,
    PERMUTATION_DENOMINATOR,
    RESULT_LIMIT,
    SENTINEL_EF,
    DetectorState,
    DriftClassification,
    DriftDecision,
    EvidenceProvenance,
    WindowEvidence,
    evaluate_drift_decision,
)
from .drift import _EFFECT_FLOORS as _DETECTOR_EFFECT_FLOORS
from .drift import _SIGNAL_ORDER as _DETECTOR_SIGNAL_ORDER
from .host_window_detector_v2 import (
    HostWindowV2Status,
    V2DetectorHead,
    V2ShadowWindow,
)
from .host_window_lineage import CommittedHostObservation
from .shadow_extraction import extract_window_evidence
from .shadow_window import (
    TRACE_COUNT,
    TRACE_QUERY_COUNT,
    WINDOW_QUERY_COUNT,
    AssembledShadowWindow,
)


__all__ = [
    "ATTESTATION_SCHEMA_VERSION",
    "POSITION_EVIDENCE_SCHEMA_VERSION",
    "RealDetectorAttestationError",
    "RealDetectorAttestation",
    "PreviousAttestedEvidence",
    "PreviousAttestedEvidenceSource",
    "GovernedV2DetectorEvaluator",
    "detector_contract_identity",
    "position_evidence_sha256",
    "attestation_document",
]


ATTESTATION_SCHEMA_VERSION = "real-detector-attestation-v1"
POSITION_EVIDENCE_SCHEMA_VERSION = "response-profile-v2-shadow-position-evidence-v1"

_ATTESTATION_DOMAIN = b"VD::REAL_DETECTOR_ATTESTATION::V1\x00"
_POSITION_EVIDENCE_DOMAIN = b"VD::SHADOW_POSITION_EVIDENCE::V2\x00"
_CONTRACT_DOMAIN = b"VD::DETECTOR_CONTRACT_IDENTITY::V1\x00"

_ISSUE_TOKEN = object()


class RealDetectorAttestationError(RuntimeError):
    """Fail-closed attestation error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> RealDetectorAttestationError:
    return RealDetectorAttestationError(code, message)


def _digest(domain: bytes, payload: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(payload)).hexdigest()


def detector_contract_identity() -> str:
    """Derive the governed detector contract identity mechanically.

    Every value comes from `drift.py` itself, so changing any governed constant
    changes the identity and invalidates every prior attestation chain. A
    caller-supplied contract digest is never accepted anywhere in this module.
    """

    payload: dict[str, object] = {
        "schema_version": "detector-contract-identity-v1",
        "contract": "ADR-002+ADR-003",
        "permutation_count": PERMUTATION_COUNT,
        "permutation_denominator": PERMUTATION_DENOMINATOR,
        "family_wise_alpha": FAMILY_WISE_ALPHA,
        "eligible_query_count": ELIGIBLE_QUERY_COUNT,
        "audit_query_count": AUDIT_QUERY_COUNT,
        "result_limit": RESULT_LIMIT,
        "sentinel_ef": SENTINEL_EF,
        "signal_order": [signal.value for signal in _DETECTOR_SIGNAL_ORDER],
        "effect_floors": {
            signal.value: _DETECTOR_EFFECT_FLOORS[signal]
            for signal in _DETECTOR_SIGNAL_ORDER
        },
        "trace_count": TRACE_COUNT,
        "trace_query_count": TRACE_QUERY_COUNT,
        "window_query_count": WINDOW_QUERY_COUNT,
    }
    return _digest(_CONTRACT_DOMAIN, payload)


def position_evidence_sha256(
    *,
    source: CommittedHostObservation,
    trace_envelope_sha256: str,
    trace_sequence_index: int,
    within_trace_index: int,
    query_id: int | str,
) -> str:
    """Bind one source position to the exact shadow record that answered it.

    Uses only fields the current architecture already has: there are four
    50-query envelope digests per 200-query window, and no per-query canonical
    digest exists, so the containing envelope digest plus the two indices plus
    the query id is the smallest exact binding.
    """

    if type(source) is not CommittedHostObservation:
        raise _error("POSITION_SOURCE_INVALID")
    if (
        type(trace_sequence_index) is not int
        or type(within_trace_index) is not int
        or not 0 <= trace_sequence_index < TRACE_COUNT
        or not 0 <= within_trace_index < TRACE_QUERY_COUNT
    ):
        raise _error("POSITION_INDEX_INVALID")
    if not isinstance(trace_envelope_sha256, str) or len(trace_envelope_sha256) != 64:
        raise _error("POSITION_ENVELOPE_DIGEST_INVALID")
    if type(query_id) not in (int, str):
        raise _error("POSITION_QUERY_ID_INVALID")
    payload: dict[str, object] = {
        "schema_version": POSITION_EVIDENCE_SCHEMA_VERSION,
        "source_sequence": source.source_sequence,
        "window_sequence": source.window_sequence,
        "within_window_index": source.within_window_index,
        "source_sha256": source.source_sha256,
        "trace_envelope_sha256": trace_envelope_sha256,
        "trace_sequence_index": trace_sequence_index,
        "within_trace_index": within_trace_index,
        "query_id": query_id,
    }
    return _digest(_POSITION_EVIDENCE_DOMAIN, payload)


@dataclass(frozen=True, slots=True, init=False)
class RealDetectorAttestation:
    """Proof that one exact head came from the governed statistical detector.

    Construction is private: only `GovernedV2DetectorEvaluator` can issue one,
    and only after actually running the accepted detector pipeline.
    """

    schema_version: str
    detector_contract_identity: str
    stream_key: object
    reference_window_sequence: int
    reference_source_window_sha256: str
    reference_shadow_window_sha256: str
    reference_assembled_manifest_sha256: str
    current_window_sequence: int
    current_source_window_sha256: str
    current_shadow_window_sha256: str
    current_assembled_manifest_sha256: str
    detector_seed: int
    previous_window_evidence_sha256: str | None
    previous_attestation_sha256: str | None
    current_window_evidence_sha256: str
    evidence_provenance_sha256: str
    detector_state: DetectorState
    detector_classification: DriftClassification
    source_revision: str
    environment_manifest_sha256: str
    detector_head_sha256: str
    attestation_sha256: str

    def __init__(self, *, _token: object, **values: object) -> None:
        if _token is not _ISSUE_TOKEN:
            raise TypeError(
                "real detector attestations are issued only by "
                "GovernedV2DetectorEvaluator"
            )
        for name, value in values.items():
            object.__setattr__(self, name, value)


def _attestation_payload(values: dict[str, object]) -> dict[str, object]:
    from .host_window_detector_v2 import _stream_document

    return {
        "schema_version": ATTESTATION_SCHEMA_VERSION,
        "detector_contract_identity": values["detector_contract_identity"],
        "stream": _stream_document(values["stream_key"]),
        "reference_window_sequence": values["reference_window_sequence"],
        "reference_source_window_sha256": values["reference_source_window_sha256"],
        "reference_shadow_window_sha256": values["reference_shadow_window_sha256"],
        "reference_assembled_manifest_sha256": values["reference_assembled_manifest_sha256"],
        "current_window_sequence": values["current_window_sequence"],
        "current_source_window_sha256": values["current_source_window_sha256"],
        "current_shadow_window_sha256": values["current_shadow_window_sha256"],
        "current_assembled_manifest_sha256": values["current_assembled_manifest_sha256"],
        "detector_seed": values["detector_seed"],
        "previous_window_evidence_sha256": values["previous_window_evidence_sha256"],
        "previous_attestation_sha256": values["previous_attestation_sha256"],
        "current_window_evidence_sha256": values["current_window_evidence_sha256"],
        "evidence_provenance_sha256": values["evidence_provenance_sha256"],
        "detector_state": values["detector_state"].value,
        "detector_classification": values["detector_classification"].value,
        "source_revision": values["source_revision"],
        "environment_manifest_sha256": values["environment_manifest_sha256"],
        "detector_head_sha256": values["detector_head_sha256"],
    }


def attestation_document(value: RealDetectorAttestation) -> dict[str, object]:
    """Canonical, self-verifying document for one attestation."""

    if type(value) is not RealDetectorAttestation:
        raise _error("ATTESTATION_INVALID")
    payload = _attestation_payload(
        {
            name: getattr(value, name)
            for name in (
                "detector_contract_identity", "stream_key",
                "reference_window_sequence", "reference_source_window_sha256",
                "reference_shadow_window_sha256", "reference_assembled_manifest_sha256",
                "current_window_sequence", "current_source_window_sha256",
                "current_shadow_window_sha256", "current_assembled_manifest_sha256",
                "detector_seed", "previous_window_evidence_sha256",
                "previous_attestation_sha256", "current_window_evidence_sha256",
                "evidence_provenance_sha256", "detector_state",
                "detector_classification", "source_revision",
                "environment_manifest_sha256", "detector_head_sha256",
            )
        }
    )
    if value.attestation_sha256 != _digest(_ATTESTATION_DOMAIN, payload):
        raise _error("ATTESTATION_DIGEST_MISMATCH")
    return {
        "attestation_payload": payload,
        "attestation_sha256": value.attestation_sha256,
    }


@dataclass(frozen=True, slots=True)
class PreviousAttestedEvidence:
    """One durably attested predecessor comparison under the same reference."""

    attestation_sha256: str
    window_evidence_sha256: str
    window_evidence: WindowEvidence
    current_window_sequence: int
    reference_window_sequence: int
    reference_source_window_sha256: str
    detector_contract_identity: str
    detector_head_sha256: str


class PreviousAttestedEvidenceSource(Protocol):
    """Injected read port over the durable attestation store.

    The evaluator *fetches* previous evidence through this port; it is never a
    caller-supplied `WindowEvidence`.
    """

    def load_previous_attested_evidence(
        self,
        *,
        stream_key: object,
        reference_window_sequence: int,
        reference_source_window_sha256: str,
        expected_current_window_sequence: int,
        detector_contract_identity: str,
    ) -> PreviousAttestedEvidence | None: ...


class GovernedV2DetectorEvaluator:
    """The only issuer of `RealDetectorAttestation`.

    Runs the accepted detector pipeline unchanged and attests the result. A
    lambda, Protocol implementation, or any other caller-supplied callable
    cannot substitute for this concrete type.
    """

    def __init__(
        self,
        *,
        previous_evidence_source: PreviousAttestedEvidenceSource,
        detector_seed: int,
        source_revision: str,
        environment_manifest_sha256: str,
    ) -> None:
        if not callable(
            getattr(previous_evidence_source, "load_previous_attested_evidence", None)
        ):
            raise TypeError(
                "previous_evidence_source must provide load_previous_attested_evidence"
            )
        if isinstance(detector_seed, bool) or not isinstance(detector_seed, int):
            raise TypeError("detector_seed must be an integer")
        if not isinstance(source_revision, str) or not source_revision:
            raise ValueError("source_revision must be a non-empty string")
        if (
            not isinstance(environment_manifest_sha256, str)
            or len(environment_manifest_sha256) != 64
        ):
            raise ValueError("environment_manifest_sha256 must be a sha256 hex digest")
        self._previous_source = previous_evidence_source
        self.detector_seed = detector_seed
        self.source_revision = source_revision
        self.environment_manifest_sha256 = environment_manifest_sha256
        self.detector_contract_identity = detector_contract_identity()

    # -- position conservation ------------------------------------------

    def verify_position_binding(
        self,
        *,
        shadow_window: V2ShadowWindow,
        assembled: AssembledShadowWindow,
        sources: tuple[CommittedHostObservation, ...],
    ) -> None:
        """Prove committed position i == shadow record i == detector input i."""

        if type(shadow_window) is not V2ShadowWindow or type(assembled) is not AssembledShadowWindow:
            raise _error("POSITION_WINDOW_INVALID")
        if not assembled.complete:
            raise _error("ASSEMBLED_WINDOW_INCOMPLETE")
        if shadow_window.status is not HostWindowV2Status.READY:
            raise _error("SHADOW_WINDOW_NOT_READY")
        if (
            len(sources) != WINDOW_QUERY_COUNT
            or len(shadow_window.positions) != WINDOW_QUERY_COUNT
            or len(assembled.query_records) != WINDOW_QUERY_COUNT
        ):
            raise _error("POSITION_COUNT_INVALID")
        if len(assembled.envelopes) != TRACE_COUNT:
            raise _error("TRACE_ENVELOPE_COUNT_INVALID")
        # ADR-014 item 5: window identifier namespaces are collapsed.
        if assembled.window_id != shadow_window.window_sequence:
            raise _error("ASSEMBLED_WINDOW_ID_MISMATCH")
        for offset, envelope in enumerate(assembled.envelopes):
            if envelope.sequence_index != offset:
                raise _error("TRACE_ENVELOPE_ORDER_INVALID")
        for index in range(WINDOW_QUERY_COUNT):
            source = sources[index]
            position = shadow_window.positions[index]
            record = assembled.query_records[index]
            envelope = assembled.envelopes[index // TRACE_QUERY_COUNT]
            if record.query_id != source.query_id:
                raise _error("POSITION_QUERY_ID_MISMATCH")
            expected = position_evidence_sha256(
                source=source,
                trace_envelope_sha256=envelope.expected_trace_sha256,
                trace_sequence_index=index // TRACE_QUERY_COUNT,
                within_trace_index=index % TRACE_QUERY_COUNT,
                query_id=record.query_id,
            )
            if (
                position.source_sequence != source.source_sequence
                or position.source_sha256 != source.source_sha256
                or not position.evaluation_eligible
                or position.evaluation_evidence_sha256 != expected
            ):
                raise _error("POSITION_EVIDENCE_MISMATCH")

    # -- governed evaluation --------------------------------------------

    def evaluate(
        self,
        *,
        reference_shadow_window: V2ShadowWindow,
        current_shadow_window: V2ShadowWindow,
        reference_assembled: AssembledShadowWindow,
        current_assembled: AssembledShadowWindow,
        reference_sources: tuple[CommittedHostObservation, ...],
        current_sources: tuple[CommittedHostObservation, ...],
        metric: Metric,
    ) -> tuple[DriftDecision, dict[str, object]]:
        """Run the accepted detector and return the decision plus attestation inputs.

        The attestation itself is minted by `attest` once the head digest is
        known, so it can bind the exact persisted head.
        """

        self.verify_position_binding(
            shadow_window=reference_shadow_window,
            assembled=reference_assembled,
            sources=reference_sources,
        )
        self.verify_position_binding(
            shadow_window=current_shadow_window,
            assembled=current_assembled,
            sources=current_sources,
        )
        if reference_shadow_window.stream_key != current_shadow_window.stream_key:
            raise _error("STREAM_MISMATCH")
        if current_shadow_window.window_sequence <= reference_shadow_window.window_sequence:
            raise _error("WINDOW_ORDER_INVALID")
        for source in (*reference_sources, *current_sources):
            if (
                source.source_revision != self.source_revision
                or source.environment_manifest_sha256 != self.environment_manifest_sha256
            ):
                raise _error("SOURCE_IDENTITY_MISMATCH")

        previous = self._previous_source.load_previous_attested_evidence(
            stream_key=current_shadow_window.stream_key,
            reference_window_sequence=reference_shadow_window.window_sequence,
            reference_source_window_sha256=reference_shadow_window.source_window_sha256,
            expected_current_window_sequence=current_shadow_window.window_sequence - 1,
            detector_contract_identity=self.detector_contract_identity,
        )
        if previous is not None and type(previous) is not PreviousAttestedEvidence:
            raise _error("PREVIOUS_EVIDENCE_INVALID")

        current_evidence = extract_window_evidence(
            reference_window=reference_assembled,
            current_window=current_assembled,
            metric=metric,
            detector_seed=self.detector_seed,
        )
        decision = evaluate_drift_decision(
            None if previous is None else previous.window_evidence,
            current_evidence,
        )
        provenance = decision.evidence_provenance
        if provenance is None or type(provenance) is not EvidenceProvenance:
            raise _error("DECISION_PROVENANCE_MISSING")
        # ADR-014: the equality the V2 head must NOT assert is asserted here,
        # in the correct domain. `EvidenceProvenance.*_manifest_sha256` is the
        # AssembledShadowWindow manifest digest of the exact windows the real
        # detector consumed. Three digest domains stay distinct and all three
        # are bound: source-window, shadow-window, and assembled manifest.
        if (
            provenance.reference_manifest_sha256 != reference_assembled.manifest_sha256
            or provenance.current_manifest_sha256 != current_assembled.manifest_sha256
        ):
            raise _error("PROVENANCE_ASSEMBLED_MANIFEST_MISMATCH")
        if (
            provenance.reference_window_id != reference_shadow_window.window_sequence
            or provenance.current_window_id != current_shadow_window.window_sequence
        ):
            raise _error("PROVENANCE_WINDOW_SEQUENCE_MISMATCH")
        # ADR-014 item 7: the first comparison under a reference has no
        # predecessor and must therefore be INSUFFICIENT_EVIDENCE.
        if previous is None and decision.state is not DetectorState.INSUFFICIENT_EVIDENCE:
            raise _error("FIRST_COMPARISON_MUST_BE_INSUFFICIENT")

        pending: dict[str, object] = {
            "detector_contract_identity": self.detector_contract_identity,
            "stream_key": current_shadow_window.stream_key,
            "reference_window_sequence": reference_shadow_window.window_sequence,
            "reference_source_window_sha256": reference_shadow_window.source_window_sha256,
            "reference_shadow_window_sha256": reference_shadow_window.shadow_window_sha256,
            "reference_assembled_manifest_sha256": reference_assembled.manifest_sha256,
            "current_window_sequence": current_shadow_window.window_sequence,
            "current_source_window_sha256": current_shadow_window.source_window_sha256,
            "current_shadow_window_sha256": current_shadow_window.shadow_window_sha256,
            "current_assembled_manifest_sha256": current_assembled.manifest_sha256,
            "detector_seed": self.detector_seed,
            "previous_window_evidence_sha256": (
                None if previous is None else previous.window_evidence_sha256
            ),
            "previous_attestation_sha256": (
                None if previous is None else previous.attestation_sha256
            ),
            "evidence_provenance_sha256": provenance.sha256,
            "detector_state": decision.state,
            "detector_classification": decision.classification,
            "source_revision": self.source_revision,
            "environment_manifest_sha256": self.environment_manifest_sha256,
            "current_window_evidence": current_evidence,
        }
        return decision, pending

    def attest(
        self,
        *,
        pending: dict[str, object],
        head: V2DetectorHead,
        current_window_evidence_sha256: str,
    ) -> RealDetectorAttestation:
        """Mint the attestation for one persisted head. Private-token only."""

        if type(head) is not V2DetectorHead:
            raise _error("ATTESTATION_HEAD_INVALID")
        if (
            head.stream_key != pending["stream_key"]
            or head.reference_window_sequence != pending["reference_window_sequence"]
            or head.reference_source_window_sha256 != pending["reference_source_window_sha256"]
            or head.current_window_sequence != pending["current_window_sequence"]
            or head.current_source_window_sha256 != pending["current_source_window_sha256"]
            or head.current_shadow_window_sha256 != pending["current_shadow_window_sha256"]
            or head.detector_state is not pending["detector_state"]
            or head.detector_classification is not pending["detector_classification"]
            or head.detector_provenance.sha256 != pending["evidence_provenance_sha256"]
        ):
            raise _error("ATTESTATION_HEAD_MISMATCH")
        if (
            not isinstance(current_window_evidence_sha256, str)
            or len(current_window_evidence_sha256) != 64
        ):
            raise _error("ATTESTATION_EVIDENCE_DIGEST_INVALID")
        values = {
            key: value for key, value in pending.items() if key != "current_window_evidence"
        }
        values["current_window_evidence_sha256"] = current_window_evidence_sha256
        values["detector_head_sha256"] = head.detector_head_sha256
        payload = _attestation_payload(values)
        values["schema_version"] = ATTESTATION_SCHEMA_VERSION
        values["attestation_sha256"] = _digest(_ATTESTATION_DOMAIN, payload)
        return RealDetectorAttestation(_token=_ISSUE_TOKEN, **values)
