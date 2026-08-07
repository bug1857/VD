"""Immutable, cryptographically-bound seal for one completed LKG-qualification run.

Purpose:
    Give Checkpoint A (ARCHITECTURE.md's ADR-002 Phase-2 addendum,
    ``73ade90``) its typed evidence-sealing contract: ``LkgRunSeal``, the
    per-position classification vocabulary it is built from
    (``LkgPositionStatus``/``LkgPositionClassification``), the workload
    identity it binds (``LkgSealWorkloadIdentity``), and the canonical
    document/digest functions ``lkg_qualification_ledger.py`` uses to
    persist and independently re-verify a seal. This module never reads a
    ledger, never performs I/O, and never classifies a real position --
    all of that is ``lkg_qualification_ledger.py``'s job (Phase-1's raw
    evidence is that module's exclusive domain, per its own docstring).
Inputs:
    Already-computed values supplied by the caller (typically the ledger
    module, after independently re-verifying a run's complete Phase-1
    evidence).
Outputs:
    A strictly-validated ``LkgRunSeal`` -- either constructed directly
    (every dataclass in this module enforces its own invariants in
    ``__post_init__``, not only during document reconstruction, so an
    invalid instance cannot exist under any construction path) or
    reconstructed from a stored canonical payload document plus a
    separately-stored, separately-verified digest column.
Failure modes:
    Every validation failure in this module raises ``ContractViolation``
    -- never ``LkgQualificationLedgerError``, which belongs exclusively to
    the ledger module's own storage/verification layer. A caller
    reconstructing a *stored* seal is expected to catch
    ``ContractViolation`` (along with ``TypeError``/``ValueError``/
    ``json.JSONDecodeError``) and re-raise it as a durable-storage error;
    this module has no opinion about that distinction.
Digest discipline:
    ``canonical_seal_document_digest`` is computed over the seal's
    *payload* document (``seal_payload_document``), which deliberately
    excludes the digest field itself -- a document is never hashed while
    containing its own digest. ``lkg_run_seal_from_payload`` reconstructs
    an ``LkgRunSeal`` from that payload plus a digest value supplied
    separately by the caller (e.g. a SQL column read alongside the
    payload column); it never trusts a digest embedded inside the parsed
    payload, because there is no such field to trust.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum

from .artifacts import canonical_json_bytes
from .config import ContractViolation
from .lkg_run_binding import lkg_ordered_query_ids_sha256


__all__ = [
    "LkgSealCompletionState",
    "LkgPositionStatus",
    "LkgSealWorkloadIdentity",
    "LkgPositionClassification",
    "LkgRunSeal",
    "SEAL_SCHEMA_VERSION",
    "SEAL_DOMAIN",
    "derive_completion_state",
    "validate_seal_reason",
    "seal_payload_document",
    "seal_payload_document_digest",
    "lkg_run_seal_from_payload",
]


SEAL_SCHEMA_VERSION = 1
SEAL_DOMAIN = b"vdbench.lkg_qualification_ledger.seal.v1\0"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_SEAL_REASON_RE = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_RFC3339_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_MAX_TEXT_CODEPOINTS = 256
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ContractViolation(f"{field} must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or normalized != value
        or value.strip() != value
        or len(value) > _MAX_TEXT_CODEPOINTS
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ContractViolation(f"{field} is not canonical")
    return value


def _sha256_hex(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ContractViolation(f"{field} must be a lowercase 64-character hex SHA-256 digest")
    return value


def validate_seal_reason(value: object) -> str:
    if not isinstance(value, str) or _SEAL_REASON_RE.fullmatch(value) is None:
        raise ContractViolation(
            f"seal_reason must be a stable uppercase reason code matching {_SEAL_REASON_RE.pattern!r}"
        )
    return value


def _rfc3339_utc(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _RFC3339_UTC_RE.fullmatch(value) is None:
        raise ContractViolation(f"{field} must be RFC3339 UTC ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ContractViolation(f"{field} must be a valid RFC3339 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ContractViolation(f"{field} must use UTC")
    return value


def _nonnegative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractViolation(f"{field} must be a non-negative integer")
    return value


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ContractViolation(f"{field} must be a positive integer")
    return value


def _int64(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractViolation(f"{field} must be a plain int")
    if not _INT64_MIN <= value <= _INT64_MAX:
        raise ContractViolation(f"{field} must fit in a signed 64-bit integer")
    return value


class LkgSealCompletionState(StrEnum):
    """Every possible aggregate completion outcome a sealed run may have.

    Deliberately distinct vocabulary from Phase 2's later
    INCOMPLETE/PASSING/FAILING evaluation states: this describes Phase-1
    evidence *shape* (did every position get a clean success, did any
    fail, were some never attempted) -- it is not a statistical verdict.
    """

    ALL_POSITIONS_SUCCESSFUL = "ALL_POSITIONS_SUCCESSFUL"
    CONTAINS_DURABLE_FAILURE = "CONTAINS_DURABLE_FAILURE"
    INCOMPLETE_NO_FAILURE = "INCOMPLETE_NO_FAILURE"


class LkgPositionStatus(StrEnum):
    """The four mutually exclusive, exhaustive classifications of one
    fixed workload position's durable attempt history. See
    lkg_qualification_ledger.py::_classify_positions for the exact
    precedence rule that assigns one of these to every position."""

    CLEAN_SUCCESS = "CLEAN_SUCCESS"
    FAILED = "FAILED"
    MALFORMED = "MALFORMED"
    MISSING = "MISSING"


def derive_completion_state(
    *, failed_position_count: int, malformed_position_count: int, missing_position_count: int
) -> LkgSealCompletionState:
    """Pure function of the three "not cleanly successful" position
    counts -- the fourth, successful_position_count, is implied."""

    if failed_position_count > 0 or malformed_position_count > 0:
        return LkgSealCompletionState.CONTAINS_DURABLE_FAILURE
    if missing_position_count > 0:
        return LkgSealCompletionState.INCOMPLETE_NO_FAILURE
    return LkgSealCompletionState.ALL_POSITIONS_SUCCESSFUL


# Every classification's reason_codes must be exactly one of these closed
# combinations -- never an arbitrary reason-code-shaped string, even one
# that would otherwise pass a generic pattern check. A caller cannot
# fabricate a plausible-looking explanation for a classification it
# doesn't actually apply to.
_ALLOWED_REASON_CODES: dict[LkgPositionStatus, frozenset[tuple[str, ...]]] = {
    LkgPositionStatus.CLEAN_SUCCESS: frozenset({()}),
    LkgPositionStatus.MISSING: frozenset({()}),
    LkgPositionStatus.FAILED: frozenset({("DURABLE_FAILURE_PRESENT",)}),
    LkgPositionStatus.MALFORMED: frozenset(
        {
            ("MULTIPLE_SUCCESSFUL_ATTEMPTS",),
            ("MULTIPLE_SUCCESSFUL_ATTEMPTS", "DURABLE_FAILURES_ALSO_PRESENT"),
        }
    ),
}


@dataclass(frozen=True, slots=True)
class LkgSealWorkloadIdentity:
    """The DATASET-003 identity fields a sealed run's evidence was captured
    against. Distinct from ``qualification_ordered_query_ids_sha256``
    (the ordering digest) and from ``qualification_query_id_array_sha256``
    (the raw ``.npy`` artifact hash, owned exclusively by
    ``lkg_dataset003_loader.py`` and never touched here) -- this is the
    dataset/manifest/role identity alone.
    """

    dataset_id: str
    dataset_version: str
    manifest_sha256: str
    query_role: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_id", _canonical_text(self.dataset_id, field="dataset_id"))
        object.__setattr__(
            self, "dataset_version", _canonical_text(self.dataset_version, field="dataset_version")
        )
        object.__setattr__(
            self, "manifest_sha256", _sha256_hex(self.manifest_sha256, field="manifest_sha256")
        )
        object.__setattr__(self, "query_role", _canonical_text(self.query_role, field="query_role"))


_WORKLOAD_IDENTITY_FIELDS = frozenset({"dataset_id", "dataset_version", "manifest_sha256", "query_role"})


def _workload_identity_document(identity: LkgSealWorkloadIdentity) -> dict[str, object]:
    return {
        "dataset_id": identity.dataset_id,
        "dataset_version": identity.dataset_version,
        "manifest_sha256": identity.manifest_sha256,
        "query_role": identity.query_role,
    }


def _workload_identity_from_document(document: object) -> LkgSealWorkloadIdentity:
    if not isinstance(document, dict) or set(document) != _WORKLOAD_IDENTITY_FIELDS:
        raise ContractViolation("workload_identity must contain exactly the expected fields")
    return LkgSealWorkloadIdentity(
        dataset_id=document["dataset_id"],
        dataset_version=document["dataset_version"],
        manifest_sha256=document["manifest_sha256"],
        query_role=document["query_role"],
    )


@dataclass(frozen=True, slots=True)
class LkgPositionClassification:
    """One fixed workload position's exact, mutually-exclusive
    classification plus the reason codes explaining it -- see
    ``_ALLOWED_REASON_CODES`` for the closed set each classification may
    carry."""

    attempt_sequence: int
    query_id: int
    classification: LkgPositionStatus
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "attempt_sequence", _nonnegative_int(self.attempt_sequence, field="attempt_sequence")
        )
        object.__setattr__(self, "query_id", _int64(self.query_id, field="query_id"))
        if not isinstance(self.classification, LkgPositionStatus):
            raise ContractViolation("classification must be an LkgPositionStatus member")
        if not isinstance(self.reason_codes, tuple) or not all(
            isinstance(code, str) for code in self.reason_codes
        ):
            raise ContractViolation("reason_codes must be a tuple of strings")
        if self.reason_codes not in _ALLOWED_REASON_CODES[self.classification]:
            raise ContractViolation(
                f"reason_codes {self.reason_codes!r} is not a permitted combination for "
                f"{self.classification.value}"
            )


_POSITION_CLASSIFICATION_FIELDS = frozenset(
    {"attempt_sequence", "query_id", "classification", "reason_codes"}
)


def _position_classification_document(entry: LkgPositionClassification) -> dict[str, object]:
    return {
        "attempt_sequence": entry.attempt_sequence,
        "query_id": entry.query_id,
        "classification": entry.classification.value,
        "reason_codes": list(entry.reason_codes),
    }


def _position_classification_from_document(document: object) -> LkgPositionClassification:
    if not isinstance(document, dict) or set(document) != _POSITION_CLASSIFICATION_FIELDS:
        raise ContractViolation("a position classification entry must contain exactly the expected fields")
    try:
        classification = LkgPositionStatus(document["classification"])
    except ValueError as exc:
        raise ContractViolation("classification must be a known LkgPositionStatus value") from exc
    reason_codes_value = document["reason_codes"]
    if not isinstance(reason_codes_value, list) or not all(
        isinstance(code, str) for code in reason_codes_value
    ):
        raise ContractViolation("reason_codes must be a list of strings")
    return LkgPositionClassification(
        attempt_sequence=document["attempt_sequence"],
        query_id=document["query_id"],
        classification=classification,
        reason_codes=tuple(reason_codes_value),
    )


@dataclass(frozen=True, slots=True)
class LkgRunSeal:
    """Immutable, cryptographically-bound summary of exactly one Phase-1
    qualification run's complete, independently re-verified evidence, at
    the moment it was sealed.

    Every invariant below is enforced in ``__post_init__`` -- on direct
    construction as much as on document reconstruction -- so an invalid
    ``LkgRunSeal`` cannot exist under any construction path. This module
    never reads a ledger itself: see ``lkg_qualification_ledger.py``'s
    ``seal_lkg_qualification_run``/``verify_seal`` for how one of these is
    actually produced from real Phase-1 evidence, and independently
    re-verified against it thereafter.
    """

    seal_schema_version: int
    run_id: str
    run_binding_sha256: str
    phase1_ledger_schema_version: int
    workload_identity: LkgSealWorkloadIdentity
    expected_query_count: int
    qualification_ordered_query_ids_sha256: str
    final_chain_head_sha256: str
    position_classifications: tuple[LkgPositionClassification, ...]
    successful_position_count: int
    failed_position_count: int
    malformed_position_count: int
    missing_position_count: int
    successful_attempt_count: int
    failed_attempt_count: int
    total_durable_attempt_count: int
    completion_state: LkgSealCompletionState
    expected_completion_state: LkgSealCompletionState
    seal_reason: str
    sealed_at_utc: str
    canonical_seal_document_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "seal_schema_version", _positive_int(self.seal_schema_version, field="seal_schema_version")
        )
        if self.seal_schema_version != SEAL_SCHEMA_VERSION:
            raise ContractViolation(f"seal_schema_version must equal {SEAL_SCHEMA_VERSION}")
        object.__setattr__(self, "run_id", _canonical_text(self.run_id, field="run_id"))
        object.__setattr__(
            self, "run_binding_sha256", _sha256_hex(self.run_binding_sha256, field="run_binding_sha256")
        )
        object.__setattr__(
            self,
            "phase1_ledger_schema_version",
            _positive_int(self.phase1_ledger_schema_version, field="phase1_ledger_schema_version"),
        )
        if not isinstance(self.workload_identity, LkgSealWorkloadIdentity):
            raise ContractViolation("workload_identity must be an LkgSealWorkloadIdentity")
        object.__setattr__(
            self,
            "expected_query_count",
            _positive_int(self.expected_query_count, field="expected_query_count"),
        )
        object.__setattr__(
            self,
            "qualification_ordered_query_ids_sha256",
            _sha256_hex(
                self.qualification_ordered_query_ids_sha256,
                field="qualification_ordered_query_ids_sha256",
            ),
        )
        object.__setattr__(
            self,
            "final_chain_head_sha256",
            _sha256_hex(self.final_chain_head_sha256, field="final_chain_head_sha256"),
        )

        if not isinstance(self.position_classifications, tuple):
            raise ContractViolation("position_classifications must be a tuple")
        if len(self.position_classifications) != self.expected_query_count:
            raise ContractViolation(
                "position_classifications must contain exactly expected_query_count entries"
            )
        for index, entry in enumerate(self.position_classifications):
            if not isinstance(entry, LkgPositionClassification):
                raise ContractViolation(
                    "every position_classifications entry must be an LkgPositionClassification"
                )
            if entry.attempt_sequence != index:
                raise ContractViolation(
                    "position_classifications must be ordered by contiguous, zero-based attempt_sequence"
                )

        object.__setattr__(
            self,
            "successful_position_count",
            _nonnegative_int(self.successful_position_count, field="successful_position_count"),
        )
        object.__setattr__(
            self,
            "failed_position_count",
            _nonnegative_int(self.failed_position_count, field="failed_position_count"),
        )
        object.__setattr__(
            self,
            "malformed_position_count",
            _nonnegative_int(self.malformed_position_count, field="malformed_position_count"),
        )
        object.__setattr__(
            self,
            "missing_position_count",
            _nonnegative_int(self.missing_position_count, field="missing_position_count"),
        )
        if (
            self.successful_position_count
            + self.failed_position_count
            + self.malformed_position_count
            + self.missing_position_count
            != self.expected_query_count
        ):
            raise ContractViolation("the four position counts must sum to expected_query_count")

        tally = {status: 0 for status in LkgPositionStatus}
        for entry in self.position_classifications:
            tally[entry.classification] += 1
        if (
            tally[LkgPositionStatus.CLEAN_SUCCESS] != self.successful_position_count
            or tally[LkgPositionStatus.FAILED] != self.failed_position_count
            or tally[LkgPositionStatus.MALFORMED] != self.malformed_position_count
            or tally[LkgPositionStatus.MISSING] != self.missing_position_count
        ):
            raise ContractViolation("summary position counts must match position_classifications exactly")

        recomputed_ordered_ids_sha256 = lkg_ordered_query_ids_sha256(
            tuple(entry.query_id for entry in self.position_classifications)
        )
        if recomputed_ordered_ids_sha256 != self.qualification_ordered_query_ids_sha256:
            raise ContractViolation(
                "qualification_ordered_query_ids_sha256 does not match position_classifications' query IDs"
            )

        object.__setattr__(
            self,
            "successful_attempt_count",
            _nonnegative_int(self.successful_attempt_count, field="successful_attempt_count"),
        )
        object.__setattr__(
            self,
            "failed_attempt_count",
            _nonnegative_int(self.failed_attempt_count, field="failed_attempt_count"),
        )
        object.__setattr__(
            self,
            "total_durable_attempt_count",
            _nonnegative_int(self.total_durable_attempt_count, field="total_durable_attempt_count"),
        )
        if self.successful_attempt_count + self.failed_attempt_count != self.total_durable_attempt_count:
            raise ContractViolation(
                "successful_attempt_count + failed_attempt_count must equal total_durable_attempt_count"
            )

        if not isinstance(self.completion_state, LkgSealCompletionState):
            raise ContractViolation("completion_state must be an LkgSealCompletionState member")
        if not isinstance(self.expected_completion_state, LkgSealCompletionState):
            raise ContractViolation("expected_completion_state must be an LkgSealCompletionState member")
        if self.completion_state != self.expected_completion_state:
            raise ContractViolation("completion_state must equal expected_completion_state")
        expected_derived_state = derive_completion_state(
            failed_position_count=self.failed_position_count,
            malformed_position_count=self.malformed_position_count,
            missing_position_count=self.missing_position_count,
        )
        if self.completion_state != expected_derived_state:
            raise ContractViolation("completion_state does not match what the position counts imply")

        object.__setattr__(self, "seal_reason", validate_seal_reason(self.seal_reason))
        object.__setattr__(self, "sealed_at_utc", _rfc3339_utc(self.sealed_at_utc, field="sealed_at_utc"))
        object.__setattr__(
            self,
            "canonical_seal_document_digest",
            _sha256_hex(self.canonical_seal_document_digest, field="canonical_seal_document_digest"),
        )


_SEAL_PAYLOAD_FIELDS = frozenset(
    {
        "seal_schema_version",
        "run_id",
        "run_binding_sha256",
        "phase1_ledger_schema_version",
        "workload_identity",
        "expected_query_count",
        "qualification_ordered_query_ids_sha256",
        "final_chain_head_sha256",
        "position_classifications",
        "successful_position_count",
        "failed_position_count",
        "malformed_position_count",
        "missing_position_count",
        "successful_attempt_count",
        "failed_attempt_count",
        "total_durable_attempt_count",
        "completion_state",
        "expected_completion_state",
        "seal_reason",
        "sealed_at_utc",
    }
)


def seal_payload_document(seal: LkgRunSeal) -> dict[str, object]:
    """The canonical, hashed payload -- deliberately excludes
    ``canonical_seal_document_digest`` itself. A document is never hashed
    while containing its own digest."""

    if not isinstance(seal, LkgRunSeal):
        raise ContractViolation("seal must be an LkgRunSeal")
    return {
        "seal_schema_version": seal.seal_schema_version,
        "run_id": seal.run_id,
        "run_binding_sha256": seal.run_binding_sha256,
        "phase1_ledger_schema_version": seal.phase1_ledger_schema_version,
        "workload_identity": _workload_identity_document(seal.workload_identity),
        "expected_query_count": seal.expected_query_count,
        "qualification_ordered_query_ids_sha256": seal.qualification_ordered_query_ids_sha256,
        "final_chain_head_sha256": seal.final_chain_head_sha256,
        "position_classifications": [
            _position_classification_document(entry) for entry in seal.position_classifications
        ],
        "successful_position_count": seal.successful_position_count,
        "failed_position_count": seal.failed_position_count,
        "malformed_position_count": seal.malformed_position_count,
        "missing_position_count": seal.missing_position_count,
        "successful_attempt_count": seal.successful_attempt_count,
        "failed_attempt_count": seal.failed_attempt_count,
        "total_durable_attempt_count": seal.total_durable_attempt_count,
        "completion_state": seal.completion_state.value,
        "expected_completion_state": seal.expected_completion_state.value,
        "seal_reason": seal.seal_reason,
        "sealed_at_utc": seal.sealed_at_utc,
    }


def seal_payload_document_digest(payload_document: dict[str, object]) -> str:
    return hashlib.sha256(SEAL_DOMAIN + canonical_json_bytes(payload_document)).hexdigest()


def lkg_run_seal_from_payload(
    payload_document: object, *, canonical_seal_document_digest: str
) -> LkgRunSeal:
    """Strictly reconstruct an ``LkgRunSeal`` from its canonical payload
    plus a digest value supplied separately by the caller (e.g. read from
    a SQL column alongside the payload column) -- the payload itself
    carries no digest field to trust instead."""

    if not isinstance(payload_document, dict) or set(payload_document) != _SEAL_PAYLOAD_FIELDS:
        raise ContractViolation("seal payload document must contain exactly the expected fields")
    workload_identity = _workload_identity_from_document(payload_document["workload_identity"])
    position_classifications_value = payload_document["position_classifications"]
    if not isinstance(position_classifications_value, list):
        raise ContractViolation("position_classifications must be a list")
    position_classifications = tuple(
        _position_classification_from_document(entry) for entry in position_classifications_value
    )
    try:
        completion_state = LkgSealCompletionState(payload_document["completion_state"])
        expected_completion_state = LkgSealCompletionState(payload_document["expected_completion_state"])
    except ValueError as exc:
        raise ContractViolation("completion_state/expected_completion_state must be known values") from exc
    return LkgRunSeal(
        seal_schema_version=payload_document["seal_schema_version"],
        run_id=payload_document["run_id"],
        run_binding_sha256=payload_document["run_binding_sha256"],
        phase1_ledger_schema_version=payload_document["phase1_ledger_schema_version"],
        workload_identity=workload_identity,
        expected_query_count=payload_document["expected_query_count"],
        qualification_ordered_query_ids_sha256=payload_document["qualification_ordered_query_ids_sha256"],
        final_chain_head_sha256=payload_document["final_chain_head_sha256"],
        position_classifications=position_classifications,
        successful_position_count=payload_document["successful_position_count"],
        failed_position_count=payload_document["failed_position_count"],
        malformed_position_count=payload_document["malformed_position_count"],
        missing_position_count=payload_document["missing_position_count"],
        successful_attempt_count=payload_document["successful_attempt_count"],
        failed_attempt_count=payload_document["failed_attempt_count"],
        total_durable_attempt_count=payload_document["total_durable_attempt_count"],
        completion_state=completion_state,
        expected_completion_state=expected_completion_state,
        seal_reason=payload_document["seal_reason"],
        sealed_at_utc=payload_document["sealed_at_utc"],
        canonical_seal_document_digest=canonical_seal_document_digest,
    )
