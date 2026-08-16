"""Phase 2's binding to a sealed Phase-1 run, and the post-seal readiness record.

Purpose:
    ``Phase2SourceBinding`` is the complete, self-verifying canonical
    identity contract binding a Phase-2 ledger to exactly one verified
    Checkpoint-A ``LkgRunSeal`` -- pinning every version this checkpoint
    depends on (its own schema, Phase 1's ledger schema, Checkpoint A's
    seal schema) and the fixed DATASET-003 geometry
    (``expected_query_count == 2400``, always). ``LkgWindowReadinessIngestion``
    is the post-seal record that binds one window's pre-seal
    ``LkgWindowOperationalReadinessEvidence`` (from ``lkg_window_readiness.py``)
    to a specific ``Phase2SourceBinding`` -- the moment Checkpoint B's two
    halves (capture, then binding) come together. Neither type reads a
    ledger or performs I/O; that is ``lkg_phase2_readiness_ledger.py``'s
    job.
Digest discipline:
    Both types follow Checkpoint A's payload-excludes-its-own-digest
    pattern, and additionally self-verify their own digest in
    ``__post_init__`` on any construction path -- a deliberate escalation
    over Checkpoint A's ``LkgRunSeal`` (which validates digest format
    only in ``__post_init__``; the ledger layer does the recompute-and-
    compare there). ``LkgWindowReadinessIngestion``'s payload embeds the
    wrapped evidence's own payload (nested) and its digest (as an
    explicit sibling value) -- never a re-encoded string.
Fixed geometry:
    ``expected_query_count`` is hard-pinned to 2400 -- there is no
    smaller or configurable qualification contract. Twelve windows of 200
    positions each, six windows per epoch, two epochs.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass

from .artifacts import canonical_json_bytes
from .config import ContractViolation
from .lkg_qualification_seal import LkgSealWorkloadIdentity
from .lkg_window_readiness import (
    LkgWindowOperationalReadinessEvidence,
    lkg_window_operational_readiness_evidence_from_payload,
    readiness_payload_document,
    validate_rfc3339_utc,
)

__all__ = [
    "EXPECTED_QUERY_COUNT",
    "INGESTION_DOMAIN",
    "INGESTION_SCHEMA_VERSION",
    "PHASE1_LEDGER_SCHEMA_VERSION",
    "SEAL_SCHEMA_VERSION_PIN",
    "SOURCE_BINDING_DOMAIN",
    "SOURCE_BINDING_SCHEMA_VERSION",
    "LkgWindowReadinessIngestion",
    "Phase2SourceBinding",
    "ingestion_payload_document",
    "ingestion_payload_document_digest",
    "lkg_window_readiness_ingestion_from_payload",
    "phase2_source_binding_from_payload",
    "source_binding_payload_document",
    "source_binding_payload_document_digest",
]


SOURCE_BINDING_SCHEMA_VERSION = 1
SOURCE_BINDING_DOMAIN = b"vdbench.phase2_source_binding.v1\0"
# Checkpoint A's own pinned versions, duplicated here as literal constants
# (not imported) since Phase2SourceBinding's job is to assert -- and
# freeze, at the moment of binding -- exactly which Checkpoint-A version
# it was built against, independent of whatever those modules' own
# constants happen to equal if they are ever bumped later.
PHASE1_LEDGER_SCHEMA_VERSION = 5
SEAL_SCHEMA_VERSION_PIN = 1
EXPECTED_QUERY_COUNT = 2400

INGESTION_SCHEMA_VERSION = 1
INGESTION_DOMAIN = b"vdbench.lkg_window_readiness_ingestion.v1\0"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_MAX_TEXT_CODEPOINTS = 256


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


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ContractViolation(f"{field} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractViolation(f"{field} must be a non-negative integer")
    return value


_WORKLOAD_IDENTITY_FIELDS = frozenset({"dataset_id", "dataset_version", "manifest_sha256", "query_role"})


def _workload_identity_document(identity: LkgSealWorkloadIdentity) -> dict[str, object]:
    # Inline duplication of lkg_qualification_seal.py's private
    # _workload_identity_document -- that function is module-private and
    # not exported, so this checkpoint builds the identical 4-key shape
    # itself rather than crossing the module-privacy boundary. This
    # mirrors lkg_qualification_ledger.py's own _seal_payload_from_evidence,
    # which does the same thing for the same reason.
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
class Phase2SourceBinding:
    """Phase 2's complete, self-verifying binding to exactly one verified
    Checkpoint-A ``LkgRunSeal``. Every invariant is enforced in
    ``__post_init__`` on any construction path, ending with an
    independent recompute-and-compare of its own digest."""

    source_binding_schema_version: int
    source_run_id: str
    source_run_binding_sha256: str
    source_phase1_ledger_schema_version: int
    source_seal_schema_version: int
    source_run_seal_digest: str
    source_sealed_chain_head_sha256: str
    workload_identity: LkgSealWorkloadIdentity
    qualification_ordered_query_ids_sha256: str
    expected_query_count: int
    canonical_source_binding_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_binding_schema_version",
            _positive_int(self.source_binding_schema_version, field="source_binding_schema_version"),
        )
        if self.source_binding_schema_version != SOURCE_BINDING_SCHEMA_VERSION:
            raise ContractViolation(
                f"source_binding_schema_version must equal {SOURCE_BINDING_SCHEMA_VERSION}"
            )
        object.__setattr__(self, "source_run_id", _canonical_text(self.source_run_id, field="source_run_id"))
        object.__setattr__(
            self,
            "source_run_binding_sha256",
            _sha256_hex(self.source_run_binding_sha256, field="source_run_binding_sha256"),
        )
        object.__setattr__(
            self,
            "source_phase1_ledger_schema_version",
            _positive_int(
                self.source_phase1_ledger_schema_version, field="source_phase1_ledger_schema_version"
            ),
        )
        if self.source_phase1_ledger_schema_version != PHASE1_LEDGER_SCHEMA_VERSION:
            raise ContractViolation(
                f"source_phase1_ledger_schema_version must equal {PHASE1_LEDGER_SCHEMA_VERSION}"
            )
        object.__setattr__(
            self,
            "source_seal_schema_version",
            _positive_int(self.source_seal_schema_version, field="source_seal_schema_version"),
        )
        if self.source_seal_schema_version != SEAL_SCHEMA_VERSION_PIN:
            raise ContractViolation(f"source_seal_schema_version must equal {SEAL_SCHEMA_VERSION_PIN}")
        object.__setattr__(
            self,
            "source_run_seal_digest",
            _sha256_hex(self.source_run_seal_digest, field="source_run_seal_digest"),
        )
        object.__setattr__(
            self,
            "source_sealed_chain_head_sha256",
            _sha256_hex(self.source_sealed_chain_head_sha256, field="source_sealed_chain_head_sha256"),
        )
        if not isinstance(self.workload_identity, LkgSealWorkloadIdentity):
            raise ContractViolation("workload_identity must be an LkgSealWorkloadIdentity")
        object.__setattr__(
            self,
            "qualification_ordered_query_ids_sha256",
            _sha256_hex(
                self.qualification_ordered_query_ids_sha256,
                field="qualification_ordered_query_ids_sha256",
            ),
        )
        object.__setattr__(
            self, "expected_query_count", _positive_int(self.expected_query_count, field="expected_query_count")
        )
        if self.expected_query_count != EXPECTED_QUERY_COUNT:
            raise ContractViolation(f"expected_query_count must equal {EXPECTED_QUERY_COUNT}")
        object.__setattr__(
            self,
            "canonical_source_binding_digest",
            _sha256_hex(self.canonical_source_binding_digest, field="canonical_source_binding_digest"),
        )

        recomputed_payload = source_binding_payload_document(self)
        recomputed_digest = source_binding_payload_document_digest(recomputed_payload)
        if recomputed_digest != self.canonical_source_binding_digest:
            raise ContractViolation(
                "canonical_source_binding_digest does not match the recomputed payload digest"
            )


_SOURCE_BINDING_PAYLOAD_FIELDS = frozenset(
    {
        "source_binding_schema_version",
        "source_run_id",
        "source_run_binding_sha256",
        "source_phase1_ledger_schema_version",
        "source_seal_schema_version",
        "source_run_seal_digest",
        "source_sealed_chain_head_sha256",
        "workload_identity",
        "qualification_ordered_query_ids_sha256",
        "expected_query_count",
    }
)


def source_binding_payload_document(binding: Phase2SourceBinding) -> dict[str, object]:
    """The canonical, hashed payload -- deliberately excludes
    ``canonical_source_binding_digest`` itself."""

    if not isinstance(binding, Phase2SourceBinding):
        raise ContractViolation("binding must be a Phase2SourceBinding")
    return {
        "source_binding_schema_version": binding.source_binding_schema_version,
        "source_run_id": binding.source_run_id,
        "source_run_binding_sha256": binding.source_run_binding_sha256,
        "source_phase1_ledger_schema_version": binding.source_phase1_ledger_schema_version,
        "source_seal_schema_version": binding.source_seal_schema_version,
        "source_run_seal_digest": binding.source_run_seal_digest,
        "source_sealed_chain_head_sha256": binding.source_sealed_chain_head_sha256,
        "workload_identity": _workload_identity_document(binding.workload_identity),
        "qualification_ordered_query_ids_sha256": binding.qualification_ordered_query_ids_sha256,
        "expected_query_count": binding.expected_query_count,
    }


def source_binding_payload_document_digest(payload_document: dict[str, object]) -> str:
    return hashlib.sha256(SOURCE_BINDING_DOMAIN + canonical_json_bytes(payload_document)).hexdigest()


def phase2_source_binding_from_payload(
    payload_document: object, *, canonical_source_binding_digest: str
) -> Phase2SourceBinding:
    """Strictly reconstruct a ``Phase2SourceBinding`` from its canonical
    payload plus a digest supplied separately by the caller."""

    if not isinstance(payload_document, dict) or set(payload_document) != _SOURCE_BINDING_PAYLOAD_FIELDS:
        raise ContractViolation("source-binding payload document must contain exactly the expected fields")
    workload_identity = _workload_identity_from_document(payload_document["workload_identity"])
    return Phase2SourceBinding(
        source_binding_schema_version=payload_document["source_binding_schema_version"],
        source_run_id=payload_document["source_run_id"],
        source_run_binding_sha256=payload_document["source_run_binding_sha256"],
        source_phase1_ledger_schema_version=payload_document["source_phase1_ledger_schema_version"],
        source_seal_schema_version=payload_document["source_seal_schema_version"],
        source_run_seal_digest=payload_document["source_run_seal_digest"],
        source_sealed_chain_head_sha256=payload_document["source_sealed_chain_head_sha256"],
        workload_identity=workload_identity,
        qualification_ordered_query_ids_sha256=payload_document["qualification_ordered_query_ids_sha256"],
        expected_query_count=payload_document["expected_query_count"],
        canonical_source_binding_digest=canonical_source_binding_digest,
    )


@dataclass(frozen=True, slots=True)
class LkgWindowReadinessIngestion:
    """The post-seal record binding one window's unmodified, pre-seal
    ``LkgWindowOperationalReadinessEvidence`` to a specific sealed run and
    ``Phase2SourceBinding``. Enforces structural self-consistency only
    (window/epoch/run_id agreement with the embedded evidence, digest
    formats); the "not checked after sealing" chronology check belongs to
    the ledger layer, which always has a freshly re-verified
    ``LkgRunSeal`` (with its ``sealed_at_utc``) in hand -- this type has
    no such field to compare against, only a seal *digest*.
    """

    ingestion_schema_version: int
    source_run_id: str
    window_index: int
    epoch_index: int
    original_evidence: LkgWindowOperationalReadinessEvidence
    source_run_seal_digest: str
    phase2_source_binding_digest: str
    ingested_at_utc: str
    canonical_ingestion_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "ingestion_schema_version",
            _positive_int(self.ingestion_schema_version, field="ingestion_schema_version"),
        )
        if self.ingestion_schema_version != INGESTION_SCHEMA_VERSION:
            raise ContractViolation(f"ingestion_schema_version must equal {INGESTION_SCHEMA_VERSION}")
        object.__setattr__(self, "source_run_id", _canonical_text(self.source_run_id, field="source_run_id"))
        object.__setattr__(
            self, "window_index", _nonnegative_int(self.window_index, field="window_index")
        )
        object.__setattr__(
            self, "epoch_index", _nonnegative_int(self.epoch_index, field="epoch_index")
        )
        if not isinstance(self.original_evidence, LkgWindowOperationalReadinessEvidence):
            raise ContractViolation("original_evidence must be an LkgWindowOperationalReadinessEvidence")
        if self.source_run_id != self.original_evidence.source_run_id:
            raise ContractViolation("source_run_id must match original_evidence.source_run_id")
        if self.window_index != self.original_evidence.window_index:
            raise ContractViolation("window_index must match original_evidence.window_index")
        if self.epoch_index != self.original_evidence.epoch_index:
            raise ContractViolation("epoch_index must match original_evidence.epoch_index")

        object.__setattr__(
            self,
            "source_run_seal_digest",
            _sha256_hex(self.source_run_seal_digest, field="source_run_seal_digest"),
        )
        object.__setattr__(
            self,
            "phase2_source_binding_digest",
            _sha256_hex(self.phase2_source_binding_digest, field="phase2_source_binding_digest"),
        )
        object.__setattr__(
            self, "ingested_at_utc", validate_rfc3339_utc(self.ingested_at_utc, field="ingested_at_utc")
        )
        object.__setattr__(
            self,
            "canonical_ingestion_digest",
            _sha256_hex(self.canonical_ingestion_digest, field="canonical_ingestion_digest"),
        )

        recomputed_payload = ingestion_payload_document(self)
        recomputed_digest = ingestion_payload_document_digest(recomputed_payload)
        if recomputed_digest != self.canonical_ingestion_digest:
            raise ContractViolation(
                "canonical_ingestion_digest does not match the recomputed payload digest"
            )


_INGESTION_PAYLOAD_FIELDS = frozenset(
    {
        "ingestion_schema_version",
        "source_run_id",
        "window_index",
        "epoch_index",
        "original_evidence",
        "original_evidence_digest",
        "source_run_seal_digest",
        "phase2_source_binding_digest",
        "ingested_at_utc",
    }
)


def ingestion_payload_document(ingestion: LkgWindowReadinessIngestion) -> dict[str, object]:
    """The canonical, hashed payload -- deliberately excludes
    ``canonical_ingestion_digest`` itself. Embeds the wrapped evidence's
    own payload (nested dict) and its digest (an explicit sibling value)
    -- never a re-encoded JSON string."""

    if not isinstance(ingestion, LkgWindowReadinessIngestion):
        raise ContractViolation("ingestion must be an LkgWindowReadinessIngestion")
    return {
        "ingestion_schema_version": ingestion.ingestion_schema_version,
        "source_run_id": ingestion.source_run_id,
        "window_index": ingestion.window_index,
        "epoch_index": ingestion.epoch_index,
        "original_evidence": readiness_payload_document(ingestion.original_evidence),
        "original_evidence_digest": ingestion.original_evidence.canonical_document_digest,
        "source_run_seal_digest": ingestion.source_run_seal_digest,
        "phase2_source_binding_digest": ingestion.phase2_source_binding_digest,
        "ingested_at_utc": ingestion.ingested_at_utc,
    }


def ingestion_payload_document_digest(payload_document: dict[str, object]) -> str:
    return hashlib.sha256(INGESTION_DOMAIN + canonical_json_bytes(payload_document)).hexdigest()


def lkg_window_readiness_ingestion_from_payload(
    payload_document: object, *, canonical_ingestion_digest: str
) -> LkgWindowReadinessIngestion:
    """Strictly reconstruct an ``LkgWindowReadinessIngestion`` from its
    canonical payload plus a digest supplied separately by the caller.
    The nested evidence is itself reconstructed strictly, including its
    own canonical round-trip, via the embedded ``original_evidence_digest``."""

    if not isinstance(payload_document, dict) or set(payload_document) != _INGESTION_PAYLOAD_FIELDS:
        raise ContractViolation("ingestion payload document must contain exactly the expected fields")
    original_evidence = lkg_window_operational_readiness_evidence_from_payload(
        payload_document["original_evidence"],
        canonical_document_digest=payload_document["original_evidence_digest"],
    )
    return LkgWindowReadinessIngestion(
        ingestion_schema_version=payload_document["ingestion_schema_version"],
        source_run_id=payload_document["source_run_id"],
        window_index=payload_document["window_index"],
        epoch_index=payload_document["epoch_index"],
        original_evidence=original_evidence,
        source_run_seal_digest=payload_document["source_run_seal_digest"],
        phase2_source_binding_digest=payload_document["phase2_source_binding_digest"],
        ingested_at_utc=payload_document["ingested_at_utc"],
        canonical_ingestion_digest=canonical_ingestion_digest,
    )
