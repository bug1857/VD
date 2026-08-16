"""Read-only inspection of historical stage4-evidence-binding-v1 documents.

Purpose:
    Let a human or audit tool inspect the fields of a pre-existing
    ``stage4-evidence-binding-v1`` JSON document -- the shape
    ``Stage4EvidenceBinding`` produced before its v2 repair added
    ``candidate_search_configuration`` -- without rewriting, deleting, or
    otherwise touching the historical bytes.
Inputs:
    A JSON-decoded ``dict`` matching the v1 document shape.
Outputs:
    ``LegacyStage4EvidenceBindingV1``, a read-only record of the v1 fields.
Dependencies:
    Standard library only.
Failure modes:
    A document that is not exactly v1-shaped (wrong schema_version, missing
    or extra fields) is rejected.
Scope:
    ``LegacyStage4EvidenceBindingV1`` is deliberately NOT a subclass of, and
    shares no base class with, ``Stage4EvidenceBinding``. No v2-aware
    function (``evaluate_recall_audit_evidence``, ``Stage4RecallAuditProducer``,
    ``build_stage4_latency_evidence``, ``combine_stage4_decision``) can ever
    accept one: they all require ``isinstance(binding, Stage4EvidenceBinding)``,
    which is unconditionally false for this type. This module grants no
    evidentiary standing to v1 documents -- it only lets their historical
    content be read.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

__all__ = [
    "LEGACY_STAGE4_EVIDENCE_BINDING_V1_SCHEMA_VERSION",
    "LegacyStage4EvidenceBindingV1",
    "parse_legacy_stage4_evidence_binding_v1",
]


LEGACY_STAGE4_EVIDENCE_BINDING_V1_SCHEMA_VERSION = "stage4-evidence-binding-v1"

_V1_FIELDS = (
    "schema_version",
    "run_id",
    "source_revision",
    "metric",
    "threshold_stratum",
    "current_ef",
    "candidate_ef",
    "last_known_good_ef",
    "identity",
    "dataset002_manifest_sha256",
    "frozen_recall_audit_ids_sha256",
    "eligible_workload_sha256",
    "candidate_selection_sha256",
    "execution_schedule_sha256",
    "recall_evidence_schema_version",
    "latency_evidence_schema_version",
)


@dataclass(frozen=True, slots=True)
class LegacyStage4EvidenceBindingV1:
    """A read-only record of one historical v1 binding document's fields.

    This is inspection evidence only. It has no ``.sha256``, no evaluator
    hookup, and satisfies no v2 evidence contract.
    """

    schema_version: str
    run_id: str
    source_revision: str
    metric: str
    threshold_stratum: str
    current_ef: int
    candidate_ef: int
    last_known_good_ef: int
    identity: Mapping[str, object]
    dataset002_manifest_sha256: str
    frozen_recall_audit_ids_sha256: str
    eligible_workload_sha256: str
    candidate_selection_sha256: str
    execution_schedule_sha256: str
    recall_evidence_schema_version: str
    latency_evidence_schema_version: str


def parse_legacy_stage4_evidence_binding_v1(
    document: object,
) -> LegacyStage4EvidenceBindingV1:
    """Parse a historical v1 binding document for inspection only.

    Rejects anything that is not exactly v1-shaped -- including a v2
    document, which has an extra ``candidate_search_configuration`` field and
    a different ``schema_version`` literal.
    """

    if not isinstance(document, Mapping) or frozenset(document) != frozenset(_V1_FIELDS):
        raise ValueError("document is not a stage4-evidence-binding-v1 document")
    if document["schema_version"] != LEGACY_STAGE4_EVIDENCE_BINDING_V1_SCHEMA_VERSION:
        raise ValueError("document schema_version is not stage4-evidence-binding-v1")
    for field in _V1_FIELDS:
        if field == "identity":
            if not isinstance(document[field], Mapping):
                raise ValueError("identity must be a document")
            continue
        if field in ("current_ef", "candidate_ef", "last_known_good_ef"):
            if isinstance(document[field], bool) or not isinstance(document[field], int):
                raise ValueError(f"{field} must be an integer")
            continue
        if not isinstance(document[field], str):
            raise ValueError(f"{field} must be a string")  # domain error type carries the governed reason code  # noqa: TRY004
    return LegacyStage4EvidenceBindingV1(**{field: document[field] for field in _V1_FIELDS})
