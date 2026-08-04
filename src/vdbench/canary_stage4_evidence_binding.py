"""Canonical, immutable provenance binding for one EXP-009 Stage-4 run.

This value object is intentionally pure.  It neither reads an artifact nor
authorizes a decision; it gives all future recall and latency evidence one
exact configuration/run/workload identity to bind and verify.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
import unicodedata

from .artifacts import canonical_json_bytes
from .canary_workload import WorkloadIdentityBinding
from .config import Metric
from .policy import ACTUATION_LADDER


__all__ = ["STAGE4_EVIDENCE_BINDING_SCHEMA_VERSION", "Stage4EvidenceBinding"]


STAGE4_EVIDENCE_BINDING_SCHEMA_VERSION = "stage4-evidence-binding-v1"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_REVISION_RE = re.compile(r"[0-9a-f]{40}\Z")
_MAX_TEXT_CODEPOINTS = 256


def _canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or normalized != value
        or value.strip() != value
        or len(value) > _MAX_TEXT_CODEPOINTS
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError(f"{field} is not canonical")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _ef(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value not in ACTUATION_LADDER:
        raise ValueError(f"{field} must be an ADR-002 actuation-ladder ef")
    return value


@dataclass(frozen=True, slots=True)
class Stage4EvidenceBinding:
    """Exact non-sensitive identity shared by recall and latency evidence."""

    run_id: str
    source_revision: str
    metric: Metric
    threshold_stratum: str
    current_ef: int
    candidate_ef: int
    last_known_good_ef: int
    identity: WorkloadIdentityBinding
    dataset002_manifest_sha256: str
    frozen_recall_audit_ids_sha256: str
    eligible_workload_sha256: str
    candidate_selection_sha256: str
    execution_schedule_sha256: str
    recall_evidence_schema_version: str
    latency_evidence_schema_version: str
    schema_version: str = STAGE4_EVIDENCE_BINDING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _canonical_text(self.run_id, field="run_id"))
        if not isinstance(self.source_revision, str) or _REVISION_RE.fullmatch(self.source_revision) is None:
            raise ValueError("source_revision must be a lowercase 40-hex git revision")
        if not isinstance(self.metric, Metric):
            raise ValueError("metric must be a Metric")
        object.__setattr__(self, "threshold_stratum", _canonical_text(self.threshold_stratum, field="threshold_stratum"))
        object.__setattr__(self, "current_ef", _ef(self.current_ef, field="current_ef"))
        object.__setattr__(self, "candidate_ef", _ef(self.candidate_ef, field="candidate_ef"))
        object.__setattr__(self, "last_known_good_ef", _ef(self.last_known_good_ef, field="last_known_good_ef"))
        if self.current_ef != self.last_known_good_ef:
            raise ValueError("current_ef must equal last_known_good_ef for Stage-4 evidence")
        if self.candidate_ef == self.last_known_good_ef:
            raise ValueError("candidate_ef must differ from last_known_good_ef")
        if not isinstance(self.identity, WorkloadIdentityBinding):
            raise ValueError("identity must be a WorkloadIdentityBinding")
        self.identity.validate()
        for field in (
            "dataset002_manifest_sha256",
            "frozen_recall_audit_ids_sha256",
            "eligible_workload_sha256",
            "candidate_selection_sha256",
            "execution_schedule_sha256",
        ):
            object.__setattr__(self, field, _sha256(getattr(self, field), field=field))
        object.__setattr__(self, "recall_evidence_schema_version", _canonical_text(self.recall_evidence_schema_version, field="recall_evidence_schema_version"))
        object.__setattr__(self, "latency_evidence_schema_version", _canonical_text(self.latency_evidence_schema_version, field="latency_evidence_schema_version"))
        if self.schema_version != STAGE4_EVIDENCE_BINDING_SCHEMA_VERSION:
            raise ValueError("schema_version is unsupported")

    def to_document(self) -> dict[str, object]:
        """Return the complete canonical-hash input, excluding no binding field."""

        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "source_revision": self.source_revision,
            "metric": self.metric.value,
            "threshold_stratum": self.threshold_stratum,
            "current_ef": self.current_ef,
            "candidate_ef": self.candidate_ef,
            "last_known_good_ef": self.last_known_good_ef,
            "identity": self.identity.to_document(),
            "dataset002_manifest_sha256": self.dataset002_manifest_sha256,
            "frozen_recall_audit_ids_sha256": self.frozen_recall_audit_ids_sha256,
            "eligible_workload_sha256": self.eligible_workload_sha256,
            "candidate_selection_sha256": self.candidate_selection_sha256,
            "execution_schedule_sha256": self.execution_schedule_sha256,
            "recall_evidence_schema_version": self.recall_evidence_schema_version,
            "latency_evidence_schema_version": self.latency_evidence_schema_version,
        }

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.to_document())).hexdigest()

    def matches_sha256(self, expected_sha256: object) -> bool:
        """Return false, rather than trusting malformed external digests."""

        return isinstance(expected_sha256, str) and _SHA256_RE.fullmatch(expected_sha256) is not None and expected_sha256 == self.sha256
