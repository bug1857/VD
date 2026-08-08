"""Pure data types, contracts, rules, statistical evaluation, and canonical serialization for Checkpoint C (LKG Qualification Evaluation).

Purpose:
    Provide Checkpoint C's immutable evidence evaluation artifacts:
    ``LkgQualificationEvaluationContract``, ``LkgEfEligibilityRule``,
    ``LkgQualificationSemanticsRule``, ``LkgWindowEvaluation``,
    ``LkgEpochEvaluation``, and ``LkgQualificationEvaluation``.
    Pure evaluation functions compute realized workload statistics
    (a high-accuracy deterministic floating-point arithmetic mean via
    ``math.fsum`` and exact nearest-rank p95 latency derived directly from
    the contract percentile) over 1,200
    observations per epoch, without population-level confidence bounds or
    sampling assumptions.

Inputs:
    Freshly verified Phase-1 seal and attempts (from Checkpoint A) and
    verified window readiness ingestions (from Checkpoint B).

Outputs:
    Strictly validated, canonical-digest-bound immutable evaluation objects.
    This module performs no I/O, reads no database, and mutates no state.
"""

from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum

from .artifacts import canonical_json_bytes
from .config import ContractViolation, SearchConfiguration
from .lkg_qualification_evidence import LkgAttemptStatus, LkgQueryAttempt
from .lkg_qualification_seal import LkgPositionClassification, LkgPositionStatus, LkgRunSeal
from .lkg_phase2_source_binding import LkgWindowReadinessIngestion
from .policy import ACTUATION_LADDER
from .search_configuration_digest import search_configuration_sha256


__all__ = [
    "LkgQualificationStatus",
    "EVALUATION_CONTRACT_SCHEMA_VERSION",
    "EVALUATION_CONTRACT_DOMAIN",
    "LkgQualificationEvaluationContract",
    "evaluation_contract_payload_document",
    "evaluation_contract_payload_document_digest",
    "lkg_qualification_evaluation_contract_from_payload",
    "EF_ELIGIBILITY_RULE_SCHEMA_VERSION",
    "EF_ELIGIBILITY_RULE_DOMAIN",
    "LkgEfEligibilityRule",
    "ef_eligibility_rule_payload_document",
    "ef_eligibility_rule_payload_document_digest",
    "lkg_ef_eligibility_rule_from_payload",
    "default_lkg_ef_eligibility_rule",
    "QUALIFICATION_SEMANTICS_RULE_SCHEMA_VERSION",
    "QUALIFICATION_SEMANTICS_RULE_DOMAIN",
    "LkgQualificationSemanticsRule",
    "qualification_semantics_rule_payload_document",
    "qualification_semantics_rule_payload_document_digest",
    "lkg_qualification_semantics_rule_from_payload",
    "default_lkg_qualification_semantics_rule",
    "WINDOW_EVALUATION_SCHEMA_VERSION",
    "WINDOW_EVALUATION_DOMAIN",
    "LkgWindowEvaluation",
    "window_evaluation_payload_document",
    "window_evaluation_payload_document_digest",
    "lkg_window_evaluation_from_payload",
    "EPOCH_EVALUATION_SCHEMA_VERSION",
    "EPOCH_EVALUATION_DOMAIN",
    "LkgEpochEvaluation",
    "epoch_evaluation_payload_document",
    "epoch_evaluation_payload_document_digest",
    "lkg_epoch_evaluation_from_payload",
    "QUALIFICATION_EVALUATION_SCHEMA_VERSION",
    "QUALIFICATION_EVALUATION_DOMAIN",
    "LkgQualificationEvaluation",
    "evaluation_payload_document",
    "evaluation_payload_document_digest",
    "lkg_qualification_evaluation_from_payload",
    "evaluate_window",
    "evaluate_epoch",
    "evaluate_run",
    "validate_rfc3339_utc",
]


EVALUATION_CONTRACT_SCHEMA_VERSION = 1
EVALUATION_CONTRACT_DOMAIN = b"vdbench.lkg_qualification_evaluation_contract.v1\0"

EF_ELIGIBILITY_RULE_SCHEMA_VERSION = 1
EF_ELIGIBILITY_RULE_DOMAIN = b"vdbench.lkg_ef_eligibility_rule.v1\0"

QUALIFICATION_SEMANTICS_RULE_SCHEMA_VERSION = 1
QUALIFICATION_SEMANTICS_RULE_DOMAIN = b"vdbench.lkg_qualification_semantics_rule.v1\0"

WINDOW_EVALUATION_SCHEMA_VERSION = 1
WINDOW_EVALUATION_DOMAIN = b"vdbench.lkg_window_evaluation.v1\0"

EPOCH_EVALUATION_SCHEMA_VERSION = 1
EPOCH_EVALUATION_DOMAIN = b"vdbench.lkg_epoch_evaluation.v1\0"

QUALIFICATION_EVALUATION_SCHEMA_VERSION = 1
QUALIFICATION_EVALUATION_DOMAIN = b"vdbench.lkg_qualification_evaluation.v1\0"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_REASON_CODE_RE = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_RFC3339_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_MAX_TEXT_CODEPOINTS = 256
_MAX_REASON_CODES = 32

_EXPECTED_QUERY_COUNT = 2400
_WINDOWS_PER_RUN = 12
_POSITIONS_PER_WINDOW = 200
_EPOCHS_PER_RUN = 2
_WINDOWS_PER_EPOCH = 6
_OBSERVATIONS_PER_EPOCH = 1200
_RECALL_FLOOR_V1 = 0.95
_LATENCY_CEILING_MS_V1 = 10.0
_LATENCY_PERCENTILE_V1 = 0.95
_READINESS_DIMENSIONS_V1 = ("health", "rollback")

_FAILING_REASON_CODES = frozenset(
    {
        "EF_NOT_ELIGIBLE_FOR_LKG",
        "POSITION_FAILED",
        "POSITION_MALFORMED",
        "QUERY_CORRECTNESS_FAILED",
        "HEALTH_NOT_CHECKED",
        "HEALTH_FAILED",
        "ROLLBACK_NOT_TESTED",
        "ROLLBACK_NOT_READY",
        "EPOCH_RECALL_BELOW_FLOOR",
        "EPOCH_LATENCY_ABOVE_CEILING",
    }
)

_INCOMPLETE_REASON_CODES = frozenset(
    {
        "AWAITING_READINESS_EVIDENCE",
        "PHASE1_POSITION_PERMANENTLY_MISSING",
        "POSITION_MISSING",
        "READINESS_MISSING",
    }
)

_WINDOW_REASON_CODES = frozenset(
    {
        "POSITION_FAILED",
        "POSITION_MALFORMED",
        "QUERY_CORRECTNESS_FAILED",
        "HEALTH_NOT_CHECKED",
        "HEALTH_FAILED",
        "ROLLBACK_NOT_TESTED",
        "ROLLBACK_NOT_READY",
        "POSITION_MISSING",
        "READINESS_MISSING",
    }
)
_EPOCH_REASON_CODES = (_WINDOW_REASON_CODES - {"READINESS_MISSING", "POSITION_MISSING"}) | frozenset(
    {
        "READINESS_MISSING",
        "POSITION_MISSING",
        "EPOCH_RECALL_BELOW_FLOOR",
        "EPOCH_LATENCY_ABOVE_CEILING",
    }
)
_FINAL_REASON_CODES = _EPOCH_REASON_CODES | frozenset(
    {
        "AWAITING_READINESS_EVIDENCE",
        "EF_NOT_ELIGIBLE_FOR_LKG",
        "PHASE1_POSITION_PERMANENTLY_MISSING",
    }
)


class LkgQualificationStatus(StrEnum):
    """Every evaluation state for a constituent window, epoch, or qualification run.

    Precedence order: FAILING > INCOMPLETE > PASSING.
    """

    INCOMPLETE = "INCOMPLETE"
    PASSING = "PASSING"
    FAILING = "FAILING"


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


def _nonnegative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractViolation(f"{field} must be a non-negative integer")
    return value


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ContractViolation(f"{field} must be a positive integer")
    return value


def _finite_float(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractViolation(f"{field} must be a real number")
    res = float(value)
    if not math.isfinite(res):
        raise ContractViolation(f"{field} must be finite")
    return res


def _bool(value: object, *, field: str) -> bool:
    if not isinstance(value, bool):
        raise ContractViolation(f"{field} must be a bool")
    return value


def _string_list(value: object, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ContractViolation(f"{field} must be a list of strings")
    return tuple(value)


def validate_rfc3339_utc(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _RFC3339_UTC_RE.fullmatch(value) is None:
        raise ContractViolation(f"{field} must be RFC3339 UTC ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ContractViolation(f"{field} must be a valid RFC3339 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ContractViolation(f"{field} must use UTC")
    return value


def _validate_reason_codes(
    codes: object,
    *,
    field: str = "status_reason_codes",
    allowed: frozenset[str] = _FAILING_REASON_CODES | _INCOMPLETE_REASON_CODES,
) -> tuple[str, ...]:
    if not isinstance(codes, (tuple, list)):
        raise ContractViolation(f"{field} must be a tuple or list of strings")
    for code in codes:
        if not isinstance(code, str) or _REASON_CODE_RE.fullmatch(code) is None:
            raise ContractViolation(f"{field} entry {code!r} is not a valid uppercase reason code")
    code_tuple = tuple(codes)
    if len(code_tuple) > _MAX_REASON_CODES:
        raise ContractViolation(f"{field} exceeds maximum allowed length of {_MAX_REASON_CODES}")
    if len(set(code_tuple)) != len(code_tuple):
        raise ContractViolation(f"{field} must not contain duplicates")
    if tuple(sorted(code_tuple)) != code_tuple:
        raise ContractViolation(f"{field} must be in lexicographically sorted order")
    unknown = set(code_tuple) - allowed
    if unknown:
        raise ContractViolation(f"{field} contains unsupported reason codes: {sorted(unknown)!r}")
    return code_tuple


# -----------------------------------------------------------------------------
# Artifact 1: LkgQualificationEvaluationContract
# -----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class LkgQualificationEvaluationContract:
    """Immutable statistical evaluation contract containing only statistical
    thresholds, sizing, and formula version identifiers.
    """

    contract_schema_version: int
    expected_query_count: int
    windows_per_run: int
    positions_per_window: int
    epoch_count: int
    windows_per_epoch: int
    observations_per_epoch: int
    recall_floor: float
    latency_ceiling_ms: float
    latency_percentile: float
    arithmetic_mean_formula_version: str
    nearest_rank_formula_version: str
    canonical_contract_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "contract_schema_version",
            _positive_int(self.contract_schema_version, field="contract_schema_version"),
        )
        if self.contract_schema_version != EVALUATION_CONTRACT_SCHEMA_VERSION:
            raise ContractViolation(
                f"contract_schema_version must equal {EVALUATION_CONTRACT_SCHEMA_VERSION}"
            )
        object.__setattr__(
            self, "expected_query_count", _positive_int(self.expected_query_count, field="expected_query_count")
        )
        if self.expected_query_count != _EXPECTED_QUERY_COUNT:
            raise ContractViolation(f"expected_query_count must equal {_EXPECTED_QUERY_COUNT}")

        object.__setattr__(self, "windows_per_run", _positive_int(self.windows_per_run, field="windows_per_run"))
        if self.windows_per_run != _WINDOWS_PER_RUN:
            raise ContractViolation(f"windows_per_run must equal {_WINDOWS_PER_RUN}")

        object.__setattr__(
            self, "positions_per_window", _positive_int(self.positions_per_window, field="positions_per_window")
        )
        if self.positions_per_window != _POSITIONS_PER_WINDOW:
            raise ContractViolation(f"positions_per_window must equal {_POSITIONS_PER_WINDOW}")

        object.__setattr__(self, "epoch_count", _positive_int(self.epoch_count, field="epoch_count"))
        if self.epoch_count != _EPOCHS_PER_RUN:
            raise ContractViolation(f"epoch_count must equal {_EPOCHS_PER_RUN}")

        object.__setattr__(
            self, "windows_per_epoch", _positive_int(self.windows_per_epoch, field="windows_per_epoch")
        )
        if self.windows_per_epoch != _WINDOWS_PER_EPOCH:
            raise ContractViolation(f"windows_per_epoch must equal {_WINDOWS_PER_EPOCH}")

        object.__setattr__(
            self,
            "observations_per_epoch",
            _positive_int(self.observations_per_epoch, field="observations_per_epoch"),
        )
        if self.observations_per_epoch != _OBSERVATIONS_PER_EPOCH:
            raise ContractViolation(f"observations_per_epoch must equal {_OBSERVATIONS_PER_EPOCH}")

        object.__setattr__(self, "recall_floor", _finite_float(self.recall_floor, field="recall_floor"))
        if self.recall_floor != _RECALL_FLOOR_V1:
            raise ContractViolation(
                f"schema-v1 recall_floor must equal {_RECALL_FLOOR_V1}"
            )

        object.__setattr__(
            self, "latency_ceiling_ms", _finite_float(self.latency_ceiling_ms, field="latency_ceiling_ms")
        )
        if self.latency_ceiling_ms != _LATENCY_CEILING_MS_V1:
            raise ContractViolation(
                "schema-v1 latency_ceiling_ms must equal "
                f"{_LATENCY_CEILING_MS_V1}"
            )

        object.__setattr__(
            self, "latency_percentile", _finite_float(self.latency_percentile, field="latency_percentile")
        )
        if self.latency_percentile != _LATENCY_PERCENTILE_V1:
            raise ContractViolation(
                "schema-v1 latency_percentile must equal "
                f"{_LATENCY_PERCENTILE_V1}"
            )

        object.__setattr__(
            self,
            "arithmetic_mean_formula_version",
            _canonical_text(self.arithmetic_mean_formula_version, field="arithmetic_mean_formula_version"),
        )
        if self.arithmetic_mean_formula_version != "fsum_arithmetic_mean.v1":
            raise ContractViolation("arithmetic_mean_formula_version must be 'fsum_arithmetic_mean.v1'")

        object.__setattr__(
            self,
            "nearest_rank_formula_version",
            _canonical_text(self.nearest_rank_formula_version, field="nearest_rank_formula_version"),
        )
        if self.nearest_rank_formula_version != "nearest_rank_ceil.v1":
            raise ContractViolation("nearest_rank_formula_version must be 'nearest_rank_ceil.v1'")

        object.__setattr__(
            self,
            "canonical_contract_digest",
            _sha256_hex(self.canonical_contract_digest, field="canonical_contract_digest"),
        )

        recomputed_payload = evaluation_contract_payload_document(self)
        recomputed_digest = evaluation_contract_payload_document_digest(recomputed_payload)
        if recomputed_digest != self.canonical_contract_digest:
            raise ContractViolation(
                "canonical_contract_digest does not match the recomputed payload digest"
            )


_EVALUATION_CONTRACT_PAYLOAD_FIELDS = frozenset(
    {
        "contract_schema_version",
        "expected_query_count",
        "windows_per_run",
        "positions_per_window",
        "epoch_count",
        "windows_per_epoch",
        "observations_per_epoch",
        "recall_floor",
        "latency_ceiling_ms",
        "latency_percentile",
        "arithmetic_mean_formula_version",
        "nearest_rank_formula_version",
    }
)


def evaluation_contract_payload_document(
    contract: LkgQualificationEvaluationContract,
) -> dict[str, object]:
    if not isinstance(contract, LkgQualificationEvaluationContract):
        raise ContractViolation("contract must be an LkgQualificationEvaluationContract")
    return {
        "contract_schema_version": contract.contract_schema_version,
        "expected_query_count": contract.expected_query_count,
        "windows_per_run": contract.windows_per_run,
        "positions_per_window": contract.positions_per_window,
        "epoch_count": contract.epoch_count,
        "windows_per_epoch": contract.windows_per_epoch,
        "observations_per_epoch": contract.observations_per_epoch,
        "recall_floor": contract.recall_floor,
        "latency_ceiling_ms": contract.latency_ceiling_ms,
        "latency_percentile": contract.latency_percentile,
        "arithmetic_mean_formula_version": contract.arithmetic_mean_formula_version,
        "nearest_rank_formula_version": contract.nearest_rank_formula_version,
    }


def evaluation_contract_payload_document_digest(payload_document: dict[str, object]) -> str:
    return hashlib.sha256(
        EVALUATION_CONTRACT_DOMAIN + canonical_json_bytes(payload_document)
    ).hexdigest()


def lkg_qualification_evaluation_contract_from_payload(
    payload_document: object, *, canonical_contract_digest: str
) -> LkgQualificationEvaluationContract:
    if (
        not isinstance(payload_document, dict)
        or set(payload_document) != _EVALUATION_CONTRACT_PAYLOAD_FIELDS
    ):
        raise ContractViolation("evaluation contract payload document must contain exactly expected fields")
    return LkgQualificationEvaluationContract(
        contract_schema_version=payload_document["contract_schema_version"],
        expected_query_count=payload_document["expected_query_count"],
        windows_per_run=payload_document["windows_per_run"],
        positions_per_window=payload_document["positions_per_window"],
        epoch_count=payload_document["epoch_count"],
        windows_per_epoch=payload_document["windows_per_epoch"],
        observations_per_epoch=payload_document["observations_per_epoch"],
        recall_floor=payload_document["recall_floor"],
        latency_ceiling_ms=payload_document["latency_ceiling_ms"],
        latency_percentile=payload_document["latency_percentile"],
        arithmetic_mean_formula_version=payload_document["arithmetic_mean_formula_version"],
        nearest_rank_formula_version=payload_document["nearest_rank_formula_version"],
        canonical_contract_digest=canonical_contract_digest,
    )


# -----------------------------------------------------------------------------
# Artifact 2: LkgEfEligibilityRule
# -----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class LkgEfEligibilityRule:
    """Immutable rule establishing which search ef values are eligible for LKG."""

    rule_schema_version: int
    eligible_ef_values: tuple[int, ...]
    canonical_rule_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "rule_schema_version",
            _positive_int(self.rule_schema_version, field="rule_schema_version"),
        )
        if self.rule_schema_version != EF_ELIGIBILITY_RULE_SCHEMA_VERSION:
            raise ContractViolation(
                f"rule_schema_version must equal {EF_ELIGIBILITY_RULE_SCHEMA_VERSION}"
            )
        if not isinstance(self.eligible_ef_values, tuple) or not self.eligible_ef_values:
            raise ContractViolation("eligible_ef_values must be a non-empty tuple of integers")
        for ef in self.eligible_ef_values:
            if isinstance(ef, bool) or not isinstance(ef, int) or ef <= 0:
                raise ContractViolation("every eligible_ef_values entry must be a positive integer")
        if len(set(self.eligible_ef_values)) != len(self.eligible_ef_values):
            raise ContractViolation("eligible_ef_values must not contain duplicates")
        if tuple(sorted(self.eligible_ef_values)) != self.eligible_ef_values:
            raise ContractViolation("eligible_ef_values must be in sorted order")
        object.__setattr__(
            self,
            "canonical_rule_digest",
            _sha256_hex(self.canonical_rule_digest, field="canonical_rule_digest"),
        )

        recomputed_payload = ef_eligibility_rule_payload_document(self)
        recomputed_digest = ef_eligibility_rule_payload_document_digest(recomputed_payload)
        if recomputed_digest != self.canonical_rule_digest:
            raise ContractViolation(
                "canonical_rule_digest does not match the recomputed payload digest"
            )


_EF_ELIGIBILITY_RULE_PAYLOAD_FIELDS = frozenset(
    {"rule_schema_version", "eligible_ef_values"}
)


def ef_eligibility_rule_payload_document(rule: LkgEfEligibilityRule) -> dict[str, object]:
    if not isinstance(rule, LkgEfEligibilityRule):
        raise ContractViolation("rule must be an LkgEfEligibilityRule")
    return {
        "rule_schema_version": rule.rule_schema_version,
        "eligible_ef_values": list(rule.eligible_ef_values),
    }


def ef_eligibility_rule_payload_document_digest(payload_document: dict[str, object]) -> str:
    return hashlib.sha256(
        EF_ELIGIBILITY_RULE_DOMAIN + canonical_json_bytes(payload_document)
    ).hexdigest()


def lkg_ef_eligibility_rule_from_payload(
    payload_document: object, *, canonical_rule_digest: str
) -> LkgEfEligibilityRule:
    if (
        not isinstance(payload_document, dict)
        or set(payload_document) != _EF_ELIGIBILITY_RULE_PAYLOAD_FIELDS
    ):
        raise ContractViolation("ef eligibility rule payload document must contain exactly expected fields")
    values = payload_document["eligible_ef_values"]
    if not isinstance(values, list):
        raise ContractViolation("eligible_ef_values must be a list")
    return LkgEfEligibilityRule(
        rule_schema_version=payload_document["rule_schema_version"],
        eligible_ef_values=tuple(values),
        canonical_rule_digest=canonical_rule_digest,
    )


def default_lkg_ef_eligibility_rule() -> LkgEfEligibilityRule:
    eligible = tuple(sorted(ACTUATION_LADDER))
    payload = {
        "rule_schema_version": EF_ELIGIBILITY_RULE_SCHEMA_VERSION,
        "eligible_ef_values": list(eligible),
    }
    digest = ef_eligibility_rule_payload_document_digest(payload)
    return lkg_ef_eligibility_rule_from_payload(payload, canonical_rule_digest=digest)


# -----------------------------------------------------------------------------
# Artifact 3: LkgQualificationSemanticsRule
# -----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class LkgQualificationSemanticsRule:
    """Immutable rule versions non-statistical semantics."""

    rule_schema_version: int
    required_readiness_dimensions: tuple[str, ...]
    query_correctness_rule_version: str
    identity_consistency_rule_version: str
    status_precedence_version: str
    finalization_rule_version: str
    canonical_rule_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "rule_schema_version",
            _positive_int(self.rule_schema_version, field="rule_schema_version"),
        )
        if self.rule_schema_version != QUALIFICATION_SEMANTICS_RULE_SCHEMA_VERSION:
            raise ContractViolation(
                f"rule_schema_version must equal {QUALIFICATION_SEMANTICS_RULE_SCHEMA_VERSION}"
            )
        if not isinstance(self.required_readiness_dimensions, tuple):
            raise ContractViolation("required_readiness_dimensions must be a tuple of strings")
        for dim in self.required_readiness_dimensions:
            _canonical_text(dim, field="required_readiness_dimensions entry")
        if len(set(self.required_readiness_dimensions)) != len(self.required_readiness_dimensions):
            raise ContractViolation("required_readiness_dimensions must not contain duplicates")
        if tuple(sorted(self.required_readiness_dimensions)) != self.required_readiness_dimensions:
            raise ContractViolation("required_readiness_dimensions must be in sorted order")
        if self.required_readiness_dimensions != _READINESS_DIMENSIONS_V1:
            raise ContractViolation(
                "schema-v1 required_readiness_dimensions must equal "
                f"{_READINESS_DIMENSIONS_V1!r}"
            )

        object.__setattr__(
            self,
            "query_correctness_rule_version",
            _canonical_text(
                self.query_correctness_rule_version, field="query_correctness_rule_version"
            ),
        )
        if self.query_correctness_rule_version != "threshold_violation_count_zero.v1":
            raise ContractViolation(
                "query_correctness_rule_version must be 'threshold_violation_count_zero.v1'"
            )

        object.__setattr__(
            self,
            "identity_consistency_rule_version",
            _canonical_text(
                self.identity_consistency_rule_version, field="identity_consistency_rule_version"
            ),
        )
        if self.identity_consistency_rule_version != "strict_run_binding_cross_check.v1":
            raise ContractViolation(
                "identity_consistency_rule_version must be 'strict_run_binding_cross_check.v1'"
            )

        object.__setattr__(
            self,
            "status_precedence_version",
            _canonical_text(self.status_precedence_version, field="status_precedence_version"),
        )
        if self.status_precedence_version != "failing_over_incomplete_over_passing.v1":
            raise ContractViolation(
                "status_precedence_version must be 'failing_over_incomplete_over_passing.v1'"
            )

        object.__setattr__(
            self,
            "finalization_rule_version",
            _canonical_text(self.finalization_rule_version, field="finalization_rule_version"),
        )
        if self.finalization_rule_version != "phase2_12_slot_closure_or_early_failing.v1":
            raise ContractViolation(
                "finalization_rule_version must be 'phase2_12_slot_closure_or_early_failing.v1'"
            )

        object.__setattr__(
            self,
            "canonical_rule_digest",
            _sha256_hex(self.canonical_rule_digest, field="canonical_rule_digest"),
        )

        recomputed_payload = qualification_semantics_rule_payload_document(self)
        recomputed_digest = qualification_semantics_rule_payload_document_digest(recomputed_payload)
        if recomputed_digest != self.canonical_rule_digest:
            raise ContractViolation(
                "canonical_rule_digest does not match the recomputed payload digest"
            )


_QUALIFICATION_SEMANTICS_RULE_PAYLOAD_FIELDS = frozenset(
    {
        "rule_schema_version",
        "required_readiness_dimensions",
        "query_correctness_rule_version",
        "identity_consistency_rule_version",
        "status_precedence_version",
        "finalization_rule_version",
    }
)


def qualification_semantics_rule_payload_document(
    rule: LkgQualificationSemanticsRule,
) -> dict[str, object]:
    if not isinstance(rule, LkgQualificationSemanticsRule):
        raise ContractViolation("rule must be an LkgQualificationSemanticsRule")
    return {
        "rule_schema_version": rule.rule_schema_version,
        "required_readiness_dimensions": list(rule.required_readiness_dimensions),
        "query_correctness_rule_version": rule.query_correctness_rule_version,
        "identity_consistency_rule_version": rule.identity_consistency_rule_version,
        "status_precedence_version": rule.status_precedence_version,
        "finalization_rule_version": rule.finalization_rule_version,
    }


def qualification_semantics_rule_payload_document_digest(payload_document: dict[str, object]) -> str:
    return hashlib.sha256(
        QUALIFICATION_SEMANTICS_RULE_DOMAIN + canonical_json_bytes(payload_document)
    ).hexdigest()


def lkg_qualification_semantics_rule_from_payload(
    payload_document: object, *, canonical_rule_digest: str
) -> LkgQualificationSemanticsRule:
    if (
        not isinstance(payload_document, dict)
        or set(payload_document) != _QUALIFICATION_SEMANTICS_RULE_PAYLOAD_FIELDS
    ):
        raise ContractViolation(
            "qualification semantics rule payload document must contain exactly expected fields"
        )
    dims = payload_document["required_readiness_dimensions"]
    if not isinstance(dims, list):
        raise ContractViolation("required_readiness_dimensions must be a list")
    return LkgQualificationSemanticsRule(
        rule_schema_version=payload_document["rule_schema_version"],
        required_readiness_dimensions=tuple(dims),
        query_correctness_rule_version=payload_document["query_correctness_rule_version"],
        identity_consistency_rule_version=payload_document["identity_consistency_rule_version"],
        status_precedence_version=payload_document["status_precedence_version"],
        finalization_rule_version=payload_document["finalization_rule_version"],
        canonical_rule_digest=canonical_rule_digest,
    )


def default_lkg_qualification_semantics_rule() -> LkgQualificationSemanticsRule:
    payload = {
        "rule_schema_version": QUALIFICATION_SEMANTICS_RULE_SCHEMA_VERSION,
        "required_readiness_dimensions": ["health", "rollback"],
        "query_correctness_rule_version": "threshold_violation_count_zero.v1",
        "identity_consistency_rule_version": "strict_run_binding_cross_check.v1",
        "status_precedence_version": "failing_over_incomplete_over_passing.v1",
        "finalization_rule_version": "phase2_12_slot_closure_or_early_failing.v1",
    }
    digest = qualification_semantics_rule_payload_document_digest(payload)
    return lkg_qualification_semantics_rule_from_payload(payload, canonical_rule_digest=digest)


# -----------------------------------------------------------------------------
# Artifact 4: LkgWindowEvaluation
# -----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class LkgWindowEvaluation:
    """Immutable evaluation artifact for one constituent 200-position window."""

    window_evaluation_schema_version: int
    window_index: int
    epoch_index: int
    first_attempt_sequence: int
    last_attempt_sequence: int
    status: LkgQualificationStatus
    status_reason_codes: tuple[str, ...]
    clean_success_position_count: int
    failed_position_count: int
    malformed_position_count: int
    missing_position_count: int
    contributing_observation_count: int
    readiness_ingestion_digest: str | None
    canonical_window_evaluation_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "window_evaluation_schema_version",
            _positive_int(
                self.window_evaluation_schema_version, field="window_evaluation_schema_version"
            ),
        )
        if self.window_evaluation_schema_version != WINDOW_EVALUATION_SCHEMA_VERSION:
            raise ContractViolation(
                f"window_evaluation_schema_version must equal {WINDOW_EVALUATION_SCHEMA_VERSION}"
            )
        object.__setattr__(
            self, "window_index", _nonnegative_int(self.window_index, field="window_index")
        )
        if not 0 <= self.window_index < _WINDOWS_PER_RUN:
            raise ContractViolation(f"window_index must be in [0, {_WINDOWS_PER_RUN})")

        object.__setattr__(
            self, "epoch_index", _nonnegative_int(self.epoch_index, field="epoch_index")
        )
        if not 0 <= self.epoch_index < _EPOCHS_PER_RUN or self.epoch_index != self.window_index // _WINDOWS_PER_EPOCH:
            raise ContractViolation("epoch_index must equal window_index // 6")

        object.__setattr__(
            self,
            "first_attempt_sequence",
            _nonnegative_int(self.first_attempt_sequence, field="first_attempt_sequence"),
        )
        if self.first_attempt_sequence != self.window_index * _POSITIONS_PER_WINDOW:
            raise ContractViolation("first_attempt_sequence must equal window_index * 200")

        object.__setattr__(
            self,
            "last_attempt_sequence",
            _nonnegative_int(self.last_attempt_sequence, field="last_attempt_sequence"),
        )
        if self.last_attempt_sequence != self.first_attempt_sequence + _POSITIONS_PER_WINDOW - 1:
            raise ContractViolation("last_attempt_sequence must equal first_attempt_sequence + 199")

        if not isinstance(self.status, LkgQualificationStatus):
            raise ContractViolation("status must be an LkgQualificationStatus member")
        object.__setattr__(
            self,
            "status_reason_codes",
            _validate_reason_codes(self.status_reason_codes, allowed=_WINDOW_REASON_CODES),
        )

        object.__setattr__(
            self,
            "clean_success_position_count",
            _nonnegative_int(self.clean_success_position_count, field="clean_success_position_count"),
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
            self.clean_success_position_count
            + self.failed_position_count
            + self.malformed_position_count
            + self.missing_position_count
            != _POSITIONS_PER_WINDOW
        ):
            raise ContractViolation("the four position counts must sum to 200")

        object.__setattr__(
            self,
            "contributing_observation_count",
            _nonnegative_int(
                self.contributing_observation_count, field="contributing_observation_count"
            ),
        )
        if self.contributing_observation_count != self.clean_success_position_count:
            raise ContractViolation(
                "contributing_observation_count must equal clean_success_position_count"
            )

        count_reason_pairs = (
            (self.failed_position_count, "POSITION_FAILED"),
            (self.malformed_position_count, "POSITION_MALFORMED"),
            (self.missing_position_count, "POSITION_MISSING"),
        )
        for count, reason in count_reason_pairs:
            if (count > 0) != (reason in self.status_reason_codes):
                raise ContractViolation(
                    f"{reason} must be present if and only if its position count is non-zero"
                )
        if (self.readiness_ingestion_digest is None) != (
            "READINESS_MISSING" in self.status_reason_codes
        ):
            raise ContractViolation(
                "READINESS_MISSING must be present if and only if readiness_ingestion_digest is None"
            )
        readiness_failure_codes = {
            "HEALTH_NOT_CHECKED",
            "HEALTH_FAILED",
            "ROLLBACK_NOT_TESTED",
            "ROLLBACK_NOT_READY",
        }
        if self.readiness_ingestion_digest is None and readiness_failure_codes.intersection(
            self.status_reason_codes
        ):
            raise ContractViolation(
                "readiness failure reason codes require a readiness ingestion digest"
            )

        # Semantic Invariants for Status
        if self.status is LkgQualificationStatus.PASSING:
            if (
                self.failed_position_count > 0
                or self.malformed_position_count > 0
                or self.missing_position_count > 0
            ):
                raise ContractViolation("PASSING window cannot contain non-clean positions")
            if self.clean_success_position_count != _POSITIONS_PER_WINDOW:
                raise ContractViolation("PASSING window requires exactly 200 clean success positions")
            if self.readiness_ingestion_digest is None:
                raise ContractViolation("PASSING window requires non-None readiness_ingestion_digest")
            if self.status_reason_codes:
                raise ContractViolation("PASSING window cannot carry reason codes")
        elif self.status is LkgQualificationStatus.FAILING:
            if not any(code in _FAILING_REASON_CODES for code in self.status_reason_codes):
                raise ContractViolation("FAILING window status requires at least one FAILING reason code")
        elif self.status is LkgQualificationStatus.INCOMPLETE:
            if any(code in _FAILING_REASON_CODES for code in self.status_reason_codes):
                raise ContractViolation("INCOMPLETE window status cannot coexist with FAILING reason codes")
            if not any(code in _INCOMPLETE_REASON_CODES for code in self.status_reason_codes):
                raise ContractViolation("INCOMPLETE window status requires at least one INCOMPLETE reason code")

        if self.readiness_ingestion_digest is not None:
            object.__setattr__(
                self,
                "readiness_ingestion_digest",
                _sha256_hex(self.readiness_ingestion_digest, field="readiness_ingestion_digest"),
            )

        object.__setattr__(
            self,
            "canonical_window_evaluation_digest",
            _sha256_hex(
                self.canonical_window_evaluation_digest,
                field="canonical_window_evaluation_digest",
            ),
        )

        recomputed_payload = window_evaluation_payload_document(self)
        recomputed_digest = window_evaluation_payload_document_digest(recomputed_payload)
        if recomputed_digest != self.canonical_window_evaluation_digest:
            raise ContractViolation(
                "canonical_window_evaluation_digest does not match the recomputed payload digest"
            )


_WINDOW_EVALUATION_PAYLOAD_FIELDS = frozenset(
    {
        "window_evaluation_schema_version",
        "window_index",
        "epoch_index",
        "first_attempt_sequence",
        "last_attempt_sequence",
        "status",
        "status_reason_codes",
        "clean_success_position_count",
        "failed_position_count",
        "malformed_position_count",
        "missing_position_count",
        "contributing_observation_count",
        "readiness_ingestion_digest",
    }
)


def window_evaluation_payload_document(window_eval: LkgWindowEvaluation) -> dict[str, object]:
    if not isinstance(window_eval, LkgWindowEvaluation):
        raise ContractViolation("window_eval must be an LkgWindowEvaluation")
    return {
        "window_evaluation_schema_version": window_eval.window_evaluation_schema_version,
        "window_index": window_eval.window_index,
        "epoch_index": window_eval.epoch_index,
        "first_attempt_sequence": window_eval.first_attempt_sequence,
        "last_attempt_sequence": window_eval.last_attempt_sequence,
        "status": window_eval.status.value,
        "status_reason_codes": list(window_eval.status_reason_codes),
        "clean_success_position_count": window_eval.clean_success_position_count,
        "failed_position_count": window_eval.failed_position_count,
        "malformed_position_count": window_eval.malformed_position_count,
        "missing_position_count": window_eval.missing_position_count,
        "contributing_observation_count": window_eval.contributing_observation_count,
        "readiness_ingestion_digest": window_eval.readiness_ingestion_digest,
    }


def window_evaluation_payload_document_digest(payload_document: dict[str, object]) -> str:
    return hashlib.sha256(
        WINDOW_EVALUATION_DOMAIN + canonical_json_bytes(payload_document)
    ).hexdigest()


def lkg_window_evaluation_from_payload(
    payload_document: object, *, canonical_window_evaluation_digest: str
) -> LkgWindowEvaluation:
    if (
        not isinstance(payload_document, dict)
        or set(payload_document) != _WINDOW_EVALUATION_PAYLOAD_FIELDS
    ):
        raise ContractViolation("window evaluation payload document must contain exactly expected fields")
    try:
        status = LkgQualificationStatus(payload_document["status"])
    except ValueError as exc:
        raise ContractViolation("status must be a known LkgQualificationStatus value") from exc
    return LkgWindowEvaluation(
        window_evaluation_schema_version=payload_document["window_evaluation_schema_version"],
        window_index=payload_document["window_index"],
        epoch_index=payload_document["epoch_index"],
        first_attempt_sequence=payload_document["first_attempt_sequence"],
        last_attempt_sequence=payload_document["last_attempt_sequence"],
        status=status,
        status_reason_codes=_string_list(
            payload_document["status_reason_codes"], field="status_reason_codes"
        ),
        clean_success_position_count=payload_document["clean_success_position_count"],
        failed_position_count=payload_document["failed_position_count"],
        malformed_position_count=payload_document["malformed_position_count"],
        missing_position_count=payload_document["missing_position_count"],
        contributing_observation_count=payload_document["contributing_observation_count"],
        readiness_ingestion_digest=payload_document["readiness_ingestion_digest"],
        canonical_window_evaluation_digest=canonical_window_evaluation_digest,
    )


# -----------------------------------------------------------------------------
# Artifact 5: LkgEpochEvaluation
# -----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class LkgEpochEvaluation:
    """Immutable evaluation artifact for one 1,200-observation epoch."""

    epoch_evaluation_schema_version: int
    epoch_index: int
    first_window_index: int
    last_window_index: int
    status: LkgQualificationStatus
    status_reason_codes: tuple[str, ...]
    window_evaluations: tuple[LkgWindowEvaluation, ...]
    observed_mean_capped_recall: float | None
    observed_p95_latency_ms: float | None
    contributing_observation_count: int
    canonical_epoch_evaluation_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "epoch_evaluation_schema_version",
            _positive_int(
                self.epoch_evaluation_schema_version, field="epoch_evaluation_schema_version"
            ),
        )
        if self.epoch_evaluation_schema_version != EPOCH_EVALUATION_SCHEMA_VERSION:
            raise ContractViolation(
                f"epoch_evaluation_schema_version must equal {EPOCH_EVALUATION_SCHEMA_VERSION}"
            )

        object.__setattr__(
            self, "epoch_index", _nonnegative_int(self.epoch_index, field="epoch_index")
        )
        if not 0 <= self.epoch_index < _EPOCHS_PER_RUN:
            raise ContractViolation(f"epoch_index must be in [0, {_EPOCHS_PER_RUN})")

        object.__setattr__(
            self, "first_window_index", _nonnegative_int(self.first_window_index, field="first_window_index")
        )
        if self.first_window_index != self.epoch_index * _WINDOWS_PER_EPOCH:
            raise ContractViolation("first_window_index must equal epoch_index * 6")

        object.__setattr__(
            self, "last_window_index", _nonnegative_int(self.last_window_index, field="last_window_index")
        )
        if self.last_window_index != self.first_window_index + _WINDOWS_PER_EPOCH - 1:
            raise ContractViolation("last_window_index must equal first_window_index + 5")

        if not isinstance(self.status, LkgQualificationStatus):
            raise ContractViolation("status must be an LkgQualificationStatus member")
        object.__setattr__(
            self,
            "status_reason_codes",
            _validate_reason_codes(self.status_reason_codes, allowed=_EPOCH_REASON_CODES),
        )

        if not isinstance(self.window_evaluations, tuple) or len(self.window_evaluations) != _WINDOWS_PER_EPOCH:
            raise ContractViolation(
                f"window_evaluations must be a tuple of exactly {_WINDOWS_PER_EPOCH} LkgWindowEvaluation objects"
            )
        for idx, we in enumerate(self.window_evaluations):
            if not isinstance(we, LkgWindowEvaluation):
                raise ContractViolation("window_evaluations entries must be LkgWindowEvaluation instances")
            if we.window_index != self.first_window_index + idx:
                raise ContractViolation("window_evaluations must be ordered by contiguous window_index")

        object.__setattr__(
            self,
            "contributing_observation_count",
            _nonnegative_int(
                self.contributing_observation_count, field="contributing_observation_count"
            ),
        )
        expected_contributors = sum(
            window.contributing_observation_count for window in self.window_evaluations
        )
        if self.contributing_observation_count != expected_contributors:
            raise ContractViolation(
                "contributing_observation_count must equal the sum of constituent-window contributors"
            )

        # Precedence checks across constituent windows
        any_failing_win = any(we.status is LkgQualificationStatus.FAILING for we in self.window_evaluations)
        any_incomplete_win = any(we.status is LkgQualificationStatus.INCOMPLETE for we in self.window_evaluations)

        if any_failing_win:
            required_reasons = tuple(
                sorted(
                    {
                        code
                        for window in self.window_evaluations
                        for code in window.status_reason_codes
                        if code in _FAILING_REASON_CODES
                    }
                )
            )
            if self.status is not LkgQualificationStatus.FAILING:
                raise ContractViolation("a FAILING constituent window requires a FAILING epoch")
            if self.status_reason_codes != required_reasons:
                raise ContractViolation(
                    "FAILING epoch reason codes must exactly propagate constituent failure reasons"
                )
            if self.observed_mean_capped_recall is not None or self.observed_p95_latency_ms is not None:
                raise ContractViolation(
                    "an epoch with a FAILING constituent must not compute partial-population statistics"
                )
        elif any_incomplete_win:
            required_reasons = tuple(
                sorted(
                    {
                        code
                        for window in self.window_evaluations
                        for code in window.status_reason_codes
                        if code in _INCOMPLETE_REASON_CODES
                    }
                )
            )
            if self.status is not LkgQualificationStatus.INCOMPLETE:
                raise ContractViolation("an INCOMPLETE constituent window requires an INCOMPLETE epoch")
            if self.status_reason_codes != required_reasons:
                raise ContractViolation(
                    "INCOMPLETE epoch reason codes must exactly propagate constituent incomplete reasons"
                )
            if self.observed_mean_capped_recall is not None or self.observed_p95_latency_ms is not None:
                raise ContractViolation("INCOMPLETE epoch must have None observed statistics")
        else:
            if self.contributing_observation_count != _OBSERVATIONS_PER_EPOCH:
                raise ContractViolation(
                    "an epoch with six PASSING windows requires exactly 1200 contributors"
                )
            if self.observed_mean_capped_recall is None or self.observed_p95_latency_ms is None:
                raise ContractViolation(
                    "an epoch with six PASSING windows requires observed statistics"
                )
            if self.status is LkgQualificationStatus.INCOMPLETE:
                raise ContractViolation("six PASSING windows cannot produce an INCOMPLETE epoch")
            if self.status is LkgQualificationStatus.PASSING and self.status_reason_codes:
                raise ContractViolation("PASSING epoch cannot carry reason codes")
            if self.status is LkgQualificationStatus.FAILING and not set(
                self.status_reason_codes
            ).issubset({"EPOCH_RECALL_BELOW_FLOOR", "EPOCH_LATENCY_ABOVE_CEILING"}):
                raise ContractViolation(
                    "an all-PASSING-window epoch may fail only an epoch SLO"
                )

        if self.status is LkgQualificationStatus.PASSING:
            if any_failing_win or any_incomplete_win:
                raise ContractViolation("PASSING epoch cannot contain non-PASSING constituent windows")
            if self.contributing_observation_count != _OBSERVATIONS_PER_EPOCH:
                raise ContractViolation(
                    "contributing_observation_count must equal 1200 for a PASSING epoch"
                )
            if self.observed_mean_capped_recall is None or self.observed_p95_latency_ms is None:
                raise ContractViolation("PASSING epoch must carry observed statistics")
            object.__setattr__(
                self,
                "observed_mean_capped_recall",
                _finite_float(self.observed_mean_capped_recall, field="observed_mean_capped_recall"),
            )
            if not 0.0 <= self.observed_mean_capped_recall <= 1.0:
                raise ContractViolation("observed_mean_capped_recall must be in [0.0, 1.0]")
            object.__setattr__(
                self,
                "observed_p95_latency_ms",
                _finite_float(self.observed_p95_latency_ms, field="observed_p95_latency_ms"),
            )
            if self.observed_p95_latency_ms < 0.0:
                raise ContractViolation("observed_p95_latency_ms must be non-negative")
            if self.status_reason_codes:
                raise ContractViolation("PASSING epoch cannot carry reason codes")
        elif self.status is LkgQualificationStatus.INCOMPLETE:
            if any_failing_win:
                raise ContractViolation("INCOMPLETE epoch cannot contain FAILING constituent windows")
            if self.observed_mean_capped_recall is not None or self.observed_p95_latency_ms is not None:
                raise ContractViolation("INCOMPLETE epoch must have None observed statistics")
            if not any(code in _INCOMPLETE_REASON_CODES for code in self.status_reason_codes):
                raise ContractViolation("INCOMPLETE epoch requires at least one INCOMPLETE reason code")
        elif self.status is LkgQualificationStatus.FAILING:
            if self.observed_mean_capped_recall is not None:
                object.__setattr__(
                    self,
                    "observed_mean_capped_recall",
                    _finite_float(self.observed_mean_capped_recall, field="observed_mean_capped_recall"),
                )
                if not 0.0 <= self.observed_mean_capped_recall <= 1.0:
                    raise ContractViolation("observed_mean_capped_recall must be in [0.0, 1.0]")
            if self.observed_p95_latency_ms is not None:
                object.__setattr__(
                    self,
                    "observed_p95_latency_ms",
                    _finite_float(self.observed_p95_latency_ms, field="observed_p95_latency_ms"),
                )
                if self.observed_p95_latency_ms < 0.0:
                    raise ContractViolation("observed_p95_latency_ms must be non-negative")
            if not any(code in _FAILING_REASON_CODES for code in self.status_reason_codes):
                raise ContractViolation("FAILING epoch status requires at least one FAILING reason code")

        object.__setattr__(
            self,
            "canonical_epoch_evaluation_digest",
            _sha256_hex(
                self.canonical_epoch_evaluation_digest, field="canonical_epoch_evaluation_digest"
            ),
        )

        recomputed_payload = epoch_evaluation_payload_document(self)
        recomputed_digest = epoch_evaluation_payload_document_digest(recomputed_payload)
        if recomputed_digest != self.canonical_epoch_evaluation_digest:
            raise ContractViolation(
                "canonical_epoch_evaluation_digest does not match the recomputed payload digest"
            )


_EPOCH_EVALUATION_PAYLOAD_FIELDS = frozenset(
    {
        "epoch_evaluation_schema_version",
        "epoch_index",
        "first_window_index",
        "last_window_index",
        "status",
        "status_reason_codes",
        "window_evaluations",
        "observed_mean_capped_recall",
        "observed_p95_latency_ms",
        "contributing_observation_count",
    }
)


def epoch_evaluation_payload_document(epoch_eval: LkgEpochEvaluation) -> dict[str, object]:
    if not isinstance(epoch_eval, LkgEpochEvaluation):
        raise ContractViolation("epoch_eval must be an LkgEpochEvaluation")
    return {
        "epoch_evaluation_schema_version": epoch_eval.epoch_evaluation_schema_version,
        "epoch_index": epoch_eval.epoch_index,
        "first_window_index": epoch_eval.first_window_index,
        "last_window_index": epoch_eval.last_window_index,
        "status": epoch_eval.status.value,
        "status_reason_codes": list(epoch_eval.status_reason_codes),
        "window_evaluations": [
            window_evaluation_payload_document(we) for we in epoch_eval.window_evaluations
        ],
        "observed_mean_capped_recall": epoch_eval.observed_mean_capped_recall,
        "observed_p95_latency_ms": epoch_eval.observed_p95_latency_ms,
        "contributing_observation_count": epoch_eval.contributing_observation_count,
    }


def epoch_evaluation_payload_document_digest(payload_document: dict[str, object]) -> str:
    return hashlib.sha256(
        EPOCH_EVALUATION_DOMAIN + canonical_json_bytes(payload_document)
    ).hexdigest()


def lkg_epoch_evaluation_from_payload(
    payload_document: object, *, canonical_epoch_evaluation_digest: str
) -> LkgEpochEvaluation:
    if (
        not isinstance(payload_document, dict)
        or set(payload_document) != _EPOCH_EVALUATION_PAYLOAD_FIELDS
    ):
        raise ContractViolation("epoch evaluation payload document must contain exactly expected fields")
    try:
        status = LkgQualificationStatus(payload_document["status"])
    except ValueError as exc:
        raise ContractViolation("status must be a known LkgQualificationStatus value") from exc
    window_evals_val = payload_document["window_evaluations"]
    if not isinstance(window_evals_val, list):
        raise ContractViolation("window_evaluations must be a list")
    window_evaluations_list: list[LkgWindowEvaluation] = []
    for we_doc in window_evals_val:
        if not isinstance(we_doc, dict):
            raise ContractViolation("window_evaluations entries must be objects")
        window_evaluations_list.append(
            lkg_window_evaluation_from_payload(
                we_doc,
                canonical_window_evaluation_digest=window_evaluation_payload_document_digest(
                    we_doc
                ),
            )
        )
    window_evaluations = tuple(window_evaluations_list)
    return LkgEpochEvaluation(
        epoch_evaluation_schema_version=payload_document["epoch_evaluation_schema_version"],
        epoch_index=payload_document["epoch_index"],
        first_window_index=payload_document["first_window_index"],
        last_window_index=payload_document["last_window_index"],
        status=status,
        status_reason_codes=_string_list(
            payload_document["status_reason_codes"], field="status_reason_codes"
        ),
        window_evaluations=window_evaluations,
        observed_mean_capped_recall=payload_document["observed_mean_capped_recall"],
        observed_p95_latency_ms=payload_document["observed_p95_latency_ms"],
        contributing_observation_count=payload_document["contributing_observation_count"],
        canonical_epoch_evaluation_digest=canonical_epoch_evaluation_digest,
    )


# -----------------------------------------------------------------------------
# Artifact 6: LkgQualificationEvaluation
# -----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class LkgQualificationEvaluation:
    """Immutable, top-level LKG qualification evaluation artifact for one run."""

    evaluation_schema_version: int
    source_run_id: str
    source_run_binding_sha256: str
    source_run_seal_digest: str
    source_sealed_phase1_chain_head_sha256: str
    qualification_dataset_id: str
    qualification_dataset_version: str
    qualification_manifest_sha256: str
    qualification_query_role: str
    qualification_ordered_query_ids_sha256: str
    evaluated_ef: int
    search_configuration_digest: str
    phase2_source_binding_digest: str
    window_ingestion_digests: tuple[str | None, ...]
    evaluation_contract: LkgQualificationEvaluationContract
    ef_eligibility_rule: LkgEfEligibilityRule
    qualification_semantics_rule: LkgQualificationSemanticsRule
    window_evaluations: tuple[LkgWindowEvaluation, ...]
    epoch_evaluations: tuple[LkgEpochEvaluation, ...]
    status: LkgQualificationStatus
    status_reason_codes: tuple[str, ...]
    qualified: bool
    evaluator_identity: str
    evaluator_source_revision: str
    evaluated_at_utc: str
    canonical_evaluation_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "evaluation_schema_version",
            _positive_int(self.evaluation_schema_version, field="evaluation_schema_version"),
        )
        if self.evaluation_schema_version != QUALIFICATION_EVALUATION_SCHEMA_VERSION:
            raise ContractViolation(
                f"evaluation_schema_version must equal {QUALIFICATION_EVALUATION_SCHEMA_VERSION}"
            )

        object.__setattr__(self, "source_run_id", _canonical_text(self.source_run_id, field="source_run_id"))
        object.__setattr__(
            self,
            "source_run_binding_sha256",
            _sha256_hex(self.source_run_binding_sha256, field="source_run_binding_sha256"),
        )
        object.__setattr__(
            self,
            "source_run_seal_digest",
            _sha256_hex(self.source_run_seal_digest, field="source_run_seal_digest"),
        )
        object.__setattr__(
            self,
            "source_sealed_phase1_chain_head_sha256",
            _sha256_hex(
                self.source_sealed_phase1_chain_head_sha256,
                field="source_sealed_phase1_chain_head_sha256",
            ),
        )

        object.__setattr__(
            self,
            "qualification_dataset_id",
            _canonical_text(self.qualification_dataset_id, field="qualification_dataset_id"),
        )
        object.__setattr__(
            self,
            "qualification_dataset_version",
            _canonical_text(
                self.qualification_dataset_version, field="qualification_dataset_version"
            ),
        )
        object.__setattr__(
            self,
            "qualification_manifest_sha256",
            _sha256_hex(self.qualification_manifest_sha256, field="qualification_manifest_sha256"),
        )
        object.__setattr__(
            self,
            "qualification_query_role",
            _canonical_text(self.qualification_query_role, field="qualification_query_role"),
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
            self, "evaluated_ef", _positive_int(self.evaluated_ef, field="evaluated_ef")
        )
        object.__setattr__(
            self,
            "search_configuration_digest",
            _sha256_hex(self.search_configuration_digest, field="search_configuration_digest"),
        )
        object.__setattr__(
            self,
            "phase2_source_binding_digest",
            _sha256_hex(
                self.phase2_source_binding_digest, field="phase2_source_binding_digest"
            ),
        )

        if not isinstance(self.window_ingestion_digests, tuple) or len(self.window_ingestion_digests) != _WINDOWS_PER_RUN:
            raise ContractViolation(
                f"window_ingestion_digests must be a tuple of exactly {_WINDOWS_PER_RUN} elements (str or None)"
            )
        for item in self.window_ingestion_digests:
            if item is not None:
                _sha256_hex(item, field="window_ingestion_digests entry")

        if not isinstance(self.evaluation_contract, LkgQualificationEvaluationContract):
            raise ContractViolation("evaluation_contract must be an LkgQualificationEvaluationContract")
        if not isinstance(self.ef_eligibility_rule, LkgEfEligibilityRule):
            raise ContractViolation("ef_eligibility_rule must be an LkgEfEligibilityRule")
        if not isinstance(self.qualification_semantics_rule, LkgQualificationSemanticsRule):
            raise ContractViolation("qualification_semantics_rule must be an LkgQualificationSemanticsRule")

        if not isinstance(self.window_evaluations, tuple) or len(self.window_evaluations) != _WINDOWS_PER_RUN:
            raise ContractViolation(
                f"window_evaluations must be a tuple of exactly {_WINDOWS_PER_RUN} LkgWindowEvaluation objects"
            )
        for idx, we in enumerate(self.window_evaluations):
            if not isinstance(we, LkgWindowEvaluation):
                raise ContractViolation("window_evaluations entries must be LkgWindowEvaluation instances")
            if we.window_index != idx:
                raise ContractViolation("window_evaluations must be ordered by window_index 0..11")
            if self.window_ingestion_digests[idx] != we.readiness_ingestion_digest:
                raise ContractViolation(
                    f"window_ingestion_digests[{idx}] must match window_evaluations[{idx}].readiness_ingestion_digest"
                )

        if not isinstance(self.epoch_evaluations, tuple) or len(self.epoch_evaluations) != _EPOCHS_PER_RUN:
            raise ContractViolation(
                f"epoch_evaluations must be a tuple of exactly {_EPOCHS_PER_RUN} LkgEpochEvaluation objects"
            )
        for idx, ee in enumerate(self.epoch_evaluations):
            if not isinstance(ee, LkgEpochEvaluation):
                raise ContractViolation("epoch_evaluations entries must be LkgEpochEvaluation instances")
            if ee.epoch_index != idx:
                raise ContractViolation("epoch_evaluations must be ordered by epoch_index 0..1")
            embedded_slice = self.window_evaluations[idx * _WINDOWS_PER_EPOCH : (idx + 1) * _WINDOWS_PER_EPOCH]
            if ee.window_evaluations != embedded_slice:
                raise ContractViolation(
                    f"epoch_evaluations[{idx}] embedded windows must match top-level window evaluations slice"
                )

            if all(
                window.status is LkgQualificationStatus.PASSING
                for window in ee.window_evaluations
            ):
                expected_slo_reasons: set[str] = set()
                if ee.observed_mean_capped_recall is None:
                    raise ContractViolation(
                        "an all-PASSING-window epoch must carry observed recall"
                    )
                if ee.observed_p95_latency_ms is None:
                    raise ContractViolation(
                        "an all-PASSING-window epoch must carry observed latency"
                    )
                if (
                    ee.observed_mean_capped_recall
                    < self.evaluation_contract.recall_floor
                ):
                    expected_slo_reasons.add("EPOCH_RECALL_BELOW_FLOOR")
                if (
                    ee.observed_p95_latency_ms
                    > self.evaluation_contract.latency_ceiling_ms
                ):
                    expected_slo_reasons.add("EPOCH_LATENCY_ABOVE_CEILING")
                expected_epoch_status = (
                    LkgQualificationStatus.FAILING
                    if expected_slo_reasons
                    else LkgQualificationStatus.PASSING
                )
                if ee.status is not expected_epoch_status or ee.status_reason_codes != tuple(
                    sorted(expected_slo_reasons)
                ):
                    raise ContractViolation(
                        "epoch status and reasons must match its observed statistics under the final evaluation contract"
                    )

        if not isinstance(self.status, LkgQualificationStatus):
            raise ContractViolation("status must be an LkgQualificationStatus member")
        object.__setattr__(
            self,
            "status_reason_codes",
            _validate_reason_codes(self.status_reason_codes, allowed=_FINAL_REASON_CODES),
        )

        expected_reason_codes = {
            code
            for epoch in self.epoch_evaluations
            for code in epoch.status_reason_codes
        }
        if self.evaluated_ef not in self.ef_eligibility_rule.eligible_ef_values:
            expected_status = LkgQualificationStatus.FAILING
            expected_reason_codes.add("EF_NOT_ELIGIBLE_FOR_LKG")
        elif any(
            epoch.status is LkgQualificationStatus.FAILING
            for epoch in self.epoch_evaluations
        ):
            expected_status = LkgQualificationStatus.FAILING
        elif any(
            epoch.status is LkgQualificationStatus.INCOMPLETE
            for epoch in self.epoch_evaluations
        ):
            expected_status = LkgQualificationStatus.INCOMPLETE
        else:
            expected_status = LkgQualificationStatus.PASSING
        if expected_status is LkgQualificationStatus.INCOMPLETE:
            if "POSITION_MISSING" in expected_reason_codes:
                expected_reason_codes.add(
                    "PHASE1_POSITION_PERMANENTLY_MISSING"
                )
            if any(digest is None for digest in self.window_ingestion_digests):
                expected_reason_codes.add("AWAITING_READINESS_EVIDENCE")
        if self.status is not expected_status:
            raise ContractViolation(
                "final status must follow ef eligibility and FAILING > INCOMPLETE > PASSING epoch precedence"
            )
        if self.status_reason_codes != tuple(sorted(expected_reason_codes)):
            raise ContractViolation(
                "final status_reason_codes must exactly bind ef and epoch-level reasons"
            )

        object.__setattr__(self, "qualified", _bool(self.qualified, field="qualified"))
        if self.qualified != (self.status is LkgQualificationStatus.PASSING):
            raise ContractViolation("qualified must equal (status == LkgQualificationStatus.PASSING)")

        if self.status is LkgQualificationStatus.PASSING:
            if any(digest is None for digest in self.window_ingestion_digests):
                raise ContractViolation("PASSING final evaluation requires all 12 non-None window_ingestion_digests")
            if self.status_reason_codes:
                raise ContractViolation("PASSING final evaluation cannot carry reason codes")
        elif self.status is LkgQualificationStatus.FAILING:
            if not any(code in _FAILING_REASON_CODES for code in self.status_reason_codes):
                raise ContractViolation("FAILING final evaluation requires at least one FAILING reason code")
        elif self.status is LkgQualificationStatus.INCOMPLETE:
            if any(code in _FAILING_REASON_CODES for code in self.status_reason_codes):
                raise ContractViolation("INCOMPLETE final evaluation cannot carry FAILING reason codes")

        object.__setattr__(
            self, "evaluator_identity", _canonical_text(self.evaluator_identity, field="evaluator_identity")
        )
        object.__setattr__(
            self,
            "evaluator_source_revision",
            _canonical_text(self.evaluator_source_revision, field="evaluator_source_revision"),
        )
        object.__setattr__(
            self, "evaluated_at_utc", validate_rfc3339_utc(self.evaluated_at_utc, field="evaluated_at_utc")
        )

        object.__setattr__(
            self,
            "canonical_evaluation_digest",
            _sha256_hex(self.canonical_evaluation_digest, field="canonical_evaluation_digest"),
        )

        recomputed_payload = evaluation_payload_document(self)
        recomputed_digest = evaluation_payload_document_digest(recomputed_payload)
        if recomputed_digest != self.canonical_evaluation_digest:
            raise ContractViolation(
                "canonical_evaluation_digest does not match the recomputed payload digest"
            )


_QUALIFICATION_EVALUATION_PAYLOAD_FIELDS = frozenset(
    {
        "evaluation_schema_version",
        "source_run_id",
        "source_run_binding_sha256",
        "source_run_seal_digest",
        "source_sealed_phase1_chain_head_sha256",
        "qualification_dataset_id",
        "qualification_dataset_version",
        "qualification_manifest_sha256",
        "qualification_query_role",
        "qualification_ordered_query_ids_sha256",
        "evaluated_ef",
        "search_configuration_digest",
        "phase2_source_binding_digest",
        "window_ingestion_digests",
        "evaluation_contract",
        "ef_eligibility_rule",
        "qualification_semantics_rule",
        "window_evaluations",
        "epoch_evaluations",
        "status",
        "status_reason_codes",
        "qualified",
        "evaluator_identity",
        "evaluator_source_revision",
        "evaluated_at_utc",
    }
)


def evaluation_payload_document(evaluation: LkgQualificationEvaluation) -> dict[str, object]:
    if not isinstance(evaluation, LkgQualificationEvaluation):
        raise ContractViolation("evaluation must be an LkgQualificationEvaluation")
    return {
        "evaluation_schema_version": evaluation.evaluation_schema_version,
        "source_run_id": evaluation.source_run_id,
        "source_run_binding_sha256": evaluation.source_run_binding_sha256,
        "source_run_seal_digest": evaluation.source_run_seal_digest,
        "source_sealed_phase1_chain_head_sha256": evaluation.source_sealed_phase1_chain_head_sha256,
        "qualification_dataset_id": evaluation.qualification_dataset_id,
        "qualification_dataset_version": evaluation.qualification_dataset_version,
        "qualification_manifest_sha256": evaluation.qualification_manifest_sha256,
        "qualification_query_role": evaluation.qualification_query_role,
        "qualification_ordered_query_ids_sha256": evaluation.qualification_ordered_query_ids_sha256,
        "evaluated_ef": evaluation.evaluated_ef,
        "search_configuration_digest": evaluation.search_configuration_digest,
        "phase2_source_binding_digest": evaluation.phase2_source_binding_digest,
        "window_ingestion_digests": list(evaluation.window_ingestion_digests),
        "evaluation_contract": evaluation_contract_payload_document(evaluation.evaluation_contract),
        "ef_eligibility_rule": ef_eligibility_rule_payload_document(evaluation.ef_eligibility_rule),
        "qualification_semantics_rule": qualification_semantics_rule_payload_document(
            evaluation.qualification_semantics_rule
        ),
        "window_evaluations": [
            window_evaluation_payload_document(we) for we in evaluation.window_evaluations
        ],
        "epoch_evaluations": [
            epoch_evaluation_payload_document(ee) for ee in evaluation.epoch_evaluations
        ],
        "status": evaluation.status.value,
        "status_reason_codes": list(evaluation.status_reason_codes),
        "qualified": evaluation.qualified,
        "evaluator_identity": evaluation.evaluator_identity,
        "evaluator_source_revision": evaluation.evaluator_source_revision,
        "evaluated_at_utc": evaluation.evaluated_at_utc,
    }


def evaluation_payload_document_digest(payload_document: dict[str, object]) -> str:
    return hashlib.sha256(
        QUALIFICATION_EVALUATION_DOMAIN + canonical_json_bytes(payload_document)
    ).hexdigest()


def lkg_qualification_evaluation_from_payload(
    payload_document: object, *, canonical_evaluation_digest: str
) -> LkgQualificationEvaluation:
    if (
        not isinstance(payload_document, dict)
        or set(payload_document) != _QUALIFICATION_EVALUATION_PAYLOAD_FIELDS
    ):
        raise ContractViolation(
            "qualification evaluation payload document must contain exactly expected fields"
        )
    try:
        status = LkgQualificationStatus(payload_document["status"])
    except ValueError as exc:
        raise ContractViolation("status must be a known LkgQualificationStatus value") from exc

    contract_doc = payload_document["evaluation_contract"]
    if not isinstance(contract_doc, dict):
        raise ContractViolation("evaluation_contract must be an object")
    contract = lkg_qualification_evaluation_contract_from_payload(
        contract_doc,
        canonical_contract_digest=evaluation_contract_payload_document_digest(contract_doc),
    )

    ef_rule_doc = payload_document["ef_eligibility_rule"]
    if not isinstance(ef_rule_doc, dict):
        raise ContractViolation("ef_eligibility_rule must be an object")
    ef_rule = lkg_ef_eligibility_rule_from_payload(
        ef_rule_doc,
        canonical_rule_digest=ef_eligibility_rule_payload_document_digest(ef_rule_doc),
    )

    sem_rule_doc = payload_document["qualification_semantics_rule"]
    if not isinstance(sem_rule_doc, dict):
        raise ContractViolation("qualification_semantics_rule must be an object")
    sem_rule = lkg_qualification_semantics_rule_from_payload(
        sem_rule_doc,
        canonical_rule_digest=qualification_semantics_rule_payload_document_digest(sem_rule_doc),
    )

    window_evals_val = payload_document["window_evaluations"]
    if not isinstance(window_evals_val, list):
        raise ContractViolation("window_evaluations must be a list")
    window_evaluations_list: list[LkgWindowEvaluation] = []
    for we_doc in window_evals_val:
        if not isinstance(we_doc, dict):
            raise ContractViolation("window_evaluations entries must be objects")
        window_evaluations_list.append(
            lkg_window_evaluation_from_payload(
                we_doc,
                canonical_window_evaluation_digest=window_evaluation_payload_document_digest(
                    we_doc
                ),
            )
        )
    window_evaluations = tuple(window_evaluations_list)

    epoch_evals_val = payload_document["epoch_evaluations"]
    if not isinstance(epoch_evals_val, list):
        raise ContractViolation("epoch_evaluations must be a list")
    epoch_evaluations_list: list[LkgEpochEvaluation] = []
    for ee_doc in epoch_evals_val:
        if not isinstance(ee_doc, dict):
            raise ContractViolation("epoch_evaluations entries must be objects")
        epoch_evaluations_list.append(
            lkg_epoch_evaluation_from_payload(
                ee_doc,
                canonical_epoch_evaluation_digest=epoch_evaluation_payload_document_digest(
                    ee_doc
                ),
            )
        )
    epoch_evaluations = tuple(epoch_evaluations_list)

    ingestion_digests = payload_document["window_ingestion_digests"]
    if not isinstance(ingestion_digests, list):
        raise ContractViolation("window_ingestion_digests must be a list")

    return LkgQualificationEvaluation(
        evaluation_schema_version=payload_document["evaluation_schema_version"],
        source_run_id=payload_document["source_run_id"],
        source_run_binding_sha256=payload_document["source_run_binding_sha256"],
        source_run_seal_digest=payload_document["source_run_seal_digest"],
        source_sealed_phase1_chain_head_sha256=payload_document["source_sealed_phase1_chain_head_sha256"],
        qualification_dataset_id=payload_document["qualification_dataset_id"],
        qualification_dataset_version=payload_document["qualification_dataset_version"],
        qualification_manifest_sha256=payload_document["qualification_manifest_sha256"],
        qualification_query_role=payload_document["qualification_query_role"],
        qualification_ordered_query_ids_sha256=payload_document["qualification_ordered_query_ids_sha256"],
        evaluated_ef=payload_document["evaluated_ef"],
        search_configuration_digest=payload_document["search_configuration_digest"],
        phase2_source_binding_digest=payload_document["phase2_source_binding_digest"],
        window_ingestion_digests=tuple(ingestion_digests),
        evaluation_contract=contract,
        ef_eligibility_rule=ef_rule,
        qualification_semantics_rule=sem_rule,
        window_evaluations=window_evaluations,
        epoch_evaluations=epoch_evaluations,
        status=status,
        status_reason_codes=_string_list(
            payload_document["status_reason_codes"], field="status_reason_codes"
        ),
        qualified=payload_document["qualified"],
        evaluator_identity=payload_document["evaluator_identity"],
        evaluator_source_revision=payload_document["evaluator_source_revision"],
        evaluated_at_utc=payload_document["evaluated_at_utc"],
        canonical_evaluation_digest=canonical_evaluation_digest,
    )


# -----------------------------------------------------------------------------
# Pure Evaluation Functions
# -----------------------------------------------------------------------------

def evaluate_window(
    *,
    window_index: int,
    position_classifications: tuple[LkgPositionClassification, ...],
    contributing_attempts: tuple[LkgQueryAttempt, ...],
    readiness_ingestion: LkgWindowReadinessIngestion | None,
    source_run_binding_sha256: str,
    search_configuration: SearchConfiguration,
) -> LkgWindowEvaluation:
    """Pure evaluation of exactly one 200-position constituent window.

    Requires exact 1-to-1 mechanical proof between every CLEAN_SUCCESS
    position classification and its corresponding verified observation,
    including that the observation's own identity duplicates
    (``ef``/``metric``/``threshold_stratum``) and the attempt's own
    ``run_binding_sha256`` agree with the run's freshly verified binding --
    nothing else in this evaluation proves that agreement.
    """

    if isinstance(window_index, bool) or not isinstance(window_index, int) or not 0 <= window_index < _WINDOWS_PER_RUN:
        raise ContractViolation(f"window_index must be in [0, {_WINDOWS_PER_RUN})")
    if not isinstance(position_classifications, tuple):
        raise ContractViolation("position_classifications must be a tuple")
    if len(position_classifications) != _EXPECTED_QUERY_COUNT:
        raise ContractViolation(f"position_classifications must contain {_EXPECTED_QUERY_COUNT} entries")
    for index, position in enumerate(position_classifications):
        if not isinstance(position, LkgPositionClassification):
            raise ContractViolation(
                "position_classifications entries must be LkgPositionClassification objects"
            )
        if position.attempt_sequence != index:
            raise ContractViolation(
                "position_classifications must be ordered by contiguous attempt_sequence"
            )
    if not isinstance(contributing_attempts, tuple):
        raise ContractViolation("contributing_attempts must be a tuple")
    source_run_binding_sha256 = _sha256_hex(
        source_run_binding_sha256, field="source_run_binding_sha256"
    )
    if not isinstance(search_configuration, SearchConfiguration):
        raise ContractViolation("search_configuration must be a SearchConfiguration")
    search_configuration.validate()
    if search_configuration.ef is None:
        raise ContractViolation("LKG evaluation requires an explicit HNSW ef")

    epoch_index = window_index // _WINDOWS_PER_EPOCH
    first_seq = window_index * _POSITIONS_PER_WINDOW
    last_seq = first_seq + _POSITIONS_PER_WINDOW - 1

    window_positions = position_classifications[first_seq : last_seq + 1]
    clean_positions = [
        p for p in window_positions if p.classification is LkgPositionStatus.CLEAN_SUCCESS
    ]
    clean_success_count = len(clean_positions)
    failed_count = sum(
        1 for p in window_positions if p.classification is LkgPositionStatus.FAILED
    )
    malformed_count = sum(
        1 for p in window_positions if p.classification is LkgPositionStatus.MALFORMED
    )
    missing_count = sum(
        1 for p in window_positions if p.classification is LkgPositionStatus.MISSING
    )

    # Cross-check 1-to-1 matching for every CLEAN_SUCCESS position
    if len(contributing_attempts) != clean_success_count:
        raise ContractViolation(
            f"contributing_attempts count ({len(contributing_attempts)}) must equal clean_success_position_count ({clean_success_count})"
        )

    # Index attempts by sequence for strict 1-to-1 verification
    att_by_seq: dict[int, LkgQueryAttempt] = {}
    for att in contributing_attempts:
        if not isinstance(att, LkgQueryAttempt):
            raise ContractViolation("contributing_attempts entries must be LkgQueryAttempt objects")
        if att.attempt_sequence in att_by_seq:
            raise ContractViolation(f"duplicate contributing attempt for sequence {att.attempt_sequence}")
        att_by_seq[att.attempt_sequence] = att

    correctness_failed = False
    for pos in clean_positions:
        att = att_by_seq.get(pos.attempt_sequence)
        if att is None:
            raise ContractViolation(
                f"Missing raw evidence contributor for CLEAN_SUCCESS position sequence {pos.attempt_sequence}"
            )
        if att.query_id != pos.query_id:
            raise ContractViolation(
                f"Contributor query_id mismatch at sequence {pos.attempt_sequence}: attempt={att.query_id}, seal={pos.query_id}"
            )
        if att.status is not LkgAttemptStatus.SUCCESS or att.observation is None:
            raise ContractViolation(
                f"Contributor at sequence {pos.attempt_sequence} must be SUCCESS with non-None observation"
            )
        if att.attempt_sequence != pos.attempt_sequence:
            raise ContractViolation(
                f"Contributor attempt_sequence mismatch at sequence {pos.attempt_sequence}"
            )
        if att.run_binding_sha256 != source_run_binding_sha256:
            raise ContractViolation(
                f"Contributor run_binding_sha256 mismatch at sequence {pos.attempt_sequence}: "
                f"attempt={att.run_binding_sha256}, expected={source_run_binding_sha256}"
            )
        if att.observation.query_id != att.query_id:
            raise ContractViolation(
                f"Observation query_id mismatch at sequence {pos.attempt_sequence}: "
                f"observation={att.observation.query_id}, attempt={att.query_id}"
            )
        if att.observation.ef != search_configuration.ef:
            raise ContractViolation(
                f"Observation ef mismatch at sequence {pos.attempt_sequence}: "
                f"obs={att.observation.ef}, expected={search_configuration.ef}"
            )
        if att.observation.metric != search_configuration.metric:
            raise ContractViolation(
                f"Observation metric mismatch at sequence {pos.attempt_sequence}: "
                f"obs={att.observation.metric}, expected={search_configuration.metric}"
            )
        if att.observation.threshold_stratum != search_configuration.threshold_label:
            raise ContractViolation(
                f"Observation threshold_stratum mismatch at sequence {pos.attempt_sequence}: "
                f"obs={att.observation.threshold_stratum!r}, "
                f"expected={search_configuration.threshold_label!r}"
            )
        recall = _finite_float(att.observation.recall, field="observation.recall")
        if not 0.0 <= recall <= 1.0:
            raise ContractViolation("observation.recall must be in [0.0, 1.0]")
        latency_ms = _finite_float(
            att.observation.latency_ms, field="observation.latency_ms"
        )
        if latency_ms < 0.0:
            raise ContractViolation("observation.latency_ms must be non-negative")
        _nonnegative_int(
            att.observation.threshold_violation_count,
            field="observation.threshold_violation_count",
        )
        if att.observation.threshold_violation_count > 0:
            correctness_failed = True

    reasons: list[str] = []
    if failed_count > 0:
        reasons.append("POSITION_FAILED")
    if malformed_count > 0:
        reasons.append("POSITION_MALFORMED")
    if missing_count > 0:
        reasons.append("POSITION_MISSING")

    if correctness_failed:
        reasons.append("QUERY_CORRECTNESS_FAILED")

    if readiness_ingestion is None:
        reasons.append("READINESS_MISSING")
    else:
        if readiness_ingestion.window_index != window_index:
            raise ContractViolation("readiness ingestion window_index does not match evaluated window")
        if readiness_ingestion.epoch_index != epoch_index:
            raise ContractViolation("readiness ingestion epoch_index does not match evaluated epoch")
        if (
            readiness_ingestion.original_evidence.first_attempt_sequence != first_seq
            or readiness_ingestion.original_evidence.last_attempt_sequence != last_seq
        ):
            raise ContractViolation("readiness ingestion sequence range does not match evaluated window")
        orig = readiness_ingestion.original_evidence
        if not orig.health_checked:
            reasons.append("HEALTH_NOT_CHECKED")
        elif not orig.health_passed:
            reasons.append("HEALTH_FAILED")

        if not orig.rollback_tested:
            reasons.append("ROLLBACK_NOT_TESTED")
        elif not orig.rollback_ready:
            reasons.append("ROLLBACK_NOT_READY")

    unique_reasons = tuple(sorted(set(reasons)))

    if any(code in _FAILING_REASON_CODES for code in unique_reasons):
        status = LkgQualificationStatus.FAILING
    elif any(code in _INCOMPLETE_REASON_CODES for code in unique_reasons):
        status = LkgQualificationStatus.INCOMPLETE
    else:
        status = LkgQualificationStatus.PASSING

    readiness_digest = (
        readiness_ingestion.canonical_ingestion_digest if readiness_ingestion else None
    )

    payload = {
        "window_evaluation_schema_version": WINDOW_EVALUATION_SCHEMA_VERSION,
        "window_index": window_index,
        "epoch_index": epoch_index,
        "first_attempt_sequence": first_seq,
        "last_attempt_sequence": last_seq,
        "status": status.value,
        "status_reason_codes": list(unique_reasons),
        "clean_success_position_count": clean_success_count,
        "failed_position_count": failed_count,
        "malformed_position_count": malformed_count,
        "missing_position_count": missing_count,
        "contributing_observation_count": clean_success_count,
        "readiness_ingestion_digest": readiness_digest,
    }
    digest = window_evaluation_payload_document_digest(payload)
    return lkg_window_evaluation_from_payload(payload, canonical_window_evaluation_digest=digest)


def evaluate_epoch(
    *,
    epoch_index: int,
    window_evaluations: tuple[LkgWindowEvaluation, ...],
    epoch_contributing_attempts: tuple[LkgQueryAttempt, ...],
    contract: LkgQualificationEvaluationContract,
) -> LkgEpochEvaluation:
    """Pure evaluation of one 1,200-observation epoch."""

    if not 0 <= epoch_index < _EPOCHS_PER_RUN:
        raise ContractViolation(f"epoch_index must be in [0, {_EPOCHS_PER_RUN})")
    if not isinstance(window_evaluations, tuple):
        raise ContractViolation("window_evaluations must be a tuple")
    if len(window_evaluations) != _WINDOWS_PER_EPOCH:
        raise ContractViolation(f"window_evaluations must contain {_WINDOWS_PER_EPOCH} entries")
    if not isinstance(epoch_contributing_attempts, tuple):
        raise ContractViolation("epoch_contributing_attempts must be a tuple")
    if not isinstance(contract, LkgQualificationEvaluationContract):
        raise ContractViolation("contract must be an LkgQualificationEvaluationContract")

    first_win = epoch_index * _WINDOWS_PER_EPOCH
    last_win = first_win + _WINDOWS_PER_EPOCH - 1

    for idx, we in enumerate(window_evaluations):
        if not isinstance(we, LkgWindowEvaluation):
            raise ContractViolation("window_evaluations entries must be LkgWindowEvaluation objects")
        if we.window_index != first_win + idx:
            raise ContractViolation("window_evaluations must be consecutive for the epoch")

    expected_contributing_count = sum(
        window.contributing_observation_count for window in window_evaluations
    )
    if len(epoch_contributing_attempts) != expected_contributing_count:
        raise ContractViolation(
            "epoch_contributing_attempts count must equal the sum of constituent-window contributors"
        )
    seen_sequences: set[int] = set()
    first_sequence = first_win * _POSITIONS_PER_WINDOW
    last_sequence = (last_win + 1) * _POSITIONS_PER_WINDOW - 1
    for attempt in epoch_contributing_attempts:
        if not isinstance(attempt, LkgQueryAttempt):
            raise ContractViolation(
                "epoch_contributing_attempts entries must be LkgQueryAttempt objects"
            )
        if attempt.attempt_sequence in seen_sequences:
            raise ContractViolation("epoch_contributing_attempts contain a duplicate sequence")
        if not first_sequence <= attempt.attempt_sequence <= last_sequence:
            raise ContractViolation("epoch contributor falls outside the epoch sequence range")
        seen_sequences.add(attempt.attempt_sequence)

    reasons: list[str] = []
    any_failing = any(we.status is LkgQualificationStatus.FAILING for we in window_evaluations)
    any_incomplete = any(we.status is LkgQualificationStatus.INCOMPLETE for we in window_evaluations)

    if any_failing:
        status = LkgQualificationStatus.FAILING
        for we in window_evaluations:
            for code in we.status_reason_codes:
                if code in _FAILING_REASON_CODES:
                    reasons.append(code)
        observed_mean_recall = None
        observed_p95_latency = None
        contributing_count = expected_contributing_count
    elif any_incomplete:
        status = LkgQualificationStatus.INCOMPLETE
        for we in window_evaluations:
            for code in we.status_reason_codes:
                if code in _INCOMPLETE_REASON_CODES:
                    reasons.append(code)
        observed_mean_recall = None
        observed_p95_latency = None
        contributing_count = expected_contributing_count
    else:
        # All 6 constituent windows PASSING
        contributing_count = len(epoch_contributing_attempts)
        if contributing_count != _OBSERVATIONS_PER_EPOCH:
            raise ContractViolation(
                f"PASSING constituent windows require exactly {_OBSERVATIONS_PER_EPOCH} contributing observations"
            )

        recalls: list[float] = []
        latencies: list[float] = []
        for att in epoch_contributing_attempts:
            if att.observation is None:
                raise ContractViolation("contributing attempt missing observation")
            recalls.append(_finite_float(att.observation.recall, field="recall"))
            latencies.append(_finite_float(att.observation.latency_ms, field="latency_ms"))

        observed_mean_recall = math.fsum(recalls) / float(_OBSERVATIONS_PER_EPOCH)
        sorted_latencies = sorted(latencies)

        # Dynamic contract percentile rank derivation
        p95_rank = math.ceil(contract.latency_percentile * contract.observations_per_epoch)
        p95_index = p95_rank - 1
        observed_p95_latency = sorted_latencies[p95_index]

        if observed_mean_recall < contract.recall_floor:
            reasons.append("EPOCH_RECALL_BELOW_FLOOR")
        if observed_p95_latency > contract.latency_ceiling_ms:
            reasons.append("EPOCH_LATENCY_ABOVE_CEILING")

        if reasons:
            status = LkgQualificationStatus.FAILING
        else:
            status = LkgQualificationStatus.PASSING

    unique_reasons = tuple(sorted(set(reasons)))
    payload = {
        "epoch_evaluation_schema_version": EPOCH_EVALUATION_SCHEMA_VERSION,
        "epoch_index": epoch_index,
        "first_window_index": first_win,
        "last_window_index": last_win,
        "status": status.value,
        "status_reason_codes": list(unique_reasons),
        "window_evaluations": [window_evaluation_payload_document(we) for we in window_evaluations],
        "observed_mean_capped_recall": observed_mean_recall,
        "observed_p95_latency_ms": observed_p95_latency,
        "contributing_observation_count": contributing_count,
    }
    digest = epoch_evaluation_payload_document_digest(payload)
    return lkg_epoch_evaluation_from_payload(payload, canonical_epoch_evaluation_digest=digest)


def evaluate_run(
    *,
    seal: LkgRunSeal,
    attempts: tuple[LkgQueryAttempt, ...],
    ingestions: tuple[LkgWindowReadinessIngestion, ...],
    contract: LkgQualificationEvaluationContract,
    ef_rule: LkgEfEligibilityRule,
    semantics_rule: LkgQualificationSemanticsRule,
    search_configuration: SearchConfiguration,
    phase2_source_binding_digest: str,
    evaluator_identity: str,
    evaluator_source_revision: str,
    evaluated_at_utc: str,
) -> LkgQualificationEvaluation:
    """Pure evaluation function converting verified evidence into one LkgQualificationEvaluation.

    ``search_configuration`` is the run's freshly verified bound
    ``SearchConfiguration`` -- every contributing
    observation is cross-checked against them (and every contributing
    attempt against ``seal.run_binding_sha256``) inside ``evaluate_window``;
    a mismatch is a source/integrity error (``ContractViolation``), never a
    qualification status.
    """

    if not isinstance(seal, LkgRunSeal):
        raise ContractViolation("seal must be an LkgRunSeal")
    if seal.expected_query_count != _EXPECTED_QUERY_COUNT:
        raise ContractViolation("seal expected_query_count must equal 2400")
    if not isinstance(attempts, tuple):
        raise ContractViolation("attempts must be a tuple")
    if not isinstance(ingestions, tuple):
        raise ContractViolation("ingestions must be a tuple")
    if not isinstance(contract, LkgQualificationEvaluationContract):
        raise ContractViolation("contract must be an LkgQualificationEvaluationContract")
    if not isinstance(ef_rule, LkgEfEligibilityRule):
        raise ContractViolation("ef_rule must be an LkgEfEligibilityRule")
    if not isinstance(semantics_rule, LkgQualificationSemanticsRule):
        raise ContractViolation("semantics_rule must be an LkgQualificationSemanticsRule")
    if not isinstance(search_configuration, SearchConfiguration):
        raise ContractViolation("search_configuration must be a SearchConfiguration")
    search_configuration.validate()
    if search_configuration.ef is None:
        raise ContractViolation("LKG evaluation requires an explicit HNSW ef")
    phase2_source_binding_digest = _sha256_hex(
        phase2_source_binding_digest, field="phase2_source_binding_digest"
    )
    search_configuration_digest = search_configuration_sha256(search_configuration)
    evaluated_ef = search_configuration.ef

    ingestion_map: dict[int, LkgWindowReadinessIngestion] = {}
    ingestion_digests: list[str | None] = [None] * _WINDOWS_PER_RUN
    for ing in ingestions:
        if not isinstance(ing, LkgWindowReadinessIngestion):
            raise ContractViolation("ingestions entries must be LkgWindowReadinessIngestion objects")
        if not 0 <= ing.window_index < _WINDOWS_PER_RUN:
            raise ContractViolation("readiness ingestion window_index must be in [0, 12)")
        if ing.window_index in ingestion_map:
            raise ContractViolation(f"duplicate readiness ingestion for window_index {ing.window_index}")
        if ing.source_run_id != seal.run_id:
            raise ContractViolation("readiness ingestion source_run_id does not match the seal")
        if ing.source_run_seal_digest != seal.canonical_seal_document_digest:
            raise ContractViolation("readiness ingestion source_run_seal_digest does not match the seal")
        if ing.phase2_source_binding_digest != phase2_source_binding_digest:
            raise ContractViolation(
                "readiness ingestion phase2_source_binding_digest does not match the evaluated source binding"
            )
        if (
            ing.original_evidence.source_run_binding_sha256
            != seal.run_binding_sha256
        ):
            raise ContractViolation(
                "readiness ingestion original evidence run binding does not match the seal"
            )
        ingestion_map[ing.window_index] = ing
        ingestion_digests[ing.window_index] = ing.canonical_ingestion_digest

    # O(N) raw-evidence index. Only the exact successful contributor for a
    # sealed CLEAN_SUCCESS position enters an evaluation population;
    # successes belonging to FAILED/MALFORMED positions remain durable
    # failure lineage and are never averaged into qualification statistics.
    successful_attempts_by_sequence: dict[int, list[LkgQueryAttempt]] = {}
    for att in attempts:
        if not isinstance(att, LkgQueryAttempt):
            raise ContractViolation("attempts entries must be LkgQueryAttempt objects")
        if not 0 <= att.attempt_sequence < _EXPECTED_QUERY_COUNT:
            raise ContractViolation("attempt_sequence must be in [0, 2400)")
        if att.status is LkgAttemptStatus.SUCCESS:
            successful_attempts_by_sequence.setdefault(att.attempt_sequence, []).append(att)

    contributors_by_window: dict[int, list[LkgQueryAttempt]] = {
        window: [] for window in range(_WINDOWS_PER_RUN)
    }
    for position in seal.position_classifications:
        if position.classification is not LkgPositionStatus.CLEAN_SUCCESS:
            continue
        matches = successful_attempts_by_sequence.get(position.attempt_sequence, [])
        if len(matches) != 1:
            raise ContractViolation(
                "every CLEAN_SUCCESS sealed position must have exactly one successful raw contributor"
            )
        contributors_by_window[position.attempt_sequence // _POSITIONS_PER_WINDOW].append(
            matches[0]
        )

    window_evaluations_list: list[LkgWindowEvaluation] = []
    for w in range(_WINDOWS_PER_RUN):
        we = evaluate_window(
            window_index=w,
            position_classifications=seal.position_classifications,
            contributing_attempts=tuple(contributors_by_window[w]),
            readiness_ingestion=ingestion_map.get(w),
            source_run_binding_sha256=seal.run_binding_sha256,
            search_configuration=search_configuration,
        )
        window_evaluations_list.append(we)
    window_evaluations = tuple(window_evaluations_list)

    epoch_evaluations_list: list[LkgEpochEvaluation] = []
    for e in range(_EPOCHS_PER_RUN):
        e_windows = window_evaluations[e * _WINDOWS_PER_EPOCH : (e + 1) * _WINDOWS_PER_EPOCH]
        e_attempts: list[LkgQueryAttempt] = []
        for w in range(e * _WINDOWS_PER_EPOCH, (e + 1) * _WINDOWS_PER_EPOCH):
            e_attempts.extend(contributors_by_window[w])
        ee = evaluate_epoch(
            epoch_index=e,
            window_evaluations=e_windows,
            epoch_contributing_attempts=tuple(e_attempts),
            contract=contract,
        )
        epoch_evaluations_list.append(ee)
    epoch_evaluations = tuple(epoch_evaluations_list)

    reasons: list[str] = []

    # Determine overall status and collect top-level reason codes
    if evaluated_ef not in ef_rule.eligible_ef_values:
        run_status = LkgQualificationStatus.FAILING
        reasons.append("EF_NOT_ELIGIBLE_FOR_LKG")
    elif any(ee.status is LkgQualificationStatus.FAILING for ee in epoch_evaluations):
        run_status = LkgQualificationStatus.FAILING
    elif any(ee.status is LkgQualificationStatus.INCOMPLETE for ee in epoch_evaluations):
        run_status = LkgQualificationStatus.INCOMPLETE
    else:
        run_status = LkgQualificationStatus.PASSING

    for ee in epoch_evaluations:
        for code in ee.status_reason_codes:
            reasons.append(code)

    if run_status is LkgQualificationStatus.INCOMPLETE:
        if any(
            position.classification is LkgPositionStatus.MISSING
            for position in seal.position_classifications
        ):
            reasons.append("PHASE1_POSITION_PERMANENTLY_MISSING")
        if any(digest is None for digest in ingestion_digests):
            reasons.append("AWAITING_READINESS_EVIDENCE")

    unique_reasons = tuple(sorted(set(reasons)))
    qualified = (run_status is LkgQualificationStatus.PASSING)

    payload = {
        "evaluation_schema_version": QUALIFICATION_EVALUATION_SCHEMA_VERSION,
        "source_run_id": seal.run_id,
        "source_run_binding_sha256": seal.run_binding_sha256,
        "source_run_seal_digest": seal.canonical_seal_document_digest,
        "source_sealed_phase1_chain_head_sha256": seal.final_chain_head_sha256,
        "qualification_dataset_id": seal.workload_identity.dataset_id,
        "qualification_dataset_version": seal.workload_identity.dataset_version,
        "qualification_manifest_sha256": seal.workload_identity.manifest_sha256,
        "qualification_query_role": seal.workload_identity.query_role,
        "qualification_ordered_query_ids_sha256": seal.qualification_ordered_query_ids_sha256,
        "evaluated_ef": evaluated_ef,
        "search_configuration_digest": search_configuration_digest,
        "phase2_source_binding_digest": phase2_source_binding_digest,
        "window_ingestion_digests": ingestion_digests,
        "evaluation_contract": evaluation_contract_payload_document(contract),
        "ef_eligibility_rule": ef_eligibility_rule_payload_document(ef_rule),
        "qualification_semantics_rule": qualification_semantics_rule_payload_document(semantics_rule),
        "window_evaluations": [window_evaluation_payload_document(we) for we in window_evaluations],
        "epoch_evaluations": [epoch_evaluation_payload_document(ee) for ee in epoch_evaluations],
        "status": run_status.value,
        "status_reason_codes": list(unique_reasons),
        "qualified": qualified,
        "evaluator_identity": evaluator_identity,
        "evaluator_source_revision": evaluator_source_revision,
        "evaluated_at_utc": evaluated_at_utc,
    }
    digest = evaluation_payload_document_digest(payload)
    return lkg_qualification_evaluation_from_payload(payload, canonical_evaluation_digest=digest)
