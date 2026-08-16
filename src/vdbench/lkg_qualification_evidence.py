"""Raw per-query LKG-qualification evidence capture (ADR-002 amendment, Phase 1).

Purpose:
    Give the LKG-qualification amendment recorded in ARCHITECTURE.md's ADR-002
    ("LKG qualification amendment", approved 2026-08-06) its first concrete
    evidence types: one immutable record of raw same-call search evidence
    per successful DATASET-003 query (``LkgQueryObservation``), and one
    typed envelope recording every attempted query -- success or failure --
    at its exact intended sequence position (``LkgQueryAttempt``). This is
    ROADMAP.md's LKG-qualification Phase 1 ("raw per-query evidence
    capture") and nothing past it -- constituent/epoch assembly, window-
    level rollback-readiness gating, and every ``policy.py``/
    ``last_known_good.py``/``canary_admission.py`` integration point remain
    Phase 2/3, unimplemented here.
Inputs:
    Facts already computed by one same-call search (recall, latency, raw
    timestamps, oracle cardinality, threshold-violation count), or a typed
    failure classification when that search or its oracle computation did
    not succeed.
Outputs:
    One ``LkgQueryAttempt`` per query the producer dispatches, wrapping
    either an ``LkgQueryObservation`` (status ``SUCCESS``) or ``None`` (any
    other status).
Dependencies:
    ``config.py`` for ``Metric``/``THRESHOLD_LABELS``, ``policy.py`` for
    ``ACTUATION_LADDER`` (only ``ef in {200, 400, 800, 1600}`` can ever
    qualify as last-known-good), and ``actuation.py`` for the shared
    ``QueryId`` alias.
Failure modes:
    ``build_lkg_query_observation``/``build_lkg_query_attempt`` reject any
    input outside this contract before a record is ever constructed.
    Neither performs a search of its own: every argument must already exist
    at the call site.
Scope note (rollback readiness):
    ARCHITECTURE.md's ADR-002 amendment makes rollback-readiness a
    per-constituent-window gate (one check per 200 queries), not a
    per-query measurement. An earlier revision of this module embedded a
    ``RollbackReadinessEvidence`` on every ``LkgQueryObservation``; that was
    the wrong granularity and has been removed. Phase 1 query evidence
    contains only raw same-call query evidence and a reference
    (``run_binding_sha256`` on ``LkgQueryAttempt``) to its immutable run
    binding. Window-level rollback-readiness (e.g. a future
    ``LkgWindowReadinessEvidence``) is Phase 2's responsibility and is not
    designed here.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import StrEnum

from .actuation import QueryId
from .config import THRESHOLD_LABELS, ContractViolation, Metric
from .policy import ACTUATION_LADDER

__all__ = [
    "LkgAttemptStatus",
    "LkgQueryAttempt",
    "LkgQueryObservation",
    "build_lkg_query_attempt",
    "build_lkg_query_observation",
]

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class LkgAttemptStatus(StrEnum):
    """Every possible typed outcome of one dispatched DATASET-003 query.

    ``MALFORMED_RESPONSE`` covers every response-validation defect the
    adapter's own decoding detects (invalid batch shape, duplicate entity
    IDs, non-finite distances, a result count above the configured limit)
    -- these are all defects in what the search response contained, not a
    distinct failure family from "the response was shaped wrong".
    """

    SUCCESS = "SUCCESS"
    CLIENT_ERROR = "CLIENT_ERROR"
    TIMEOUT = "TIMEOUT"
    MALFORMED_RESPONSE = "MALFORMED_RESPONSE"
    ORACLE_ERROR = "ORACLE_ERROR"


@dataclass(frozen=True, slots=True)
class LkgQueryObservation:
    """One raw same-call observation for exactly one successful query.

    Carries only facts already computed by that one search call and its
    paired oracle computation -- no run/dataset identity (that lives on the
    enclosing ``LkgQueryAttempt``/``LkgRunBinding``) and no rollback
    readiness (a window-level Phase 2 concern, not a per-query one).
    """

    query_id: QueryId
    metric: Metric
    threshold_stratum: str
    ef: int
    recall: float
    latency_ms: float
    start_ns: int
    end_ns: int
    exact_cardinality: int
    threshold_violation_count: int


@dataclass(frozen=True, slots=True)
class LkgQueryAttempt:
    """One typed record of exactly one dispatched-query attempt.

    Every attempted query -- successful or not -- becomes exactly one of
    these, so a failure can never simply disappear: ``attempt_sequence`` is
    the query's fixed 0-based position in the workload's ordered query IDs
    (constant across retries), ``attempt_number`` is the 1-based retry
    counter for this exact ``query_id`` (a fresh crash-safe attempt
    identity -- see ``lkg_qualification_producer.py``'s crash-before-append
    semantics), ``error_code`` is a stable, non-sensitive classification
    string present iff ``status`` is not ``SUCCESS``, and ``observation`` is
    the raw same-call evidence present iff ``status`` is ``SUCCESS``.
    """

    query_id: QueryId
    attempt_sequence: int
    attempt_number: int
    status: LkgAttemptStatus
    error_code: str | None
    run_binding_sha256: str
    observation: LkgQueryObservation | None


def _validate_query_id(query_id: object) -> None:
    if isinstance(query_id, bool) or not isinstance(query_id, (int, str)):
        raise ContractViolation("query_id must be a canonical integer or string")
    if isinstance(query_id, str) and not query_id:
        raise ContractViolation("string query_id must be non-empty")


def _validate_unit_interval(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractViolation(f"{field_name} must be a real number")
    numeric = float(value)
    if not math.isfinite(numeric) or not 0.0 <= numeric <= 1.0:
        raise ContractViolation(f"{field_name} must be finite and in [0.0, 1.0]")
    return numeric


def _validate_finite_nonnegative_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractViolation(f"{field_name} must be a real number")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0.0:
        raise ContractViolation(f"{field_name} must be finite and non-negative")
    return numeric


def _validate_nonnegative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractViolation(f"{field_name} must be a non-negative integer")
    return value


def _validate_sha256_hex(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ContractViolation(f"{field_name} must be a lower-case 64-character hex SHA-256 digest")
    return value


def build_lkg_query_observation(
    *,
    query_id: QueryId,
    metric: Metric,
    threshold_stratum: str,
    ef: int,
    recall: float,
    latency_ms: float,
    start_ns: int,
    end_ns: int,
    exact_cardinality: int,
    threshold_violation_count: int,
) -> LkgQueryObservation:
    """Construct one ``LkgQueryObservation`` from already-computed facts.

    Every argument must already exist at the call site before this
    function runs -- it performs no I/O and issues no query of its own.
    """

    _validate_query_id(query_id)
    if not isinstance(metric, Metric):
        raise ContractViolation("metric must be a Metric enum member")
    if not isinstance(threshold_stratum, str) or threshold_stratum not in THRESHOLD_LABELS:
        raise ContractViolation(f"threshold_stratum must be one of {THRESHOLD_LABELS}")
    if isinstance(ef, bool) or not isinstance(ef, int) or ef not in ACTUATION_LADDER:
        raise ContractViolation("ef must be in the ADR-002 actuation ladder")
    validated_recall = _validate_unit_interval(recall, field_name="recall")
    validated_latency_ms = _validate_finite_nonnegative_float(
        latency_ms, field_name="latency_ms"
    )
    if isinstance(start_ns, bool) or not isinstance(start_ns, int) or start_ns < 0:
        raise ContractViolation("start_ns must be a non-negative integer")
    if isinstance(end_ns, bool) or not isinstance(end_ns, int) or end_ns < start_ns:
        raise ContractViolation("end_ns must be a monotonic integer no earlier than start_ns")
    validated_exact_cardinality = _validate_nonnegative_int(
        exact_cardinality, field_name="exact_cardinality"
    )
    validated_violation_count = _validate_nonnegative_int(
        threshold_violation_count, field_name="threshold_violation_count"
    )

    return LkgQueryObservation(
        query_id=query_id,
        metric=metric,
        threshold_stratum=threshold_stratum,
        ef=ef,
        recall=validated_recall,
        latency_ms=validated_latency_ms,
        start_ns=start_ns,
        end_ns=end_ns,
        exact_cardinality=validated_exact_cardinality,
        threshold_violation_count=validated_violation_count,
    )


def build_lkg_query_attempt(
    *,
    query_id: QueryId,
    attempt_sequence: int,
    attempt_number: int,
    status: LkgAttemptStatus,
    run_binding_sha256: str,
    error_code: str | None = None,
    observation: LkgQueryObservation | None = None,
) -> LkgQueryAttempt:
    """Construct one typed ``LkgQueryAttempt``, success or failure.

    Enforces the success/failure shape invariant: a ``SUCCESS`` attempt
    must carry an ``LkgQueryObservation`` and no error code; every other
    status must carry a non-empty error code and no observation. This is
    the single choke point that makes "a failed attempt never counts as a
    successful raw observation" a structural guarantee, not a convention.
    """

    _validate_query_id(query_id)
    if isinstance(attempt_sequence, bool) or not isinstance(attempt_sequence, int) or attempt_sequence < 0:
        raise ContractViolation("attempt_sequence must be a non-negative integer")
    if isinstance(attempt_number, bool) or not isinstance(attempt_number, int) or attempt_number < 1:
        raise ContractViolation("attempt_number must be a positive integer")
    if not isinstance(status, LkgAttemptStatus):
        raise ContractViolation("status must be an LkgAttemptStatus member")
    validated_run_binding_sha256 = _validate_sha256_hex(
        run_binding_sha256, field_name="run_binding_sha256"
    )

    if status is LkgAttemptStatus.SUCCESS:
        if error_code is not None:
            raise ContractViolation("a SUCCESS attempt must not carry an error_code")
        if not isinstance(observation, LkgQueryObservation):
            raise ContractViolation("a SUCCESS attempt must carry an LkgQueryObservation")
        if observation.query_id != query_id:
            raise ContractViolation("observation.query_id must match the attempt's query_id")
    else:
        if not isinstance(error_code, str) or not error_code:
            raise ContractViolation(f"a {status.value} attempt must carry a non-empty error_code")
        if observation is not None:
            raise ContractViolation(f"a {status.value} attempt must not carry an observation")

    return LkgQueryAttempt(
        query_id=query_id,
        attempt_sequence=attempt_sequence,
        attempt_number=attempt_number,
        status=status,
        error_code=error_code,
        run_binding_sha256=validated_run_binding_sha256,
        observation=observation,
    )
