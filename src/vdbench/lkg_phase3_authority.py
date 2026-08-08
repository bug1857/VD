"""Checkpoint-C-backed last-known-good authority for Phase 3.

Purpose:
    Convert exactly one freshly verified terminal Checkpoint-C evaluation into
    the immutable authority that later Phase-3 policy, persistence, and
    admission integrations may consume.  This module is deliberately a trust
    boundary, not another qualification engine: it reads the terminal artifact
    through ``LkgQualificationEvaluationLedger.get_final_evaluation()`` and
    never recomputes windows, epochs, statistics, Phase-1 attempts, or Phase-2
    readiness.
Inputs:
    Open concrete Checkpoint-C, Phase-1, and Phase-2 ledgers; the immutable
    ``LkgRunBinding`` that the caller expects the evaluation to describe; and
    the expected canonical evaluation digest from the reviewed/finalized
    Checkpoint-C artifact.
Outputs:
    ``LkgPhase3AuthorityResolution``.  Only a Checkpoint-C ``PASSING`` artifact
    whose complete run/search/workload lineage matches may carry a usable
    ``LkgPhase3Authority``.  Checkpoint C performs a fresh replay of its bound
    Phase-1 and Phase-2 sources before Phase 3 may consume the artifact.
Failure modes:
    Missing, non-PASSING, corrupted, replayed, or identity-mismatched evidence
    returns no authority and explicit deterministic reason codes.  A
    Checkpoint-C ledger verification error is preserved as diagnostic metadata
    without accepting any evidence.
Dependencies:
    Pure identity/configuration contracts plus the Checkpoint-C ledger.  There
    are no Milvus, network, actuation, policy, or legacy ``QualificationResult``
    dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
import re

from .config import Metric, SearchConfiguration
from .lkg_qualification_evaluation import (
    LkgQualificationEvaluation,
    LkgQualificationStatus,
)
from .lkg_qualification_evaluation_ledger import (
    LkgQualificationEvaluationError,
    LkgQualificationEvaluationLedger,
)
from .lkg_qualification_ledger import LkgQualificationLedger
from .lkg_phase2_readiness_ledger import Phase2ReadinessLedger
from .lkg_run_binding import LkgRunBinding
from .search_configuration_digest import search_configuration_sha256


__all__ = [
    "LkgPhase3Authority",
    "LkgPhase3AuthorityResolution",
    "resolve_lkg_phase3_authority",
]


_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_AUTHORITY_CONSTRUCTION_TOKEN = object()

_INPUT_INVALID = "PHASE3_AUTHORITY_INPUT_INVALID"
_CHECKPOINT_C_VERIFICATION_FAILED = "PHASE3_CHECKPOINT_C_VERIFICATION_FAILED"
_CHECKPOINT_C_REPLAY_FAILED = "PHASE3_CHECKPOINT_C_REPLAY_FAILED"
_CHECKPOINT_C_REPLAY_MISMATCH = "PHASE3_CHECKPOINT_C_REPLAY_MISMATCH"
_TERMINAL_EVALUATION_MISSING = "PHASE3_TERMINAL_EVALUATION_MISSING"
_EVALUATION_DIGEST_MISMATCH = "PHASE3_EVALUATION_DIGEST_MISMATCH"
_EVALUATION_NOT_PASSING = "PHASE3_EVALUATION_NOT_PASSING"
_QUALIFIED_FLAG_INVALID = "PHASE3_QUALIFIED_FLAG_INVALID"
_PASSING_REASONS_PRESENT = "PHASE3_PASSING_REASONS_PRESENT"
_SOURCE_RUN_ID_MISMATCH = "PHASE3_SOURCE_RUN_ID_MISMATCH"
_SOURCE_RUN_BINDING_MISMATCH = "PHASE3_SOURCE_RUN_BINDING_MISMATCH"
_EVALUATED_EF_MISMATCH = "PHASE3_EVALUATED_EF_MISMATCH"
_SEARCH_CONFIGURATION_MISMATCH = "PHASE3_SEARCH_CONFIGURATION_MISMATCH"
_DATASET_ID_MISMATCH = "PHASE3_DATASET_ID_MISMATCH"
_DATASET_VERSION_MISMATCH = "PHASE3_DATASET_VERSION_MISMATCH"
_MANIFEST_MISMATCH = "PHASE3_MANIFEST_MISMATCH"
_QUERY_ROLE_MISMATCH = "PHASE3_QUERY_ROLE_MISMATCH"
_ORDERED_QUERY_IDS_MISMATCH = "PHASE3_ORDERED_QUERY_IDS_MISMATCH"


@dataclass(frozen=True, slots=True, init=False)
class LkgPhase3Authority:
    """Immutable projection of one verified PASSING Checkpoint-C artifact.

    Construction is intentionally private to ``resolve_lkg_phase3_authority``.
    Keeping both immutable source objects prevents a projection from drifting
    away from its canonical C digest or run-binding digest while exposing the
    exact identities needed by later Phase-3 integrations.
    """

    _evaluation: LkgQualificationEvaluation
    _run_binding: LkgRunBinding

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "LkgPhase3Authority can only be created by "
            "resolve_lkg_phase3_authority()"
        )

    @classmethod
    def _from_verified(
        cls,
        *,
        evaluation: LkgQualificationEvaluation,
        run_binding: LkgRunBinding,
        construction_token: object,
    ) -> LkgPhase3Authority:
        if construction_token is not _AUTHORITY_CONSTRUCTION_TOKEN:
            raise TypeError("LkgPhase3Authority construction token is invalid")
        authority = object.__new__(cls)
        object.__setattr__(authority, "_evaluation", evaluation)
        object.__setattr__(authority, "_run_binding", run_binding)
        return authority

    @property
    def checkpoint_c_evaluation(self) -> LkgQualificationEvaluation:
        """The complete immutable terminal Checkpoint-C artifact."""

        return self._evaluation

    @property
    def run_binding(self) -> LkgRunBinding:
        """The complete immutable source run/search/workload binding."""

        return self._run_binding

    @property
    def canonical_evaluation_digest(self) -> str:
        return self._evaluation.canonical_evaluation_digest

    @property
    def source_run_id(self) -> str:
        return self._evaluation.source_run_id

    @property
    def source_run_binding_sha256(self) -> str:
        return self._evaluation.source_run_binding_sha256

    @property
    def source_run_seal_digest(self) -> str:
        return self._evaluation.source_run_seal_digest

    @property
    def source_sealed_phase1_chain_head_sha256(self) -> str:
        return self._evaluation.source_sealed_phase1_chain_head_sha256

    @property
    def phase2_source_binding_digest(self) -> str:
        return self._evaluation.phase2_source_binding_digest

    @property
    def evaluated_ef(self) -> int:
        return self._evaluation.evaluated_ef

    @property
    def search_configuration(self) -> SearchConfiguration:
        return self._run_binding.search_configuration

    @property
    def search_configuration_digest(self) -> str:
        return self._evaluation.search_configuration_digest

    @property
    def metric(self) -> Metric:
        return self._run_binding.search_configuration.metric

    @property
    def threshold_stratum(self) -> str:
        return self._run_binding.search_configuration.threshold_label

    @property
    def collection_name(self) -> str:
        return self._run_binding.collection_name

    @property
    def index_identity(self) -> str:
        return self._run_binding.index_identity

    @property
    def data_identity(self) -> str:
        return self._run_binding.base_data_identity

    @property
    def qualification_dataset_id(self) -> str:
        return self._evaluation.qualification_dataset_id

    @property
    def qualification_dataset_version(self) -> str:
        return self._evaluation.qualification_dataset_version

    @property
    def qualification_manifest_sha256(self) -> str:
        return self._evaluation.qualification_manifest_sha256

    @property
    def qualification_query_role(self) -> str:
        return self._evaluation.qualification_query_role

    @property
    def qualification_ordered_query_ids_sha256(self) -> str:
        return self._evaluation.qualification_ordered_query_ids_sha256

    @property
    def qualification_query_id_array_sha256(self) -> str:
        return self._run_binding.qualification_query_id_array_sha256

    @property
    def qualification_query_array_sha256(self) -> str:
        return self._run_binding.qualification_query_array_sha256

    @property
    def qualification_expected_query_count(self) -> int:
        return self._run_binding.qualification_expected_query_count

    @property
    def environment_identity(self) -> str:
        return self._run_binding.environment_identity

    @property
    def source_revision(self) -> str:
        return self._run_binding.source_revision


@dataclass(frozen=True, slots=True)
class LkgPhase3AuthorityResolution:
    """Fail-closed outcome of resolving one Phase-3 LKG authority."""

    authority: LkgPhase3Authority | None
    reason_codes: tuple[str, ...]
    checkpoint_c_error_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.reason_codes, tuple):
            raise TypeError("reason_codes must be a tuple")
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise ValueError("reason_codes must be unique and sorted")
        if (self.authority is None) == (not self.reason_codes):
            raise ValueError(
                "a usable authority requires no reasons; a failed resolution "
                "requires at least one reason"
            )
        if self.checkpoint_c_error_code is not None and (
            not isinstance(self.checkpoint_c_error_code, str)
            or not self.checkpoint_c_error_code
        ):
            raise ValueError("checkpoint_c_error_code must be None or a non-empty string")

    @property
    def usable(self) -> bool:
        return self.authority is not None


def _failed(
    *reason_codes: str,
    checkpoint_c_error_code: str | None = None,
) -> LkgPhase3AuthorityResolution:
    return LkgPhase3AuthorityResolution(
        authority=None,
        reason_codes=tuple(sorted(set(reason_codes))),
        checkpoint_c_error_code=checkpoint_c_error_code,
    )


def _lineage_mismatch_reasons(
    evaluation: LkgQualificationEvaluation,
    run_binding: LkgRunBinding,
) -> tuple[str, ...]:
    reasons: set[str] = set()
    if evaluation.source_run_id != run_binding.run_id:
        reasons.add(_SOURCE_RUN_ID_MISMATCH)
    if evaluation.source_run_binding_sha256 != run_binding.sha256:
        reasons.add(_SOURCE_RUN_BINDING_MISMATCH)
    if evaluation.evaluated_ef != run_binding.search_configuration.ef:
        reasons.add(_EVALUATED_EF_MISMATCH)
    if evaluation.search_configuration_digest != search_configuration_sha256(
        run_binding.search_configuration
    ):
        reasons.add(_SEARCH_CONFIGURATION_MISMATCH)
    if evaluation.qualification_dataset_id != run_binding.qualification_dataset_id:
        reasons.add(_DATASET_ID_MISMATCH)
    if (
        evaluation.qualification_dataset_version
        != run_binding.qualification_dataset_version
    ):
        reasons.add(_DATASET_VERSION_MISMATCH)
    if (
        evaluation.qualification_manifest_sha256
        != run_binding.qualification_manifest_sha256
    ):
        reasons.add(_MANIFEST_MISMATCH)
    if evaluation.qualification_query_role != run_binding.qualification_query_role:
        reasons.add(_QUERY_ROLE_MISMATCH)
    if (
        evaluation.qualification_ordered_query_ids_sha256
        != run_binding.qualification_ordered_query_ids_sha256
    ):
        reasons.add(_ORDERED_QUERY_IDS_MISMATCH)
    return tuple(sorted(reasons))


def resolve_lkg_phase3_authority(
    *,
    evaluation_ledger: LkgQualificationEvaluationLedger,
    phase1_ledger: LkgQualificationLedger,
    phase2_readiness_ledger: Phase2ReadinessLedger,
    run_binding: LkgRunBinding,
    expected_canonical_evaluation_digest: str,
) -> LkgPhase3AuthorityResolution:
    """Resolve one new Phase-3 authority from freshly verified C evidence.

    The expected digest is mandatory: it pins the exact reviewed terminal
    artifact and therefore rejects replacement by a different internally
    valid Checkpoint-C row.  This method delegates fresh upstream replay to
    Checkpoint C; Phase 3 does not inspect or recompute any source evidence or
    statistical result itself.
    """

    if (
        not isinstance(evaluation_ledger, LkgQualificationEvaluationLedger)
        or not isinstance(phase1_ledger, LkgQualificationLedger)
        or not isinstance(phase2_readiness_ledger, Phase2ReadinessLedger)
        or not isinstance(run_binding, LkgRunBinding)
        or not isinstance(expected_canonical_evaluation_digest, str)
        or _SHA256_RE.fullmatch(expected_canonical_evaluation_digest) is None
    ):
        return _failed(_INPUT_INVALID)

    try:
        evaluation = evaluation_ledger.get_final_evaluation()
    except LkgQualificationEvaluationError as exc:
        return _failed(
            _CHECKPOINT_C_VERIFICATION_FAILED,
            checkpoint_c_error_code=exc.code,
        )

    if evaluation is None:
        return _failed(_TERMINAL_EVALUATION_MISSING)
    if not isinstance(evaluation, LkgQualificationEvaluation):
        return _failed(_CHECKPOINT_C_VERIFICATION_FAILED)

    try:
        replayed_evaluation = evaluation_ledger.evaluate_and_finalize(
            phase1_ledger=phase1_ledger,
            phase2_readiness_ledger=phase2_readiness_ledger,
            evaluator_identity=evaluation.evaluator_identity,
            evaluator_source_revision=evaluation.evaluator_source_revision,
            evaluated_at_utc=evaluation.evaluated_at_utc,
        )
    except LkgQualificationEvaluationError as exc:
        return _failed(
            _CHECKPOINT_C_REPLAY_FAILED,
            checkpoint_c_error_code=exc.code,
        )
    if (
        not isinstance(replayed_evaluation, LkgQualificationEvaluation)
        or replayed_evaluation != evaluation
    ):
        return _failed(_CHECKPOINT_C_REPLAY_MISMATCH)

    reasons: set[str] = set()
    if evaluation.canonical_evaluation_digest != expected_canonical_evaluation_digest:
        reasons.add(_EVALUATION_DIGEST_MISMATCH)
    if evaluation.status is not LkgQualificationStatus.PASSING:
        reasons.add(_EVALUATION_NOT_PASSING)
    else:
        if evaluation.qualified is not True:
            reasons.add(_QUALIFIED_FLAG_INVALID)
        if evaluation.status_reason_codes:
            reasons.add(_PASSING_REASONS_PRESENT)
    reasons.update(_lineage_mismatch_reasons(evaluation, run_binding))
    if reasons:
        return _failed(*reasons)

    return LkgPhase3AuthorityResolution(
        authority=LkgPhase3Authority._from_verified(
            evaluation=evaluation,
            run_binding=run_binding,
            construction_token=_AUTHORITY_CONSTRUCTION_TOKEN,
        ),
        reason_codes=(),
    )
