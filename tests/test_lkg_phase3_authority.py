"""Regression tests for the Checkpoint-C-only Phase-3 LKG authority boundary."""

from __future__ import annotations

import unittest
from unittest import mock

from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.lkg_phase2_readiness_ledger import Phase2ReadinessLedger
from vdbench.lkg_phase3_authority import (
    LkgPhase3Authority,
    resolve_lkg_phase3_authority,
)
from vdbench.lkg_qualification_evaluation import (
    LkgQualificationEvaluation,
    LkgQualificationStatus,
)
from vdbench.lkg_qualification_evaluation_ledger import (
    LkgQualificationEvaluationError,
    LkgQualificationEvaluationLedger,
)
from vdbench.lkg_qualification_ledger import LkgQualificationLedger
from vdbench.lkg_run_binding import LkgRunBinding
from vdbench.policy import QualificationResult
from vdbench.search_configuration_digest import search_configuration_sha256

_EVALUATION_DIGEST = "e" * 64


def _run_binding(**overrides: object) -> LkgRunBinding:
    values: dict[str, object] = {
        "run_id": "run-phase3-001",
        "producer_identity": "checkpoint-a-producer-v1",
        "search_configuration": SearchConfiguration(
            metric=Metric.L2,
            threshold_label="target-075",
            radius=1.0,
            index_track=IndexTrack.HNSW,
            ef=400,
            limit=100,
            consistency_level="Strong",
        ),
        "collection_name": "lkg_l2_hnsw",
        "base_data_identity": "base-data-v1",
        "index_identity": "hnsw-index-v1",
        "qualification_dataset_id": "DATASET-003",
        "qualification_dataset_version": "DATASET-003-v1",
        "qualification_manifest_sha256": "a" * 64,
        "qualification_query_role": "lkg_qualification",
        "qualification_query_id_array_sha256": "b" * 64,
        "qualification_ordered_query_ids_sha256": "c" * 64,
        "qualification_query_array_sha256": "d" * 64,
        "qualification_expected_query_count": 2_400,
        "environment_identity": "env-001",
        "source_revision": "a017c1b",
    }
    values.update(overrides)
    return LkgRunBinding(**values)


def _evaluation(
    run_binding: LkgRunBinding,
    **overrides: object,
) -> LkgQualificationEvaluation:
    """Return a typed ledger-output double for authority boundary tests.

    Checkpoint C's own pure and ledger suites prove construction and canonical
    reconstruction of the large 2,400-position artifact.  This suite isolates
    the new Phase-3 trust boundary while preserving the concrete public type
    identity returned by ``get_final_evaluation``.
    """

    evaluation = mock.create_autospec(LkgQualificationEvaluation, instance=True)
    values: dict[str, object] = {
        "canonical_evaluation_digest": _EVALUATION_DIGEST,
        "source_run_id": run_binding.run_id,
        "source_run_binding_sha256": run_binding.sha256,
        "source_run_seal_digest": "1" * 64,
        "source_sealed_phase1_chain_head_sha256": "2" * 64,
        "phase2_source_binding_digest": "3" * 64,
        "evaluated_ef": run_binding.search_configuration.ef,
        "search_configuration_digest": search_configuration_sha256(
            run_binding.search_configuration
        ),
        "qualification_dataset_id": run_binding.qualification_dataset_id,
        "qualification_dataset_version": run_binding.qualification_dataset_version,
        "qualification_manifest_sha256": run_binding.qualification_manifest_sha256,
        "qualification_query_role": run_binding.qualification_query_role,
        "qualification_ordered_query_ids_sha256": (
            run_binding.qualification_ordered_query_ids_sha256
        ),
        "status": LkgQualificationStatus.PASSING,
        "qualified": True,
        "status_reason_codes": (),
        "evaluator_identity": "checkpoint-c-evaluator-v1",
        "evaluator_source_revision": "checkpoint-c-revision-v1",
        "evaluated_at_utc": "2026-08-08T12:00:00.000000Z",
    }
    values.update(overrides)
    for field, value in values.items():
        setattr(evaluation, field, value)
    return evaluation


def _ledger_with(
    evaluation: LkgQualificationEvaluation | None,
) -> LkgQualificationEvaluationLedger:
    ledger = mock.create_autospec(LkgQualificationEvaluationLedger, instance=True)
    ledger.get_final_evaluation.return_value = evaluation
    ledger.evaluate_and_finalize.return_value = evaluation
    return ledger


def _upstream_ledgers() -> tuple[LkgQualificationLedger, Phase2ReadinessLedger]:
    return (
        mock.create_autospec(LkgQualificationLedger, instance=True),
        mock.create_autospec(Phase2ReadinessLedger, instance=True),
    )


class LkgPhase3AuthorityTests(unittest.TestCase):
    def test_passing_checkpoint_c_artifact_produces_bound_authority(self) -> None:
        run_binding = _run_binding()
        evaluation = _evaluation(run_binding)
        ledger = _ledger_with(evaluation)
        phase1_ledger, phase2_ledger = _upstream_ledgers()

        result = resolve_lkg_phase3_authority(
            evaluation_ledger=ledger,
            phase1_ledger=phase1_ledger,
            phase2_readiness_ledger=phase2_ledger,
            run_binding=run_binding,
            expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
        )

        self.assertTrue(result.usable)
        self.assertEqual(result.reason_codes, ())
        self.assertIsNotNone(result.authority)
        authority = result.authority
        assert authority is not None
        self.assertIs(authority.checkpoint_c_evaluation, evaluation)
        self.assertIs(authority.run_binding, run_binding)
        self.assertEqual(authority.canonical_evaluation_digest, _EVALUATION_DIGEST)
        self.assertEqual(authority.source_run_id, run_binding.run_id)
        self.assertEqual(authority.source_run_binding_sha256, run_binding.sha256)
        self.assertEqual(authority.source_run_seal_digest, "1" * 64)
        self.assertEqual(
            authority.source_sealed_phase1_chain_head_sha256, "2" * 64
        )
        self.assertEqual(authority.phase2_source_binding_digest, "3" * 64)
        self.assertEqual(authority.evaluated_ef, 400)
        self.assertIs(authority.search_configuration, run_binding.search_configuration)
        self.assertEqual(
            authority.search_configuration_digest,
            search_configuration_sha256(run_binding.search_configuration),
        )
        self.assertIs(authority.metric, Metric.L2)
        self.assertEqual(authority.threshold_stratum, "target-075")
        self.assertEqual(authority.collection_name, "lkg_l2_hnsw")
        self.assertEqual(authority.index_identity, "hnsw-index-v1")
        self.assertEqual(authority.data_identity, "base-data-v1")
        self.assertEqual(authority.qualification_dataset_id, "DATASET-003")
        self.assertEqual(authority.qualification_dataset_version, "DATASET-003-v1")
        self.assertEqual(authority.qualification_manifest_sha256, "a" * 64)
        self.assertEqual(authority.qualification_query_role, "lkg_qualification")
        self.assertEqual(authority.qualification_ordered_query_ids_sha256, "c" * 64)
        self.assertEqual(authority.qualification_query_id_array_sha256, "b" * 64)
        self.assertEqual(authority.qualification_query_array_sha256, "d" * 64)
        self.assertEqual(authority.qualification_expected_query_count, 2_400)
        self.assertEqual(authority.environment_identity, "env-001")
        self.assertEqual(authority.source_revision, "a017c1b")
        ledger.get_final_evaluation.assert_called_once_with()
        ledger.evaluate_and_finalize.assert_called_once_with(
            phase1_ledger=phase1_ledger,
            phase2_readiness_ledger=phase2_ledger,
            evaluator_identity="checkpoint-c-evaluator-v1",
            evaluator_source_revision="checkpoint-c-revision-v1",
            evaluated_at_utc="2026-08-08T12:00:00.000000Z",
        )

    def test_authority_cannot_be_constructed_without_verified_factory(self) -> None:
        with self.assertRaises(TypeError):
            LkgPhase3Authority()
        with self.assertRaises(TypeError):
            LkgPhase3Authority._from_verified(
                evaluation=_evaluation(_run_binding()),
                run_binding=_run_binding(),
                construction_token=object(),
            )

    def test_missing_terminal_evaluation_fails_closed(self) -> None:
        phase1_ledger, phase2_ledger = _upstream_ledgers()
        ledger = _ledger_with(None)
        result = resolve_lkg_phase3_authority(
            evaluation_ledger=ledger,
            phase1_ledger=phase1_ledger,
            phase2_readiness_ledger=phase2_ledger,
            run_binding=_run_binding(),
            expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
        )
        self.assertFalse(result.usable)
        self.assertEqual(result.reason_codes, ("PHASE3_TERMINAL_EVALUATION_MISSING",))
        ledger.evaluate_and_finalize.assert_not_called()

    def test_failing_and_incomplete_evaluations_never_authorize(self) -> None:
        run_binding = _run_binding()
        cases = (
            (LkgQualificationStatus.FAILING, ("EPOCH_RECALL_BELOW_FLOOR",)),
            (
                LkgQualificationStatus.INCOMPLETE,
                ("AWAITING_READINESS_EVIDENCE",),
            ),
        )
        for status, reason_codes in cases:
            with self.subTest(status=status):
                evaluation = _evaluation(
                    run_binding,
                    status=status,
                    qualified=False,
                    status_reason_codes=reason_codes,
                )
                phase1_ledger, phase2_ledger = _upstream_ledgers()
                result = resolve_lkg_phase3_authority(
                    evaluation_ledger=_ledger_with(evaluation),
                    phase1_ledger=phase1_ledger,
                    phase2_readiness_ledger=phase2_ledger,
                    run_binding=run_binding,
                    expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
                )
                self.assertFalse(result.usable)
                self.assertIn("PHASE3_EVALUATION_NOT_PASSING", result.reason_codes)

    def test_checkpoint_c_tamper_error_fails_closed_and_preserves_code(self) -> None:
        ledger = _ledger_with(None)
        ledger.get_final_evaluation.side_effect = LkgQualificationEvaluationError(
            "canonical row mismatch",
            code="LKG_QUAL_EVAL_FINAL_CORRUPTED",
        )
        phase1_ledger, phase2_ledger = _upstream_ledgers()
        result = resolve_lkg_phase3_authority(
            evaluation_ledger=ledger,
            phase1_ledger=phase1_ledger,
            phase2_readiness_ledger=phase2_ledger,
            run_binding=_run_binding(),
            expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
        )
        self.assertFalse(result.usable)
        self.assertEqual(
            result.reason_codes, ("PHASE3_CHECKPOINT_C_VERIFICATION_FAILED",)
        )
        self.assertEqual(
            result.checkpoint_c_error_code, "LKG_QUAL_EVAL_FINAL_CORRUPTED"
        )
        ledger.evaluate_and_finalize.assert_not_called()

    def test_replay_failure_refuses_authority(self) -> None:
        run_binding = _run_binding()
        ledger = _ledger_with(_evaluation(run_binding))
        ledger.evaluate_and_finalize.side_effect = LkgQualificationEvaluationError(
            "Phase-1 source replay mismatch",
            code="LKG_QUAL_EVAL_REPLAY_MISMATCH",
        )
        phase1_ledger, phase2_ledger = _upstream_ledgers()

        result = resolve_lkg_phase3_authority(
            evaluation_ledger=ledger,
            phase1_ledger=phase1_ledger,
            phase2_readiness_ledger=phase2_ledger,
            run_binding=run_binding,
            expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
        )

        self.assertFalse(result.usable)
        self.assertEqual(result.reason_codes, ("PHASE3_CHECKPOINT_C_REPLAY_FAILED",))
        self.assertEqual(
            result.checkpoint_c_error_code, "LKG_QUAL_EVAL_REPLAY_MISMATCH"
        )

    def test_unexpected_replay_error_propagates(self) -> None:
        run_binding = _run_binding()
        ledger = _ledger_with(_evaluation(run_binding))
        ledger.evaluate_and_finalize.side_effect = RuntimeError(
            "unexpected replay failure"
        )
        phase1_ledger, phase2_ledger = _upstream_ledgers()

        with self.assertRaisesRegex(RuntimeError, "unexpected replay failure"):
            resolve_lkg_phase3_authority(
                evaluation_ledger=ledger,
                phase1_ledger=phase1_ledger,
                phase2_readiness_ledger=phase2_ledger,
                run_binding=run_binding,
                expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
            )

    def test_get_final_evaluation_alone_cannot_authorize(self) -> None:
        run_binding = _run_binding()
        stored_evaluation = _evaluation(run_binding)
        replayed_evaluation = _evaluation(
            run_binding,
            canonical_evaluation_digest="f" * 64,
        )
        ledger = _ledger_with(stored_evaluation)
        ledger.evaluate_and_finalize.return_value = replayed_evaluation
        phase1_ledger, phase2_ledger = _upstream_ledgers()

        result = resolve_lkg_phase3_authority(
            evaluation_ledger=ledger,
            phase1_ledger=phase1_ledger,
            phase2_readiness_ledger=phase2_ledger,
            run_binding=run_binding,
            expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
        )

        self.assertFalse(result.usable)
        self.assertEqual(
            result.reason_codes, ("PHASE3_CHECKPOINT_C_REPLAY_MISMATCH",)
        )

    def test_expected_digest_pins_exact_reviewed_terminal_artifact(self) -> None:
        run_binding = _run_binding()
        phase1_ledger, phase2_ledger = _upstream_ledgers()
        result = resolve_lkg_phase3_authority(
            evaluation_ledger=_ledger_with(_evaluation(run_binding)),
            phase1_ledger=phase1_ledger,
            phase2_readiness_ledger=phase2_ledger,
            run_binding=run_binding,
            expected_canonical_evaluation_digest="f" * 64,
        )
        self.assertFalse(result.usable)
        self.assertIn("PHASE3_EVALUATION_DIGEST_MISMATCH", result.reason_codes)

    def test_every_run_search_and_workload_lineage_mismatch_fails_closed(self) -> None:
        run_binding = _run_binding()
        mismatch_cases: tuple[tuple[str, object, str], ...] = (
            ("source_run_id", "other-run", "PHASE3_SOURCE_RUN_ID_MISMATCH"),
            (
                "source_run_binding_sha256",
                "4" * 64,
                "PHASE3_SOURCE_RUN_BINDING_MISMATCH",
            ),
            ("evaluated_ef", 800, "PHASE3_EVALUATED_EF_MISMATCH"),
            (
                "search_configuration_digest",
                "5" * 64,
                "PHASE3_SEARCH_CONFIGURATION_MISMATCH",
            ),
            (
                "qualification_dataset_id",
                "DATASET-OTHER",
                "PHASE3_DATASET_ID_MISMATCH",
            ),
            (
                "qualification_dataset_version",
                "DATASET-003-v2",
                "PHASE3_DATASET_VERSION_MISMATCH",
            ),
            (
                "qualification_manifest_sha256",
                "6" * 64,
                "PHASE3_MANIFEST_MISMATCH",
            ),
            (
                "qualification_query_role",
                "other_role",
                "PHASE3_QUERY_ROLE_MISMATCH",
            ),
            (
                "qualification_ordered_query_ids_sha256",
                "7" * 64,
                "PHASE3_ORDERED_QUERY_IDS_MISMATCH",
            ),
        )
        for field, mismatched_value, expected_reason in mismatch_cases:
            with self.subTest(field=field):
                evaluation = _evaluation(run_binding, **{field: mismatched_value})
                phase1_ledger, phase2_ledger = _upstream_ledgers()
                result = resolve_lkg_phase3_authority(
                    evaluation_ledger=_ledger_with(evaluation),
                    phase1_ledger=phase1_ledger,
                    phase2_readiness_ledger=phase2_ledger,
                    run_binding=run_binding,
                    expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
                )
                self.assertFalse(result.usable)
                self.assertIn(expected_reason, result.reason_codes)

    def test_binding_only_identity_changes_break_checkpoint_c_lineage(self) -> None:
        original_binding = _run_binding()
        evaluation = _evaluation(original_binding)
        binding_changes: tuple[tuple[str, object], ...] = (
            ("collection_name", "other-collection"),
            ("base_data_identity", "other-data"),
            ("index_identity", "other-index"),
            ("qualification_query_id_array_sha256", "8" * 64),
            ("qualification_query_array_sha256", "9" * 64),
            ("qualification_expected_query_count", 2_399),
            ("environment_identity", "other-environment"),
            ("source_revision", "other-revision"),
        )
        for field, value in binding_changes:
            with self.subTest(field=field):
                phase1_ledger, phase2_ledger = _upstream_ledgers()
                result = resolve_lkg_phase3_authority(
                    evaluation_ledger=_ledger_with(evaluation),
                    phase1_ledger=phase1_ledger,
                    phase2_readiness_ledger=phase2_ledger,
                    run_binding=_run_binding(**{field: value}),
                    expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
                )
                self.assertFalse(result.usable)
                self.assertIn(
                    "PHASE3_SOURCE_RUN_BINDING_MISMATCH", result.reason_codes
                )

    def test_passing_artifact_with_contradictory_qualification_fails_closed(self) -> None:
        run_binding = _run_binding()
        for overrides, expected_reason in (
            ({"qualified": False}, "PHASE3_QUALIFIED_FLAG_INVALID"),
            (
                {"status_reason_codes": ("AWAITING_READINESS_EVIDENCE",)},
                "PHASE3_PASSING_REASONS_PRESENT",
            ),
        ):
            with self.subTest(overrides=overrides):
                phase1_ledger, phase2_ledger = _upstream_ledgers()
                result = resolve_lkg_phase3_authority(
                    evaluation_ledger=_ledger_with(
                        _evaluation(run_binding, **overrides)
                    ),
                    phase1_ledger=phase1_ledger,
                    phase2_readiness_ledger=phase2_ledger,
                    run_binding=run_binding,
                    expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
                )
                self.assertFalse(result.usable)
                self.assertIn(expected_reason, result.reason_codes)

    def test_legacy_qualification_result_is_not_a_phase3_authority_source(self) -> None:
        legacy = QualificationResult(
            qualified=True,
            ef=400,
            reasons=(),
            metric=Metric.L2,
            threshold_stratum="target-075",
            configuration_identity="legacy-config",
            index_identity="legacy-index",
            data_identity="legacy-data",
        )
        phase1_ledger, phase2_ledger = _upstream_ledgers()
        result = resolve_lkg_phase3_authority(
            evaluation_ledger=legacy,  # type: ignore[arg-type]
            phase1_ledger=phase1_ledger,
            phase2_readiness_ledger=phase2_ledger,
            run_binding=_run_binding(),
            expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
        )
        self.assertFalse(result.usable)
        self.assertEqual(result.reason_codes, ("PHASE3_AUTHORITY_INPUT_INVALID",))

    def test_invalid_digest_or_run_binding_type_fails_before_ledger_read(self) -> None:
        ledger = _ledger_with(_evaluation(_run_binding()))
        phase1_ledger, phase2_ledger = _upstream_ledgers()
        for run_binding, digest in (
            (_run_binding(), "not-a-digest"),
            (object(), _EVALUATION_DIGEST),
        ):
            with self.subTest(run_binding=run_binding, digest=digest):
                result = resolve_lkg_phase3_authority(
                    evaluation_ledger=ledger,
                    phase1_ledger=phase1_ledger,
                    phase2_readiness_ledger=phase2_ledger,
                    run_binding=run_binding,  # type: ignore[arg-type]
                    expected_canonical_evaluation_digest=digest,
                )
                self.assertFalse(result.usable)
                self.assertEqual(
                    result.reason_codes, ("PHASE3_AUTHORITY_INPUT_INVALID",)
                )
        ledger.get_final_evaluation.assert_not_called()

    def test_nonconcrete_upstream_ledgers_fail_before_checkpoint_c_read(self) -> None:
        run_binding = _run_binding()
        valid_phase1, valid_phase2 = _upstream_ledgers()
        for phase1_ledger, phase2_ledger in (
            (object(), valid_phase2),
            (valid_phase1, object()),
        ):
            with self.subTest(
                phase1_type=type(phase1_ledger).__name__,
                phase2_type=type(phase2_ledger).__name__,
            ):
                ledger = _ledger_with(_evaluation(run_binding))
                result = resolve_lkg_phase3_authority(
                    evaluation_ledger=ledger,
                    phase1_ledger=phase1_ledger,  # type: ignore[arg-type]
                    phase2_readiness_ledger=phase2_ledger,  # type: ignore[arg-type]
                    run_binding=run_binding,
                    expected_canonical_evaluation_digest=_EVALUATION_DIGEST,
                )
                self.assertFalse(result.usable)
                self.assertEqual(
                    result.reason_codes, ("PHASE3_AUTHORITY_INPUT_INVALID",)
                )
                ledger.get_final_evaluation.assert_not_called()
                ledger.evaluate_and_finalize.assert_not_called()


if __name__ == "__main__":
    unittest.main()
