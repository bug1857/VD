"""Hand-checkable tests for the EXP-009 1,200-query recall-audit evaluator.

This module composes the existing, unmodified
``one_sided_hoeffding_recall_lower_bound`` with completeness and
evidence-binding checks over exactly one authorized
(metric, threshold stratum, numeric radius, candidate ef, dataset/index
identity) contract. It never pools multiple contexts, and it never touches
``canary_schedule_evaluation.py`` (which remains latency-only, its own
``recall_bound_evaluated`` field staying an honest, permanent ``False``).

Estimand note: the Hoeffding bound is valid here because DATASET-002's 1,200
recall-audit query vectors are i.i.d. draws from an assumed standard-normal
generating distribution (``dataset002.py``'s ``PCG64``-seeded generation),
frozen after generation, and every observation in one evaluation is scored
against the identical frozen numeric radius. The bound targets expected
recall under that generating distribution, not the (trivially exact) census
mean of these fixed vectors.
"""

from __future__ import annotations

from math import log, sqrt
import unittest

from vdbench.canary_recall_audit_evaluation import (
    RECALL_AUDIT_EVALUATOR_VERSION,
    EvaluationStatus,
    Stage4RecallAuditEvaluation,
    Stage4RecallAuditReport,
    build_recall_audit_report,
    evaluate_recall_audit_evidence,
)
from vdbench.canary_recall_audit_ledger import RecallAuditObservation
from vdbench.canary_stage4_evidence_binding import Stage4EvidenceBinding
from vdbench.canary_statistics import EXP009_RECALL_AUDIT_COUNT
from vdbench.canary_workload import WorkloadIdentityBinding
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.dataset002 import DATASET002_SCHEMA_VERSION


def _search_configuration(**overrides) -> SearchConfiguration:
    fields = dict(
        metric=Metric.COSINE,
        threshold_label="target-025",
        radius=0.2,
        index_track=IndexTrack.HNSW,
        ef=800,
        limit=100,
        consistency_level="Strong",
    )
    fields.update(overrides)
    return SearchConfiguration(**fields)


def _identity(**overrides) -> WorkloadIdentityBinding:
    fields = dict(
        configuration_identity="a" * 16,
        data_identity="DATASET-001-v1:sha256:" + "b" * 64,
        flat_binding_id="c" * 16,
        hnsw_binding_id="d" * 16,
    )
    fields.update(overrides)
    return WorkloadIdentityBinding(**fields)


_FROZEN_QUERY_IDS_SHA256 = "f" * 64


def _binding(**overrides) -> Stage4EvidenceBinding:
    fields = dict(
        run_id="exp009-stage4-run-001",
        source_revision="0" * 40,
        metric=Metric.COSINE,
        threshold_stratum="target-025",
        current_ef=400,
        candidate_ef=800,
        last_known_good_ef=400,
        candidate_search_configuration=_search_configuration(),
        identity=_identity(),
        dataset002_manifest_sha256="e" * 64,
        frozen_recall_audit_ids_sha256=_FROZEN_QUERY_IDS_SHA256,
        eligible_workload_sha256="1" * 64,
        candidate_selection_sha256="2" * 64,
        execution_schedule_sha256="3" * 64,
        recall_evidence_schema_version="recall-audit-hoeffding-1200-v1",
        latency_evidence_schema_version="exp009-stage4-execution-schedule-v1",
    )
    fields.update(overrides)
    return Stage4EvidenceBinding(**fields)


_CONTEXT = dict(
    search_configuration=_search_configuration(),
    identity=_identity(),
    dataset002_manifest_sha256="e" * 64,
    dataset002_schema_version=DATASET002_SCHEMA_VERSION,
    binding=_binding(),
    frozen_query_ids_sha256=_FROZEN_QUERY_IDS_SHA256,
)


def _obs(query_id: int, capped_recall: float, *, result_cap: int = 100, **overrides):
    matched_count = round(capped_recall * result_cap)
    base = query_id * result_cap
    oracle_ids = tuple(range(base, base + result_cap))
    # Decoy IDs for the "missed" portion of the candidate set: large,
    # out-of-range positive sentinels well clear of any real oracle_ids
    # range, since result IDs are non-negative by this codebase's convention.
    decoy_base = 10_000_000 + base
    candidate_ids = oracle_ids[:matched_count] + tuple(
        range(decoy_base, decoy_base + (result_cap - matched_count))
    )
    fields = dict(
        query_id=query_id,
        search_configuration=_CONTEXT["search_configuration"],
        identity=_CONTEXT["identity"],
        dataset002_manifest_sha256=_CONTEXT["dataset002_manifest_sha256"],
        dataset002_schema_version=_CONTEXT["dataset002_schema_version"],
        oracle_result_ids=oracle_ids,
        candidate_result_ids=candidate_ids,
        producer_run_id=_CONTEXT["binding"].run_id,
        recorded_at_utc="2026-08-04T00:00:00Z",
    )
    fields.update(overrides)
    return RecallAuditObservation(**fields)


class EvaluateRecallAuditEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.expected_ids = frozenset(range(EXP009_RECALL_AUDIT_COUNT))

    def test_all_perfect_recall_passes_with_hand_computed_bound(self) -> None:
        observations = tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT))

        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids,
            observations=observations,
            **_CONTEXT,
        )

        self.assertIsInstance(result, Stage4RecallAuditEvaluation)
        expected_margin = sqrt(log(20) / (2 * EXP009_RECALL_AUDIT_COUNT))
        self.assertAlmostEqual(result.observed_mean, 1.0, places=15)
        self.assertAlmostEqual(result.margin, expected_margin, places=15)
        self.assertAlmostEqual(result.lower_bound, 1.0 - expected_margin, places=15)
        self.assertEqual(result.sample_count, EXP009_RECALL_AUDIT_COUNT)
        self.assertTrue(result.recall_audit_complete_and_passing)
        self.assertEqual(result.reason_codes, ())
        self.assertEqual(result.alpha, 0.05)
        self.assertEqual(result.evaluator_method_version, RECALL_AUDIT_EVALUATOR_VERSION)
        self.assertIsInstance(result.evidence_digest, str)
        self.assertEqual(len(result.evidence_digest), 64)
        self.assertEqual(result.status, EvaluationStatus.PASSING)
        self.assertEqual(result.evidence_binding_sha256, _CONTEXT["binding"].sha256)

    def test_recall_floor_exact_0_95_accepted(self) -> None:
        observations = tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT))
        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids,
            observations=observations,
            recall_floor=0.95,
            **_CONTEXT,
        )
        self.assertIsInstance(result, Stage4RecallAuditEvaluation)

    def test_recall_floor_rejects_alternate_and_malformed_values(self) -> None:
        observations = tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT))
        for bad_floor in (
            0.949,
            0.951,
            True,
            False,
            float("nan"),
            float("inf"),
            float("-inf"),
            1,
            "0.95",
        ):
            with self.subTest(bad_floor=bad_floor):
                with self.assertRaises(ValueError):
                    evaluate_recall_audit_evidence(
                        expected_query_ids=self.expected_ids,
                        observations=observations,
                        recall_floor=bad_floor,
                        **_CONTEXT,
                    )

    def test_evidence_digest_changes_if_any_observation_changes(self) -> None:
        observations = tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT))
        result_a = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        mutated = observations[:-1] + (_obs(EXP009_RECALL_AUDIT_COUNT - 1, 0.5),)
        result_b = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=mutated, **_CONTEXT
        )
        self.assertNotEqual(result_a.evidence_digest, result_b.evidence_digest)

    def test_all_zero_recall_fails_the_floor(self) -> None:
        observations = tuple(_obs(qid, 0.0) for qid in range(EXP009_RECALL_AUDIT_COUNT))

        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )

        self.assertEqual(result.lower_bound, 0.0)
        self.assertFalse(result.recall_audit_complete_and_passing)
        # Evidence was complete and well-formed; the bound simply did not
        # clear the floor. This is FAILING, not INCOMPLETE.
        self.assertEqual(result.status, EvaluationStatus.FAILING)

    def test_wrong_sample_count_is_not_applicable(self) -> None:
        observations = tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT - 1))

        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )

        self.assertFalse(result.recall_audit_complete_and_passing)
        self.assertIn("OBSERVATION_COUNT_INVALID", result.reason_codes)
        self.assertIsNone(result.lower_bound)
        self.assertIsNone(result.evidence_digest)
        # Malformed/incomplete evidence, not a floor breach: INCOMPLETE.
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)

    def test_duplicate_query_id_is_not_applicable(self) -> None:
        observations = tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT - 1))
        observations = observations + (_obs(0, 1.0),)

        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )

        self.assertFalse(result.recall_audit_complete_and_passing)
        self.assertIn("DUPLICATE_QUERY_ID", result.reason_codes)

    def test_query_id_outside_expected_set_is_not_applicable(self) -> None:
        observations = tuple(_obs(qid, 1.0) for qid in range(1, EXP009_RECALL_AUDIT_COUNT + 1))

        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )

        self.assertFalse(result.recall_audit_complete_and_passing)
        self.assertIn("QUERY_ID_NOT_IN_FROZEN_SET", result.reason_codes)

    def _pooled_batch_with_one_mismatch(self, **mismatch) -> tuple:
        observations = tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT))
        return observations[:-1] + (
            _obs(EXP009_RECALL_AUDIT_COUNT - 1, 1.0, **mismatch),
        )

    def test_mismatched_metric_is_not_applicable(self) -> None:
        observations = self._pooled_batch_with_one_mismatch(
            search_configuration=_search_configuration(metric=Metric.L2, radius=0.6)
        )
        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        self.assertFalse(result.recall_audit_complete_and_passing)
        self.assertIn("OBSERVATION_CONTEXT_MISMATCH", result.reason_codes)

    def test_mismatched_threshold_stratum_is_not_applicable(self) -> None:
        observations = self._pooled_batch_with_one_mismatch(
            search_configuration=_search_configuration(threshold_label="target-075", radius=0.6)
        )
        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        self.assertIn("OBSERVATION_CONTEXT_MISMATCH", result.reason_codes)

    def test_mismatched_numeric_radius_under_same_label_is_not_applicable(self) -> None:
        """Two observations could share a stratum *label* while disagreeing on
        the actual frozen radius -- this must be caught, not just the label."""
        observations = self._pooled_batch_with_one_mismatch(
            search_configuration=_search_configuration(radius=0.25)  # same label, different radius
        )
        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        self.assertIn("OBSERVATION_CONTEXT_MISMATCH", result.reason_codes)

    def test_mismatched_ef_is_not_applicable(self) -> None:
        observations = self._pooled_batch_with_one_mismatch(
            search_configuration=_search_configuration(ef=400)
        )
        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        self.assertIn("OBSERVATION_CONTEXT_MISMATCH", result.reason_codes)

    def test_mismatched_identity_is_not_applicable(self) -> None:
        observations = self._pooled_batch_with_one_mismatch(
            identity=_identity(hnsw_binding_id="different")
        )
        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        self.assertIn("OBSERVATION_CONTEXT_MISMATCH", result.reason_codes)

    def test_mismatched_dataset002_manifest_is_not_applicable(self) -> None:
        observations = self._pooled_batch_with_one_mismatch(
            dataset002_manifest_sha256="f" * 64
        )
        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        self.assertIn("OBSERVATION_CONTEXT_MISMATCH", result.reason_codes)

    def test_mismatched_producer_run_cannot_be_relabelled_with_binding(self) -> None:
        observations = self._pooled_batch_with_one_mismatch(
            producer_run_id="different-stage4-run"
        )
        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("OBSERVATION_PRODUCER_RUN_MISMATCH", result.reason_codes)
        self.assertIsNone(result.evidence_digest)

    def test_evidence_digest_binds_producer_run_lineage(self) -> None:
        observations = tuple(
            _obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT)
        )
        original = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        alternate_binding = _binding(run_id="alternate-stage4-run")
        alternate_observations = tuple(
            _obs(qid, 1.0, producer_run_id=alternate_binding.run_id)
            for qid in range(EXP009_RECALL_AUDIT_COUNT)
        )
        alternate = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids,
            observations=alternate_observations,
            **{**_CONTEXT, "binding": alternate_binding},
        )
        self.assertNotEqual(original.evidence_digest, alternate.evidence_digest)

    def test_empty_observations_is_not_applicable(self) -> None:
        result = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=(), **_CONTEXT
        )
        self.assertFalse(result.recall_audit_complete_and_passing)
        self.assertIsNone(result.lower_bound)

    def test_expected_query_id_set_must_match_contract_size(self) -> None:
        with self.assertRaisesRegex(ValueError, "1200"):
            evaluate_recall_audit_evidence(
                expected_query_ids=frozenset(range(600)), observations=(), **_CONTEXT
            )

    def test_every_not_applicable_path_sets_incomplete_status_directly(self) -> None:
        """The evaluator sets ``status`` at each return point in its own
        control flow; nothing external reverse-engineers it from reason
        codes later. This test walks every not-applicable path this file
        exercises and confirms each one is INCOMPLETE, not FAILING."""

        cases = [
            tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT - 1)),  # wrong count
            (),  # empty
        ]
        for observations in cases:
            result = evaluate_recall_audit_evidence(
                expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
            )
            self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)


class EvidenceBindingTests(unittest.TestCase):
    """ADR-008 Stage-4 evidence-binding repair: a recall evaluation may only
    ever claim ``evidence_binding_sha256`` when the supplied context has been
    independently confirmed to match the declared ``Stage4EvidenceBinding`` --
    never on the caller's say-so alone. Any mismatch is INCOMPLETE, never a
    generic completeness failure, and never proceeds to compute a bound."""

    def setUp(self) -> None:
        self.expected_ids = frozenset(range(EXP009_RECALL_AUDIT_COUNT))
        self.observations = tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT))

    def _evaluate(self, **context_overrides):
        context = dict(_CONTEXT)
        context.update(context_overrides)
        return evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=self.observations, **context
        )

    def test_mismatched_binding_metric_is_incomplete(self) -> None:
        # The binding's own coherence check now requires candidate_search_configuration
        # to agree with the binding's header metric -- so a self-consistent
        # L2 binding is constructed, and the mismatch is against the caller's
        # (still-COSINE) claimed search_configuration in _CONTEXT.
        result = self._evaluate(
            binding=_binding(
                metric=Metric.L2,
                candidate_search_configuration=_search_configuration(metric=Metric.L2, radius=0.6),
            )
        )
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)
        self.assertIsNone(result.evidence_binding_sha256)
        self.assertIsNone(result.lower_bound)

    def test_mismatched_binding_threshold_stratum_is_incomplete(self) -> None:
        result = self._evaluate(
            binding=_binding(
                threshold_stratum="target-075",
                candidate_search_configuration=_search_configuration(threshold_label="target-075"),
            )
        )
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)
        self.assertIsNone(result.evidence_binding_sha256)

    def test_mismatched_binding_candidate_ef_is_incomplete(self) -> None:
        result = self._evaluate(
            binding=_binding(
                candidate_ef=400,
                last_known_good_ef=200,
                current_ef=200,
                candidate_search_configuration=_search_configuration(ef=400),
            )
        )
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)
        self.assertIsNone(result.evidence_binding_sha256)

    def test_mismatched_binding_radius_is_incomplete(self) -> None:
        """The radius-binding-omission repair: a binding whose embedded
        candidate_search_configuration agrees on metric/threshold_label/ef
        but disagrees on radius alone must still fail the gate -- this is
        the exact shape of the originally discovered exploit."""
        result = self._evaluate(
            binding=_binding(candidate_search_configuration=_search_configuration(radius=0.6))
        )
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)
        self.assertIsNone(result.evidence_binding_sha256)
        self.assertIsNone(result.lower_bound)
        self.assertEqual(result.sample_count, 0)

    def test_mismatched_binding_identity_is_incomplete(self) -> None:
        result = self._evaluate(binding=_binding(identity=_identity(hnsw_binding_id="different")))
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)
        self.assertIsNone(result.evidence_binding_sha256)

    def test_mismatched_binding_manifest_is_incomplete(self) -> None:
        result = self._evaluate(binding=_binding(dataset002_manifest_sha256="9" * 64))
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)
        self.assertIsNone(result.evidence_binding_sha256)

    def test_mismatched_frozen_query_ids_digest_is_incomplete(self) -> None:
        """The caller's own claimed digest of the frozen-ID population must
        equal the one baked into the binding -- a self-consistent but
        differently-declared population must still fail closed."""
        result = self._evaluate(frozen_query_ids_sha256="0" * 64)
        self.assertEqual(result.status, EvaluationStatus.INCOMPLETE)
        self.assertIn("EVIDENCE_BINDING_MISMATCH", result.reason_codes)
        self.assertIsNone(result.evidence_binding_sha256)

    def test_hand_authored_binding_with_forged_sha256_is_still_checked_by_value(self) -> None:
        """A binding object's own .sha256 is always re-derived from its own
        canonical fields -- there is no way to hand someone a binding whose
        claimed digest doesn't match what it actually contains, so a
        'hash-mismatched' binding attack is structurally not possible here.
        This test proves the contrapositive: two differently-constructed
        bindings claiming the same *intended* configuration but actually
        differing in one field never collide on .sha256."""
        b1 = _binding()
        b2 = _binding(run_id="a-different-run-id")
        self.assertNotEqual(b1.sha256, b2.sha256)

    def test_binding_mismatch_examines_no_observations(self) -> None:
        """A binding-context mismatch must fail before any per-observation
        work, not after partially scanning observations."""
        result = self._evaluate(
            binding=_binding(
                metric=Metric.L2,
                candidate_search_configuration=_search_configuration(metric=Metric.L2, radius=0.6),
            )
        )
        self.assertEqual(result.sample_count, 0)
        self.assertIsNone(result.evidence_digest)


class BuildRecallAuditReportTests(unittest.TestCase):
    """``Stage4RecallAuditReport`` is a recall-only status. It must never be
    mistaken for a complete Stage-4 qualification -- it self-identifies via
    ``report_kind`` and carries no latency information at all."""

    def setUp(self) -> None:
        self.expected_ids = frozenset(range(EXP009_RECALL_AUDIT_COUNT))

    def test_passing_recall_evaluation_yields_passing_recall_only_report(self) -> None:
        observations = tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT))
        recall_evaluation = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )

        report = build_recall_audit_report(recall_evaluation)

        self.assertIsInstance(report, Stage4RecallAuditReport)
        self.assertEqual(report.report_kind, "RECALL_AUDIT_ONLY")
        self.assertEqual(report.status, EvaluationStatus.PASSING)
        self.assertIs(report.recall_evaluation, recall_evaluation)

    def test_report_kind_is_never_a_qualification_claim(self) -> None:
        observations = tuple(_obs(qid, 1.0) for qid in range(EXP009_RECALL_AUDIT_COUNT))
        recall_evaluation = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        report = build_recall_audit_report(recall_evaluation)
        self.assertNotIn("QUALIFICATION", report.report_kind)
        self.assertNotIn("STAGE4_DECISION", report.report_kind)

    def test_failing_recall_evaluation_yields_failing_report(self) -> None:
        observations = tuple(_obs(qid, 0.0) for qid in range(EXP009_RECALL_AUDIT_COUNT))
        recall_evaluation = evaluate_recall_audit_evidence(
            expected_query_ids=self.expected_ids, observations=observations, **_CONTEXT
        )
        report = build_recall_audit_report(recall_evaluation)
        self.assertEqual(report.status, EvaluationStatus.FAILING)


if __name__ == "__main__":
    unittest.main()
