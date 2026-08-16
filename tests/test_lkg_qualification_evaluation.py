"""Pure Checkpoint-C contract and evaluation regression tests."""

from __future__ import annotations

import math
import unittest
from dataclasses import replace

from vdbench.config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from vdbench.lkg_phase2_source_binding import (
    INGESTION_SCHEMA_VERSION,
    LkgWindowReadinessIngestion,
    ingestion_payload_document_digest,
)
from vdbench.lkg_qualification_evaluation import (
    EVALUATION_CONTRACT_SCHEMA_VERSION,
    LkgEpochEvaluation,
    LkgQualificationEvaluation,
    LkgQualificationStatus,
    LkgWindowEvaluation,
    default_lkg_ef_eligibility_rule,
    default_lkg_qualification_semantics_rule,
    ef_eligibility_rule_payload_document,
    epoch_evaluation_payload_document,
    evaluate_epoch,
    evaluate_run,
    evaluate_window,
    evaluation_contract_payload_document,
    evaluation_contract_payload_document_digest,
    evaluation_payload_document,
    evaluation_payload_document_digest,
    lkg_ef_eligibility_rule_from_payload,
    lkg_epoch_evaluation_from_payload,
    lkg_qualification_evaluation_contract_from_payload,
    lkg_qualification_evaluation_from_payload,
    lkg_qualification_semantics_rule_from_payload,
    lkg_window_evaluation_from_payload,
    qualification_semantics_rule_payload_document,
    qualification_semantics_rule_payload_document_digest,
    window_evaluation_payload_document,
    window_evaluation_payload_document_digest,
)
from vdbench.lkg_qualification_evidence import (
    LkgAttemptStatus,
    LkgQueryAttempt,
    LkgQueryObservation,
)
from vdbench.lkg_qualification_seal import (
    LkgPositionClassification,
    LkgPositionStatus,
    LkgRunSeal,
    LkgSealWorkloadIdentity,
    derive_completion_state,
)
from vdbench.lkg_run_binding import lkg_ordered_query_ids_sha256
from vdbench.lkg_window_readiness import (
    FakeLkgWindowOperationalReadinessProvider,
    lkg_window_operational_readiness_evidence_from_payload,
    readiness_payload_document,
    readiness_payload_document_digest,
)

RUN_BINDING_SHA256 = "a" * 64
SEAL_DIGEST = "e" * 64
SOURCE_BINDING_DIGEST = "d" * 64


def _contract():
    payload = {
        "contract_schema_version": EVALUATION_CONTRACT_SCHEMA_VERSION,
        "expected_query_count": 2400,
        "windows_per_run": 12,
        "positions_per_window": 200,
        "epoch_count": 2,
        "windows_per_epoch": 6,
        "observations_per_epoch": 1200,
        "recall_floor": 0.95,
        "latency_ceiling_ms": 10.0,
        "latency_percentile": 0.95,
        "arithmetic_mean_formula_version": "fsum_arithmetic_mean.v1",
        "nearest_rank_formula_version": "nearest_rank_ceil.v1",
    }
    return lkg_qualification_evaluation_contract_from_payload(
        payload,
        canonical_contract_digest=evaluation_contract_payload_document_digest(payload),
    )


def _search_configuration(*, ef: int = 200, metric: Metric = Metric.L2) -> SearchConfiguration:
    return SearchConfiguration(
        metric=metric,
        threshold_label="target-075",
        radius=1.0,
        index_track=IndexTrack.HNSW,
        ef=ef,
        limit=100,
        consistency_level="Strong",
    )


def _positions(
    overrides: dict[int, LkgPositionStatus] | None = None,
) -> tuple[LkgPositionClassification, ...]:
    overrides = overrides or {}
    reason_by_status = {
        LkgPositionStatus.CLEAN_SUCCESS: (),
        LkgPositionStatus.FAILED: ("DURABLE_FAILURE_PRESENT",),
        LkgPositionStatus.MALFORMED: ("MULTIPLE_SUCCESSFUL_ATTEMPTS",),
        LkgPositionStatus.MISSING: (),
    }
    result = []
    for sequence in range(2400):
        status = overrides.get(sequence, LkgPositionStatus.CLEAN_SUCCESS)
        result.append(
            LkgPositionClassification(
                attempt_sequence=sequence,
                query_id=1000 + sequence,
                classification=status,
                reason_codes=reason_by_status[status],
            )
        )
    return tuple(result)


def _attempt(
    sequence: int,
    *,
    recall: float = 0.96,
    latency_ms: float = 5.0,
    threshold_violations: int = 0,
    metric: Metric = Metric.L2,
    ef: int = 200,
    threshold_stratum: str = "target-075",
) -> LkgQueryAttempt:
    query_id = 1000 + sequence
    observation = LkgQueryObservation(
        query_id=query_id,
        metric=metric,
        threshold_stratum=threshold_stratum,
        ef=ef,
        recall=recall,
        latency_ms=latency_ms,
        start_ns=sequence * 2,
        end_ns=sequence * 2 + 1,
        exact_cardinality=10,
        threshold_violation_count=threshold_violations,
    )
    return LkgQueryAttempt(
        query_id=query_id,
        attempt_sequence=sequence,
        attempt_number=1,
        status=LkgAttemptStatus.SUCCESS,
        error_code=None,
        run_binding_sha256=RUN_BINDING_SHA256,
        observation=observation,
    )


def _attempts_for_positions(
    positions: tuple[LkgPositionClassification, ...],
    **observation_overrides,
) -> tuple[LkgQueryAttempt, ...]:
    return tuple(
        _attempt(position.attempt_sequence, **observation_overrides)
        for position in positions
        if position.classification is LkgPositionStatus.CLEAN_SUCCESS
    )


def _ingestion(
    window_index: int,
    *,
    health_checked: bool = True,
    health_passed: bool = True,
    rollback_tested: bool = True,
    rollback_ready: bool = True,
) -> LkgWindowReadinessIngestion:
    provider = FakeLkgWindowOperationalReadinessProvider()
    evidence = provider.capture_or_return(
        readiness_check_id=f"check-{window_index}",
        source_run_id="run-001",
        source_run_binding_sha256=RUN_BINDING_SHA256,
        window_index=window_index,
        epoch_index=window_index // 6,
        first_attempt_sequence=window_index * 200,
        last_attempt_sequence=window_index * 200 + 199,
    )
    payload = readiness_payload_document(evidence)
    payload.update(
        {
            "health_checked": health_checked,
            "health_passed": health_passed,
            "rollback_tested": rollback_tested,
            "rollback_ready": rollback_ready,
            "reason_codes": [],
        }
    )
    evidence = lkg_window_operational_readiness_evidence_from_payload(
        payload,
        canonical_document_digest=readiness_payload_document_digest(payload),
    )
    ingestion_payload = {
        "ingestion_schema_version": INGESTION_SCHEMA_VERSION,
        "source_run_id": "run-001",
        "window_index": window_index,
        "epoch_index": window_index // 6,
        "original_evidence": readiness_payload_document(evidence),
        "original_evidence_digest": evidence.canonical_document_digest,
        "source_run_seal_digest": SEAL_DIGEST,
        "phase2_source_binding_digest": SOURCE_BINDING_DIGEST,
        "ingested_at_utc": "2026-01-01T00:01:00.000000Z",
    }
    return LkgWindowReadinessIngestion(
        ingestion_schema_version=INGESTION_SCHEMA_VERSION,
        source_run_id="run-001",
        window_index=window_index,
        epoch_index=window_index // 6,
        original_evidence=evidence,
        source_run_seal_digest=SEAL_DIGEST,
        phase2_source_binding_digest=SOURCE_BINDING_DIGEST,
        ingested_at_utc="2026-01-01T00:01:00.000000Z",
        canonical_ingestion_digest=ingestion_payload_document_digest(ingestion_payload),
    )


def _seal(positions: tuple[LkgPositionClassification, ...]) -> LkgRunSeal:
    counts = {
        status: sum(position.classification is status for position in positions)
        for status in LkgPositionStatus
    }
    completion = derive_completion_state(
        failed_position_count=counts[LkgPositionStatus.FAILED],
        malformed_position_count=counts[LkgPositionStatus.MALFORMED],
        missing_position_count=counts[LkgPositionStatus.MISSING],
    )
    return LkgRunSeal(
        seal_schema_version=1,
        run_id="run-001",
        run_binding_sha256=RUN_BINDING_SHA256,
        phase1_ledger_schema_version=5,
        workload_identity=LkgSealWorkloadIdentity(
            dataset_id="DATASET-003",
            dataset_version="DATASET-003-v1",
            manifest_sha256="b" * 64,
            query_role="lkg_qualification",
        ),
        expected_query_count=2400,
        qualification_ordered_query_ids_sha256=lkg_ordered_query_ids_sha256(
            tuple(position.query_id for position in positions)
        ),
        final_chain_head_sha256="c" * 64,
        position_classifications=positions,
        successful_position_count=counts[LkgPositionStatus.CLEAN_SUCCESS],
        failed_position_count=counts[LkgPositionStatus.FAILED],
        malformed_position_count=counts[LkgPositionStatus.MALFORMED],
        missing_position_count=counts[LkgPositionStatus.MISSING],
        successful_attempt_count=counts[LkgPositionStatus.CLEAN_SUCCESS],
        failed_attempt_count=counts[LkgPositionStatus.FAILED],
        total_durable_attempt_count=(
            counts[LkgPositionStatus.CLEAN_SUCCESS]
            + counts[LkgPositionStatus.FAILED]
            + counts[LkgPositionStatus.MALFORMED] * 2
        ),
        completion_state=completion,
        expected_completion_state=completion,
        seal_reason="RUN_COMPLETE",
        sealed_at_utc="2026-01-01T00:00:00.000000Z",
        canonical_seal_document_digest=SEAL_DIGEST,
    )


def _window(
    window_index: int,
    positions: tuple[LkgPositionClassification, ...],
    attempts: tuple[LkgQueryAttempt, ...],
    ingestion: LkgWindowReadinessIngestion | None,
) -> LkgWindowEvaluation:
    first = window_index * 200
    last = first + 200
    contributors = tuple(
        attempt for attempt in attempts if first <= attempt.attempt_sequence < last
    )
    return evaluate_window(
        window_index=window_index,
        position_classifications=positions,
        contributing_attempts=contributors,
        readiness_ingestion=ingestion,
        source_run_binding_sha256=RUN_BINDING_SHA256,
        search_configuration=_search_configuration(),
    )


def _passing_run() -> LkgQualificationEvaluation:
    positions = _positions()
    return evaluate_run(
        seal=_seal(positions),
        attempts=_attempts_for_positions(positions),
        ingestions=tuple(_ingestion(index) for index in range(12)),
        contract=_contract(),
        ef_rule=default_lkg_ef_eligibility_rule(),
        semantics_rule=default_lkg_qualification_semantics_rule(),
        search_configuration=_search_configuration(),
        phase2_source_binding_digest=SOURCE_BINDING_DIGEST,
        evaluator_identity="evaluator-v1",
        evaluator_source_revision="revision-v1",
        evaluated_at_utc="2026-01-01T00:02:00.000000Z",
    )


class TestArtifactContracts(unittest.TestCase):
    def test_status_enum_exact_values(self):
        self.assertEqual(
            [status.value for status in LkgQualificationStatus],
            ["INCOMPLETE", "PASSING", "FAILING"],
        )

    def test_evaluation_contract_round_trip(self):
        contract = _contract()
        payload = evaluation_contract_payload_document(contract)
        self.assertEqual(
            lkg_qualification_evaluation_contract_from_payload(
                payload, canonical_contract_digest=contract.canonical_contract_digest
            ),
            contract,
        )

    def test_schema_v1_statistical_values_are_pinned(self):
        for field, alternate in (
            ("recall_floor", 0.94),
            ("latency_ceiling_ms", 9.0),
            ("latency_percentile", 0.90),
        ):
            payload = evaluation_contract_payload_document(_contract())
            payload[field] = alternate
            with (
                self.subTest(field=field, alternate=alternate),
                self.assertRaises(ContractViolation),
            ):
                lkg_qualification_evaluation_contract_from_payload(
                    payload,
                    canonical_contract_digest=(
                        evaluation_contract_payload_document_digest(payload)
                    ),
                )

    def test_ef_rule_round_trip_and_eligibility(self):
        rule = default_lkg_ef_eligibility_rule()
        payload = ef_eligibility_rule_payload_document(rule)
        self.assertEqual(
            lkg_ef_eligibility_rule_from_payload(
                payload, canonical_rule_digest=rule.canonical_rule_digest
            ),
            rule,
        )
        for ef in (200, 400, 800, 1600):
            with self.subTest(ef=ef):
                self.assertIn(ef, rule.eligible_ef_values)
        self.assertNotIn(100, rule.eligible_ef_values)

    def test_semantics_rule_round_trip(self):
        rule = default_lkg_qualification_semantics_rule()
        payload = qualification_semantics_rule_payload_document(rule)
        self.assertEqual(
            lkg_qualification_semantics_rule_from_payload(
                payload, canonical_rule_digest=rule.canonical_rule_digest
            ),
            rule,
        )

    def test_schema_v1_readiness_dimensions_are_pinned(self):
        for dimensions in ([], ["health"], ["rollback"], ["health", "other"]):
            payload = qualification_semantics_rule_payload_document(
                default_lkg_qualification_semantics_rule()
            )
            payload["required_readiness_dimensions"] = dimensions
            with (
                self.subTest(dimensions=dimensions),
                self.assertRaises(ContractViolation),
            ):
                lkg_qualification_semantics_rule_from_payload(
                    payload,
                    canonical_rule_digest=(
                        qualification_semantics_rule_payload_document_digest(
                            payload
                        )
                    ),
                )

    def test_window_epoch_and_final_round_trip(self):
        evaluation = _passing_run()
        window = evaluation.window_evaluations[0]
        epoch = evaluation.epoch_evaluations[0]
        self.assertEqual(
            lkg_window_evaluation_from_payload(
                window_evaluation_payload_document(window),
                canonical_window_evaluation_digest=window.canonical_window_evaluation_digest,
            ),
            window,
        )
        self.assertEqual(
            lkg_epoch_evaluation_from_payload(
                epoch_evaluation_payload_document(epoch),
                canonical_epoch_evaluation_digest=epoch.canonical_epoch_evaluation_digest,
            ),
            epoch,
        )
        self.assertEqual(
            lkg_qualification_evaluation_from_payload(
                evaluation_payload_document(evaluation),
                canonical_evaluation_digest=evaluation.canonical_evaluation_digest,
            ),
            evaluation,
        )

    def test_unknown_and_missing_fields_rejected(self):
        payload = evaluation_contract_payload_document(_contract())
        unknown = dict(payload, unexpected=True)
        missing = dict(payload)
        del missing["recall_floor"]
        for candidate in (unknown, missing):
            with (
                self.subTest(candidate=candidate),
                self.assertRaises(ContractViolation),
            ):
                lkg_qualification_evaluation_contract_from_payload(
                    candidate,
                    canonical_contract_digest=evaluation_contract_payload_document_digest(
                        candidate
                    ),
                )

    def test_malformed_type_and_digest_rejected(self):
        payload = evaluation_contract_payload_document(_contract())
        malformed = dict(payload, expected_query_count="2400")
        with self.assertRaises(ContractViolation):
            lkg_qualification_evaluation_contract_from_payload(
                malformed,
                canonical_contract_digest=evaluation_contract_payload_document_digest(
                    malformed
                ),
            )
        with self.assertRaises(ContractViolation):
            lkg_qualification_evaluation_contract_from_payload(
                payload, canonical_contract_digest="0" * 64
            )

    def test_unsupported_formula_and_rule_versions_rejected(self):
        contract_payload = evaluation_contract_payload_document(_contract())
        contract_payload["nearest_rank_formula_version"] = "unknown.v1"
        with self.assertRaises(ContractViolation):
            lkg_qualification_evaluation_contract_from_payload(
                contract_payload,
                canonical_contract_digest=evaluation_contract_payload_document_digest(
                    contract_payload
                ),
            )
        semantics = qualification_semantics_rule_payload_document(
            default_lkg_qualification_semantics_rule()
        )
        semantics["finalization_rule_version"] = "unknown.v1"
        from vdbench.lkg_qualification_evaluation import (
            qualification_semantics_rule_payload_document_digest,
        )

        with self.assertRaises(ContractViolation):
            lkg_qualification_semantics_rule_from_payload(
                semantics,
                canonical_rule_digest=qualification_semantics_rule_payload_document_digest(
                    semantics
                ),
            )

    def test_exact_geometry_and_window_ranges(self):
        contract = _contract()
        self.assertEqual(
            (
                contract.expected_query_count,
                contract.windows_per_run,
                contract.positions_per_window,
                contract.epoch_count,
                contract.windows_per_epoch,
                contract.observations_per_epoch,
            ),
            (2400, 12, 200, 2, 6, 1200),
        )
        evaluation = _passing_run()
        self.assertEqual(
            [
                (window.first_attempt_sequence, window.last_attempt_sequence)
                for window in evaluation.window_evaluations
            ],
            [(index * 200, index * 200 + 199) for index in range(12)],
        )
        self.assertEqual(
            [
                (epoch.first_window_index, epoch.last_window_index)
                for epoch in evaluation.epoch_evaluations
            ],
            [(0, 5), (6, 11)],
        )

    def test_window_counts_must_sum_to_200(self):
        payload = window_evaluation_payload_document(_passing_run().window_evaluations[0])
        payload["clean_success_position_count"] = 199
        payload["contributing_observation_count"] = 199
        with self.assertRaises(ContractViolation):
            lkg_window_evaluation_from_payload(
                payload,
                canonical_window_evaluation_digest=window_evaluation_payload_document_digest(
                    payload
                ),
            )

    def test_final_nested_windows_mismatch_rejected(self):
        evaluation = _passing_run()
        payload = evaluation_payload_document(evaluation)
        payload["epoch_evaluations"][0]["window_evaluations"][0] = payload[
            "window_evaluations"
        ][1]
        with self.assertRaises(ContractViolation):
            lkg_qualification_evaluation_from_payload(
                payload,
                canonical_evaluation_digest=evaluation_payload_document_digest(payload),
            )

    def test_final_readiness_digest_tuple_mismatch_rejected(self):
        evaluation = _passing_run()
        payload = evaluation_payload_document(evaluation)
        payload["window_ingestion_digests"][0] = "f" * 64
        with self.assertRaises(ContractViolation):
            lkg_qualification_evaluation_from_payload(
                payload,
                canonical_evaluation_digest=evaluation_payload_document_digest(payload),
            )

    def test_final_status_and_qualified_contradictions_rejected(self):
        evaluation = _passing_run()
        for field, value in (("status", "INCOMPLETE"), ("qualified", False)):
            payload = evaluation_payload_document(evaluation)
            payload[field] = value
            with (
                self.subTest(field=field),
                self.assertRaises(ContractViolation),
            ):
                lkg_qualification_evaluation_from_payload(
                    payload,
                    canonical_evaluation_digest=evaluation_payload_document_digest(
                        payload
                    ),
                )

    def test_final_contract_rejects_epoch_statistic_status_contradiction(self):
        payload = evaluation_payload_document(_passing_run())
        payload["epoch_evaluations"][0]["observed_mean_capped_recall"] = 0.1
        with self.assertRaises(ContractViolation):
            lkg_qualification_evaluation_from_payload(
                payload,
                canonical_evaluation_digest=evaluation_payload_document_digest(payload),
            )


class TestRawEvidenceBinding(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.positions = _positions()
        cls.attempts = tuple(_attempt(sequence) for sequence in range(200))
        cls.ingestion = _ingestion(0)

    def _evaluate(self, attempts):
        return evaluate_window(
            window_index=0,
            position_classifications=self.positions,
            contributing_attempts=tuple(attempts),
            readiness_ingestion=self.ingestion,
            source_run_binding_sha256=RUN_BINDING_SHA256,
            search_configuration=_search_configuration(),
        )

    def test_exact_200_contributors_pass(self):
        result = self._evaluate(self.attempts)
        self.assertEqual(result.status, LkgQualificationStatus.PASSING)
        self.assertEqual(result.contributing_observation_count, 200)

    def test_zero_contributors_is_source_error(self):
        with self.assertRaises(ContractViolation):
            self._evaluate(())

    def test_199_contributors_is_source_error(self):
        with self.assertRaises(ContractViolation):
            self._evaluate(self.attempts[:-1])

    def test_duplicate_contributor_is_source_error(self):
        with self.assertRaises(ContractViolation):
            self._evaluate(self.attempts[:-1] + (self.attempts[0],))

    def test_wrong_sequence_is_source_error(self):
        bad = replace(self.attempts[0], attempt_sequence=1)
        with self.assertRaises(ContractViolation):
            self._evaluate((bad,) + self.attempts[1:])

    def test_wrong_attempt_query_id_is_source_error(self):
        bad = replace(self.attempts[0], query_id=999999)
        with self.assertRaises(ContractViolation):
            self._evaluate((bad,) + self.attempts[1:])

    def test_wrong_run_binding_is_source_error(self):
        bad = replace(self.attempts[0], run_binding_sha256="f" * 64)
        with self.assertRaises(ContractViolation):
            self._evaluate((bad,) + self.attempts[1:])

    def test_wrong_observation_query_id_is_source_error(self):
        bad = replace(
            self.attempts[0],
            observation=replace(self.attempts[0].observation, query_id=999999),
        )
        with self.assertRaises(ContractViolation):
            self._evaluate((bad,) + self.attempts[1:])

    def test_wrong_observation_ef_is_source_error(self):
        bad = replace(
            self.attempts[0], observation=replace(self.attempts[0].observation, ef=400)
        )
        with self.assertRaises(ContractViolation):
            self._evaluate((bad,) + self.attempts[1:])

    def test_wrong_observation_metric_is_source_error(self):
        bad = replace(
            self.attempts[0],
            observation=replace(self.attempts[0].observation, metric=Metric.COSINE),
        )
        with self.assertRaises(ContractViolation):
            self._evaluate((bad,) + self.attempts[1:])

    def test_wrong_threshold_stratum_is_source_error(self):
        bad = replace(
            self.attempts[0],
            observation=replace(
                self.attempts[0].observation, threshold_stratum="target-050"
            ),
        )
        with self.assertRaises(ContractViolation):
            self._evaluate((bad,) + self.attempts[1:])


class TestStatusReadinessAndStatistics(unittest.TestCase):
    def _window_with_status(
        self,
        status: LkgPositionStatus,
        *,
        readiness: LkgWindowReadinessIngestion | None = None,
    ) -> LkgWindowEvaluation:
        positions = _positions({0: status})
        attempts = _attempts_for_positions(positions)
        return _window(0, positions, attempts, readiness or _ingestion(0))

    def test_failed_position_fails_window(self):
        result = self._window_with_status(LkgPositionStatus.FAILED)
        self.assertEqual(result.status, LkgQualificationStatus.FAILING)
        self.assertIn("POSITION_FAILED", result.status_reason_codes)

    def test_malformed_position_fails_window(self):
        result = self._window_with_status(LkgPositionStatus.MALFORMED)
        self.assertEqual(result.status, LkgQualificationStatus.FAILING)
        self.assertIn("POSITION_MALFORMED", result.status_reason_codes)

    def test_missing_position_is_incomplete(self):
        result = self._window_with_status(LkgPositionStatus.MISSING)
        self.assertEqual(result.status, LkgQualificationStatus.INCOMPLETE)
        self.assertIn("POSITION_MISSING", result.status_reason_codes)

    def test_failing_precedes_incomplete(self):
        positions = _positions({0: LkgPositionStatus.FAILED, 1: LkgPositionStatus.MISSING})
        result = _window(0, positions, _attempts_for_positions(positions), None)
        self.assertEqual(result.status, LkgQualificationStatus.FAILING)
        self.assertIn("READINESS_MISSING", result.status_reason_codes)

    def test_threshold_violation_fails_window(self):
        positions = _positions()
        attempts = tuple(
            _attempt(sequence, threshold_violations=1 if sequence == 0 else 0)
            for sequence in range(200)
        )
        result = _window(0, positions, attempts, _ingestion(0))
        self.assertEqual(result.status, LkgQualificationStatus.FAILING)
        self.assertIn("QUERY_CORRECTNESS_FAILED", result.status_reason_codes)

    def test_missing_readiness_is_incomplete(self):
        positions = _positions()
        result = _window(0, positions, tuple(_attempt(i) for i in range(200)), None)
        self.assertEqual(result.status, LkgQualificationStatus.INCOMPLETE)
        self.assertIn("READINESS_MISSING", result.status_reason_codes)

    def test_each_readiness_failure_is_failing(self):
        cases = (
            ({"health_checked": False, "health_passed": False}, "HEALTH_NOT_CHECKED"),
            ({"health_checked": True, "health_passed": False}, "HEALTH_FAILED"),
            ({"rollback_tested": False, "rollback_ready": False}, "ROLLBACK_NOT_TESTED"),
            ({"rollback_tested": True, "rollback_ready": False}, "ROLLBACK_NOT_READY"),
        )
        positions = _positions()
        attempts = tuple(_attempt(i) for i in range(200))
        for kwargs, reason in cases:
            with self.subTest(reason=reason):
                result = _window(0, positions, attempts, _ingestion(0, **kwargs))
                self.assertEqual(result.status, LkgQualificationStatus.FAILING)
                self.assertIn(reason, result.status_reason_codes)

    def _epoch(
        self,
        *,
        recalls: list[float] | None = None,
        latencies: list[float] | None = None,
        incomplete_window: int | None = None,
        failing_window: int | None = None,
    ) -> LkgEpochEvaluation:
        recalls = recalls or [0.96] * 1200
        latencies = latencies or [5.0] * 1200
        positions = _positions()
        attempts = tuple(
            _attempt(
                sequence,
                recall=recalls[sequence],
                latency_ms=latencies[sequence],
                threshold_violations=(1 if failing_window == sequence // 200 and sequence % 200 == 0 else 0),
            )
            for sequence in range(1200)
        )
        windows = []
        for window_index in range(6):
            readiness = None if incomplete_window == window_index else _ingestion(window_index)
            windows.append(_window(window_index, positions, attempts, readiness))
        return evaluate_epoch(
            epoch_index=0,
            window_evaluations=tuple(windows),
            epoch_contributing_attempts=attempts,
            contract=_contract(),
        )

    def test_no_per_window_recall_or_latency_slo(self):
        positions = _positions()
        attempts = tuple(_attempt(i, recall=0.0, latency_ms=100.0) for i in range(200))
        result = _window(0, positions, attempts, _ingestion(0))
        self.assertEqual(result.status, LkgQualificationStatus.PASSING)

    def test_incomplete_epoch_has_no_statistics(self):
        epoch = self._epoch(incomplete_window=0)
        self.assertEqual(epoch.status, LkgQualificationStatus.INCOMPLETE)
        self.assertIsNone(epoch.observed_mean_capped_recall)
        self.assertIsNone(epoch.observed_p95_latency_ms)

    def test_failed_window_is_not_dropped(self):
        epoch = self._epoch(failing_window=0)
        self.assertEqual(epoch.status, LkgQualificationStatus.FAILING)
        self.assertIsNone(epoch.observed_mean_capped_recall)
        self.assertIsNone(epoch.observed_p95_latency_ms)

    def test_passing_epoch_requires_exactly_1200_contributors(self):
        positions = _positions()
        attempts = tuple(_attempt(i) for i in range(1200))
        windows = tuple(
            _window(index, positions, attempts, _ingestion(index)) for index in range(6)
        )
        with self.assertRaises(ContractViolation):
            evaluate_epoch(
                epoch_index=0,
                window_evaluations=windows,
                epoch_contributing_attempts=attempts[:-1],
                contract=_contract(),
            )

    def test_recall_exactly_floor_passes_and_next_float_below_fails(self):
        passing = self._epoch(recalls=[0.95] * 1200)
        failing = self._epoch(recalls=[math.nextafter(0.95, 0.0)] * 1200)
        self.assertEqual(passing.status, LkgQualificationStatus.PASSING)
        self.assertEqual(failing.status, LkgQualificationStatus.FAILING)
        self.assertIn("EPOCH_RECALL_BELOW_FLOOR", failing.status_reason_codes)

    def test_p95_exactly_ceiling_passes_and_next_float_above_fails(self):
        passing = self._epoch(latencies=[10.0] * 1200)
        failing = self._epoch(latencies=[math.nextafter(10.0, math.inf)] * 1200)
        self.assertEqual(passing.status, LkgQualificationStatus.PASSING)
        self.assertEqual(failing.status, LkgQualificationStatus.FAILING)
        self.assertIn("EPOCH_LATENCY_ABOVE_CEILING", failing.status_reason_codes)

    def test_nearest_rank_uses_index_1139_without_interpolation(self):
        pass_latencies = [5.0] * 1140 + [20.0] * 60
        fail_latencies = [5.0] * 1139 + [20.0] * 61
        passing = self._epoch(latencies=pass_latencies)
        failing = self._epoch(latencies=fail_latencies)
        self.assertEqual(passing.observed_p95_latency_ms, 5.0)
        self.assertEqual(failing.observed_p95_latency_ms, 20.0)

    def test_ineligible_ef_reason_without_invalid_success_observations(self):
        positions = _positions(
            {sequence: LkgPositionStatus.MISSING for sequence in range(2400)}
        )
        evaluation = evaluate_run(
            seal=_seal(positions),
            attempts=(),
            ingestions=(),
            contract=_contract(),
            ef_rule=default_lkg_ef_eligibility_rule(),
            semantics_rule=default_lkg_qualification_semantics_rule(),
            search_configuration=_search_configuration(ef=100),
            phase2_source_binding_digest=SOURCE_BINDING_DIGEST,
            evaluator_identity="evaluator-v1",
            evaluator_source_revision="revision-v1",
            evaluated_at_utc="2026-01-01T00:02:00.000000Z",
        )
        self.assertEqual(evaluation.status, LkgQualificationStatus.FAILING)
        self.assertIn("EF_NOT_ELIGIBLE_FOR_LKG", evaluation.status_reason_codes)
        self.assertFalse(evaluation.qualified)

    def test_incomplete_run_carries_master_lifecycle_reason_codes(self):
        positions = _positions(
            {sequence: LkgPositionStatus.MISSING for sequence in range(2400)}
        )
        evaluation = evaluate_run(
            seal=_seal(positions),
            attempts=(),
            ingestions=(),
            contract=_contract(),
            ef_rule=default_lkg_ef_eligibility_rule(),
            semantics_rule=default_lkg_qualification_semantics_rule(),
            search_configuration=_search_configuration(),
            phase2_source_binding_digest=SOURCE_BINDING_DIGEST,
            evaluator_identity="evaluator-v1",
            evaluator_source_revision="revision-v1",
            evaluated_at_utc="2026-01-01T00:02:00.000000Z",
        )
        self.assertEqual(evaluation.status, LkgQualificationStatus.INCOMPLETE)
        self.assertIn(
            "AWAITING_READINESS_EVIDENCE", evaluation.status_reason_codes
        )
        self.assertIn(
            "PHASE1_POSITION_PERMANENTLY_MISSING",
            evaluation.status_reason_codes,
        )


if __name__ == "__main__":
    unittest.main()
