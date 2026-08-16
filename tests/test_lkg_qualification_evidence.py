"""TDD coverage for LKG-qualification Phase 1 raw per-query evidence capture."""

from __future__ import annotations

import unittest

from vdbench.config import ContractViolation, Metric
from vdbench.lkg_qualification_evidence import (
    LkgAttemptStatus,
    LkgQueryAttempt,
    LkgQueryObservation,
    build_lkg_query_attempt,
    build_lkg_query_observation,
)

_RUN_BINDING_SHA256 = "b" * 64


def _observation_kwargs(**overrides: object) -> dict[str, object]:
    fields: dict[str, object] = {
        "query_id": 1,
        "metric": Metric.L2,
        "threshold_stratum": "target-075",
        "ef": 400,
        "recall": 1.0,
        "latency_ms": 1.2,
        "start_ns": 1_000,
        "end_ns": 2_000,
        "exact_cardinality": 10,
        "threshold_violation_count": 0,
    }
    fields.update(overrides)
    return fields


class RollbackReadinessRemovalTests(unittest.TestCase):
    """Blocker 3: rollback readiness is no longer a per-query concern."""

    def test_rollback_readiness_type_no_longer_exists_in_this_module(self) -> None:
        import vdbench.lkg_qualification_evidence as module

        self.assertFalse(hasattr(module, "RollbackReadinessEvidence"))

    def test_observation_has_no_rollback_readiness_field(self) -> None:
        observation = build_lkg_query_observation(**_observation_kwargs())
        self.assertNotIn("rollback_readiness", observation.__dataclass_fields__)

    def test_observation_has_no_dataset_identity_fields(self) -> None:
        """Dataset/run identity lives on LkgRunBinding, referenced by hash
        from LkgQueryAttempt -- not duplicated onto every observation."""

        observation = build_lkg_query_observation(**_observation_kwargs())
        for field in (
            "qualification_dataset_id",
            "qualification_dataset_version",
            "qualification_manifest_sha256",
            "qualification_query_role",
            "run_binding_sha256",
        ):
            self.assertNotIn(field, observation.__dataclass_fields__)


class BuildLkgQueryObservationTests(unittest.TestCase):
    def test_valid_observation_is_constructed(self) -> None:
        observation = build_lkg_query_observation(**_observation_kwargs())
        self.assertIsInstance(observation, LkgQueryObservation)
        self.assertEqual(observation.query_id, 1)
        self.assertEqual(observation.recall, 1.0)

    def test_result_is_immutable(self) -> None:
        observation = build_lkg_query_observation(**_observation_kwargs())
        with self.assertRaises(AttributeError):
            observation.recall = 0.0  # type: ignore[misc]

    def test_string_query_id_is_accepted(self) -> None:
        observation = build_lkg_query_observation(**_observation_kwargs(query_id="q-1"))
        self.assertEqual(observation.query_id, "q-1")

    # -- ef must be on the ADR-002 actuation ladder -------------------------

    def test_sentinel_ef_100_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(ef=100))

    def test_ef_not_on_ladder_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(ef=300))

    def test_every_eligible_ef_is_accepted(self) -> None:
        for ef in (200, 400, 800, 1600):
            with self.subTest(ef=ef):
                build_lkg_query_observation(**_observation_kwargs(ef=ef))  # must not raise

    # -- query_id ------------------------------------------------------------

    def test_bool_query_id_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(query_id=True))

    def test_empty_string_query_id_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(query_id=""))

    # -- metric / threshold_stratum ------------------------------------------

    def test_plain_string_metric_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(metric="L2"))

    def test_unknown_threshold_stratum_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(threshold_stratum="bogus"))

    # -- recall ---------------------------------------------------------------

    def test_recall_above_one_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(recall=1.5))

    def test_recall_below_zero_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(recall=-0.1))

    def test_recall_bool_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(recall=True))

    def test_recall_nan_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(recall=float("nan")))

    def test_recall_boundary_values_are_accepted(self) -> None:
        build_lkg_query_observation(**_observation_kwargs(recall=0.0))  # must not raise
        build_lkg_query_observation(**_observation_kwargs(recall=1.0))  # must not raise

    # -- latency / raw timestamps ---------------------------------------------

    def test_negative_latency_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(latency_ms=-0.001))

    def test_latency_infinite_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(latency_ms=float("inf")))

    def test_negative_start_ns_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(start_ns=-1))

    def test_end_ns_before_start_ns_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(start_ns=2_000, end_ns=1_000))

    def test_start_ns_bool_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(start_ns=True))

    def test_equal_start_and_end_ns_is_accepted(self) -> None:
        build_lkg_query_observation(
            **_observation_kwargs(start_ns=1_000, end_ns=1_000)
        )  # must not raise

    # -- exact_cardinality / threshold_violation_count -------------------------

    def test_negative_exact_cardinality_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(exact_cardinality=-1))

    def test_negative_threshold_violation_count_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(threshold_violation_count=-1))

    def test_float_threshold_violation_count_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_observation(**_observation_kwargs(threshold_violation_count=1.0))


class BuildLkgQueryAttemptTests(unittest.TestCase):
    def _success_observation(self, query_id: object = 1) -> LkgQueryObservation:
        return build_lkg_query_observation(**_observation_kwargs(query_id=query_id))

    def test_valid_success_attempt(self) -> None:
        attempt = build_lkg_query_attempt(
            query_id=1,
            attempt_sequence=0,
            attempt_number=1,
            status=LkgAttemptStatus.SUCCESS,
            run_binding_sha256=_RUN_BINDING_SHA256,
            observation=self._success_observation(),
        )
        self.assertIsInstance(attempt, LkgQueryAttempt)
        self.assertIsNone(attempt.error_code)
        self.assertIsNotNone(attempt.observation)

    def test_valid_failure_attempt(self) -> None:
        for status in (
            LkgAttemptStatus.CLIENT_ERROR,
            LkgAttemptStatus.TIMEOUT,
            LkgAttemptStatus.MALFORMED_RESPONSE,
            LkgAttemptStatus.ORACLE_ERROR,
        ):
            with self.subTest(status=status):
                attempt = build_lkg_query_attempt(
                    query_id=1,
                    attempt_sequence=0,
                    attempt_number=1,
                    status=status,
                    run_binding_sha256=_RUN_BINDING_SHA256,
                    error_code=f"{status.value}:injected",
                )
                self.assertIsNone(attempt.observation)
                self.assertEqual(attempt.error_code, f"{status.value}:injected")

    def test_result_is_immutable(self) -> None:
        attempt = build_lkg_query_attempt(
            query_id=1,
            attempt_sequence=0,
            attempt_number=1,
            status=LkgAttemptStatus.SUCCESS,
            run_binding_sha256=_RUN_BINDING_SHA256,
            observation=self._success_observation(),
        )
        with self.assertRaises(AttributeError):
            attempt.status = LkgAttemptStatus.CLIENT_ERROR  # type: ignore[misc]

    # -- success/failure shape invariant --------------------------------------

    def test_success_with_error_code_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=0,
                attempt_number=1,
                status=LkgAttemptStatus.SUCCESS,
                run_binding_sha256=_RUN_BINDING_SHA256,
                error_code="SHOULD_NOT_BE_HERE",
                observation=self._success_observation(),
            )

    def test_success_without_observation_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=0,
                attempt_number=1,
                status=LkgAttemptStatus.SUCCESS,
                run_binding_sha256=_RUN_BINDING_SHA256,
            )

    def test_success_observation_query_id_must_match(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=0,
                attempt_number=1,
                status=LkgAttemptStatus.SUCCESS,
                run_binding_sha256=_RUN_BINDING_SHA256,
                observation=self._success_observation(query_id=2),
            )

    def test_failure_without_error_code_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=0,
                attempt_number=1,
                status=LkgAttemptStatus.CLIENT_ERROR,
                run_binding_sha256=_RUN_BINDING_SHA256,
            )

    def test_failure_with_empty_error_code_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=0,
                attempt_number=1,
                status=LkgAttemptStatus.CLIENT_ERROR,
                run_binding_sha256=_RUN_BINDING_SHA256,
                error_code="",
            )

    def test_failure_with_observation_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=0,
                attempt_number=1,
                status=LkgAttemptStatus.CLIENT_ERROR,
                run_binding_sha256=_RUN_BINDING_SHA256,
                error_code="CLIENT_ERROR:Foo",
                observation=self._success_observation(),
            )

    # -- sequence / number / status / binding validation -----------------------

    def test_negative_attempt_sequence_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=-1,
                attempt_number=1,
                status=LkgAttemptStatus.SUCCESS,
                run_binding_sha256=_RUN_BINDING_SHA256,
                observation=self._success_observation(),
            )

    def test_zero_attempt_number_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=0,
                attempt_number=0,
                status=LkgAttemptStatus.SUCCESS,
                run_binding_sha256=_RUN_BINDING_SHA256,
                observation=self._success_observation(),
            )

    def test_second_attempt_number_is_accepted(self) -> None:
        build_lkg_query_attempt(
            query_id=1,
            attempt_sequence=0,
            attempt_number=2,
            status=LkgAttemptStatus.SUCCESS,
            run_binding_sha256=_RUN_BINDING_SHA256,
            observation=self._success_observation(),
        )  # must not raise

    def test_plain_string_status_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=0,
                attempt_number=1,
                status="SUCCESS",  # type: ignore[arg-type]
                run_binding_sha256=_RUN_BINDING_SHA256,
                observation=self._success_observation(),
            )

    def test_malformed_run_binding_sha256_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=0,
                attempt_number=1,
                status=LkgAttemptStatus.SUCCESS,
                run_binding_sha256="not-hex",
                observation=self._success_observation(),
            )

    def test_uppercase_run_binding_sha256_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            build_lkg_query_attempt(
                query_id=1,
                attempt_sequence=0,
                attempt_number=1,
                status=LkgAttemptStatus.SUCCESS,
                run_binding_sha256="B" * 64,
                observation=self._success_observation(),
            )


if __name__ == "__main__":
    unittest.main()
