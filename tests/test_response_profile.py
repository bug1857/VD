from __future__ import annotations

import hashlib
import math
import unicodedata
import unittest
from copy import deepcopy
from dataclasses import fields, replace
from unittest.mock import patch

from vdbench.artifacts import canonical_json_bytes
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.response_profile import (
    ALPHA_CELL,
    ESTIMATOR_CONTRACT_VERSION,
    MEASURED_SEARCH_COUNT,
    OBSERVATION_COUNT,
    P95_LCB_RANK,
    P95_POINT_RANK,
    P95_UCB_RANK,
    PROFILE_HASH_DOMAIN,
    PROFILE_SCHEMA_VERSION,
    SUPPORTED_EFS,
    CalibratedResponseProfile,
    ResponseProfileCalibrationEvidence,
    ResponseProfileContractError,
    ResponseProfileEfObservation,
    ResponseProfileIdentity,
    ResponseProfileQueryObservation,
    build_calibrated_response_profile,
    compute_response_profile_estimates,
    derive_v1_latency_ranks,
    response_profile_document,
    response_profile_payload,
    verify_calibrated_response_profile,
    verify_response_profile_document,
)

GOLDEN_PROFILE_SHA256 = "7e99628c74cec49056787100b601b542ac6f42f6962163fb3b08baac02b9cb0b"
GOLDEN_PROFILE_PAYLOAD_SHA256 = (
    "81fb248e212decded573c11423925a00e0e8304b9f3de283fda11d05a9f725f6"
)


def _digest(character: str) -> str:
    return character * 64


def _configurations(
    *,
    metric: Metric = Metric.L2,
    threshold: str = "target-075",
    radius: float = 0.75,
) -> tuple[SearchConfiguration, ...]:
    return tuple(
        SearchConfiguration(
            metric=metric,
            threshold_label=threshold,
            radius=radius,
            index_track=IndexTrack.HNSW,
            ef=ef,
        )
        for ef in SUPPORTED_EFS
    )


def _identity(**changes: object) -> ResponseProfileIdentity:
    value = ResponseProfileIdentity(
        metric=Metric.L2,
        threshold_stratum="target-075",
        search_configurations=_configurations(),
        hnsw_index_identity="hnsw-index-identity",
        data_identity="dataset-identity",
        workload_manifest_sha256=_digest("1"),
        ordered_query_payload_sha256=_digest("2"),
        replay_schedule_sha256=_digest("3"),
        control_profile_sha256=_digest("4"),
        environment_manifest_sha256=_digest("5"),
        source_revision="revision/r1-not-limited-to-a-git-sha",
        calibration_started_at_utc="2026-08-09T00:00:00Z",
        calibration_completed_at_utc="2026-08-09T00:10:00Z",
        generated_at_utc="2026-08-09T00:10:01Z",
    )
    return replace(value, **changes)


def _evidence(
    *,
    recall_for: object | None = None,
    latency_for: object | None = None,
    string_ids: bool = False,
    raw_evidence_sha256: str | None = None,
) -> ResponseProfileCalibrationEvidence:
    def recall(index: int, ef: int) -> float:
        if recall_for is None:
            return 0.75
        return recall_for(index, ef)  # type: ignore[operator,no-any-return]

    def latency(index: int, ef: int) -> float:
        if latency_for is None:
            return float(index + 1) + ef / 10_000.0
        return latency_for(index, ef)  # type: ignore[operator,no-any-return]

    observations = tuple(
        ResponseProfileQueryObservation(
            query_id=f"query-{index:04d}" if string_ids else index,
            responses=tuple(
                ResponseProfileEfObservation(
                    ef=ef,
                    capped_recall=recall(index, ef),
                    latency_ms=latency(index, ef),
                )
                for ef in SUPPORTED_EFS
            ),
        )
        for index in range(OBSERVATION_COUNT)
    )
    return ResponseProfileCalibrationEvidence(
        raw_evidence_sha256=raw_evidence_sha256 or _digest("6"),
        observations=observations,
    )


def _replace_observation(
    evidence: ResponseProfileCalibrationEvidence,
    index: int,
    observation: ResponseProfileQueryObservation,
) -> ResponseProfileCalibrationEvidence:
    values = list(evidence.observations)
    values[index] = observation
    return replace(evidence, observations=tuple(values))


def _replace_ef_result(
    evidence: ResponseProfileCalibrationEvidence,
    *,
    query_index: int,
    ef_index: int,
    **changes: object,
) -> ResponseProfileCalibrationEvidence:
    query = evidence.observations[query_index]
    results = list(query.responses)
    results[ef_index] = replace(results[ef_index], **changes)
    return _replace_observation(
        evidence, query_index, replace(query, responses=tuple(results))
    )


def _assert_code(
    case: unittest.TestCase,
    code: str,
    callable_: object,
) -> None:
    with case.assertRaises(ResponseProfileContractError) as raised:
        callable_()  # type: ignore[operator]
    case.assertEqual(raised.exception.code, code)


class ResponseProfileStatisticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.evidence = _evidence()

    def test_contract_constants_are_frozen(self) -> None:
        self.assertEqual(SUPPORTED_EFS, (200, 400, 800, 1600))
        self.assertEqual(OBSERVATION_COUNT, 1200)
        self.assertEqual(MEASURED_SEARCH_COUNT, 4800)
        self.assertEqual(ALPHA_CELL, 0.003125)
        self.assertEqual((P95_LCB_RANK, P95_POINT_RANK, P95_UCB_RANK), (1118, 1140, 1161))

    def test_exact_integer_binomial_inversion_reproduces_v1_ranks(self) -> None:
        self.assertEqual(derive_v1_latency_ranks(), (1118, 1161))

    def test_statistics_use_formula_and_one_based_ranks(self) -> None:
        estimates = compute_response_profile_estimates(self.evidence)
        epsilon = math.sqrt(math.log(320.0) / 2400.0)
        for estimate, ef in zip(estimates, SUPPORTED_EFS, strict=True):
            self.assertEqual(estimate.ef, ef)
            self.assertEqual(estimate.observation_count, 1200)
            self.assertEqual(estimate.mean_recall, 0.75)
            self.assertEqual(estimate.recall_lcb, 0.75 - epsilon)
            self.assertEqual(estimate.recall_ucb, 0.75 + epsilon)
            self.assertEqual(estimate.p95_latency_lcb_ms, 1118.0 + ef / 10_000.0)
            self.assertEqual(estimate.p95_latency_ms, 1140.0 + ef / 10_000.0)
            self.assertEqual(estimate.p95_latency_ucb_ms, 1161.0 + ef / 10_000.0)

    def test_recall_bounds_clip_only_to_unit_interval(self) -> None:
        zeros = compute_response_profile_estimates(
            _evidence(recall_for=lambda _index, _ef: 0.0)
        )
        ones = compute_response_profile_estimates(
            _evidence(recall_for=lambda _index, _ef: 1.0)
        )
        self.assertTrue(all(value.recall_lcb == 0.0 for value in zeros))
        self.assertTrue(all(value.recall_ucb == 1.0 for value in ones))

    def test_recall_mean_uses_math_fsum_in_canonical_order(self) -> None:
        tiny = 2.0**-53
        evidence = _evidence(
            recall_for=lambda index, ef: (
                1.0 if ef == 200 and index == 0 else tiny if ef == 200 else 0.5
            )
        )
        values = [1.0, *([tiny] * (OBSERVATION_COUNT - 1))]
        original_fsum = math.fsum
        expected = original_fsum(values) / OBSERVATION_COUNT
        with patch("vdbench.response_profile.math.fsum", wraps=original_fsum) as fsum:
            estimate = compute_response_profile_estimates(evidence)[0]
        self.assertEqual(estimate.mean_recall, expected)
        self.assertEqual(fsum.call_count, len(SUPPORTED_EFS))
        self.assertEqual(tuple(fsum.call_args_list[0].args[0]), tuple(values))

    def test_latency_ties_are_retained_without_jitter(self) -> None:
        estimates = compute_response_profile_estimates(
            _evidence(latency_for=lambda _index, _ef: 7.5)
        )
        for estimate in estimates:
            self.assertEqual(
                (
                    estimate.p95_latency_lcb_ms,
                    estimate.p95_latency_ms,
                    estimate.p95_latency_ucb_ms,
                ),
                (7.5, 7.5, 7.5),
            )


class ResponseProfileInputValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.evidence = _evidence()

    def test_wrong_observation_counts_fail(self) -> None:
        for values in (
            self.evidence.observations[:-1],
            (*self.evidence.observations, self.evidence.observations[-1]),
        ):
            _assert_code(
                self,
                "OBSERVATION_COUNT_INVALID",
                lambda values=values: compute_response_profile_estimates(
                    replace(self.evidence, observations=values)
                ),
            )

    def test_nonfinite_boolean_and_non_float_recall_fail(self) -> None:
        for value in (float("nan"), float("inf"), -float("inf"), True, 1, "0.5"):
            evidence = _replace_ef_result(
                self.evidence, query_index=0, ef_index=0, capped_recall=value
            )
            _assert_code(
                self,
                "OBSERVATION_VALUE_INVALID",
                lambda evidence=evidence: compute_response_profile_estimates(evidence),
            )

    def test_out_of_range_recall_fails(self) -> None:
        for value in (-0.01, 1.01):
            evidence = _replace_ef_result(
                self.evidence, query_index=0, ef_index=0, capped_recall=value
            )
            _assert_code(
                self,
                "OBSERVATION_VALUE_INVALID",
                lambda evidence=evidence: compute_response_profile_estimates(evidence),
            )

    def test_nonfinite_boolean_non_float_and_negative_latency_fail(self) -> None:
        for value in (float("nan"), float("inf"), True, 1, "1.0", -0.01):
            evidence = _replace_ef_result(
                self.evidence, query_index=0, ef_index=0, latency_ms=value
            )
            _assert_code(
                self,
                "OBSERVATION_VALUE_INVALID",
                lambda evidence=evidence: compute_response_profile_estimates(evidence),
            )

    def test_negative_zero_is_canonicalized(self) -> None:
        evidence = _replace_ef_result(
            self.evidence,
            query_index=0,
            ef_index=0,
            capped_recall=-0.0,
            latency_ms=-0.0,
        )
        estimate = compute_response_profile_estimates(evidence)[0]
        self.assertGreaterEqual(estimate.mean_recall, 0.0)

    def test_exact_ef_family_and_order_are_required(self) -> None:
        query = self.evidence.observations[0]
        cases = (
            query.responses[:-1],
            (*query.responses, query.responses[-1]),
            tuple(reversed(query.responses)),
            (
                replace(query.responses[0], ef=201),
                *query.responses[1:],
            ),
        )
        for responses in cases:
            evidence = _replace_observation(
                self.evidence, 0, replace(query, responses=tuple(responses))
            )
            _assert_code(
                self,
                "EF_FAMILY_INVALID",
                lambda evidence=evidence: compute_response_profile_estimates(evidence),
            )

    def test_bool_ef_fails(self) -> None:
        evidence = _replace_ef_result(
            self.evidence, query_index=0, ef_index=0, ef=True
        )
        _assert_code(
            self,
            "EF_FAMILY_INVALID",
            lambda: compute_response_profile_estimates(evidence),
        )

    def test_integer_and_nfc_string_query_ids_are_supported(self) -> None:
        self.assertEqual(len(compute_response_profile_estimates(self.evidence)), 4)
        strings = _evidence(string_ids=True)
        self.assertEqual(len(compute_response_profile_estimates(strings)), 4)

    def test_query_id_bool_empty_and_non_nfc_fail(self) -> None:
        first = self.evidence.observations[0]
        for value in (True, "", "e\u0301"):
            evidence = _replace_observation(
                self.evidence, 0, replace(first, query_id=value)
            )
            _assert_code(
                self,
                "QUERY_ID_INVALID",
                lambda evidence=evidence: compute_response_profile_estimates(evidence),
            )
        self.assertEqual(unicodedata.normalize("NFC", "e\u0301"), "é")

    def test_mixed_integer_and_string_query_ids_are_supported(self) -> None:
        first = self.evidence.observations[0]
        mixed = _replace_observation(
            self.evidence, 0, replace(first, query_id="query-zero")
        )
        self.assertEqual(len(compute_response_profile_estimates(mixed)), 4)

    def test_duplicate_and_cross_type_canonical_query_id_collisions_fail(self) -> None:
        duplicate = _replace_observation(
            self.evidence,
            1,
            replace(self.evidence.observations[1], query_id=0),
        )
        _assert_code(
            self,
            "QUERY_ID_DUPLICATE",
            lambda: compute_response_profile_estimates(duplicate),
        )
        first = replace(self.evidence.observations[0], query_id="1")
        collision = _replace_observation(self.evidence, 0, first)
        _assert_code(
            self,
            "QUERY_ID_DUPLICATE",
            lambda: compute_response_profile_estimates(collision),
        )

    def test_raw_evidence_digest_shape_is_validated_but_authenticity_is_not_claimed(self) -> None:
        arbitrary_valid_digest = "f" * 64
        evidence = replace(self.evidence, raw_evidence_sha256=arbitrary_valid_digest)
        profile = build_calibrated_response_profile(
            identity=_identity(), evidence=evidence
        )
        self.assertEqual(profile.raw_evidence_sha256, arbitrary_valid_digest)
        with self.assertRaises(ResponseProfileContractError):
            build_calibrated_response_profile(
                identity=_identity(),
                evidence=replace(evidence, raw_evidence_sha256="not-a-digest"),
            )

    def test_object_forged_evidence_fails_with_stable_reason(self) -> None:
        forged = object.__new__(ResponseProfileCalibrationEvidence)
        _assert_code(
            self,
            "EVIDENCE_INVALID",
            lambda: compute_response_profile_estimates(forged),
        )


class ResponseProfileIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.evidence = _evidence()

    def test_search_configurations_may_differ_only_by_ef(self) -> None:
        configurations = list(_configurations())
        configurations[1] = replace(configurations[1], radius=0.8)
        _assert_code(
            self,
            "SEARCH_CONFIGURATION_INVALID",
            lambda: build_calibrated_response_profile(
                identity=_identity(search_configurations=tuple(configurations)),
                evidence=self.evidence,
            ),
        )

    def test_wrong_config_count_order_track_metric_or_stratum_fails(self) -> None:
        base = _configurations()
        cases = [
            base[:-1],
            tuple(reversed(base)),
            (
                SearchConfiguration(
                    metric=Metric.L2,
                    threshold_label="target-075",
                    radius=0.75,
                    index_track=IndexTrack.FLAT,
                ),
                *base[1:],
            ),
            (replace(base[0], metric=Metric.COSINE, radius=0.25), *base[1:]),
            (replace(base[0], threshold_label="target-025"), *base[1:]),
        ]
        for configurations in cases:
            with (
                self.subTest(configurations=configurations),
                self.assertRaises(ResponseProfileContractError),
            ):
                build_calibrated_response_profile(
                    identity=_identity(
                        search_configurations=tuple(configurations)
                    ),
                    evidence=self.evidence,
                )

    def test_object_forged_search_configuration_fails_reconstruction(self) -> None:
        forged = object.__new__(SearchConfiguration)
        object.__setattr__(forged, "metric", Metric.L2)
        configurations = (forged, *_configurations()[1:])
        _assert_code(
            self,
            "SEARCH_CONFIGURATION_INVALID",
            lambda: build_calibrated_response_profile(
                identity=_identity(search_configurations=configurations),
                evidence=self.evidence,
            ),
        )

    def test_digest_fields_and_timestamps_fail_closed(self) -> None:
        _assert_code(
            self,
            "IDENTITY_INVALID",
            lambda: build_calibrated_response_profile(
                identity=_identity(workload_manifest_sha256="bad"),
                evidence=self.evidence,
            ),
        )
        for changes in (
            {"calibration_started_at_utc": "2026-13-01T00:00:00Z"},
            {"generated_at_utc": "2026-08-09T00:09:00Z"},
            {"generated_at_utc": "2026-08-09T00:10:01+00:00"},
        ):
            _assert_code(
                self,
                "TIMESTAMP_INVALID",
                lambda changes=changes: build_calibrated_response_profile(
                    identity=_identity(**changes), evidence=self.evidence
                ),
            )

    def test_source_revision_uses_existing_nonempty_text_contract(self) -> None:
        profile = build_calibrated_response_profile(
            identity=_identity(source_revision="release/r1-candidate"),
            evidence=self.evidence,
        )
        self.assertEqual(profile.source_revision, "release/r1-candidate")
        _assert_code(
            self,
            "IDENTITY_INVALID",
            lambda: build_calibrated_response_profile(
                identity=_identity(source_revision=""), evidence=self.evidence
            ),
        )

    def test_object_forged_identity_fails_with_stable_reason(self) -> None:
        forged = object.__new__(ResponseProfileIdentity)
        _assert_code(
            self,
            "IDENTITY_INVALID",
            lambda: build_calibrated_response_profile(
                identity=forged, evidence=self.evidence
            ),
        )


class ResponseProfileCanonicalVerificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.identity = _identity()
        cls.evidence = _evidence()
        cls.profile = build_calibrated_response_profile(
            identity=cls.identity, evidence=cls.evidence
        )
        cls.document = response_profile_document(cls.profile)

    def test_profile_payload_is_exact_and_digest_is_detached(self) -> None:
        payload = response_profile_payload(self.profile)
        self.assertNotIn("profile_sha256", payload)
        expected = hashlib.sha256(
            PROFILE_HASH_DOMAIN + canonical_json_bytes(payload)
        ).hexdigest()
        self.assertEqual(self.profile.profile_sha256, expected)
        self.assertEqual(
            set(self.document), {"profile_payload", "profile_sha256"}
        )
        self.assertEqual(payload["schema_version"], PROFILE_SCHEMA_VERSION)
        self.assertEqual(
            payload["estimator_contract_version"], ESTIMATOR_CONTRACT_VERSION
        )

    def test_frozen_canonical_profile_golden_digests(self) -> None:
        payload = response_profile_payload(self.profile)
        self.assertEqual(self.profile.profile_sha256, GOLDEN_PROFILE_SHA256)
        self.assertEqual(
            hashlib.sha256(canonical_json_bytes(payload)).hexdigest(),
            GOLDEN_PROFILE_PAYLOAD_SHA256,
        )

    def test_same_input_replay_is_byte_identical(self) -> None:
        second = build_calibrated_response_profile(
            identity=self.identity, evidence=self.evidence
        )
        self.assertEqual(self.profile, second)
        self.assertEqual(
            canonical_json_bytes(response_profile_document(self.profile)),
            canonical_json_bytes(response_profile_document(second)),
        )

    def test_document_verification_fully_recomputes(self) -> None:
        verified = verify_response_profile_document(
            document=self.document,
            identity=self.identity,
            evidence=self.evidence,
        )
        self.assertEqual(verified, self.profile)
        self.assertEqual(
            verify_calibrated_response_profile(
                profile=self.profile,
                identity=self.identity,
                evidence=self.evidence,
            ),
            self.profile,
        )

    def test_stored_value_and_bound_tamper_fail(self) -> None:
        fields_to_tamper = (
            "observation_count",
            "mean_recall",
            "recall_lcb",
            "recall_ucb",
            "p95_latency_ms",
            "p95_latency_lcb_ms",
            "p95_latency_ucb_ms",
        )
        for field_name in fields_to_tamper:
            document = deepcopy(self.document)
            estimate = document["profile_payload"]["estimates"][0]
            estimate[field_name] = (
                1199 if field_name == "observation_count" else estimate[field_name] + 0.1
            )
            _assert_code(
                self,
                "PROFILE_RECOMPUTATION_MISMATCH",
                lambda document=document: verify_response_profile_document(
                    document=document,
                    identity=self.identity,
                    evidence=self.evidence,
                ),
            )

    def test_digest_and_payload_tamper_fail(self) -> None:
        document = deepcopy(self.document)
        document["profile_sha256"] = "0" * 64
        _assert_code(
            self,
            "PROFILE_DIGEST_MISMATCH",
            lambda: verify_response_profile_document(
                document=document, identity=self.identity, evidence=self.evidence
            ),
        )
        document = deepcopy(self.document)
        document["profile_payload"]["data_identity"] = "tampered"
        _assert_code(
            self,
            "PROFILE_RECOMPUTATION_MISMATCH",
            lambda: verify_response_profile_document(
                document=document, identity=self.identity, evidence=self.evidence
            ),
        )

    def test_identity_mismatch_fails_even_if_stored_profile_is_self_consistent(self) -> None:
        _assert_code(
            self,
            "PROFILE_RECOMPUTATION_MISMATCH",
            lambda: verify_response_profile_document(
                document=self.document,
                identity=_identity(data_identity="different-dataset"),
                evidence=self.evidence,
            ),
        )

    def test_raw_evidence_digest_mismatch_fails_as_identity_mismatch(self) -> None:
        _assert_code(
            self,
            "PROFILE_RECOMPUTATION_MISMATCH",
            lambda: verify_response_profile_document(
                document=self.document,
                identity=self.identity,
                evidence=replace(self.evidence, raw_evidence_sha256=_digest("a")),
            ),
        )

    def test_unknown_missing_and_self_digest_payload_fields_fail(self) -> None:
        documents = []
        extra_envelope = deepcopy(self.document)
        extra_envelope["unexpected"] = None
        documents.append(extra_envelope)
        missing_payload = deepcopy(self.document)
        del missing_payload["profile_payload"]["metric"]
        documents.append(missing_payload)
        extra_payload = deepcopy(self.document)
        extra_payload["profile_payload"]["unexpected"] = None
        documents.append(extra_payload)
        self_digest = deepcopy(self.document)
        self_digest["profile_payload"]["profile_sha256"] = self_digest[
            "profile_sha256"
        ]
        documents.append(self_digest)
        extra_estimate = deepcopy(self.document)
        extra_estimate["profile_payload"]["estimates"][0]["unexpected"] = None
        documents.append(extra_estimate)
        for document in documents:
            _assert_code(
                self,
                "PROFILE_SCHEMA_INVALID",
                lambda document=document: verify_response_profile_document(
                    document=document,
                    identity=self.identity,
                    evidence=self.evidence,
                ),
            )

    def test_wrong_ef_family_or_order_in_profile_fails(self) -> None:
        for supported in ([200, 400, 800], [1600, 800, 400, 200]):
            document = deepcopy(self.document)
            document["profile_payload"]["supported_efs"] = supported
            _assert_code(
                self,
                "PROFILE_RECOMPUTATION_MISMATCH",
                lambda document=document: verify_response_profile_document(
                    document=document,
                    identity=self.identity,
                    evidence=self.evidence,
                ),
            )

        document = deepcopy(self.document)
        document["profile_payload"]["estimates"][0]["ef"] = 201
        _assert_code(
            self,
            "PROFILE_RECOMPUTATION_MISMATCH",
            lambda: verify_response_profile_document(
                document=document, identity=self.identity, evidence=self.evidence
            ),
        )

    def test_schema_and_estimator_version_tamper_fail(self) -> None:
        for field_name in ("schema_version", "estimator_contract_version"):
            document = deepcopy(self.document)
            document["profile_payload"][field_name] = "unsupported-v999"
            _assert_code(
                self,
                "PROFILE_RECOMPUTATION_MISMATCH",
                lambda document=document: verify_response_profile_document(
                    document=document,
                    identity=self.identity,
                    evidence=self.evidence,
                ),
            )

    def test_unsupported_search_configuration_field_fails(self) -> None:
        document = deepcopy(self.document)
        document["profile_payload"]["search_configurations"][0][
            "unsupported"
        ] = True
        _assert_code(
            self,
            "PROFILE_RECOMPUTATION_MISMATCH",
            lambda: verify_response_profile_document(
                document=document, identity=self.identity, evidence=self.evidence
            ),
        )

    def test_type_sensitive_verification_rejects_bool_for_integer(self) -> None:
        document = deepcopy(self.document)
        document["profile_payload"]["estimates"][0]["observation_count"] = True
        _assert_code(
            self,
            "PROFILE_RECOMPUTATION_MISMATCH",
            lambda: verify_response_profile_document(
                document=document, identity=self.identity, evidence=self.evidence
            ),
        )

    def test_profile_constructor_is_private(self) -> None:
        with self.assertRaises(TypeError):
            CalibratedResponseProfile()

    def test_malformed_and_tampered_object_forgery_cannot_bypass_verification(self) -> None:
        malformed = object.__new__(CalibratedResponseProfile)
        _assert_code(
            self,
            "PROFILE_OBJECT_INVALID",
            lambda: verify_calibrated_response_profile(
                profile=malformed,
                identity=self.identity,
                evidence=self.evidence,
            ),
        )

        tampered = object.__new__(CalibratedResponseProfile)
        for field in fields(CalibratedResponseProfile):
            object.__setattr__(tampered, field.name, getattr(self.profile, field.name))
        object.__setattr__(tampered, "profile_sha256", "0" * 64)
        _assert_code(
            self,
            "PROFILE_DIGEST_MISMATCH",
            lambda: verify_calibrated_response_profile(
                profile=tampered,
                identity=self.identity,
                evidence=self.evidence,
            ),
        )

    def test_exactly_reproduced_object_is_accepted_only_after_semantic_reverification(self) -> None:
        reproduced = object.__new__(CalibratedResponseProfile)
        for field in fields(CalibratedResponseProfile):
            object.__setattr__(reproduced, field.name, getattr(self.profile, field.name))
        verified = verify_calibrated_response_profile(
            profile=reproduced,
            identity=self.identity,
            evidence=self.evidence,
        )
        self.assertEqual(verified, self.profile)

    def test_profile_document_verifier_does_not_claim_raw_json_duplicate_detection(self) -> None:
        # The boundary intentionally accepts an already parsed Mapping. Strict
        # duplicate-key rejection belongs to a future raw-byte codec.
        verified = verify_response_profile_document(
            document=dict(self.document),
            identity=self.identity,
            evidence=self.evidence,
        )
        self.assertEqual(verified.profile_sha256, self.profile.profile_sha256)


if __name__ == "__main__":
    unittest.main()
