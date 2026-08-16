"""TDD coverage for the canonical, domain-separated SearchConfiguration digest."""

from __future__ import annotations

import unittest
from dataclasses import replace

from vdbench.artifacts import canonical_json_bytes
from vdbench.config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from vdbench.search_configuration_digest import (
    SEARCH_CONFIGURATION_DOCUMENT_SCHEMA_VERSION,
    SEARCH_CONFIGURATION_HASH_DOMAIN,
    search_configuration_document,
    search_configuration_from_document,
    search_configuration_sha256,
)

_BASE = SearchConfiguration(
    metric=Metric.L2,
    threshold_label="target-075",
    radius=0.6,
    index_track=IndexTrack.HNSW,
    ef=800,
    limit=100,
    consistency_level="Strong",
)

_COSINE_BASE = SearchConfiguration(
    metric=Metric.COSINE,
    threshold_label="target-025",
    radius=0.2,
    index_track=IndexTrack.HNSW,
    ef=800,
    limit=100,
    consistency_level="Strong",
)


class SearchConfigurationDigestTests(unittest.TestCase):
    def test_document_includes_every_field_and_the_derived_range_filter(self) -> None:
        document = search_configuration_document(_BASE)

        self.assertEqual(
            document,
            {
                "schema_version": SEARCH_CONFIGURATION_DOCUMENT_SCHEMA_VERSION,
                "metric": "L2",
                "threshold_label": "target-075",
                "radius": 0.6,
                "range_filter": 0.0,
                "index_track": "HNSW",
                "ef": 800,
                "limit": 100,
                "consistency_level": "Strong",
            },
        )

    def test_rejects_non_search_configuration(self) -> None:
        with self.assertRaises(TypeError):
            search_configuration_document(object())
        with self.assertRaises(TypeError):
            search_configuration_sha256(object())

    def test_rejects_an_invalid_configuration_before_hashing(self) -> None:
        invalid = replace(_BASE, limit=101)
        with self.assertRaises(ContractViolation):
            search_configuration_sha256(invalid)

    def test_digest_is_deterministic(self) -> None:
        self.assertEqual(search_configuration_sha256(_BASE), search_configuration_sha256(_BASE))

    def test_each_field_independently_changes_the_digest(self) -> None:
        baseline = search_configuration_sha256(_BASE)
        variants = (
            replace(_BASE, radius=3.0),
            replace(_BASE, metric=Metric.COSINE, threshold_label="target-075", radius=0.5),
            replace(_BASE, threshold_label="target-025"),
            replace(_BASE, ef=400),
            replace(_BASE, consistency_level="Strong"),  # unchanged control: same digest expected below
        )
        for variant in variants[:-1]:
            with self.subTest(variant=variant):
                self.assertNotEqual(search_configuration_sha256(variant), baseline)
        self.assertEqual(search_configuration_sha256(variants[-1]), baseline)

    def test_range_filter_cannot_be_independently_supplied(self) -> None:
        with self.assertRaises(TypeError):
            SearchConfiguration(
                metric=Metric.L2,
                threshold_label="target-075",
                radius=0.6,
                range_filter=99.0,
                index_track=IndexTrack.HNSW,
                ef=800,
            )

    # -- root-validated numeric contract: accepted-configuration congruence --
    #
    # The invariant applies only to accepted configurations:
    #   if a.validate() succeeds and b.validate() succeeds and a == b:
    #       search_configuration_document(a) == search_configuration_document(b)
    #       search_configuration_sha256(a) == search_configuration_sha256(b)
    #
    # limit/ef are now root-rejected for any non-int/bool representation, so
    # the only remaining accepted-equal-but-differently-serializable case is
    # radius's -0.0 vs 0.0 (and integer 0 vs 0.0), which only COSINE can
    # express (L2 radius must be strictly greater than 0.0).

    _ACCEPTED_CONGRUENT_PAIRS = (
        ("cosine_radius_negative_zero_vs_positive_zero", "radius", -0.0, 0.0),
        ("cosine_radius_integer_zero_vs_float_zero", "radius", 0, 0.0),
    )

    def test_accepted_configuration_congruence_invariant(self) -> None:
        for name, field, value_a, value_b in self._ACCEPTED_CONGRUENT_PAIRS:
            with self.subTest(case=name):
                a = replace(_COSINE_BASE, **{field: value_a})
                b = replace(_COSINE_BASE, **{field: value_b})
                a.validate()
                b.validate()
                self.assertEqual(a, b)
                self.assertEqual(search_configuration_document(a), search_configuration_document(b))
                self.assertEqual(search_configuration_sha256(a), search_configuration_sha256(b))

    def test_limit_integer_valued_float_is_now_rejected_at_the_root_not_normalized(self) -> None:
        """Formerly normalized by this module; now root-rejected by
        SearchConfiguration.validate() before the serializer ever runs."""
        a = replace(_BASE, limit=100)
        b = replace(_BASE, limit=100.0)
        self.assertEqual(a, b)  # Python equality still holds (100 == 100.0)
        a.validate()  # accepted
        with self.assertRaises(ContractViolation):
            b.validate()  # now rejected at the root
        with self.assertRaises(ContractViolation):
            search_configuration_sha256(b)

    def test_rejected_configurations_cannot_receive_a_digest(self) -> None:
        rejected = (
            replace(_BASE, radius=False),
            replace(_BASE, radius="0.6"),
            replace(_BASE, radius=None),
            replace(_BASE, radius=float("nan")),
            replace(_BASE, limit=100.0),
            replace(_BASE, limit=True),
            replace(_BASE, ef=800.0),
            replace(_BASE, ef=True),
        )
        for cfg in rejected:
            with self.subTest(cfg=cfg):
                with self.assertRaises(ContractViolation):
                    cfg.validate()
                with self.assertRaises(ContractViolation):
                    search_configuration_document(cfg)
                with self.assertRaises(ContractViolation):
                    search_configuration_sha256(cfg)

    def test_repeated_serialization_is_stable(self) -> None:
        first = search_configuration_document(_BASE)
        second = search_configuration_document(_BASE)
        self.assertEqual(first, second)
        self.assertEqual(search_configuration_sha256(_BASE), search_configuration_sha256(_BASE))

    def test_invalid_nan_and_infinities_still_rejected(self) -> None:
        for bad in (float("nan"), float("inf"), float("-inf")):
            with (
                self.subTest(radius=bad),
                self.assertRaises(ContractViolation),
            ):
                search_configuration_sha256(replace(_COSINE_BASE, radius=bad))

    def test_inappropriate_bool_radius_is_rejected(self) -> None:
        for bad in (True, False):
            with (
                self.subTest(radius=bad),
                self.assertRaises(ContractViolation),
            ):
                replace(_COSINE_BASE, radius=bad).validate()

    def test_non_numeric_radius_is_rejected(self) -> None:
        cfg = replace(_COSINE_BASE, radius="0.2")
        with self.assertRaises((ContractViolation, TypeError)):
            search_configuration_sha256(cfg)

    def test_l2_radius_zero_remains_invalid(self) -> None:
        for zero in (0.0, -0.0, 0):
            with self.subTest(radius=zero):
                cfg = replace(_BASE, radius=zero)
                with self.assertRaises(ContractViolation):
                    cfg.validate()
                with self.assertRaises(ContractViolation):
                    search_configuration_sha256(cfg)

    # -- hash domain -----------------------------------------------------

    def test_digest_is_the_domain_prefix_concatenated_with_canonical_bytes(self) -> None:
        expected_bytes = SEARCH_CONFIGURATION_HASH_DOMAIN + canonical_json_bytes(
            search_configuration_document(_BASE)
        )
        import hashlib

        self.assertEqual(search_configuration_sha256(_BASE), hashlib.sha256(expected_bytes).hexdigest())

    def test_domain_is_not_an_ordinary_document_field(self) -> None:
        document = search_configuration_document(_BASE)
        self.assertNotIn("domain", document)
        self.assertNotIn(SEARCH_CONFIGURATION_HASH_DOMAIN, canonical_json_bytes(document))

    def test_changing_the_domain_changes_the_digest(self) -> None:
        import hashlib

        real_digest = search_configuration_sha256(_BASE)
        alternate_domain = b"a-different-domain.v1\0"
        alternate_digest = hashlib.sha256(
            alternate_domain + canonical_json_bytes(search_configuration_document(_BASE))
        ).hexdigest()
        self.assertNotEqual(real_digest, alternate_digest)

    # -- search_configuration_from_document (governed reconstruction) ----

    def test_canonical_round_trip_is_exact(self) -> None:
        for configuration in (_BASE, _COSINE_BASE):
            with self.subTest(configuration=configuration):
                document = search_configuration_document(configuration)
                rebuilt = search_configuration_from_document(document)
                self.assertEqual(rebuilt, configuration)
                self.assertEqual(search_configuration_document(rebuilt), document)

    def test_missing_field_rejected(self) -> None:
        document = dict(search_configuration_document(_BASE))
        del document["ef"]
        with self.assertRaises(ContractViolation):
            search_configuration_from_document(document)

    def test_unknown_field_rejected(self) -> None:
        document = dict(search_configuration_document(_BASE))
        document["unexpected"] = "x"
        with self.assertRaises(ContractViolation):
            search_configuration_from_document(document)

    def test_wrong_schema_version_rejected(self) -> None:
        document = dict(search_configuration_document(_BASE))
        document["schema_version"] = "search-configuration-document-v0"
        with self.assertRaises(ContractViolation):
            search_configuration_from_document(document)

    def test_malformed_metric_enum_rejected(self) -> None:
        document = dict(search_configuration_document(_BASE))
        document["metric"] = "NOT_A_METRIC"
        with self.assertRaises(ContractViolation):
            search_configuration_from_document(document)

    def test_malformed_index_track_enum_rejected(self) -> None:
        document = dict(search_configuration_document(_BASE))
        document["index_track"] = "NOT_A_TRACK"
        with self.assertRaises(ContractViolation):
            search_configuration_from_document(document)

    def test_bool_as_int_rejected_for_limit(self) -> None:
        document = dict(search_configuration_document(_BASE))
        document["limit"] = True
        with self.assertRaises(ContractViolation):
            search_configuration_from_document(document)

    def test_bool_as_int_rejected_for_ef(self) -> None:
        document = dict(search_configuration_document(_BASE))
        document["ef"] = True
        with self.assertRaises(ContractViolation):
            search_configuration_from_document(document)

    def test_nan_radius_rejected(self) -> None:
        document = dict(search_configuration_document(_BASE))
        document["radius"] = float("nan")
        with self.assertRaises(ContractViolation):
            search_configuration_from_document(document)

    def test_infinite_radius_rejected(self) -> None:
        document = dict(search_configuration_document(_BASE))
        document["radius"] = float("inf")
        with self.assertRaises(ContractViolation):
            search_configuration_from_document(document)

    def test_wrong_type_document_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            search_configuration_from_document("not a dict")
        with self.assertRaises(ContractViolation):
            search_configuration_from_document(None)


if __name__ == "__main__":
    unittest.main()
