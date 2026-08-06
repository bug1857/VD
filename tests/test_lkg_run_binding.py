"""TDD coverage for the canonical, domain-separated LKG run-identity binding."""

from __future__ import annotations

import unittest

from vdbench.config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from vdbench.lkg_run_binding import (
    LKG_RUN_BINDING_DOCUMENT_SCHEMA_VERSION,
    LKG_RUN_BINDING_HASH_DOMAIN,
    LkgRunBinding,
    lkg_ordered_query_ids_sha256,
    lkg_run_binding_document,
    lkg_run_binding_from_document,
    lkg_run_binding_sha256,
)


def _configuration(**overrides: object) -> SearchConfiguration:
    fields: dict[str, object] = dict(
        metric=Metric.L2,
        threshold_label="target-075",
        radius=5.0,
        index_track=IndexTrack.HNSW,
        ef=400,
    )
    fields.update(overrides)
    return SearchConfiguration(**fields)


def _binding(**overrides: object) -> LkgRunBinding:
    fields: dict[str, object] = dict(
        run_id="run-1",
        producer_identity="producer-v1",
        search_configuration=_configuration(),
        collection_name="lkg_l2_hnsw",
        base_data_identity="data-v1",
        index_identity="index-v1",
        qualification_dataset_id="DATASET-003",
        qualification_dataset_version="DATASET-003-v1",
        qualification_manifest_sha256="a" * 64,
        qualification_query_role="lkg_qualification",
        qualification_query_id_array_sha256="b" * 64,
        qualification_ordered_query_ids_sha256="d" * 64,
        qualification_query_array_sha256="c" * 64,
        qualification_expected_query_count=2_400,
        environment_identity="env-v1",
        source_revision="deadbeef",
    )
    fields.update(overrides)
    return LkgRunBinding(**fields)


class LkgRunBindingTests(unittest.TestCase):
    def test_valid_binding_constructs(self) -> None:
        binding = _binding()
        self.assertEqual(binding.run_id, "run-1")
        self.assertEqual(binding.search_configuration, _configuration())
        self.assertEqual(binding.qualification_expected_query_count, 2_400)

    def test_is_immutable(self) -> None:
        binding = _binding()
        with self.assertRaises(AttributeError):
            binding.run_id = "other"  # type: ignore[misc]
        with self.assertRaises(AttributeError):
            del binding.run_id

    def test_equal_bindings_are_equal_and_hash_equal(self) -> None:
        first = _binding()
        second = _binding()
        self.assertEqual(first, second)
        self.assertEqual(hash(first), hash(second))

    def test_differing_field_breaks_equality(self) -> None:
        first = _binding()
        second = _binding(run_id="run-2")
        self.assertNotEqual(first, second)

    def test_equality_against_foreign_type_is_not_implemented(self) -> None:
        self.assertNotEqual(_binding(), object())

    def test_invalid_search_configuration_type_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(search_configuration=object())

    def test_invalid_search_configuration_value_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(search_configuration=_configuration(ef=999))

    def test_empty_run_id_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(run_id="")

    def test_empty_producer_identity_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(producer_identity="")

    def test_empty_collection_name_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(collection_name="")

    def test_empty_base_data_identity_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(base_data_identity="")

    def test_empty_index_identity_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(index_identity="")

    def test_empty_qualification_dataset_id_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_dataset_id="")

    def test_empty_qualification_dataset_version_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_dataset_version="")

    def test_malformed_manifest_sha256_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_manifest_sha256="not-hex")

    def test_uppercase_manifest_sha256_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_manifest_sha256="A" * 64)

    def test_empty_qualification_query_role_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_query_role="")

    def test_empty_environment_identity_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(environment_identity="")

    def test_empty_source_revision_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(source_revision="")

    # -- blocker 4: complete DATASET-003 workload commitment ---------------------

    def test_malformed_query_id_array_sha256_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_query_id_array_sha256="not-hex")

    def test_uppercase_query_id_array_sha256_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_query_id_array_sha256="B" * 64)

    def test_malformed_ordered_query_ids_sha256_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_ordered_query_ids_sha256="not-hex")

    def test_uppercase_ordered_query_ids_sha256_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_ordered_query_ids_sha256="B" * 64)

    def test_malformed_query_array_sha256_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_query_array_sha256="not-hex")

    def test_zero_expected_query_count_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_expected_query_count=0)

    def test_negative_expected_query_count_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_expected_query_count=-1)

    def test_bool_expected_query_count_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_expected_query_count=True)

    def test_float_expected_query_count_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _binding(qualification_expected_query_count=2400.0)


class LkgRunBindingDigestTests(unittest.TestCase):
    def test_document_includes_every_field(self) -> None:
        binding = _binding()
        document = lkg_run_binding_document(binding)
        self.assertEqual(document["schema_version"], LKG_RUN_BINDING_DOCUMENT_SCHEMA_VERSION)
        self.assertEqual(document["run_id"], "run-1")
        self.assertEqual(document["collection_name"], "lkg_l2_hnsw")
        self.assertEqual(document["qualification_query_id_array_sha256"], "b" * 64)
        self.assertEqual(document["qualification_query_array_sha256"], "c" * 64)
        self.assertEqual(document["qualification_expected_query_count"], 2_400)
        self.assertIn("search_configuration", document)
        self.assertIsInstance(document["search_configuration"], dict)

    def test_rejects_non_binding_input(self) -> None:
        with self.assertRaises(TypeError):
            lkg_run_binding_document(object())
        with self.assertRaises(TypeError):
            lkg_run_binding_sha256(object())

    def test_digest_is_deterministic(self) -> None:
        binding = _binding()
        self.assertEqual(lkg_run_binding_sha256(binding), lkg_run_binding_sha256(binding))

    def test_equal_bindings_produce_equal_digests(self) -> None:
        self.assertEqual(lkg_run_binding_sha256(_binding()), lkg_run_binding_sha256(_binding()))

    def test_each_field_independently_changes_the_digest(self) -> None:
        baseline = lkg_run_binding_sha256(_binding())
        variants = (
            _binding(run_id="run-2"),
            _binding(producer_identity="producer-v2"),
            _binding(search_configuration=_configuration(ef=800)),
            _binding(collection_name="other_collection"),
            _binding(base_data_identity="data-v2"),
            _binding(index_identity="index-v2"),
            _binding(qualification_dataset_version="DATASET-003-v2"),
            _binding(qualification_manifest_sha256="b" * 64),
            _binding(qualification_query_id_array_sha256="f" * 64),
            _binding(qualification_ordered_query_ids_sha256="9" * 64),
            _binding(qualification_query_array_sha256="e" * 64),
            _binding(qualification_expected_query_count=9),
            _binding(environment_identity="env-v2"),
            _binding(source_revision="cafebabe"),
        )
        for variant in variants:
            with self.subTest(variant=variant):
                self.assertNotEqual(lkg_run_binding_sha256(variant), baseline)

    def test_sha256_property_matches_the_module_function(self) -> None:
        binding = _binding()
        self.assertEqual(binding.sha256, lkg_run_binding_sha256(binding))

    def test_digest_is_the_domain_prefix_concatenated_with_canonical_bytes(self) -> None:
        from vdbench.artifacts import canonical_json_bytes
        import hashlib

        binding = _binding()
        expected = hashlib.sha256(
            LKG_RUN_BINDING_HASH_DOMAIN + canonical_json_bytes(lkg_run_binding_document(binding))
        ).hexdigest()
        self.assertEqual(lkg_run_binding_sha256(binding), expected)

    def test_changing_the_domain_changes_the_digest(self) -> None:
        import hashlib

        from vdbench.artifacts import canonical_json_bytes

        binding = _binding()
        real_digest = lkg_run_binding_sha256(binding)
        alternate_domain = b"a-different-domain.v1\0"
        alternate_digest = hashlib.sha256(
            alternate_domain + canonical_json_bytes(lkg_run_binding_document(binding))
        ).hexdigest()
        self.assertNotEqual(real_digest, alternate_digest)


class LkgRunBindingReconstructionTests(unittest.TestCase):
    """Blocker 2: the complete binding must be independently reconstructable
    and verifiable from its stored canonical document alone."""

    def test_round_trip_reconstructs_an_equal_binding(self) -> None:
        binding = _binding()
        document = lkg_run_binding_document(binding)
        reconstructed = lkg_run_binding_from_document(document)
        self.assertEqual(reconstructed, binding)
        self.assertEqual(lkg_run_binding_sha256(reconstructed), binding.sha256)

    def test_round_trip_survives_a_json_string_serialization_hop(self) -> None:
        import json

        binding = _binding()
        document_json = json.dumps(lkg_run_binding_document(binding))
        reconstructed = lkg_run_binding_from_document(json.loads(document_json))
        self.assertEqual(reconstructed, binding)

    def test_non_dict_document_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            lkg_run_binding_from_document("not-a-dict")

    def test_unknown_top_level_field_is_rejected(self) -> None:
        document = lkg_run_binding_document(_binding())
        document["extra_field"] = "unexpected"
        with self.assertRaises(ContractViolation):
            lkg_run_binding_from_document(document)

    def test_missing_top_level_field_is_rejected(self) -> None:
        document = lkg_run_binding_document(_binding())
        del document["source_revision"]
        with self.assertRaises(ContractViolation):
            lkg_run_binding_from_document(document)

    def test_wrong_top_level_schema_version_is_rejected(self) -> None:
        document = lkg_run_binding_document(_binding())
        document["schema_version"] = "lkg-run-binding-document-v1"
        with self.assertRaises(ContractViolation):
            lkg_run_binding_from_document(document)

    def test_unknown_nested_search_configuration_field_is_rejected(self) -> None:
        document = lkg_run_binding_document(_binding())
        document["search_configuration"] = dict(document["search_configuration"])
        document["search_configuration"]["extra"] = "unexpected"
        with self.assertRaises(ContractViolation):
            lkg_run_binding_from_document(document)

    def test_missing_nested_search_configuration_field_is_rejected(self) -> None:
        document = lkg_run_binding_document(_binding())
        document["search_configuration"] = dict(document["search_configuration"])
        del document["search_configuration"]["ef"]
        with self.assertRaises(ContractViolation):
            lkg_run_binding_from_document(document)

    def test_wrong_nested_schema_version_is_rejected(self) -> None:
        document = lkg_run_binding_document(_binding())
        document["search_configuration"] = dict(document["search_configuration"])
        document["search_configuration"]["schema_version"] = "search-configuration-document-v0"
        with self.assertRaises(ContractViolation):
            lkg_run_binding_from_document(document)

    def test_noncanonical_negative_zero_radius_is_rejected(self) -> None:
        """The whole-document canonical byte round-trip catches a
        noncanonical numeric representation a per-field type check alone
        would miss: -0.0 and 0.0 are numerically equal but serialize to
        different JSON bytes."""

        cosine_configuration = _configuration(
            metric=Metric.COSINE, threshold_label="target-025", radius=0.0, ef=400
        )
        binding = _binding(search_configuration=cosine_configuration)
        document = lkg_run_binding_document(binding)
        document["search_configuration"] = dict(document["search_configuration"])
        document["search_configuration"]["radius"] = -0.0
        with self.assertRaises(ContractViolation):
            lkg_run_binding_from_document(document)

    def test_reconstructed_binding_still_enforces_field_validation(self) -> None:
        """A document with a structurally valid but semantically invalid
        field (e.g. an unregistered threshold label) must still fail via
        LkgRunBinding's own constructor validation."""

        document = lkg_run_binding_document(_binding())
        document["qualification_dataset_id"] = ""
        with self.assertRaises(ContractViolation):
            lkg_run_binding_from_document(document)


class LkgOrderedQueryIdsSha256Tests(unittest.TestCase):
    """Blocker 1: the semantic ordered-query-ID digest is a distinct,
    NumPy-independent identity from the raw .npy artifact hash
    (qualification_query_id_array_sha256), which this module never
    reconstructs or reinterprets."""

    def test_same_order_produces_the_same_digest(self) -> None:
        first = lkg_ordered_query_ids_sha256((10, 20, 30))
        second = lkg_ordered_query_ids_sha256((10, 20, 30))
        self.assertEqual(first, second)

    def test_digest_is_independent_of_input_container_type(self) -> None:
        """A list and a tuple of the same values produce the same digest --
        the canonical form is the encoded bytes, not the Python container."""

        self.assertEqual(
            lkg_ordered_query_ids_sha256([10, 20, 30]),
            lkg_ordered_query_ids_sha256((10, 20, 30)),
        )

    def test_changed_order_changes_the_digest(self) -> None:
        self.assertNotEqual(
            lkg_ordered_query_ids_sha256((10, 20, 30)),
            lkg_ordered_query_ids_sha256((30, 20, 10)),
        )

    def test_changed_value_changes_the_digest(self) -> None:
        self.assertNotEqual(
            lkg_ordered_query_ids_sha256((10, 20, 30)),
            lkg_ordered_query_ids_sha256((10, 20, 31)),
        )

    def test_changed_count_changes_the_digest(self) -> None:
        self.assertNotEqual(
            lkg_ordered_query_ids_sha256((10, 20, 30)),
            lkg_ordered_query_ids_sha256((10, 20, 30, 40)),
        )

    def test_single_element_and_empty_are_handled(self) -> None:
        lkg_ordered_query_ids_sha256((10,))  # must not raise
        with self.assertRaises(ContractViolation):
            lkg_ordered_query_ids_sha256(())

    def test_bool_query_id_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            lkg_ordered_query_ids_sha256((10, True, 30))

    def test_non_integer_query_id_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            lkg_ordered_query_ids_sha256((10, "20", 30))
        with self.assertRaises(ContractViolation):
            lkg_ordered_query_ids_sha256((10, 20.0, 30))

    def test_int64_overflow_above_max_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            lkg_ordered_query_ids_sha256((10, 2**63, 30))

    def test_int64_overflow_below_min_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            lkg_ordered_query_ids_sha256((10, -(2**63) - 1, 30))

    def test_int64_boundary_values_are_accepted(self) -> None:
        lkg_ordered_query_ids_sha256((2**63 - 1, -(2**63)))  # must not raise

    def test_non_sequence_input_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            lkg_ordered_query_ids_sha256(10)  # type: ignore[arg-type]
        with self.assertRaises(ContractViolation):
            lkg_ordered_query_ids_sha256({10, 20, 30})  # type: ignore[arg-type]

    def test_digest_does_not_depend_on_numpy_serialization(self) -> None:
        """Directly proves this digest is computed via the explicit binary
        contract (domain + 8-byte LE count + 8-byte LE signed IDs), not by
        reserializing a NumPy array -- independent reimplementation of the
        exact same contract must reproduce it."""

        import hashlib

        from vdbench.lkg_run_binding import ORDERED_QUERY_IDS_DIGEST_DOMAIN

        ids = (10, 20, 30)
        payload = bytearray(ORDERED_QUERY_IDS_DIGEST_DOMAIN)
        payload += len(ids).to_bytes(8, byteorder="little", signed=True)
        for query_id in ids:
            payload += query_id.to_bytes(8, byteorder="little", signed=True)
        expected = hashlib.sha256(bytes(payload)).hexdigest()
        self.assertEqual(lkg_ordered_query_ids_sha256(ids), expected)

    def test_raw_artifact_hash_and_semantic_digest_are_separate_fields(self) -> None:
        """qualification_query_id_array_sha256 (raw .npy artifact hash) and
        qualification_ordered_query_ids_sha256 (semantic ordered-ID digest)
        are independently settable and independently verified -- setting
        one never derives or constrains the other."""

        ids = (10, 20, 30)
        semantic_digest = lkg_ordered_query_ids_sha256(ids)
        binding = _binding(
            qualification_query_id_array_sha256="a" * 64,  # unrelated raw artifact hash
            qualification_ordered_query_ids_sha256=semantic_digest,
        )
        self.assertEqual(binding.qualification_query_id_array_sha256, "a" * 64)
        self.assertEqual(binding.qualification_ordered_query_ids_sha256, semantic_digest)
        self.assertNotEqual(
            binding.qualification_query_id_array_sha256,
            binding.qualification_ordered_query_ids_sha256,
        )


if __name__ == "__main__":
    unittest.main()
