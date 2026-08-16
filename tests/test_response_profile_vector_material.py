"""Coverage for the supplemental ``response-profile-vector-material-v1``
artifact and the governed run-binding/oracle-manifest loaders built on it.

Every test here uses deterministic in-memory fixtures and temporary values
only. Nothing contacts Milvus, etcd, or MinIO, and nothing produces real
EXP-011 evidence: the artifact is a transport for already-computed calibration
and warm-up vectors, verified but non-authorizing.
"""

from __future__ import annotations

import ast
import base64
import copy
import unittest
from pathlib import Path

from tests.test_exp011_live_acquisition import _digest, _Fixture, _member
from vdbench.config import Metric
from vdbench.response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    WARMUP_QUERY_COUNT,
    ResponseProfileRoleKind,
    build_artifact_source_namespace,
    build_calibration_population_manifest,
    build_response_profile_cell,
    build_response_profile_replay_schedule,
    build_response_profile_role,
    build_response_profile_role_manifest,
)
from vdbench.response_profile_lifecycle import (
    build_response_profile_run_binding,
    response_profile_run_binding_document,
)
from vdbench.response_profile_semantic import (
    build_response_profile_oracle_manifest,
    build_response_profile_oracle_record,
    oracle_manifest_document,
)
from vdbench.response_profile_vector_material import (
    VECTOR_MATERIAL_SCHEMA_VERSION,
    ResponseProfileVectorMaterialError,
    VerifiedResponseProfileVectorMaterial,
    _material_digest,
    load_response_profile_vector_material,
    response_profile_oracle_manifest_from_document,
    response_profile_run_binding_from_document,
    response_profile_vector_material_document,
)

MODULE_PATH = (
    Path(__file__).parents[1] / "src" / "vdbench" / "response_profile_vector_material.py"
)
_CALIBRATION = ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION.value
_WARMUP = ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP.value


def _build_variant_run(*, offset: float, dataset_id: str, run_id: str, source_revision: str):
    """A fully governed but distinct run (different vectors, so a different
    population digest) for cross-run substitution tests."""

    namespace = build_artifact_source_namespace(
        dataset_id=dataset_id, dataset_version="v1", generation_manifest_sha256=_digest("a")
    )
    calibration_members = tuple(
        _member(index, namespace=namespace, offset=offset)
        for index in range(CALIBRATION_QUERY_COUNT)
    )
    calibration_manifest = build_response_profile_role_manifest(
        role=build_response_profile_role(
            kind=ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION
        ),
        members=calibration_members,
    )
    population = build_calibration_population_manifest(
        cell=build_response_profile_cell(metric=Metric.L2, threshold_stratum="target-075"),
        calibration_role_manifest=calibration_manifest,
    )
    schedule = build_response_profile_replay_schedule(
        population=population, source_revision=source_revision
    )
    warmup_members = tuple(
        _member(index + 20_000, namespace=namespace, offset=40_000.0 + offset)
        for index in range(WARMUP_QUERY_COUNT)
    )
    warmup = build_response_profile_role_manifest(
        role=build_response_profile_role(kind=ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP),
        members=warmup_members,
    )
    run_binding = build_response_profile_run_binding(
        run_id=run_id,
        created_at_utc="2026-08-11T00:00:00Z",
        population=population,
        replay_schedule=schedule,
        warmup_role_manifest=warmup,
        source_revision=source_revision,
    )
    records = tuple(
        build_response_profile_oracle_record(
            observation_identity_sha256=member.observation_identity.observation_identity_sha256,
            query_id_sha256=member.query_identity.query_id_sha256,
            query_payload_sha256=member.query_payload_identity.query_payload_sha256,
            limit=member.query_payload_identity.limit,
            full_count=0,
            capped_ids=(),
            capped_distances=(),
            metric=Metric.L2,
            radius=member.query_payload_identity.radius,
            range_filter=member.query_payload_identity.range_filter,
        )
        for member in calibration_members
    )
    oracle = build_response_profile_oracle_manifest(population=population, records=records)
    return run_binding, oracle


def _resign(document: dict) -> dict:
    unsigned = {key: value for key, value in document.items() if key != "vector_material_sha256"}
    document["vector_material_sha256"] = _material_digest(unsigned)
    return document


class ResponseProfileVectorMaterialTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = _Fixture()  # run A
        cls.run_binding_a = cls.fixture.run_binding
        cls.oracle_a = cls.fixture.oracle_manifest
        cls.run_binding_doc_a = response_profile_run_binding_document(cls.run_binding_a)
        cls.oracle_doc_a = oracle_manifest_document(cls.oracle_a)
        cls.material_doc_a = response_profile_vector_material_document(cls.run_binding_a)
        cls.run_binding_b, cls.oracle_b = _build_variant_run(
            offset=1000.0,
            dataset_id="DATASET-EXP011-LIVE-FIXTURE-B",
            run_id="exp011-live-fixture-b",
            source_revision="revision/exp011-live-fixture-b-v1",
        )
        cls.run_binding_doc_b = response_profile_run_binding_document(cls.run_binding_b)
        cls.oracle_doc_b = oracle_manifest_document(cls.oracle_b)

    def _material(self) -> dict:
        return copy.deepcopy(self.material_doc_a)

    # -- build / round trip ---------------------------------------------

    def test_material_document_shape(self) -> None:
        document = self.material_doc_a
        self.assertEqual(document["schema_version"], VECTOR_MATERIAL_SCHEMA_VERSION)
        self.assertEqual(
            len(document["vectors"]), CALIBRATION_QUERY_COUNT + WARMUP_QUERY_COUNT
        )

    def test_canonical_round_trip_is_exact(self) -> None:
        material = load_response_profile_vector_material(self.material_doc_a)
        self.assertIsInstance(material, VerifiedResponseProfileVectorMaterial)
        run_binding = response_profile_run_binding_from_document(
            self.run_binding_doc_a, vector_material=material
        )
        self.assertEqual(run_binding, self.run_binding_a)
        self.assertEqual(
            response_profile_run_binding_document(run_binding), self.run_binding_doc_a
        )
        oracle = response_profile_oracle_manifest_from_document(
            self.oracle_doc_a, vector_material=material
        )
        self.assertEqual(oracle_manifest_document(oracle), self.oracle_doc_a)

    # -- self-digest -----------------------------------------------------

    def test_broken_self_digest_rejected(self) -> None:
        bad = self._material()
        bad["vector_material_sha256"] = "0" * 64
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            load_response_profile_vector_material(bad)
        self.assertEqual(raised.exception.code, "VECTOR_MATERIAL_DIGEST_MISMATCH")

    def test_unknown_top_level_field_rejected(self) -> None:
        bad = self._material()
        bad["extra"] = "x"
        with self.assertRaises(ResponseProfileVectorMaterialError):
            load_response_profile_vector_material(bad)

    def test_wrong_schema_version_rejected(self) -> None:
        bad = self._material()
        bad["schema_version"] = "response-profile-vector-material-v0"
        _resign(bad)
        with self.assertRaises(ResponseProfileVectorMaterialError):
            load_response_profile_vector_material(bad)

    def test_wrong_type_document_rejected(self) -> None:
        with self.assertRaises(ResponseProfileVectorMaterialError):
            load_response_profile_vector_material("not a dict")
        with self.assertRaises(ResponseProfileVectorMaterialError):
            load_response_profile_vector_material(None)

    # -- per-vector integrity -------------------------------------------

    def test_missing_vector_rejected(self) -> None:
        bad = self._material()
        bad["vectors"] = bad["vectors"][:-1]
        _resign(bad)
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            load_response_profile_vector_material(bad)
        self.assertEqual(raised.exception.code, "VECTOR_SET_MISMATCH")

    def test_extra_vector_rejected(self) -> None:
        bad = self._material()
        extra = copy.deepcopy(bad["vectors"][0])
        extra["canonical_order_index"] = CALIBRATION_QUERY_COUNT  # one past the end
        bad["vectors"].append(extra)
        _resign(bad)
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            load_response_profile_vector_material(bad)
        self.assertIn(raised.exception.code, {"VECTOR_SET_MISMATCH", "VECTOR_DUPLICATE"})

    def test_duplicate_vector_rejected(self) -> None:
        bad = self._material()
        bad["vectors"].append(copy.deepcopy(bad["vectors"][0]))
        _resign(bad)
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            load_response_profile_vector_material(bad)
        self.assertEqual(raised.exception.code, "VECTOR_DUPLICATE")

    def test_wrong_order_rejected(self) -> None:
        bad = self._material()
        calibration = [v for v in bad["vectors"] if v["role"] == _CALIBRATION]
        calibration[0]["canonical_order_index"], calibration[1]["canonical_order_index"] = (
            calibration[1]["canonical_order_index"],
            calibration[0]["canonical_order_index"],
        )
        _resign(bad)
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            load_response_profile_vector_material(bad)
        self.assertEqual(raised.exception.code, "VECTOR_DIGEST_MISMATCH")

    def test_invalid_role_rejected(self) -> None:
        bad = self._material()
        bad["vectors"][0]["role"] = "NOT_A_ROLE"
        _resign(bad)
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            load_response_profile_vector_material(bad)
        self.assertEqual(raised.exception.code, "VECTOR_ROLE_INVALID")

    def test_malformed_vector_bytes_rejected(self) -> None:
        bad = self._material()
        bad["vectors"][0]["canonical_vector_bytes_base64"] = "not valid base64 @@@"
        _resign(bad)
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            load_response_profile_vector_material(bad)
        self.assertEqual(raised.exception.code, "VECTOR_BYTES_MALFORMED")

    def test_wrong_dimensions_rejected(self) -> None:
        bad = self._material()
        # Real bytes are 4 (one <f4); claim 2 dimensions so len != dimensions*4.
        bad["vectors"][0]["dimensions"] = 2
        _resign(bad)
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            load_response_profile_vector_material(bad)
        self.assertEqual(raised.exception.code, "VECTOR_DIMENSIONS_INVALID")

    def test_tampered_vector_bytes_rejected(self) -> None:
        bad = self._material()
        # Replace with a different, still-valid one-<f4 vector; its digest no
        # longer matches the member's recorded vector_sha256.
        import numpy as np

        other = np.asarray([987654.0], dtype="<f4").tobytes()
        bad["vectors"][0]["canonical_vector_bytes_base64"] = base64.b64encode(other).decode("ascii")
        _resign(bad)
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            load_response_profile_vector_material(bad)
        self.assertEqual(raised.exception.code, "VECTOR_DIGEST_MISMATCH")

    def test_tampered_embedded_role_manifest_rejected(self) -> None:
        bad = self._material()
        members = bad["calibration_role_manifest_document"]["role_manifest_payload"]["members"]
        members[0]["query_id"] = members[1]["query_id"]
        _resign(bad)
        with self.assertRaises(ResponseProfileVectorMaterialError):
            load_response_profile_vector_material(bad)

    # -- governed loaders: authority stays with the input documents ------

    def test_run_binding_reconstruction_is_byte_exact(self) -> None:
        material = load_response_profile_vector_material(self.material_doc_a)
        run_binding = response_profile_run_binding_from_document(
            self.run_binding_doc_a, vector_material=material
        )
        self.assertEqual(
            response_profile_run_binding_document(run_binding), self.run_binding_doc_a
        )

    def test_oracle_reconstruction_is_byte_exact(self) -> None:
        material = load_response_profile_vector_material(self.material_doc_a)
        oracle = response_profile_oracle_manifest_from_document(
            self.oracle_doc_a, vector_material=material
        )
        self.assertEqual(oracle_manifest_document(oracle), self.oracle_doc_a)

    def test_tampered_run_binding_document_rejected(self) -> None:
        material = load_response_profile_vector_material(self.material_doc_a)
        tampered = copy.deepcopy(self.run_binding_doc_a)
        tampered["run_binding_payload"]["run_id"] = "a-different-run-id"
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            response_profile_run_binding_from_document(tampered, vector_material=material)
        self.assertEqual(raised.exception.code, "RUN_BINDING_DOCUMENT_MISMATCH")

    def test_tampered_oracle_document_rejected(self) -> None:
        material = load_response_profile_vector_material(self.material_doc_a)
        tampered = copy.deepcopy(self.oracle_doc_a)
        tampered["oracle_manifest_sha256"] = "0" * 64
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            response_profile_oracle_manifest_from_document(tampered, vector_material=material)
        self.assertEqual(raised.exception.code, "ORACLE_DOCUMENT_MISMATCH")

    # -- cross-run substitution -----------------------------------------

    def test_cross_run_run_binding_substitution_rejected(self) -> None:
        """Run A's vector material against run B's run-binding document must be
        rejected: the reconstructed run-binding document differs from B's."""

        material_a = load_response_profile_vector_material(self.material_doc_a)
        with self.assertRaises(ResponseProfileVectorMaterialError) as raised:
            response_profile_run_binding_from_document(
                self.run_binding_doc_b, vector_material=material_a
            )
        self.assertEqual(raised.exception.code, "RUN_BINDING_DOCUMENT_MISMATCH")

    def test_cross_run_oracle_substitution_rejected(self) -> None:
        """Run A's vector material against run B's oracle document must be
        rejected: B's records reference a population A did not supply."""

        material_a = load_response_profile_vector_material(self.material_doc_a)
        with self.assertRaises(ResponseProfileVectorMaterialError):
            response_profile_oracle_manifest_from_document(
                self.oracle_doc_b, vector_material=material_a
            )

    def test_variant_run_b_round_trips_with_its_own_material(self) -> None:
        """Sanity: run B is itself fully reconstructable from its own material,
        so the cross-run rejections above are about substitution, not a broken
        run B."""

        material_b = load_response_profile_vector_material(
            response_profile_vector_material_document(self.run_binding_b)
        )
        run_binding = response_profile_run_binding_from_document(
            self.run_binding_doc_b, vector_material=material_b
        )
        self.assertEqual(
            response_profile_run_binding_document(run_binding), self.run_binding_doc_b
        )
        oracle = response_profile_oracle_manifest_from_document(
            self.oracle_doc_b, vector_material=material_b
        )
        self.assertEqual(oracle_manifest_document(oracle), self.oracle_doc_b)

    def test_loaders_require_verified_material_type(self) -> None:
        with self.assertRaises(ResponseProfileVectorMaterialError):
            response_profile_run_binding_from_document(
                self.run_binding_doc_a, vector_material=object()
            )
        with self.assertRaises(ResponseProfileVectorMaterialError):
            response_profile_oracle_manifest_from_document(
                self.oracle_doc_a, vector_material=object()
            )


class ResponseProfileVectorMaterialAdversarialTests(unittest.TestCase):
    def test_module_has_no_network_milvus_or_authority_dependency(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"), filename=str(MODULE_PATH))
        imported = {
            node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        } | {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        forbidden = {
            "socket", "http", "http.client", "urllib", "urllib.request", "requests",
            "pymilvus", "policy", "canary_admission", "canary_approval",
            "canary_activation", "canary_route_authority", "canary_route_state",
            "canary_live_runner", "canary_grant_store", "milvus", "milvus_actuation",
        }
        offending = {
            item
            for item in imported
            if any(item == name or item.endswith(f".{name}") for name in forbidden)
        }
        self.assertFalse(offending, offending)


if __name__ == "__main__":
    unittest.main()
