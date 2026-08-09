from __future__ import annotations

from dataclasses import fields
import hashlib
import unittest
from unittest.mock import patch

import numpy as np

from vdbench.artifacts import canonical_json_bytes
from vdbench.config import IndexTrack, Metric, SearchConfiguration, THRESHOLD_LABELS
from vdbench.drift import canonical_serialize_tuple
from vdbench.response_profile import SUPPORTED_EFS
from vdbench.response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    MEASURED_POSITION_COUNT,
    PROSPECTIVE_SEGMENT_COUNT,
    REPLAY_MASTER_SEED,
    SCHEDULE_NUMPY_VERSION,
    SUPPORTED_RESPONSE_PROFILE_CELLS,
    WARMUP_QUERY_COUNT,
    ArtifactSourceNamespace,
    CalibrationPopulationManifest,
    CanonicalQueryIdentity,
    ResponseProfileEvidenceContractError,
    ResponseProfileQueryPayload,
    ResponseProfileReplaySchedule,
    ResponseProfileRoleKind,
    build_artifact_source_namespace,
    build_calibration_population_manifest,
    build_canonical_query_identity,
    build_live_stream_source_namespace,
    build_observation_identity,
    build_query_vector_identity,
    build_response_profile_cell,
    build_response_profile_query_payload,
    build_response_profile_replay_schedule,
    build_response_profile_role,
    build_response_profile_role_manifest,
    build_response_profile_role_member,
    calibration_population_document,
    calibration_population_payload,
    canonical_response_profile_query_id_bytes,
    cell_payload,
    observation_identity_payload,
    ordered_query_payloads_payload,
    query_payload,
    replay_schedule_document,
    replay_schedule_payload,
    role_manifest_document,
    role_manifest_payload,
    role_payload,
    source_namespace_document,
    source_namespace_payload,
    validate_role_manifest_disjointness,
    verify_calibration_population_manifest,
    verify_response_profile_replay_schedule,
    verify_response_profile_role_manifest,
)


GOLDEN_QUERY_ID_SHA256 = "b308b64a0771ef42cdff86166f714442c55922078d2a69b62d257e1cb63bab3b"
GOLDEN_VECTOR_SHA256 = "1228a716655ebe1cc5b75475c851ac1d8ff47fbf784e4cdb90759fb1a9a86c84"
GOLDEN_QUERY_PAYLOAD_SHA256 = "c84aceb151781f1d76c81387a9c2bac5ac7385a0071f4d2ad1eec6f00dd3abff"
GOLDEN_MEMBER_ZERO_QUERY_PAYLOAD_SHA256 = (
    "4a66b4947843027c21d228713355e6f5736cc1fc5c5629adb4b052677e18f612"
)
GOLDEN_ARTIFACT_NAMESPACE_SHA256 = "5a3d1f06b822e48560b32133e4d269d5e9329a0e138d49cc99440b9140fedd08"
GOLDEN_LIVE_NAMESPACE_SHA256 = "167afba0797e096a88db37fdd7c8620cdb7b0b87b2e6615d2c063f5c307daf56"
GOLDEN_OBSERVATION_IDENTITY_SHA256 = "9c4dde6a84b8ab32cb3ed5f38babbb15fe6499fe1dff2e04d3e8b157400b56b2"
GOLDEN_CELL_ID = "8346b405b4e66ea1db098edb3e4d939abd1b3a5151fcd74b5e9cb293e28ad983"
GOLDEN_ROLE_ID = "09c918adb3e22b684927f36adb3a32e4e62f2650ddf649119c5b0f60a567fd91"
GOLDEN_ROLE_MANIFEST_SHA256 = "bc208aded1620c5409c6b8064c47ff17a27c263f09fc6657139913dbae7434a5"
GOLDEN_ORDERED_PAYLOAD_SHA256 = "49178fc2596fe4b540f9cff560c2a107c80968896e3661c2b0e129aca0931bfb"
GOLDEN_POPULATION_SHA256 = "82484b9a9b9cb2fae5c15074419ec114aff5c0cf61bb7b60e87d775b618c5d4d"
GOLDEN_QUERY_ORDER_SEED_SHA256 = "6350c58b4a06a4e318d9fb7ff84ed7025273cbb62202ee705fad6a6134560c65"
GOLDEN_QUERY_ORDER_SEED_U64 = 7_156_437_009_924_793_571
GOLDEN_SCHEDULE_SHA256 = "0fd7feadbb5627ae7d1bbaa5c3b2bdc2a3411853cd7287683884af33b5215825"


def _digest(character: str) -> str:
    return character * 64


def _configuration(
    *,
    metric: Metric = Metric.L2,
    threshold: str = "target-075",
    radius: float = 0.75,
    index_track: IndexTrack = IndexTrack.FLAT,
    ef: int | None = None,
) -> SearchConfiguration:
    return SearchConfiguration(
        metric=metric,
        threshold_label=threshold,
        radius=radius,
        index_track=index_track,
        ef=ef,
    )


def _member(
    index: int,
    *,
    namespace: object,
    query_id: int | str | None = None,
    offset: float = 0.0,
    configuration: SearchConfiguration | None = None,
):
    vector = np.asarray([float(index + 1) + offset], dtype="<f4")
    vector_identity = build_query_vector_identity(vector)
    payload = build_response_profile_query_payload(
        vector_identity=vector_identity,
        search_configuration=configuration or _configuration(),
    )
    return build_response_profile_role_member(
        source_namespace=namespace,
        query_identity=build_canonical_query_identity(
            index if query_id is None else query_id
        ),
        vector_identity=vector_identity,
        query_payload_identity=payload,
    )


def _manifest(
    *,
    role_kind: ResponseProfileRoleKind,
    members: tuple[object, ...],
    prospective_segment_index: int | None = None,
):
    return build_response_profile_role_manifest(
        role=build_response_profile_role(
            kind=role_kind,
            prospective_segment_index=prospective_segment_index,
        ),
        members=members,
    )


def _forge(value: object, **changes: object):
    forged = object.__new__(type(value))
    for field in fields(value):
        object.__setattr__(
            forged,
            field.name,
            changes.get(field.name, getattr(value, field.name)),
        )
    return forged


def _assert_code(case: unittest.TestCase, code: str, operation: object) -> None:
    with case.assertRaises(ResponseProfileEvidenceContractError) as raised:
        operation()  # type: ignore[operator]
    case.assertEqual(raised.exception.code, code)


class ResponseProfileEvidenceFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.artifact_source = build_artifact_source_namespace(
            dataset_id="DATASET-R2",
            dataset_version="v1",
            generation_manifest_sha256=_digest("a"),
        )
        cls.live_source = build_live_stream_source_namespace(
            stream_id="stream-r2",
            data_identity="data-r2",
            source_workload_manifest_sha256=_digest("b"),
        )
        cls.cell = build_response_profile_cell(
            metric=Metric.L2, threshold_stratum="target-075"
        )
        cls.calibration_members = tuple(
            _member(index, namespace=cls.artifact_source)
            for index in range(CALIBRATION_QUERY_COUNT)
        )
        cls.calibration_manifest = _manifest(
            role_kind=ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION,
            members=cls.calibration_members,
        )
        cls.population = build_calibration_population_manifest(
            cell=cls.cell,
            calibration_role_manifest=cls.calibration_manifest,
        )
        cls.schedule = build_response_profile_replay_schedule(
            population=cls.population,
            source_revision="revision/r2-a-golden",
        )


class CanonicalLeafContractTests(ResponseProfileEvidenceFixture):
    def test_contract_constants_are_frozen(self) -> None:
        self.assertEqual(CALIBRATION_QUERY_COUNT, 1200)
        self.assertEqual(MEASURED_POSITION_COUNT, 4800)
        self.assertEqual(WARMUP_QUERY_COUNT, 200)
        self.assertEqual(PROSPECTIVE_SEGMENT_COUNT, 20)
        self.assertEqual(REPLAY_MASTER_SEED, 20260810)
        self.assertEqual(SCHEDULE_NUMPY_VERSION, "2.5.1")
        self.assertEqual(SUPPORTED_EFS, (200, 400, 800, 1600))

    def test_query_id_int_and_string_one_are_the_same_canonical_identity(self) -> None:
        integer = build_canonical_query_identity(1)
        string = build_canonical_query_identity("1")
        self.assertEqual(
            canonical_response_profile_query_id_bytes(1),
            canonical_response_profile_query_id_bytes("1"),
        )
        self.assertEqual(integer.query_id_sha256, string.query_id_sha256)
        self.assertEqual(integer.query_id_sha256, GOLDEN_QUERY_ID_SHA256)

    def test_other_mixed_query_ids_remain_valid(self) -> None:
        self.assertNotEqual(
            build_canonical_query_identity(1).query_id_sha256,
            build_canonical_query_identity("query-1").query_id_sha256,
        )

    def test_invalid_query_ids_fail_closed(self) -> None:
        for value in (True, "", "e\u0301", 1.0, None):
            with self.subTest(value=value):
                _assert_code(
                    self,
                    "QUERY_ID_INVALID",
                    lambda value=value: build_canonical_query_identity(value),
                )

    def test_vector_digest_uses_exact_governed_bytes(self) -> None:
        vector = np.asarray([1.0, -0.0, 2.5], dtype="<f4")
        identity = build_query_vector_identity(vector)
        framed = canonical_serialize_tuple(("dtype", "<f4", "dimensions", 3))
        expected = hashlib.sha256(
            b"VD::RESPONSE_PROFILE_QUERY_VECTOR::V1\x00"
            + framed
            + vector.tobytes(order="C")
        ).hexdigest()
        self.assertEqual(identity.dimensions, 3)
        self.assertEqual(identity.vector_sha256, expected)
        self.assertEqual(identity.vector_sha256, GOLDEN_VECTOR_SHA256)

    def test_noncanonical_vectors_fail_closed(self) -> None:
        cases = (
            np.asarray([], dtype="<f4"),
            np.asarray([[1.0]], dtype="<f4"),
            np.asarray([1.0], dtype="<f8"),
            np.asarray([1.0], dtype=">f4"),
            np.asarray([1.0, 2.0, 3.0, 4.0], dtype="<f4")[::2],
            np.asarray([np.nan], dtype="<f4"),
            np.asarray([np.inf], dtype="<f4"),
        )
        for value in cases:
            with self.subTest(dtype=value.dtype, shape=value.shape):
                _assert_code(
                    self,
                    "VECTOR_INVALID",
                    lambda value=value: build_query_vector_identity(value),
                )
        _assert_code(
            self,
            "VECTOR_INVALID",
            lambda: build_query_vector_identity([1.0]),  # type: ignore[arg-type]
        )

    def test_generic_query_payload_accepts_flat_and_hnsw_common_semantics(self) -> None:
        vector = build_query_vector_identity(np.asarray([3.0], dtype="<f4"))
        flat = build_response_profile_query_payload(
            vector_identity=vector,
            search_configuration=_configuration(index_track=IndexTrack.FLAT),
        )
        hnsw = build_response_profile_query_payload(
            vector_identity=vector,
            search_configuration=_configuration(index_track=IndexTrack.HNSW, ef=400),
        )
        self.assertEqual(flat, hnsw)
        document = query_payload(flat)
        self.assertNotIn("index_track", document)
        self.assertNotIn("ef", document)
        self.assertEqual(set(document), {
            "schema_version", "vector_sha256", "metric", "threshold_stratum",
            "radius", "range_filter", "limit", "consistency_level",
        })
        self.assertEqual(flat.query_payload_sha256, GOLDEN_QUERY_PAYLOAD_SHA256)

    def test_forged_query_payload_fails_recomputation(self) -> None:
        payload = self.calibration_members[0].query_payload_identity
        forged = _forge(payload, query_payload_sha256=_digest("f"))
        _assert_code(self, "QUERY_PAYLOAD_INVALID", lambda: query_payload(forged))

    def test_forged_vector_digest_cannot_bypass_recomputation(self) -> None:
        member = self.calibration_members[0]
        forged_vector = _forge(member.vector_identity, vector_sha256=_digest("f"))
        _assert_code(
            self,
            "VECTOR_IDENTITY_INVALID",
            lambda: build_response_profile_role_member(
                source_namespace=member.source_namespace,
                query_identity=member.query_identity,
                vector_identity=forged_vector,
                query_payload_identity=member.query_payload_identity,
            ),
        )

    def test_source_namespace_payloads_are_exact_and_golden(self) -> None:
        artifact_payload = source_namespace_payload(self.artifact_source)
        live_payload = source_namespace_payload(self.live_source)
        self.assertEqual(set(artifact_payload), {
            "schema_version", "source_kind", "dataset_id", "dataset_version",
            "generation_manifest_sha256",
        })
        self.assertEqual(set(live_payload), {
            "schema_version", "source_kind", "stream_id", "data_identity",
            "source_workload_manifest_sha256",
        })
        self.assertEqual(
            self.artifact_source.source_namespace_sha256,
            GOLDEN_ARTIFACT_NAMESPACE_SHA256,
        )
        self.assertEqual(
            self.live_source.source_namespace_sha256,
            GOLDEN_LIVE_NAMESPACE_SHA256,
        )

    def test_observation_identity_binds_namespace_and_local_query_id(self) -> None:
        query = build_canonical_query_identity(7)
        artifact = build_observation_identity(
            source_namespace=self.artifact_source, query_identity=query
        )
        live = build_observation_identity(
            source_namespace=self.live_source, query_identity=query
        )
        self.assertEqual(artifact.query_id_sha256, live.query_id_sha256)
        self.assertNotEqual(
            artifact.observation_identity_sha256,
            live.observation_identity_sha256,
        )
        self.assertEqual(
            artifact.observation_identity_sha256,
            GOLDEN_OBSERVATION_IDENTITY_SHA256,
        )
        self.assertEqual(
            set(observation_identity_payload(artifact)),
            {"schema_version", "source_namespace_sha256", "query_id_sha256"},
        )

    def test_source_documents_keep_detached_digest_outside_payload(self) -> None:
        document = source_namespace_document(self.artifact_source)
        self.assertEqual(
            set(document),
            {"source_namespace_payload", "source_namespace_sha256"},
        )
        self.assertNotIn(
            "source_namespace_sha256", document["source_namespace_payload"]
        )

    def test_plain_string_cannot_replace_source_kind_enum(self) -> None:
        forged = _forge(self.artifact_source, source_kind="ARTIFACT")
        _assert_code(
            self,
            "SOURCE_NAMESPACE_INVALID",
            lambda: source_namespace_payload(forged),
        )


class RoleAndPopulationContractTests(ResponseProfileEvidenceFixture):
    def test_exact_exp010_v1_predictive_cell_family(self) -> None:
        expected = {
            (Metric.L2, "target-075"),
            (Metric.COSINE, "target-025"),
        }
        self.assertEqual(set(SUPPORTED_RESPONSE_PROFILE_CELLS), expected)
        for metric in Metric:
            for threshold_stratum in THRESHOLD_LABELS:
                with self.subTest(metric=metric, threshold_stratum=threshold_stratum):
                    if (metric, threshold_stratum) in expected:
                        cell = build_response_profile_cell(
                            metric=metric,
                            threshold_stratum=threshold_stratum,
                        )
                        self.assertEqual(cell.metric, metric)
                        self.assertEqual(cell.threshold_stratum, threshold_stratum)
                    else:
                        _assert_code(
                            self,
                            "CELL_UNSUPPORTED",
                            lambda metric=metric, threshold_stratum=threshold_stratum: (
                                build_response_profile_cell(
                                    metric=metric,
                                    threshold_stratum=threshold_stratum,
                                )
                            ),
                        )

    def test_generic_query_payload_remains_available_outside_predictive_cells(self) -> None:
        vector = build_query_vector_identity(np.asarray([17.0], dtype="<f4"))
        for configuration in (
            _configuration(metric=Metric.L2, threshold="target-025", radius=0.25),
            _configuration(
                metric=Metric.COSINE,
                threshold="target-075",
                radius=0.75,
            ),
        ):
            with self.subTest(configuration=configuration):
                payload = build_response_profile_query_payload(
                    vector_identity=vector,
                    search_configuration=configuration,
                )
                self.assertEqual(payload.metric, configuration.metric)
                self.assertEqual(
                    payload.threshold_stratum,
                    configuration.threshold_label,
                )

    def test_cell_and_role_digests_use_exact_governed_payloads(self) -> None:
        role = self.calibration_manifest.role
        self.assertEqual(
            cell_payload(self.cell),
            {
                "schema_version": "response-profile-cell-v1",
                "metric": "L2",
                "threshold_stratum": "target-075",
            },
        )
        self.assertEqual(
            role_payload(role),
            {
                "schema_version": "response-profile-role-v1",
                "kind": "RESPONSE_PROFILE_CALIBRATION",
                "prospective_segment_index": None,
            },
        )
        self.assertEqual(self.cell.cell_id, GOLDEN_CELL_ID)
        self.assertEqual(role.role_or_segment_id, GOLDEN_ROLE_ID)

    def test_closed_role_and_segment_validation(self) -> None:
        prospective = build_response_profile_role(
            kind=ResponseProfileRoleKind.RESPONSE_PROFILE_PROSPECTIVE_VALIDATION,
            prospective_segment_index=19,
        )
        self.assertEqual(prospective.prospective_segment_index, 19)
        for index in (-1, 20, None, True):
            with self.subTest(index=index):
                _assert_code(
                    self,
                    "ROLE_INVALID",
                    lambda index=index: build_response_profile_role(
                        kind=ResponseProfileRoleKind.RESPONSE_PROFILE_PROSPECTIVE_VALIDATION,
                        prospective_segment_index=index,  # type: ignore[arg-type]
                    ),
                )
        _assert_code(
            self,
            "ROLE_INVALID",
            lambda: build_response_profile_role(
                kind=ResponseProfileRoleKind.DETECTOR_EVIDENCE,
                prospective_segment_index=0,
            ),
        )

    def test_generic_unrelated_role_does_not_invent_1200_members(self) -> None:
        manifest = _manifest(
            role_kind=ResponseProfileRoleKind.DETECTOR_EVIDENCE,
            members=(_member(5000, namespace=self.artifact_source),),
        )
        self.assertEqual(len(manifest.members), 1)

    def test_role_specific_cardinalities_fail_closed(self) -> None:
        one = (_member(6000, namespace=self.artifact_source),)
        for kind, index in (
            (ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP, None),
            (ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION, None),
            (ResponseProfileRoleKind.RESPONSE_PROFILE_PROSPECTIVE_VALIDATION, 0),
        ):
            with self.subTest(kind=kind):
                _assert_code(
                    self,
                    "ROLE_MEMBER_COUNT_INVALID",
                    lambda kind=kind, index=index: _manifest(
                        role_kind=kind,
                        prospective_segment_index=index,
                        members=one,
                    ),
                )

    def test_warmup_manifest_is_exactly_200_and_reusable_without_new_membership(self) -> None:
        members = tuple(
            _member(index, namespace=self.live_source, offset=10_000.0)
            for index in range(WARMUP_QUERY_COUNT)
        )
        first = _manifest(
            role_kind=ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP,
            members=members,
        )
        rebuilt = _manifest(
            role_kind=ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP,
            members=members,
        )
        self.assertEqual(first, rebuilt)
        self.assertEqual(first.role_manifest_sha256, rebuilt.role_manifest_sha256)
        self.assertEqual(len(first.members), 200)

    def test_role_manifest_is_strict_and_reconstructive(self) -> None:
        rebuilt = verify_response_profile_role_manifest(self.calibration_manifest)
        self.assertEqual(rebuilt, self.calibration_manifest)
        document = role_manifest_document(rebuilt)
        self.assertEqual(
            set(document), {"role_manifest_payload", "role_manifest_sha256"}
        )
        self.assertNotIn("role_manifest_sha256", document["role_manifest_payload"])
        self.assertEqual(
            rebuilt.role_manifest_sha256, GOLDEN_ROLE_MANIFEST_SHA256
        )
        forged = _forge(rebuilt, role_manifest_sha256=_digest("e"))
        _assert_code(
            self,
            "ROLE_MANIFEST_INVALID",
            lambda: verify_response_profile_role_manifest(forged),
        )

    def test_role_manifest_rejects_duplicate_vectors_and_payloads(self) -> None:
        member = _member(9000, namespace=self.artifact_source)
        duplicate = build_response_profile_role_member(
            source_namespace=self.live_source,
            query_identity=build_canonical_query_identity("different-query"),
            vector_identity=member.vector_identity,
            query_payload_identity=member.query_payload_identity,
        )
        _assert_code(
            self,
            "ROLE_VECTOR_DUPLICATE",
            lambda: _manifest(
                role_kind=ResponseProfileRoleKind.DETECTOR_EVIDENCE,
                members=(member, duplicate),
            ),
        )

    def test_disjointness_uses_source_local_query_identity(self) -> None:
        left = _manifest(
            role_kind=ResponseProfileRoleKind.DETECTOR_EVIDENCE,
            members=(
                _member(
                    11,
                    namespace=self.artifact_source,
                    query_id="same-local-id",
                    offset=20_000.0,
                ),
            ),
        )
        right_cross_source = _manifest(
            role_kind=ResponseProfileRoleKind.PHASE3_QUALIFICATION,
            members=(
                _member(
                    12,
                    namespace=self.live_source,
                    query_id="same-local-id",
                    offset=30_000.0,
                ),
            ),
        )
        validate_role_manifest_disjointness((left, right_cross_source))
        right_same_source = _manifest(
            role_kind=ResponseProfileRoleKind.STAGE4_ROUTING,
            members=(
                _member(
                    13,
                    namespace=self.artifact_source,
                    query_id="same-local-id",
                    offset=40_000.0,
                ),
            ),
        )
        _assert_code(
            self,
            "ROLE_QUERY_OVERLAP",
            lambda: validate_role_manifest_disjointness((left, right_same_source)),
        )

    def test_cross_role_vector_and_payload_overlap_remain_global(self) -> None:
        member = _member(15, namespace=self.artifact_source, offset=50_000.0)
        left = _manifest(
            role_kind=ResponseProfileRoleKind.DETECTOR_EVIDENCE,
            members=(member,),
        )
        duplicate = build_response_profile_role_member(
            source_namespace=self.live_source,
            query_identity=build_canonical_query_identity("new-id"),
            vector_identity=member.vector_identity,
            query_payload_identity=member.query_payload_identity,
        )
        right = _manifest(
            role_kind=ResponseProfileRoleKind.PHASE3_QUALIFICATION,
            members=(duplicate,),
        )
        _assert_code(
            self,
            "ROLE_VECTOR_OVERLAP",
            lambda: validate_role_manifest_disjointness((left, right)),
        )

    def test_calibration_population_binds_r1_workload_and_ordered_payload_digests(self) -> None:
        population = verify_calibration_population_manifest(self.population)
        document = calibration_population_document(population)
        ordered = ordered_query_payloads_payload(self.calibration_manifest)
        self.assertEqual(len(ordered["query_payload_sha256"]), 1200)
        self.assertEqual(
            population.ordered_query_payload_sha256,
            GOLDEN_ORDERED_PAYLOAD_SHA256,
        )
        self.assertEqual(population.workload_manifest_sha256, GOLDEN_POPULATION_SHA256)
        self.assertEqual(
            document["workload_manifest_sha256"],
            population.workload_manifest_sha256,
        )
        self.assertNotIn(
            "workload_manifest_sha256",
            document["calibration_population_payload"],
        )

    def test_calibration_population_rejects_cell_mismatch(self) -> None:
        cosine_members = tuple(
            _member(
                index,
                namespace=self.live_source,
                offset=60_000.0,
                configuration=_configuration(
                    metric=Metric.COSINE,
                    threshold="target-025",
                    radius=0.25,
                ),
            )
            for index in range(CALIBRATION_QUERY_COUNT)
        )
        manifest = _manifest(
            role_kind=ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION,
            members=cosine_members,
        )
        _assert_code(
            self,
            "CALIBRATION_CELL_MISMATCH",
            lambda: build_calibration_population_manifest(
                cell=self.cell, calibration_role_manifest=manifest
            ),
        )

    def test_calibration_rejects_int_string_collision_across_namespaces(self) -> None:
        members = list(self.calibration_members)
        members[0] = _member(
            70_000,
            namespace=self.artifact_source,
            query_id=1,
            offset=70_000.0,
        )
        members[1] = _member(
            70_001,
            namespace=self.live_source,
            query_id="1",
            offset=80_000.0,
        )
        manifest = _manifest(
            role_kind=ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION,
            members=tuple(members),
        )
        _assert_code(
            self,
            "CALIBRATION_QUERY_ID_DUPLICATE",
            lambda: build_calibration_population_manifest(
                cell=self.cell, calibration_role_manifest=manifest
            ),
        )

    def test_forged_population_fails_recomputation(self) -> None:
        forged = _forge(self.population, workload_manifest_sha256=_digest("d"))
        _assert_code(
            self,
            "CALIBRATION_POPULATION_INVALID",
            lambda: verify_calibration_population_manifest(forged),
        )


class ReplayScheduleContractTests(ResponseProfileEvidenceFixture):
    def test_schedule_has_exact_block_and_position_shape(self) -> None:
        schedule = self.schedule
        self.assertEqual(len(schedule.blocks), 1200)
        self.assertEqual(sum(len(block.positions) for block in schedule.blocks), 4800)
        self.assertEqual(
            {position.ef for position in schedule.blocks[0].positions},
            set(SUPPORTED_EFS),
        )
        self.assertEqual(
            tuple(position.position_index for position in schedule.blocks[-1].positions),
            (4796, 4797, 4798, 4799),
        )

    def test_schedule_seed_formula_is_independently_reproduced(self) -> None:
        values = (
            REPLAY_MASTER_SEED,
            self.population.cell.cell_id,
            self.population.calibration_role_manifest.role.role_or_segment_id,
            self.population.workload_manifest_sha256,
            "revision/r2-a-golden",
            "QUERY_ORDER",
        )
        digest = hashlib.sha256(
            b"VD::RESPONSE_PROFILE_SCHEDULE_SEED::V1\x00"
            + canonical_serialize_tuple(values)
        ).digest()
        self.assertEqual(self.schedule.query_order_seed.seed_tuple, values)
        self.assertEqual(self.schedule.query_order_seed.seed_sha256, digest.hex())
        self.assertEqual(
            self.schedule.query_order_seed.seed_u64,
            int.from_bytes(digest[:8], "big", signed=False),
        )
        self.assertEqual(
            self.schedule.query_order_seed.seed_sha256,
            GOLDEN_QUERY_ORDER_SEED_SHA256,
        )
        self.assertEqual(
            self.schedule.query_order_seed.seed_u64,
            GOLDEN_QUERY_ORDER_SEED_U64,
        )

    def test_schedule_is_deterministic_and_fully_reconstructive(self) -> None:
        rebuilt = build_response_profile_replay_schedule(
            population=self.population,
            source_revision="revision/r2-a-golden",
        )
        verified = verify_response_profile_replay_schedule(
            self.schedule,
            population=self.population,
            source_revision="revision/r2-a-golden",
        )
        self.assertEqual(rebuilt, self.schedule)
        self.assertEqual(verified, self.schedule)
        self.assertEqual(
            canonical_json_bytes(replay_schedule_payload(rebuilt)),
            canonical_json_bytes(replay_schedule_payload(self.schedule)),
        )
        self.assertEqual(self.schedule.replay_schedule_sha256, GOLDEN_SCHEDULE_SHA256)

    def test_schedule_document_has_detached_digest(self) -> None:
        document = replay_schedule_document(self.schedule)
        self.assertEqual(
            set(document), {"replay_schedule_payload", "replay_schedule_sha256"}
        )
        self.assertNotIn(
            "replay_schedule_sha256", document["replay_schedule_payload"]
        )
        self.assertEqual(document["replay_schedule_payload"]["position_count"], 4800)

    def test_source_revision_changes_schedule(self) -> None:
        changed = build_response_profile_replay_schedule(
            population=self.population,
            source_revision="revision/r2-a-other",
        )
        self.assertNotEqual(
            changed.query_order_seed.seed_sha256,
            self.schedule.query_order_seed.seed_sha256,
        )
        self.assertNotEqual(
            changed.replay_schedule_sha256,
            self.schedule.replay_schedule_sha256,
        )

    def test_wrong_numpy_version_fails_closed_before_schedule_generation(self) -> None:
        with patch("vdbench.response_profile_evidence.np.__version__", "2.6.0"):
            _assert_code(
                self,
                "NUMPY_VERSION_UNSUPPORTED",
                lambda: build_response_profile_replay_schedule(
                    population=self.population,
                    source_revision="revision/r2-a-golden",
                ),
            )

    def test_forged_schedule_and_position_fail_recomputation(self) -> None:
        first_block = self.schedule.blocks[0]
        forged_position = _forge(first_block.positions[0], ef=999)
        forged_block = _forge(
            first_block,
            positions=(forged_position, *first_block.positions[1:]),
        )
        forged_schedule = _forge(
            self.schedule,
            blocks=(forged_block, *self.schedule.blocks[1:]),
        )
        _assert_code(
            self,
            "REPLAY_SCHEDULE_INVALID",
            lambda: verify_response_profile_replay_schedule(
                forged_schedule,
                population=self.population,
                source_revision="revision/r2-a-golden",
            ),
        )

    def test_bool_cannot_replace_integer_zero_in_replay_position(self) -> None:
        first_block = self.schedule.blocks[0]
        forged_position = _forge(first_block.positions[0], position_index=False)
        forged_block = _forge(
            first_block,
            positions=(forged_position, *first_block.positions[1:]),
        )
        forged_schedule = _forge(
            self.schedule,
            blocks=(forged_block, *self.schedule.blocks[1:]),
        )
        _assert_code(
            self,
            "REPLAY_SCHEDULE_INVALID",
            lambda: verify_response_profile_replay_schedule(
                forged_schedule,
                population=self.population,
                source_revision="revision/r2-a-golden",
            ),
        )

    def test_float_cannot_replace_equal_integer_in_replay_position(self) -> None:
        first_block = self.schedule.blocks[0]
        first_position = first_block.positions[0]
        forged_position = _forge(first_position, ef=float(first_position.ef))
        forged_block = _forge(
            first_block,
            positions=(forged_position, *first_block.positions[1:]),
        )
        forged_schedule = _forge(
            self.schedule,
            blocks=(forged_block, *self.schedule.blocks[1:]),
        )
        _assert_code(
            self,
            "REPLAY_SCHEDULE_INVALID",
            lambda: verify_response_profile_replay_schedule(
                forged_schedule,
                population=self.population,
                source_revision="revision/r2-a-golden",
            ),
        )

    def test_object_forged_schedule_with_missing_slots_fails_closed(self) -> None:
        malformed = object.__new__(ResponseProfileReplaySchedule)
        _assert_code(
            self,
            "REPLAY_SCHEDULE_INVALID",
            lambda: verify_response_profile_replay_schedule(
                malformed,
                population=self.population,
                source_revision="revision/r2-a-golden",
            ),
        )

    def test_schedule_binds_population_exactly(self) -> None:
        forged_population = _forge(
            self.population, workload_manifest_sha256=_digest("c")
        )
        _assert_code(
            self,
            "CALIBRATION_POPULATION_INVALID",
            lambda: verify_response_profile_replay_schedule(
                self.schedule,
                population=forged_population,
                source_revision="revision/r2-a-golden",
            ),
        )

    def test_contract_types_and_payloads_contain_no_result_or_authority_fields(self) -> None:
        forbidden = {
            "recall", "latency", "result", "started", "completed", "epoch",
            "retry", "root_pin", "policy", "admission", "route", "execution",
            "actuation", "milvus", "freshness",
        }
        type_names = (
            CanonicalQueryIdentity,
            ResponseProfileQueryPayload,
            ArtifactSourceNamespace,
            CalibrationPopulationManifest,
            ResponseProfileReplaySchedule,
        )
        for type_ in type_names:
            names = {field.name.lower() for field in fields(type_)}
            for token in forbidden:
                self.assertFalse(
                    any(token in name for name in names),
                    f"{type_.__name__} unexpectedly contains {token}",
                )
        serialized = canonical_json_bytes(
            {
                "role": role_manifest_payload(self.calibration_manifest),
                "population": calibration_population_payload(self.population),
                "schedule": replay_schedule_payload(self.schedule),
            }
        ).decode("utf-8")
        for token in forbidden:
            self.assertNotIn(f'"{token}"', serialized)


class GoldenProtocolFixtureTests(ResponseProfileEvidenceFixture):
    def test_literal_protocol_digest_fixture(self) -> None:
        self.maxDiff = None
        actual = (
            build_canonical_query_identity(1).query_id_sha256,
            build_query_vector_identity(
                np.asarray([1.0, -0.0, 2.5], dtype="<f4")
            ).vector_sha256,
            self.calibration_members[0].query_payload_identity.query_payload_sha256,
            self.artifact_source.source_namespace_sha256,
            self.live_source.source_namespace_sha256,
            build_observation_identity(
                source_namespace=self.artifact_source,
                query_identity=build_canonical_query_identity(7),
            ).observation_identity_sha256,
            self.cell.cell_id,
            self.calibration_manifest.role.role_or_segment_id,
            self.calibration_manifest.role_manifest_sha256,
            self.population.ordered_query_payload_sha256,
            self.population.workload_manifest_sha256,
            self.schedule.query_order_seed.seed_sha256,
            self.schedule.query_order_seed.seed_u64,
            self.schedule.replay_schedule_sha256,
        )
        expected = (
            GOLDEN_QUERY_ID_SHA256,
            GOLDEN_VECTOR_SHA256,
            GOLDEN_MEMBER_ZERO_QUERY_PAYLOAD_SHA256,
            GOLDEN_ARTIFACT_NAMESPACE_SHA256,
            GOLDEN_LIVE_NAMESPACE_SHA256,
            GOLDEN_OBSERVATION_IDENTITY_SHA256,
            GOLDEN_CELL_ID,
            GOLDEN_ROLE_ID,
            GOLDEN_ROLE_MANIFEST_SHA256,
            GOLDEN_ORDERED_PAYLOAD_SHA256,
            GOLDEN_POPULATION_SHA256,
            GOLDEN_QUERY_ORDER_SEED_SHA256,
            GOLDEN_QUERY_ORDER_SEED_U64,
            GOLDEN_SCHEDULE_SHA256,
        )
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
