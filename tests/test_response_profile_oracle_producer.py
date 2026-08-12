"""Focused coverage for the independent response-profile exact-oracle producer.

Every oracle value here is computed independently from a verified corpus by
``vdbench.oracle.exact_range_search``. No acquisition result, measured
response, policy value, or Milvus client is involved, and no tolerance is
applied anywhere. The deliberately empty oracle used by
``tests/test_v2_offline_engine.py`` is structural plumbing only and is never
imported here.
"""

from __future__ import annotations

import ast
from pathlib import Path
import tempfile
import unittest

import numpy as np

from vdbench.artifacts import write_dataset_artifacts
from vdbench.config import DatasetSpec, IndexTrack, Metric, SearchConfiguration
from vdbench.dataset import DatasetBundle, FrozenThreshold
from vdbench.oracle import exact_range_search
from vdbench.response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    ResponseProfileRoleKind,
    build_artifact_source_namespace,
    build_calibration_population_manifest,
    build_canonical_query_identity,
    build_live_stream_source_namespace,
    build_query_vector_identity,
    build_response_profile_cell,
    build_response_profile_query_payload,
    build_response_profile_role,
    build_response_profile_role_manifest,
    build_response_profile_role_member,
)
from vdbench.response_profile_oracle_producer import (
    ResponseProfileOracleProducerError,
    produce_response_profile_oracle,
)

MODULE_PATH = (
    Path(__file__).parents[1] / "src" / "vdbench" / "response_profile_oracle_producer.py"
)

_DIMENSIONS = 4
# Deliberately larger than the governed result limit so the capped branch is
# genuinely exercised rather than assumed.
_BASE_COUNT = 260
_L2_RADIUS = 12.0
_COSINE_RADIUS = -0.20


def _spec() -> DatasetSpec:
    return DatasetSpec(
        dataset_id="DATASET-001",
        version="DATASET-001-v1",
        seed=20260812,
        dimensions=_DIMENSIONS,
        base_count=_BASE_COUNT,
        calibration_query_count=1,
        measured_query_count=1,
        dtype="<f4",
        distribution="independent standard normal",
        generator="numpy.random.Generator(numpy.random.PCG64(seed))",
    )


def _write_corpus(directory: Path) -> Path:
    """Write one small but structurally exact DATASET-001 directory.

    ``write_dataset_artifacts`` is the accepted writer, so the resulting
    manifest and ``SHA256SUMS`` are verifiable by the unchanged
    ``verify_dataset_artifacts`` the producer relies on.
    """

    spec = _spec()
    generator = np.random.Generator(np.random.PCG64(spec.seed))
    base = generator.standard_normal((_BASE_COUNT, _DIMENSIONS)).astype("<f4", copy=False)
    queries = generator.standard_normal((2, _DIMENSIONS)).astype("<f4", copy=False)
    bundle = DatasetBundle(
        ids=np.arange(_BASE_COUNT, dtype=np.int64),
        base_vectors=np.ascontiguousarray(base),
        calibration_queries=np.ascontiguousarray(queries[:1]),
        measured_queries=np.ascontiguousarray(queries[1:]),
        spec=spec,
    )
    thresholds = {
        Metric.L2: (FrozenThreshold("target-075", 75, _L2_RADIUS, 75.0),),
        Metric.COSINE: (FrozenThreshold("target-025", 25, _COSINE_RADIUS, 25.0),),
    }
    target = directory / "dataset001"
    write_dataset_artifacts(target, bundle, thresholds, ())
    return target


def _configuration(metric: Metric, stratum: str, radius: float) -> SearchConfiguration:
    return SearchConfiguration(
        metric=metric,
        threshold_label=stratum,
        radius=radius,
        index_track=IndexTrack.FLAT,
        ef=None,
    )


def _query_vectors(count: int) -> tuple[np.ndarray, ...]:
    generator = np.random.Generator(np.random.PCG64(777))
    return tuple(
        np.ascontiguousarray(
            generator.standard_normal(_DIMENSIONS).astype("<f4", copy=False)
        )
        for _ in range(count)
    )


def _population(
    *,
    namespace,
    metric: Metric = Metric.L2,
    stratum: str = "target-075",
    radius: float = _L2_RADIUS,
    vectors: tuple[np.ndarray, ...] | None = None,
):
    configuration = _configuration(metric, stratum, radius)
    supplied = vectors if vectors is not None else _query_vectors(CALIBRATION_QUERY_COUNT)
    members = []
    for index, vector in enumerate(supplied):
        vector_identity = build_query_vector_identity(vector)
        members.append(
            build_response_profile_role_member(
                source_namespace=namespace,
                query_identity=build_canonical_query_identity(index),
                vector_identity=vector_identity,
                query_payload_identity=build_response_profile_query_payload(
                    vector_identity=vector_identity,
                    search_configuration=configuration,
                ),
            )
        )
    manifest = build_response_profile_role_manifest(
        role=build_response_profile_role(
            kind=ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION
        ),
        members=tuple(members),
    )
    return build_calibration_population_manifest(
        cell=build_response_profile_cell(metric=metric, threshold_stratum=stratum),
        calibration_role_manifest=manifest,
    )


class ResponseProfileOracleProducerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._directory = tempfile.TemporaryDirectory()
        root = Path(cls._directory.name)
        cls.corpus = _write_corpus(root)
        cls.base_vectors = np.load(cls.corpus / "base_vectors.npy", allow_pickle=False)
        cls.base_ids = np.load(cls.corpus / "base_ids.npy", allow_pickle=False)
        from vdbench.artifacts import sha256_file

        cls.data_identity = (
            "DATASET-001-v1:sha256:"
            + sha256_file(cls.corpus / "generation_manifest.json")
        )
        cls.live_namespace = build_live_stream_source_namespace(
            stream_id="served-l2",
            data_identity=cls.data_identity,
            source_workload_manifest_sha256="a" * 64,
        )
        cls.vectors = _query_vectors(CALIBRATION_QUERY_COUNT)
        cls.population = _population(namespace=cls.live_namespace, vectors=cls.vectors)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._directory.cleanup()

    def _produce(self, **changes):
        values = {
            "population": self.population,
            "dataset001_dir": self.corpus,
            "expected_base_data_identity": self.data_identity,
            "expected_metric": Metric.L2,
            "expected_threshold_stratum": "target-075",
            "expected_dimensions": _DIMENSIONS,
        }
        values.update(changes)
        return produce_response_profile_oracle(**values)

    # -- exactness -------------------------------------------------------

    def test_deterministic_exact_l2_oracle_matches_independent_recomputation(self) -> None:
        product = self._produce()
        self.assertEqual(len(product.records), CALIBRATION_QUERY_COUNT)
        members = self.population.calibration_role_manifest.members
        for offset in (0, 1, 599, CALIBRATION_QUERY_COUNT - 1):
            member = members[offset]
            payload = member.query_payload_identity
            expected = exact_range_search(
                self.base_vectors,
                self.base_ids,
                np.frombuffer(member.vector_identity.canonical_vector_bytes, dtype="<f4"),
                Metric.L2,
                radius=payload.radius,
                range_filter=payload.range_filter,
                limit=payload.limit,
            )
            record = product.records[offset]
            self.assertEqual(record.full_count, expected.full_count)
            self.assertEqual(record.capped_ids, tuple(hit.id for hit in expected.hits))
            self.assertEqual(
                record.capped_distances, tuple(hit.score for hit in expected.hits)
            )

    def test_deterministic_exact_cosine_oracle(self) -> None:
        namespace = build_live_stream_source_namespace(
            stream_id="served-cosine",
            data_identity=self.data_identity,
            source_workload_manifest_sha256="b" * 64,
        )
        population = _population(
            namespace=namespace,
            metric=Metric.COSINE,
            stratum="target-025",
            radius=_COSINE_RADIUS,
            vectors=self.vectors,
        )
        product = self._produce(
            population=population,
            expected_metric=Metric.COSINE,
            expected_threshold_stratum="target-025",
        )
        self.assertEqual(len(product.records), CALIBRATION_QUERY_COUNT)
        member = population.calibration_role_manifest.members[7]
        payload = member.query_payload_identity
        expected = exact_range_search(
            self.base_vectors,
            self.base_ids,
            np.frombuffer(member.vector_identity.canonical_vector_bytes, dtype="<f4"),
            Metric.COSINE,
            radius=payload.radius,
            range_filter=payload.range_filter,
            limit=payload.limit,
        )
        self.assertEqual(product.records[7].capped_ids, tuple(h.id for h in expected.hits))
        self.assertEqual(product.records[7].full_count, expected.full_count)

    def test_exact_capped_and_full_count_behaviour(self) -> None:
        product = self._produce()
        limit = self.population.calibration_role_manifest.members[0].query_payload_identity.limit
        saw_capped = False
        for record in product.records:
            self.assertEqual(len(record.capped_ids), min(record.full_count, limit))
            self.assertEqual(len(record.capped_distances), len(record.capped_ids))
            self.assertLessEqual(len(record.capped_ids), limit)
            if record.full_count > limit:
                saw_capped = True
        self.assertTrue(saw_capped, "fixture must exercise the capped branch")

    # -- population and order -------------------------------------------

    def test_exact_1200_record_population_and_manifest_binding(self) -> None:
        product = self._produce()
        self.assertEqual(len(product.records), 1200)
        self.assertEqual(len(product.oracle_manifest.records), 1200)
        self.assertEqual(
            product.oracle_manifest.workload_manifest_sha256,
            self.population.workload_manifest_sha256,
        )
        self.assertEqual(product.base_data_identity, self.data_identity)
        self.assertEqual(product.dimensions, _DIMENSIONS)

    def test_records_follow_canonical_population_order(self) -> None:
        product = self._produce()
        members = self.population.calibration_role_manifest.members
        self.assertEqual(
            tuple(record.query_id_sha256 for record in product.records),
            tuple(member.query_identity.query_id_sha256 for member in members),
        )
        self.assertEqual(
            tuple(record.observation_identity_sha256 for record in product.records),
            tuple(
                member.observation_identity.observation_identity_sha256
                for member in members
            ),
        )

    def test_deterministic_replay_is_logically_identical(self) -> None:
        first = self._produce()
        second = self._produce()
        self.assertEqual(
            first.oracle_manifest.oracle_manifest_sha256,
            second.oracle_manifest.oracle_manifest_sha256,
        )
        self.assertEqual(first.oracle_producer_sha256, second.oracle_producer_sha256)
        self.assertEqual(
            tuple(record.oracle_record_sha256 for record in first.records),
            tuple(record.oracle_record_sha256 for record in second.records),
        )

    # -- fail-closed inputs ---------------------------------------------

    def test_duplicate_member_population_is_rejected(self) -> None:
        duplicated = (self.vectors[0],) + self.vectors[1:-1] + (self.vectors[0],)
        with self.assertRaises(Exception) as raised:
            _population(namespace=self.live_namespace, vectors=duplicated)
        self.assertNotIsInstance(raised.exception, AssertionError)

    def test_missing_member_population_is_rejected(self) -> None:
        with self.assertRaises(Exception) as raised:
            _population(namespace=self.live_namespace, vectors=self.vectors[:-1])
        self.assertNotIsInstance(raised.exception, AssertionError)

    def test_query_vector_substitution_is_rejected(self) -> None:
        """A substituted query vector changes the member's vector digest, and
        the population can no longer be verified as the frozen population."""

        substituted = (self.vectors[1],) + self.vectors[1:]
        with self.assertRaises(Exception) as raised:
            _population(namespace=self.live_namespace, vectors=substituted)
        self.assertNotIsInstance(raised.exception, AssertionError)

    def test_query_payload_substitution_is_rejected(self) -> None:
        """A population frozen under a different radius has different query
        payload digests and must not be answered against this expectation."""

        other = _population(
            namespace=self.live_namespace,
            radius=_L2_RADIUS / 2.0,
            vectors=self.vectors,
        )
        self.assertNotEqual(
            other.ordered_query_payload_sha256,
            self.population.ordered_query_payload_sha256,
        )
        product = self._produce(population=other)
        self.assertNotEqual(
            product.oracle_manifest.oracle_manifest_sha256,
            self._produce().oracle_manifest.oracle_manifest_sha256,
        )

    def test_base_data_identity_mismatch_fails_closed(self) -> None:
        with self.assertRaises(ResponseProfileOracleProducerError) as raised:
            self._produce(expected_base_data_identity="DATASET-001-v1:sha256:" + "0" * 64)
        self.assertEqual(raised.exception.code, "ORACLE_DATA_IDENTITY_MISMATCH")

    def test_member_namespace_not_binding_corpus_fails_closed(self) -> None:
        foreign = build_live_stream_source_namespace(
            stream_id="served-l2",
            data_identity="DATASET-001-v1:sha256:" + "1" * 64,
            source_workload_manifest_sha256="a" * 64,
        )
        population = _population(namespace=foreign, vectors=self.vectors)
        with self.assertRaises(ResponseProfileOracleProducerError) as raised:
            self._produce(population=population)
        self.assertEqual(raised.exception.code, "ORACLE_MEMBER_CORPUS_MISMATCH")

    def test_artifact_namespace_binds_generation_manifest(self) -> None:
        from vdbench.artifacts import sha256_file

        namespace = build_artifact_source_namespace(
            dataset_id="DATASET-001",
            dataset_version="DATASET-001-v1",
            generation_manifest_sha256=sha256_file(
                self.corpus / "generation_manifest.json"
            ),
        )
        population = _population(namespace=namespace, vectors=self.vectors)
        product = self._produce(population=population)
        self.assertEqual(len(product.records), CALIBRATION_QUERY_COUNT)

        wrong = build_artifact_source_namespace(
            dataset_id="DATASET-001",
            dataset_version="DATASET-001-v1",
            generation_manifest_sha256="2" * 64,
        )
        with self.assertRaises(ResponseProfileOracleProducerError) as raised:
            self._produce(population=_population(namespace=wrong, vectors=self.vectors))
        self.assertEqual(raised.exception.code, "ORACLE_MEMBER_CORPUS_MISMATCH")

    def test_dimensions_mismatch_fails_closed(self) -> None:
        with self.assertRaises(ResponseProfileOracleProducerError) as raised:
            self._produce(expected_dimensions=_DIMENSIONS + 1)
        self.assertEqual(raised.exception.code, "ORACLE_DIMENSIONS_MISMATCH")

    def test_metric_and_stratum_mismatch_fails_closed(self) -> None:
        for changes in (
            {"expected_metric": Metric.COSINE, "expected_threshold_stratum": "target-025"},
            {"expected_threshold_stratum": "target-025"},
        ):
            with self.subTest(changes=tuple(changes)):
                with self.assertRaises(ResponseProfileOracleProducerError) as raised:
                    self._produce(**changes)
                self.assertEqual(raised.exception.code, "ORACLE_CELL_MISMATCH")

    def test_unverifiable_corpus_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ResponseProfileOracleProducerError) as raised:
                self._produce(dataset001_dir=Path(directory) / "absent")
            self.assertEqual(raised.exception.code, "ORACLE_CORPUS_UNVERIFIED")

    def test_tampered_corpus_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            corpus = _write_corpus(Path(directory))
            vectors = np.load(corpus / "base_vectors.npy", allow_pickle=False)
            vectors[0][0] = np.float32(float(vectors[0][0]) + 1.0)
            with (corpus / "base_vectors.npy").open("wb") as handle:
                np.save(handle, vectors, allow_pickle=False)
            with self.assertRaises(ResponseProfileOracleProducerError) as raised:
                self._produce(dataset001_dir=corpus)
            self.assertEqual(raised.exception.code, "ORACLE_CORPUS_UNVERIFIED")


class ResponseProfileOracleProducerIndependenceTests(unittest.TestCase):
    def test_producer_has_no_acquisition_policy_or_milvus_dependency(self) -> None:
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
            "response_profile_milvus_adapter",
            "exp011_live_acquisition",
            "exp011_offline_acquisition",
            "policy",
            "actuation",
            "milvus",
            "milvus_actuation",
            "milvus_serving",
            "canary_routing",
            "canary_route_authority",
            "canary_approval",
            "canary_grant_store",
            "canary_admission",
            "canary_live_runner",
            "pymilvus",
        }
        offending = {
            item
            for item in imported
            if any(item == name or item.endswith(f".{name}") for name in forbidden)
        }
        self.assertFalse(offending, offending)

    def test_producer_never_references_a_client_or_search_call(self) -> None:
        source = MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(MODULE_PATH))
        attributes = {
            node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
        }
        self.assertFalse(
            attributes & {"search", "insert", "upsert", "delete", "describe_index"}
        )
        self.assertNotIn("MilvusClient", source)
        self.assertNotIn("START_CANARY", source)

    def test_producer_applies_no_numeric_tolerance(self) -> None:
        """Scan identifiers and keywords, not raw text: the module docstring
        legitimately *names* the forbidden helpers to state that it avoids
        them, so a substring scan would be self-defeating."""

        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"), filename=str(MODULE_PATH))
        identifiers = {
            node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
        } | {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
        keywords = {
            keyword.arg
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            for keyword in node.keywords
        }
        self.assertFalse(identifiers & {"isclose", "allclose", "ulp", "approx"})
        self.assertFalse(keywords & {"atol", "rtol", "tolerance", "epsilon"})


if __name__ == "__main__":
    unittest.main()
