"""ADR-014 coverage for the production-capable v2 Milvus shadow adapter.

Every test injects a fake client. No PyMilvus client is constructed, no
service is contacted, and no real search is issued. The adapter's own
`build_readonly_milvus_client` factory is never invoked here.
"""

from __future__ import annotations

import ast
import tempfile
import unittest
from pathlib import Path

import numpy as np

from vdbench.config import IndexTrack, Metric
from vdbench.exp012_scale_contract import Exp012ScaleProfile, build_exp012_scale_contract
from vdbench.host_observation import CompletedRangeQueryObservation, ServedQueryOutcome
from vdbench.host_window_lineage import SQLiteHostResponseCommitStore
from vdbench.milvus import CollectionIdentity
from vdbench.milvus_actuation import CollectionIdentityBinding, StackHealth
from vdbench.oracle import exact_range_search
from vdbench.shadow_event_types import MonitorStreamKey
from vdbench.shadow_search_telemetry import (
    SQLiteShadowSearchTelemetryStore,
    ShadowSearchRole,
    ShadowSearchTelemetryBinding,
)
from vdbench.shadow_window import TRACE_QUERY_COUNT
from vdbench.v2_milvus_shadow_capture import (
    V2MilvusShadowCaptureError,
    V2MilvusShadowCaptureExecutor,
    V2ShadowCaptureIdentityBinding,
)

MODULE_PATH = (
    Path(__file__).parents[1] / "src" / "vdbench" / "v2_milvus_shadow_capture.py"
)
DATASET001 = Path(__file__).parents[1] / "artifacts" / "exp-001" / "dataset"
DATA_IDENTITY = (
    "DATASET-001-v1:sha256:"
    "b6cb56a3eee60f6728be1d08a465e2a2500eec4089b4466da76fe2e886b51da9"
)
_RADIUS = 180.0
_SERVED_EF = 400
_REVISION = "revision/exp010-live"
_ENVIRONMENT = "e" * 64
_FLAT = "exp001_l2_flat"
_HNSW = "exp001_l2_hnsw"


def _stream() -> MonitorStreamKey:
    return MonitorStreamKey(
        "v2-live", Metric.L2, "target-075", "config-v1", DATA_IDENTITY,
        "flat-index-v1", "hnsw-index-v1",
    )


def _identity(track: IndexTrack) -> CollectionIdentity:
    description: dict[str, object] = {
        "index_type": track.value,
        "metric_type": Metric.L2.value,
    }
    if track is IndexTrack.HNSW:
        description.update({"M": "16", "efConstruction": "200"})
    return CollectionIdentity(
        _FLAT if track is IndexTrack.FLAT else _HNSW,
        Metric.L2.value,
        track.value,
        description,
    )


def _binding() -> V2ShadowCaptureIdentityBinding:
    return V2ShadowCaptureIdentityBinding(
        flat_collection_name=_FLAT,
        hnsw_collection_name=_HNSW,
        flat_binding=CollectionIdentityBinding(
            identity_id="flat-index-v1", expected=_identity(IndexTrack.FLAT)
        ),
        hnsw_binding=CollectionIdentityBinding(
            identity_id="hnsw-index-v1", expected=_identity(IndexTrack.HNSW)
        ),
    )


class _FakeHealthProbe:
    def check(self) -> StackHealth:
        return StackHealth(etcd_healthy=True, minio_healthy=True)


class _OracleBackedClient:
    """Answers every search with the exact oracle for the supplied vector.

    This stands in for a perfect-recall Milvus. It never consults a query id:
    it can only answer whatever vector the adapter actually sends, which is
    what makes the live-vector conservation tests meaningful.
    """

    def __init__(self) -> None:
        self.base_ids = np.load(DATASET001 / "base_ids.npy", allow_pickle=False)
        self.base_vectors = np.load(DATASET001 / "base_vectors.npy", allow_pickle=False)
        self.searched_vectors: list[tuple[float, ...]] = []
        self.calls: list[tuple[str, int | None]] = []

    def search(self, **kwargs: object):
        collection = kwargs["collection_name"]
        params = kwargs["search_params"]["params"]
        ef = params.get("ef")
        self.calls.append((collection, ef))
        vector = np.asarray(kwargs["data"][0], dtype="<f4")
        self.searched_vectors.append(tuple(float(v) for v in vector))
        result = exact_range_search(
            self.base_vectors, self.base_ids, vector, Metric.L2,
            radius=params["radius"], range_filter=params["range_filter"], limit=100,
        )
        return [[{"id": hit.id, "distance": hit.score} for hit in result.hits]]

    def describe_index(self, **kwargs: object):
        name = kwargs["collection_name"]
        track = IndexTrack.FLAT if name == _FLAT else IndexTrack.HNSW
        return dict(_identity(track).description)


def _observations(count: int, *, dimensions: int = 128):
    generator = np.random.Generator(np.random.PCG64(4242))
    with tempfile.TemporaryDirectory() as directory, SQLiteHostResponseCommitStore(
        Path(directory) / "source.sqlite3",
        stream_key=_stream(),
        source_revision=_REVISION,
        environment_manifest_sha256=_ENVIRONMENT,
    ) as store:
        for index in range(count):
            vector = generator.standard_normal(dimensions).astype("<f4")
            store.commit_response(
                CompletedRangeQueryObservation(
                    index, "2026-08-12T00:00:00Z", _stream(),
                    tuple(float(v) for v in vector), _RADIUS, 0.0, 100, _SERVED_EF,
                    ServedQueryOutcome(True, False, 1, 1.0),
                ),
                committed_at_utc="2026-08-12T00:00:00Z",
            )
        return store.poll(consumer_id="fixture", limit=count)


def _executor(client, telemetry=None) -> V2MilvusShadowCaptureExecutor:
    return V2MilvusShadowCaptureExecutor(
        client=client,
        stream_key=_stream(),
        dataset001_dir=DATASET001,
        identity_binding=_binding(),
        threshold_radius=_RADIUS,
        served_ef=_SERVED_EF,
        source_revision=_REVISION,
        environment_manifest_sha256=_ENVIRONMENT,
        stack_health_probe=_FakeHealthProbe(),
        occurred_at_clock=lambda: "2026-08-12T00:00:05Z",
        search_telemetry_store=telemetry,
    )


class V2MilvusShadowCaptureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sources = _observations(TRACE_QUERY_COUNT)

    def test_fifty_committed_observations_make_one_valid_trace(self) -> None:
        client = _OracleBackedClient()
        trace = _executor(client).capture(self.sources, trace_sequence_index=0)
        self.assertEqual(len(trace.queries), TRACE_QUERY_COUNT)
        self.assertTrue(trace.complete, trace.reason_codes)
        self.assertEqual(trace.sentinel_ef, 100)
        self.assertEqual(trace.data_identity, DATA_IDENTITY)

    def test_the_committed_query_vector_is_the_vector_actually_searched(self) -> None:
        """The decisive preflight property: no query-id lookup into any
        pre-registered workload; the live committed vector is what is sent."""

        client = _OracleBackedClient()
        trace = _executor(client).capture(self.sources, trace_sequence_index=0)
        committed = [item.query_vector for item in self.sources]
        for offset, source_vector in enumerate(committed):
            with self.subTest(position=offset):
                # Recorded on the trace ...
                self.assertEqual(trace.queries[offset].query_vector, source_vector)
                self.assertEqual(trace.queries[offset].query_id, self.sources[offset].query_id)
        # ... and actually transmitted to the client, for both searches.
        self.assertEqual(len(client.searched_vectors), TRACE_QUERY_COUNT * 2)
        for offset, source_vector in enumerate(committed):
            self.assertEqual(client.searched_vectors[offset * 2], source_vector)
            self.assertEqual(client.searched_vectors[offset * 2 + 1], source_vector)

    def test_live_query_ids_need_no_pre_registration(self) -> None:
        """These ids exist only because live traffic produced them."""

        client = _OracleBackedClient()
        trace = _executor(client).capture(self.sources, trace_sequence_index=0)
        self.assertEqual(
            tuple(q.query_id for q in trace.queries),
            tuple(s.query_id for s in self.sources),
        )

    def test_exactly_one_flat_and_one_sentinel_search_per_query(self) -> None:
        client = _OracleBackedClient()
        _executor(client).capture(self.sources, trace_sequence_index=0)
        flat = [call for call in client.calls if call[0] == _FLAT]
        hnsw = [call for call in client.calls if call[0] == _HNSW]
        self.assertEqual(len(flat), TRACE_QUERY_COUNT)
        self.assertEqual(len(hnsw), TRACE_QUERY_COUNT)
        # No candidate/LKG search: every HNSW call is the sentinel ef.
        self.assertEqual({call[1] for call in hnsw}, {100})
        self.assertEqual({call[1] for call in flat}, {None})

    def test_scale_telemetry_binds_every_physical_search_to_source_and_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            binding = ShadowSearchTelemetryBinding(
                campaign_id="exp012-scale-test",
                scale_contract=build_exp012_scale_contract(
                    Exp012ScaleProfile.SCALE_2400
                ),
                stream_key=_stream(),
                source_revision=_REVISION,
                environment_manifest_sha256=_ENVIRONMENT,
            )
            with SQLiteShadowSearchTelemetryStore(
                Path(raw) / "telemetry.sqlite3", binding=binding
            ) as telemetry:
                _executor(_OracleBackedClient(), telemetry).capture(
                    self.sources, trace_sequence_index=0
                )
                records = telemetry.records()
                self.assertEqual(len(records), TRACE_QUERY_COUNT * 2)
                for offset, source in enumerate(self.sources):
                    pair = records[offset * 2 : offset * 2 + 2]
                    self.assertEqual(
                        tuple(item.role for item in pair),
                        (
                            ShadowSearchRole.FLAT_REFERENCE,
                            ShadowSearchRole.HNSW_SENTINEL,
                        ),
                    )
                    self.assertEqual({item.source_sequence for item in pair}, {source.source_sequence})
                    self.assertEqual({item.source_sha256 for item in pair}, {source.source_sha256})
                    self.assertEqual(len({item.attempt_sha256 for item in pair}), 1)

    def test_oracle_comes_from_dataset001_base_material(self) -> None:
        client = _OracleBackedClient()
        trace = _executor(client).capture(self.sources, trace_sequence_index=0)
        base_ids = np.load(DATASET001 / "base_ids.npy", allow_pickle=False)
        base_vectors = np.load(DATASET001 / "base_vectors.npy", allow_pickle=False)
        for offset in (0, 7, TRACE_QUERY_COUNT - 1):
            expected = exact_range_search(
                base_vectors, base_ids,
                np.asarray(self.sources[offset].query_vector, dtype="<f4"),
                Metric.L2, radius=_RADIUS, range_filter=0.0, limit=100,
            )
            record = trace.queries[offset]
            self.assertEqual(record.exact_cardinality, expected.full_count)
            self.assertEqual(
                tuple(h.id for h in record.oracle_result.hits),
                tuple(h.id for h in expected.hits),
            )

    def test_query_order_is_preserved(self) -> None:
        client = _OracleBackedClient()
        trace = _executor(client).capture(self.sources, trace_sequence_index=0)
        self.assertEqual(
            [q.query_id for q in trace.queries],
            [s.query_id for s in self.sources],
        )

    # -- fail-closed --------------------------------------------------

    def test_partial_slice_fails_closed(self) -> None:
        with self.assertRaises(V2MilvusShadowCaptureError) as raised:
            _executor(_OracleBackedClient()).capture(
                self.sources[:-1], trace_sequence_index=0
            )
        self.assertEqual(raised.exception.code, "CAPTURE_SLICE_COUNT_INVALID")

    def test_duplicate_observation_fails_closed(self) -> None:
        duplicated = (self.sources[0],) + self.sources[1:-1] + (self.sources[0],)
        with self.assertRaises(V2MilvusShadowCaptureError) as raised:
            _executor(_OracleBackedClient()).capture(duplicated, trace_sequence_index=0)
        self.assertEqual(raised.exception.code, "CAPTURE_DUPLICATE_QUERY_ID")

    def test_foreign_stream_source_revision_and_environment_fail_closed(self) -> None:
        client = _OracleBackedClient()
        for label, kwargs, code in (
            ("source_revision", {"source_revision": "revision/other"},
             "CAPTURE_SOURCE_REVISION_MISMATCH"),
            ("environment", {"environment_manifest_sha256": "f" * 64},
             "CAPTURE_ENVIRONMENT_MISMATCH"),
        ):
            with self.subTest(case=label):
                values = {
                    "client": client, "stream_key": _stream(),
                    "dataset001_dir": DATASET001, "identity_binding": _binding(),
                    "threshold_radius": _RADIUS, "served_ef": _SERVED_EF,
                    "source_revision": _REVISION,
                    "environment_manifest_sha256": _ENVIRONMENT,
                    "stack_health_probe": _FakeHealthProbe(),
                    "occurred_at_clock": lambda: "2026-08-12T00:00:05Z",
                }
                values.update(kwargs)
                executor = V2MilvusShadowCaptureExecutor(**values)
                with self.assertRaises(V2MilvusShadowCaptureError) as raised:
                    executor.capture(self.sources, trace_sequence_index=0)
                self.assertEqual(raised.exception.code, code)

    def test_data_identity_mismatch_fails_closed_at_construction(self) -> None:
        foreign = MonitorStreamKey(
            "v2-live", Metric.L2, "target-075", "config-v1",
            "DATASET-001-v1:sha256:" + "0" * 64, "flat-index-v1", "hnsw-index-v1",
        )
        with self.assertRaises(V2MilvusShadowCaptureError) as raised:
            V2MilvusShadowCaptureExecutor(
                client=_OracleBackedClient(), stream_key=foreign,
                dataset001_dir=DATASET001, identity_binding=_binding(),
                threshold_radius=_RADIUS, served_ef=_SERVED_EF,
                source_revision=_REVISION, environment_manifest_sha256=_ENVIRONMENT,
                stack_health_probe=_FakeHealthProbe(),
                occurred_at_clock=lambda: "2026-08-12T00:00:05Z",
            )
        self.assertEqual(raised.exception.code, "CAPTURE_DATA_IDENTITY_MISMATCH")

    def test_dimension_mismatch_fails_closed(self) -> None:
        wrong = _observations(TRACE_QUERY_COUNT, dimensions=4)
        with self.assertRaises(V2MilvusShadowCaptureError) as raised:
            _executor(_OracleBackedClient()).capture(wrong, trace_sequence_index=0)
        self.assertEqual(raised.exception.code, "CAPTURE_DIMENSIONS_MISMATCH")


class V2MilvusShadowCaptureGuardTests(unittest.TestCase):
    def test_module_never_constructs_a_client_at_import_or_capture(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"), filename=str(MODULE_PATH))
        # PyMilvus may only be imported inside the lazy factory.
        module_level = {
            alias.name
            for node in tree.body
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in getattr(node, "names", [])
        }
        self.assertNotIn("pymilvus", module_level)
        self.assertNotIn("MilvusClient", module_level)

    def test_module_has_no_policy_or_canary_authority_dependency(self) -> None:
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
            "policy", "canary_admission", "canary_approval", "canary_activation",
            "canary_route_authority", "canary_routing", "canary_live_runner",
            "canary_grant_store",
        }
        offending = {
            item for item in imported
            if any(item == name or item.endswith(f".{name}") for name in forbidden)
        }
        self.assertFalse(offending, offending)
        self.assertNotIn("START_CANARY", MODULE_PATH.read_text(encoding="utf-8"))

    def test_no_test_in_this_file_contacts_a_live_endpoint(self) -> None:
        """AST-scanned, not text-scanned: this guard necessarily names the
        forbidden symbols itself, so a substring check would be self-defeating."""

        tree = ast.parse(Path(__file__).read_text(encoding="utf-8"), filename=__file__)
        called = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        } | {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertNotIn("build_readonly_milvus_client", called)
        self.assertNotIn("MilvusClient", called)
        self.assertNotIn("connect", called)


if __name__ == "__main__":
    unittest.main()
