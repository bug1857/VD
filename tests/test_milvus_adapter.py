import unittest
from dataclasses import replace

import numpy as np

from vdbench.config import (
    EXP001_DATASET_SPEC,
    ContractViolation,
    IndexTrack,
    Metric,
    SearchConfiguration,
)
from vdbench.dataset import DatasetBundle
from vdbench.milvus import MilvusHarness


class FakeSchema:
    def __init__(self) -> None:
        self.fields = []

    def add_field(self, **kwargs) -> None:
        self.fields.append(kwargs)


class FakeIndexParams:
    def __init__(self) -> None:
        self.indexes = []

    def add_index(self, **kwargs) -> None:
        self.indexes.append(kwargs)


class FakeClient:
    def __init__(self, bundle: DatasetBundle) -> None:
        self.bundle = bundle
        self.schema = FakeSchema()
        self.index_params = FakeIndexParams()
        self.calls = []
        self.search_response = [[{"id": 0, "distance": 0.25}]]
        self.load_state = {"state": "Loaded"}
        self.index_description = None

    def has_collection(self, **kwargs):
        self.calls.append(("has_collection", kwargs))
        return False

    def create_schema(self, **kwargs):
        self.calls.append(("create_schema", kwargs))
        return self.schema

    def create_collection(self, **kwargs):
        self.calls.append(("create_collection", kwargs))

    def insert(self, **kwargs):
        self.calls.append(("insert", kwargs))
        return {}

    def flush(self, **kwargs):
        self.calls.append(("flush", kwargs))
        return {}

    def get_collection_stats(self, **kwargs):
        self.calls.append(("get_collection_stats", kwargs))
        return {"row_count": str(self.bundle.spec.base_count)}

    def query(self, **kwargs):
        self.calls.append(("query", kwargs))
        return [
            {"id": int(self.bundle.ids[0]), "vector": self.bundle.base_vectors[0].tolist()},
            {"id": int(self.bundle.ids[-1]), "vector": self.bundle.base_vectors[-1].tolist()},
        ]

    def prepare_index_params(self):
        self.calls.append(("prepare_index_params", {}))
        return self.index_params

    def create_index(self, **kwargs):
        self.calls.append(("create_index", kwargs))

    def load_collection(self, **kwargs):
        self.calls.append(("load_collection", kwargs))

    def get_load_state(self, **kwargs):
        self.calls.append(("get_load_state", kwargs))
        return self.load_state

    def describe_index(self, **kwargs):
        self.calls.append(("describe_index", kwargs))
        if self.index_description is not None:
            return self.index_description
        track = self.index_params.indexes[0] if self.index_params.indexes else {
            "index_type": "HNSW",
            "metric_type": "L2",
            "params": {"M": 16, "efConstruction": 200},
        }
        return {
            "index_name": "vector_index",
            "index_type": track["index_type"],
            "metric_type": track["metric_type"],
            "params": track.get("params", {}),
            "state": "Finished",
            "build_id": 42,
        }

    def search(self, **kwargs):
        self.calls.append(("search", kwargs))
        return self.search_response


class MilvusAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        spec = replace(
            EXP001_DATASET_SPEC,
            version="adapter-unit-v1",
            dimensions=2,
            base_count=2,
            calibration_query_count=1,
            measured_query_count=1,
        )
        self.bundle = DatasetBundle(
            ids=np.asarray([0, 1], dtype=np.int64),
            base_vectors=np.asarray([[0.0, 0.0], [1.0, 1.0]], dtype="<f4"),
            calibration_queries=np.asarray([[0.5, 0.5]], dtype="<f4"),
            measured_queries=np.asarray([[0.25, 0.25]], dtype="<f4"),
            spec=spec,
        )

    def test_hnsw_setup_uses_pinned_schema_build_params_and_readback(self) -> None:
        client = FakeClient(self.bundle)
        harness = MilvusHarness(
            client, dimensions=2, data_types=lambda: ("INT64", "FLOAT_VECTOR")
        )
        identity = harness.create_and_load_collection(
            name="exp002_l2_hnsw",
            metric=Metric.L2,
            track=IndexTrack.HNSW,
            dataset=self.bundle,
        )
        self.assertEqual(identity.collection_name, "exp002_l2_hnsw")
        self.assertEqual(client.schema.fields[0]["is_primary"], True)
        self.assertEqual(client.schema.fields[1]["dim"], 2)
        self.assertEqual(
            client.index_params.indexes,
            [
                {
                    "field_name": "vector",
                    "index_name": "vector_index",
                    "index_type": "HNSW",
                    "metric_type": "L2",
                    "params": {"M": 16, "efConstruction": 200},
                }
            ],
        )
        create = next(kwargs for name, kwargs in client.calls if name == "create_collection")
        self.assertEqual(create["consistency_level"], "Strong")

    def test_read_back_occurs_only_after_collection_reaches_loaded_state(self) -> None:
        client = FakeClient(self.bundle)
        harness = MilvusHarness(
            client, dimensions=2, data_types=lambda: ("INT64", "FLOAT_VECTOR")
        )

        harness.create_and_load_collection(
            name="exp002_l2_hnsw",
            metric=Metric.L2,
            track=IndexTrack.HNSW,
            dataset=self.bundle,
        )

        call_names = [name for name, _ in client.calls]
        self.assertLess(
            call_names.index("create_index"), call_names.index("load_collection")
        )
        self.assertLess(
            call_names.index("load_collection"), call_names.index("get_load_state")
        )
        self.assertLess(call_names.index("get_load_state"), call_names.index("query"))

    def test_unloaded_collection_fails_before_read_back(self) -> None:
        client = FakeClient(self.bundle)
        client.load_state = {"state": "NotLoaded"}
        harness = MilvusHarness(
            client, dimensions=2, data_types=lambda: ("INT64", "FLOAT_VECTOR")
        )

        with self.assertRaisesRegex(
            ContractViolation, "collection did not reach Loaded state"
        ):
            harness.create_and_load_collection(
                name="exp002_l2_hnsw",
                metric=Metric.L2,
                track=IndexTrack.HNSW,
                dataset=self.bundle,
            )

        self.assertNotIn("query", [name for name, _ in client.calls])

    def test_hnsw_validation_accepts_milvus_3_flattened_build_params(self) -> None:
        client = FakeClient(self.bundle)
        client.index_description = {
            "index_name": "vector_index",
            "index_type": "HNSW",
            "metric_type": "L2",
            "M": "16",
            "efConstruction": "200",
            "state": "Finished",
            "build_id": 42,
        }
        harness = MilvusHarness(client, dimensions=2)

        identity = harness.index_identity(
            "exp002_l2_hnsw", Metric.L2, IndexTrack.HNSW
        )

        self.assertEqual(identity.description, client.index_description)

    def test_flat_setup_has_no_hnsw_build_parameters(self) -> None:
        client = FakeClient(self.bundle)
        harness = MilvusHarness(
            client, dimensions=2, data_types=lambda: ("INT64", "FLOAT_VECTOR")
        )
        harness.create_and_load_collection(
            name="exp002_cosine_flat",
            metric=Metric.COSINE,
            track=IndexTrack.FLAT,
            dataset=self.bundle,
        )
        self.assertNotIn("params", client.index_params.indexes[0])
        self.assertEqual(client.index_params.indexes[0]["index_type"], "FLAT")

    def test_search_request_has_exact_range_params_limit_and_no_payload(self) -> None:
        client = FakeClient(self.bundle)
        harness = MilvusHarness(client, dimensions=2)
        configuration = SearchConfiguration(
            metric=Metric.COSINE,
            threshold_label="target-025",
            radius=0.5,
            index_track=IndexTrack.HNSW,
            ef=400,
        )
        hits = harness.search(
            name="exp002_cosine_hnsw",
            query=self.bundle.measured_queries[0],
            configuration=configuration,
        )
        self.assertEqual(hits[0].id, 0)
        request = next(kwargs for name, kwargs in client.calls if name == "search")
        self.assertEqual(request["limit"], 100)
        self.assertEqual(request["output_fields"], [])
        self.assertEqual(request["consistency_level"], "Strong")
        self.assertEqual(
            request["search_params"],
            {
                "metric_type": "COSINE",
                "params": {"radius": 0.5, "range_filter": 1.0, "ef": 400},
            },
        )


if __name__ == "__main__":
    unittest.main()
