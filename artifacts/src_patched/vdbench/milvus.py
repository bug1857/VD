"""Thin synchronous PyMilvus adapter for the immutable EXP-001 contract.

PyMilvus is imported lazily so unit tests exercise all request construction with
an in-memory fake and never require or contact a live Milvus service.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Callable, Protocol

import numpy as np
import numpy.typing as npt

from .config import (
    CONSISTENCY_LEVEL,
    HNSW_EF_CONSTRUCTION,
    HNSW_M,
    INDEX_NAME,
    INSERT_BATCH_SIZE,
    PRIMARY_FIELD,
    RESULT_LIMIT,
    VECTOR_FIELD,
    ContractViolation,
    IndexTrack,
    Metric,
    SearchConfiguration,
)
from .dataset import DatasetBundle


class SchemaLike(Protocol):
    def add_field(self, **kwargs: object) -> None: ...


class IndexParamsLike(Protocol):
    def add_index(self, **kwargs: object) -> None: ...


class ClientLike(Protocol):
    def has_collection(self, *, collection_name: str) -> bool: ...
    def create_schema(self, **kwargs: object) -> SchemaLike: ...
    def create_collection(self, **kwargs: object) -> None: ...
    def prepare_index_params(self) -> IndexParamsLike: ...
    def create_index(self, **kwargs: object) -> None: ...
    def insert(self, **kwargs: object) -> object: ...
    def flush(self, **kwargs: object) -> object: ...
    def get_collection_stats(self, **kwargs: object) -> dict[str, object]: ...
    def query(self, **kwargs: object) -> list[dict[str, object]]: ...
    def load_collection(self, **kwargs: object) -> None: ...
    def get_load_state(self, **kwargs: object) -> object: ...
    def describe_index(self, **kwargs: object) -> object: ...
    def search(self, **kwargs: object) -> object: ...


@dataclass(frozen=True, slots=True)
class SearchHit:
    id: int
    score: float


@dataclass(frozen=True, slots=True)
class CollectionIdentity:
    collection_name: str
    metric: str
    index_track: str
    description: object


def _validate_index_description(
    description: object, metric: Metric, track: IndexTrack
) -> None:
    if not isinstance(description, dict):
        raise ContractViolation("Milvus index description must be a mapping")
    if description.get("index_type") != track.value:
        raise ContractViolation("Milvus index metadata has the wrong index_type")
    if description.get("metric_type") != metric.value:
        raise ContractViolation("Milvus index metadata has the wrong metric_type")
    state = description.get("state")
    if state is not None and state != "Finished":
        raise ContractViolation(f"Milvus index state is not Finished: {state}")
    if track is IndexTrack.HNSW:
        params = description.get("params")
        if isinstance(params, str):
            params = json.loads(params)
        elif params is None:
            params = description
        if not isinstance(params, dict):
            raise ContractViolation("HNSW index metadata omits build parameters")
        if int(params.get("M", -1)) != HNSW_M:
            raise ContractViolation("HNSW metadata M does not match 16")
        if int(params.get("efConstruction", -1)) != HNSW_EF_CONSTRUCTION:
            raise ContractViolation("HNSW metadata efConstruction does not match 200")


def collection_name(prefix: str, metric: Metric, track: IndexTrack) -> str:
    """Build a stable experiment-scoped collection name."""

    normalized = "".join(character if character.isalnum() else "_" for character in prefix)
    if not normalized or len(normalized) > 220:
        raise ContractViolation("collection prefix must contain 1-220 characters")
    return f"{normalized}_{metric.value.lower()}_{track.value.lower()}"


def _default_data_types() -> tuple[object, object]:
    from pymilvus import DataType

    return DataType.INT64, DataType.FLOAT_VECTOR


class MilvusHarness:
    """Construct collections and execute synchronous, fully materialized searches."""

    def __init__(
        self,
        client: ClientLike,
        *,
        dimensions: int,
        data_types: Callable[[], tuple[object, object]] = _default_data_types,
    ) -> None:
        self.client = client
        self.dimensions = dimensions
        self._data_types = data_types

    def create_and_load_collection(
        self,
        *,
        name: str,
        metric: Metric,
        track: IndexTrack,
        dataset: DatasetBundle,
    ) -> CollectionIdentity:
        """Create one clean FLAT/HNSW collection, ingest, read back, and load it."""

        if dataset.base_vectors.shape != (dataset.spec.base_count, self.dimensions):
            raise ContractViolation("dataset shape does not match collection dimensions")
        if self.client.has_collection(collection_name=name):
            raise ContractViolation(f"collection already exists: {name}")

        int64_type, vector_type = self._data_types()
        schema = self.client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(
            field_name=PRIMARY_FIELD,
            datatype=int64_type,
            is_primary=True,
        )
        schema.add_field(
            field_name=VECTOR_FIELD,
            datatype=vector_type,
            dim=self.dimensions,
        )
        self.client.create_collection(
            collection_name=name,
            schema=schema,
            consistency_level=CONSISTENCY_LEVEL,
        )

        for start in range(0, dataset.spec.base_count, INSERT_BATCH_SIZE):
            stop = min(start + INSERT_BATCH_SIZE, dataset.spec.base_count)
            rows = [
                {
                    PRIMARY_FIELD: int(dataset.ids[index]),
                    VECTOR_FIELD: dataset.base_vectors[index].tolist(),
                }
                for index in range(start, stop)
            ]
            self.client.insert(collection_name=name, data=rows)
        self.client.flush(collection_name=name)
        stats = self.client.get_collection_stats(collection_name=name)
        row_count = int(stats.get("row_count", stats.get("num_entities", -1)))
        if row_count != dataset.spec.base_count:
            raise ContractViolation(
                f"Milvus row count {row_count} != expected {dataset.spec.base_count}"
            )

        sample_ids = (int(dataset.ids[0]), int(dataset.ids[-1]))

        index_params = self.client.prepare_index_params()
        parameters: dict[str, object] = {
            "field_name": VECTOR_FIELD,
            "index_name": INDEX_NAME,
            "index_type": track.value,
            "metric_type": metric.value,
        }
        if track is IndexTrack.HNSW:
            parameters["params"] = {"M": HNSW_M, "efConstruction": HNSW_EF_CONSTRUCTION}
        index_params.add_index(**parameters)
        self.client.create_index(collection_name=name, index_params=index_params, sync=True)
        self.client.load_collection(collection_name=name)
        load_state = self.client.get_load_state(collection_name=name)
        if "Loaded" not in str(load_state):
            raise ContractViolation(f"collection did not reach Loaded state: {load_state}")

        read_back = self.client.query(
            collection_name=name,
            filter=f"{PRIMARY_FIELD} in [{sample_ids[0]}, {sample_ids[1]}]",
            output_fields=[PRIMARY_FIELD, VECTOR_FIELD],
            consistency_level=CONSISTENCY_LEVEL,
            limit=2,
        )
        by_id = {int(row[PRIMARY_FIELD]): row[VECTOR_FIELD] for row in read_back}
        for position, identifier in ((0, sample_ids[0]), (-1, sample_ids[1])):
            actual = np.asarray(by_id.get(identifier), dtype="<f4")
            if not np.array_equal(actual, dataset.base_vectors[position]):
                raise ContractViolation(f"Milvus read-back mismatch for id={identifier}")
        description = self.client.describe_index(
            collection_name=name, index_name=INDEX_NAME
        )
        _validate_index_description(description, metric, track)
        return CollectionIdentity(name, metric.value, track.value, description)

    def index_identity(
        self, name: str, metric: Metric, track: IndexTrack
    ) -> CollectionIdentity:
        """Capture index metadata outside all timed boundaries."""

        description = self.client.describe_index(
            collection_name=name, index_name=INDEX_NAME
        )
        _validate_index_description(description, metric, track)
        return CollectionIdentity(
            name,
            metric.value,
            track.value,
            description,
        )

    def search(
        self,
        *,
        name: str,
        query: npt.NDArray[np.float32],
        configuration: SearchConfiguration,
    ) -> tuple[SearchHit, ...]:
        """Execute and fully materialize one contract-valid search response."""

        configuration.validate()
        search_params: dict[str, object] = {
            "metric_type": configuration.metric.value,
            "params": {
                "radius": configuration.radius,
                "range_filter": configuration.range_filter,
            },
        }
        if configuration.index_track is IndexTrack.HNSW:
            search_params["params"]["ef"] = configuration.ef  # type: ignore[index]
        response = self.client.search(
            collection_name=name,
            data=[np.asarray(query, dtype="<f4").tolist()],
            anns_field=VECTOR_FIELD,
            search_params=search_params,
            limit=RESULT_LIMIT,
            output_fields=[],
            consistency_level=CONSISTENCY_LEVEL,
        )
        materialized = tuple(tuple(group) for group in response)  # type: ignore[arg-type]
        if len(materialized) != 1:
            raise ContractViolation("single-query search returned an invalid batch shape")
        hits: list[SearchHit] = []
        for item in materialized[0]:
            identifier = item.get("id") if isinstance(item, dict) else item.id
            score = item.get("distance") if isinstance(item, dict) else item.distance
            hits.append(SearchHit(id=int(identifier), score=float(score)))
        if len({hit.id for hit in hits}) != len(hits):
            raise ContractViolation("Milvus returned duplicate IDs")
        return tuple(hits)
