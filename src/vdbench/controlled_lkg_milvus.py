"""Pinned ordinary ORM connection for ADR-026, with no implicit write replay.

No connection is made at import. Construction runs under an external process
deadline; numeric SDK RPC timeouts are deliberately avoided for mutations.
The Connect handshake's SDK transport timeout is not an application mutation.
"""
from __future__ import annotations

import hashlib
import math
import time
import uuid
from pathlib import Path
from typing import Any

from .controlled_rematerialization import require

RPC_POLICY = {"timeout": None, "retry_times": 0, "retry_on_rate_limit": False, "_async": False}
_SDK_FILES = {
    "orm/collection.py": "f0b6c54bf3e9254c1c7c4b0a4566b0a6b50265b247bc505bcd27287cee74a51f",
    "orm/connections.py": "3a3ba2ebac12e06d19edd89809541458b273715b38e763f38c75a9ff8f1cb7d5",
    "orm/prepare.py": "4350ed8bb1262229d6119610719ad409ed443a5944023e3831aa7ddbc0387fdf",
    "client/grpc_handler.py": "9a3660633713d5c1b886af37e57626ded2ca819e94fbfd7a7ba5a74f1fe2312c",
    "client/prepare.py": "8a382046249740c885cb008794b9f596814f18fda627cf42abe13efd73357702",
    "client/entity_helper.py": "93a85db42e62897cdc1518441b03e2781160f3e5125ea21d113ae942ba19c5b7",
    "decorators.py": "2c0436be7fc9a6f595e53289afeb336e5d4ba174e15387f5080ed443cfdde68c",
    "client/connection_manager.py": "83e4147e3f4f009f3391e0bd8448bc73c17c1c740cb0f9a5de2a3f6b958cb9cf",
    "client/types.py": "9ce9d14ac9730afa877729586f32da7cbcb6a00f7715312fd19ee03d3a3c6fc4",
    "client/field_data_extractors.py": "f3812ca07f447250cd65a890e773dda0353b3dcf29f6086b52874189a9368ef7",
    "client/abstract.py": "042e222fb7bd8f7638d7cc24c49aa199a359c688c4d94aa070e27d1131527f83",
    "client/search_result.py": "cacdefe48a419e4b6569fba1ad237bb93b7c72de5facf5df6841a5d5c02a262e",
}


def verify_sdk_source() -> dict[str, str]:
    import pymilvus

    require(pymilvus.__version__ == "3.0.1", "SDK_VERSION_MISMATCH")
    root = Path(pymilvus.__file__).parent
    observed = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in _SDK_FILES}
    require(observed == _SDK_FILES, "SDK_SOURCE_MISMATCH")
    return observed


class PinnedColumnarMilvus:
    """Explicit fixed front gateway, default database, unique ordinary alias.

    This class never reconnects, repairs, switches gateways, or constructs a
    ManagedMilvusClient. Callers own the deadline, revocation and control proof.
    """

    def __init__(self, *, host: str, username: str, password: str,
                 deadline_ns: int, monotonic_ns=time.monotonic_ns) -> None:
        import ipaddress
        from pymilvus import connections

        verify_sdk_source()
        address = ipaddress.IPv4Address(host)
        require(str(address) == host and address.is_private and not address.is_loopback,
                "FIXED_GATEWAY_INVALID")
        self._clock, self._deadline = monotonic_ns, deadline_ns
        self.alias = "vd_controlled_" + uuid.uuid4().hex
        self._collections: dict[str, Any] = {}
        try:
            connections.connect(alias=self.alias, host=host, port="19530", db_name="default",
                                user=username, password=password, keep_alive=False,
                                grpc_options={"grpc.enable_retries": 0})
            self._handler = connections._fetch_handler(self.alias)
            require(type(self._handler).__name__ == "GrpcHandler", "ORDINARY_CONNECTION_REQUIRED")
            self._context = connections._generate_call_context(self.alias, db_name="default")
        except BaseException:
            connections.disconnect(self.alias)
            connections.remove_connection(self.alias)
            raise

    def check(self) -> None:
        require(self._clock() < self._deadline, "NATIVE_OPERATION_DEADLINE")

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        self.check()
        result = getattr(self._handler, method)(*args, context=self._context,
                                              **RPC_POLICY, **kwargs)
        self.check()
        return result

    def _collection(self, name: str) -> Any:
        from pymilvus import Collection

        if name not in self._collections:
            self._collections[name] = Collection(name, using=self.alias, **RPC_POLICY)
        return self._collections[name]

    def has_collection(self, name: str) -> bool:
        return self._call("has_collection", name)

    def create_collection(self, name: str) -> int:
        from pymilvus import CollectionSchema, DataType, FieldSchema

        schema = CollectionSchema([
            FieldSchema("id", DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema("vector", DataType.FLOAT_VECTOR, dim=128),
        ], auto_id=False, enable_dynamic_field=False)
        self._call("create_collection", name, schema=schema, consistency_level="Strong")
        raw = self.describe_collection(name)
        return raw["collection_id"]

    def insert(self, name: str, pks: tuple[int, ...], vectors: Any) -> dict[str, Any]:
        self.check()
        result = self._collection(name).insert([list(pks), vectors], **RPC_POLICY)
        self.check()
        return {"insert_count": result.insert_count, "primary_keys": result.primary_keys}

    def flush(self, name: str) -> dict[str, Any]:
        from pymilvus.client.grpc_handler import _api_level_md, check_status
        from pymilvus.client.prepare import Prepare

        self.check()
        # SDK flush's nested polling drops retry flags. Dispatch the same
        # native Flush request once and use explicit non-retrying read polling.
        response = self._handler._stub.Flush(Prepare.flush_param([name]), timeout=None,
                                             metadata=_api_level_md(self._context))
        check_status(response.status)
        require(name in response.coll_segIDs and name in response.coll_flush_ts,
                "FLUSH_ACK_INVALID")
        segments = list(response.coll_segIDs[name].data)
        timestamp = response.coll_flush_ts[name]
        while not self._call("get_flush_state", segments, name, timestamp):
            time.sleep(0.1)
        return {"flush_timestamp": timestamp, "segment_ids": segments}

    def create_index(self, name: str, params: dict[str, Any]) -> None:
        self._call("create_index", name, "vector", params, index_name="vector", sync=False)
        while True:
            progress = self._call("get_index_build_progress", name, index_name="vector")
            require(type(progress) is dict and all(type(progress.get(k)) is int and progress[k] >= 0
                    for k in ("total_rows", "indexed_rows", "pending_index_rows")),
                    "INDEX_PROGRESS_INVALID")
            require(progress.get("state") in {"Unissued", "InProgress", "Finished"},
                    "INDEX_BUILD_FAILED_OR_UNKNOWN")
            if (progress["state"] == "Finished" and progress["pending_index_rows"] == 0
                    and progress["indexed_rows"] == progress["total_rows"]):
                require(progress["total_rows"] >= 10000, "INDEX_BUILD_INCOMPLETE")
                break
            time.sleep(0.1)

    def load(self, name: str) -> None:
        from pymilvus.client.grpc_handler import _api_level_md, check_status
        from pymilvus.client.prepare import Prepare

        self.check()
        response = self._handler._stub.LoadCollection(Prepare.load_collection(name, 1),
                    timeout=None, metadata=_api_level_md(self._context))
        check_status(response)
        while True:
            # load_collection_progress drops retry flags in its nested read.
            progress = self._call("get_loading_progress", name)
            require(type(progress) is int and 0 <= progress <= 100, "LOAD_PROGRESS_INVALID")
            if progress == 100:
                break
            time.sleep(0.1)

    def describe_collection(self, name: str) -> dict[str, Any]:
        return self._call("describe_collection", name)

    def describe_index(self, name: str) -> dict[str, Any]:
        return self._call("describe_index", name, index_name="vector")

    def query(self, name: str, expression: str, fields: list[str]) -> list[dict[str, Any]]:
        # Omit limit, offset and partition_names entirely. Materialize the SDK's
        # lazy vectors before the control/deadline checks after this response.
        rows = self._call("query", name, expression, output_fields=fields,
                          consistency_level="Strong", strict_float32=True)
        result = []
        for row in rows:
            value = dict(row)
            if "vector" in value:
                import numpy as np

                # strict_float32 materializes the native FLOAT_VECTOR as an
                # ndarray. Validate before conversion so a FLOAT64 response
                # cannot be rounded into a canonical value by this adapter.
                vector = value["vector"]
                require(type(vector) is np.ndarray and vector.dtype.str == "<f4"
                        and vector.shape == (128,) and bool(np.isfinite(vector).all()),
                        "NATIVE_VECTOR_ENCODING_INVALID")
                value["vector"] = vector.tolist()
            result.append(value)
        self.check()
        return result

    def search(self, *, collection_name: str, data: Any, anns_field: str,
               search_params: Any, limit: int, output_fields: Any,
               consistency_level: str) -> list[list[dict[str, Any]]]:
        self.check()
        result = self._collection(collection_name).search(data, anns_field, param=search_params,
                    limit=limit, output_fields=output_fields, consistency_level=consistency_level,
                    **RPC_POLICY)
        batches = []
        for hits in result:
            batch = []
            for hit in hits:
                require(type(hit.id) is int and type(hit.distance) is float
                        and math.isfinite(hit.distance), "SEARCH_RESPONSE_INVALID")
                batch.append({"id": hit.id, "distance": hit.distance})
            batches.append(batch)
        self.check()
        return batches

    def close(self) -> None:
        from pymilvus import connections

        connections.disconnect(self.alias)
        connections.remove_connection(self.alias)
        self._collections.clear()
