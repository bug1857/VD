from __future__ import annotations

import copy
import unittest
from datetime import UTC, datetime

from vdbench.canonical_serialization import strict_canonical_digest
from vdbench.gate_c_execution_environment import (
    GateCExecutionEnvironmentError,
    GateCExecutionEnvironmentObservationSpec,
    build_gate_c_execution_environment_attestation,
    build_gate_c_execution_environment_identity,
    gate_c_execution_environment_attestation_document,
    gate_c_execution_environment_identity_document,
    observe_gate_c_execution_environment,
    parse_gate_c_execution_environment_attestation_document,
    parse_gate_c_execution_environment_identity_document,
    verify_gate_c_execution_environment_eligibility,
)


_COLLECTION_SCHEMA_DOMAIN = b"VD::EXP012_GATE_C_COLLECTION_SCHEMA::V1\x00"
_INDEX_IDENTITY_DOMAIN = b"VD::EXP012_GATE_C_INDEX_IDENTITY::V1\x00"


def _container(role: str, ordinal: int) -> dict[str, object]:
    return {
        "role": role,
        "container_name": f"vd-{role}",
        "container_id": f"{ordinal:064x}",
        "image_id": f"{ordinal + 10:064x}",
        "repository_digests": [f"vd/{role}@sha256:{ordinal + 20:064x}"],
        "started_at": f"2026-08-26T00:00:0{ordinal}Z",
        "restart_count": 0,
        "oom_killed": False,
        "labels": [
            {"name": "com.docker.compose.project", "value": "vd"},
            {"name": "com.docker.compose.service", "value": role},
        ],
        "mounts": [
            {
                "type": "volume",
                "name": f"vd-{role}-data",
                "source": f"/var/lib/docker/volumes/vd-{role}",
                "destination": f"/var/lib/{role}",
                "mode": "rw",
                "read_only": False,
            }
        ],
        "networks": [
            {
                "network_name": "vd_default",
                "network_id": "9" * 64,
                "endpoint_id": f"{ordinal + 30:064x}",
                "gateway": "172.18.0.1",
                "ip_address": f"172.18.0.{ordinal + 1}",
                "ip_prefix_len": 16,
                "global_ipv6_address": "",
                "global_ipv6_prefix_len": 0,
                "mac_address": f"02:42:ac:12:00:0{ordinal}",
                "aliases": [f"vd-{role}"],
            }
        ],
        "published_ports": [
            {
                "container_port": 19000 + ordinal,
                "protocol": "tcp",
                "host_ip": "127.0.0.1",
                "host_port": 19000 + ordinal,
            }
        ],
    }


def _data_plane(role: str, collection: str) -> dict[str, object]:
    fields = [
        {
            "name": "id",
            "data_type": "INT64",
            "is_primary": True,
            "auto_id": False,
            "description": "",
            "parameters": [],
        },
        {
            "name": "vector",
            "data_type": "FLOAT_VECTOR",
            "is_primary": False,
            "auto_id": False,
            "description": "",
            "parameters": [{"name": "dim", "value": 128}],
        },
    ]
    schema_payload = {
        "schema_version": "exp012-gate-c-collection-schema-v1",
        "collection_name": collection,
        "database_name": "default",
        "fields": fields,
    }
    schema_sha = strict_canonical_digest(_COLLECTION_SCHEMA_DOMAIN, schema_payload)
    index_type = "FLAT" if role == "flat" else "HNSW"
    parameters = [] if role == "flat" else [
        {"name": "M", "value": 16},
        {"name": "efConstruction", "value": 200},
    ]
    index_payload = {
        "schema_version": "exp012-gate-c-index-identity-v1",
        "collection_name": collection,
        "database_name": "default",
        "collection_schema_sha256": schema_sha,
        "index_name": "vector_idx",
        "index_type": index_type,
        "index_metric": "L2",
        "index_parameters": parameters,
    }
    return {
        "role": role,
        "collection_name": collection,
        "database_name": "default",
        "collection_schema": fields,
        "collection_schema_sha256": schema_sha,
        "metric": "L2",
        "dimensions": 128,
        "entity_count": 10_000,
        "index_name": "vector_idx",
        "index_type": index_type,
        "index_metric": "L2",
        "index_parameters": parameters,
        "index_identity_sha256": strict_canonical_digest(
            _INDEX_IDENTITY_DOMAIN, index_payload
        ),
    }


def _rehash_data_plane(item: dict[str, object]) -> None:
    schema_payload = {
        "schema_version": "exp012-gate-c-collection-schema-v1",
        "collection_name": item["collection_name"],
        "database_name": item["database_name"],
        "fields": item["collection_schema"],
    }
    item["collection_schema_sha256"] = strict_canonical_digest(
        _COLLECTION_SCHEMA_DOMAIN, schema_payload
    )
    index_payload = {
        "schema_version": "exp012-gate-c-index-identity-v1",
        "collection_name": item["collection_name"],
        "database_name": item["database_name"],
        "collection_schema_sha256": item["collection_schema_sha256"],
        "index_name": item["index_name"],
        "index_type": item["index_type"],
        "index_metric": item["index_metric"],
        "index_parameters": item["index_parameters"],
    }
    item["index_identity_sha256"] = strict_canonical_digest(
        _INDEX_IDENTITY_DOMAIN, index_payload
    )


def _governed() -> dict[str, object]:
    flat = _data_plane("flat", "flat_collection")
    hnsw = _data_plane("hnsw", "hnsw_collection")

    def gate_a_live(item: dict[str, object]) -> dict[str, object]:
        live = {
            "collection_name": item["collection_name"],
            "index_name": item["index_name"],
            "index_type": item["index_type"],
            "metric_type": item["index_metric"],
            "row_count": item["entity_count"],
            "dimensions": item["dimensions"],
            "indexed_rows": item["entity_count"],
            "pending_index_rows": 0,
            "index_state": "Finished",
            "load_state": "Loaded",
        }
        if item["role"] == "hnsw":
            live.update({"M": 16, "efConstruction": 200})
        return live

    return {
        "campaign_identity": "exp012-scale10000-v1",
        "scale_contract_sha256": "1" * 64,
        "gate_a_evidence_sha256": "2" * 64,
        "source_revision": "3" * 40,
        "environment_manifest_sha256": "4" * 64,
        "data_identity": "DATASET-001-v1:sha256:data",
        "configuration_identity": "exp010-serving-config-v1:sha256:cfg",
        "flat_binding_id": "flat-binding",
        "hnsw_binding_id": "hnsw-binding",
        "metric": "L2",
        "dimensions": 128,
        "expected_entity_count": 10_000,
        "served_ef": 400,
        "consistency_level": "Strong",
        "flat_collection_name": "flat_collection",
        "hnsw_collection_name": "hnsw_collection",
        "flat_gate_a_binding": {
            "binding_id": "flat-binding",
            "live": gate_a_live(flat),
        },
        "hnsw_gate_a_binding": {
            "binding_id": "hnsw-binding",
            "live": gate_a_live(hnsw),
        },
    }


def _metadata(*, observed_at: str = "2026-08-26T00:10:00Z") -> dict[str, object]:
    return {
        "observed_at_utc": observed_at,
        "container_health": {"etcd": True, "minio": True, "milvus": True},
        "milvus_healthz": True,
        "collection_readiness": {"flat": True, "hnsw": True},
        "index_readiness": {"flat": True, "hnsw": True},
    }


def environment_fixture(
    *,
    observed_at: str = "2026-08-26T00:10:00Z",
    governed: dict[str, object] | None = None,
    execution_source_revision: str = "5" * 40,
):
    endpoint = {
        "scheme": "http",
        "host": "127.0.0.1",
        "port": 19530,
        "transport_security": "PLAINTEXT",
    }
    containers = [_container(role, index) for index, role in enumerate(
        ("etcd", "minio", "milvus"), start=1
    )]
    data_plane = [
        _data_plane("flat", "flat_collection"),
        _data_plane("hnsw", "hnsw_collection"),
    ]
    identity = build_gate_c_execution_environment_identity(
        endpoint=endpoint,
        containers=containers,
        data_plane=data_plane,
        expected_entity_count=10_000,
    )
    attestation = build_gate_c_execution_environment_attestation(
        identity=identity,
        execution_source_revision=execution_source_revision,
        governed_bindings=_governed() if governed is None else governed,
        observed_runtime={
            "endpoint": identity.payload["endpoint"],
            "containers": identity.payload["containers"],
            "data_plane": identity.payload["data_plane"],
        },
        observation_metadata=_metadata(observed_at=observed_at),
        compatibility_verified=True,
    )
    return endpoint, containers, data_plane, identity, attestation


class GateCExecutionEnvironmentTests(unittest.TestCase):
    def test_frozen_identity_and_attestation_digests(self) -> None:
        *_unused, identity, attestation = environment_fixture()
        self.assertEqual(
            identity.execution_environment_identity_sha256,
            "ef021af3c021b8e8e7c52dc524d5ff886259cc137c5f37ed93f544a917216146",
        )
        self.assertEqual(
            attestation.execution_environment_attestation_sha256,
            "3191ddd43b52bc669900fe6173332b0c84c60d41f999cb7a967babe855d4d8a4",
        )

    def test_identity_and_attestation_round_trip_exactly(self) -> None:
        *_unused, identity, attestation = environment_fixture()
        self.assertEqual(
            parse_gate_c_execution_environment_identity_document(
                gate_c_execution_environment_identity_document(identity)
            ).canonical_payload_bytes,
            identity.canonical_payload_bytes,
        )
        self.assertEqual(
            parse_gate_c_execution_environment_attestation_document(
                gate_c_execution_environment_attestation_document(attestation)
            ).canonical_payload_bytes,
            attestation.canonical_payload_bytes,
        )
        self.assertEqual(
            verify_gate_c_execution_environment_eligibility(attestation), identity
        )

    def test_gate_a_binding_projection_accepts_exact_flat_and_hnsw_metadata(self) -> None:
        *_unused, identity, attestation = environment_fixture()
        governed = attestation.payload["governed_bindings"]
        self.assertEqual(
            governed["flat_gate_a_binding"]["binding_id"],
            governed["flat_binding_id"],
        )
        self.assertEqual(
            governed["hnsw_gate_a_binding"]["binding_id"],
            governed["hnsw_binding_id"],
        )
        self.assertEqual(
            verify_gate_c_execution_environment_eligibility(attestation), identity
        )

    def test_gate_a_binding_projection_rejects_governed_index_drift(self) -> None:
        endpoint, containers, planes, _identity, _attestation = environment_fixture()
        mutations = (
            ("flat-index-name", 0, lambda item: item.update(index_name="other-index")),
            ("flat-index-type", 0, lambda item: item.update(index_type="IVF_FLAT")),
            ("flat-index-metric", 0, lambda item: item.update(index_metric="COSINE")),
            (
                "hnsw-M",
                1,
                lambda item: item["index_parameters"][0].update(value=32),
            ),
            (
                "hnsw-ef-construction",
                1,
                lambda item: item["index_parameters"][1].update(value=201),
            ),
        )
        for name, index, mutation in mutations:
            with self.subTest(name=name):
                changed = copy.deepcopy(planes)
                mutation(changed[index])
                _rehash_data_plane(changed[index])
                identity = build_gate_c_execution_environment_identity(
                    endpoint=endpoint,
                    containers=containers,
                    data_plane=changed,
                    expected_entity_count=10_000,
                )
                with self.assertRaisesRegex(
                    GateCExecutionEnvironmentError,
                    "GATE_C_EXECUTION_COMPATIBILITY_FAILED",
                ):
                    build_gate_c_execution_environment_attestation(
                        identity=identity,
                        execution_source_revision="5" * 40,
                        governed_bindings=_governed(),
                        observed_runtime={
                            "endpoint": identity.payload["endpoint"],
                            "containers": identity.payload["containers"],
                            "data_plane": identity.payload["data_plane"],
                        },
                        observation_metadata=_metadata(),
                        compatibility_verified=True,
                    )

    def test_gate_a_binding_authority_cannot_substitute_expected_id_or_live_record(self) -> None:
        *_unused, identity, _attestation = environment_fixture()
        cases = []
        wrong_id = copy.deepcopy(_governed())
        wrong_id["hnsw_gate_a_binding"]["binding_id"] = "other-binding"
        cases.append(wrong_id)
        wrong_live = copy.deepcopy(_governed())
        wrong_live["hnsw_gate_a_binding"]["live"]["M"] = 32
        cases.append(wrong_live)
        for governed in cases:
            with self.subTest(governed=governed):
                with self.assertRaises(GateCExecutionEnvironmentError):
                    build_gate_c_execution_environment_attestation(
                        identity=identity,
                        execution_source_revision="5" * 40,
                        governed_bindings=governed,
                        observed_runtime={
                            "endpoint": identity.payload["endpoint"],
                            "containers": identity.payload["containers"],
                            "data_plane": identity.payload["data_plane"],
                        },
                        observation_metadata=_metadata(),
                        compatibility_verified=True,
                    )

    def test_stable_identity_excludes_all_provenance_and_transient_metadata(self) -> None:
        *_unused, first_identity, first = environment_fixture()
        *_unused, second_identity, second = environment_fixture(
            observed_at="2026-08-26T00:11:00Z"
        )
        self.assertEqual(
            first_identity.execution_environment_identity_sha256,
            second_identity.execution_environment_identity_sha256,
        )
        self.assertNotEqual(
            first.execution_environment_attestation_sha256,
            second.execution_environment_attestation_sha256,
        )
        identity_keys = set(first_identity.payload)
        self.assertTrue(
            {
                "source_revision",
                "execution_source_revision",
                "environment_manifest_sha256",
                "observed_at_utc",
                "health",
                "readiness",
            }.isdisjoint(identity_keys)
        )

    def test_readiness_changes_attestation_not_identity_and_fails_eligibility(self) -> None:
        *_unused, identity, attestation = environment_fixture()
        document = gate_c_execution_environment_attestation_document(attestation)
        payload = copy.deepcopy(document["attestation_payload"])
        payload["observation_metadata"]["collection_readiness"]["flat"] = False
        rebuilt = build_gate_c_execution_environment_attestation(
            identity=identity,
            execution_source_revision=payload["execution_source_revision"],
            governed_bindings=payload["governed_bindings"],
            observed_runtime=payload["observed_runtime"],
            observation_metadata=payload["observation_metadata"],
            compatibility_verified=True,
        )
        self.assertEqual(
            rebuilt.execution_environment_identity_sha256,
            identity.execution_environment_identity_sha256,
        )
        with self.assertRaisesRegex(
            GateCExecutionEnvironmentError,
            "GATE_C_EXECUTION_ENVIRONMENT_NOT_READY",
        ):
            verify_gate_c_execution_environment_eligibility(rebuilt)

    def test_every_stable_runtime_category_changes_identity(self) -> None:
        endpoint, containers, planes, identity, _attestation = environment_fixture()
        mutations = (
            ("endpoint", lambda value: value.update(port=19531)),
            ("container-id", lambda value: value[0].update(container_id="f" * 64)),
            ("image", lambda value: value[0].update(image_id="e" * 64)),
            ("repository-digest", lambda value: value[0].update(
                repository_digests=["vd/etcd@sha256:" + "e" * 64]
            )),
            ("started", lambda value: value[0].update(started_at="2026-08-27T00:00:01Z")),
            ("restart", lambda value: value[0].update(restart_count=1)),
            ("oom", lambda value: value[0].update(oom_killed=True)),
            ("mount", lambda value: value[0]["mounts"][0].update(source="/changed")),
            ("network", lambda value: value[0]["networks"][0].update(network_id="e" * 64)),
            ("port", lambda value: value[0]["published_ports"][0].update(host_port=29999)),
            ("label", lambda value: value[0]["labels"][0].update(value="changed")),
            ("collection", lambda value: value[0].update(collection_name="changed")),
            ("schema", lambda value: value[0]["collection_schema"][0].update(description="changed")),
            ("index-name", lambda value: value[0].update(index_name="changed")),
            ("index-type", lambda value: value[0].update(index_type="IVF_FLAT")),
            ("index-metric", lambda value: value[0].update(index_metric="COSINE")),
            ("index-parameters", lambda value: value[1]["index_parameters"][0].update(value=32)),
            ("dimension", lambda value: value[0].update(dimensions=129)),
            ("metric", lambda value: value[0].update(metric="COSINE")),
        )
        for name, mutation in mutations:
            with self.subTest(name=name):
                changed_endpoint = copy.deepcopy(endpoint)
                changed_containers = copy.deepcopy(containers)
                changed_planes = copy.deepcopy(planes)
                target = (
                    changed_endpoint
                    if name == "endpoint"
                    else changed_containers
                    if name in {
                        "container-id", "image", "repository-digest", "started",
                        "restart", "oom", "mount", "network", "port", "label",
                    }
                    else changed_planes
                )
                mutation(target)
                if name in {
                    "collection", "schema", "index-name", "index-type",
                    "index-metric", "index-parameters",
                }:
                    # A supplied derived digest cannot mask changed canonical bytes.
                    with self.assertRaises(GateCExecutionEnvironmentError):
                        build_gate_c_execution_environment_identity(
                            endpoint=changed_endpoint,
                            containers=changed_containers,
                            data_plane=changed_planes,
                            expected_entity_count=10_000,
                        )
                    continue
                changed = build_gate_c_execution_environment_identity(
                    endpoint=changed_endpoint,
                    containers=changed_containers,
                    data_plane=changed_planes,
                    expected_entity_count=10_000,
                )
                self.assertNotEqual(
                    changed.execution_environment_identity_sha256,
                    identity.execution_environment_identity_sha256,
                )

    def test_entity_count_is_exact_integer_equal_and_governed(self) -> None:
        endpoint, containers, planes, _identity, _attestation = environment_fixture()
        cases = (True, 10_000.0, "10000", 9_999)
        for value in cases:
            with self.subTest(value=value):
                changed = copy.deepcopy(planes)
                changed[0]["entity_count"] = value
                with self.assertRaises(GateCExecutionEnvironmentError):
                    build_gate_c_execution_environment_identity(
                        endpoint=endpoint,
                        containers=containers,
                        data_plane=changed,
                        expected_entity_count=10_000,
                    )
        changed = copy.deepcopy(planes)
        changed[1]["entity_count"] = 9_999
        with self.assertRaises(GateCExecutionEnvironmentError):
            build_gate_c_execution_environment_identity(
                endpoint=endpoint,
                containers=containers,
                data_plane=changed,
                expected_entity_count=10_000,
            )

        forged_container = copy.deepcopy(containers)
        forged_container[0]["networks"][0]["gateway"] = 0
        with self.assertRaises(GateCExecutionEnvironmentError):
            build_gate_c_execution_environment_identity(
                endpoint=endpoint,
                containers=forged_container,
                data_plane=planes,
                expected_entity_count=10_000,
            )

    def test_tamper_unknown_fields_and_identity_substitution_fail(self) -> None:
        *_unused, identity, attestation = environment_fixture()
        identity_document = gate_c_execution_environment_identity_document(identity)
        forged = copy.deepcopy(identity_document)
        forged["identity_payload"]["unexpected"] = True
        with self.assertRaises(GateCExecutionEnvironmentError):
            parse_gate_c_execution_environment_identity_document(forged)
        attestation_document = gate_c_execution_environment_attestation_document(
            attestation
        )
        forged = copy.deepcopy(attestation_document)
        forged["attestation_payload"]["execution_source_revision"] = "6" * 40
        with self.assertRaises(GateCExecutionEnvironmentError):
            parse_gate_c_execution_environment_attestation_document(forged)

    def test_metadata_observer_has_no_search_surface_and_performs_no_search(self) -> None:
        class Reader:
            def __init__(self) -> None:
                self.calls: list[str] = []
                self.collection_name_override: str | None = None
                self.index_name_override: str | None = None
                self.dimensions = 128

            def describe_collection(self, *, collection_name: str):
                self.calls.append("describe_collection")
                return {
                    "collection_name": (
                        self.collection_name_override or collection_name
                    ),
                    "fields": [
                    {"name": "id", "type": "INT64", "is_primary": True,
                     "auto_id": False, "description": "", "params": {}},
                    {"name": "vector", "type": "FLOAT_VECTOR",
                     "is_primary": False, "auto_id": False,
                     "description": "", "params": {"dim": self.dimensions}},
                    ],
                }

            def describe_index(self, *, collection_name: str, index_name: str):
                self.calls.append("describe_index")
                return {
                    "index_name": self.index_name_override or index_name,
                    "index_type": "FLAT" if "flat" in collection_name else "HNSW",
                    "metric_type": "L2", "state": "Finished",
                    "indexed_rows": 10_000, "pending_index_rows": 0,
                    "params": {} if "flat" in collection_name else
                    {"M": 16, "efConstruction": 200},
                }

            def get_collection_stats(self, *, collection_name: str):
                self.calls.append("get_collection_stats")
                return {"row_count": 10_000}

            def get_load_state(self, *, collection_name: str):
                self.calls.append("get_load_state")
                return {"state": "Loaded"}

        def docker_document(role: str, ordinal: int) -> dict[str, object]:
            container = _container(role, ordinal)
            return {
                "Id": container["container_id"],
                "Image": container["image_id"],
                "RepoDigests": container["repository_digests"],
                "RestartCount": 0,
                "Config": {"Image": container["repository_digests"][0], "Labels": {}},
                "State": {"Status": "running", "OOMKilled": False,
                          "StartedAt": container["started_at"],
                          "Health": {"Status": "healthy"}},
                "Mounts": [],
                "NetworkSettings": {"Networks": {}, "Ports": {}},
            }

        documents = {
            "vd-etcd": docker_document("etcd", 1),
            "vd-minio": docker_document("minio", 2),
            "vd-milvus": docker_document("milvus", 3),
        }
        reader = Reader()
        attestation = observe_gate_c_execution_environment(
            GateCExecutionEnvironmentObservationSpec(
                milvus_uri="http://127.0.0.1:19530",
                database_name="default",
                etcd_container="vd-etcd",
                minio_container="vd-minio",
                milvus_container="vd-milvus",
                flat_collection_name="flat_collection",
                hnsw_collection_name="hnsw_collection",
                index_name="vector_idx",
                metric="L2",
                dimensions=128,
                expected_entity_count=10_000,
            ),
            metadata_reader=reader,
            container_inspector=documents.__getitem__,
            image_inspector=lambda image_id: {
                "RepoDigests": next(
                    document["RepoDigests"]
                    for document in documents.values()
                    if document["Image"] == image_id
                )
            },
            milvus_healthz_probe=lambda: True,
            execution_source_revision="5" * 40,
            governed_bindings=_governed(),
            clock=lambda: datetime(2026, 8, 26, tzinfo=UTC),
        )
        verify_gate_c_execution_environment_eligibility(attestation)
        self.assertEqual(len(reader.calls), 8)
        self.assertFalse(hasattr(reader, "search"))

        for attribute, value in (
            ("collection_name_override", "other-collection"),
            ("index_name_override", "other-index"),
            ("dimensions", 256),
        ):
            with self.subTest(attribute=attribute):
                drifted = Reader()
                setattr(drifted, attribute, value)
                with self.assertRaisesRegex(
                    GateCExecutionEnvironmentError,
                    "GATE_C_EXECUTION_COMPATIBILITY_FAILED",
                ):
                    observe_gate_c_execution_environment(
                        GateCExecutionEnvironmentObservationSpec(
                            milvus_uri="http://127.0.0.1:19530",
                            database_name="default",
                            etcd_container="vd-etcd",
                            minio_container="vd-minio",
                            milvus_container="vd-milvus",
                            flat_collection_name="flat_collection",
                            hnsw_collection_name="hnsw_collection",
                            index_name="vector_idx",
                            metric="L2",
                            dimensions=128,
                            expected_entity_count=10_000,
                        ),
                        metadata_reader=drifted,
                        container_inspector=documents.__getitem__,
                        image_inspector=lambda image_id: {
                            "RepoDigests": next(
                                document["RepoDigests"]
                                for document in documents.values()
                                if document["Image"] == image_id
                            )
                        },
                        milvus_healthz_probe=lambda: True,
                        execution_source_revision="5" * 40,
                        governed_bindings=_governed(),
                        clock=lambda: datetime(2026, 8, 26, tzinfo=UTC),
                    )


if __name__ == "__main__":
    unittest.main()
