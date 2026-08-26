"""Prospective v3 execution-environment provenance for bounded Gate C.

This module keeps current execution-runtime identity separate from frozen
Gate-A/Gate-B provenance and from the source revision executing Gate C.  Its
observer surface is metadata-only: Docker inspection, Milvus metadata, and a
health endpoint are permitted; vector search and every mutation are absent.

The digests provide local integrity/provenance under the repository's local
host trust model.  They are not hostile-host cryptographic attestation.
"""

from __future__ import annotations

import http.client
import ipaddress
import json
import os
import re
import socket
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from urllib.parse import quote, urlsplit

from .canonical_serialization import (
    decode_strict_canonical_json,
    strict_canonical_digest,
    strict_canonical_json_bytes,
)

__all__ = [
    "EXECUTION_ENVIRONMENT_ATTESTATION_SCHEMA_VERSION",
    "EXECUTION_ENVIRONMENT_IDENTITY_SCHEMA_VERSION",
    "GateCExecutionEnvironmentAttestation",
    "GateCExecutionEnvironmentError",
    "GateCExecutionEnvironmentIdentity",
    "GateCExecutionEnvironmentObservationSpec",
    "GateCMetadataReader",
    "DockerExecutionMetadataInspector",
    "build_gate_c_execution_environment_attestation",
    "build_gate_c_execution_environment_identity",
    "gate_c_execution_environment_attestation_document",
    "gate_c_execution_environment_identity_document",
    "observe_gate_c_execution_environment",
    "parse_gate_c_execution_environment_attestation_document",
    "parse_gate_c_execution_environment_identity_document",
    "verify_gate_c_execution_environment_eligibility",
]


EXECUTION_ENVIRONMENT_IDENTITY_SCHEMA_VERSION = (
    "exp012-scale-gate-c-execution-environment-identity-v1"
)
EXECUTION_ENVIRONMENT_ATTESTATION_SCHEMA_VERSION = (
    "exp012-scale-gate-c-execution-environment-attestation-v1"
)
_IDENTITY_DOMAIN = (
    b"VD::EXP012_SCALE_GATE_C_EXECUTION_ENVIRONMENT_IDENTITY::V1\x00"
)
_ATTESTATION_DOMAIN = (
    b"VD::EXP012_SCALE_GATE_C_EXECUTION_ENVIRONMENT_ATTESTATION::V1\x00"
)
_COLLECTION_SCHEMA_DOMAIN = b"VD::EXP012_GATE_C_COLLECTION_SCHEMA::V1\x00"
_INDEX_IDENTITY_DOMAIN = b"VD::EXP012_GATE_C_INDEX_IDENTITY::V1\x00"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_REVISION = re.compile(r"[0-9a-f]{40}")
_RFC3339_UTC = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,9})?Z"
)
_CONTAINER_ROLES = ("etcd", "minio", "milvus")
_DATA_PLANE_ROLES = ("flat", "hnsw")
_APPROVED_LABELS = frozenset(
    {
        "com.docker.compose.config-hash",
        "com.docker.compose.container-number",
        "com.docker.compose.image",
        "com.docker.compose.oneoff",
        "com.docker.compose.project",
        "com.docker.compose.project.config_files",
        "com.docker.compose.project.working_dir",
        "com.docker.compose.service",
        "com.docker.compose.version",
    }
)


class GateCExecutionEnvironmentError(ValueError):
    """Fail-closed execution-environment error with a stable reason code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> GateCExecutionEnvironmentError:
    return GateCExecutionEnvironmentError(code)


def _text(value: object, *, code: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise _error(code)
    if any(ord(character) < 0x20 for character in value):
        raise _error(code)
    strict_canonical_json_bytes(value)
    return value


def _optional_text(value: object, *, code: str) -> str:
    if type(value) is not str:
        raise _error(code)
    if value == "":
        return value
    return _text(value, code=code)


def _sha(value: object, *, code: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _error(code)
    return value


def _revision(value: object, *, code: str) -> str:
    if type(value) is not str or _REVISION.fullmatch(value) is None:
        raise _error(code)
    return value


def _integer(value: object, *, minimum: int, code: str) -> int:
    if type(value) is not int or value < minimum:
        raise _error(code)
    return value


def _boolean(value: object, *, code: str) -> bool:
    if type(value) is not bool:
        raise _error(code)
    return value


def _mapping(value: object, fields: set[str], *, code: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != fields:
        raise _error(code)
    return value


def _tuple_of_mappings(value: object, *, code: str) -> tuple[dict[str, object], ...]:
    if type(value) not in (list, tuple) or any(type(item) is not dict for item in value):
        raise _error(code)
    return tuple(value)


@dataclass(frozen=True, slots=True, init=False)
class GateCExecutionEnvironmentIdentity:
    """Builder-issued immutable canonical stable runtime identity."""

    canonical_payload_bytes: bytes
    execution_environment_identity_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("execution-environment identities are builder-issued")

    @property
    def payload(self) -> dict[str, object]:
        value = decode_strict_canonical_json(self.canonical_payload_bytes)
        if type(value) is not dict:
            raise _error("GATE_C_EXECUTION_ENVIRONMENT_IDENTITY_INVALID")
        return value


@dataclass(frozen=True, slots=True, init=False)
class GateCExecutionEnvironmentAttestation:
    """Builder-issued immutable current observation and compatibility proof."""

    canonical_payload_bytes: bytes
    execution_environment_attestation_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("execution-environment attestations are builder-issued")

    @property
    def payload(self) -> dict[str, object]:
        value = decode_strict_canonical_json(self.canonical_payload_bytes)
        if type(value) is not dict:
            raise _error("GATE_C_EXECUTION_ENVIRONMENT_ATTESTATION_INVALID")
        return value

    @property
    def execution_environment_identity_sha256(self) -> str:
        return _sha(
            self.payload["execution_environment_identity_sha256"],
            code="GATE_C_EXECUTION_ENVIRONMENT_ATTESTATION_INVALID",
        )

    @property
    def execution_source_revision(self) -> str:
        return _revision(
            self.payload["execution_source_revision"],
            code="GATE_C_EXECUTION_ENVIRONMENT_ATTESTATION_INVALID",
        )


def _make_identity(payload: dict[str, object]) -> GateCExecutionEnvironmentIdentity:
    result = object.__new__(GateCExecutionEnvironmentIdentity)
    raw = strict_canonical_json_bytes(payload)
    object.__setattr__(result, "canonical_payload_bytes", raw)
    object.__setattr__(
        result,
        "execution_environment_identity_sha256",
        strict_canonical_digest(_IDENTITY_DOMAIN, payload),
    )
    return result


def _make_attestation(
    payload: dict[str, object],
) -> GateCExecutionEnvironmentAttestation:
    result = object.__new__(GateCExecutionEnvironmentAttestation)
    raw = strict_canonical_json_bytes(payload)
    object.__setattr__(result, "canonical_payload_bytes", raw)
    object.__setattr__(
        result,
        "execution_environment_attestation_sha256",
        strict_canonical_digest(_ATTESTATION_DOMAIN, payload),
    )
    return result


def _validate_endpoint(value: object) -> dict[str, object]:
    endpoint = _mapping(
        value,
        {"scheme", "host", "port", "transport_security"},
        code="GATE_C_EXECUTION_ENDPOINT_INVALID",
    )
    scheme = _text(endpoint["scheme"], code="GATE_C_EXECUTION_ENDPOINT_INVALID").lower()
    host = _text(endpoint["host"], code="GATE_C_EXECUTION_ENDPOINT_INVALID").lower()
    port = _integer(endpoint["port"], minimum=1, code="GATE_C_EXECUTION_ENDPOINT_INVALID")
    if port > 65535:
        raise _error("GATE_C_EXECUTION_ENDPOINT_INVALID")
    security = _text(
        endpoint["transport_security"], code="GATE_C_EXECUTION_ENDPOINT_INVALID"
    )
    if security not in {"PLAINTEXT", "TLS"}:
        raise _error("GATE_C_EXECUTION_ENDPOINT_INVALID")
    if (scheme in {"https", "grpcs"}) != (security == "TLS"):
        raise _error("GATE_C_EXECUTION_ENDPOINT_INVALID")
    return {
        "scheme": scheme,
        "host": host,
        "port": port,
        "transport_security": security,
    }


def _validate_name_value_list(value: object, *, code: str) -> list[dict[str, object]]:
    items = _tuple_of_mappings(value, code=code)
    result: list[dict[str, object]] = []
    prior: str | None = None
    for item in items:
        item = _mapping(item, {"name", "value"}, code=code)
        name = _text(item["name"], code=code)
        scalar = item["value"]
        if type(scalar) not in (str, int) or type(scalar) is bool:
            raise _error(code)
        if type(scalar) is str:
            _text(scalar, code=code)
        if prior is not None and name <= prior:
            raise _error(code)
        prior = name
        result.append({"name": name, "value": scalar})
    return result


def _validate_container(value: object, expected_role: str) -> dict[str, object]:
    code = "GATE_C_EXECUTION_CONTAINER_INVALID"
    fields = {
        "role", "container_name", "container_id", "image_id",
        "repository_digests", "started_at", "restart_count", "oom_killed",
        "labels", "mounts", "networks", "published_ports",
    }
    item = _mapping(value, fields, code=code)
    if item["role"] != expected_role:
        raise _error(code)
    container_id = _text(item["container_id"], code=code)
    image_id = _text(item["image_id"], code=code)
    if len(container_id) < 12 or len(image_id) < 12:
        raise _error(code)
    digests = item["repository_digests"]
    if type(digests) not in (list, tuple):
        raise _error(code)
    canonical_digests = tuple(_text(value, code=code) for value in digests)
    if canonical_digests != tuple(sorted(set(canonical_digests))):
        raise _error(code)
    started_at = _text(item["started_at"], code=code)
    if _RFC3339_UTC.fullmatch(started_at) is None:
        raise _error(code)
    labels = _validate_name_value_list(item["labels"], code=code)
    if any(entry["name"] not in _APPROVED_LABELS for entry in labels):
        raise _error(code)

    mounts: list[dict[str, object]] = []
    for mount in _tuple_of_mappings(item["mounts"], code=code):
        mount = _mapping(
            mount,
            {"type", "name", "source", "destination", "mode", "read_only"},
            code=code,
        )
        mounts.append(
            {
                "type": _text(mount["type"], code=code),
                "name": "" if mount["name"] == "" else _text(mount["name"], code=code),
                "source": _text(mount["source"], code=code),
                "destination": _text(mount["destination"], code=code),
                "mode": "" if mount["mode"] == "" else _text(mount["mode"], code=code),
                "read_only": _boolean(mount["read_only"], code=code),
            }
        )
    if mounts != sorted(mounts, key=lambda entry: (entry["destination"], entry["source"])):
        raise _error(code)

    networks: list[dict[str, object]] = []
    network_fields = {
        "network_name", "network_id", "endpoint_id", "gateway", "ip_address",
        "ip_prefix_len", "global_ipv6_address", "global_ipv6_prefix_len",
        "mac_address", "aliases",
    }
    for network in _tuple_of_mappings(item["networks"], code=code):
        network = _mapping(network, network_fields, code=code)
        aliases = network["aliases"]
        if type(aliases) not in (list, tuple):
            raise _error(code)
        canonical_aliases = tuple(_text(alias, code=code) for alias in aliases)
        if canonical_aliases != tuple(sorted(set(canonical_aliases))):
            raise _error(code)
        networks.append(
            {
                "network_name": _text(network["network_name"], code=code),
                "network_id": _text(network["network_id"], code=code),
                "endpoint_id": _text(network["endpoint_id"], code=code),
                "gateway": _optional_text(network["gateway"], code=code),
                "ip_address": _optional_text(network["ip_address"], code=code),
                "ip_prefix_len": _integer(network["ip_prefix_len"], minimum=0, code=code),
                "global_ipv6_address": _optional_text(
                    network["global_ipv6_address"], code=code
                ),
                "global_ipv6_prefix_len": _integer(
                    network["global_ipv6_prefix_len"], minimum=0, code=code
                ),
                "mac_address": _optional_text(network["mac_address"], code=code),
                "aliases": list(canonical_aliases),
            }
        )
    if networks != sorted(networks, key=lambda entry: entry["network_name"]):
        raise _error(code)

    ports: list[dict[str, object]] = []
    for port_binding in _tuple_of_mappings(item["published_ports"], code=code):
        port_binding = _mapping(
            port_binding,
            {"container_port", "protocol", "host_ip", "host_port"},
            code=code,
        )
        container_port = _integer(port_binding["container_port"], minimum=1, code=code)
        host_port = _integer(port_binding["host_port"], minimum=1, code=code)
        if container_port > 65535 or host_port > 65535:
            raise _error(code)
        protocol = _text(port_binding["protocol"], code=code).lower()
        if protocol not in {"tcp", "udp", "sctp"}:
            raise _error(code)
        host_ip = _text(port_binding["host_ip"], code=code)
        try:
            ipaddress.ip_address(host_ip)
        except ValueError as exc:
            raise _error(code) from exc
        ports.append(
            {
                "container_port": container_port,
                "protocol": protocol,
                "host_ip": host_ip,
                "host_port": host_port,
            }
        )
    if ports != sorted(
        ports,
        key=lambda entry: (
            entry["container_port"], entry["protocol"], entry["host_ip"], entry["host_port"]
        ),
    ):
        raise _error(code)

    return {
        "role": expected_role,
        "container_name": _text(item["container_name"], code=code),
        "container_id": container_id,
        "image_id": image_id,
        "repository_digests": list(canonical_digests),
        "started_at": started_at,
        "restart_count": _integer(item["restart_count"], minimum=0, code=code),
        "oom_killed": _boolean(item["oom_killed"], code=code),
        "labels": labels,
        "mounts": mounts,
        "networks": networks,
        "published_ports": ports,
    }


def _validate_collection_schema(value: object) -> list[dict[str, object]]:
    code = "GATE_C_EXECUTION_COLLECTION_SCHEMA_INVALID"
    fields = _tuple_of_mappings(value, code=code)
    canonical: list[dict[str, object]] = []
    names: set[str] = set()
    for field in fields:
        field = _mapping(
            field,
            {"name", "data_type", "is_primary", "auto_id", "description", "parameters"},
            code=code,
        )
        name = _text(field["name"], code=code)
        if name in names:
            raise _error(code)
        names.add(name)
        description = field["description"]
        if type(description) is not str:
            raise _error(code)
        canonical.append(
            {
                "name": name,
                "data_type": _text(field["data_type"], code=code),
                "is_primary": _boolean(field["is_primary"], code=code),
                "auto_id": _boolean(field["auto_id"], code=code),
                "description": description,
                "parameters": _validate_name_value_list(field["parameters"], code=code),
            }
        )
    return canonical


def _validate_data_plane(value: object, expected_role: str) -> dict[str, object]:
    code = "GATE_C_EXECUTION_DATA_PLANE_INVALID"
    fields = {
        "role", "collection_name", "database_name", "collection_schema",
        "collection_schema_sha256", "metric", "dimensions", "entity_count",
        "index_name", "index_type", "index_metric", "index_parameters",
        "index_identity_sha256",
    }
    item = _mapping(value, fields, code=code)
    if item["role"] != expected_role:
        raise _error(code)
    schema = _validate_collection_schema(item["collection_schema"])
    schema_payload = {
        "schema_version": "exp012-gate-c-collection-schema-v1",
        "collection_name": _text(item["collection_name"], code=code),
        "database_name": _text(item["database_name"], code=code),
        "fields": schema,
    }
    schema_sha = strict_canonical_digest(_COLLECTION_SCHEMA_DOMAIN, schema_payload)
    if item["collection_schema_sha256"] != schema_sha:
        raise _error(code)
    parameters = _validate_name_value_list(item["index_parameters"], code=code)
    index_payload = {
        "schema_version": "exp012-gate-c-index-identity-v1",
        "collection_name": schema_payload["collection_name"],
        "database_name": schema_payload["database_name"],
        "collection_schema_sha256": schema_sha,
        "index_name": _text(item["index_name"], code=code),
        "index_type": _text(item["index_type"], code=code),
        "index_metric": _text(item["index_metric"], code=code),
        "index_parameters": parameters,
    }
    index_sha = strict_canonical_digest(_INDEX_IDENTITY_DOMAIN, index_payload)
    if item["index_identity_sha256"] != index_sha:
        raise _error(code)
    return {
        "role": expected_role,
        "collection_name": schema_payload["collection_name"],
        "database_name": schema_payload["database_name"],
        "collection_schema": schema,
        "collection_schema_sha256": schema_sha,
        "metric": _text(item["metric"], code=code),
        "dimensions": _integer(item["dimensions"], minimum=1, code=code),
        "entity_count": _integer(item["entity_count"], minimum=0, code=code),
        "index_name": index_payload["index_name"],
        "index_type": index_payload["index_type"],
        "index_metric": index_payload["index_metric"],
        "index_parameters": parameters,
        "index_identity_sha256": index_sha,
    }


def build_gate_c_execution_environment_identity(
    *, endpoint: Mapping[str, object], containers: Sequence[Mapping[str, object]],
    data_plane: Sequence[Mapping[str, object]], expected_entity_count: int,
) -> GateCExecutionEnvironmentIdentity:
    """Build stable identity from observed runtime/data-plane facts only."""

    expected_count = _integer(
        expected_entity_count,
        minimum=1,
        code="GATE_C_EXECUTION_ENTITY_COUNT_INVALID",
    )
    if type(containers) not in (list, tuple) or len(containers) != 3:
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_IDENTITY_INVALID")
    if type(data_plane) not in (list, tuple) or len(data_plane) != 2:
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_IDENTITY_INVALID")
    canonical_containers = [
        _validate_container(value, role)
        for role, value in zip(_CONTAINER_ROLES, containers, strict=True)
    ]
    canonical_data_plane = [
        _validate_data_plane(value, role)
        for role, value in zip(_DATA_PLANE_ROLES, data_plane, strict=True)
    ]
    counts = {item["entity_count"] for item in canonical_data_plane}
    if counts != {expected_count}:
        raise _error("GATE_C_EXECUTION_ENTITY_COUNT_INVALID")
    payload: dict[str, object] = {
        "schema_version": EXECUTION_ENVIRONMENT_IDENTITY_SCHEMA_VERSION,
        "endpoint": _validate_endpoint(endpoint),
        "containers": canonical_containers,
        "data_plane": canonical_data_plane,
    }
    return _make_identity(payload)


def gate_c_execution_environment_identity_document(
    identity: GateCExecutionEnvironmentIdentity,
) -> dict[str, object]:
    if type(identity) is not GateCExecutionEnvironmentIdentity:
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_IDENTITY_INVALID")
    try:
        payload = identity.payload
        rebuilt = build_gate_c_execution_environment_identity(
            endpoint=payload["endpoint"],
            containers=payload["containers"],
            data_plane=payload["data_plane"],
            expected_entity_count=payload["data_plane"][0]["entity_count"],
        )
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        if isinstance(exc, GateCExecutionEnvironmentError):
            raise
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_IDENTITY_INVALID") from exc
    if (
        rebuilt.canonical_payload_bytes != identity.canonical_payload_bytes
        or rebuilt.execution_environment_identity_sha256
        != identity.execution_environment_identity_sha256
    ):
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_IDENTITY_INVALID")
    return {
        "identity_payload": payload,
        "execution_environment_identity_sha256": (
            rebuilt.execution_environment_identity_sha256
        ),
    }


def parse_gate_c_execution_environment_identity_document(
    document: Mapping[str, object],
) -> GateCExecutionEnvironmentIdentity:
    code = "GATE_C_EXECUTION_ENVIRONMENT_IDENTITY_INVALID"
    value = _mapping(
        document,
        {"identity_payload", "execution_environment_identity_sha256"},
        code=code,
    )
    payload = value["identity_payload"]
    if type(payload) is not dict or set(payload) != {
        "schema_version", "endpoint", "containers", "data_plane"
    } or payload["schema_version"] != EXECUTION_ENVIRONMENT_IDENTITY_SCHEMA_VERSION:
        raise _error(code)
    try:
        data_plane = payload["data_plane"]
        if type(data_plane) is not list or not data_plane:
            raise TypeError
        identity = build_gate_c_execution_environment_identity(
            endpoint=payload["endpoint"],
            containers=payload["containers"],
            data_plane=data_plane,
            expected_entity_count=data_plane[0]["entity_count"],
        )
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        if isinstance(exc, GateCExecutionEnvironmentError):
            raise
        raise _error(code) from exc
    if (
        value["execution_environment_identity_sha256"]
        != identity.execution_environment_identity_sha256
        or dict(document) != gate_c_execution_environment_identity_document(identity)
    ):
        raise _error(code)
    return identity


_GOVERNED_FIELDS = {
    "campaign_identity", "scale_contract_sha256", "gate_a_evidence_sha256",
    "source_revision", "environment_manifest_sha256", "data_identity",
    "configuration_identity", "flat_binding_id", "hnsw_binding_id",
    "metric", "dimensions", "expected_entity_count", "served_ef",
    "consistency_level", "flat_collection_name", "hnsw_collection_name",
    "flat_gate_a_binding", "hnsw_gate_a_binding",
}
_OBSERVATION_FIELDS = {
    "observed_at_utc", "container_health", "milvus_healthz",
    "collection_readiness", "index_readiness",
}


_GATE_A_LIVE_COMMON_FIELDS = {
    "collection_name", "index_name", "index_type", "metric_type",
    "row_count", "dimensions", "indexed_rows", "pending_index_rows",
    "index_state", "load_state",
}


def _validate_gate_a_binding_authority(
    value: object, *, role: str, expected_binding_id: str,
) -> dict[str, object]:
    """Reconstruct the frozen Gate-A ID-to-live-metadata authority.

    Gate A v1 treated each binding ID as a governed stable-project identity;
    it did not derive that opaque ID from live metadata.  Its verified evidence
    nevertheless binds the ID to the exact live metadata it observed.  V3 uses
    that preserved record as the strongest truthful compatibility projection
    rather than inventing a replacement digest algorithm.
    """

    code = "GATE_C_EXECUTION_GATE_A_BINDING_INVALID"
    authority = _mapping(value, {"binding_id", "live"}, code=code)
    binding_id = _text(authority["binding_id"], code=code)
    if binding_id != expected_binding_id:
        raise _error(code)
    fields = set(_GATE_A_LIVE_COMMON_FIELDS)
    if role == "hnsw":
        fields.update({"M", "efConstruction"})
    live = _mapping(authority["live"], fields, code=code)
    expected_type = "FLAT" if role == "flat" else "HNSW"
    canonical: dict[str, object] = {
        "collection_name": _text(live["collection_name"], code=code),
        "index_name": _text(live["index_name"], code=code),
        "index_type": _text(live["index_type"], code=code),
        "metric_type": _text(live["metric_type"], code=code),
        "row_count": _integer(live["row_count"], minimum=0, code=code),
        "dimensions": _integer(live["dimensions"], minimum=1, code=code),
        "indexed_rows": _integer(live["indexed_rows"], minimum=0, code=code),
        "pending_index_rows": _integer(
            live["pending_index_rows"], minimum=0, code=code
        ),
        "index_state": _text(live["index_state"], code=code),
        "load_state": _text(live["load_state"], code=code),
    }
    if canonical["index_type"] != expected_type:
        raise _error(code)
    if role == "hnsw":
        canonical["M"] = _integer(live["M"], minimum=1, code=code)
        canonical["efConstruction"] = _integer(
            live["efConstruction"], minimum=1, code=code
        )
    return {"binding_id": binding_id, "live": canonical}


def _validate_governed_bindings(value: object) -> dict[str, object]:
    code = "GATE_C_EXECUTION_GOVERNED_BINDINGS_INVALID"
    item = _mapping(value, _GOVERNED_FIELDS, code=code)
    for name in (
        "scale_contract_sha256", "gate_a_evidence_sha256",
        "environment_manifest_sha256",
    ):
        _sha(item[name], code=code)
    _revision(item["source_revision"], code=code)
    for name in _GOVERNED_FIELDS - {
        "scale_contract_sha256", "gate_a_evidence_sha256", "source_revision",
        "environment_manifest_sha256", "dimensions", "expected_entity_count",
        "served_ef", "flat_gate_a_binding", "hnsw_gate_a_binding",
    }:
        _text(item[name], code=code)
    for name in ("dimensions", "expected_entity_count", "served_ef"):
        _integer(item[name], minimum=1, code=code)
    governed = dict(item)
    governed["flat_gate_a_binding"] = _validate_gate_a_binding_authority(
        item["flat_gate_a_binding"],
        role="flat",
        expected_binding_id=item["flat_binding_id"],
    )
    governed["hnsw_gate_a_binding"] = _validate_gate_a_binding_authority(
        item["hnsw_gate_a_binding"],
        role="hnsw",
        expected_binding_id=item["hnsw_binding_id"],
    )
    return governed


def _index_parameter(data_plane: Mapping[str, object], name: str) -> int:
    code = "GATE_C_EXECUTION_COMPATIBILITY_FAILED"
    parameters = data_plane.get("index_parameters")
    if type(parameters) is not list:
        raise _error(code)
    matches = [item for item in parameters if item.get("name") == name]
    if len(matches) != 1:
        raise _error(code)
    value = matches[0].get("value")
    if type(value) is int:
        return _integer(value, minimum=1, code=code)
    if type(value) is str:
        try:
            parsed = int(value)
        except ValueError as exc:
            raise _error(code) from exc
        if parsed < 1 or str(parsed) != value:
            raise _error(code)
        return parsed
    raise _error(code)


def _gate_a_stable_projection(
    data_plane: Mapping[str, object], *, role: str,
) -> dict[str, object]:
    projection: dict[str, object] = {
        "collection_name": data_plane["collection_name"],
        "index_name": data_plane["index_name"],
        "index_type": data_plane["index_type"],
        "metric_type": data_plane["index_metric"],
        "row_count": data_plane["entity_count"],
        "dimensions": data_plane["dimensions"],
    }
    if role == "hnsw":
        projection["M"] = _index_parameter(data_plane, "M")
        projection["efConstruction"] = _index_parameter(
            data_plane, "efConstruction"
        )
    return projection


def _historical_gate_a_stable_projection(
    authority: Mapping[str, object], *, role: str,
) -> dict[str, object]:
    live = authority["live"]
    fields = {
        "collection_name", "index_name", "index_type", "metric_type",
        "row_count", "dimensions",
    }
    if role == "hnsw":
        fields.update({"M", "efConstruction"})
    return {name: live[name] for name in fields}


def _verify_gate_a_binding_compatibility(
    data_plane: Sequence[Mapping[str, object]],
    governed: Mapping[str, object],
) -> None:
    code = "GATE_C_EXECUTION_COMPATIBILITY_FAILED"
    for index, role in enumerate(_DATA_PLANE_ROLES):
        authority = governed[f"{role}_gate_a_binding"]
        if (
            authority["binding_id"] != governed[f"{role}_binding_id"]
            or _gate_a_stable_projection(data_plane[index], role=role)
            != _historical_gate_a_stable_projection(authority, role=role)
        ):
            raise _error(code)


def _validate_observation_metadata(value: object) -> dict[str, object]:
    code = "GATE_C_EXECUTION_OBSERVATION_METADATA_INVALID"
    item = _mapping(value, _OBSERVATION_FIELDS, code=code)
    observed_at = _text(item["observed_at_utc"], code=code)
    if _RFC3339_UTC.fullmatch(observed_at) is None:
        raise _error(code)
    health = _mapping(
        item["container_health"], set(_CONTAINER_ROLES), code=code
    )
    readiness = _mapping(
        item["collection_readiness"], set(_DATA_PLANE_ROLES), code=code
    )
    indexes = _mapping(item["index_readiness"], set(_DATA_PLANE_ROLES), code=code)
    return {
        "observed_at_utc": observed_at,
        "container_health": {
            role: _boolean(health[role], code=code) for role in _CONTAINER_ROLES
        },
        "milvus_healthz": _boolean(item["milvus_healthz"], code=code),
        "collection_readiness": {
            role: _boolean(readiness[role], code=code) for role in _DATA_PLANE_ROLES
        },
        "index_readiness": {
            role: _boolean(indexes[role], code=code) for role in _DATA_PLANE_ROLES
        },
    }


def build_gate_c_execution_environment_attestation(
    *, identity: GateCExecutionEnvironmentIdentity, execution_source_revision: str,
    governed_bindings: Mapping[str, object], observed_runtime: Mapping[str, object],
    observation_metadata: Mapping[str, object], compatibility_verified: bool,
) -> GateCExecutionEnvironmentAttestation:
    identity_document = gate_c_execution_environment_identity_document(identity)
    governed = _validate_governed_bindings(governed_bindings)
    observed = _mapping(
        observed_runtime,
        {"endpoint", "containers", "data_plane"},
        code="GATE_C_EXECUTION_OBSERVED_RUNTIME_INVALID",
    )
    reconstructed = build_gate_c_execution_environment_identity(
        endpoint=observed["endpoint"],
        containers=observed["containers"],
        data_plane=observed["data_plane"],
        expected_entity_count=governed["expected_entity_count"],
    )
    if (
        reconstructed.execution_environment_identity_sha256
        != identity.execution_environment_identity_sha256
        or reconstructed.canonical_payload_bytes != identity.canonical_payload_bytes
    ):
        raise _error("GATE_C_EXECUTION_OBSERVED_RUNTIME_MISMATCH")
    compatibility = _boolean(
        compatibility_verified,
        code="GATE_C_EXECUTION_COMPATIBILITY_INVALID",
    )
    if not compatibility:
        raise _error("GATE_C_EXECUTION_COMPATIBILITY_FAILED")
    data_plane = identity.payload["data_plane"]
    if (
        data_plane[0]["collection_name"] != governed["flat_collection_name"]
        or data_plane[1]["collection_name"] != governed["hnsw_collection_name"]
        or any(item["metric"] != governed["metric"] for item in data_plane)
        or any(item["dimensions"] != governed["dimensions"] for item in data_plane)
        or any(
            item["entity_count"] != governed["expected_entity_count"]
            for item in data_plane
        )
    ):
        raise _error("GATE_C_EXECUTION_COMPATIBILITY_FAILED")
    _verify_gate_a_binding_compatibility(data_plane, governed)
    payload: dict[str, object] = {
        "schema_version": EXECUTION_ENVIRONMENT_ATTESTATION_SCHEMA_VERSION,
        "execution_environment_identity": identity_document,
        "execution_environment_identity_sha256": (
            identity.execution_environment_identity_sha256
        ),
        "execution_source_revision": _revision(
            execution_source_revision,
            code="GATE_C_EXECUTION_SOURCE_REVISION_INVALID",
        ),
        "governed_bindings": governed,
        "observed_runtime": identity.payload | {},
        "observation_metadata": _validate_observation_metadata(observation_metadata),
        "compatibility_verification": {"status": "PASS"},
    }
    payload["observed_runtime"].pop("schema_version")
    return _make_attestation(payload)


def gate_c_execution_environment_attestation_document(
    attestation: GateCExecutionEnvironmentAttestation,
) -> dict[str, object]:
    if type(attestation) is not GateCExecutionEnvironmentAttestation:
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_ATTESTATION_INVALID")
    parsed = parse_gate_c_execution_environment_attestation_document(
        {
            "attestation_payload": attestation.payload,
            "execution_environment_attestation_sha256": (
                attestation.execution_environment_attestation_sha256
            ),
        },
        _skip_round_trip=True,
    )
    return {
        "attestation_payload": parsed.payload,
        "execution_environment_attestation_sha256": (
            parsed.execution_environment_attestation_sha256
        ),
    }


def parse_gate_c_execution_environment_attestation_document(
    document: Mapping[str, object], *, _skip_round_trip: bool = False,
) -> GateCExecutionEnvironmentAttestation:
    code = "GATE_C_EXECUTION_ENVIRONMENT_ATTESTATION_INVALID"
    value = _mapping(
        document,
        {"attestation_payload", "execution_environment_attestation_sha256"},
        code=code,
    )
    payload = value["attestation_payload"]
    expected = {
        "schema_version", "execution_environment_identity",
        "execution_environment_identity_sha256", "execution_source_revision",
        "governed_bindings", "observed_runtime", "observation_metadata",
        "compatibility_verification",
    }
    if type(payload) is not dict or set(payload) != expected or payload[
        "schema_version"
    ] != EXECUTION_ENVIRONMENT_ATTESTATION_SCHEMA_VERSION:
        raise _error(code)
    try:
        identity = parse_gate_c_execution_environment_identity_document(
            payload["execution_environment_identity"]
        )
        compatibility = _mapping(
            payload["compatibility_verification"], {"status"}, code=code
        )
        if compatibility["status"] != "PASS":
            raise _error(code)
        rebuilt = build_gate_c_execution_environment_attestation(
            identity=identity,
            execution_source_revision=payload["execution_source_revision"],
            governed_bindings=payload["governed_bindings"],
            observed_runtime=payload["observed_runtime"],
            observation_metadata=payload["observation_metadata"],
            compatibility_verified=True,
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, GateCExecutionEnvironmentError):
            raise
        raise _error(code) from exc
    if (
        payload["execution_environment_identity_sha256"]
        != identity.execution_environment_identity_sha256
        or value["execution_environment_attestation_sha256"]
        != rebuilt.execution_environment_attestation_sha256
    ):
        raise _error(code)
    if not _skip_round_trip and dict(document) != {
        "attestation_payload": rebuilt.payload,
        "execution_environment_attestation_sha256": (
            rebuilt.execution_environment_attestation_sha256
        ),
    }:
        raise _error(code)
    return rebuilt


def verify_gate_c_execution_environment_eligibility(
    attestation: GateCExecutionEnvironmentAttestation,
) -> GateCExecutionEnvironmentIdentity:
    document = gate_c_execution_environment_attestation_document(attestation)
    verified = parse_gate_c_execution_environment_attestation_document(document)
    metadata = verified.payload["observation_metadata"]
    predicates = (
        *metadata["container_health"].values(),
        metadata["milvus_healthz"],
        *metadata["collection_readiness"].values(),
        *metadata["index_readiness"].values(),
    )
    if any(value is not True for value in predicates):
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_NOT_READY")
    return parse_gate_c_execution_environment_identity_document(
        verified.payload["execution_environment_identity"]
    )


class GateCMetadataReader(Protocol):
    """Metadata-only Milvus surface; deliberately has no search method."""

    def describe_collection(self, *, collection_name: str) -> object: ...
    def describe_index(self, *, collection_name: str, index_name: str) -> object: ...
    def get_collection_stats(self, *, collection_name: str) -> object: ...
    def get_load_state(self, *, collection_name: str) -> object: ...


class DockerExecutionMetadataInspector:
    """Read-only Docker container/image metadata transport over the local socket."""

    def __init__(
        self,
        socket_path: str | os.PathLike[str] = "/var/run/docker.sock",
        *,
        timeout_seconds: float = 2.0,
    ) -> None:
        self._socket_path = Path(socket_path)
        if (
            type(timeout_seconds) is not float
            or not (0.0 < timeout_seconds <= 30.0)
        ):
            raise _error("GATE_C_EXECUTION_DOCKER_INSPECTOR_INVALID")
        self._timeout_seconds = timeout_seconds

    def _inspect(self, resource: str, identity: str) -> dict[str, object]:
        target = quote(identity, safe="")
        request = (
            f"GET /{resource}/{target}/json HTTP/1.1\r\n"
            "Host: docker\r\n"
            "Connection: close\r\n"
            "Accept: application/json\r\n\r\n"
        ).encode("ascii")
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.settimeout(self._timeout_seconds)
            client.connect(os.fspath(self._socket_path))
            client.sendall(request)
            response = http.client.HTTPResponse(client)
            response.begin()
            payload = response.read()
            if response.status != 200:
                raise OSError(f"docker inspect status {response.status}")
        finally:
            client.close()
        try:
            document = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _error("GATE_C_EXECUTION_DOCKER_METADATA_INVALID") from exc
        if type(document) is not dict:
            raise _error("GATE_C_EXECUTION_DOCKER_METADATA_INVALID")
        return document

    def inspect_container(self, name: str) -> dict[str, object]:
        return self._inspect("containers", _text(
            name, code="GATE_C_EXECUTION_DOCKER_INSPECTOR_INVALID"
        ))

    def inspect_image(self, image_id: str) -> dict[str, object]:
        return self._inspect("images", _text(
            image_id, code="GATE_C_EXECUTION_DOCKER_INSPECTOR_INVALID"
        ))


@dataclass(frozen=True, slots=True)
class GateCExecutionEnvironmentObservationSpec:
    milvus_uri: str
    database_name: str
    etcd_container: str
    minio_container: str
    milvus_container: str
    flat_collection_name: str
    hnsw_collection_name: str
    index_name: str
    metric: str
    dimensions: int
    expected_entity_count: int

    def __post_init__(self) -> None:
        for value in (
            self.milvus_uri, self.database_name, self.etcd_container,
            self.minio_container, self.milvus_container,
            self.flat_collection_name, self.hnsw_collection_name,
            self.index_name, self.metric,
        ):
            _text(value, code="GATE_C_EXECUTION_OBSERVATION_SPEC_INVALID")
        _integer(self.dimensions, minimum=1, code="GATE_C_EXECUTION_OBSERVATION_SPEC_INVALID")
        _integer(
            self.expected_entity_count,
            minimum=1,
            code="GATE_C_EXECUTION_OBSERVATION_SPEC_INVALID",
        )


def _normalized_endpoint(uri: str) -> dict[str, object]:
    parsed = urlsplit(uri)
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https", "grpc", "grpcs"}:
        raise _error("GATE_C_EXECUTION_ENDPOINT_INVALID")
    if parsed.username is not None or parsed.password is not None or parsed.path not in {"", "/"}:
        raise _error("GATE_C_EXECUTION_ENDPOINT_INVALID")
    if parsed.hostname is None or parsed.port is None:
        raise _error("GATE_C_EXECUTION_ENDPOINT_INVALID")
    return {
        "scheme": scheme,
        "host": parsed.hostname.lower(),
        "port": parsed.port,
        "transport_security": "TLS" if scheme in {"https", "grpcs"} else "PLAINTEXT",
    }


def _canonical_scalar(value: object, *, code: str) -> str | int:
    if type(value) is int:
        return value
    if type(value) is str:
        return _text(value, code=code)
    raise _error(code)


def _normalize_fields(description: Mapping[str, object]) -> list[dict[str, object]]:
    raw_fields = description.get("fields")
    if type(raw_fields) not in (list, tuple):
        raise _error("GATE_C_EXECUTION_COLLECTION_SCHEMA_INVALID")
    fields: list[dict[str, object]] = []
    for raw in raw_fields:
        if not isinstance(raw, Mapping):
            raise _error("GATE_C_EXECUTION_COLLECTION_SCHEMA_INVALID")
        params = raw.get("params", {})
        if not isinstance(params, Mapping):
            raise _error("GATE_C_EXECUTION_COLLECTION_SCHEMA_INVALID")
        fields.append(
            {
                "name": str(raw.get("name", "")),
                "data_type": str(raw.get("type", raw.get("data_type", ""))),
                "is_primary": raw.get("is_primary", False),
                "auto_id": raw.get("auto_id", False),
                "description": str(raw.get("description", "")),
                "parameters": [
                    {"name": str(name), "value": _canonical_scalar(value, code="GATE_C_EXECUTION_COLLECTION_SCHEMA_INVALID")}
                    for name, value in sorted(params.items())
                ],
            }
        )
    return fields


def _normalize_index_parameters(index: Mapping[str, object]) -> list[dict[str, object]]:
    excluded = {
        "collection_name", "field_name", "index_name", "index_type",
        "metric_type", "state", "indexed_rows", "pending_index_rows",
        "total_rows",
    }
    nested = index.get("params")
    raw = dict(nested) if isinstance(nested, Mapping) else {}
    for name, value in index.items():
        if name not in excluded and name != "params" and type(value) in (str, int) and type(value) is not bool:
            raw[name] = value
    return [
        {"name": str(name), "value": _canonical_scalar(value, code="GATE_C_EXECUTION_DATA_PLANE_INVALID")}
        for name, value in sorted(raw.items())
    ]


def _normalize_container(role: str, name: str, document: Mapping[str, object]) -> tuple[dict[str, object], bool]:
    code = "GATE_C_EXECUTION_CONTAINER_INVALID"
    state = document.get("State")
    config = document.get("Config")
    network_settings = document.get("NetworkSettings")
    if not isinstance(state, Mapping) or not isinstance(config, Mapping) or not isinstance(network_settings, Mapping):
        raise _error(code)
    labels = config.get("Labels") or {}
    if not isinstance(labels, Mapping):
        raise _error(code)
    mounts_raw = document.get("Mounts") or []
    if type(mounts_raw) is not list:
        raise _error(code)
    mounts = sorted(
        (
            {
                "type": str(item.get("Type", "")),
                "name": str(item.get("Name", "")),
                "source": str(item.get("Source", "")),
                "destination": str(item.get("Destination", "")),
                "mode": str(item.get("Mode", "")),
                "read_only": not bool(item.get("RW", False)),
            }
            for item in mounts_raw
            if isinstance(item, Mapping)
        ),
        key=lambda item: (item["destination"], item["source"]),
    )
    networks_raw = network_settings.get("Networks") or {}
    if not isinstance(networks_raw, Mapping):
        raise _error(code)
    networks = []
    for network_name, raw in sorted(networks_raw.items()):
        if not isinstance(raw, Mapping):
            raise _error(code)
        aliases = raw.get("Aliases") or []
        if type(aliases) is not list:
            raise _error(code)
        networks.append(
            {
                "network_name": str(network_name),
                "network_id": str(raw.get("NetworkID", "")),
                "endpoint_id": str(raw.get("EndpointID", "")),
                "gateway": str(raw.get("Gateway", "")),
                "ip_address": str(raw.get("IPAddress", "")),
                "ip_prefix_len": int(raw.get("IPPrefixLen", 0)),
                "global_ipv6_address": str(raw.get("GlobalIPv6Address", "")),
                "global_ipv6_prefix_len": int(raw.get("GlobalIPv6PrefixLen", 0)),
                "mac_address": str(raw.get("MacAddress", "")),
                "aliases": sorted(set(str(value) for value in aliases)),
            }
        )
    ports_raw = network_settings.get("Ports") or {}
    if not isinstance(ports_raw, Mapping):
        raise _error(code)
    ports = []
    for container_key, bindings in ports_raw.items():
        port_text, separator, protocol = str(container_key).partition("/")
        if not separator or bindings is None:
            continue
        if type(bindings) is not list:
            raise _error(code)
        for binding in bindings:
            if not isinstance(binding, Mapping):
                raise _error(code)
            ports.append(
                {
                    "container_port": int(port_text),
                    "protocol": protocol,
                    "host_ip": str(binding.get("HostIp", "")),
                    "host_port": int(binding.get("HostPort", 0)),
                }
            )
    ports.sort(key=lambda item: (item["container_port"], item["protocol"], item["host_ip"], item["host_port"]))
    health = state.get("Health")
    health_status = health.get("Status") if isinstance(health, Mapping) else None
    healthy = (
        state.get("Status") == "running"
        and state.get("OOMKilled") is False
        and health_status in {None, "healthy"}
    )
    repository_digests_raw = document.get("RepoDigests") or []
    if type(repository_digests_raw) is not list or any(
        type(value) is not str for value in repository_digests_raw
    ):
        raise _error(code)
    return (
        {
            "role": role,
            "container_name": name,
            "container_id": document.get("Id"),
            "image_id": document.get("Image"),
            "repository_digests": sorted(set(repository_digests_raw)),
            "started_at": state.get("StartedAt"),
            "restart_count": document.get("RestartCount"),
            "oom_killed": state.get("OOMKilled"),
            "labels": [
                {"name": str(key), "value": str(value)}
                for key, value in sorted(labels.items())
                if key in _APPROVED_LABELS
            ],
            "mounts": mounts,
            "networks": networks,
            "published_ports": ports,
        },
        healthy,
    )


def _normalize_data_plane(
    role: str, name: str, spec: GateCExecutionEnvironmentObservationSpec,
    reader: GateCMetadataReader,
) -> tuple[dict[str, object], bool, bool]:
    try:
        description = reader.describe_collection(collection_name=name)
        index = reader.describe_index(collection_name=name, index_name=spec.index_name)
        stats = reader.get_collection_stats(collection_name=name)
        load = reader.get_load_state(collection_name=name)
    except Exception as exc:
        raise _error("GATE_C_EXECUTION_METADATA_UNAVAILABLE") from exc
    if not all(isinstance(value, Mapping) for value in (description, index, stats, load)):
        raise _error("GATE_C_EXECUTION_METADATA_INVALID")
    if description.get("collection_name", name) != name:
        raise _error("GATE_C_EXECUTION_COMPATIBILITY_FAILED")
    observed_dimensions: int | None = None
    raw_fields = description.get("fields")
    if isinstance(raw_fields, (list, tuple)):
        for field in raw_fields:
            if not isinstance(field, Mapping):
                continue
            parameters = field.get("params")
            if (
                isinstance(parameters, Mapping)
                and type(parameters.get("dim")) is int
            ):
                observed_dimensions = parameters["dim"]
                break
    if observed_dimensions != spec.dimensions:
        raise _error("GATE_C_EXECUTION_COMPATIBILITY_FAILED")
    observed_index_name = index.get("index_name", spec.index_name)
    if observed_index_name != spec.index_name:
        raise _error("GATE_C_EXECUTION_COMPATIBILITY_FAILED")
    fields = _normalize_fields(description)
    schema_payload = {
        "schema_version": "exp012-gate-c-collection-schema-v1",
        "collection_name": name,
        "database_name": spec.database_name,
        "fields": fields,
    }
    schema_sha = strict_canonical_digest(_COLLECTION_SCHEMA_DOMAIN, schema_payload)
    params = _normalize_index_parameters(index)
    index_type = str(index.get("index_type", ""))
    index_metric = str(index.get("metric_type", ""))
    index_payload = {
        "schema_version": "exp012-gate-c-index-identity-v1",
        "collection_name": name,
        "database_name": spec.database_name,
        "collection_schema_sha256": schema_sha,
        "index_name": observed_index_name,
        "index_type": index_type,
        "index_metric": index_metric,
        "index_parameters": params,
    }
    state = load.get("state")
    load_ready = state == "Loaded" or str(state) == "Loaded"
    index_ready = (
        index.get("state") == "Finished"
        and index.get("pending_index_rows") == 0
        and index.get("indexed_rows") == spec.expected_entity_count
    )
    return (
        {
            "role": role,
            "collection_name": name,
            "database_name": spec.database_name,
            "collection_schema": fields,
            "collection_schema_sha256": schema_sha,
            "metric": spec.metric,
            "dimensions": observed_dimensions,
            "entity_count": stats.get("row_count"),
            "index_name": observed_index_name,
            "index_type": index_type,
            "index_metric": index_metric,
            "index_parameters": params,
            "index_identity_sha256": strict_canonical_digest(_INDEX_IDENTITY_DOMAIN, index_payload),
        },
        load_ready,
        index_ready,
    )


def observe_gate_c_execution_environment(
    spec: GateCExecutionEnvironmentObservationSpec,
    *, metadata_reader: GateCMetadataReader,
    container_inspector: Callable[[str], object],
    image_inspector: Callable[[str], object],
    milvus_healthz_probe: Callable[[], bool],
    execution_source_revision: str,
    governed_bindings: Mapping[str, object],
    clock: Callable[[], datetime] | None = None,
) -> GateCExecutionEnvironmentAttestation:
    """Perform one metadata-only observation and return a verified attestation."""

    if type(spec) is not GateCExecutionEnvironmentObservationSpec:
        raise _error("GATE_C_EXECUTION_OBSERVATION_SPEC_INVALID")
    containers = []
    health: dict[str, bool] = {}
    for role, name in zip(
        _CONTAINER_ROLES,
        (spec.etcd_container, spec.minio_container, spec.milvus_container),
        strict=True,
    ):
        try:
            raw = container_inspector(name)
        except Exception as exc:
            raise _error("GATE_C_EXECUTION_CONTAINER_UNAVAILABLE") from exc
        if not isinstance(raw, Mapping):
            raise _error("GATE_C_EXECUTION_CONTAINER_INVALID")
        image_id = raw.get("Image")
        if type(image_id) is not str or not image_id:
            raise _error("GATE_C_EXECUTION_CONTAINER_INVALID")
        try:
            image = image_inspector(image_id)
        except Exception as exc:
            raise _error("GATE_C_EXECUTION_IMAGE_UNAVAILABLE") from exc
        if not isinstance(image, Mapping):
            raise _error("GATE_C_EXECUTION_IMAGE_INVALID")
        observed = dict(raw)
        observed["RepoDigests"] = image.get("RepoDigests")
        normalized, ready = _normalize_container(role, name, observed)
        containers.append(normalized)
        health[role] = ready
    data_plane = []
    loaded: dict[str, bool] = {}
    index_ready: dict[str, bool] = {}
    for role, name in zip(
        _DATA_PLANE_ROLES,
        (spec.flat_collection_name, spec.hnsw_collection_name),
        strict=True,
    ):
        normalized, is_loaded, is_index_ready = _normalize_data_plane(
            role, name, spec, metadata_reader
        )
        data_plane.append(normalized)
        loaded[role] = is_loaded
        index_ready[role] = is_index_ready
    identity = build_gate_c_execution_environment_identity(
        endpoint=_normalized_endpoint(spec.milvus_uri),
        containers=containers,
        data_plane=data_plane,
        expected_entity_count=spec.expected_entity_count,
    )
    now = (clock or (lambda: datetime.now(UTC)))()
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise _error("GATE_C_EXECUTION_OBSERVATION_TIME_INVALID")
    metadata = {
        "observed_at_utc": now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "container_health": health,
        "milvus_healthz": _boolean(
            milvus_healthz_probe(), code="GATE_C_EXECUTION_OBSERVATION_METADATA_INVALID"
        ),
        "collection_readiness": loaded,
        "index_readiness": index_ready,
    }
    return build_gate_c_execution_environment_attestation(
        identity=identity,
        execution_source_revision=execution_source_revision,
        governed_bindings=governed_bindings,
        observed_runtime={
            "endpoint": identity.payload["endpoint"],
            "containers": identity.payload["containers"],
            "data_plane": identity.payload["data_plane"],
        },
        observation_metadata=metadata,
        compatibility_verified=True,
    )
