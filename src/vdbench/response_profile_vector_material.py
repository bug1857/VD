"""Supplemental ``response-profile-vector-material-v1`` artifact + governed
loaders that reconstruct ``ResponseProfileRunBinding`` and
``ResponseProfileOracleManifest`` from their canonical documents.

Why this module exists
======================
The governed canonical documents of the response-profile track are, by
deliberate design, digest-carrying rather than raw-material-carrying:

* ``response_profile_run_binding_document`` records only top-level digests
  (``workload_manifest_sha256``/``warmup_role_manifest_sha256``/
  ``replay_schedule_sha256``) -- never the population/warm-up member
  structure.
* ``calibration_population_document`` records only ordered digest lists
  (``ordered_vector_sha256`` etc.) -- never member payloads.
* ``role_manifest_document`` (the one document that *does* carry full member
  payloads: query id, source namespace, query-payload semantics) records each
  member's ``vector_sha256`` **but never the raw
  ``QueryVectorIdentity.canonical_vector_bytes``**, per the evidence module's
  own stated design ("Digests ... are not signatures and do not authenticate
  an external raw artifact").
* ``oracle_manifest_document`` records digest-bound oracle records only.

So neither type is reconstructable from its own canonical document alone. The
missing material is exactly (a) the raw canonical vector bytes for every
calibration and warm-up member, and (b) the full member-level structure that
lives in the two ``role_manifest_document`` values but not in the run-binding
or oracle documents.

This module supplies that missing material as ONE additive, versioned,
supplemental artifact and NOTHING more. It does not modify or reinterpret the
meaning of any existing canonical document: it *transports* the two existing
``role_manifest_document`` values verbatim alongside a separate raw-vector
section, and it never writes raw vectors into any existing canonical document.

Authority
=========
The supplemental artifact is fully verified but **non-authorizing**. The
existing run-binding and oracle-manifest digests remain the sole authority:
every loader here reconstructs the governed object through the existing
contract factories and then requires the reconstruction's own canonical
document to be **byte-identical** to the authoritative input document. A
vector-material bundle for a different run (or a tampered one) reproduces a
different canonical document and is therefore rejected -- the material can
never substitute for, or override, the governed digests.

No policy, admission, grant, activation, routing, freshness, or Milvus
dependency is imported here; this module performs pure offline reconstruction.
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass

import numpy as np

from .artifacts import canonical_json_bytes
from .config import IndexTrack, Metric, SearchConfiguration
from .response_profile_evidence import (
    CalibrationPopulationManifest,
    QueryVectorIdentity,
    ResponseProfileCell,
    ResponseProfileEvidenceContractError,
    ResponseProfileRoleKind,
    ResponseProfileRoleManifest,
    ResponseProfileSourceKind,
    build_artifact_source_namespace,
    build_calibration_population_manifest,
    build_canonical_query_identity,
    build_live_stream_source_namespace,
    build_query_vector_identity,
    build_response_profile_cell,
    build_response_profile_query_payload,
    build_response_profile_replay_schedule,
    build_response_profile_role,
    build_response_profile_role_manifest,
    build_response_profile_role_member,
    role_manifest_document,
    source_namespace_document,
)
from .response_profile_lifecycle import (
    ResponseProfileRunBinding,
    build_response_profile_run_binding,
    response_profile_run_binding_document,
)
from .response_profile_semantic import (
    ResponseProfileOracleManifest,
    ResponseProfileSemanticError,
    build_response_profile_oracle_manifest,
    build_response_profile_oracle_record,
    oracle_manifest_document,
)

__all__ = [
    "VECTOR_MATERIAL_SCHEMA_VERSION",
    "ResponseProfileVectorMaterialError",
    "VerifiedResponseProfileVectorMaterial",
    "load_response_profile_vector_material",
    "response_profile_oracle_manifest_from_document",
    "response_profile_run_binding_from_document",
    "response_profile_vector_material_document",
]


VECTOR_MATERIAL_SCHEMA_VERSION = "response-profile-vector-material-v1"
VECTOR_MATERIAL_HASH_DOMAIN = b"VD::RESPONSE_PROFILE_VECTOR_MATERIAL::V1\x00"

_CALIBRATION_ROLE = ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION
_WARMUP_ROLE = ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP

_MATERIAL_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "cell_metric",
        "cell_threshold_stratum",
        "calibration_role_manifest_document",
        "warmup_role_manifest_document",
        "vectors",
        "vector_material_sha256",
    }
)
_VECTOR_ENTRY_KEYS = frozenset(
    {
        "role",
        "canonical_order_index",
        "dimensions",
        "vector_sha256",
        "canonical_vector_bytes_base64",
    }
)


class ResponseProfileVectorMaterialError(ValueError):
    """Fail-closed error for any vector-material or reconstruction defect."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileVectorMaterialError:
    return ResponseProfileVectorMaterialError(message, code=code)


def _material_digest(payload: dict[str, object]) -> str:
    return hashlib.sha256(
        VECTOR_MATERIAL_HASH_DOMAIN + canonical_json_bytes(payload)
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class VerifiedResponseProfileVectorMaterial:
    """The fully verified, non-authorizing reconstruction inputs a vector
    material supplies. Every field here has already been reconstructed through
    the existing contract factories and cross-checked for byte-exact canonical
    round-trip; the objects are safe to hand to the run-binding/oracle loaders.
    """

    cell: ResponseProfileCell
    calibration_role_manifest: ResponseProfileRoleManifest
    warmup_role_manifest: ResponseProfileRoleManifest
    population: CalibrationPopulationManifest


# ---------------------------------------------------------------------------
# Building (fixtures / a future producer emit material through this path)
# ---------------------------------------------------------------------------


def _vector_entries(
    role: ResponseProfileRoleKind, manifest: ResponseProfileRoleManifest
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for index, member in enumerate(manifest.members):
        vector = member.vector_identity
        entries.append(
            {
                "role": role.value,
                "canonical_order_index": index,
                "dimensions": vector.dimensions,
                "vector_sha256": vector.vector_sha256,
                "canonical_vector_bytes_base64": base64.b64encode(
                    vector.canonical_vector_bytes
                ).decode("ascii"),
            }
        )
    return entries


def response_profile_vector_material_document(
    run_binding: ResponseProfileRunBinding,
) -> dict[str, object]:
    """Emit the supplemental ``response-profile-vector-material-v1`` document
    for one already-governed ``ResponseProfileRunBinding``.

    The two ``role_manifest_document`` values are embedded verbatim (their
    meaning is not changed); the raw canonical vector bytes are carried only in
    the separate ``vectors`` section, never inside those documents.
    """

    if type(run_binding) is not ResponseProfileRunBinding:
        raise _error("VECTOR_MATERIAL_INVALID", "run binding must be concrete")
    population = run_binding.population
    calibration_manifest = population.calibration_role_manifest
    warmup_manifest = run_binding.warmup_role_manifest
    document: dict[str, object] = {
        "schema_version": VECTOR_MATERIAL_SCHEMA_VERSION,
        "cell_metric": population.cell.metric.value,
        "cell_threshold_stratum": population.cell.threshold_stratum,
        "calibration_role_manifest_document": role_manifest_document(
            calibration_manifest
        ),
        "warmup_role_manifest_document": role_manifest_document(warmup_manifest),
        "vectors": [
            *_vector_entries(_CALIBRATION_ROLE, calibration_manifest),
            *_vector_entries(_WARMUP_ROLE, warmup_manifest),
        ],
    }
    document["vector_material_sha256"] = _material_digest(document)
    return document


# ---------------------------------------------------------------------------
# Loading / strict verification
# ---------------------------------------------------------------------------


def _require_exact_int(value: object, *, field: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise _error(
            "VECTOR_MATERIAL_INVALID", f"{field} must be an integer >= {minimum}"
        )
    return value


def _vector_identity_from_entry(entry: object) -> tuple[str, int, QueryVectorIdentity]:
    if type(entry) is not dict or frozenset(entry) != _VECTOR_ENTRY_KEYS:
        raise _error("VECTOR_MATERIAL_INVALID", "vector entry fields differ")
    role = entry["role"]
    if role not in (_CALIBRATION_ROLE.value, _WARMUP_ROLE.value):
        raise _error("VECTOR_ROLE_INVALID", "vector entry role is not a governed role")
    index = _require_exact_int(entry["canonical_order_index"], field="canonical_order_index", minimum=0)
    dimensions = _require_exact_int(entry["dimensions"], field="dimensions", minimum=1)
    declared_digest = entry["vector_sha256"]
    encoded = entry["canonical_vector_bytes_base64"]
    if not isinstance(declared_digest, str) or not isinstance(encoded, str):
        raise _error("VECTOR_MATERIAL_INVALID", "vector entry digest/bytes must be strings")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError) as exc:
        raise _error("VECTOR_BYTES_MALFORMED", "vector bytes are not valid base64") from exc
    if len(raw) != dimensions * 4:
        raise _error("VECTOR_DIMENSIONS_INVALID", "vector byte length does not match dimensions")
    array = np.frombuffer(raw, dtype="<f4")
    try:
        vector = build_query_vector_identity(np.ascontiguousarray(array, dtype="<f4"))
    except ResponseProfileEvidenceContractError as exc:
        raise _error("VECTOR_BYTES_MALFORMED", "vector bytes are not a valid finite vector") from exc
    if vector.dimensions != dimensions:
        raise _error("VECTOR_DIMENSIONS_INVALID", "recomputed vector dimensions differ")
    if vector.vector_sha256 != declared_digest:
        raise _error("VECTOR_DIGEST_MISMATCH", "recomputed vector digest differs from the declared digest")
    return role, index, vector


def _vectors_by_role(vectors: object) -> dict[str, dict[int, QueryVectorIdentity]]:
    if type(vectors) is not list or not vectors:
        raise _error("VECTOR_MATERIAL_INVALID", "vectors must be a non-empty list")
    grouped: dict[str, dict[int, QueryVectorIdentity]] = {
        _CALIBRATION_ROLE.value: {},
        _WARMUP_ROLE.value: {},
    }
    seen_digests: dict[str, set[str]] = {
        _CALIBRATION_ROLE.value: set(),
        _WARMUP_ROLE.value: set(),
    }
    for entry in vectors:
        role, index, vector = _vector_identity_from_entry(entry)
        by_index = grouped[role]
        if index in by_index:
            raise _error("VECTOR_DUPLICATE", "duplicate (role, canonical_order_index) vector entry")
        if vector.vector_sha256 in seen_digests[role]:
            raise _error("VECTOR_DUPLICATE", "duplicate vector digest within a role")
        by_index[index] = vector
        seen_digests[role].add(vector.vector_sha256)
    return grouped


def _reconstruct_source_namespace(document: object):
    if type(document) is not dict or frozenset(document) != {
        "source_namespace_payload",
        "source_namespace_sha256",
    }:
        raise _error("SOURCE_NAMESPACE_INVALID", "source namespace document fields differ")
    payload = document["source_namespace_payload"]
    if type(payload) is not dict:
        raise _error("SOURCE_NAMESPACE_INVALID", "source namespace payload is not an object")
    try:
        kind = ResponseProfileSourceKind(payload["source_kind"])
    except (KeyError, ValueError) as exc:
        raise _error("SOURCE_NAMESPACE_INVALID", "source namespace kind is invalid") from exc
    try:
        if kind is ResponseProfileSourceKind.ARTIFACT:
            reconstructed = build_artifact_source_namespace(
                dataset_id=payload["dataset_id"],
                dataset_version=payload["dataset_version"],
                generation_manifest_sha256=payload["generation_manifest_sha256"],
            )
        else:
            reconstructed = build_live_stream_source_namespace(
                stream_id=payload["stream_id"],
                data_identity=payload["data_identity"],
                source_workload_manifest_sha256=payload["source_workload_manifest_sha256"],
            )
    except (KeyError, TypeError, ResponseProfileEvidenceContractError) as exc:
        raise _error("SOURCE_NAMESPACE_INVALID", "source namespace failed reconstruction") from exc
    if source_namespace_document(reconstructed) != document:
        raise _error(
            "SOURCE_NAMESPACE_INVALID",
            "source namespace is not byte-identical to its canonical reconstruction",
        )
    return reconstructed


def _reconstruct_role_manifest(
    document: object,
    *,
    vectors_by_index: dict[int, QueryVectorIdentity],
    expected_role_kind: ResponseProfileRoleKind,
) -> ResponseProfileRoleManifest:
    if type(document) is not dict or frozenset(document) != {
        "role_manifest_payload",
        "role_manifest_sha256",
    }:
        raise _error("ROLE_MANIFEST_DOCUMENT_INVALID", "role manifest document fields differ")
    payload = document["role_manifest_payload"]
    if type(payload) is not dict or frozenset(payload) != {
        "schema_version",
        "role",
        "source_namespaces",
        "member_count",
        "members",
    }:
        raise _error("ROLE_MANIFEST_DOCUMENT_INVALID", "role manifest payload fields differ")
    role_document = payload["role"]
    if type(role_document) is not dict or frozenset(role_document) != {
        "schema_version",
        "kind",
        "prospective_segment_index",
    }:
        raise _error("ROLE_MANIFEST_DOCUMENT_INVALID", "role fields differ")
    try:
        kind = ResponseProfileRoleKind(role_document["kind"])
    except ValueError as exc:
        raise _error("ROLE_KIND_INVALID", "role kind is not a governed role") from exc
    if kind is not expected_role_kind:
        raise _error("ROLE_KIND_INVALID", "role manifest role kind does not match its slot")

    try:
        role = build_response_profile_role(
            kind=kind,
            prospective_segment_index=role_document["prospective_segment_index"],
        )
    except (TypeError, ResponseProfileEvidenceContractError) as exc:
        raise _error("ROLE_KIND_INVALID", "role failed reconstruction") from exc

    source_documents = payload["source_namespaces"]
    if type(source_documents) is not list or not source_documents:
        raise _error("ROLE_MANIFEST_DOCUMENT_INVALID", "source_namespaces must be a non-empty list")
    sources = {}
    for source_document in source_documents:
        reconstructed = _reconstruct_source_namespace(source_document)
        sources[reconstructed.source_namespace_sha256] = reconstructed

    members_payload = payload["members"]
    if type(members_payload) is not list:
        raise _error("ROLE_MANIFEST_DOCUMENT_INVALID", "members must be a list")
    member_count = _require_exact_int(payload["member_count"], field="member_count", minimum=1)
    if len(members_payload) != member_count:
        raise _error("ROLE_MEMBER_COUNT_INVALID", "member_count does not match the member list")
    if set(vectors_by_index) != set(range(member_count)):
        raise _error(
            "VECTOR_SET_MISMATCH",
            "vector material does not supply exactly one vector per member position",
        )

    members = []
    for position, member_payload in enumerate(members_payload):
        if type(member_payload) is not dict or frozenset(member_payload) != {
            "canonical_order_index",
            "source_namespace_sha256",
            "query_id",
            "query_id_sha256",
            "observation_identity_sha256",
            "vector_sha256",
            "query_payload",
            "query_payload_sha256",
        }:
            raise _error("ROLE_MANIFEST_DOCUMENT_INVALID", "member payload fields differ")
        if member_payload["canonical_order_index"] != position:
            raise _error("VECTOR_ORDER_INVALID", "member canonical order index is out of order")
        source = sources.get(member_payload["source_namespace_sha256"])
        if source is None:
            raise _error("ROLE_MANIFEST_DOCUMENT_INVALID", "member references an unknown source namespace")
        vector = vectors_by_index[position]
        if vector.vector_sha256 != member_payload["vector_sha256"]:
            raise _error(
                "VECTOR_DIGEST_MISMATCH",
                "supplied vector digest differs from the member's recorded vector digest",
            )
        query_payload_document = member_payload["query_payload"]
        if type(query_payload_document) is not dict:
            raise _error("ROLE_MANIFEST_DOCUMENT_INVALID", "member query payload is not an object")
        try:
            query_identity = build_canonical_query_identity(member_payload["query_id"])
            surrogate = SearchConfiguration(
                metric=Metric(query_payload_document["metric"]),
                threshold_label=query_payload_document["threshold_stratum"],
                radius=query_payload_document["radius"],
                index_track=IndexTrack.FLAT,
                ef=None,
                limit=query_payload_document["limit"],
                consistency_level=query_payload_document["consistency_level"],
            )
            payload_identity = build_response_profile_query_payload(
                vector_identity=vector, search_configuration=surrogate
            )
            member = build_response_profile_role_member(
                source_namespace=source,
                query_identity=query_identity,
                vector_identity=vector,
                query_payload_identity=payload_identity,
            )
        except (KeyError, TypeError, ValueError, ResponseProfileEvidenceContractError) as exc:
            if isinstance(exc, ResponseProfileVectorMaterialError):
                raise
            raise _error("ROLE_MEMBER_RECONSTRUCTION_FAILED", "role member failed reconstruction") from exc
        members.append(member)

    try:
        manifest = build_response_profile_role_manifest(role=role, members=tuple(members))
    except ResponseProfileEvidenceContractError as exc:
        raise _error("ROLE_MANIFEST_RECONSTRUCTION_FAILED", "role manifest failed reconstruction") from exc
    if role_manifest_document(manifest) != document:
        raise _error(
            "ROLE_MANIFEST_DOCUMENT_MISMATCH",
            "reconstructed role manifest is not byte-identical to its canonical document",
        )
    return manifest


def load_response_profile_vector_material(
    document: object,
) -> VerifiedResponseProfileVectorMaterial:
    """Strictly verify one ``response-profile-vector-material-v1`` document and
    return the reconstructed, cross-checked, **non-authorizing** inputs.

    Fails closed on: unknown/missing fields, a broken self-digest, a malformed
    or non-finite vector, a wrong-dimensioned vector, a digest mismatch, a
    missing/extra/duplicate/out-of-order vector, a wrong role, or any nested
    document that does not round-trip byte-identically through its own contract
    factory.
    """

    if type(document) is not dict or frozenset(document) != _MATERIAL_TOP_LEVEL_KEYS:
        raise _error("VECTOR_MATERIAL_INVALID", "vector material document fields differ")
    if document["schema_version"] != VECTOR_MATERIAL_SCHEMA_VERSION:
        raise _error("VECTOR_MATERIAL_INVALID", "vector material schema is unsupported")

    recorded_digest = document["vector_material_sha256"]
    if not isinstance(recorded_digest, str):
        raise _error("VECTOR_MATERIAL_INVALID", "vector_material_sha256 must be a string")
    unsigned = {key: value for key, value in document.items() if key != "vector_material_sha256"}
    if _material_digest(unsigned) != recorded_digest:
        raise _error("VECTOR_MATERIAL_DIGEST_MISMATCH", "vector material self-digest is invalid")

    grouped = _vectors_by_role(document["vectors"])
    calibration_manifest = _reconstruct_role_manifest(
        document["calibration_role_manifest_document"],
        vectors_by_index=grouped[_CALIBRATION_ROLE.value],
        expected_role_kind=_CALIBRATION_ROLE,
    )
    warmup_manifest = _reconstruct_role_manifest(
        document["warmup_role_manifest_document"],
        vectors_by_index=grouped[_WARMUP_ROLE.value],
        expected_role_kind=_WARMUP_ROLE,
    )

    try:
        cell = build_response_profile_cell(
            metric=Metric(document["cell_metric"]),
            threshold_stratum=document["cell_threshold_stratum"],
        )
    except (KeyError, TypeError, ValueError, ResponseProfileEvidenceContractError) as exc:
        if isinstance(exc, ResponseProfileVectorMaterialError):
            raise
        raise _error("VECTOR_MATERIAL_CELL_INVALID", "vector material cell is invalid") from exc

    try:
        population = build_calibration_population_manifest(
            cell=cell, calibration_role_manifest=calibration_manifest
        )
    except ResponseProfileEvidenceContractError as exc:
        raise _error(
            "VECTOR_MATERIAL_POPULATION_INVALID",
            "vector material population failed reconstruction",
        ) from exc

    return VerifiedResponseProfileVectorMaterial(
        cell=cell,
        calibration_role_manifest=calibration_manifest,
        warmup_role_manifest=warmup_manifest,
        population=population,
    )


# ---------------------------------------------------------------------------
# Governed object loaders (authority stays with the input documents)
# ---------------------------------------------------------------------------


def response_profile_run_binding_from_document(
    document: object,
    *,
    vector_material: VerifiedResponseProfileVectorMaterial,
) -> ResponseProfileRunBinding:
    """Reconstruct one ``ResponseProfileRunBinding`` from its canonical
    document plus verified vector material, and prove the reconstruction is
    byte-identical to the authoritative input document.

    The replay schedule is not carried by any input: it is recomputed
    deterministically from the reconstructed population + source revision, so a
    substituted schedule cannot exist. A vector material for a different run
    yields a different population digest, which makes the reconstructed
    run-binding document differ from the input -- and is rejected here.
    """

    if type(vector_material) is not VerifiedResponseProfileVectorMaterial:
        raise _error("VECTOR_MATERIAL_INVALID", "vector material must be verified first")
    if type(document) is not dict or frozenset(document) != {
        "run_binding_payload",
        "run_binding_sha256",
    }:
        raise _error("RUN_BINDING_DOCUMENT_INVALID", "run binding document fields differ")
    payload = document["run_binding_payload"]
    if type(payload) is not dict or frozenset(payload) != {
        "schema_version",
        "run_id",
        "created_at_utc",
        "cell_id",
        "workload_manifest_sha256",
        "replay_schedule_sha256",
        "warmup_role_manifest_sha256",
        "source_revision",
    }:
        raise _error("RUN_BINDING_DOCUMENT_INVALID", "run binding payload fields differ")

    population = vector_material.population
    try:
        schedule = build_response_profile_replay_schedule(
            population=population, source_revision=payload["source_revision"]
        )
        run_binding = build_response_profile_run_binding(
            run_id=payload["run_id"],
            created_at_utc=payload["created_at_utc"],
            population=population,
            replay_schedule=schedule,
            warmup_role_manifest=vector_material.warmup_role_manifest,
            source_revision=payload["source_revision"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("RUN_BINDING_RECONSTRUCTION_FAILED", "run binding failed reconstruction") from exc
    if response_profile_run_binding_document(run_binding) != document:
        raise _error(
            "RUN_BINDING_DOCUMENT_MISMATCH",
            "reconstructed run binding is not byte-identical to its authoritative document",
        )
    return run_binding


def response_profile_oracle_manifest_from_document(
    document: object,
    *,
    vector_material: VerifiedResponseProfileVectorMaterial,
) -> ResponseProfileOracleManifest:
    """Reconstruct one ``ResponseProfileOracleManifest`` from its canonical
    document plus verified vector material, and prove the reconstruction is
    byte-identical to the authoritative input document.

    The oracle's records are re-bound to the same reconstructed calibration
    population the run-binding loader uses, so an oracle document whose records
    reference a different population (an oracle/run-binding vector-set
    mismatch) fails closed here.
    """

    if type(vector_material) is not VerifiedResponseProfileVectorMaterial:
        raise _error("VECTOR_MATERIAL_INVALID", "vector material must be verified first")
    if type(document) is not dict or frozenset(document) != {
        "oracle_manifest_payload",
        "oracle_manifest_sha256",
    }:
        raise _error("ORACLE_DOCUMENT_INVALID", "oracle manifest document fields differ")
    payload = document["oracle_manifest_payload"]
    if type(payload) is not dict or frozenset(payload) != {
        "schema_version",
        "workload_manifest_sha256",
        "record_count",
        "records",
    }:
        raise _error("ORACLE_DOCUMENT_INVALID", "oracle manifest payload fields differ")
    record_documents = payload["records"]
    if type(record_documents) is not list:
        raise _error("ORACLE_DOCUMENT_INVALID", "oracle records must be a list")

    population = vector_material.population
    members = population.calibration_role_manifest.members
    if len(record_documents) != len(members):
        raise _error(
            "ORACLE_VECTOR_SET_MISMATCH",
            "oracle record count does not match the reconstructed population",
        )
    metric = population.cell.metric

    records = []
    try:
        for record_document, member in zip(record_documents, members, strict=True):
            if type(record_document) is not dict or frozenset(record_document) != {
                "oracle_record_payload",
                "oracle_record_sha256",
            }:
                raise _error("ORACLE_DOCUMENT_INVALID", "oracle record document fields differ")
            record_payload = record_document["oracle_record_payload"]
            if type(record_payload) is not dict or frozenset(record_payload) != {
                "schema_version",
                "observation_identity_sha256",
                "query_id_sha256",
                "query_payload_sha256",
                "limit",
                "full_count",
                "capped_ids",
                "capped_distances",
            }:
                raise _error("ORACLE_DOCUMENT_INVALID", "oracle record payload fields differ")
            payload_identity = member.query_payload_identity
            records.append(
                build_response_profile_oracle_record(
                    observation_identity_sha256=record_payload["observation_identity_sha256"],
                    query_id_sha256=record_payload["query_id_sha256"],
                    query_payload_sha256=record_payload["query_payload_sha256"],
                    limit=record_payload["limit"],
                    full_count=record_payload["full_count"],
                    capped_ids=tuple(record_payload["capped_ids"]),
                    capped_distances=tuple(record_payload["capped_distances"]),
                    metric=metric,
                    radius=payload_identity.radius,
                    range_filter=payload_identity.range_filter,
                )
            )
    except ResponseProfileVectorMaterialError:
        raise
    except (KeyError, TypeError, ValueError, ResponseProfileSemanticError) as exc:
        raise _error("ORACLE_RECORD_RECONSTRUCTION_FAILED", "oracle record failed reconstruction") from exc

    try:
        oracle = build_response_profile_oracle_manifest(
            population=population, records=tuple(records)
        )
    except (TypeError, ValueError, ResponseProfileSemanticError) as exc:
        raise _error(
            "ORACLE_MANIFEST_RECONSTRUCTION_FAILED",
            "oracle manifest failed reconstruction",
        ) from exc
    if oracle_manifest_document(oracle) != document:
        raise _error(
            "ORACLE_DOCUMENT_MISMATCH",
            "reconstructed oracle manifest is not byte-identical to its authoritative document",
        )
    return oracle
