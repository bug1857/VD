"""Canonical, domain-separated identity binding for one LKG-qualification run.

Purpose:
    Bind every identity ARCHITECTURE.md's ADR-002 LKG-qualification amendment
    and the current Phase-1 task require fixed *before* any client dispatch:
    the run itself, the producer process, the complete candidate
    ``SearchConfiguration``, the Milvus collection/base-data identity, the
    index/build identity, the environment, and the source revision. The
    DATASET-003 workload commitment is complete, not just a manifest hash:
    dataset ID/version/manifest hash/query role, plus the query-ID array's
    own SHA-256, the query-vector array's own SHA-256, and the expected
    query count -- so a mismatch on exact workload content or count fails
    the binding, not only a mismatch on which manifest produced it. One
    immutable ``LkgRunBinding`` and its domain-separated SHA-256 digest give
    every ``LkgQueryAttempt`` in a run one exact, independently
    reconstructable and verifiable identity to point back to.
Inputs:
    Already-known identity strings/values, gathered by the caller before
    dispatch begins. This module performs no I/O and issues no query.
Outputs:
    A canonical, sorted-key JSON document and its domain-separated SHA-256
    digest for one ``LkgRunBinding``.
Dependencies:
    ``config.py`` for ``SearchConfiguration``, and
    ``search_configuration_digest.py`` for the exact nested
    candidate-configuration document/digest convention this module embeds
    (mirroring ``Stage4EvidenceBinding``'s "embed the complete typed
    configuration" pattern) -- never ``milvus_actuation.py`` or any
    canary/actuation module.
Failure modes:
    A non-``LkgRunBinding`` input, one whose embedded ``SearchConfiguration``
    fails its own ``validate()``, or one with a blank/malformed identity
    field is rejected before any document is built.
"""

from __future__ import annotations

import hashlib
import re

from .artifacts import canonical_json_bytes
from .config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from .search_configuration_digest import (
    SEARCH_CONFIGURATION_DOCUMENT_SCHEMA_VERSION,
    search_configuration_document,
)

__all__ = [
    "LKG_RUN_BINDING_DOCUMENT_SCHEMA_VERSION",
    "LKG_RUN_BINDING_HASH_DOMAIN",
    "ORDERED_QUERY_IDS_DIGEST_DOMAIN",
    "LkgRunBinding",
    "lkg_ordered_query_ids_sha256",
    "lkg_run_binding_document",
    "lkg_run_binding_from_document",
    "lkg_run_binding_sha256",
]


LKG_RUN_BINDING_DOCUMENT_SCHEMA_VERSION = "lkg-run-binding-document-v3"

# A fixed, versioned byte prefix -- never a JSON field -- matching
# search_configuration_digest.py's hash-domain convention exactly. v3 adds
# qualification_ordered_query_ids_sha256, a semantic ordered-query-ID
# digest independent of any array serialization format (see
# lkg_ordered_query_ids_sha256 below) -- distinct from and never a
# replacement for qualification_query_id_array_sha256, the raw DATASET-003
# .npy artifact's own SHA-256, which is computed and verified exclusively
# by lkg_dataset003_loader.py / sha256_file and never reconstructed here.
LKG_RUN_BINDING_HASH_DOMAIN = b"vdbench.lkg-run-binding-document.v3\0"

# A fixed, versioned byte prefix for the semantic ordered-query-ID digest --
# disjoint from LKG_RUN_BINDING_HASH_DOMAIN and from every other domain in
# this repository.
ORDERED_QUERY_IDS_DIGEST_DOMAIN = b"vdbench.lkg-ordered-query-ids.v1\0"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def lkg_ordered_query_ids_sha256(ordered_query_ids: object) -> str:
    """Canonical, domain-separated digest of an ordered DATASET-003
    query-ID sequence -- semantic identity only, deliberately independent
    of any array *serialization format*.

    This is not, and must never become, a replacement for
    ``qualification_query_id_array_sha256`` (the raw ``.npy`` artifact's
    own byte-exact SHA-256, computed and verified exclusively by
    ``lkg_dataset003_loader.py``/``sha256_file``). Recreating a NumPy array
    and calling ``numpy.save()`` to reproduce that artifact hash would
    conflate byte-exact artifact identity with semantic ordered-query
    identity, and would silently depend on NumPy's serialization/header
    behavior across environments or versions. This function instead uses
    one explicit, stable, non-NumPy binary contract:
    ``domain prefix + count (8-byte little-endian signed) + each query ID
    (8-byte little-endian signed), concatenated in order`` -- so the same
    ordered IDs always produce the same digest regardless of NumPy version,
    platform, or array serialization details.
    """

    if not isinstance(ordered_query_ids, (list, tuple)):
        raise ContractViolation("ordered_query_ids must be a list or tuple of integers")
    ids = tuple(ordered_query_ids)
    if not ids:
        raise ContractViolation("ordered_query_ids must be non-empty")
    payload = bytearray(ORDERED_QUERY_IDS_DIGEST_DOMAIN)
    payload += len(ids).to_bytes(8, byteorder="little", signed=True)
    for query_id in ids:
        if isinstance(query_id, bool) or not isinstance(query_id, int):
            raise ContractViolation("every ordered_query_ids entry must be a plain int")
        if not _INT64_MIN <= query_id <= _INT64_MAX:
            raise ContractViolation("every query_id must fit in a signed 64-bit integer")
        payload += query_id.to_bytes(8, byteorder="little", signed=True)
    return hashlib.sha256(bytes(payload)).hexdigest()


def _nonempty_str(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ContractViolation(
            f"{field_name} must be a non-empty string of at most 256 characters"
        )
    return value


def _sha256_hex(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ContractViolation(f"{field_name} must be a lower-case 64-character hex SHA-256 digest")
    return value


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ContractViolation(f"{field_name} must be a positive integer")
    return value


class LkgRunBinding:
    """One immutable, fully self-describing LKG-qualification run identity.

    Deliberately a plain, hand-validated class rather than a frozen
    dataclass: every field is validated in ``__init__`` (mirroring
    ``Stage4EvidenceBinding``'s constructor-time coherence checks) so an
    ``LkgRunBinding`` can never exist half-formed -- unlike
    ``LkgQueryObservation``, which stays a lightweight, unvalidated evidence
    record by design (see ``lkg_qualification_evidence.py``), this binding is
    the run-wide identity commitment every observation is checked against,
    so it validates eagerly.
    """

    __slots__ = (
        "base_data_identity",
        "collection_name",
        "environment_identity",
        "index_identity",
        "producer_identity",
        "qualification_dataset_id",
        "qualification_dataset_version",
        "qualification_expected_query_count",
        "qualification_manifest_sha256",
        "qualification_ordered_query_ids_sha256",
        "qualification_query_array_sha256",
        "qualification_query_id_array_sha256",
        "qualification_query_role",
        "run_id",
        "search_configuration",
        "source_revision",
    )

    def __init__(
        self,
        *,
        run_id: str,
        producer_identity: str,
        search_configuration: SearchConfiguration,
        collection_name: str,
        base_data_identity: str,
        index_identity: str,
        qualification_dataset_id: str,
        qualification_dataset_version: str,
        qualification_manifest_sha256: str,
        qualification_query_role: str,
        qualification_query_id_array_sha256: str,
        qualification_ordered_query_ids_sha256: str,
        qualification_query_array_sha256: str,
        qualification_expected_query_count: int,
        environment_identity: str,
        source_revision: str,
    ) -> None:
        if not isinstance(search_configuration, SearchConfiguration):
            raise ContractViolation("search_configuration must be a SearchConfiguration")
        search_configuration.validate()
        object.__setattr__(self, "run_id", _nonempty_str(run_id, field_name="run_id"))
        object.__setattr__(
            self, "producer_identity", _nonempty_str(producer_identity, field_name="producer_identity")
        )
        object.__setattr__(self, "search_configuration", search_configuration)
        object.__setattr__(
            self, "collection_name", _nonempty_str(collection_name, field_name="collection_name")
        )
        object.__setattr__(
            self,
            "base_data_identity",
            _nonempty_str(base_data_identity, field_name="base_data_identity"),
        )
        object.__setattr__(
            self, "index_identity", _nonempty_str(index_identity, field_name="index_identity")
        )
        object.__setattr__(
            self,
            "qualification_dataset_id",
            _nonempty_str(qualification_dataset_id, field_name="qualification_dataset_id"),
        )
        object.__setattr__(
            self,
            "qualification_dataset_version",
            _nonempty_str(
                qualification_dataset_version, field_name="qualification_dataset_version"
            ),
        )
        object.__setattr__(
            self,
            "qualification_manifest_sha256",
            _sha256_hex(
                qualification_manifest_sha256, field_name="qualification_manifest_sha256"
            ),
        )
        object.__setattr__(
            self,
            "qualification_query_role",
            _nonempty_str(qualification_query_role, field_name="qualification_query_role"),
        )
        object.__setattr__(
            self,
            "qualification_query_id_array_sha256",
            _sha256_hex(
                qualification_query_id_array_sha256,
                field_name="qualification_query_id_array_sha256",
            ),
        )
        object.__setattr__(
            self,
            "qualification_ordered_query_ids_sha256",
            _sha256_hex(
                qualification_ordered_query_ids_sha256,
                field_name="qualification_ordered_query_ids_sha256",
            ),
        )
        object.__setattr__(
            self,
            "qualification_query_array_sha256",
            _sha256_hex(
                qualification_query_array_sha256,
                field_name="qualification_query_array_sha256",
            ),
        )
        object.__setattr__(
            self,
            "qualification_expected_query_count",
            _positive_int(
                qualification_expected_query_count,
                field_name="qualification_expected_query_count",
            ),
        )
        object.__setattr__(
            self,
            "environment_identity",
            _nonempty_str(environment_identity, field_name="environment_identity"),
        )
        object.__setattr__(
            self, "source_revision", _nonempty_str(source_revision, field_name="source_revision")
        )

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"LkgRunBinding is immutable: cannot set {name!r}")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"LkgRunBinding is immutable: cannot delete {name!r}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LkgRunBinding):
            return NotImplemented
        return all(getattr(self, field) == getattr(other, field) for field in self.__slots__)

    def __hash__(self) -> int:
        return hash(tuple(getattr(self, field) for field in self.__slots__))

    def __repr__(self) -> str:
        fields = ", ".join(f"{field}={getattr(self, field)!r}" for field in self.__slots__)
        return f"LkgRunBinding({fields})"

    @property
    def sha256(self) -> str:
        """The derived, domain-separated identity digest for this binding."""

        return lkg_run_binding_sha256(self)


def lkg_run_binding_document(binding: LkgRunBinding) -> dict[str, object]:
    """Return the complete canonical document for one LkgRunBinding."""

    if not isinstance(binding, LkgRunBinding):
        raise TypeError("binding must be an LkgRunBinding")
    return {
        "schema_version": LKG_RUN_BINDING_DOCUMENT_SCHEMA_VERSION,
        "run_id": binding.run_id,
        "producer_identity": binding.producer_identity,
        "search_configuration": search_configuration_document(binding.search_configuration),
        "collection_name": binding.collection_name,
        "base_data_identity": binding.base_data_identity,
        "index_identity": binding.index_identity,
        "qualification_dataset_id": binding.qualification_dataset_id,
        "qualification_dataset_version": binding.qualification_dataset_version,
        "qualification_manifest_sha256": binding.qualification_manifest_sha256,
        "qualification_query_role": binding.qualification_query_role,
        "qualification_query_id_array_sha256": binding.qualification_query_id_array_sha256,
        "qualification_ordered_query_ids_sha256": binding.qualification_ordered_query_ids_sha256,
        "qualification_query_array_sha256": binding.qualification_query_array_sha256,
        "qualification_expected_query_count": binding.qualification_expected_query_count,
        "environment_identity": binding.environment_identity,
        "source_revision": binding.source_revision,
    }


def lkg_run_binding_sha256(binding: LkgRunBinding) -> str:
    """Return the domain-separated SHA-256 of the canonical document."""

    document = lkg_run_binding_document(binding)
    return hashlib.sha256(
        LKG_RUN_BINDING_HASH_DOMAIN + canonical_json_bytes(document)
    ).hexdigest()


_BINDING_DOCUMENT_FIELDS = frozenset(
    {
        "schema_version",
        "run_id",
        "producer_identity",
        "search_configuration",
        "collection_name",
        "base_data_identity",
        "index_identity",
        "qualification_dataset_id",
        "qualification_dataset_version",
        "qualification_manifest_sha256",
        "qualification_query_role",
        "qualification_query_id_array_sha256",
        "qualification_ordered_query_ids_sha256",
        "qualification_query_array_sha256",
        "qualification_expected_query_count",
        "environment_identity",
        "source_revision",
    }
)
_SEARCH_CONFIGURATION_DOCUMENT_FIELDS = frozenset(
    {
        "schema_version",
        "metric",
        "threshold_label",
        "radius",
        "range_filter",
        "index_track",
        "ef",
        "limit",
        "consistency_level",
    }
)


def lkg_run_binding_from_document(document: object) -> LkgRunBinding:
    """Strictly reconstruct one ``LkgRunBinding`` from its stored document.

    This is the reverse of ``lkg_run_binding_document`` -- the exact
    counterpart the ledger's run table needs to independently verify a
    persisted binding rather than trust an unpersisted in-memory object.
    Every field set (top-level and the nested ``search_configuration``
    sub-document) is checked for an *exact* match against the expected
    field set -- an unknown, missing, or extra field is rejected, not
    silently ignored -- and a whole-document canonical byte round-trip
    (``canonical_json_bytes(document) == canonical_json_bytes(reconstructed)``)
    catches any noncanonical numeric representation (e.g. ``-0.0`` vs
    ``0.0``) or contradictory derived field a per-field check alone could
    miss. This mirrors ``canary_stage4_qualification_report.py``'s proven
    strict v2 document-loading pattern.
    """

    try:
        if not isinstance(document, dict) or frozenset(document) != _BINDING_DOCUMENT_FIELDS:
            raise ValueError("run binding document top-level fields are invalid")
        if document["schema_version"] != LKG_RUN_BINDING_DOCUMENT_SCHEMA_VERSION:
            raise ValueError("run binding document schema_version is unsupported")
        sc_doc = document["search_configuration"]
        if (
            not isinstance(sc_doc, dict)
            or frozenset(sc_doc) != _SEARCH_CONFIGURATION_DOCUMENT_FIELDS
        ):
            raise ValueError("run binding search_configuration fields are invalid")
        if sc_doc["schema_version"] != SEARCH_CONFIGURATION_DOCUMENT_SCHEMA_VERSION:
            raise ValueError("run binding search_configuration schema_version is unsupported")

        search_configuration = SearchConfiguration(
            metric=Metric(sc_doc["metric"]),
            threshold_label=sc_doc["threshold_label"],
            radius=sc_doc["radius"],
            index_track=IndexTrack(sc_doc["index_track"]),
            ef=sc_doc["ef"],
            limit=sc_doc["limit"],
            consistency_level=sc_doc["consistency_level"],
        )
        binding = LkgRunBinding(
            run_id=document["run_id"],
            producer_identity=document["producer_identity"],
            search_configuration=search_configuration,
            collection_name=document["collection_name"],
            base_data_identity=document["base_data_identity"],
            index_identity=document["index_identity"],
            qualification_dataset_id=document["qualification_dataset_id"],
            qualification_dataset_version=document["qualification_dataset_version"],
            qualification_manifest_sha256=document["qualification_manifest_sha256"],
            qualification_query_role=document["qualification_query_role"],
            qualification_query_id_array_sha256=document["qualification_query_id_array_sha256"],
            qualification_ordered_query_ids_sha256=document["qualification_ordered_query_ids_sha256"],
            qualification_query_array_sha256=document["qualification_query_array_sha256"],
            qualification_expected_query_count=document["qualification_expected_query_count"],
            environment_identity=document["environment_identity"],
            source_revision=document["source_revision"],
        )

        reconstructed_document = lkg_run_binding_document(binding)
        if canonical_json_bytes(document) != canonical_json_bytes(reconstructed_document):
            raise ValueError(
                "run binding document is not byte-identical to its canonical "
                "reconstruction (noncanonical numeric representation or a "
                "contradictory derived field)"
            )
        return binding
    except (KeyError, TypeError, ValueError, ContractViolation) as exc:
        raise ContractViolation(f"run binding document is malformed: {exc}") from exc
