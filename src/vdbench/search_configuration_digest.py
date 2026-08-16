"""Canonical, domain-separated document/digest for one SearchConfiguration.

Purpose:
    Give any consumer that needs to bind a candidate ``SearchConfiguration``
    (metric, threshold label, radius, derived range_filter, index track, ef,
    limit, consistency level) one exact, hash-verifiable identity, so no field
    -- radius included -- can silently diverge under an otherwise-unchanged
    binding. This module exists specifically so ``Stage4EvidenceBinding``
    (``canary_stage4_evidence_binding.py``) and the recall-audit ledger
    (``canary_recall_audit_ledger.py``) can share one canonical
    representation without either importing the other's private helpers.
Inputs:
    An already-validated ``SearchConfiguration``.
Outputs:
    A canonical, sorted-key JSON document and its domain-separated SHA-256
    digest.
Dependencies:
    ``config.py`` and ``artifacts.py`` only. Deliberately not placed in
    ``config.py`` itself: ``artifacts.py`` already imports from ``config.py``,
    so ``config.py`` cannot import ``canonical_json_bytes`` back from
    ``artifacts.py`` without a circular import.
Failure modes:
    A non-``SearchConfiguration`` input, or one that fails its own
    ``validate()``, is rejected before any document is built.
Root-validated numeric contract:
    ``SearchConfiguration.validate()`` is the root type gate: it now rejects
    a non-``int``/bool ``limit`` or ``ef`` outright, so by the time this
    module ever sees a validated configuration, ``limit``/``ef`` are already
    guaranteed to be genuine ``int`` (or ``None`` for ``ef`` on a FLAT
    configuration) -- this module serializes them directly, with no
    accept-and-normalize step of its own. ``radius`` remains a ``float`` by
    contract, and ``validate()`` alone does not fully close the ``-0.0``
    vs. ``0.0`` case (both compare numerically equal to a valid value, so
    two ``==`` configurations could still serialize to different JSON bytes
    for radius). ``_canonical_finite_float`` closes that gap, and is kept as
    an explicit, redundant defense-in-depth layer -- it does not trust that
    ``validate()`` was actually called correctly by every caller, and never
    normalizes an invalid value into a valid one.
Hash domain:
    The digest is ``sha256(SEARCH_CONFIGURATION_HASH_DOMAIN + canonical_json_bytes(document))``
    -- a fixed, versioned byte prefix concatenated directly onto the
    canonical JSON bytes, never embedded as an ordinary JSON field a caller
    could shadow or duplicate. Changing the domain constant (a new version)
    changes every digest computed under it, by construction.
"""

from __future__ import annotations

import hashlib
import math

from .artifacts import canonical_json_bytes
from .config import ContractViolation, IndexTrack, Metric, SearchConfiguration

__all__ = [
    "SEARCH_CONFIGURATION_DOCUMENT_SCHEMA_VERSION",
    "SEARCH_CONFIGURATION_HASH_DOMAIN",
    "search_configuration_document",
    "search_configuration_from_document",
    "search_configuration_sha256",
]


SEARCH_CONFIGURATION_DOCUMENT_SCHEMA_VERSION = "search-configuration-document-v1"

# A fixed, globally unique, versioned byte prefix -- never a JSON field, so
# no caller-supplied document can ever contain or shadow it. The trailing
# NUL byte is not required for uniqueness (canonical_json_bytes always
# starts with ``{``, so no valid JSON document could ever collide with this
# prefix even without it) but makes the domain boundary visually explicit
# and unambiguous in the byte contract below.
SEARCH_CONFIGURATION_HASH_DOMAIN = b"vdbench.search-configuration-document.v1\0"


def _canonical_finite_float(value: object, *, field_name: str) -> float:
    """Validate and canonicalize one float field to a single canonical form.

    Rejects booleans and non-numeric types outright (a bool is never a
    legitimate radius, regardless of Python's ``bool`` being an ``int``
    subclass). Rejects non-finite values. Every zero-valued result --
    ``-0.0``, integer ``0``, ``0.0`` -- normalizes to the single positive
    float ``0.0`` so equal configurations always serialize identically.
    Never coerces an out-of-range or otherwise invalid value into a valid
    one; that remains solely ``SearchConfiguration.validate()``'s job,
    called before this function ever runs.
    """

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractViolation(f"{field_name} must be a real number")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ContractViolation(f"{field_name} must be finite")
    if numeric == 0.0:
        return 0.0
    return numeric


def search_configuration_document(configuration: SearchConfiguration) -> dict[str, object]:
    """Return the complete canonical document for one SearchConfiguration.

    Excludes no field: metric, threshold_label, radius, derived range_filter,
    index_track, ef, limit, consistency_level are all present. ``limit`` and
    ``ef`` are serialized directly as the ``int``/``None`` values
    ``validate()`` already guarantees; only ``radius``/``range_filter`` pass
    through the defense-in-depth float canonicalizer, since those are the
    only fields where an accepted-equal value could still serialize
    differently (``-0.0`` vs. ``0.0``).
    """

    if not isinstance(configuration, SearchConfiguration):
        raise TypeError("configuration must be a SearchConfiguration")
    configuration.validate()
    return {
        "schema_version": SEARCH_CONFIGURATION_DOCUMENT_SCHEMA_VERSION,
        "metric": configuration.metric.value,
        "threshold_label": configuration.threshold_label,
        "radius": _canonical_finite_float(configuration.radius, field_name="radius"),
        "range_filter": _canonical_finite_float(configuration.range_filter, field_name="range_filter"),
        "index_track": configuration.index_track.value,
        "ef": configuration.ef,
        "limit": configuration.limit,
        "consistency_level": configuration.consistency_level,
    }


def search_configuration_sha256(configuration: SearchConfiguration) -> str:
    """Return the domain-separated SHA-256 of the canonical document.

    Computed as exactly
    ``sha256(SEARCH_CONFIGURATION_HASH_DOMAIN + canonical_json_bytes(document))``
    -- see the module docstring's "Hash domain" section for the full byte
    contract.
    """

    document = search_configuration_document(configuration)
    return hashlib.sha256(
        SEARCH_CONFIGURATION_HASH_DOMAIN + canonical_json_bytes(document)
    ).hexdigest()


def search_configuration_from_document(document: object) -> SearchConfiguration:
    """Strictly reconstruct one ``SearchConfiguration`` from its canonical
    document (the exact inverse of ``search_configuration_document``).

    Delegates all real validation to ``SearchConfiguration.validate()`` --
    the type's own root gate -- rather than re-implementing it; this
    function's own job is limited to strict document-shape/primitive-type
    checking (no bool-as-int, no string-to-number coercion, exact key set)
    and the canonical round-trip proof: the reconstructed object's own
    freshly computed ``search_configuration_document()`` must equal the
    input byte-for-byte, including the derived ``range_filter`` field no
    constructor argument can directly set.
    """

    if type(document) is not dict or frozenset(document) != {
        "schema_version",
        "metric",
        "threshold_label",
        "radius",
        "range_filter",
        "index_track",
        "ef",
        "limit",
        "consistency_level",
    }:
        raise ContractViolation("search configuration document fields differ")
    if document["schema_version"] != SEARCH_CONFIGURATION_DOCUMENT_SCHEMA_VERSION:
        raise ContractViolation("search configuration document schema differs")
    try:
        metric = Metric(document["metric"])
        index_track = IndexTrack(document["index_track"])
    except ValueError as exc:
        raise ContractViolation("search configuration enum field is invalid") from exc
    threshold_label = document["threshold_label"]
    if not isinstance(threshold_label, str):
        raise ContractViolation("threshold_label must be a string")
    radius = document["radius"]
    if isinstance(radius, bool) or not isinstance(radius, (int, float)):
        raise ContractViolation("radius must be a real number")
    ef = document["ef"]
    if ef is not None and (isinstance(ef, bool) or not isinstance(ef, int)):
        raise ContractViolation("ef must be an integer or null")
    limit = document["limit"]
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ContractViolation("limit must be an integer")
    consistency_level = document["consistency_level"]
    if not isinstance(consistency_level, str):
        raise ContractViolation("consistency_level must be a string")

    configuration = SearchConfiguration(
        metric=metric,
        threshold_label=threshold_label,
        radius=float(radius),
        index_track=index_track,
        ef=ef,
        limit=limit,
        consistency_level=consistency_level,
    )
    configuration.validate()
    reconstructed_document = search_configuration_document(configuration)
    if reconstructed_document != document:
        raise ContractViolation(
            "search configuration document is not byte-identical to its "
            "canonical reconstruction"
        )
    return configuration
