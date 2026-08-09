"""Neutral binding of fresh D1 authority to a verified-latest D2 reference.

Purpose:
    Provide the single authority-pair boundary shared by Phase-3 consumers
    without introducing a policy-to-admission or admission-to-policy
    dependency.  The pair proves exact identity equality between one concrete
    D1 authority and one concrete D2 verified-head snapshot.
Inputs:
    A concrete ``LkgPhase3Authority`` and a concrete store-issued
    ``VerifiedLatestLkgPhase3AuthorityReference``.
Outputs:
    One immutable ``LkgPhase3AuthorityPair`` created only by
    ``bind_lkg_phase3_authority``.
Failure modes:
    Non-concrete, malformed, or identity-mismatched inputs are rejected.  D2
    record metadata is deliberately not compared with D1 identity.
Dependencies:
    D1 and D2 value contracts only.  This module performs no I/O, source
    replay, persistence access, policy/admission/actuation work, Milvus calls,
    or statistical inspection/recomputation.  The provider/composition root,
    not this pure binder, owns same-refresh acquisition semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .lkg_phase3_authority import LkgPhase3Authority
    from .lkg_phase3_persistence import (
        PersistedLkgPhase3AuthorityReference,
        VerifiedLatestLkgPhase3AuthorityReference,
    )


__all__ = [
    "LkgPhase3AuthorityPair",
    "bind_lkg_phase3_authority",
]


_PAIR_CONSTRUCTION_TOKEN = object()
_D2_RECORD_METADATA_FIELDS = frozenset(
    {
        "record_schema_version",
        "sequence_number",
        "persisted_at_utc",
        "previous_record_digest",
        "canonical_record_digest",
    }
)
@dataclass(frozen=True, slots=True, init=False)
class LkgPhase3AuthorityPair:
    """One validated D1 authority and its exact D2 verified-head snapshot.

    Private construction is API discipline, not cryptographic authenticity.
    The value does not claim that its two components were acquired during the
    same refresh; that invariant belongs to the provider/composition root.
    """

    _authority: LkgPhase3Authority
    _verified_latest_reference: VerifiedLatestLkgPhase3AuthorityReference

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "LkgPhase3AuthorityPair can only be created by "
            "bind_lkg_phase3_authority()"
        )

    @classmethod
    def _from_validated(
        cls,
        *,
        authority: LkgPhase3Authority,
        verified_latest_reference: VerifiedLatestLkgPhase3AuthorityReference,
        construction_token: object,
    ) -> LkgPhase3AuthorityPair:
        if construction_token is not _PAIR_CONSTRUCTION_TOKEN:
            raise TypeError("Phase-3 LKG pair construction token is invalid")
        value = object.__new__(cls)
        object.__setattr__(value, "_authority", authority)
        object.__setattr__(
            value, "_verified_latest_reference", verified_latest_reference
        )
        return value

    @property
    def authority(self) -> LkgPhase3Authority:
        return self._authority

    @property
    def verified_latest_reference(
        self,
    ) -> VerifiedLatestLkgPhase3AuthorityReference:
        return self._verified_latest_reference


def bind_lkg_phase3_authority(
    *,
    authority: object,
    verified_latest_reference: object,
) -> LkgPhase3AuthorityPair:
    """Purely bind exact D1/D2 identities; perform no replay or store access."""

    # Lazy contract imports keep this neutral value module safely importable
    # below policy despite the historical evidence -> actuation -> policy
    # dependency.  No replay, store construction, or I/O occurs here.
    from .lkg_phase3_authority import LkgPhase3Authority
    from .lkg_phase3_persistence import (
        PersistedLkgPhase3AuthorityReference,
        VerifiedLatestLkgPhase3AuthorityReference,
    )

    if type(authority) is not LkgPhase3Authority:
        raise TypeError("authority must be a concrete LkgPhase3Authority")
    if type(verified_latest_reference) is not VerifiedLatestLkgPhase3AuthorityReference:
        raise TypeError(
            "verified_latest_reference must be a concrete store-issued "
            "VerifiedLatestLkgPhase3AuthorityReference"
        )
    try:
        reference = verified_latest_reference.reference
        if type(reference) is not PersistedLkgPhase3AuthorityReference:
            raise TypeError("verified latest reference contains an invalid record")
        mismatches = _identity_mismatches(
            authority,
            reference,
            reference_type=PersistedLkgPhase3AuthorityReference,
        )
    except AttributeError as exc:
        raise TypeError("Phase-3 authority pair is structurally malformed") from exc
    if mismatches:
        raise ValueError(
            "D1/D2 Phase-3 authority identity mismatch: " + ",".join(mismatches)
        )
    return LkgPhase3AuthorityPair._from_validated(
        authority=authority,
        verified_latest_reference=verified_latest_reference,
        construction_token=_PAIR_CONSTRUCTION_TOKEN,
    )


def _identity_mismatches(
    authority: object,
    reference: object,
    *,
    reference_type: type[object],
) -> tuple[str, ...]:
    mismatches: list[str] = []
    identity_fields = (
        field.name
        for field in fields(reference_type)
        if field.name not in _D2_RECORD_METADATA_FIELDS
    )
    for field_name in identity_fields:
        authority_value = getattr(authority, field_name)
        if field_name == "metric":
            authority_value = authority_value.value
        if getattr(reference, field_name) != authority_value:
            mismatches.append(field_name)
    return tuple(mismatches)
