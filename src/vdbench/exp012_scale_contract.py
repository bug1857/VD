"""Immutable EXP-012-SCALE campaign contracts.

The scale profiles extend the number of canonical 200-source windows without
changing EXP-010 evidence, Gate-A authority, detector semantics, or the two
read-only searches performed for each source.  This module is pure: it opens no
store, contacts no service, and creates no admission or actuation authority.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from enum import StrEnum

from .canonical_serialization import strict_canonical_digest
from .shadow_window import WINDOW_QUERY_COUNT

__all__ = [
    "EXP012_SCALE_CONTRACT_SCHEMA_VERSION",
    "Exp012ScaleContract",
    "Exp012ScaleContractError",
    "Exp012ScaleProfile",
    "build_exp012_scale_contract",
    "exp012_scale_contract_payload",
    "verify_exp012_scale_contract",
]


EXP012_SCALE_CONTRACT_SCHEMA_VERSION = "exp012-scale-contract-v1"
_CONTRACT_DOMAIN = b"VD::EXP012_SCALE_CONTRACT::V1\x00"
_SEARCHES_PER_SOURCE = 2


class Exp012ScaleContractError(ValueError):
    """Fail-closed contract error carrying one stable reason code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class Exp012ScaleProfile(StrEnum):
    SCALE_2400 = "scale-2400"
    SCALE_10000 = "scale-10000"


_PROFILE_TARGETS = {
    Exp012ScaleProfile.SCALE_2400: 2400,
    Exp012ScaleProfile.SCALE_10000: 10000,
}


@dataclass(frozen=True, slots=True)
class Exp012ScaleContract:
    schema_version: str
    experiment_id: str
    profile: Exp012ScaleProfile
    target_source_records: int
    window_query_count: int
    expected_windows: int
    searches_per_source: int
    expected_physical_searches: int
    contract_sha256: str


def _profile(value: object) -> Exp012ScaleProfile:
    if type(value) is not Exp012ScaleProfile:
        raise Exp012ScaleContractError("EXP012_SCALE_PROFILE_INVALID")
    return value


def exp012_scale_contract_payload(
    contract: Exp012ScaleContract,
) -> dict[str, object]:
    """Return the exact detached-digest payload after full reconstruction."""

    verified = verify_exp012_scale_contract(contract)
    return {
        "schema_version": verified.schema_version,
        "experiment_id": verified.experiment_id,
        "profile": verified.profile.value,
        "target_source_records": verified.target_source_records,
        "window_query_count": verified.window_query_count,
        "expected_windows": verified.expected_windows,
        "searches_per_source": verified.searches_per_source,
        "expected_physical_searches": verified.expected_physical_searches,
    }


def build_exp012_scale_contract(
    profile: Exp012ScaleProfile,
) -> Exp012ScaleContract:
    """Build one of the only two governed EXP-012-SCALE profiles."""

    profile = _profile(profile)
    target = _PROFILE_TARGETS[profile]
    payload: dict[str, object] = {
        "schema_version": EXP012_SCALE_CONTRACT_SCHEMA_VERSION,
        "experiment_id": "EXP-012-SCALE",
        "profile": profile.value,
        "target_source_records": target,
        "window_query_count": WINDOW_QUERY_COUNT,
        "expected_windows": target // WINDOW_QUERY_COUNT,
        "searches_per_source": _SEARCHES_PER_SOURCE,
        "expected_physical_searches": target * _SEARCHES_PER_SOURCE,
    }
    return Exp012ScaleContract(
        schema_version=EXP012_SCALE_CONTRACT_SCHEMA_VERSION,
        experiment_id="EXP-012-SCALE",
        profile=profile,
        target_source_records=target,
        window_query_count=WINDOW_QUERY_COUNT,
        expected_windows=target // WINDOW_QUERY_COUNT,
        searches_per_source=_SEARCHES_PER_SOURCE,
        expected_physical_searches=target * _SEARCHES_PER_SOURCE,
        contract_sha256=strict_canonical_digest(_CONTRACT_DOMAIN, payload),
    )


def verify_exp012_scale_contract(
    contract: Exp012ScaleContract,
) -> Exp012ScaleContract:
    """Reconstruct all values; forged or extended contracts fail closed."""

    if type(contract) is not Exp012ScaleContract:
        raise Exp012ScaleContractError("EXP012_SCALE_CONTRACT_INVALID")
    expected = build_exp012_scale_contract(_profile(contract.profile))
    for item in fields(Exp012ScaleContract):
        actual_value = getattr(contract, item.name)
        expected_value = getattr(expected, item.name)
        if type(actual_value) is not type(expected_value) or actual_value != expected_value:
            raise Exp012ScaleContractError("EXP012_SCALE_CONTRACT_INVALID")
    return expected
