"""Neutral, non-actuating runtime values shared by Stage-4 composition ports."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .canary_route_state import RouteStateBinding

__all__ = ["Stage4RuntimeReadiness", "Stage4SlotSafety"]


_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


@dataclass(frozen=True, slots=True)
class Stage4RuntimeReadiness:
    """Read-only serving readiness supplied to the Stage-4 admission boundary.

    The composition root creates this only after a health/load/exact-identity
    preflight. It intentionally carries no policy, approval, query, client, or
    configuration-mutation capability.
    """

    binding: RouteStateBinding
    serving_preflight_complete: bool
    observed_at_utc: str
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Stage4SlotSafety:
    """Read-only health/identity facts immediately adjacent to one search.

    Both facts must be true only when a reason is absent.  A partial failure is
    represented explicitly so the durable Stage-4 ledger can distinguish health
    and identity evidence without trusting an implicit default.
    """

    health_ok: bool
    identity_ok: bool
    reason_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.health_ok, bool) or not isinstance(self.identity_ok, bool):
            raise TypeError("slot safety values must be bool")
        safe = self.health_ok and self.identity_ok
        if safe != (self.reason_code is None):
            raise ValueError("safe slot state must have no reason; unsafe state requires one")
        if self.reason_code is not None and (
            not isinstance(self.reason_code, str) or _CODE.fullmatch(self.reason_code) is None
        ):
            raise ValueError("slot safety reason is invalid")
