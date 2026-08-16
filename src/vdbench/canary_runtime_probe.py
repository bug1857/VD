"""Read-only Stage-4 runtime-probe adapter for the human-gated live root.

Purpose:
    Adapt an existing structural serving-preflight result to the small runtime
    probe port consumed by ``Stage4LiveRunner``.  The adapter is bound to one
    exact stream and route-state identity and performs no search, activation,
    approval, routing, rollback, configuration mutation, or client creation.
Inputs:
    A dependency-injected preflight-only port exposing ``preflight()``, exact
    ``MonitorStreamKey``/``RouteStateBinding`` values, and an injected UTC
    evidence clock.
Outputs:
    ``Stage4RuntimeReadiness`` for admission and ``Stage4SlotSafety`` for a
    pre/post slot probe.  Every unavailable, malformed, ambiguous, or unknown
    input fails closed with stable non-sensitive reason codes.
Complexity:
    O(number of preflight reason codes) per call; no query vector or hit payload
    is retained.
Failure modes:
    Requested-binding mismatch and invalid clocks avoid the port entirely.
    Port exceptions, malformed results, stream-scope ambiguity, and unrecognized
    reasons report incomplete readiness and unsafe slot state.
Extension point:
    ``MilvusRangeServingExecutor.preflight`` satisfies the structural port, but
    this adapter intentionally does not import that executor or PyMilvus.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from .canary_route_state import RouteStateBinding
from .canary_runtime_types import Stage4RuntimeReadiness, Stage4SlotSafety
from .shadow_event_types import MonitorStreamKey

__all__ = ["ServingPreflightPort", "Stage4ServingRuntimeProbe"]


_UTC = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_TRACK = frozenset({"FLAT", "HNSW"})
_HEALTH_PREFIXES = frozenset(
    {
        "STACK_HEALTH_UNAVAILABLE",
        "STACK_HEALTH_UNHEALTHY",
        "COLLECTION_LOAD_STATE_UNAVAILABLE",
        "COLLECTION_NOT_LOADED",
    }
)
_IDENTITY_PREFIXES = frozenset(
    {
        "COLLECTION_IDENTITY_UNAVAILABLE",
        "COLLECTION_IDENTITY_MISMATCH",
        "COLLECTION_BINDING_UNAVAILABLE",
    }
)


class ServingPreflightPort(Protocol):
    """Minimal structural shape supplied by a read-only serving adapter."""

    def preflight(self) -> object: ...


@dataclass(frozen=True, slots=True)
class _Inspection:
    """Private normalized result shared by admission and slot safety calls."""

    complete: bool
    health_ok: bool
    identity_ok: bool
    reason_codes: tuple[str, ...]


class Stage4ServingRuntimeProbe:
    """Map exactly one structural serving-preflight port to Stage-4 facts."""

    def __init__(
        self,
        *,
        expected_binding: RouteStateBinding,
        expected_stream: MonitorStreamKey,
        serving_preflight: ServingPreflightPort,
        utc_now: Callable[[], str],
    ) -> None:
        if not isinstance(expected_binding, RouteStateBinding):
            raise TypeError("expected_binding must be a RouteStateBinding")
        if not isinstance(expected_stream, MonitorStreamKey):
            raise TypeError("expected_stream must be a MonitorStreamKey")
        if not _stream_matches_binding(expected_stream, expected_binding):
            raise ValueError("expected stream must match expected binding")
        if not callable(getattr(serving_preflight, "preflight", None)):
            raise TypeError("serving_preflight must provide preflight")
        if not callable(utc_now):
            raise TypeError("utc_now must be callable")
        self._binding = expected_binding
        self._serving_preflight = serving_preflight
        self._utc_now = utc_now

    def preflight(self, *, binding: object) -> Stage4RuntimeReadiness:
        """Collect one timestamped read-only admission fact for the exact stream."""

        if binding != self._binding:
            return Stage4RuntimeReadiness(
                binding=self._binding,
                serving_preflight_complete=False,
                observed_at_utc="",
                reason_codes=("RUNTIME_BINDING_MISMATCH",),
            )
        timestamp = _read_utc(self._utc_now)
        if timestamp is None:
            return Stage4RuntimeReadiness(
                binding=self._binding,
                serving_preflight_complete=False,
                observed_at_utc="",
                reason_codes=("RUNTIME_PROBE_CLOCK_INVALID",),
            )
        inspection = self._inspect()
        return Stage4RuntimeReadiness(
            binding=self._binding,
            serving_preflight_complete=inspection.complete,
            observed_at_utc=timestamp,
            reason_codes=inspection.reason_codes,
        )

    def slot_safety(self, *, binding: object) -> Stage4SlotSafety:
        """Collect one read-only health/identity fact outside search timing."""

        if binding != self._binding:
            return Stage4SlotSafety(False, False, "RUNTIME_BINDING_MISMATCH")
        inspection = self._inspect()
        if inspection.complete:
            return Stage4SlotSafety(True, True)
        return Stage4SlotSafety(
            inspection.health_ok,
            inspection.identity_ok,
            inspection.reason_codes[0],
        )

    def _inspect(self) -> _Inspection:
        try:
            result = self._serving_preflight.preflight()
        except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
            return _unsafe("SERVING_PREFLIGHT_UNAVAILABLE")
        return _normalize_preflight(result)


def _normalize_preflight(result: object) -> _Inspection:
    """Validate and classify only the documented structural result fields."""

    complete = getattr(result, "complete", None)
    checked = getattr(result, "checked_stream_count", None)
    reasons = getattr(result, "reason_codes", None)
    if (
        not isinstance(complete, bool)
        or isinstance(checked, bool)
        or not isinstance(checked, int)
        or checked < 0
        or not isinstance(reasons, tuple)
        or not all(isinstance(reason, str) for reason in reasons)
    ):
        return _unsafe("SERVING_PREFLIGHT_RESULT_INVALID")
    # A probe is constructed for one stream only. A complete single-stream
    # preflight reports one checked stream; a failed one reports zero. Any
    # other count proves a caller widened the serving-preflight scope.
    if (complete and checked != 1) or (not complete and checked != 0):
        return _unsafe("STAGE4_STREAM_SCOPE_AMBIGUOUS")
    if complete:
        return _Inspection(True, True, True, ()) if not reasons else _unsafe(
            "SERVING_PREFLIGHT_RESULT_INVALID"
        )
    if not reasons:
        return _unsafe("SERVING_PREFLIGHT_INCOMPLETE")

    normalized: list[tuple[str, bool, bool]] = []
    for reason in reasons:
        value = _classify_reason(reason)
        if value is None:
            return _unsafe("SERVING_PREFLIGHT_REASON_UNKNOWN")
        normalized.append(value)
    health_ok = not any(item[1] for item in normalized)
    identity_ok = not any(item[2] for item in normalized)
    return _Inspection(
        False,
        health_ok,
        identity_ok,
        tuple(dict.fromkeys(item[0] for item in normalized)),
    )


def _classify_reason(reason: str) -> tuple[str, bool, bool] | None:
    """Convert one known serving reason to a canonical Stage-4 code/facts."""

    if reason in {"STACK_HEALTH_UNAVAILABLE", "STACK_HEALTH_UNHEALTHY"}:
        return reason, True, False
    if reason.count(":") != 1:
        return None
    prefix, track = reason.split(":", maxsplit=1)
    if track not in _TRACK:
        return None
    canonical = f"{prefix}_{track}"
    if prefix in _HEALTH_PREFIXES:
        return canonical, True, False
    if prefix in _IDENTITY_PREFIXES:
        return canonical, False, True
    return None


def _unsafe(reason: str) -> _Inspection:
    return _Inspection(False, False, False, (reason,))


def _stream_matches_binding(
    stream: MonitorStreamKey, binding: RouteStateBinding
) -> bool:
    return bool(
        stream.metric is binding.metric
        and stream.threshold_stratum == binding.threshold_stratum
        and stream.configuration_identity == binding.configuration_identity
        and stream.data_identity == binding.data_identity
        and stream.flat_binding_id == binding.flat_binding_id
        and stream.hnsw_binding_id == binding.hnsw_binding_id
    )


def _read_utc(clock: Callable[[], str]) -> str | None:
    try:
        value = clock()
    except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
        return None
    if not isinstance(value, str) or _UTC.fullmatch(value) is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        return None
    return value
