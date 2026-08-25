"""Neutral immutable range bound for canonical Gate-C window execution."""

from __future__ import annotations

from dataclasses import dataclass, fields

__all__ = [
    "GateCWindowExecutionBound",
    "GateCWindowExecutionError",
    "verify_gate_c_window_execution_bound",
]


class GateCWindowExecutionError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> GateCWindowExecutionError:
    return GateCWindowExecutionError(code)


@dataclass(frozen=True, slots=True)
class GateCWindowExecutionBound:
    """The sole authoritative representation of a contiguous Gate-C bound."""

    start_window_sequence: int
    window_count: int

    def __post_init__(self) -> None:
        if type(self.start_window_sequence) is not int or self.start_window_sequence < 0:
            raise _error("GATE_C_EXECUTION_BOUND_START_INVALID")
        if type(self.window_count) is not int or self.window_count <= 0:
            raise _error("GATE_C_EXECUTION_BOUND_COUNT_INVALID")

    @property
    def allowed_window_sequences(self) -> tuple[int, ...]:
        return tuple(
            range(
                self.start_window_sequence,
                self.start_window_sequence + self.window_count,
            )
        )

    @property
    def expected_next_window_sequence(self) -> int:
        return self.start_window_sequence + self.window_count


def verify_gate_c_window_execution_bound(
    value: GateCWindowExecutionBound,
) -> GateCWindowExecutionBound:
    if type(value) is not GateCWindowExecutionBound:
        raise _error("GATE_C_EXECUTION_BOUND_INVALID")
    try:
        rebuilt = GateCWindowExecutionBound(
            start_window_sequence=value.start_window_sequence,
            window_count=value.window_count,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("GATE_C_EXECUTION_BOUND_INVALID") from exc
    if any(
        type(getattr(value, item.name)) is not type(getattr(rebuilt, item.name))
        or getattr(value, item.name) != getattr(rebuilt, item.name)
        for item in fields(GateCWindowExecutionBound)
    ):
        raise _error("GATE_C_EXECUTION_BOUND_INVALID")
    return rebuilt
