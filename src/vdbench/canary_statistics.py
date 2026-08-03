"""Pure, offline statistics for EXP-009's proposed canary contract.

Purpose:
    Make the finite-manifest latency and synthetic-query recall calculations
    explicit, reproducible, and independently testable before any routing code.
Inputs:
    The pre-registered EXP-009 cardinalities and bounded recall observations.
Outputs:
    Exact finite-population coverage and a one-sided Hoeffding recall bound.
Dependencies:
    Python standard library only; never Milvus, PyMilvus, or an actuation client.
Failure modes:
    Any non-contract count, non-finite value, or out-of-range recall fails closed.
Scope:
    The latency calculation applies only to the frozen finite manifest under the
    ADR-008 randomization and no-interference assumptions. It is not an IID or
    production-latency confidence interval.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from math import ceil, comb, isfinite, log, sqrt
from numbers import Real


EXP009_ROUTING_POPULATION_COUNT = 600
EXP009_CANDIDATE_COUNT = 60
EXP009_RECALL_AUDIT_COUNT = 1_200
EXP009_CONFIDENCE_LEVEL = 0.95
EXP009_LATENCY_PERCENTILE = 0.95


@dataclass(frozen=True, slots=True)
class FinitePopulationP95Bound:
    """Coverage of a maximum from a simple random sample without replacement.

    ``conservative_tail_count`` is the strict upper tail used for a lower-bound
    coverage calculation. It deliberately does not rely on a percentile-tie
    convention at the nearest-rank threshold.
    """

    population_size: int
    sample_size: int
    percentile: float
    percentile_rank: int
    conservative_tail_count: int
    coverage_probability: float


@dataclass(frozen=True, slots=True)
class RecallLowerBound:
    """One-sided Hoeffding lower bound for mean capped recall in ``[0, 1]``."""

    observation_count: int
    observed_mean: float
    margin: float
    lower_bound: float
    confidence_level: float


def finite_population_p95_bound(
    *,
    population_size: int,
    sample_size: int,
    percentile: float = EXP009_LATENCY_PERCENTILE,
) -> FinitePopulationP95Bound:
    """Return a conservative coverage calculation for a finite p95 target.

    For nearest-rank p95, the strict upper tail has ``N - ceil(.95 * N)``
    elements. A uniformly selected sample without replacement misses every one
    of those elements with probability ``C(N-tail, n) / C(N, n)``. Observing a
    sample maximum above that tail threshold is sufficient for it to be at
    least the nearest-rank p95; ties can only increase actual coverage.
    """

    if (
        isinstance(population_size, bool)
        or not isinstance(population_size, int)
        or population_size <= 1
    ):
        raise ValueError("population_size must be an integer greater than one")
    if (
        isinstance(sample_size, bool)
        or not isinstance(sample_size, int)
        or not 1 <= sample_size <= population_size
    ):
        raise ValueError("sample_size must be an integer in [1, population_size]")
    if (
        isinstance(percentile, bool)
        or not isinstance(percentile, Real)
        or not isfinite(float(percentile))
        or not 0.0 < float(percentile) < 1.0
    ):
        raise ValueError("percentile must be finite and strictly between zero and one")

    normalized_percentile = float(percentile)
    rank = ceil(normalized_percentile * population_size)
    strict_upper_tail = population_size - rank
    if strict_upper_tail <= 0:
        raise ValueError("percentile leaves no strict upper tail for coverage")
    if sample_size > population_size - strict_upper_tail:
        miss_probability = 0.0
    else:
        miss_probability = comb(population_size - strict_upper_tail, sample_size) / comb(
            population_size, sample_size
        )
    return FinitePopulationP95Bound(
        population_size=population_size,
        sample_size=sample_size,
        percentile=normalized_percentile,
        percentile_rank=rank,
        conservative_tail_count=strict_upper_tail,
        coverage_probability=1.0 - miss_probability,
    )


def exp009_latency_bound_contract() -> FinitePopulationP95Bound:
    """Return the frozen EXP-009 60-of-600 finite-manifest calculation."""

    return finite_population_p95_bound(
        population_size=EXP009_ROUTING_POPULATION_COUNT,
        sample_size=EXP009_CANDIDATE_COUNT,
    )


def one_sided_hoeffding_recall_lower_bound(
    recalls: Iterable[float],
) -> RecallLowerBound:
    """Compute EXP-009's pre-registered one-sided 95% recall lower bound.

    The bound is ``mean(recall) - sqrt(log(1 / alpha) / (2n))`` with
    ``alpha = 0.05`` and exactly 1,200 values in ``[0, 1]``. It estimates only
    the independently generated DATASET-002 recall-audit query population.
    """

    values = tuple(recalls)
    if len(values) != EXP009_RECALL_AUDIT_COUNT:
        raise ValueError("EXP-009 recall bound requires exactly 1200 observations")
    normalized: list[float] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValueError("recall values must be finite and within [0, 1]")
        numeric = float(value)
        if not isfinite(numeric) or not 0.0 <= numeric <= 1.0:
            raise ValueError("recall values must be finite and within [0, 1]")
        normalized.append(numeric)

    observed_mean = sum(normalized) / EXP009_RECALL_AUDIT_COUNT
    alpha = 1.0 - EXP009_CONFIDENCE_LEVEL
    margin = sqrt(log(1.0 / alpha) / (2.0 * EXP009_RECALL_AUDIT_COUNT))
    return RecallLowerBound(
        observation_count=EXP009_RECALL_AUDIT_COUNT,
        observed_mean=observed_mean,
        margin=margin,
        lower_bound=max(0.0, observed_mean - margin),
        confidence_level=EXP009_CONFIDENCE_LEVEL,
    )


__all__ = [
    "EXP009_CANDIDATE_COUNT",
    "EXP009_CONFIDENCE_LEVEL",
    "EXP009_LATENCY_PERCENTILE",
    "EXP009_RECALL_AUDIT_COUNT",
    "EXP009_ROUTING_POPULATION_COUNT",
    "FinitePopulationP95Bound",
    "RecallLowerBound",
    "exp009_latency_bound_contract",
    "finite_population_p95_bound",
    "one_sided_hoeffding_recall_lower_bound",
]
