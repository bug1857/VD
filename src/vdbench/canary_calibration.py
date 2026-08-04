"""Pre-registered offline diagnostics for EXP-009 Stage 1.

Purpose:
    Regression-check the implemented finite-population and Hoeffding calculations
    using frozen synthetic replay configurations.
Inputs:
    Only the fixed seeds, replay counts, and synthetic bounded distributions
    recorded in EXP-009.
Outputs:
    Immutable result values suitable for a later checksummed experiment artifact.
Dependencies:
    NumPy and :mod:`vdbench.canary_statistics`; no Milvus, routing, approval,
    CSPRNG selection, policy, or actuation dependency.
Limitations:
    The analytic hypergeometric calculation is the latency-coverage proof.  The
    deterministic PCG64 replay does not validate a production CSPRNG, live
    no-interference, IID latency, or real HNSW recall.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from numbers import Real

import numpy as np

from .canary_statistics import (
    EXP009_CANDIDATE_COUNT,
    EXP009_CONFIDENCE_LEVEL,
    EXP009_RECALL_AUDIT_COUNT,
    EXP009_ROUTING_POPULATION_COUNT,
    exp009_latency_bound_contract,
)


FINITE_POPULATION_CALIBRATION_SEED = 20260810
FINITE_POPULATION_CALIBRATION_REPLAYS = 100_000
RECALL_CALIBRATION_SEED = 20260811
RECALL_CALIBRATION_REPLAYS = 10_000
RECALL_CALIBRATION_MEANS = (0.50, 0.95, 0.99)


@dataclass(frozen=True, slots=True)
class FinitePopulationCalibration:
    """A fixed PCG64 diagnostic for the exact 60-of-600 tail calculation."""

    seed: int
    replay_count: int
    population_size: int
    sample_size: int
    strict_upper_tail_count: int
    tail_hit_count: int
    empirical_coverage: float
    analytic_coverage: float

    def validate(self) -> None:
        _seed(self.seed)
        _positive_int(self.replay_count, field="replay_count")
        if self.population_size != EXP009_ROUTING_POPULATION_COUNT:
            raise ValueError("population_size must equal the EXP-009 contract")
        if self.sample_size != EXP009_CANDIDATE_COUNT:
            raise ValueError("sample_size must equal the EXP-009 contract")
        exact = exp009_latency_bound_contract()
        if self.strict_upper_tail_count != exact.conservative_tail_count:
            raise ValueError("strict_upper_tail_count must equal the EXP-009 contract")
        if not 0 <= self.tail_hit_count <= self.replay_count:
            raise ValueError("tail_hit_count must be within replay_count")
        _probability(self.empirical_coverage, field="empirical_coverage")
        _probability(self.analytic_coverage, field="analytic_coverage")
        if self.empirical_coverage != self.tail_hit_count / self.replay_count:
            raise ValueError("empirical_coverage must equal tail_hit_count/replay_count")
        if self.analytic_coverage != exact.coverage_probability:
            raise ValueError("analytic_coverage must equal the exact contract")

    def to_document(self) -> dict[str, int | float]:
        self.validate()
        return {
            "seed": self.seed,
            "replay_count": self.replay_count,
            "population_size": self.population_size,
            "sample_size": self.sample_size,
            "strict_upper_tail_count": self.strict_upper_tail_count,
            "tail_hit_count": self.tail_hit_count,
            "empirical_coverage": self.empirical_coverage,
            "analytic_coverage": self.analytic_coverage,
        }


@dataclass(frozen=True, slots=True)
class RecallCalibration:
    """A stationary Bernoulli regression diagnostic for the fixed bound."""

    seed: int
    replay_count: int
    observations_per_replay: int
    true_mean: float
    hoeffding_margin: float
    noncoverage_count: int
    empirical_noncoverage: float
    confidence_level: float

    def validate(self) -> None:
        _seed(self.seed)
        _positive_int(self.replay_count, field="replay_count")
        if self.observations_per_replay != EXP009_RECALL_AUDIT_COUNT:
            raise ValueError("observations_per_replay must equal the EXP-009 contract")
        _probability(self.true_mean, field="true_mean")
        expected_margin = math.sqrt(
            math.log(1.0 / (1.0 - EXP009_CONFIDENCE_LEVEL))
            / (2.0 * EXP009_RECALL_AUDIT_COUNT)
        )
        if self.hoeffding_margin != expected_margin:
            raise ValueError("hoeffding_margin must equal the pre-registered formula")
        if not 0 <= self.noncoverage_count <= self.replay_count:
            raise ValueError("noncoverage_count must be within replay_count")
        _probability(self.empirical_noncoverage, field="empirical_noncoverage")
        if self.empirical_noncoverage != self.noncoverage_count / self.replay_count:
            raise ValueError("empirical_noncoverage must equal noncoverage_count/replay_count")
        if self.confidence_level != EXP009_CONFIDENCE_LEVEL:
            raise ValueError("confidence_level must equal the EXP-009 contract")

    def to_document(self) -> dict[str, int | float]:
        self.validate()
        return {
            "seed": self.seed,
            "replay_count": self.replay_count,
            "observations_per_replay": self.observations_per_replay,
            "true_mean": self.true_mean,
            "hoeffding_margin": self.hoeffding_margin,
            "noncoverage_count": self.noncoverage_count,
            "empirical_noncoverage": self.empirical_noncoverage,
            "confidence_level": self.confidence_level,
        }


@dataclass(frozen=True, slots=True)
class Exp009CalibrationResult:
    """All frozen diagnostics, kept separate from a later live evidence run."""

    finite_population: FinitePopulationCalibration
    recall: tuple[RecallCalibration, ...]

    def validate(self) -> None:
        self.finite_population.validate()
        if len(self.recall) != len(RECALL_CALIBRATION_MEANS):
            raise ValueError("recall calibration scenario count is invalid")
        if tuple(result.true_mean for result in self.recall) != RECALL_CALIBRATION_MEANS:
            raise ValueError("recall calibration means differ from the pre-registration")
        expected_seeds = tuple(
            RECALL_CALIBRATION_SEED + index
            for index in range(len(RECALL_CALIBRATION_MEANS))
        )
        if tuple(result.seed for result in self.recall) != expected_seeds:
            raise ValueError("recall calibration seeds differ from the pre-registration")
        for result in self.recall:
            result.validate()

    def to_document(self) -> dict[str, object]:
        self.validate()
        return {
            "finite_population": self.finite_population.to_document(),
            "recall": [result.to_document() for result in self.recall],
        }


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _seed(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("seed must be a non-negative integer")
    return value


def _probability(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{field} must be finite and within [0, 1]")
    normalized = float(value)
    if not math.isfinite(normalized) or not 0.0 <= normalized <= 1.0:
        raise ValueError(f"{field} must be finite and within [0, 1]")
    return normalized


def simulate_finite_population_diagnostic(
    *,
    seed: int = FINITE_POPULATION_CALIBRATION_SEED,
    replay_count: int = FINITE_POPULATION_CALIBRATION_REPLAYS,
) -> FinitePopulationCalibration:
    """Run a deterministic hypergeometric diagnostic of the exact contract.

    A hypergeometric variate is distributionally equivalent to counting strict
    upper-tail IDs in a simple-random sample without replacement.  It avoids
    materializing 100,000 full 600-ID permutations and is explicitly not the
    real candidate-selection mechanism, which remains ``secrets.SystemRandom``.
    """

    normalized_seed = _seed(seed)
    normalized_count = _positive_int(replay_count, field="replay_count")
    exact = exp009_latency_bound_contract()
    generator = np.random.Generator(np.random.PCG64(normalized_seed))
    tail_hits = generator.hypergeometric(
        ngood=exact.conservative_tail_count,
        nbad=exact.population_size - exact.conservative_tail_count,
        nsample=exact.sample_size,
        size=normalized_count,
    )
    count = int(np.count_nonzero(tail_hits > 0))
    result = FinitePopulationCalibration(
        seed=normalized_seed,
        replay_count=normalized_count,
        population_size=exact.population_size,
        sample_size=exact.sample_size,
        strict_upper_tail_count=exact.conservative_tail_count,
        tail_hit_count=count,
        empirical_coverage=count / normalized_count,
        analytic_coverage=exact.coverage_probability,
    )
    result.validate()
    return result


def simulate_recall_diagnostic(
    *,
    seed: int,
    replay_count: int,
    true_mean: float,
) -> RecallCalibration:
    """Replay independent stationary Bernoulli recalls against the fixed LCB.

    The binomial draw is exactly the sum of 1,200 independent Bernoulli capped
    recall values.  It makes no assertion about a live query stream; its only
    purpose is regression-calibration of the frozen calculation and evidence
    recording path under declared query-generator assumptions.
    """

    normalized_seed = _seed(seed)
    normalized_count = _positive_int(replay_count, field="replay_count")
    mean = _probability(true_mean, field="true_mean")
    margin = math.sqrt(
        math.log(1.0 / (1.0 - EXP009_CONFIDENCE_LEVEL))
        / (2.0 * EXP009_RECALL_AUDIT_COUNT)
    )
    generator = np.random.Generator(np.random.PCG64(normalized_seed))
    sums = generator.binomial(EXP009_RECALL_AUDIT_COUNT, mean, size=normalized_count)
    lower_bounds = sums / EXP009_RECALL_AUDIT_COUNT - margin
    noncoverage = int(np.count_nonzero(lower_bounds > mean))
    result = RecallCalibration(
        seed=normalized_seed,
        replay_count=normalized_count,
        observations_per_replay=EXP009_RECALL_AUDIT_COUNT,
        true_mean=mean,
        hoeffding_margin=margin,
        noncoverage_count=noncoverage,
        empirical_noncoverage=noncoverage / normalized_count,
        confidence_level=EXP009_CONFIDENCE_LEVEL,
    )
    result.validate()
    return result


def run_exp009_calibration(
    *,
    finite_population_replay_count: int = FINITE_POPULATION_CALIBRATION_REPLAYS,
    recall_replay_count: int = RECALL_CALIBRATION_REPLAYS,
) -> Exp009CalibrationResult:
    """Run exactly the three pre-registered recall scenarios and one latency run."""

    finite = simulate_finite_population_diagnostic(
        seed=FINITE_POPULATION_CALIBRATION_SEED,
        replay_count=finite_population_replay_count,
    )
    recall = tuple(
        simulate_recall_diagnostic(
            seed=RECALL_CALIBRATION_SEED + scenario_index,
            replay_count=recall_replay_count,
            true_mean=true_mean,
        )
        for scenario_index, true_mean in enumerate(RECALL_CALIBRATION_MEANS)
    )
    result = Exp009CalibrationResult(finite_population=finite, recall=recall)
    result.validate()
    return result


__all__ = [
    "FINITE_POPULATION_CALIBRATION_REPLAYS",
    "FINITE_POPULATION_CALIBRATION_SEED",
    "RECALL_CALIBRATION_MEANS",
    "RECALL_CALIBRATION_REPLAYS",
    "RECALL_CALIBRATION_SEED",
    "Exp009CalibrationResult",
    "FinitePopulationCalibration",
    "RecallCalibration",
    "run_exp009_calibration",
    "simulate_finite_population_diagnostic",
    "simulate_recall_diagnostic",
]
