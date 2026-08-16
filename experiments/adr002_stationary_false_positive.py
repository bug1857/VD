"""Offline stationary-replay evidence for ADR-002's detector false-positive rate.

This experiment evaluates 299 non-overlapping detector decisions for each of
L2 and COSINE.  Each metric has one immutable reference window and 598 current
windows.  Every reference/current window is generated from the same frozen
stationary distributions with NumPy ``Generator(PCG64)`` and master seed
``20260802``; no drift is injected.

The four stationary signal distributions are:

* query vectors: independent Normal(0, 1), shape (200, 128), float64;
* thresholds: L2 LogNormal(0, 0.25), COSINE Uniform(-0.25, 0.95), n=200;
* exact cardinalities: Poisson(75), n=50;
* sentinel recall: Beta(98, 2), n=50.

The experiment invokes the production offline statistical core, including all
9,999 permutations, Holm correction, effect gates, and two-window decision
logic.  It neither imports PyMilvus nor accesses a live database.  Results are
reported separately by metric; the cross-metric total is descriptive only.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass

import numpy as np

from vdbench.config import Metric
from vdbench.drift import (
    AUDIT_QUERY_COUNT,
    ELIGIBLE_QUERY_COUNT,
    DetectorState,
    RecallAuditSample,
    Signal,
    WindowEvidence,
    evaluate_drift_decision,
    finalize_window_evidence,
    ks_signal_test,
    query_vector_signal_test,
    recall_signal_test,
    select_audit_sample,
)

MASTER_SEED = 20_260_802
DECISIONS_PER_METRIC = 299
CURRENT_WINDOWS_PER_METRIC = 2 * DECISIONS_PER_METRIC
DIMENSIONS = 128
CONFIDENCE_LEVEL = 0.95
METRICS = (Metric.L2, Metric.COSINE)

COLLECTION_DATA_IDENTITY = "adr002-stationary-replay-data-v1"
INDEX_BUILD_IDENTITY = "adr002-stationary-replay-sentinel-ef100-v1"

ProgressCallback = Callable[[Metric, int, int], None]


@dataclass(frozen=True, slots=True)
class StationaryWindow:
    """Synthetic data needed to evaluate all four detector signals."""

    window_id: str
    metric: Metric
    query_vectors: np.ndarray
    thresholds: np.ndarray
    exact_cardinalities: np.ndarray
    recall_audit: RecallAuditSample


@dataclass(frozen=True, slots=True)
class MetricReplayResult:
    """False-positive evidence for one metric, kept statistically separate."""

    metric: str
    decisions: int
    current_windows: int
    false_positives: int
    false_positive_point_estimate: float
    one_sided_95_clopper_pearson_upper: float
    decision_state_counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class StationaryReplayResult:
    """Complete replay output with a descriptive-only cross-metric total."""

    experiment: str
    master_seed: int
    generator: str
    dimensions: int
    decisions_per_metric: int
    current_windows_per_metric: int
    confidence_level: float
    permutation_count_per_signal: int
    worker_processes: int
    metric_results: tuple[MetricReplayResult, ...]
    aggregate_descriptive_total_decisions: int
    aggregate_descriptive_total_false_positives: int
    aggregate_note: str
    adr_002_status: str
    elapsed_seconds: float


_WORKER_REFERENCE: StationaryWindow | None = None
_WORKER_MASTER_SEED: int | None = None
_WORKER_DIMENSIONS: int | None = None


def _metric_stream_id(metric: Metric) -> int:
    return 0 if metric is Metric.L2 else 1


def _window_id(metric: Metric, ordinal: int) -> str:
    label = "reference" if ordinal == 0 else f"current-{ordinal:04d}"
    return f"stationary-{metric.value}-{label}"


def _window_rng(*, master_seed: int, metric: Metric, ordinal: int):
    seed = np.random.SeedSequence(
        entropy=master_seed,
        spawn_key=(_metric_stream_id(metric), ordinal),
    )
    return np.random.Generator(np.random.PCG64(seed))


def _generate_window(
    *, master_seed: int, metric: Metric, ordinal: int, dimensions: int
) -> StationaryWindow:
    """Draw one window from the frozen stationary null distribution."""

    rng = _window_rng(master_seed=master_seed, metric=metric, ordinal=ordinal)
    window_id = _window_id(metric, ordinal)
    query_vectors = rng.normal(
        loc=0.0,
        scale=1.0,
        size=(ELIGIBLE_QUERY_COUNT, dimensions),
    ).astype(np.float64)
    if metric is Metric.L2:
        thresholds = rng.lognormal(
            mean=0.0, sigma=0.25, size=ELIGIBLE_QUERY_COUNT
        ).astype(np.float64)
    else:
        thresholds = rng.uniform(
            low=-0.25, high=0.95, size=ELIGIBLE_QUERY_COUNT
        ).astype(np.float64)
    exact_cardinalities = rng.poisson(lam=75.0, size=AUDIT_QUERY_COUNT).astype(np.int64)

    selection = select_audit_sample(
        tuple(range(ELIGIBLE_QUERY_COUNT)),
        detector_seed=master_seed,
        metric=metric,
        window_id=window_id,
    )
    if not selection.complete:
        raise RuntimeError(f"audit selection failed: {selection.reason}")
    recall_values = rng.beta(a=98.0, b=2.0, size=AUDIT_QUERY_COUNT).astype(np.float64)
    recall_audit = RecallAuditSample(
        window_id=window_id,
        metric=metric,
        expected_audit_ids=selection.query_ids,
        observed_audit_ids=selection.query_ids,
        values=recall_values,
        flat_oracle_agreement=np.ones(AUDIT_QUERY_COUNT, dtype=bool),
        collection_data_identity=COLLECTION_DATA_IDENTITY,
        index_build_identity=INDEX_BUILD_IDENTITY,
    )
    return StationaryWindow(
        window_id=window_id,
        metric=metric,
        query_vectors=query_vectors,
        thresholds=thresholds,
        exact_cardinalities=exact_cardinalities,
        recall_audit=recall_audit,
    )


def _evaluate_window(
    reference: StationaryWindow,
    current: StationaryWindow,
    *,
    detector_seed: int,
) -> WindowEvidence:
    if reference.metric is not current.metric:
        raise ValueError("reference and current metrics must match")
    signals = (
        query_vector_signal_test(
            reference.query_vectors,
            current.query_vectors,
            metric=current.metric,
            detector_seed=detector_seed,
            window_id=current.window_id,
        ),
        ks_signal_test(
            reference.thresholds,
            current.thresholds,
            signal=Signal.THRESHOLD,
            metric=current.metric,
            detector_seed=detector_seed,
            window_id=current.window_id,
        ),
        ks_signal_test(
            reference.exact_cardinalities,
            current.exact_cardinalities,
            signal=Signal.CARDINALITY,
            metric=current.metric,
            detector_seed=detector_seed,
            window_id=current.window_id,
        ),
        recall_signal_test(
            reference.recall_audit,
            current.recall_audit,
            detector_seed=detector_seed,
        ),
    )
    evidence = finalize_window_evidence(
        metric=current.metric,
        window_id=current.window_id,
        signals=signals,
    )
    if not evidence.complete:
        reasons = ", ".join(evidence.reason_codes)
        raise RuntimeError(f"incomplete window {current.window_id}: {reasons}")
    return evidence


def _initialize_worker(
    reference: StationaryWindow, master_seed: int, dimensions: int
) -> None:
    global _WORKER_REFERENCE, _WORKER_MASTER_SEED, _WORKER_DIMENSIONS
    _WORKER_REFERENCE = reference
    _WORKER_MASTER_SEED = master_seed
    _WORKER_DIMENSIONS = dimensions


def _evaluate_window_ordinal(ordinal: int) -> WindowEvidence:
    if (
        _WORKER_REFERENCE is None
        or _WORKER_MASTER_SEED is None
        or _WORKER_DIMENSIONS is None
    ):
        raise RuntimeError("stationary replay worker was not initialized")
    current = _generate_window(
        master_seed=_WORKER_MASTER_SEED,
        metric=_WORKER_REFERENCE.metric,
        ordinal=ordinal,
        dimensions=_WORKER_DIMENSIONS,
    )
    return _evaluate_window(
        _WORKER_REFERENCE,
        current,
        detector_seed=_WORKER_MASTER_SEED,
    )


def _binomial_cdf(successes: int, trials: int, probability: float) -> float:
    if probability <= 0.0:
        return 1.0
    if probability >= 1.0:
        return 1.0 if successes == trials else 0.0
    log_probability = math.log(probability)
    log_complement = math.log1p(-probability)
    terms = [
        math.lgamma(trials + 1)
        - math.lgamma(count + 1)
        - math.lgamma(trials - count + 1)
        + count * log_probability
        + (trials - count) * log_complement
        for count in range(successes + 1)
    ]
    largest = max(terms)
    return math.exp(largest) * math.fsum(math.exp(term - largest) for term in terms)


def clopper_pearson_upper(
    false_positives: int,
    decisions: int,
    *,
    confidence_level: float = CONFIDENCE_LEVEL,
) -> float:
    """Return the one-sided exact Clopper-Pearson binomial upper bound."""

    if isinstance(false_positives, bool) or isinstance(decisions, bool):
        raise TypeError("counts must be integers, not booleans")
    if decisions <= 0 or not 0 <= false_positives <= decisions:
        raise ValueError("counts must satisfy 0 <= false_positives <= decisions")
    if not 0.0 < confidence_level < 1.0:
        raise ValueError("confidence_level must lie strictly between zero and one")
    if false_positives == decisions:
        return 1.0

    alpha = 1.0 - confidence_level
    lower = 0.0
    upper = 1.0
    for _ in range(100):
        midpoint = (lower + upper) / 2.0
        if _binomial_cdf(false_positives, decisions, midpoint) > alpha:
            lower = midpoint
        else:
            upper = midpoint
    return (lower + upper) / 2.0


def _iter_window_evidence(
    *,
    reference: StationaryWindow,
    master_seed: int,
    dimensions: int,
    current_windows: int,
    workers: int,
):
    ordinals = range(1, current_windows + 1)
    if workers == 1:
        for ordinal in ordinals:
            current = _generate_window(
                master_seed=master_seed,
                metric=reference.metric,
                ordinal=ordinal,
                dimensions=dimensions,
            )
            yield _evaluate_window(reference, current, detector_seed=master_seed)
        return

    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=_initialize_worker,
        initargs=(reference, master_seed, dimensions),
    ) as executor:
        yield from executor.map(_evaluate_window_ordinal, ordinals, chunksize=1)


def run_metric_replay(
    metric: Metric,
    *,
    decisions: int = DECISIONS_PER_METRIC,
    master_seed: int = MASTER_SEED,
    dimensions: int = DIMENSIONS,
    workers: int = 1,
    progress: ProgressCallback | None = None,
) -> MetricReplayResult:
    """Run non-overlapping stationary detector decisions for one metric."""

    if decisions <= 0 or dimensions <= 0 or workers <= 0:
        raise ValueError("decisions, dimensions, and workers must be positive")
    current_windows = 2 * decisions
    reference = _generate_window(
        master_seed=master_seed,
        metric=metric,
        ordinal=0,
        dimensions=dimensions,
    )
    state_counts: Counter[str] = Counter()
    false_positives = 0
    pending: WindowEvidence | None = None
    completed = 0
    for window in _iter_window_evidence(
        reference=reference,
        master_seed=master_seed,
        dimensions=dimensions,
        current_windows=current_windows,
        workers=workers,
    ):
        if pending is None:
            pending = window
            continue
        decision = evaluate_drift_decision(pending, window)
        state_counts[decision.state.value] += 1
        if decision.state is DetectorState.DRIFT:
            false_positives += 1
        pending = None
        completed += 1
        if progress is not None:
            progress(metric, completed, decisions)

    if pending is not None or completed != decisions:
        raise RuntimeError("non-overlapping decision construction was incomplete")
    return MetricReplayResult(
        metric=metric.value,
        decisions=decisions,
        current_windows=current_windows,
        false_positives=false_positives,
        false_positive_point_estimate=false_positives / decisions,
        one_sided_95_clopper_pearson_upper=clopper_pearson_upper(
            false_positives, decisions
        ),
        decision_state_counts={
            state.value: state_counts[state.value] for state in DetectorState
        },
    )


def run_stationary_replay(
    *,
    decisions_per_metric: int = DECISIONS_PER_METRIC,
    master_seed: int = MASTER_SEED,
    dimensions: int = DIMENSIONS,
    workers: int = 1,
    progress: ProgressCallback | None = None,
) -> StationaryReplayResult:
    """Run and report statistically separate L2 and COSINE null replays."""

    started = time.monotonic()
    metric_results = tuple(
        run_metric_replay(
            metric,
            decisions=decisions_per_metric,
            master_seed=master_seed,
            dimensions=dimensions,
            workers=workers,
            progress=progress,
        )
        for metric in METRICS
    )
    return StationaryReplayResult(
        experiment="ADR-002 stationary false-positive validation",
        master_seed=master_seed,
        generator="NumPy Generator(PCG64) with SeedSequence spawn keys",
        dimensions=dimensions,
        decisions_per_metric=decisions_per_metric,
        current_windows_per_metric=2 * decisions_per_metric,
        confidence_level=CONFIDENCE_LEVEL,
        permutation_count_per_signal=9_999,
        worker_processes=workers,
        metric_results=metric_results,
        aggregate_descriptive_total_decisions=sum(
            item.decisions for item in metric_results
        ),
        aggregate_descriptive_total_false_positives=sum(
            item.false_positives for item in metric_results
        ),
        aggregate_note=(
            "Descriptive count only; no combined cross-metric statistical claim."
        ),
        adr_002_status="Proposed",
        elapsed_seconds=time.monotonic() - started,
    )


def _print_progress(metric: Metric, completed: int, total: int) -> None:
    if completed == 1 or completed % 25 == 0 or completed == total:
        print(
            f"progress metric={metric.value} decisions={completed}/{total}",
            file=sys.stderr,
            flush=True,
        )


def _print_plain_results(result: StationaryReplayResult) -> None:
    print(f"master_seed={result.master_seed}")
    print(f"decisions_per_metric={result.decisions_per_metric}")
    print(f"current_windows_per_metric={result.current_windows_per_metric}")
    for metric_result in result.metric_results:
        prefix = metric_result.metric
        print(
            f"{prefix} false_positive_count="
            f"{metric_result.false_positives}/{metric_result.decisions}"
        )
        print(
            f"{prefix} false_positive_point_estimate="
            f"{metric_result.false_positive_point_estimate:.12f}"
        )
        print(
            f"{prefix} one_sided_95_clopper_pearson_upper="
            f"{metric_result.one_sided_95_clopper_pearson_upper:.12f}"
        )
    print(
        "aggregate_descriptive_total_decisions="
        f"{result.aggregate_descriptive_total_decisions}"
    )
    print(
        "aggregate_descriptive_total_false_positives="
        f"{result.aggregate_descriptive_total_false_positives}"
    )
    print(f"aggregate_note={result.aggregate_note}")
    print(f"adr_002_status={result.adr_002_status}")
    print("result_json=" + json.dumps(asdict(result), sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workers",
        type=int,
        default=min(8, os.cpu_count() or 1),
        help="independent worker processes; does not alter deterministic results",
    )
    args = parser.parse_args(argv)
    result = run_stationary_replay(
        decisions_per_metric=DECISIONS_PER_METRIC,
        master_seed=MASTER_SEED,
        dimensions=DIMENSIONS,
        workers=args.workers,
        progress=_print_progress,
    )
    _print_plain_results(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
