"""Offline ADR-002 drift-injection validation with objective ground truth.

The replay uses one immutable stationary reference window per metric and the
same four synthetic signal distributions as the stationary false-positive
experiment.  Two non-overlapping stationary decision pairs precede injection
at pair index 2.  Abrupt scenarios then run for six injected pairs.  Gradual
vector drift ramps its Normal mean from 0.0 to 0.5 over eight current windows
(four pairs), then holds 0.5 for four plateau pairs.

Detection delay is reported as ``first_DRIFT_pair_index - 2`` in
non-overlapping-pair units.  This is explicitly not sliding-window operational
delay.  Results are descriptive evidence only; no inferential claim is made.

This module invokes the real offline detector with all 9,999 permutations,
Holm correction, effect gates, and classification logic.  It neither imports
PyMilvus nor accesses a live database.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from enum import StrEnum
import json
import os
import sys
import time
from typing import Callable, Sequence

import numpy as np

from experiments.adr002_stationary_false_positive import (
    COLLECTION_DATA_IDENTITY,
    INDEX_BUILD_IDENTITY,
    StationaryWindow,
    _evaluate_window,
)
from vdbench.config import Metric
from vdbench.drift import (
    AUDIT_QUERY_COUNT,
    ELIGIBLE_QUERY_COUNT,
    DetectorState,
    DriftClassification,
    RecallAuditSample,
    Signal,
    WindowEvidence,
    evaluate_drift_decision,
    select_audit_sample,
)

MASTER_SEED = 20_260_803
DIMENSIONS = 128
METRICS = (Metric.L2, Metric.COSINE)

BASELINE_PAIRS = 2
INJECTION_PAIR_INDEX = 2
ABRUPT_INJECTED_PAIRS = 6
GRADUAL_RAMP_PAIRS = 4
GRADUAL_PLATEAU_PAIRS = 4

BASE_VECTOR_MEAN = 0.0
INJECTED_VECTOR_MEAN = 0.5
VECTOR_STDDEV = 1.0
BASE_L2_THRESHOLD_MEAN = 0.0
INJECTED_L2_THRESHOLD_MEAN = 0.5
L2_THRESHOLD_SIGMA = 0.25
BASE_COSINE_THRESHOLD_LOW = -0.25
INJECTED_COSINE_THRESHOLD_LOW = 0.47
COSINE_THRESHOLD_HIGH = 0.95
BASE_CARDINALITY_LAMBDA = 75.0
INJECTED_CARDINALITY_LAMBDA = 90.0
BASE_RECALL_BETA = (98.0, 2.0)
INJECTED_RECALL_BETA = (90.0, 10.0)

DELAY_SEMANTICS = (
    "non-overlapping-pair delay = first_DRIFT_pair_index - 2; "
    "distinct from sliding-window operational delay"
)
DESCRIPTIVE_NOTE = "Descriptive rates only; no inferential claim."

ProgressCallback = Callable[[Metric, str, int, int], None]


class ScenarioName(StrEnum):
    """The five frozen ADR-002 injection scenarios in reporting order."""

    ABRUPT_VECTOR_ONLY = "abrupt-vector-only"
    ABRUPT_THRESHOLD_ONLY = "abrupt-threshold-only"
    ABRUPT_CARDINALITY_ONLY = "abrupt-cardinality-only"
    ABRUPT_QUALITY_ONLY = "abrupt-quality-only"
    GRADUAL_VECTOR = "gradual-vector"


@dataclass(frozen=True, slots=True)
class ScenarioSpec:
    """Frozen scenario identity, schedule, and expected classification."""

    name: ScenarioName
    stream_id: int
    expected_classification: DriftClassification
    injected_pairs: int
    ramp_pairs: int = 0
    plateau_pairs: int = 0

    @property
    def total_pairs(self) -> int:
        return BASELINE_PAIRS + self.injected_pairs

    @property
    def total_current_windows(self) -> int:
        return 2 * self.total_pairs


SCENARIOS = (
    ScenarioSpec(
        name=ScenarioName.ABRUPT_VECTOR_ONLY,
        stream_id=1,
        expected_classification=DriftClassification.INPUT_DRIFT,
        injected_pairs=ABRUPT_INJECTED_PAIRS,
    ),
    ScenarioSpec(
        name=ScenarioName.ABRUPT_THRESHOLD_ONLY,
        stream_id=2,
        expected_classification=DriftClassification.INPUT_DRIFT,
        injected_pairs=ABRUPT_INJECTED_PAIRS,
    ),
    ScenarioSpec(
        name=ScenarioName.ABRUPT_CARDINALITY_ONLY,
        stream_id=3,
        expected_classification=DriftClassification.INPUT_DRIFT,
        injected_pairs=ABRUPT_INJECTED_PAIRS,
    ),
    ScenarioSpec(
        name=ScenarioName.ABRUPT_QUALITY_ONLY,
        stream_id=4,
        expected_classification=DriftClassification.QUALITY_DRIFT,
        injected_pairs=ABRUPT_INJECTED_PAIRS,
    ),
    ScenarioSpec(
        name=ScenarioName.GRADUAL_VECTOR,
        stream_id=5,
        expected_classification=DriftClassification.INPUT_DRIFT,
        injected_pairs=GRADUAL_RAMP_PAIRS + GRADUAL_PLATEAU_PAIRS,
        ramp_pairs=GRADUAL_RAMP_PAIRS,
        plateau_pairs=GRADUAL_PLATEAU_PAIRS,
    ),
)
SCENARIO_BY_NAME = {spec.name: spec for spec in SCENARIOS}


@dataclass(frozen=True, slots=True)
class WindowParameters:
    """Frozen synthetic-distribution parameters for one current window."""

    vector_mean: float
    l2_threshold_mean: float
    cosine_threshold_low: float
    cardinality_lambda: float
    recall_beta_a: float
    recall_beta_b: float


@dataclass(frozen=True, slots=True)
class WindowTask:
    """Pickle-safe identity for one independently evaluated current window."""

    metric: Metric
    scenario: ScenarioName
    ordinal: int


@dataclass(frozen=True, slots=True)
class TriggerEffect:
    """Raw effect evidence from both windows supporting one trigger."""

    signal: str
    previous_effect: float
    current_effect: float
    minimum_effect: float
    effect_floor: float
    minimum_gate_ratio: float


@dataclass(frozen=True, slots=True)
class ScenarioTrialResult:
    """One metric/scenario replay scored against known injection ground truth."""

    metric: str
    scenario: str
    expected_classification: str
    injection_pair_index: int
    total_pairs: int
    first_drift_pair_index: int | None
    non_overlapping_pair_delay: int | None
    false_negative: bool
    first_drift_classification: str | None
    classification_correct: bool | None
    triggering_signals: tuple[str, ...]
    triggering_effects: tuple[TriggerEffect, ...]
    detector_confidence: float | None
    detector_magnitude: float | None
    pre_injection_drift_pairs: tuple[int, ...]
    decision_states: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ScenarioDescriptiveSummary:
    """Descriptive rates across exactly the two metric trials for a scenario."""

    scenario: str
    metric_trials: int
    detected_trials: int
    false_negative_count: int
    false_negative_rate: float
    classification_evaluable_trials: int
    classification_correct_count: int
    classification_accuracy_among_detected: float | None


@dataclass(frozen=True, slots=True)
class DriftInjectionResult:
    """Complete offline evidence with no inferential interpretation."""

    experiment: str
    master_seed: int
    generator: str
    dimensions: int
    baseline_pairs: int
    injection_pair_index: int
    delay_semantics: str
    worker_processes: int
    trials: tuple[ScenarioTrialResult, ...]
    scenario_descriptive_summaries: tuple[ScenarioDescriptiveSummary, ...]
    aggregate_descriptive_total_trials: int
    aggregate_descriptive_false_negatives: int
    aggregate_descriptive_classification_correct: int
    aggregate_descriptive_classification_evaluable: int
    descriptive_note: str
    adr_002_status: str
    elapsed_seconds: float


_WORKER_REFERENCES: dict[Metric, StationaryWindow] | None = None
_WORKER_MASTER_SEED: int | None = None
_WORKER_DIMENSIONS: int | None = None


def _metric_stream_id(metric: Metric) -> int:
    return 0 if metric is Metric.L2 else 1


def _rng(
    *,
    master_seed: int,
    metric: Metric,
    stream_id: int,
    ordinal: int,
):
    seed = np.random.SeedSequence(
        entropy=master_seed,
        spawn_key=(_metric_stream_id(metric), stream_id, ordinal),
    )
    return np.random.Generator(np.random.PCG64(seed))


def _reference_window_id(metric: Metric) -> str:
    return f"drift-injection-{metric.value}-reference"


def _current_window_id(metric: Metric, scenario: ScenarioName, ordinal: int) -> str:
    return f"drift-injection-{metric.value}-{scenario.value}-current-{ordinal:04d}"


def _injected_window_index(ordinal: int) -> int | None:
    first_injected_ordinal = 2 * INJECTION_PAIR_INDEX + 1
    if ordinal < first_injected_ordinal:
        return None
    return ordinal - first_injected_ordinal


def window_parameters(spec: ScenarioSpec, ordinal: int) -> WindowParameters:
    """Return the pre-registered parameters for a current-window ordinal."""

    if not 1 <= ordinal <= spec.total_current_windows:
        raise ValueError("ordinal is outside the scenario schedule")
    injected_index = _injected_window_index(ordinal)
    vector_mean = BASE_VECTOR_MEAN
    l2_threshold_mean = BASE_L2_THRESHOLD_MEAN
    cosine_threshold_low = BASE_COSINE_THRESHOLD_LOW
    cardinality_lambda = BASE_CARDINALITY_LAMBDA
    recall_beta_a, recall_beta_b = BASE_RECALL_BETA

    if injected_index is not None:
        if spec.name is ScenarioName.ABRUPT_VECTOR_ONLY:
            vector_mean = INJECTED_VECTOR_MEAN
        elif spec.name is ScenarioName.ABRUPT_THRESHOLD_ONLY:
            l2_threshold_mean = INJECTED_L2_THRESHOLD_MEAN
            cosine_threshold_low = INJECTED_COSINE_THRESHOLD_LOW
        elif spec.name is ScenarioName.ABRUPT_CARDINALITY_ONLY:
            cardinality_lambda = INJECTED_CARDINALITY_LAMBDA
        elif spec.name is ScenarioName.ABRUPT_QUALITY_ONLY:
            recall_beta_a, recall_beta_b = INJECTED_RECALL_BETA
        elif spec.name is ScenarioName.GRADUAL_VECTOR:
            ramp_windows = 2 * spec.ramp_pairs
            if injected_index < ramp_windows:
                vector_mean = float(
                    np.linspace(
                        BASE_VECTOR_MEAN,
                        INJECTED_VECTOR_MEAN,
                        num=ramp_windows,
                        dtype=np.float64,
                    )[injected_index]
                )
            else:
                vector_mean = INJECTED_VECTOR_MEAN
        else:
            raise ValueError(f"unsupported scenario: {spec.name}")

    return WindowParameters(
        vector_mean=vector_mean,
        l2_threshold_mean=l2_threshold_mean,
        cosine_threshold_low=cosine_threshold_low,
        cardinality_lambda=cardinality_lambda,
        recall_beta_a=recall_beta_a,
        recall_beta_b=recall_beta_b,
    )


def _make_window(
    *,
    master_seed: int,
    metric: Metric,
    stream_id: int,
    ordinal: int,
    window_id: str,
    dimensions: int,
    parameters: WindowParameters,
) -> StationaryWindow:
    rng = _rng(
        master_seed=master_seed,
        metric=metric,
        stream_id=stream_id,
        ordinal=ordinal,
    )
    query_vectors = rng.normal(
        loc=parameters.vector_mean,
        scale=VECTOR_STDDEV,
        size=(ELIGIBLE_QUERY_COUNT, dimensions),
    ).astype(np.float64)
    if metric is Metric.L2:
        thresholds = rng.lognormal(
            mean=parameters.l2_threshold_mean,
            sigma=L2_THRESHOLD_SIGMA,
            size=ELIGIBLE_QUERY_COUNT,
        ).astype(np.float64)
    else:
        thresholds = rng.uniform(
            low=parameters.cosine_threshold_low,
            high=COSINE_THRESHOLD_HIGH,
            size=ELIGIBLE_QUERY_COUNT,
        ).astype(np.float64)
    exact_cardinalities = rng.poisson(
        lam=parameters.cardinality_lambda,
        size=AUDIT_QUERY_COUNT,
    ).astype(np.int64)

    selection = select_audit_sample(
        tuple(range(ELIGIBLE_QUERY_COUNT)),
        detector_seed=master_seed,
        metric=metric,
        window_id=window_id,
    )
    if not selection.complete:
        raise RuntimeError(f"audit selection failed: {selection.reason}")
    recall_values = rng.beta(
        a=parameters.recall_beta_a,
        b=parameters.recall_beta_b,
        size=AUDIT_QUERY_COUNT,
    ).astype(np.float64)
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


def generate_reference(
    metric: Metric,
    *,
    master_seed: int = MASTER_SEED,
    dimensions: int = DIMENSIONS,
) -> StationaryWindow:
    """Generate the one immutable stationary reference for a metric."""

    parameters = WindowParameters(
        vector_mean=BASE_VECTOR_MEAN,
        l2_threshold_mean=BASE_L2_THRESHOLD_MEAN,
        cosine_threshold_low=BASE_COSINE_THRESHOLD_LOW,
        cardinality_lambda=BASE_CARDINALITY_LAMBDA,
        recall_beta_a=BASE_RECALL_BETA[0],
        recall_beta_b=BASE_RECALL_BETA[1],
    )
    return _make_window(
        master_seed=master_seed,
        metric=metric,
        stream_id=0,
        ordinal=0,
        window_id=_reference_window_id(metric),
        dimensions=dimensions,
        parameters=parameters,
    )


def generate_current_window(
    metric: Metric,
    spec: ScenarioSpec,
    ordinal: int,
    *,
    master_seed: int = MASTER_SEED,
    dimensions: int = DIMENSIONS,
) -> StationaryWindow:
    """Generate one deterministic current window under the frozen scenario."""

    return _make_window(
        master_seed=master_seed,
        metric=metric,
        stream_id=spec.stream_id,
        ordinal=ordinal,
        window_id=_current_window_id(metric, spec.name, ordinal),
        dimensions=dimensions,
        parameters=window_parameters(spec, ordinal),
    )


def _initialize_worker(
    references: dict[Metric, StationaryWindow],
    master_seed: int,
    dimensions: int,
) -> None:
    global _WORKER_REFERENCES, _WORKER_MASTER_SEED, _WORKER_DIMENSIONS
    _WORKER_REFERENCES = references
    _WORKER_MASTER_SEED = master_seed
    _WORKER_DIMENSIONS = dimensions


def _evaluate_task(task: WindowTask) -> tuple[WindowTask, WindowEvidence]:
    if (
        _WORKER_REFERENCES is None
        or _WORKER_MASTER_SEED is None
        or _WORKER_DIMENSIONS is None
    ):
        raise RuntimeError("drift-injection replay worker was not initialized")
    spec = SCENARIO_BY_NAME[task.scenario]
    current = generate_current_window(
        task.metric,
        spec,
        task.ordinal,
        master_seed=_WORKER_MASTER_SEED,
        dimensions=_WORKER_DIMENSIONS,
    )
    evidence = _evaluate_window(
        _WORKER_REFERENCES[task.metric],
        current,
        detector_seed=_WORKER_MASTER_SEED,
    )
    return task, evidence


def _tasks() -> tuple[WindowTask, ...]:
    return tuple(
        WindowTask(metric=metric, scenario=spec.name, ordinal=ordinal)
        for metric in METRICS
        for spec in SCENARIOS
        for ordinal in range(1, spec.total_current_windows + 1)
    )


def _evaluate_all_windows(
    *,
    master_seed: int,
    dimensions: int,
    workers: int,
    progress: ProgressCallback | None,
) -> dict[tuple[Metric, ScenarioName], tuple[WindowEvidence, ...]]:
    if workers <= 0:
        raise ValueError("workers must be positive")
    references = {
        metric: generate_reference(
            metric, master_seed=master_seed, dimensions=dimensions
        )
        for metric in METRICS
    }
    tasks = _tasks()
    grouped: dict[tuple[Metric, ScenarioName], list[WindowEvidence]] = {
        (metric, spec.name): [] for metric in METRICS for spec in SCENARIOS
    }

    if workers == 1:
        _initialize_worker(references, master_seed, dimensions)
        evaluated = map(_evaluate_task, tasks)
    else:
        executor = ProcessPoolExecutor(
            max_workers=workers,
            initializer=_initialize_worker,
            initargs=(references, master_seed, dimensions),
        )
        evaluated = executor.map(_evaluate_task, tasks, chunksize=1)

    try:
        for task, evidence in evaluated:
            key = (task.metric, task.scenario)
            grouped[key].append(evidence)
            if progress is not None:
                progress(
                    task.metric,
                    task.scenario.value,
                    len(grouped[key]),
                    SCENARIO_BY_NAME[task.scenario].total_current_windows,
                )
    finally:
        if workers != 1:
            executor.shutdown()

    return {key: tuple(windows) for key, windows in grouped.items()}


def _trigger_effects(
    previous: WindowEvidence,
    current: WindowEvidence,
    signals: Sequence[Signal],
) -> tuple[TriggerEffect, ...]:
    previous_by_signal = previous.by_signal()
    current_by_signal = current.by_signal()
    effects: list[TriggerEffect] = []
    for signal in signals:
        previous_evidence = previous_by_signal[signal]
        current_evidence = current_by_signal[signal]
        previous_effect = float(previous_evidence.effect)
        current_effect = float(current_evidence.effect)
        effect_floor = float(previous_evidence.effect_floor)
        effects.append(
            TriggerEffect(
                signal=signal.value,
                previous_effect=previous_effect,
                current_effect=current_effect,
                minimum_effect=min(previous_effect, current_effect),
                effect_floor=effect_floor,
                minimum_gate_ratio=min(previous_effect, current_effect) / effect_floor,
            )
        )
    return tuple(effects)


def score_scenario(
    metric: Metric,
    spec: ScenarioSpec,
    windows: Sequence[WindowEvidence],
) -> ScenarioTrialResult:
    """Score one replay using its frozen onset and classification ground truth."""

    if len(windows) != spec.total_current_windows:
        raise ValueError("window count does not match the frozen scenario schedule")
    decisions = tuple(
        evaluate_drift_decision(windows[index], windows[index + 1])
        for index in range(0, len(windows), 2)
    )
    pre_injection_drift_pairs = tuple(
        pair_index
        for pair_index, decision in enumerate(decisions[:INJECTION_PAIR_INDEX])
        if decision.state is DetectorState.DRIFT
    )
    first_drift_pair_index: int | None = None
    first_decision = None
    for pair_index in range(INJECTION_PAIR_INDEX, len(decisions)):
        if decisions[pair_index].state is DetectorState.DRIFT:
            first_drift_pair_index = pair_index
            first_decision = decisions[pair_index]
            break

    if first_drift_pair_index is None or first_decision is None:
        return ScenarioTrialResult(
            metric=metric.value,
            scenario=spec.name.value,
            expected_classification=spec.expected_classification.value,
            injection_pair_index=INJECTION_PAIR_INDEX,
            total_pairs=spec.total_pairs,
            first_drift_pair_index=None,
            non_overlapping_pair_delay=None,
            false_negative=True,
            first_drift_classification=None,
            classification_correct=None,
            triggering_signals=(),
            triggering_effects=(),
            detector_confidence=None,
            detector_magnitude=None,
            pre_injection_drift_pairs=pre_injection_drift_pairs,
            decision_states=tuple(decision.state.value for decision in decisions),
        )

    previous = windows[2 * first_drift_pair_index]
    current = windows[2 * first_drift_pair_index + 1]
    return ScenarioTrialResult(
        metric=metric.value,
        scenario=spec.name.value,
        expected_classification=spec.expected_classification.value,
        injection_pair_index=INJECTION_PAIR_INDEX,
        total_pairs=spec.total_pairs,
        first_drift_pair_index=first_drift_pair_index,
        non_overlapping_pair_delay=(first_drift_pair_index - INJECTION_PAIR_INDEX),
        false_negative=False,
        first_drift_classification=first_decision.classification.value,
        classification_correct=(
            first_decision.classification is spec.expected_classification
        ),
        triggering_signals=tuple(
            signal.value for signal in first_decision.triggering_signals
        ),
        triggering_effects=_trigger_effects(
            previous,
            current,
            first_decision.triggering_signals,
        ),
        detector_confidence=first_decision.significance_evidence_score,
        detector_magnitude=first_decision.drift_magnitude,
        pre_injection_drift_pairs=pre_injection_drift_pairs,
        decision_states=tuple(decision.state.value for decision in decisions),
    )


def _summarize_scenario(
    spec: ScenarioSpec,
    trials: Sequence[ScenarioTrialResult],
) -> ScenarioDescriptiveSummary:
    matching = [trial for trial in trials if trial.scenario == spec.name.value]
    if len(matching) != len(METRICS):
        raise ValueError("scenario summary requires one trial per metric")
    detected = [trial for trial in matching if not trial.false_negative]
    correct = sum(trial.classification_correct is True for trial in detected)
    return ScenarioDescriptiveSummary(
        scenario=spec.name.value,
        metric_trials=len(matching),
        detected_trials=len(detected),
        false_negative_count=sum(trial.false_negative for trial in matching),
        false_negative_rate=sum(trial.false_negative for trial in matching)
        / len(matching),
        classification_evaluable_trials=len(detected),
        classification_correct_count=correct,
        classification_accuracy_among_detected=(
            correct / len(detected) if detected else None
        ),
    )


def run_drift_injection(
    *,
    master_seed: int = MASTER_SEED,
    dimensions: int = DIMENSIONS,
    workers: int = 1,
    progress: ProgressCallback | None = None,
) -> DriftInjectionResult:
    """Run all ten metric/scenario trials through the real detector."""

    if dimensions <= 0:
        raise ValueError("dimensions must be positive")
    started = time.monotonic()
    evidence = _evaluate_all_windows(
        master_seed=master_seed,
        dimensions=dimensions,
        workers=workers,
        progress=progress,
    )
    trials = tuple(
        score_scenario(metric, spec, evidence[(metric, spec.name)])
        for metric in METRICS
        for spec in SCENARIOS
    )
    summaries = tuple(_summarize_scenario(spec, trials) for spec in SCENARIOS)
    return DriftInjectionResult(
        experiment="ADR-002 offline drift-injection validation",
        master_seed=master_seed,
        generator="NumPy Generator(PCG64) with SeedSequence spawn keys",
        dimensions=dimensions,
        baseline_pairs=BASELINE_PAIRS,
        injection_pair_index=INJECTION_PAIR_INDEX,
        delay_semantics=DELAY_SEMANTICS,
        worker_processes=workers,
        trials=trials,
        scenario_descriptive_summaries=summaries,
        aggregate_descriptive_total_trials=len(trials),
        aggregate_descriptive_false_negatives=sum(
            trial.false_negative for trial in trials
        ),
        aggregate_descriptive_classification_correct=sum(
            trial.classification_correct is True for trial in trials
        ),
        aggregate_descriptive_classification_evaluable=sum(
            trial.classification_correct is not None for trial in trials
        ),
        descriptive_note=DESCRIPTIVE_NOTE,
        adr_002_status="Proposed",
        elapsed_seconds=time.monotonic() - started,
    )


def _print_progress(metric: Metric, scenario: str, completed: int, total: int) -> None:
    if completed == 1 or completed == total:
        print(
            f"progress metric={metric.value} scenario={scenario} "
            f"windows={completed}/{total}",
            file=sys.stderr,
            flush=True,
        )


def _format_trigger_effects(effects: Sequence[TriggerEffect]) -> str:
    if not effects:
        return "none"
    return ";".join(
        f"{effect.signal}(previous={effect.previous_effect:.12f},"
        f"current={effect.current_effect:.12f},"
        f"minimum={effect.minimum_effect:.12f},"
        f"floor={effect.effect_floor:.12f},"
        f"minimum_gate_ratio={effect.minimum_gate_ratio:.12f})"
        for effect in effects
    )


def _print_plain_results(result: DriftInjectionResult) -> None:
    print(f"master_seed={result.master_seed}")
    print(f"injection_pair_index={result.injection_pair_index}")
    print(f"delay_semantics={result.delay_semantics}")
    for trial in result.trials:
        prefix = f"metric={trial.metric} scenario={trial.scenario}"
        print(f"{prefix} first_DRIFT_pair_index={trial.first_drift_pair_index}")
        print(
            f"{prefix} non_overlapping_pair_delay="
            f"{trial.non_overlapping_pair_delay}"
        )
        print(f"{prefix} false_negative={int(trial.false_negative)}")
        print(
            f"{prefix} first_DRIFT_classification="
            f"{trial.first_drift_classification}"
        )
        correctness = (
            None
            if trial.classification_correct is None
            else int(trial.classification_correct)
        )
        print(f"{prefix} classification_correct={correctness}")
        print(
            f"{prefix} triggering_effects="
            f"{_format_trigger_effects(trial.triggering_effects)}"
        )
    for summary in result.scenario_descriptive_summaries:
        print(
            f"scenario={summary.scenario} descriptive_false_negative_rate="
            f"{summary.false_negative_count}/{summary.metric_trials}="
            f"{summary.false_negative_rate:.12f}"
        )
        accuracy = (
            "None"
            if summary.classification_accuracy_among_detected is None
            else f"{summary.classification_accuracy_among_detected:.12f}"
        )
        print(
            f"scenario={summary.scenario} descriptive_classification_accuracy="
            f"{summary.classification_correct_count}/"
            f"{summary.classification_evaluable_trials}={accuracy}"
        )
    print(f"descriptive_note={result.descriptive_note}")
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
    result = run_drift_injection(
        master_seed=MASTER_SEED,
        dimensions=DIMENSIONS,
        workers=args.workers,
        progress=_print_progress,
    )
    _print_plain_results(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
