"""Post-timing EXP-001 metric aggregation from immutable raw query records."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from statistics import mean, median, stdev

import numpy as np

from .config import MEASURED_REPETITIONS


def _distribution(values: Sequence[float], *, critical: float = 1.96) -> dict[str, float]:
    if not values:
        raise ValueError("cannot summarize an empty distribution")
    average = mean(values)
    deviation = stdev(values) if len(values) > 1 else 0.0
    margin = critical * deviation / math.sqrt(len(values))
    return {
        "mean": average,
        "median": median(values),
        "minimum": min(values),
        "sample_stddev": deviation,
        "ci95_low": average - margin,
        "ci95_high": average + margin,
    }


def summarize_records(
    records: Sequence[Mapping[str, object]], *, expected_queries: int = 200
) -> dict[str, object]:
    """Compute recall, latency, QPS, cardinality, and validity diagnostics.

    Per-repetition p50/p95 use NumPy's linear percentile. Their cross-repetition
    95% confidence intervals use Student t(df=4)=2.776445105 for the fixed five
    repetitions. Recall's query-level interval uses a disclosed normal 1.96 CI.
    QPS is successful queries divided by summed client-observed search time; it is
    null for any repetition containing a failure or an incomplete query count.
    """

    groups: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for record in records:
        groups[str(record["configuration_key"])].append(record)

    summaries: dict[str, object] = {}
    for key, group in groups.items():
        by_repetition: dict[int, list[Mapping[str, object]]] = defaultdict(list)
        for record in group:
            by_repetition[int(record["repetition"])].append(record)

        repetition_rows: list[dict[str, object]] = []
        p50_values: list[float] = []
        p95_values: list[float] = []
        qps_values: list[float] = []
        all_successes: list[Mapping[str, object]] = []
        total_failures = 0
        for repetition in sorted(by_repetition):
            rows = by_repetition[repetition]
            successes = [row for row in rows if row["status"] == "success"]
            failures = [row for row in rows if row["status"] != "success"]
            total_failures += len(failures)
            all_successes.extend(successes)
            latencies_ms = [float(row["latency_ns"]) / 1_000_000 for row in successes]
            p50 = float(np.percentile(latencies_ms, 50)) if latencies_ms else None
            p95 = float(np.percentile(latencies_ms, 95)) if latencies_ms else None
            valid_qps = len(successes) == expected_queries and not failures
            elapsed_seconds = sum(latencies_ms) / 1000.0
            qps = expected_queries / elapsed_seconds if valid_qps and elapsed_seconds else None
            if p50 is not None:
                p50_values.append(p50)
                p95_values.append(p95)  # type: ignore[arg-type]
            if qps is not None:
                qps_values.append(qps)
            repetition_rows.append(
                {
                    "repetition": repetition,
                    "successful_queries": len(successes),
                    "failed_queries": len(failures),
                    "p50_latency_ms": p50,
                    "p95_latency_ms": p95,
                    "qps": qps,
                }
            )

        recalls = [float(row["recall_at_threshold"]) for row in all_successes]
        returned = [int(row["result_cardinality"]) for row in all_successes]
        full = [int(row["oracle_full_cardinality"]) for row in all_successes]
        capped_reference = [min(value, 100) for value in full]
        p95_summary = (
            _distribution(p95_values, critical=2.776445105)
            if len(p95_values) == MEASURED_REPETITIONS
            else None
        )
        summaries[key] = {
            "repetitions": repetition_rows,
            "recall_at_threshold": _distribution(recalls) if recalls else None,
            "p50_latency_ms": (
                _distribution(p50_values, critical=2.776445105)
                if len(p50_values) == MEASURED_REPETITIONS
                else None
            ),
            "p95_latency_ms": p95_summary,
            "p95_coefficient_of_variation": (
                p95_summary["sample_stddev"] / p95_summary["mean"]
                if p95_summary and p95_summary["mean"]
                else None
            ),
            "qps": (
                _distribution(qps_values, critical=2.776445105)
                if len(qps_values) == MEASURED_REPETITIONS
                else None
            ),
            "cardinality": {
                "mean_returned": mean(returned) if returned else None,
                "mean_full_oracle": mean(full) if full else None,
                "fraction_oracle_capped": (
                    sum(value > 100 for value in full) / len(full) if full else None
                ),
                "empty_result_rate": (
                    sum(value == 0 for value in returned) / len(returned)
                    if returned
                    else None
                ),
                "mean_absolute_count_difference": (
                    mean(
                        abs(actual - reference)
                        for actual, reference in zip(returned, capped_reference, strict=True)
                    )
                    if returned
                    else None
                ),
            },
            "diagnostics": {
                "failed_query_count": total_failures,
                "threshold_violation_count": sum(
                    len(row["threshold_violations"]) for row in all_successes
                ),
                "valid_qps_comparison": len(qps_values) == MEASURED_REPETITIONS,
            },
        }
    return {
        "method": {
            "latency_percentile": "numpy.percentile(method=linear)",
            "cross_repetition_ci95": "Student t, df=4, critical=2.776445105",
            "recall_ci95": "normal approximation, critical=1.96",
            "qps_denominator": "sum of client-observed measured search latency",
        },
        "configurations": summaries,
    }
