import unittest

from vdbench.metrics import summarize_records


class MetricSummaryTests(unittest.TestCase):
    def test_required_metrics_are_derived_from_five_complete_repetitions(self) -> None:
        records = []
        for repetition in range(5):
            for query in range(200):
                records.append(
                    {
                        "status": "success",
                        "configuration_key": "L2:target-005:HNSW:ef=100",
                        "repetition": repetition,
                        "latency_ns": 1_000_000 + repetition * 100_000 + query,
                        "recall_at_threshold": 1.0,
                        "result_cardinality": 5,
                        "oracle_full_cardinality": 5,
                        "threshold_violations": [],
                    }
                )
        summary = summarize_records(records)
        result = summary["configurations"]["L2:target-005:HNSW:ef=100"]
        self.assertEqual(result["recall_at_threshold"]["mean"], 1.0)
        self.assertIsNotNone(result["p50_latency_ms"])
        self.assertIsNotNone(result["p95_latency_ms"])
        self.assertIsNotNone(result["qps"])
        self.assertEqual(result["cardinality"]["mean_returned"], 5)
        self.assertTrue(result["diagnostics"]["valid_qps_comparison"])

    def test_any_failed_query_invalidates_qps_comparison(self) -> None:
        records = [
            {
                "status": "failed",
                "configuration_key": "L2:target-005:HNSW:ef=100",
                "repetition": 0,
                "latency_ns": 1,
            }
        ]
        result = summarize_records(records)["configurations"][
            "L2:target-005:HNSW:ef=100"
        ]
        self.assertIsNone(result["qps"])
        self.assertFalse(result["diagnostics"]["valid_qps_comparison"])


if __name__ == "__main__":
    unittest.main()
