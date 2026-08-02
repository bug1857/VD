import ast
import importlib.util
import math
from pathlib import Path
import sys
import unittest

from vdbench.config import Metric

EXPERIMENT_PATH = (
    Path(__file__).parents[1] / "experiments" / "adr002_stationary_false_positive.py"
)
SPEC = importlib.util.spec_from_file_location(
    "adr002_stationary_false_positive", EXPERIMENT_PATH
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load stationary false-positive experiment")
experiment = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = experiment
SPEC.loader.exec_module(experiment)


class ClopperPearsonTests(unittest.TestCase):
    def test_zero_false_positives_matches_closed_form(self) -> None:
        expected = 1.0 - 0.05 ** (1.0 / 299.0)
        actual = experiment.clopper_pearson_upper(0, 299)
        self.assertAlmostEqual(actual, expected, places=15)

    def test_all_false_positives_has_upper_bound_one(self) -> None:
        self.assertEqual(experiment.clopper_pearson_upper(7, 7), 1.0)

    def test_general_bound_inverts_binomial_cdf(self) -> None:
        upper = experiment.clopper_pearson_upper(2, 20)
        self.assertAlmostEqual(experiment._binomial_cdf(2, 20, upper), 0.05, places=12)


class StationaryReplayContractTests(unittest.TestCase):
    def test_fixed_full_replay_contract(self) -> None:
        self.assertEqual(experiment.MASTER_SEED, 20_260_802)
        self.assertEqual(experiment.DECISIONS_PER_METRIC, 299)
        self.assertEqual(experiment.CURRENT_WINDOWS_PER_METRIC, 598)
        self.assertEqual(experiment.DIMENSIONS, 128)
        self.assertEqual(experiment.METRICS, (Metric.L2, Metric.COSINE))

    def test_one_non_overlapping_decision_uses_actual_detector(self) -> None:
        result = experiment.run_metric_replay(
            Metric.L2,
            decisions=1,
            master_seed=experiment.MASTER_SEED,
            dimensions=4,
            workers=1,
        )
        self.assertEqual(result.decisions, 1)
        self.assertEqual(result.current_windows, 2)
        self.assertEqual(sum(result.decision_state_counts.values()), 1)
        self.assertEqual(result.false_positives, 0)
        self.assertEqual(result.false_positive_point_estimate, 0.0)
        self.assertTrue(
            math.isclose(
                result.one_sided_95_clopper_pearson_upper,
                0.95,
                rel_tol=0.0,
                abs_tol=1e-15,
            )
        )

    def test_experiment_has_no_pymilvus_import(self) -> None:
        tree = ast.parse(EXPERIMENT_PATH.read_text(encoding="utf-8"))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        self.assertFalse(any(name.startswith("pymilvus") for name in imports))


if __name__ == "__main__":
    unittest.main()
