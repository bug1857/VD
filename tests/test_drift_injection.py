import ast
import math
import unittest
from pathlib import Path

import numpy as np

from experiments import adr002_drift_injection as experiment
from vdbench.drift import DriftClassification

EXPERIMENT_PATH = (
    Path(__file__).parents[1] / "experiments" / "adr002_drift_injection.py"
)


class DriftInjectionContractTests(unittest.TestCase):
    def test_frozen_schedule_and_expected_classifications(self) -> None:
        self.assertEqual(experiment.MASTER_SEED, 20_260_803)
        self.assertEqual(experiment.BASELINE_PAIRS, 2)
        self.assertEqual(experiment.INJECTION_PAIR_INDEX, 2)
        self.assertEqual(experiment.ABRUPT_INJECTED_PAIRS, 6)
        self.assertEqual(experiment.GRADUAL_RAMP_PAIRS, 4)
        self.assertEqual(experiment.GRADUAL_PLATEAU_PAIRS, 4)
        self.assertEqual(
            tuple(spec.total_pairs for spec in experiment.SCENARIOS),
            (8, 8, 8, 8, 10),
        )
        self.assertEqual(
            tuple(spec.expected_classification for spec in experiment.SCENARIOS),
            (
                DriftClassification.INPUT_DRIFT,
                DriftClassification.INPUT_DRIFT,
                DriftClassification.INPUT_DRIFT,
                DriftClassification.QUALITY_DRIFT,
                DriftClassification.INPUT_DRIFT,
            ),
        )

    def test_frozen_abrupt_injection_parameters_start_at_pair_two(self) -> None:
        last_baseline_ordinal = 4
        first_injected_ordinal = 5
        vector = experiment.SCENARIO_BY_NAME[experiment.ScenarioName.ABRUPT_VECTOR_ONLY]
        threshold = experiment.SCENARIO_BY_NAME[
            experiment.ScenarioName.ABRUPT_THRESHOLD_ONLY
        ]
        cardinality = experiment.SCENARIO_BY_NAME[
            experiment.ScenarioName.ABRUPT_CARDINALITY_ONLY
        ]
        quality = experiment.SCENARIO_BY_NAME[
            experiment.ScenarioName.ABRUPT_QUALITY_ONLY
        ]

        self.assertEqual(
            experiment.window_parameters(vector, last_baseline_ordinal).vector_mean,
            0.0,
        )
        self.assertEqual(
            experiment.window_parameters(vector, first_injected_ordinal).vector_mean,
            0.5,
        )
        threshold_parameters = experiment.window_parameters(
            threshold, first_injected_ordinal
        )
        self.assertEqual(threshold_parameters.l2_threshold_mean, 0.5)
        self.assertEqual(threshold_parameters.cosine_threshold_low, 0.47)
        self.assertEqual(
            experiment.window_parameters(
                cardinality, first_injected_ordinal
            ).cardinality_lambda,
            90.0,
        )
        quality_parameters = experiment.window_parameters(
            quality, first_injected_ordinal
        )
        self.assertEqual(
            (quality_parameters.recall_beta_a, quality_parameters.recall_beta_b),
            (90.0, 10.0),
        )

    def test_gradual_vector_schedule_is_eight_window_linear_ramp_then_plateau(
        self,
    ) -> None:
        gradual = experiment.SCENARIO_BY_NAME[experiment.ScenarioName.GRADUAL_VECTOR]
        actual_ramp = np.array(
            [
                experiment.window_parameters(gradual, ordinal).vector_mean
                for ordinal in range(5, 13)
            ]
        )
        np.testing.assert_array_equal(
            actual_ramp,
            np.linspace(0.0, 0.5, num=8, dtype=np.float64),
        )
        self.assertTrue(
            all(
                math.isclose(
                    experiment.window_parameters(gradual, ordinal).vector_mean,
                    0.5,
                )
                for ordinal in range(13, 21)
            )
        )

    def test_reference_and_current_generation_are_deterministic(self) -> None:
        first_reference = experiment.generate_reference(
            experiment.Metric.L2, dimensions=4
        )
        second_reference = experiment.generate_reference(
            experiment.Metric.L2, dimensions=4
        )
        np.testing.assert_array_equal(
            first_reference.query_vectors, second_reference.query_vectors
        )
        spec = experiment.SCENARIO_BY_NAME[
            experiment.ScenarioName.ABRUPT_CARDINALITY_ONLY
        ]
        first_current = experiment.generate_current_window(
            experiment.Metric.COSINE, spec, 5, dimensions=4
        )
        second_current = experiment.generate_current_window(
            experiment.Metric.COSINE, spec, 5, dimensions=4
        )
        np.testing.assert_array_equal(
            first_current.exact_cardinalities,
            second_current.exact_cardinalities,
        )

    def test_delay_label_explicitly_excludes_sliding_window_semantics(self) -> None:
        self.assertIn("non-overlapping-pair", experiment.DELAY_SEMANTICS)
        self.assertIn("distinct from sliding-window", experiment.DELAY_SEMANTICS)

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
