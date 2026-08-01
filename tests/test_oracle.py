import math
import unittest

import numpy as np

from vdbench.config import ContractViolation, Metric
from vdbench.oracle import exact_scores


class ExactOracleTests(unittest.TestCase):
    def test_l2_is_squared_euclidean_with_float64_output(self) -> None:
        base = np.asarray([[3.0, 4.0], [-3.0, 4.0]], dtype="<f4")
        scores = exact_scores(base, np.asarray([0.0, 0.0], dtype="<f4"), Metric.L2)
        self.assertEqual(scores.dtype, np.dtype("float64"))
        np.testing.assert_array_equal(scores, np.asarray([25.0, 25.0]))

    def test_cosine_known_values_use_float64_output(self) -> None:
        base = np.asarray([[1.0, 0.0], [1.0, 1.0]], dtype="<f4")
        scores = exact_scores(base, np.asarray([1.0, 0.0], dtype="<f4"), Metric.COSINE)
        self.assertEqual(scores.dtype, np.dtype("float64"))
        np.testing.assert_allclose(scores, [1.0, 1.0 / math.sqrt(2.0)], rtol=0, atol=1e-15)

    def test_cosine_rejects_zero_norm_vectors(self) -> None:
        with self.assertRaisesRegex(ContractViolation, "zero-norm"):
            exact_scores(
                np.asarray([[0.0, 0.0]], dtype="<f4"),
                np.asarray([1.0, 0.0], dtype="<f4"),
                Metric.COSINE,
            )


if __name__ == "__main__":
    unittest.main()
