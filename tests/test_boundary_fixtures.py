import unittest

import numpy as np

from vdbench.dataset import boundary_fixtures
from vdbench.oracle import exact_range_search


class BoundaryFixtureTests(unittest.TestCase):
    def test_required_categories_and_exact_outputs(self) -> None:
        fixtures = boundary_fixtures()
        self.assertEqual(
            {fixture.category for fixture in fixtures},
            {
                "threshold-equality",
                "empty-result",
                "all-match",
                "duplicate-distance",
                "result-cap",
            },
        )
        for fixture in fixtures:
            with self.subTest(fixture=fixture.name):
                result = exact_range_search(
                    np.asarray(fixture.base_vectors, dtype="<f4"),
                    np.asarray(fixture.ids, dtype=np.int64),
                    np.asarray(fixture.query, dtype="<f4"),
                    fixture.metric,
                    radius=fixture.radius,
                    range_filter=fixture.range_filter,
                    limit=fixture.limit,
                )
                self.assertEqual(result.ids, fixture.expected_ids)
                self.assertEqual(result.full_count, fixture.expected_full_count)

    def test_result_cap_reports_uncapped_cardinality(self) -> None:
        fixture = next(value for value in boundary_fixtures() if value.category == "result-cap")
        result = exact_range_search(
            np.asarray(fixture.base_vectors, dtype="<f4"),
            np.asarray(fixture.ids),
            np.asarray(fixture.query, dtype="<f4"),
            fixture.metric,
            radius=fixture.radius,
            range_filter=fixture.range_filter,
            limit=fixture.limit,
        )
        self.assertEqual(len(result.hits), 100)
        self.assertEqual(result.full_count, 105)
        self.assertTrue(result.capped)


if __name__ == "__main__":
    unittest.main()
