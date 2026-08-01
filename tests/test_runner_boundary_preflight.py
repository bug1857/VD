import unittest

from vdbench.dataset import boundary_fixtures
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.runner import validate_boundary_fixtures_live


class FakeBoundaryHarness:
    def __init__(self) -> None:
        self.created = []
        self.expected = {fixture.name: fixture.expected_ids for fixture in boundary_fixtures()}

    def create_and_load_collection(self, **kwargs):
        self.created.append(kwargs)
        return CollectionIdentity(
            kwargs["name"], kwargs["metric"].value, kwargs["track"].value, {}
        )

    def search(self, *, name, query, configuration):
        return tuple(SearchHit(identifier, 0.0) for identifier in self.expected[configuration.threshold_label])


class BoundaryPreflightTests(unittest.TestCase):
    def test_all_boundary_fixtures_are_loaded_and_compared_before_timing(self) -> None:
        backend = FakeBoundaryHarness()
        results = validate_boundary_fixtures_live(
            backend=backend, collection_prefix="unit_exp002"
        )
        self.assertEqual(len(backend.created), len(boundary_fixtures()))
        self.assertEqual(len(results), len(boundary_fixtures()))
        self.assertTrue(all(result["status"] == "matched" for result in results))
        self.assertTrue(all(item["track"].value == "FLAT" for item in backend.created))


if __name__ == "__main__":
    unittest.main()
