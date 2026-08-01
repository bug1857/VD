import unittest

from vdbench.config import (
    HNSW_EF_SWEEP,
    EXP001_DATASET_SPEC,
    ContractViolation,
    IndexTrack,
    Metric,
    SearchConfiguration,
    build_search_configurations,
)
from vdbench.protocol import build_schedule


class ConfigurationScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.configurations = build_search_configurations(
            {Metric.L2: (1.0, 2.0, 3.0), Metric.COSINE: (0.9, 0.5, 0.1)}
        )

    def test_exact_36_configuration_matrix_and_ef_sweep(self) -> None:
        self.assertEqual(len(self.configurations), 36)
        observed = {
            value.ef
            for value in self.configurations
            if value.index_track is IndexTrack.HNSW
        }
        self.assertEqual(observed, set(HNSW_EF_SWEEP))

    def test_dataset_defaults_are_exactly_the_exp001_contract(self) -> None:
        self.assertEqual(EXP001_DATASET_SPEC.seed, 20260801)
        self.assertEqual(EXP001_DATASET_SPEC.base_count, 10_000)
        self.assertEqual(EXP001_DATASET_SPEC.calibration_query_count, 50)
        self.assertEqual(EXP001_DATASET_SPEC.measured_query_count, 200)
        self.assertEqual(EXP001_DATASET_SPEC.dimensions, 128)
        self.assertEqual(EXP001_DATASET_SPEC.dtype, "<f4")
        self.assertEqual(
            EXP001_DATASET_SPEC.generator,
            "numpy.random.Generator(numpy.random.PCG64(seed))",
        )

    def test_ef_below_limit_is_rejected_before_backend_call(self) -> None:
        configuration = SearchConfiguration(
            metric=Metric.L2,
            threshold_label="target-005",
            radius=1.0,
            index_track=IndexTrack.HNSW,
            ef=99,
        )
        with self.assertRaisesRegex(ContractViolation, "one of"):
            configuration.validate()

    def test_schedule_is_deterministic_and_has_exact_protocol_counts(self) -> None:
        first = build_schedule(self.configurations)
        second = build_schedule(self.configurations)
        self.assertEqual(first, second)
        self.assertEqual(len(first.warmup), 36)
        self.assertTrue(all(len(value.query_order) == 50 for value in first.warmup))
        self.assertEqual(len(first.repetitions), 5)
        self.assertTrue(
            all(
                len(repetition.configurations) == 36
                and all(len(value.query_order) == 200 for value in repetition.configurations)
                for repetition in first.repetitions
            )
        )
        for repetition in first.repetitions:
            self.assertEqual(
                len({value.query_order for value in repetition.configurations}), 1
            )
            self.assertEqual(
                len({value.query_seed for value in repetition.configurations}), 1
            )


if __name__ == "__main__":
    unittest.main()
