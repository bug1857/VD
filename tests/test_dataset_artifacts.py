import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np

from vdbench.artifacts import verify_dataset_artifacts, write_dataset_artifacts
from vdbench.config import EXP001_DATASET_SPEC, ContractViolation, Metric
from vdbench.dataset import (
    boundary_fixtures,
    calibrate_thresholds,
    generate_dataset,
)


class DatasetArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = replace(
            EXP001_DATASET_SPEC,
            version="unit-fixture-v1",
            dimensions=4,
            base_count=100,
            calibration_query_count=5,
            measured_query_count=7,
        )

    def test_generator_is_deterministic_disjoint_and_little_endian_float32(self) -> None:
        first = generate_dataset(self.spec)
        second = generate_dataset(self.spec)
        self.assertTrue(np.array_equal(first.base_vectors, second.base_vectors))
        self.assertTrue(np.array_equal(first.calibration_queries, second.calibration_queries))
        self.assertTrue(np.array_equal(first.measured_queries, second.measured_queries))
        self.assertEqual(first.base_vectors.dtype.str, "<f4")
        self.assertEqual(first.calibration_queries.shape, (5, 4))
        self.assertEqual(first.measured_queries.shape, (7, 4))
        self.assertFalse(
            np.any(
                np.all(
                    first.calibration_queries[:, None, :]
                    == first.measured_queries[None, :, :],
                    axis=2,
                )
            )
        )

    def test_every_written_artifact_is_checksummed_and_tampering_is_detected(self) -> None:
        bundle = generate_dataset(self.spec)
        thresholds = calibrate_thresholds(bundle.base_vectors, bundle.calibration_queries)
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "dataset"
            manifest = write_dataset_artifacts(
                output, bundle, thresholds, boundary_fixtures()
            )
            verified = verify_dataset_artifacts(output)
            self.assertEqual(verified, manifest)
            self.assertEqual(
                set(manifest["artifacts"]),
                {
                    "base_ids.npy",
                    "base_vectors.npy",
                    "calibration_queries.npy",
                    "measured_queries.npy",
                    "thresholds.json",
                    "boundary_fixtures.json",
                },
            )
            sums = (output / "SHA256SUMS").read_text(encoding="utf-8")
            self.assertIn("generation_manifest.json", sums)
            with (output / "measured_queries.npy").open("ab") as handle:
                handle.write(b"tamper")
            with self.assertRaisesRegex(ContractViolation, "size mismatch"):
                verify_dataset_artifacts(output)

    def test_calibration_emits_three_finite_thresholds_per_metric(self) -> None:
        bundle = generate_dataset(self.spec)
        thresholds = calibrate_thresholds(bundle.base_vectors, bundle.calibration_queries)
        for metric in Metric:
            self.assertEqual(len(thresholds[metric]), 3)
            self.assertTrue(all(np.isfinite(value.radius) for value in thresholds[metric]))


if __name__ == "__main__":
    unittest.main()
