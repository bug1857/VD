"""TDD coverage for the DATASET-003 lkg_qualification loader/validator."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

from vdbench.artifacts import write_dataset_artifacts
from vdbench.config import ContractViolation, EXP001_DATASET_SPEC
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import DATASET002_SPEC, generate_dataset002, write_dataset002_artifacts
from vdbench.dataset003 import Dataset003Spec, generate_dataset003, write_dataset003_artifacts
from vdbench.lkg_dataset003_loader import LkgDataset003Workload, load_dataset003_workload


def _small_dataset001(path: Path) -> None:
    spec = replace(
        EXP001_DATASET_SPEC,
        version="dataset001-fixture-v1",
        dimensions=4,
        base_count=100,
        calibration_query_count=5,
        measured_query_count=7,
    )
    bundle = generate_dataset(spec)
    write_dataset_artifacts(
        path,
        bundle,
        calibrate_thresholds(bundle.base_vectors, bundle.calibration_queries),
        boundary_fixtures(),
    )


def _small_dataset002(path: Path, *, dataset001_dir: Path) -> None:
    spec = replace(
        DATASET002_SPEC,
        version="dataset002-fixture-v1",
        dimensions=4,
        seed=20260809,
        routing_query_count=6,
        recall_audit_query_count=12,
    )
    write_dataset002_artifacts(path, generate_dataset002(spec), dataset001_dir=dataset001_dir)


_DATASET003_SPEC = Dataset003Spec(
    dataset_id="DATASET-003",
    version="dataset003-fixture-v1",
    seed=20260806,
    dimensions=4,
    lkg_qualification_query_count=9,
    dtype="<f4",
    distribution="independent standard normal",
    generator="numpy.random.Generator(numpy.random.PCG64(seed))",
)


def _resign_manifest(output: Path, filenames: tuple[str, ...]) -> None:
    """Recompute manifest artifact entries + SHA256SUMS after editing a file
    in place, mirroring test_dataset003.py's own tamper-fixture pattern."""

    manifest_path = output / "dataset003_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for filename in filenames:
        payload = (output / filename).read_bytes()
        manifest["artifacts"][filename] = {
            "file": filename,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    all_filenames = (*filenames_for_sums(), "dataset003_manifest.json")
    entries = [
        f"{hashlib.sha256((output / name).read_bytes()).hexdigest()}  {name}"
        for name in all_filenames
    ]
    (output / "SHA256SUMS").write_text("\n".join(entries) + "\n", encoding="utf-8")


def filenames_for_sums() -> tuple[str, ...]:
    return (
        "lkg_qualification_ids.npy",
        "lkg_qualification_queries.npy",
        "inherited_dataset001.json",
        "inherited_dataset002.json",
    )


class LkgDataset003LoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self.dataset001_dir = root / "dataset001"
        self.dataset002_dir = root / "dataset002"
        self.output_dir = root / "dataset003"
        _small_dataset001(self.dataset001_dir)
        _small_dataset002(self.dataset002_dir, dataset001_dir=self.dataset001_dir)
        write_dataset003_artifacts(
            self.output_dir,
            generate_dataset003(_DATASET003_SPEC),
            dataset001_dir=self.dataset001_dir,
            dataset002_dir=self.dataset002_dir,
        )

    def _load(self) -> LkgDataset003Workload:
        return load_dataset003_workload(
            self.output_dir,
            dataset001_dir=self.dataset001_dir,
            dataset002_dir=self.dataset002_dir,
        )

    def test_valid_workload_loads_exactly_the_expected_population(self) -> None:
        workload = self._load()
        self.assertEqual(len(workload.query_ids), 9)
        self.assertEqual(len(workload.query_vectors), 9)
        self.assertEqual(set(workload.query_vectors), set(workload.query_ids))

    def test_query_ids_are_strictly_ascending_and_unique(self) -> None:
        workload = self._load()
        self.assertEqual(list(workload.query_ids), sorted(set(workload.query_ids)))

    def test_query_vectors_have_the_expected_dtype_and_shape(self) -> None:
        workload = self._load()
        for query_id in workload.query_ids:
            vector = workload.query_vectors[query_id]
            self.assertEqual(vector.dtype.str, "<f4")
            self.assertEqual(vector.shape, (4,))

    def test_identity_fields_match_the_fixture_spec(self) -> None:
        workload = self._load()
        self.assertEqual(workload.dataset_id, "DATASET-003")
        self.assertEqual(workload.dataset_version, "dataset003-fixture-v1")
        self.assertEqual(workload.query_role, "lkg_qualification")
        self.assertEqual(len(workload.manifest_sha256), 64)
        self.assertEqual(len(workload.query_id_array_sha256), 64)
        self.assertEqual(len(workload.query_array_sha256), 64)
        self.assertNotEqual(workload.query_id_array_sha256, workload.query_array_sha256)

    def test_manifest_sha256_matches_the_actual_manifest_file(self) -> None:
        from vdbench.artifacts import sha256_file

        workload = self._load()
        self.assertEqual(
            workload.manifest_sha256,
            sha256_file(self.output_dir / "dataset003_manifest.json"),
        )

    def test_query_id_array_sha256_matches_the_actual_ids_file(self) -> None:
        from vdbench.artifacts import sha256_file

        workload = self._load()
        self.assertEqual(
            workload.query_id_array_sha256,
            sha256_file(self.output_dir / "lkg_qualification_ids.npy"),
        )

    def test_repeated_loads_are_identical(self) -> None:
        first = self._load()
        second = self._load()
        self.assertEqual(first.query_ids, second.query_ids)
        self.assertEqual(first.manifest_sha256, second.manifest_sha256)

    # -- fails closed before dispatch --------------------------------------------

    def test_tampered_query_array_is_rejected_before_any_workload_is_returned(self) -> None:
        queries_path = self.output_dir / "lkg_qualification_queries.npy"
        array = np.load(queries_path, allow_pickle=False)
        array[0, 0] = array[0, 0] + 1.0
        np.save(queries_path, array, allow_pickle=False)
        # Deliberately do NOT re-sign the manifest/SHA256SUMS: a tampered
        # array with a stale checksum must be caught by the strict verifier.
        with self.assertRaises(ContractViolation):
            self._load()

    def test_tampered_and_resigned_query_array_disagrees_with_the_generator(self) -> None:
        """Even a self-consistent (re-signed) tamper is caught: the strict
        verifier re-derives the array from the deterministic generator and
        requires a byte-exact match, not just internal checksum agreement."""

        queries_path = self.output_dir / "lkg_qualification_queries.npy"
        array = np.load(queries_path, allow_pickle=False)
        array[0, 0] = array[0, 0] + 1.0
        np.save(queries_path, array, allow_pickle=False)
        _resign_manifest(self.output_dir, ("lkg_qualification_queries.npy",))
        with self.assertRaises(ContractViolation):
            self._load()

    def test_missing_output_directory_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            load_dataset003_workload(
                self.output_dir / "does-not-exist",
                dataset001_dir=self.dataset001_dir,
                dataset002_dir=self.dataset002_dir,
            )

    def test_wrong_dataset001_directory_is_rejected(self) -> None:
        # dataset003.py's own verify_dataset_artifacts dependency raises a
        # raw FileNotFoundError for a directory that isn't a DATASET-001
        # artifact directory at all (not this loader's own ContractViolation
        # layer) -- either way, no workload is ever returned.
        with self.assertRaises((ContractViolation, OSError)):
            load_dataset003_workload(
                self.output_dir,
                dataset001_dir=self.dataset002_dir,  # deliberately swapped
                dataset002_dir=self.dataset002_dir,
            )


if __name__ == "__main__":
    unittest.main()
