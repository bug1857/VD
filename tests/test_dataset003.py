"""TDD coverage for the immutable, cross-dataset-disjoint DATASET-003 workload."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np

from vdbench.artifacts import write_dataset_artifacts
from vdbench.config import EXP001_DATASET_SPEC, ContractViolation
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import (
    DATASET002_QUERY_IDENTITY_SCOPE,
    DATASET002_SPEC,
    Dataset002Spec,
    generate_dataset002,
    write_dataset002_artifacts,
)
from vdbench.dataset003 import (
    DATASET003_ARTIFACTS,
    DATASET003_SCHEMA_VERSION,
    DATASET003_SPEC,
    LKG_QUALIFICATION_ID_OFFSET,
    LKG_QUALIFICATION_ROLE,
    Dataset003Bundle,
    Dataset003Spec,
    generate_dataset003,
    verify_dataset003_artifacts,
    write_dataset003_artifacts,
)


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


def _small_dataset002(path: Path, *, dataset001_dir: Path) -> Dataset002Spec:
    spec = replace(
        DATASET002_SPEC,
        version="dataset002-fixture-v1",
        dimensions=4,
        seed=20260809,
        routing_query_count=6,
        recall_audit_query_count=12,
    )
    write_dataset002_artifacts(
        path,
        generate_dataset002(spec),
        dataset001_dir=dataset001_dir,
    )
    return spec


def _sha256sums_for(output: Path, filenames: tuple[str, ...]) -> str:
    entries = [
        f"{hashlib.sha256((output / filename).read_bytes()).hexdigest()}  {filename}"
        for filename in filenames
    ]
    return "\n".join(entries) + "\n"


def _corrupt_one_oracle_score(dataset002_output: Path) -> None:
    """Structurally-valid, hash-consistent, oracle-semantically-wrong DATASET-002.

    Mirrors the real discovered COSINE score-reproduction mismatch: same
    role/metric/threshold/hit-membership/hit-order/full_count/capped, only a
    floating-point score differs. Used to prove DATASET-003 depends only on
    DATASET-002's query identity, never its oracle semantics.
    """

    path = dataset002_output / "oracle_records.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        record = json.loads(line)
        if record["hits"]:
            record["hits"][0]["score"] = record["hits"][0]["score"] + 1e-9
            lines[index] = json.dumps(record, sort_keys=True, separators=(",", ":"))
            break
    else:
        raise AssertionError("no oracle record with a hit was found to corrupt")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    manifest_path = dataset002_output / "dataset002_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    all_filenames = (
        "routing_ids.npy",
        "routing_queries.npy",
        "recall_audit_ids.npy",
        "recall_audit_queries.npy",
        "inherited_dataset001.json",
        "oracle_records.jsonl",
    )
    for filename in all_filenames:
        payload = (dataset002_output / filename).read_bytes()
        manifest["artifacts"][filename] = {
            "file": filename,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    entries = [
        f"{hashlib.sha256((dataset002_output / filename).read_bytes()).hexdigest()}  {filename}"
        for filename in all_filenames
    ] + [f"{hashlib.sha256(manifest_path.read_bytes()).hexdigest()}  dataset002_manifest.json"]
    (dataset002_output / "SHA256SUMS").write_text("\n".join(entries) + "\n", encoding="utf-8")


class Dataset003Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = Dataset003Spec(
            dataset_id="DATASET-003",
            version="dataset003-fixture-v1",
            seed=20260806,
            dimensions=4,
            lkg_qualification_query_count=9,
            dtype="<f4",
            distribution="independent standard normal",
            generator="numpy.random.Generator(numpy.random.PCG64(seed))",
        )

    def test_production_spec_has_exactly_2400_queries_at_the_dataset001_offset(self) -> None:
        self.assertEqual(DATASET003_SPEC.lkg_qualification_query_count, 2_400)
        self.assertEqual(LKG_QUALIFICATION_ID_OFFSET, EXP001_DATASET_SPEC.base_count)
        bundle = generate_dataset003(DATASET003_SPEC)
        self.assertEqual(int(bundle.lkg_qualification_ids[0]), LKG_QUALIFICATION_ID_OFFSET)
        self.assertEqual(
            int(bundle.lkg_qualification_ids[-1]),
            LKG_QUALIFICATION_ID_OFFSET + 2_400 - 1,
        )

    def test_generator_is_deterministic_and_little_endian(self) -> None:
        first = generate_dataset003(self.spec)
        second = generate_dataset003(self.spec)

        np.testing.assert_array_equal(first.lkg_qualification_ids, second.lkg_qualification_ids)
        np.testing.assert_array_equal(
            first.lkg_qualification_queries, second.lkg_qualification_queries
        )
        self.assertEqual(first.lkg_qualification_queries.dtype.str, "<f4")
        self.assertEqual(first.lkg_qualification_queries.shape, (9, 4))

    def test_artifacts_round_trip_and_bind_query_role(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            dataset002 = root / "dataset002"
            output = root / "dataset003"
            _small_dataset001(dataset001)
            _small_dataset002(dataset002, dataset001_dir=dataset001)

            manifest = write_dataset003_artifacts(
                output,
                generate_dataset003(self.spec),
                dataset001_dir=dataset001,
                dataset002_dir=dataset002,
            )
            verified = verify_dataset003_artifacts(
                output, dataset001_dir=dataset001, dataset002_dir=dataset002
            )

            self.assertEqual(verified, manifest)
            self.assertEqual(manifest["schema_version"], DATASET003_SCHEMA_VERSION)
            self.assertEqual(manifest["query_role"], LKG_QUALIFICATION_ROLE)
            self.assertEqual(set(manifest["artifacts"]), set(DATASET003_ARTIFACTS))

    def test_writer_and_verifier_leave_dataset002_byte_for_byte_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            dataset002 = root / "dataset002"
            output = root / "dataset003"
            _small_dataset001(dataset001)
            _small_dataset002(dataset002, dataset001_dir=dataset001)

            before = {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(dataset002.iterdir())
            }

            write_dataset003_artifacts(
                output,
                generate_dataset003(self.spec),
                dataset001_dir=dataset001,
                dataset002_dir=dataset002,
            )
            verify_dataset003_artifacts(
                output, dataset001_dir=dataset001, dataset002_dir=dataset002
            )

            after = {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(dataset002.iterdir())
            }
            self.assertEqual(before, after)

    def test_writer_refuses_ids_colliding_with_dataset001_base_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            dataset002 = root / "dataset002"
            output = root / "dataset003"
            _small_dataset001(dataset001)
            _small_dataset002(dataset002, dataset001_dir=dataset001)

            bundle = generate_dataset003(self.spec)
            colliding = Dataset003Bundle(
                lkg_qualification_ids=np.array(
                    [0, *bundle.lkg_qualification_ids[1:]], dtype=np.int64
                ),
                lkg_qualification_queries=bundle.lkg_qualification_queries,
                spec=bundle.spec,
            )
            with self.assertRaisesRegex(ContractViolation, "disagrees with the deterministic generator"):
                write_dataset003_artifacts(
                    output,
                    colliding,
                    dataset001_dir=dataset001,
                    dataset002_dir=dataset002,
                )

    def test_verifier_fails_closed_on_hidden_overlap_with_dataset002_recall_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            dataset002 = root / "dataset002"
            output = root / "dataset003"
            _small_dataset001(dataset001)
            _small_dataset002(dataset002, dataset001_dir=dataset001)
            write_dataset003_artifacts(
                output,
                generate_dataset003(self.spec),
                dataset001_dir=dataset001,
                dataset002_dir=dataset002,
            )

            recall_audit_ids = np.load(dataset002 / "recall_audit_ids.npy", allow_pickle=False)
            ids_path = output / "lkg_qualification_ids.npy"
            ids = np.load(ids_path, allow_pickle=False)
            ids[0] = recall_audit_ids[0]
            with ids_path.open("wb") as handle:
                np.save(handle, ids, allow_pickle=False)

            manifest_path = output / "dataset003_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            payload = ids_path.read_bytes()
            manifest["artifacts"][ids_path.name] = {
                "file": ids_path.name,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
            manifest_path.write_text(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            sums_path = output / "SHA256SUMS"
            sums_path.write_text(
                _sha256sums_for(output, (*DATASET003_ARTIFACTS, "dataset003_manifest.json")),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ContractViolation, "disagree with the deterministic generator"):
                verify_dataset003_artifacts(
                    output, dataset001_dir=dataset001, dataset002_dir=dataset002
                )

    def test_writer_refuses_when_dataset002_directory_is_tampered(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            dataset002 = root / "dataset002"
            output = root / "dataset003"
            _small_dataset001(dataset001)
            _small_dataset002(dataset002, dataset001_dir=dataset001)

            with (dataset002 / "routing_queries.npy").open("ab") as handle:
                handle.write(b"tamper")

            with self.assertRaises(ContractViolation):
                write_dataset003_artifacts(
                    output,
                    generate_dataset003(self.spec),
                    dataset001_dir=dataset001,
                    dataset002_dir=dataset002,
                )

    def test_writer_succeeds_against_oracle_corrupted_but_structurally_valid_dataset002(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            dataset002 = root / "dataset002"
            output = root / "dataset003"
            _small_dataset001(dataset001)
            _small_dataset002(dataset002, dataset001_dir=dataset001)
            _corrupt_one_oracle_score(dataset002)

            # This is the whole point of the narrow-verifier dependency: a
            # DATASET-002 whose oracle_records.jsonl disagrees with a fresh
            # recomputation (exactly the real discovered COSINE issue) must
            # not block DATASET-003, because DATASET-003 never depends on
            # DATASET-002's oracle semantics.
            manifest = write_dataset003_artifacts(
                output,
                generate_dataset003(self.spec),
                dataset001_dir=dataset001,
                dataset002_dir=dataset002,
            )
            verified = verify_dataset003_artifacts(
                output, dataset001_dir=dataset001, dataset002_dir=dataset002
            )
            self.assertEqual(verified, manifest)

    def test_manifest_binds_the_narrow_verification_scope_and_is_tamper_detected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            dataset002 = root / "dataset002"
            output = root / "dataset003"
            _small_dataset001(dataset001)
            _small_dataset002(dataset002, dataset001_dir=dataset001)

            manifest = write_dataset003_artifacts(
                output,
                generate_dataset003(self.spec),
                dataset001_dir=dataset001,
                dataset002_dir=dataset002,
            )
            inherited = manifest["inherited_dataset002"]
            self.assertEqual(inherited["verification_scope"], DATASET002_QUERY_IDENTITY_SCOPE)
            self.assertEqual(inherited["dataset_id"], "DATASET-002")
            self.assertIn("manifest_sha256", inherited)
            self.assertIn("routing_ids_sha256", inherited)
            self.assertIn("recall_audit_ids_sha256", inherited)
            self.assertIn("inherited_dataset001", manifest)

            # A caller cannot substitute a differently-scoped or hand-built
            # parent result: tampering the recorded scope tag alone (leaving
            # everything else, including hashes, self-consistent) must still
            # be caught, because the verifier re-derives this field itself
            # from a real verify_dataset002_query_identity() call rather than
            # trusting whatever is stored on disk.
            manifest_path = output / "dataset003_manifest.json"
            tampered = json.loads(manifest_path.read_text(encoding="utf-8"))
            tampered["inherited_dataset002"]["verification_scope"] = "FULL_ORACLE_VERIFIED"
            manifest_path.write_text(
                json.dumps(tampered, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
            )
            sums_path = output / "SHA256SUMS"
            sums_path.write_text(
                _sha256sums_for(output, (*DATASET003_ARTIFACTS, "dataset003_manifest.json")),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ContractViolation, "inherited DATASET-002 identity mismatch"):
                verify_dataset003_artifacts(
                    output, dataset001_dir=dataset001, dataset002_dir=dataset002
                )

    def test_writer_refuses_an_existing_output_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset001 = root / "dataset001"
            dataset002 = root / "dataset002"
            output = root / "dataset003"
            _small_dataset001(dataset001)
            _small_dataset002(dataset002, dataset001_dir=dataset001)
            write_dataset003_artifacts(
                output,
                generate_dataset003(self.spec),
                dataset001_dir=dataset001,
                dataset002_dir=dataset002,
            )

            with self.assertRaises(ContractViolation):
                write_dataset003_artifacts(
                    output,
                    generate_dataset003(self.spec),
                    dataset001_dir=dataset001,
                    dataset002_dir=dataset002,
                )


if __name__ == "__main__":
    unittest.main()
