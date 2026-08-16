from __future__ import annotations

import ast
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.test_exp011_live_acquisition import (
    COLLECTION,
    DIMENSIONS,
    _FakeMilvusClient,
    _FakeStackHealthProbe,
    _Fixture,
)
from vdbench.config import Metric
from vdbench.drift import DetectorState, DriftClassification
from vdbench.exp011_live_acquisition import (
    load_control_artifact,
    load_oracle_manifest_artifact,
    load_run_binding_artifact,
    load_static_identity_artifact,
    load_vector_material_artifact,
    run_exp011_live_acquisition,
)
from vdbench.exp011_preparation import (
    Exp011PreparationError,
    prepare_exp011_acquisition_inputs,
)
from vdbench.response_profile_detector_head import build_response_profile_detector_head
from vdbench.response_profile_monitor_store import ResponseProfileMonitorStateStore
from vdbench.workload_monitor import MonitorStreamState

MODULE_PATH = Path(__file__).parents[1] / "src" / "vdbench" / "exp011_preparation.py"


class Exp011PreparationTests(unittest.TestCase):
    def _store(self, path: Path, fixture: _Fixture) -> ResponseProfileMonitorStateStore:
        times = iter(("2026-08-10T23:59:57Z", "2026-08-10T23:59:58Z"))
        with patch(
            "vdbench.response_profile_monitor_store.secrets.token_hex",
            return_value="6" * 64,
        ):
            store = ResponseProfileMonitorStateStore(
                path,
                expected_stream_key=fixture.control.stream_key,
                utc_now=lambda: next(times),
            )
        head = build_response_profile_detector_head(
            stream_key=fixture.control.stream_key,
            window_sequence=fixture.control.trigger_window_sequence,
            detector_state=DetectorState.NO_DRIFT,
            detector_classification=DriftClassification.NONE,
            detector_provenance=fixture.control.detector_provenance,
        )
        store.save(
            MonitorStreamState(
                stream_key=fixture.control.stream_key,
                next_window_sequence=fixture.control.trigger_window_sequence + 1,
                latest_detector_head=head,
            )
        )
        return store

    def _prepare(self, root: Path, fixture: _Fixture, store: ResponseProfileMonitorStateStore):
        return prepare_exp011_acquisition_inputs(
            output_dir=root / "prepared",
            monitor_store=store,
            stream_key=fixture.control.stream_key,
            population=fixture.run_binding.population,
            warmup_role_manifest=fixture.run_binding.warmup_role_manifest,
            oracle_records=fixture.oracle_manifest.records,
            run_id="exp011-prepared-structural-fixture",
            created_at_utc="2026-08-10T23:59:59Z",
            source_revision=fixture.static_identity.source_revision,
            search_configurations=fixture.static_identity.search_configurations,
            hnsw_index_identity=fixture.static_identity.hnsw_index_identity,
            data_identity=fixture.static_identity.data_identity,
            environment_manifest_sha256=fixture.static_identity.environment_manifest_sha256,
            frozen_at_utc="2026-08-10T23:59:59.500000Z",
        )

    def test_prepares_exactly_five_pre_run_inputs_and_round_trips_loaders(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self._store(root / "monitor.sqlite3", fixture) as store:
                prepared = self._prepare(root, fixture, store)
            self.assertEqual(
                {path.name for path in prepared.output_dir.iterdir()},
                {
                    "run_binding.json",
                    "static_identity.json",
                    "control.json",
                    "oracle_manifest.json",
                    "vector_material.json",
                },
            )
            material = load_vector_material_artifact(prepared.vector_material_path)
            binding = load_run_binding_artifact(
                prepared.run_binding_path, vector_material=material
            )
            oracle = load_oracle_manifest_artifact(
                prepared.oracle_manifest_path, vector_material=material
            )
            self.assertEqual(binding, prepared.run_binding)
            self.assertEqual(oracle, prepared.oracle_manifest)
            self.assertEqual(load_control_artifact(prepared.control_path), prepared.control)
            self.assertEqual(
                load_static_identity_artifact(prepared.static_identity_path),
                prepared.static_identity,
            )

    def test_acquisition_consumes_prepared_structural_inputs_offline(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self._store(root / "monitor.sqlite3", fixture) as store:
                prepared = self._prepare(root, fixture, store)
            material = load_vector_material_artifact(prepared.vector_material_path)
            binding = load_run_binding_artifact(
                prepared.run_binding_path, vector_material=material
            )
            oracle = load_oracle_manifest_artifact(
                prepared.oracle_manifest_path, vector_material=material
            )
            result = run_exp011_live_acquisition(
                client=_FakeMilvusClient(search_response=[[]]),
                stack_health_probe=_FakeStackHealthProbe(),
                collection_name=COLLECTION,
                dimensions=DIMENSIONS,
                metric=Metric.L2,
                ledger_path=root / "lifecycle.sqlite3",
                run_binding=binding,
                static_identity=load_static_identity_artifact(
                    prepared.static_identity_path
                ),
                control=load_control_artifact(prepared.control_path),
                oracle_manifest=oracle,
                output_dir=root / "structural-run",
                evidence_status="STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE",
                max_blocks=1,
            )
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["closed_block_count"], 1)
            self.assertFalse(result.producer_complete)
            self.assertIsNone(result.profile)
            self.assertIsNone(result.root_pinned_capability)

    def test_missing_verified_detector_head_refuses_without_output(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch(
                "vdbench.response_profile_monitor_store.secrets.token_hex",
                return_value="6" * 64,
            ):
                store = ResponseProfileMonitorStateStore(
                    root / "empty.sqlite3",
                    expected_stream_key=fixture.control.stream_key,
                )
            with store, self.assertRaises(Exp011PreparationError) as raised:
                self._prepare(root, fixture, store)
            self.assertEqual(raised.exception.code, "PREPARATION_DETECTOR_HEAD_REQUIRED")
            self.assertFalse((root / "prepared").exists())

    def test_independent_oracle_records_are_required_not_derived(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                self._store(root / "monitor.sqlite3", fixture) as store,
                self.assertRaises(Exp011PreparationError),
            ):
                prepare_exp011_acquisition_inputs(
                    output_dir=root / "prepared",
                    monitor_store=store,
                    stream_key=fixture.control.stream_key,
                    population=fixture.run_binding.population,
                    warmup_role_manifest=fixture.run_binding.warmup_role_manifest,
                    oracle_records=(),
                    run_id="exp011-missing-oracle",
                    created_at_utc="2026-08-10T23:59:59Z",
                    source_revision=fixture.static_identity.source_revision,
                    search_configurations=fixture.static_identity.search_configurations,
                    hnsw_index_identity=fixture.static_identity.hnsw_index_identity,
                    data_identity=fixture.static_identity.data_identity,
                    environment_manifest_sha256=fixture.static_identity.environment_manifest_sha256,
                    frozen_at_utc="2026-08-10T23:59:59.500000Z",
                )
            self.assertFalse((root / "prepared").exists())

    def test_module_has_no_milvus_policy_grant_route_or_actuation_dependency(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
        forbidden = {
            "pymilvus",
            "milvus",
            "policy",
            "canary_approval",
            "canary_activation",
            "canary_route_state",
            "actuation",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[-1] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[-1])
        self.assertTrue(imported.isdisjoint(forbidden), imported & forbidden)


if __name__ == "__main__":
    unittest.main()
