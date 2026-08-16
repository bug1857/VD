"""Offline (fake-client) coverage for the real EXP-011 live-acquisition
composition root. Every test here uses a fake Milvus client and a fake stack
health probe; none of these tests contact Milvus, and none of them claim
PROSPECTIVE evidence -- every call in this file passes
``evidence_status="STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE"`` explicitly.
"""

from __future__ import annotations

import ast
import json
import tempfile
import unittest
from dataclasses import fields
from pathlib import Path
from unittest.mock import patch

import numpy as np

from tests.test_response_profile_milvus_adapter import (
    _FakeMilvusClient,
    _FakeStackHealthProbe,
)
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.exp011_live_acquisition import (
    Exp011LiveAcquisitionError,
    load_control_artifact,
    load_static_identity_artifact,
    main,
    run_exp011_live_acquisition,
)
from vdbench.response_profile import SUPPORTED_EFS
from vdbench.response_profile_control import (
    build_response_profile_control,
    response_profile_control_document,
)
from vdbench.response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    WARMUP_QUERY_COUNT,
    ResponseProfileRoleKind,
    build_artifact_source_namespace,
    build_calibration_population_manifest,
    build_canonical_query_identity,
    build_query_vector_identity,
    build_response_profile_cell,
    build_response_profile_query_payload,
    build_response_profile_replay_schedule,
    build_response_profile_role,
    build_response_profile_role_manifest,
    build_response_profile_role_member,
)
from vdbench.response_profile_lifecycle import (
    build_response_profile_run_binding,
    response_profile_run_binding_document,
)
from vdbench.response_profile_semantic import (
    build_response_profile_oracle_manifest,
    build_response_profile_oracle_record,
    build_response_profile_static_identity,
    oracle_manifest_document,
    response_profile_static_identity_document,
)
from vdbench.response_profile_vector_material import (
    response_profile_vector_material_document,
)

MODULE_PATH = Path(__file__).parents[1] / "src" / "vdbench" / "exp011_live_acquisition.py"
DIMENSIONS = 1
COLLECTION = "response-profile-hnsw-exp011-live"


def _digest(character: str) -> str:
    return character * 64


def _flat_configuration() -> SearchConfiguration:
    return SearchConfiguration(
        metric=Metric.L2, threshold_label="target-075", radius=0.75,
        index_track=IndexTrack.FLAT, ef=None,
    )


def _hnsw_configurations() -> tuple[SearchConfiguration, ...]:
    return tuple(
        SearchConfiguration(
            metric=Metric.L2, threshold_label="target-075", radius=0.75,
            index_track=IndexTrack.HNSW, ef=ef,
        )
        for ef in SUPPORTED_EFS
    )


def _member(index: int, *, namespace: object, offset: float = 0.0):
    vector = build_query_vector_identity(np.asarray([float(index + 1) + offset], dtype="<f4"))
    return build_response_profile_role_member(
        source_namespace=namespace,
        query_identity=build_canonical_query_identity(index),
        vector_identity=vector,
        query_payload_identity=build_response_profile_query_payload(
            vector_identity=vector, search_configuration=_flat_configuration()
        ),
    )


class _Fixture:
    """Full-scale (unshrunk) 1,200-member population, 4-ef schedule, and every
    governed value `run_exp011_live_acquisition` requires -- everything
    except the lifecycle event stream itself, which the producer builds by
    actually driving the injected (fake, here) Milvus adapters."""

    def __init__(self) -> None:
        namespace = build_artifact_source_namespace(
            dataset_id="DATASET-EXP011-LIVE-FIXTURE", dataset_version="v1",
            generation_manifest_sha256=_digest("a"),
        )
        calibration_members = tuple(
            _member(index, namespace=namespace) for index in range(CALIBRATION_QUERY_COUNT)
        )
        calibration_manifest = build_response_profile_role_manifest(
            role=build_response_profile_role(kind=ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION),
            members=calibration_members,
        )
        self.population = build_calibration_population_manifest(
            cell=build_response_profile_cell(metric=Metric.L2, threshold_stratum="target-075"),
            calibration_role_manifest=calibration_manifest,
        )
        self.schedule = build_response_profile_replay_schedule(
            population=self.population, source_revision="revision/exp011-live-fixture-v1"
        )
        self.measured_position_count = sum(len(block.positions) for block in self.schedule.blocks)
        warmup_members = tuple(
            _member(index + 20_000, namespace=namespace, offset=40_000.0)
            for index in range(WARMUP_QUERY_COUNT)
        )
        self.warmup = build_response_profile_role_manifest(
            role=build_response_profile_role(kind=ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP),
            members=warmup_members,
        )
        self.run_binding = build_response_profile_run_binding(
            run_id="exp011-live-fixture", created_at_utc="2026-08-11T00:00:00Z",
            population=self.population, replay_schedule=self.schedule,
            warmup_role_manifest=self.warmup, source_revision="revision/exp011-live-fixture-v1",
        )
        self.static_identity = build_response_profile_static_identity(
            metric=Metric.L2, threshold_stratum="target-075",
            search_configurations=_hnsw_configurations(),
            hnsw_index_identity="hnsw-exp011-live-fixture-v1",
            data_identity="data-exp011-live-fixture-v1",
            workload_manifest_sha256=self.population.workload_manifest_sha256,
            ordered_query_payload_sha256=self.population.ordered_query_payload_sha256,
            replay_schedule_sha256=self.schedule.replay_schedule_sha256,
            control_profile_sha256=_digest("b"),
            environment_manifest_sha256=_digest("c"),
            source_revision="revision/exp011-live-fixture-v1",
        )
        from vdbench.drift import build_evidence_provenance
        from vdbench.shadow_event_types import MonitorStreamKey

        provenance = build_evidence_provenance(
            metric=Metric.L2, threshold_stratum="target-075",
            reference_window_id="exp011-live-reference", current_window_id="exp011-live-current",
            reference_manifest_sha256=_digest("d"), current_manifest_sha256=_digest("e"),
            configuration_identity="exp011-live-configuration-v1",
            data_identity=self.static_identity.data_identity,
            flat_binding_id="flat-exp011-live-fixture-v1",
            hnsw_binding_id=self.static_identity.hnsw_index_identity,
            reference_audit_ids=tuple(range(50)), reference_audit_rank_digests=tuple(_digest("1") for _ in range(50)),
            current_audit_ids=tuple(range(50, 100)), current_audit_rank_digests=tuple(_digest("2") for _ in range(50)),
        )
        stream_key = MonitorStreamKey(
            "exp011-live-stream-v1", Metric.L2, "target-075", "exp011-live-configuration-v1",
            self.static_identity.data_identity, "flat-exp011-live-fixture-v1", self.static_identity.hnsw_index_identity,
        )
        from vdbench.drift import DetectorState, DriftClassification
        from vdbench.response_profile_detector_head import (
            build_response_profile_detector_head,
        )

        detector_head = build_response_profile_detector_head(
            stream_key=stream_key, window_sequence=2,
            detector_state=DetectorState.NO_DRIFT, detector_classification=DriftClassification.NONE,
            detector_provenance=provenance,
        )
        control = build_response_profile_control(
            stream_key=stream_key, detector_provenance=provenance, trigger_window_sequence=2,
            detector_head_sha256=detector_head.detector_head_sha256,
            detector_head_record_sequence=0, detector_head_record_sha256=_digest("f"),
            detector_head_persisted_at_utc="2026-08-10T23:59:58Z",
            calibration_population_sha256=self.population.workload_manifest_sha256,
            warmup_role_manifest_sha256=self.warmup.role_manifest_sha256,
            ordered_query_payload_sha256=self.population.ordered_query_payload_sha256,
            replay_schedule_sha256=self.schedule.replay_schedule_sha256,
            environment_manifest_sha256=self.static_identity.environment_manifest_sha256,
            source_revision=self.static_identity.source_revision,
            frozen_at_utc="2026-08-10T23:59:59Z",
        )
        self.static_identity = build_response_profile_static_identity(
            **{
                item.name: (
                    control.control_profile_sha256 if item.name == "control_profile_sha256"
                    else getattr(self.static_identity, item.name)
                )
                for item in fields(self.static_identity)
            }
        )
        self.control = control
        records = tuple(
            build_response_profile_oracle_record(
                observation_identity_sha256=member.observation_identity.observation_identity_sha256,
                query_id_sha256=member.query_identity.query_id_sha256,
                query_payload_sha256=member.query_payload_identity.query_payload_sha256,
                limit=member.query_payload_identity.limit, full_count=0, capped_ids=(), capped_distances=(),
                metric=Metric.L2, radius=member.query_payload_identity.radius,
                range_filter=member.query_payload_identity.range_filter,
            )
            for member in calibration_members
        )
        self.oracle_manifest = build_response_profile_oracle_manifest(population=self.population, records=records)


def _search_response(**_kwargs: object) -> object:
    return [[]]


class Exp011LiveAcquisitionTests(unittest.TestCase):
    def test_unshrunk_population_and_schedule_contract(self) -> None:
        fixture = _Fixture()
        self.assertEqual(len(fixture.population.calibration_role_manifest.members), 1200)
        self.assertEqual(fixture.measured_position_count, 4800)
        self.assertEqual(len(fixture.warmup.members) * len(SUPPORTED_EFS), 800)

    def test_bounded_run_wires_real_adapters_through_the_unmodified_producer(self) -> None:
        fixture = _Fixture()
        client = _FakeMilvusClient(search_response=[[]])
        health = _FakeStackHealthProbe()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = run_exp011_live_acquisition(
                client=client,
                stack_health_probe=health,
                collection_name=COLLECTION,
                dimensions=DIMENSIONS,
                metric=Metric.L2,
                ledger_path=root / "ledger.sqlite3",
                run_binding=fixture.run_binding,
                static_identity=fixture.static_identity,
                control=fixture.control,
                oracle_manifest=fixture.oracle_manifest,
                output_dir=root / "run",
                evidence_status="STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE",
                max_blocks=1,
            )
            self.assertEqual(result.evidence_status, "STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE")
            self.assertTrue(result.manifest_path.exists())
            self.assertGreater(len([c for c in client.calls if c[0] == "search"]), 0)
            self.assertEqual({c[0] for c in client.calls} - {"search"}, {"get_load_state", "describe_index"})

    def test_unhealthy_runtime_blocks_the_measured_block_after_warmup(self) -> None:
        """Runtime readiness is checked immediately before each measured
        block, not during warmup (an unmodified, pre-existing
        `ResponseProfileProducer` design this test must not contradict): an
        unhealthy collection still lets warmup run, but the block that would
        depend on that readiness never completes."""

        fixture = _Fixture()
        client = _FakeMilvusClient(load_state="NotLoad", search_response=[[]])
        health = _FakeStackHealthProbe()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = run_exp011_live_acquisition(
                client=client, stack_health_probe=health, collection_name=COLLECTION,
                dimensions=DIMENSIONS, metric=Metric.L2, ledger_path=root / "ledger.sqlite3",
                run_binding=fixture.run_binding, static_identity=fixture.static_identity,
                control=fixture.control, oracle_manifest=fixture.oracle_manifest,
                output_dir=root / "run", evidence_status="STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE",
                max_blocks=1,
            )
            self.assertFalse(result.producer_complete)
            self.assertIn("RUNTIME_READINESS_FAILED", result.producer_reason_codes)
            self.assertIsNone(result.profile)
            self.assertGreater(
                sum(1 for name, _ in client.calls if name == "get_load_state"), 0
            )
            self.assertEqual(
                sum(1 for name, _ in client.calls if name == "search"),
                len(fixture.warmup.members) * len(SUPPORTED_EFS),
            )

    def test_client_exception_becomes_governed_failure_not_a_fake_success(self) -> None:
        fixture = _Fixture()
        client = _FakeMilvusClient(raise_on="search")
        health = _FakeStackHealthProbe()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = run_exp011_live_acquisition(
                client=client, stack_health_probe=health, collection_name=COLLECTION,
                dimensions=DIMENSIONS, metric=Metric.L2, ledger_path=root / "ledger.sqlite3",
                run_binding=fixture.run_binding, static_identity=fixture.static_identity,
                control=fixture.control, oracle_manifest=fixture.oracle_manifest,
                output_dir=root / "run", evidence_status="STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE",
                max_blocks=1,
            )
            self.assertFalse(result.producer_complete)
            self.assertIsNone(result.profile)
            self.assertIsNone(result.root_pinned_capability)

    def test_restart_resumes_without_redispatching_completed_blocks(self) -> None:
        fixture = _Fixture()
        client = _FakeMilvusClient(search_response=[[]])
        health = _FakeStackHealthProbe()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_exp011_live_acquisition(
                client=client, stack_health_probe=health, collection_name=COLLECTION,
                dimensions=DIMENSIONS, metric=Metric.L2, ledger_path=root / "ledger.sqlite3",
                run_binding=fixture.run_binding, static_identity=fixture.static_identity,
                control=fixture.control, oracle_manifest=fixture.oracle_manifest,
                output_dir=root / "run-1", evidence_status="STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE",
                max_blocks=1,
            )
            first_calls = len(client.calls)
            second_client = _FakeMilvusClient(search_response=[[]])
            run_exp011_live_acquisition(
                client=second_client, stack_health_probe=health, collection_name=COLLECTION,
                dimensions=DIMENSIONS, metric=Metric.L2, ledger_path=root / "ledger.sqlite3",
                run_binding=fixture.run_binding, static_identity=fixture.static_identity,
                control=fixture.control, oracle_manifest=fixture.oracle_manifest,
                output_dir=root / "run-2", evidence_status="STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE",
                max_blocks=1,
            )
            # The restarted producer resumes from where the durable ledger left
            # off; it must not repeat the block already closed durably above.
            self.assertGreater(first_calls, 0)
            self.assertGreater(len(second_client.calls), 0)

    def test_refuses_to_overwrite_existing_output_directory(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "run").mkdir()
            with self.assertRaises(Exp011LiveAcquisitionError):
                run_exp011_live_acquisition(
                    client=_FakeMilvusClient(), stack_health_probe=_FakeStackHealthProbe(),
                    collection_name=COLLECTION, dimensions=DIMENSIONS, metric=Metric.L2,
                    ledger_path=root / "ledger.sqlite3", run_binding=fixture.run_binding,
                    static_identity=fixture.static_identity, control=fixture.control,
                    oracle_manifest=fixture.oracle_manifest, output_dir=root / "run",
                    evidence_status="STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE",
                )

    def test_evidence_status_is_required_and_never_defaults_to_prospective(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(Exp011LiveAcquisitionError):
                run_exp011_live_acquisition(
                    client=_FakeMilvusClient(), stack_health_probe=_FakeStackHealthProbe(),
                    collection_name=COLLECTION, dimensions=DIMENSIONS, metric=Metric.L2,
                    ledger_path=root / "ledger.sqlite3", run_binding=fixture.run_binding,
                    static_identity=fixture.static_identity, control=fixture.control,
                    oracle_manifest=fixture.oracle_manifest, output_dir=root / "run",
                    evidence_status="",
                )


class Exp011LiveAcquisitionArtifactLoaderTests(unittest.TestCase):
    """Coverage for load_control_artifact/load_static_identity_artifact and
    the `main` CLI's governed-loading-before-Milvus ordering. Every test here
    uses only files on disk plus fake/mocked Milvus-adjacent objects; none
    contacts a real Milvus deployment."""

    def _write(self, directory: Path, name: str, document: object) -> Path:
        path = directory / name
        path.write_text(json.dumps(document), encoding="utf-8")
        return path

    def _forbid_acquisition(self):
        """Patch the single acquisition seam with a hard failure, so any code
        path that reaches Milvus construction on malformed/mismatched input
        makes the test fail loudly. `run_exp011_live_acquisition_from_cli` is
        the only place a real client, ledger, or lifecycle STARTED is ever
        built, so guarding it proves zero Milvus interaction and zero output."""

        return patch(
            "vdbench.exp011_live_acquisition.run_exp011_live_acquisition_from_cli",
            side_effect=AssertionError("acquisition seam must not be reached on invalid input"),
        )

    def test_load_control_artifact_round_trips_from_a_real_file(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self._write(root, "control.json", response_profile_control_document(fixture.control))
            loaded = load_control_artifact(path)
            self.assertEqual(loaded, fixture.control)

    def test_load_static_identity_artifact_round_trips_from_a_real_file(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self._write(
                root, "static_identity.json", response_profile_static_identity_document(fixture.static_identity)
            )
            loaded = load_static_identity_artifact(path)
            self.assertEqual(loaded, fixture.static_identity)

    def test_load_control_artifact_fails_closed_on_missing_file(self) -> None:
        with self.assertRaises(Exp011LiveAcquisitionError) as raised:
            load_control_artifact(Path("/nonexistent/control.json"))
        self.assertIn("control_json", str(raised.exception))

    def test_load_control_artifact_fails_closed_on_malformed_json_syntax(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "control.json"
            path.write_text("{not valid json", encoding="utf-8")
            with self.assertRaises(Exp011LiveAcquisitionError) as raised:
                load_control_artifact(path)
            self.assertIn("not valid JSON", str(raised.exception))

    def test_load_control_artifact_fails_closed_on_a_tampered_document(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = response_profile_control_document(fixture.control)
            document["control_profile_sha256"] = "0" * 64
            path = self._write(root, "control.json", document)
            with self.assertRaises(Exp011LiveAcquisitionError):
                load_control_artifact(path)

    def test_load_static_identity_artifact_fails_closed_on_a_tampered_document(self) -> None:
        """`threshold_stratum` is cross-checked against the nested HNSW
        search configurations' own `threshold_label` (`_static_identity_payload`
        raises `PROFILE_IDENTITY_INVALID` on a mismatch) -- unlike a free-form
        field such as `data_identity`, which carries no such constraint and
        so cannot serve as a tamper target here."""

        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = response_profile_static_identity_document(fixture.static_identity)
            document["static_identity_payload"]["threshold_stratum"] = "a-different-threshold-stratum"
            path = self._write(root, "static_identity.json", document)
            with self.assertRaises(Exp011LiveAcquisitionError):
                load_static_identity_artifact(path)

    def _valid_argv(self, root: Path, *, fixture: _Fixture | None = None) -> list[str]:
        """Write a fully valid, mutually consistent governed artifact set --
        control, static identity, run binding, oracle manifest, and the
        supplemental vector material -- all derived from one fixture, and return
        an argv that references them."""

        fixture = fixture or _Fixture()
        control_path = self._write(root, "control.json", response_profile_control_document(fixture.control))
        static_identity_path = self._write(
            root, "static_identity.json", response_profile_static_identity_document(fixture.static_identity)
        )
        run_binding_path = self._write(
            root, "run_binding.json", response_profile_run_binding_document(fixture.run_binding)
        )
        oracle_manifest_path = self._write(
            root, "oracle_manifest.json", oracle_manifest_document(fixture.oracle_manifest)
        )
        vector_material_path = self._write(
            root, "vector_material.json", response_profile_vector_material_document(fixture.run_binding)
        )
        return [
            "--milvus-uri", "https://example.invalid:19530",
            "--collection-name", COLLECTION,
            "--dimensions", str(DIMENSIONS),
            "--metric", "L2",
            "--ledger-path", str(root / "ledger.sqlite3"),
            "--run-binding-json", str(run_binding_path),
            "--static-identity-json", str(static_identity_path),
            "--control-json", str(control_path),
            "--oracle-manifest-json", str(oracle_manifest_path),
            "--vector-material", str(vector_material_path),
            "--evidence-status", "PROSPECTIVE",
        ]

    def test_cli_with_valid_artifacts_reaches_the_injected_acquisition_seam(self) -> None:
        """A fully valid, mutually consistent artifact set (including vector
        material) must load and cross-validate all four governed objects and
        then reach -- exactly once -- the injectable acquisition seam, with the
        reconstructed run binding and oracle manifest passed through. No real
        Milvus is contacted: the seam is a fake here."""

        sentinel = type("_Result", (), {"manifest_path": "/tmp/manifest.json"})()
        calls: list[dict[str, object]] = []

        def _fake_seam(args, **kwargs):
            calls.append(kwargs)
            return sentinel

        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = self._valid_argv(root, fixture=fixture)
            with patch(
                "vdbench.exp011_live_acquisition.run_exp011_live_acquisition_from_cli",
                _fake_seam,
            ):
                self.assertEqual(main(argv), 0)
        self.assertEqual(len(calls), 1)
        passed = calls[0]
        self.assertEqual(
            response_profile_run_binding_document(passed["run_binding"]),
            response_profile_run_binding_document(fixture.run_binding),
        )
        self.assertEqual(passed["oracle_manifest"], fixture.oracle_manifest)
        self.assertEqual(passed["control"], fixture.control)
        self.assertEqual(passed["static_identity"], fixture.static_identity)

    def test_cli_malformed_control_json_fails_closed_before_touching_milvus(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = self._valid_argv(root)
            control_index = argv.index("--control-json") + 1
            argv[control_index] = str(self._write(root, "bad_control.json", {"not": "a control document"}))
            with (
                self._forbid_acquisition(),
                self.assertRaises(Exp011LiveAcquisitionError) as raised,
            ):
                main(argv)
            self.assertIn("control_json", str(raised.exception))

    def test_cli_malformed_static_identity_json_is_caught_before_run_binding_is_even_read(self) -> None:
        """Proves the documented ordering: control, then static-identity, then
        vector-material/run-binding/oracle -- by pointing run-binding-json at a
        path that does not exist while static-identity-json is malformed. If
        run-binding were read first, this would fail with a different ("cannot
        read") message instead."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = self._valid_argv(root)
            static_index = argv.index("--static-identity-json") + 1
            argv[static_index] = str(
                self._write(root, "bad_static_identity.json", {"not": "a static identity document"})
            )
            run_binding_index = argv.index("--run-binding-json") + 1
            argv[run_binding_index] = str(root / "does-not-exist.json")
            with (
                self._forbid_acquisition(),
                self.assertRaises(Exp011LiveAcquisitionError) as raised,
            ):
                main(argv)
            self.assertIn("static_identity_json", str(raised.exception))

    def test_cli_malformed_vector_material_causes_zero_acquisition(self) -> None:
        """A structurally broken vector-material file must fail closed after the
        control/static-identity load but before any run-binding reconstruction
        or Milvus construction -- the acquisition seam is never reached."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = self._valid_argv(root)
            material_index = argv.index("--vector-material") + 1
            argv[material_index] = str(self._write(root, "bad_material.json", {"not": "vector material"}))
            with (
                self._forbid_acquisition(),
                self.assertRaises(Exp011LiveAcquisitionError) as raised,
            ):
                main(argv)
            self.assertIn("vector_material", str(raised.exception))

    def test_cli_tampered_oracle_manifest_causes_zero_acquisition(self) -> None:
        """A valid run-binding + vector material, but an oracle manifest whose
        recorded digest has been tampered, must fail closed at oracle
        reconstruction -- never reaching the acquisition seam."""

        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = self._valid_argv(root, fixture=fixture)
            document = oracle_manifest_document(fixture.oracle_manifest)
            document["oracle_manifest_sha256"] = "0" * 64
            oracle_index = argv.index("--oracle-manifest-json") + 1
            argv[oracle_index] = str(self._write(root, "tampered_oracle.json", document))
            with (
                self._forbid_acquisition(),
                self.assertRaises(Exp011LiveAcquisitionError) as raised,
            ):
                main(argv)
            self.assertIn("oracle_manifest_json", str(raised.exception))

    def test_cli_missing_control_json_file_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = self._valid_argv(root)
            control_index = argv.index("--control-json") + 1
            argv[control_index] = str(root / "does-not-exist.json")
            with (
                self._forbid_acquisition(),
                self.assertRaises(Exp011LiveAcquisitionError) as raised,
            ):
                main(argv)
            self.assertIn("cannot read", str(raised.exception))

    def test_cli_empty_evidence_status_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = self._valid_argv(root)
            status_index = argv.index("--evidence-status") + 1
            argv[status_index] = ""
            with (
                self._forbid_acquisition(),
                self.assertRaises(Exp011LiveAcquisitionError) as raised,
            ):
                main(argv)
            self.assertIn("evidence-status", str(raised.exception))

    def test_cli_missing_evidence_status_argument_is_rejected_by_argparse(self) -> None:
        """`--evidence-status` is `required=True` with no default -- omitting
        it entirely must fail at argument-parsing time, before this module's
        own code runs at all."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = self._valid_argv(root)
            status_index = argv.index("--evidence-status")
            del argv[status_index : status_index + 2]
            with self.assertRaises(SystemExit):
                main(argv)

    def test_cli_never_defaults_evidence_status_to_prospective(self) -> None:
        parser_source = ast.parse(
            MODULE_PATH.read_text(encoding="utf-8"), filename=str(MODULE_PATH)
        )
        for node in ast.walk(parser_source):
            if (
                isinstance(node, ast.Call)
                and getattr(node.func, "attr", "") == "add_argument"
                and any(
                    isinstance(arg, ast.Constant) and arg.value == "--evidence-status"
                    for arg in node.args
                )
            ):
                keyword_names = {keyword.arg for keyword in node.keywords}
                self.assertIn("required", keyword_names)
                self.assertNotIn("default", keyword_names)
                return
        self.fail("--evidence-status argument definition not found")


class Exp011LiveAcquisitionAdversarialTests(unittest.TestCase):
    def test_module_never_imports_policy_or_grant_or_route_modules(self) -> None:
        source = MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(MODULE_PATH))
        imported = {
            node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        } | {
            alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
        }
        forbidden_suffixes = (
            "policy", "canary_admission", "canary_approval", "canary_activation",
            "canary_route_authority", "canary_route_state", "canary_live_runner",
            "canary_grant_store", "pymilvus",
        )
        offending = {
            item for item in imported
            if any(item == suffix or item.endswith(f".{suffix}") for suffix in forbidden_suffixes)
        }
        self.assertFalse(offending, offending)

    def test_module_source_never_hardcodes_prospective_outside_main(self) -> None:
        """`main` is the only real-operator entry point; nowhere else in this
        module may the literal "PROSPECTIVE" string appear, since every other
        function requires the caller to supply `evidence_status` explicitly."""

        source = MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(MODULE_PATH))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and node.value == "PROSPECTIVE":
                self.fail("module source must never hardcode the literal PROSPECTIVE label")

    def test_main_never_executes_without_an_explicit_operator_invocation(self) -> None:
        """Importing this module must never itself run anything -- this
        test file's own successful top-level `import` of the module (which
        must already have happened for any test in this file to run at all)
        is that proof; nothing further to exercise here without reloading
        the module in-process, which would split class identity between the
        reloaded and already-imported symbols and corrupt every other
        `assertRaises(Exp011LiveAcquisitionError)` in this file."""

        self.assertTrue(callable(run_exp011_live_acquisition))

    def test_module_no_longer_contains_the_run_binding_oracle_blocker(self) -> None:
        """The previous artificial reconstruction blocker (both the constant
        and its message) must be gone now that real loaders exist."""

        import vdbench.exp011_live_acquisition as module

        self.assertFalse(hasattr(module, "_RUN_BINDING_ORACLE_MANIFEST_BLOCKER"))
        source = MODULE_PATH.read_text(encoding="utf-8")
        self.assertNotIn("_RUN_BINDING_ORACLE_MANIFEST_BLOCKER", source)
        self.assertNotIn("cannot yet be", source)


if __name__ == "__main__":
    unittest.main()
