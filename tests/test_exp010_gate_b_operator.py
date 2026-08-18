"""Coverage for the committed canonical Gate-B operator entrypoint.

The entrypoint's whole value is that it is auditable, so most of these tests
are about what it must *never* do: generate a query, advance a Gate-C window,
capture a shadow, create a store during preflight, accept a request during
preflight, or host a campaign under authority no Gate A attested.

Every executor and client here is a fake. Nothing in this file contacts Milvus,
Docker, or a network peer beyond binding an ephemeral loopback port.
"""

from __future__ import annotations

import ast
import contextlib
import inspect
import io
import json
import socket
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import vdbench.exp010_gate_b_operator as gate_b_operator
from tests.test_exp010_live_runner import DATASET001
from vdbench.config import Metric
from vdbench.exp010_gate_a_operator import (
    GATE_A_EVIDENCE_FILENAME,
    GATE_A_EVIDENCE_SUBDIRECTORY,
    Exp010GateAObservation,
    Exp010GateAOperands,
    build_gate_a_evidence,
)
from vdbench.exp010_gate_b_operator import (
    GATE_B_PLAN_SCHEMA_VERSION,
    GATE_B_TARGET_SOURCE_RECORDS,
    OPERAND_FIELDS,
    STORE_SUBDIRECTORY,
    Exp010GateBOperatorError,
    build_gate_b_plan,
    load_operands,
    main,
)
from vdbench.exp010_live_runner import build_environment_manifest_sha256
from vdbench.exp010_serving_configuration import (
    Exp010ServingConfiguration,
    derive_serving_configuration_identity,
)
from vdbench.shadow_window import WINDOW_QUERY_COUNT

_REVISION = "0123456789abcdef0123456789abcdef01234567"

_GATE_A_OBSERVATION: dict[str, object] = {
    "milvus_uri": "http://milvus.invalid:19530",
    "deployment_identity": "ENV-TEST-exp010-l2-v1",
    "flat_collection_name": "vd_test_l2_flat",
    "hnsw_collection_name": "vd_test_l2_hnsw",
    "metric": "L2",
    "threshold_stratum": "target-075",
    "dimensions": 128,
    "flat_index_identity": "flat-index-v1",
    "hnsw_index_identity": "hnsw-index-v1",
    "data_identity": "DATASET-001-v1:sha256:" + "a" * 64,
    "source_revision": _REVISION,
    "served_ef": 400,
    "observed_at_utc": "2026-08-17T00:00:00Z",
}

_SERVING = Exp010ServingConfiguration(
    metric=Metric.L2,
    threshold_stratum="target-075",
    threshold_radius=2.0,
    range_filter=0.0,
    limit=100,
    served_ef=400,
    dimensions=128,
    consistency_level="Strong",
)
_CONFIGURATION_IDENTITY = derive_serving_configuration_identity(_SERVING)


def _free_port() -> int:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])
    finally:
        probe.close()


def _gate_a_evidence_document(root: Path, **overrides: object) -> dict[str, object]:
    """Build a genuine, verifiable Gate-A evidence document for this root."""

    observation = dict(_GATE_A_OBSERVATION)
    environment = build_environment_manifest_sha256(observation)
    operands = Exp010GateAOperands(
        deployment_identity=str(observation["deployment_identity"]),
        stream_id="exp010-gate-b-test",
        campaign_root=root,
        milvus_uri=str(observation["milvus_uri"]),
        flat_collection_name="vd_test_l2_flat",
        hnsw_collection_name="vd_test_l2_hnsw",
        metric=Metric.L2,
        threshold_stratum="target-075",
        threshold_radius=2.0,
        range_filter=0.0,
        limit=100,
        served_ef=400,
        dimensions=128,
        consistency_level="Strong",
        configuration_identity=_CONFIGURATION_IDENTITY,
        flat_binding_id="flat-index-v1",
        hnsw_binding_id="hnsw-index-v1",
        source_revision=_REVISION,
        expected_row_count=10000,
        hnsw_m=16,
        hnsw_ef_construction=200,
        dataset001_dir=DATASET001,
        etcd_container="milvus-etcd",
        minio_container="milvus-minio",
        milvus_container="milvus-standalone",
    )
    live_flat = {
        "collection_name": "vd_test_l2_flat",
        "index_name": "vector_index",
        "index_type": "FLAT",
        "metric_type": "L2",
        "row_count": 10000,
        "dimensions": 128,
        "indexed_rows": 10000,
        "pending_index_rows": 0,
        "index_state": "Finished",
        "load_state": "Loaded",
    }
    live_hnsw = dict(live_flat)
    live_hnsw.update(
        {
            "collection_name": "vd_test_l2_hnsw",
            "index_type": "HNSW",
            "M": 16,
            "efConstruction": 200,
        }
    )
    container = {
        "container_name": "x",
        "container_id": "c" * 64,
        "status": "running",
        "health": "healthy",
        "restart_count": 0,
        "oom_killed": False,
        "started_at": "2026-08-17T00:00:00Z",
    }
    observed = Exp010GateAObservation(
        observed_at_utc="2026-08-17T00:00:00Z",
        data_identity=str(observation["data_identity"]),
        generation_manifest_sha256="b" * 64,
        base_vectors_sha256="c" * 64,
        dataset_version="DATASET-001-v1",
        flat=live_flat,
        hnsw=live_hnsw,
        containers={
            "etcd": dict(container, container_name="milvus-etcd"),
            "minio": dict(container, container_name="milvus-minio"),
            "milvus": dict(container, container_name="milvus-standalone"),
        },
        environment_manifest_sha256=environment,
        environment_observation=observation,
    )
    document = build_gate_a_evidence(operands, observed)
    if overrides:
        document.update(overrides)
        body = {k: v for k, v in document.items() if k != "evidence_sha256"}
        document["evidence_sha256"] = gate_b_operator.strict_canonical_digest(
            b"VD::EXP010_GATE_A_EVIDENCE::V1\x00", body
        )
    return document


def _seed_gate_a_evidence(root: Path, **overrides: object) -> dict[str, object]:
    """Persist Gate-A evidence exactly where Gate B will look for it."""

    document = _gate_a_evidence_document(root, **overrides)
    directory = root / GATE_A_EVIDENCE_SUBDIRECTORY
    directory.mkdir(parents=True, exist_ok=True)
    (directory / GATE_A_EVIDENCE_FILENAME).write_bytes(
        gate_b_operator.strict_canonical_json_bytes(document)
    )
    return document


def _operand_document(root: Path, **overrides: object) -> dict[str, object]:
    document: dict[str, object] = {
        "campaign_root": str(root),
        "detector_seed": 20260817,
        "host_address": "127.0.0.1",
        "host_port": _free_port(),
        "target_source_records": GATE_B_TARGET_SOURCE_RECORDS,
        "etcd_container": "milvus-etcd",
        "minio_container": "milvus-minio",
    }
    document.update(overrides)
    return document


def _write_operands(root: Path, path: Path, **overrides: object) -> Path:
    path.write_bytes(
        json.dumps(_operand_document(root, **overrides)).encode("utf-8")
    )
    return path


class GateBAuthorityTests(unittest.TestCase):
    """Authority is inherited from Gate A, never asserted by an operator."""

    def test_gate_a_evidence_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            root.mkdir()
            path = _write_operands(root, Path(raw) / "operands.json")
            with self.assertRaises(Exp010GateBOperatorError) as caught:
                load_operands(path)
            self.assertEqual(
                caught.exception.code, "GATE_B_GATE_A_AUTHORITY_UNVERIFIED"
            )

    def test_malformed_gate_a_evidence_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            directory = root / GATE_A_EVIDENCE_SUBDIRECTORY
            directory.mkdir(parents=True)
            (directory / GATE_A_EVIDENCE_FILENAME).write_bytes(b"{not json")
            path = _write_operands(root, Path(raw) / "operands.json")
            with self.assertRaises(Exp010GateBOperatorError) as caught:
                load_operands(path)
            self.assertEqual(
                caught.exception.code, "GATE_B_GATE_A_AUTHORITY_UNVERIFIED"
            )

    def test_substituted_evidence_digest_is_refused(self) -> None:
        """A tampered body whose digest was not recomputed must fail closed."""

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            document = _gate_a_evidence_document(root)
            document["source_revision"] = "f" * 40  # digest deliberately stale
            directory = root / GATE_A_EVIDENCE_SUBDIRECTORY
            directory.mkdir(parents=True)
            (directory / GATE_A_EVIDENCE_FILENAME).write_bytes(
                gate_b_operator.strict_canonical_json_bytes(document)
            )
            path = _write_operands(root, Path(raw) / "operands.json")
            with self.assertRaises(Exp010GateBOperatorError) as caught:
                load_operands(path)
            self.assertEqual(
                caught.exception.code, "GATE_B_GATE_A_AUTHORITY_UNVERIFIED"
            )

    def test_authority_is_inherited_not_operand_supplied(self) -> None:
        """No governed identity may be restated in the operand file."""

        for governed in (
            "source_revision",
            "configuration_identity",
            "environment_manifest_sha256",
            "flat_binding_id",
            "hnsw_binding_id",
            "data_identity",
            "deployment_identity",
            "stream_id",
            "milvus_uri",
            "store_root",
            "exp010_output_dir",
        ):
            self.assertNotIn(governed, OPERAND_FIELDS)

    def test_inherited_identities_match_gate_a(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            document = _seed_gate_a_evidence(root)
            path = _write_operands(root, Path(raw) / "operands.json")
            operands = load_operands(path)
            self.assertEqual(operands.gate_a_evidence_sha256, document["evidence_sha256"])
            self.assertEqual(
                operands.authority["source_revision"], _REVISION
            )
            self.assertEqual(
                operands.authority["configuration_identity"], _CONFIGURATION_IDENTITY
            )
            self.assertEqual(
                operands.deployment_identity,
                str(_GATE_A_OBSERVATION["deployment_identity"]),
            )
            self.assertEqual(
                operands.data_identity, str(_GATE_A_OBSERVATION["data_identity"])
            )


class GateBOperandTests(unittest.TestCase):
    def test_operand_set_is_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(
                root, Path(raw) / "operands.json", unexpected_key=1
            )
            with self.assertRaises(Exp010GateBOperatorError) as caught:
                load_operands(path)
            self.assertEqual(caught.exception.code, "GATE_B_OPERANDS_UNEXPECTED")

    def test_missing_operand_is_refused_not_defaulted(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            document = _operand_document(root)
            del document["detector_seed"]
            path = Path(raw) / "operands.json"
            path.write_bytes(json.dumps(document).encode("utf-8"))
            with self.assertRaises(Exp010GateBOperatorError) as caught:
                load_operands(path)
            self.assertEqual(caught.exception.code, "GATE_B_OPERANDS_INCOMPLETE")
            self.assertIn("detector_seed", str(caught.exception))

    def test_detector_seed_must_be_an_exact_int(self) -> None:
        for bad in (True, 1.0, "20260817", None):
            with tempfile.TemporaryDirectory() as raw:
                root = Path(raw) / "campaign"
                _seed_gate_a_evidence(root)
                path = _write_operands(
                    root, Path(raw) / "operands.json", detector_seed=bad
                )
                with self.assertRaises(Exp010GateBOperatorError) as caught:
                    load_operands(path)
                self.assertEqual(caught.exception.code, "GATE_B_OPERAND_INVALID")

    def test_non_loopback_address_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(
                root, Path(raw) / "operands.json", host_address="0.0.0.0"
            )
            with self.assertRaises(Exp010GateBOperatorError) as caught:
                load_operands(path)
            self.assertEqual(
                caught.exception.code, "GATE_B_HOST_ADDRESS_NOT_LOOPBACK"
            )

    def test_target_must_be_whole_windows(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(
                root, Path(raw) / "operands.json", target_source_records=599
            )
            with self.assertRaises(Exp010GateBOperatorError) as caught:
                load_operands(path)
            self.assertEqual(
                caught.exception.code, "GATE_B_TARGET_NOT_WHOLE_WINDOWS"
            )

    def test_canonical_path_derivation(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(root, Path(raw) / "operands.json")
            operands = load_operands(path)
            self.assertEqual(operands.store_root, root / "stores")
            self.assertEqual(operands.output_dir, root / "output")
            # Symmetric with Gate C, which derives campaign_root from store_root.
            self.assertEqual(operands.store_root.parent, operands.campaign_root)

    def test_governed_target_is_three_windows(self) -> None:
        self.assertEqual(GATE_B_TARGET_SOURCE_RECORDS, 3 * WINDOW_QUERY_COUNT)
        self.assertEqual(GATE_B_TARGET_SOURCE_RECORDS, 600)


class GateBSeparationTests(unittest.TestCase):
    """Gate separation is structural, and these tests read the AST to prove it."""

    def _module_ast(self) -> ast.Module:
        return ast.parse(inspect.getsource(gate_b_operator))

    def test_never_calls_gate_c_or_gate_e_entrypoints(self) -> None:
        forbidden = {
            "process_ready_windows",
            "trigger_state",
            "capture_exp010_population",
        }
        called: set[str] = set()
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Call):
                function = node.func
                if isinstance(function, ast.Attribute):
                    called.add(function.attr)
                elif isinstance(function, ast.Name):
                    called.add(function.id)
        self.assertEqual(called & forbidden, set())

    def test_never_imports_a_generator_or_sampler(self) -> None:
        """No vector sampler, no random source, no replay, no benchmark."""

        imported: set[str] = set()
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
                imported.update(alias.name for alias in node.names)
        for banned in ("random", "numpy", "np", "secrets"):
            self.assertNotIn(banned, imported)

    def test_module_constructs_no_query_vector(self) -> None:
        """`query_vector` is never assigned or built inside the operator."""

        source = inspect.getsource(gate_b_operator)
        # It may be *named* in prose, but never assigned as a Python target.
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        self.assertNotEqual(target.id, "query_vector")

    def test_shadow_capture_executor_refuses(self) -> None:
        executor = gate_b_operator._RefusingShadowCaptureExecutor()
        with self.assertRaises(Exp010GateBOperatorError) as caught:
            executor.capture((), trace_sequence_index=0)
        self.assertEqual(caught.exception.code, "GATE_B_SHADOW_CAPTURE_FORBIDDEN")

    def test_preflight_serving_executor_refuses(self) -> None:
        executor = gate_b_operator._RefusingServingExecutor()
        with self.assertRaises(Exp010GateBOperatorError) as caught:
            executor.execute(object())
        self.assertEqual(
            caught.exception.code, "GATE_B_PREFLIGHT_SERVING_FORBIDDEN"
        )


class GateBPreflightTests(unittest.TestCase):
    def test_preflight_creates_no_stores(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(root, Path(raw) / "operands.json")
            plan = build_gate_b_plan(load_operands(path))
            self.assertFalse((root / STORE_SUBDIRECTORY).exists())
            self.assertFalse((root / "output").exists())
            self.assertEqual(plan["restart"]["state"], "FRESH")
            self.assertEqual(plan["source_target"]["durable_source_records"], 0)

    def test_would_create_claims_only_what_gate_b_creates(self) -> None:
        """`output/` is Gate E's capture location and is not a Gate-B effect."""

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(root, Path(raw) / "operands.json")
            plan = build_gate_b_plan(load_operands(path))
            created = plan["would_create"]
            self.assertIn(str(root / STORE_SUBDIRECTORY), created)
            self.assertNotIn(str(root / "output"), created)
            self.assertTrue(plan["would_not_create"]["output_dir"])
            self.assertTrue(plan["would_not_create"]["gate_c_evidence"])

    def test_preflight_reports_zero_effects(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(root, Path(raw) / "operands.json")
            plan = build_gate_b_plan(load_operands(path))
            self.assertEqual(plan["physical_searches_issued_by_preflight"], 0)
            self.assertEqual(plan["serve_calls_issued_by_preflight"], 0)
            self.assertEqual(plan["schema_version"], GATE_B_PLAN_SCHEMA_VERSION)
            self.assertEqual(plan["gate"], "B")
            self.assertTrue(plan["gate_a"]["authority_inherited"])

    def test_incomplete_campaign_fails_closed_at_the_authority_check(self) -> None:
        """An incompleteness marker means Gate A never committed.

        `load_verified_gate_a_evidence` already refuses any non-COMPLETE state,
        so on the CLI path that check fires first and owns this invariant. The
        campaign-state guard in `build_gate_b_plan` is defence-in-depth for
        direct API callers, exercised separately below.
        """

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            (root / ".gate_a_incomplete").write_text("{}", encoding="utf-8")
            path = _write_operands(root, Path(raw) / "operands.json")
            with self.assertRaises(Exp010GateBOperatorError) as caught:
                load_operands(path)
            self.assertEqual(
                caught.exception.code, "GATE_B_GATE_A_AUTHORITY_UNVERIFIED"
            )

    def test_campaign_state_guard_holds_for_direct_api_callers(self) -> None:
        """`build_gate_b_plan` re-checks state rather than trusting its caller."""

        with tempfile.TemporaryDirectory() as raw:
            good = Path(raw) / "good"
            _seed_gate_a_evidence(good)
            operands = load_operands(
                _write_operands(good, Path(raw) / "operands.json")
            )
            # Same verified authority, but pointed at a root Gate A never
            # completed: a caller bypassing load_operands must still fail.
            bare = Path(raw) / "bare"
            bare.mkdir()
            rebound = type(operands)(
                campaign_root=bare,
                detector_seed=operands.detector_seed,
                host_address=operands.host_address,
                host_port=operands.host_port,
                target_source_records=operands.target_source_records,
                etcd_container=operands.etcd_container,
                minio_container=operands.minio_container,
                authority=operands.authority,
                deployment_identity=operands.deployment_identity,
                data_identity=operands.data_identity,
                gate_a_evidence_sha256=operands.gate_a_evidence_sha256,
            )
            with self.assertRaises(Exp010GateBOperatorError) as caught:
                build_gate_b_plan(rebound)
            self.assertEqual(caught.exception.code, "GATE_B_CAMPAIGN_NOT_COMPLETE")

    def test_preflight_refuses_partial_store_set(self) -> None:
        """A half-present store set is ambiguous and is never repaired."""

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            stores = root / STORE_SUBDIRECTORY
            stores.mkdir(parents=True)
            (stores / "v2_source.sqlite3").write_bytes(b"")
            path = _write_operands(root, Path(raw) / "operands.json")
            with self.assertRaises(Exp010GateBOperatorError) as caught:
                build_gate_b_plan(load_operands(path))
            self.assertEqual(caught.exception.code, "GATE_B_STORE_SET_INCOMPLETE")

    def test_preflight_refuses_unbindable_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            held = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            held.bind(("127.0.0.1", 0))
            held.listen(1)
            port = int(held.getsockname()[1])
            try:
                path = _write_operands(
                    root, Path(raw) / "operands.json", host_port=port
                )
                with self.assertRaises(Exp010GateBOperatorError) as caught:
                    build_gate_b_plan(load_operands(path))
                self.assertEqual(
                    caught.exception.code, "GATE_B_ENDPOINT_UNBINDABLE"
                )
            finally:
                held.close()

    def test_preflight_plan_digest_is_load_bearing(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(root, Path(raw) / "operands.json")
            plan = build_gate_b_plan(load_operands(path))
            stated = plan.pop("plan_sha256")
            recomputed = gate_b_operator.strict_canonical_digest(
                gate_b_operator._PLAN_DOMAIN, plan
            )
            self.assertEqual(stated, recomputed)
            plan["detector_seed"] = 1
            self.assertNotEqual(
                stated,
                gate_b_operator.strict_canonical_digest(
                    gate_b_operator._PLAN_DOMAIN, plan
                ),
            )


class GateBCliTests(unittest.TestCase):
    def test_preflight_never_reaches_the_hosting_seam(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(root, Path(raw) / "operands.json")
            stream = io.StringIO()
            with mock.patch.object(
                gate_b_operator, "run_gate_b_host_from_cli"
            ) as host:
                with contextlib.redirect_stdout(stream):
                    code = main(["--operands", str(path), "--mode", "preflight"])
            self.assertEqual(code, 0)
            host.assert_not_called()
            document = json.loads(stream.getvalue())
            self.assertEqual(document["gate"], "B")

    def test_execute_requires_the_explicit_confirmation_flag(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(root, Path(raw) / "operands.json")
            errors = io.StringIO()
            with mock.patch.object(
                gate_b_operator, "run_gate_b_host_from_cli"
            ) as host:
                with contextlib.redirect_stderr(errors):
                    code = main(["--operands", str(path), "--mode", "execute"])
            self.assertEqual(code, 2)
            host.assert_not_called()
            self.assertIn("GATE_B_CONFIRMATION_REQUIRED", errors.getvalue())

    def test_execute_with_confirmation_reaches_the_seam_once(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            path = _write_operands(root, Path(raw) / "operands.json")
            stream = io.StringIO()
            with mock.patch.object(
                gate_b_operator,
                "run_gate_b_host_from_cli",
                return_value={
                    "gate": "B",
                    "durable_source_records": GATE_B_TARGET_SOURCE_RECORDS,
                    "complete_windows": 3,
                },
            ) as host:
                with contextlib.redirect_stdout(stream):
                    code = main(
                        [
                            "--operands",
                            str(path),
                            "--mode",
                            "execute",
                            "--confirm-gate-b-ingress",
                        ]
                    )
            self.assertEqual(code, 0)
            self.assertEqual(host.call_count, 1)

    def test_authority_failure_reports_its_reason_code(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            root.mkdir()
            path = _write_operands(root, Path(raw) / "operands.json")
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                code = main(["--operands", str(path), "--mode", "preflight"])
            self.assertEqual(code, 2)
            self.assertIn("GATE_B_GATE_A_AUTHORITY_UNVERIFIED", errors.getvalue())


class GateBIsolationTests(unittest.TestCase):
    """The operator must never write outside the campaign it was given."""

    def test_preflight_writes_nothing_at_all(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            before = {
                str(item.relative_to(raw)): item.stat().st_mtime_ns
                for item in Path(raw).rglob("*")
            }
            path = _write_operands(root, Path(raw) / "operands.json")
            build_gate_b_plan(load_operands(path))
            after = {
                str(item.relative_to(raw)): item.stat().st_mtime_ns
                for item in Path(raw).rglob("*")
                if item.name != "operands.json"
            }
            for name, mtime in before.items():
                self.assertIn(name, after)
                self.assertEqual(after[name], mtime, name)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()


class GateBServingPreflightSeamTests(unittest.TestCase):
    """Regression cover for the real `ServingPreflightResult` contract.

    The original defect read `getattr(admission, "admitted", False)`. That
    attribute does not exist on `ServingPreflightResult`, so a perfectly
    healthy stack was refused unconditionally and the empty `reason_codes`
    surfaced as "UNKNOWN". Tests using fakes could not catch it, because a fake
    is free to invent whichever attribute the code happens to read.

    These tests therefore drive `run_gate_b_host_from_cli` with the REAL
    frozen dataclass. Only true external boundaries are stubbed (Milvus client,
    Docker probe, harness, dataset pin, plan/binding construction); the
    admission decision itself stays real. If the operator ever again reads a
    field the contract does not define, `complete=True` would stop proceeding
    and these tests fail.
    """

    def _patched_seam(self, result, runner_sentinel):
        """Patch only external boundaries, leaving the decision under test."""

        import vdbench.docker_health as docker_health
        import vdbench.exp010_v2_host as v2_host
        import vdbench.milvus_actuation as milvus_actuation
        import vdbench.milvus_serving as milvus_serving
        import vdbench.v2_milvus_shadow_capture as shadow_capture

        class _Executor:
            def __init__(self, **_: object) -> None:
                pass

            def preflight(self):
                return result  # the REAL ServingPreflightResult

        class _Harness:
            def __init__(self, *_: object, **__: object) -> None:
                pass

            def index_identity(self, *_: object, **__: object) -> object:
                return object()

        class _Binding:
            def __init__(self, **_: object) -> None:
                pass

        class _Plan:
            def __init__(self, **_: object) -> None:
                pass

        class _Dataset:
            data_identity = "DATASET-001-v1:sha256:" + "a" * 64

        def _boom(*_: object, **__: object):
            raise runner_sentinel

        return contextlib.ExitStack(), [
            mock.patch.object(shadow_capture, "build_readonly_milvus_client",
                              lambda *_a, **_k: object()),
            mock.patch.object(docker_health, "DockerSocketHealthProbe",
                              lambda **_k: object()),
            mock.patch.object(milvus_actuation, "MilvusHarness", _Harness),
            mock.patch.object(milvus_actuation, "CollectionIdentityBinding", _Binding),
            mock.patch.object(v2_host, "pin_dataset001_identity",
                              lambda *_a, **_k: _Dataset()),
            mock.patch.object(milvus_serving, "HostServingPlan", _Plan),
            mock.patch.object(milvus_serving, "MilvusRangeServingExecutor", _Executor),
            mock.patch.object(gate_b_operator, "Exp010LiveRunner", _boom),
        ]

    def _run_seam(self, result):
        from vdbench.exp010_gate_b_operator import run_gate_b_host_from_cli

        class _PastPreflight(Exception):
            """Raised by the patched runner: proves the gate was passed."""

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "campaign"
            _seed_gate_a_evidence(root)
            operands = load_operands(
                _write_operands(root, Path(raw) / "operands.json")
            )
            stack, patches = self._patched_seam(result, _PastPreflight())
            with stack:
                for patch in patches:
                    stack.enter_context(patch)
                try:
                    run_gate_b_host_from_cli(operands)
                except _PastPreflight:
                    return "PASSED_PREFLIGHT"
                except Exp010GateBOperatorError as exc:
                    return exc
        return "NO_OUTCOME"

    def test_uses_the_real_result_type(self) -> None:
        """Guard the contract itself, so a rename is loud rather than silent."""

        from vdbench.milvus_serving import ServingPreflightResult

        result = ServingPreflightResult(
            complete=True, checked_stream_count=1, reason_codes=()
        )
        self.assertTrue(hasattr(result, "complete"))
        self.assertTrue(hasattr(result, "reason_codes"))
        self.assertTrue(hasattr(result, "checked_stream_count"))
        # The exact field the original defect invented must not exist.
        self.assertFalse(hasattr(result, "admitted"))

    def test_complete_true_proceeds_past_serving_preflight(self) -> None:
        from vdbench.milvus_serving import ServingPreflightResult

        outcome = self._run_seam(
            ServingPreflightResult(
                complete=True, checked_stream_count=1, reason_codes=()
            )
        )
        self.assertEqual(
            outcome,
            "PASSED_PREFLIGHT",
            "a complete admission must not be refused "
            f"(got {outcome!r}) — this is the original defect",
        )

    def test_complete_false_fails_closed(self) -> None:
        from vdbench.milvus_serving import ServingPreflightResult

        outcome = self._run_seam(
            ServingPreflightResult(
                complete=False,
                checked_stream_count=1,
                reason_codes=("STACK_HEALTH_UNHEALTHY",),
            )
        )
        self.assertIsInstance(outcome, Exp010GateBOperatorError)
        self.assertEqual(outcome.code, "GATE_B_SERVING_PREFLIGHT_REFUSED")

    def test_reason_codes_are_propagated_verbatim(self) -> None:
        from vdbench.milvus_serving import ServingPreflightResult

        outcome = self._run_seam(
            ServingPreflightResult(
                complete=False,
                checked_stream_count=1,
                reason_codes=("COLLECTION_LOAD_STATE_UNAVAILABLE:FLAT", "X_Y"),
            )
        )
        self.assertIsInstance(outcome, Exp010GateBOperatorError)
        message = str(outcome)
        self.assertIn("COLLECTION_LOAD_STATE_UNAVAILABLE:FLAT", message)
        self.assertIn("X_Y", message)
        self.assertNotIn("UNKNOWN", message)

    def test_empty_reason_codes_are_reported_honestly(self) -> None:
        """A refusal with no codes must not be labelled with an invented one."""

        from vdbench.milvus_serving import ServingPreflightResult

        outcome = self._run_seam(
            ServingPreflightResult(
                complete=False, checked_stream_count=0, reason_codes=()
            )
        )
        self.assertIsInstance(outcome, Exp010GateBOperatorError)
        self.assertIn("NO_REASON_REPORTED", str(outcome))
