"""Focused offline proof of the ADR-017 Gate-A operator boundary.

Nothing here contacts Milvus, Docker, or the network: the metadata reader, the
container inspector, the revision resolver, and the clock are all injected. The
DATASET-001 corpus is the real committed one, because `data_identity` is derived
rather than asserted and a fake corpus would prove nothing.
"""

from __future__ import annotations

import io
import json
import os
import unittest
from contextlib import redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from vdbench.config import INDEX_NAME, Metric
from vdbench.exp010_gate_a_operator import (
    GATE_A_EVIDENCE_FILENAME,
    GATE_A_EVIDENCE_SUBDIRECTORY,
    OPERAND_FIELDS,
    Exp010GateAOperatorError,
    build_gate_a_evidence,
    build_gate_a_plan,
    initialize_v5_campaign,
    load_operands,
    main,
    observe_environment,
)
from vdbench.exp010_serving_configuration import (
    Exp010ServingConfiguration,
    derive_serving_configuration_identity,
)

DATASET001 = Path(__file__).parents[1] / "artifacts" / "exp-001" / "dataset"

#: Proven stable project identities (ADR-017 item 2). These are the values the
#: V4 durable store bindings carry, and the digest preimage proof in ADR-017
#: reproduces V4's bound `configuration_identity` from the serving operands.
_FLAT_BINDING = "b63cf68a332127416d0cdf5372d4b8f4bac0c27d8f44b59c78b0953c4669bb46"
_HNSW_BINDING = "2db7944f6aa5190736ddafd1d25391aba648b5931734fc4b833ff02b3cec7bca"
_RADIUS = 191.85897352125554
_REVISION = "717cd8028731d618c39f4cf3d0be01f8cbd33847"
_ROWS = 10000
_DIMENSIONS = 128


def _configuration_identity() -> str:
    return derive_serving_configuration_identity(
        Exp010ServingConfiguration(
            metric=Metric.L2,
            threshold_stratum="target-075",
            threshold_radius=_RADIUS,
            range_filter=0.0,
            limit=100,
            served_ef=400,
            dimensions=_DIMENSIONS,
            consistency_level="Strong",
        )
    )


def _operand_values(campaign_root: Path, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "deployment_identity": "ENV-001-exp010-l2-v1",
        "stream_id": "exp010-live-l2-target075-v5",
        "campaign_root": str(campaign_root),
        "milvus_uri": "http://localhost:19530",
        "flat_collection_name": "vd_exp010_l2_flat_v1",
        "hnsw_collection_name": "vd_exp010_l2_hnsw_v1",
        "metric": "L2",
        "threshold_stratum": "target-075",
        "threshold_radius": _RADIUS,
        "range_filter": 0.0,
        "limit": 100,
        "served_ef": 400,
        "dimensions": _DIMENSIONS,
        "consistency_level": "Strong",
        "configuration_identity": _configuration_identity(),
        "flat_binding_id": _FLAT_BINDING,
        "hnsw_binding_id": _HNSW_BINDING,
        "source_revision": _REVISION,
        "expected_row_count": _ROWS,
        "hnsw_m": 16,
        "hnsw_ef_construction": 200,
        "dataset001_dir": str(DATASET001),
        "etcd_container": "milvus-etcd",
        "minio_container": "milvus-minio",
        "milvus_container": "milvus-standalone",
    }
    values.update(overrides)
    return values


def _write_operands(directory: Path, values: dict[str, object]) -> Path:
    path = directory / "gate_a_operands.json"
    path.write_text(json.dumps(values), encoding="utf-8")
    return path


class _FakeMetadataClient:
    """Exposes the four metadata calls, and a `search` that must never run."""

    def __init__(self, **overrides: object) -> None:
        self.overrides = overrides
        self.searches = 0
        self.calls: list[str] = []

    def _value(self, name: str, default: object) -> object:
        return self.overrides.get(name, default)

    def describe_collection(self, collection_name: str) -> dict[str, object]:
        self.calls.append(f"describe_collection:{collection_name}")
        return {
            "collection_name": self._value("collection_name", collection_name),
            "fields": [
                {"name": "id", "params": {}},
                {
                    "name": "vector",
                    "params": {"dim": self._value("dim", _DIMENSIONS)},
                },
            ],
        }

    def get_collection_stats(self, collection_name: str) -> dict[str, object]:
        self.calls.append(f"get_collection_stats:{collection_name}")
        return {"row_count": self._value("row_count", _ROWS)}

    def get_load_state(self, collection_name: str) -> dict[str, object]:
        self.calls.append(f"get_load_state:{collection_name}")
        return {"state": self._value("load_state", "Loaded")}

    def describe_index(
        self, collection_name: str, index_name: str
    ) -> dict[str, object]:
        self.calls.append(f"describe_index:{collection_name}")
        is_hnsw = "hnsw" in collection_name
        document: dict[str, object] = {
            "index_name": index_name,
            "field_name": "vector",
            "index_type": self._value(
                "hnsw_index_type" if is_hnsw else "flat_index_type",
                "HNSW" if is_hnsw else "FLAT",
            ),
            "metric_type": self._value("metric_type", "L2"),
            "state": self._value("index_state", "Finished"),
            "total_rows": _ROWS,
            "indexed_rows": self._value("indexed_rows", _ROWS),
            "pending_index_rows": self._value("pending_index_rows", 0),
        }
        if is_hnsw:
            document["M"] = str(self._value("M", 16))
            document["efConstruction"] = str(self._value("efConstruction", 200))
        return document

    def search(self, *args: object, **kwargs: object) -> object:
        self.searches += 1
        raise AssertionError("Gate A must never issue a physical search")


def _container_document(name: str) -> dict[str, object]:
    return {
        "Id": f"sha256:{name}",
        "RestartCount": 0,
        "State": {
            "Status": "running",
            "StartedAt": "2026-08-16T12:04:09.262267046Z",
            "OOMKilled": False,
            "Health": {"Status": "healthy"},
        },
    }


def _inspector(**overrides: object):
    def inspect(name: str) -> dict[str, object]:
        document = _container_document(name)
        for key, value in overrides.items():
            document["State"][key] = value
        return document

    return inspect


def _clock() -> datetime:
    return datetime(2026, 8, 16, 12, 30, 0, tzinfo=UTC)


def _observe(operands, client=None, inspector=None, clock=_clock):
    return observe_environment(
        operands,
        metadata_reader=client or _FakeMetadataClient(),
        container_inspector=inspector or _inspector(),
        revision_resolver=lambda: _REVISION,
        clock=clock,
    )


class GateAOperandTests(unittest.TestCase):
    def test_incomplete_operands_are_rejected(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            values = _operand_values(root / "v5")
            del values["deployment_identity"]
            path = _write_operands(root, values)
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                load_operands(path)
        self.assertEqual(raised.exception.code, "GATE_A_OPERANDS_INCOMPLETE")
        self.assertIn("deployment_identity", str(raised.exception))

    def test_unknown_extra_operands_are_rejected(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            values = _operand_values(root / "v5", environment_manifest_sha256="e" * 64)
            path = _write_operands(root, values)
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                load_operands(path)
        self.assertEqual(raised.exception.code, "GATE_A_OPERANDS_UNEXPECTED")

    def test_deployment_identity_has_no_default(self) -> None:
        """ADR-017 item 3: it is a governed operator input, never defaulted."""

        self.assertIn("deployment_identity", OPERAND_FIELDS)
        for empty in ("", "   "):
            with TemporaryDirectory() as temporary:
                root = Path(temporary)
                path = _write_operands(
                    root, _operand_values(root / "v5", deployment_identity=empty)
                )
                with self.assertRaises(Exp010GateAOperatorError) as raised:
                    load_operands(path)
            self.assertEqual(raised.exception.code, "GATE_A_OPERAND_INVALID")

    def test_data_identity_can_never_be_supplied_by_the_operator(self) -> None:
        """It is derived from the verified corpus, so it is not an operand."""

        self.assertNotIn("data_identity", OPERAND_FIELDS)
        self.assertNotIn("environment_manifest_sha256", OPERAND_FIELDS)

    def test_served_ef_change_invalidates_the_governed_configuration(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _write_operands(root, _operand_values(root / "v5", served_ef=800))
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                load_operands(path)
        self.assertEqual(
            raised.exception.code, "GATE_A_CONFIGURATION_IDENTITY_MISMATCH"
        )

    def test_stale_foreign_configuration_identity_is_rejected(self) -> None:
        stale = (
            "exp010-serving-config-v1:sha256:"
            "0000000000000000000000000000000000000000000000000000000000000000"
        )
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _write_operands(
                root, _operand_values(root / "v5", configuration_identity=stale)
            )
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                load_operands(path)
        self.assertEqual(
            raised.exception.code, "GATE_A_CONFIGURATION_IDENTITY_MISMATCH"
        )

    def test_identical_binding_ids_and_collections_are_rejected(self) -> None:
        for overrides in (
            {"hnsw_binding_id": _FLAT_BINDING},
            {"hnsw_collection_name": "vd_exp010_l2_flat_v1"},
        ):
            with TemporaryDirectory() as temporary:
                root = Path(temporary)
                path = _write_operands(root, _operand_values(root / "v5", **overrides))
                with self.assertRaises(Exp010GateAOperatorError) as raised:
                    load_operands(path)
            self.assertEqual(raised.exception.code, "GATE_A_OPERAND_INVALID")

    def test_malformed_source_revision_is_rejected(self) -> None:
        for bad in ("717cd80", _REVISION.upper(), "z" * 40):
            with TemporaryDirectory() as temporary:
                root = Path(temporary)
                path = _write_operands(
                    root, _operand_values(root / "v5", source_revision=bad)
                )
                with self.assertRaises(Exp010GateAOperatorError) as raised:
                    load_operands(path)
            self.assertEqual(raised.exception.code, "GATE_A_OPERAND_INVALID")

    def test_non_normalized_or_relative_campaign_root_is_rejected(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            traversal = _operand_values(root / "v5")
            traversal["campaign_root"] = f"{root}/live-v5/../../escape"
            path = _write_operands(root, traversal)
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                load_operands(path)
            self.assertEqual(raised.exception.code, "GATE_A_CAMPAIGN_ROOT_UNSAFE")

            relative = _operand_values(root / "v5")
            relative["campaign_root"] = "relative/v5"
            path = _write_operands(root, relative)
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                load_operands(path)
            self.assertEqual(raised.exception.code, "GATE_A_OPERAND_INVALID")


class GateAObservationTests(unittest.TestCase):
    def _operands(self, root: Path, **overrides: object):
        return load_operands(
            _write_operands(root, _operand_values(root / "v5", **overrides))
        )

    def test_correct_fresh_live_metadata_is_accepted(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            observation = _observe(self._operands(root))
        self.assertEqual(observation.observed_at_utc, "2026-08-16T12:30:00Z")
        self.assertEqual(observation.flat["index_type"], "FLAT")
        self.assertEqual(observation.hnsw["index_type"], "HNSW")
        self.assertEqual(observation.hnsw["M"], 16)
        self.assertEqual(observation.hnsw["efConstruction"], 200)
        self.assertEqual(len(observation.environment_manifest_sha256), 64)
        self.assertTrue(
            observation.data_identity.startswith("DATASET-001-v1:sha256:")
        )
        self.assertEqual(
            observation.environment_observation["flat_index_identity"], _FLAT_BINDING
        )
        self.assertEqual(
            observation.environment_observation["hnsw_index_identity"], _HNSW_BINDING
        )

    def test_observation_issues_zero_searches(self) -> None:
        client = _FakeMetadataClient()
        with TemporaryDirectory() as temporary:
            _observe(self._operands(Path(temporary)), client=client)
        self.assertEqual(client.searches, 0)
        self.assertFalse(any(call.startswith("search") for call in client.calls))

    def test_the_metadata_reader_cannot_search_at_all(self) -> None:
        """Structural, not conventional: the wrapper defines no `search`."""

        from vdbench.exp010_gate_a_operator import _ReadOnlyMetadataReader

        reader = _ReadOnlyMetadataReader(_FakeMetadataClient())
        self.assertFalse(hasattr(reader, "search"))
        self.assertIsNone(getattr(type(reader), "search", None))

    def test_source_revision_mismatch_is_rejected(self) -> None:
        with TemporaryDirectory() as temporary:
            operands = self._operands(Path(temporary))
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                observe_environment(
                    operands,
                    metadata_reader=_FakeMetadataClient(),
                    container_inspector=_inspector(),
                    revision_resolver=lambda: "0" * 40,
                    clock=_clock,
                )
        self.assertEqual(raised.exception.code, "GATE_A_SOURCE_REVISION_MISMATCH")

    def test_source_revision_is_bound_into_the_evidence(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands = self._operands(root)
            evidence = build_gate_a_evidence(operands, _observe(operands))
        self.assertEqual(evidence["source_revision"], _REVISION)
        self.assertEqual(
            evidence["environment_observation"]["source_revision"], _REVISION
        )

    def test_live_mismatches_are_each_rejected(self) -> None:
        cases = (
            ({"collection_name": "wrong_collection"}, "GATE_A_COLLECTION_NAME_MISMATCH"),
            ({"row_count": 9999}, "GATE_A_ROW_COUNT_MISMATCH"),
            ({"dim": 64}, "GATE_A_DIMENSION_MISMATCH"),
            ({"metric_type": "COSINE"}, "GATE_A_METRIC_MISMATCH"),
            ({"flat_index_type": "HNSW"}, "GATE_A_INDEX_TYPE_MISMATCH"),
            ({"hnsw_index_type": "FLAT"}, "GATE_A_INDEX_TYPE_MISMATCH"),
            ({"M": 32}, "GATE_A_HNSW_M_MISMATCH"),
            ({"efConstruction": 400}, "GATE_A_HNSW_EF_CONSTRUCTION_MISMATCH"),
            ({"index_state": "InProgress"}, "GATE_A_INDEX_NOT_FINISHED"),
            ({"indexed_rows": 5000}, "GATE_A_INDEX_INCOMPLETE"),
            ({"pending_index_rows": 7}, "GATE_A_INDEX_INCOMPLETE"),
            ({"load_state": "NotLoad"}, "GATE_A_COLLECTION_NOT_LOADED"),
        )
        for overrides, expected in cases:
            with self.subTest(expected=expected), TemporaryDirectory() as temporary:
                operands = self._operands(Path(temporary))
                with self.assertRaises(Exp010GateAOperatorError) as raised:
                    _observe(operands, client=_FakeMetadataClient(**overrides))
                self.assertEqual(raised.exception.code, expected)

    def test_container_lifetime_failures_are_rejected(self) -> None:
        cases = (
            ({"Status": "exited"}, "GATE_A_CONTAINER_NOT_RUNNING"),
            ({"OOMKilled": True}, "GATE_A_CONTAINER_OOM_KILLED"),
            ({"Health": {"Status": "unhealthy"}}, "GATE_A_CONTAINER_NOT_HEALTHY"),
        )
        for overrides, expected in cases:
            with self.subTest(expected=expected), TemporaryDirectory() as temporary:
                operands = self._operands(Path(temporary))
                with self.assertRaises(Exp010GateAOperatorError) as raised:
                    _observe(operands, inspector=_inspector(**overrides))
                self.assertEqual(raised.exception.code, expected)

    def test_container_lifetime_identities_are_recorded(self) -> None:
        with TemporaryDirectory() as temporary:
            observation = _observe(self._operands(Path(temporary)))
        self.assertEqual(
            sorted(observation.containers), ["etcd", "milvus", "minio"]
        )
        for entry in observation.containers.values():
            self.assertEqual(entry["status"], "running")
            self.assertEqual(entry["health"], "healthy")
            self.assertEqual(entry["restart_count"], 0)
            self.assertIs(entry["oom_killed"], False)
            self.assertTrue(entry["started_at"])

    def test_manifest_digest_is_deterministic_and_time_sensitive(self) -> None:
        with TemporaryDirectory() as temporary:
            operands = self._operands(Path(temporary))
            first = _observe(operands)
            repeat = _observe(operands)
            later = _observe(
                operands,
                clock=lambda: datetime(2026, 8, 16, 13, 0, 0, tzinfo=UTC),
            )
        self.assertEqual(
            first.environment_manifest_sha256, repeat.environment_manifest_sha256
        )
        self.assertNotEqual(
            first.environment_manifest_sha256, later.environment_manifest_sha256
        )

    def test_v4_environment_digest_is_never_reproduced(self) -> None:
        """A fresh Gate A cannot coincide with the retired V4 manifest."""

        v4 = "49b309a4067cc89c7dadee0e54beb27a673851029672453b94351a6fdf9b6549"
        with TemporaryDirectory() as temporary:
            observation = _observe(self._operands(Path(temporary)))
        self.assertNotEqual(observation.environment_manifest_sha256, v4)


class GateAInitializationTests(unittest.TestCase):
    def _prepare(
        self, root: Path, campaign_root: Path | None = None, **overrides: object
    ):
        operands = load_operands(
            _write_operands(
                root,
                _operand_values(campaign_root or (root / "v5"), **overrides),
            )
        )
        return operands, _observe(operands)

    def test_preflight_creates_nothing(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands, observation = self._prepare(root)
            plan = build_gate_a_plan(operands, observation)
            self.assertFalse(operands.campaign_root.exists())
            self.assertEqual(
                sorted(entry.name for entry in root.iterdir()),
                ["gate_a_operands.json"],
            )
        self.assertEqual(plan["physical_searches_issued_by_preflight"], 0)
        self.assertEqual(plan["serve_calls_issued_by_preflight"], 0)
        self.assertIs(plan["campaign_root_exists"], False)
        self.assertEqual(len(plan["plan_sha256"]), 64)

    def test_plan_reports_exactly_what_execute_would_write(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands, observation = self._prepare(root)
            plan = build_gate_a_plan(operands, observation)
            evidence = initialize_v5_campaign(operands, observation)
            written = sorted(
                str(path) for path in operands.campaign_root.rglob("*")
            )
        self.assertEqual(
            plan["would_create"][0], str(operands.campaign_root)
        )
        self.assertEqual(plan["evidence_sha256"], evidence["evidence_sha256"])
        self.assertEqual(
            written,
            sorted(
                [str(operands.evidence_directory), str(operands.evidence_path)]
            ),
        )

    def test_execute_creates_a_complete_verifiable_campaign(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands, observation = self._prepare(root)
            evidence = initialize_v5_campaign(operands, observation)
            self.assertTrue(operands.evidence_path.is_file())
            stored = json.loads(operands.evidence_path.read_text(encoding="utf-8"))
            # Gate A owns the campaign root and the gate_a subdirectory only.
            self.assertEqual(
                sorted(entry.name for entry in operands.campaign_root.iterdir()),
                [GATE_A_EVIDENCE_SUBDIRECTORY],
            )
            self.assertEqual(
                sorted(entry.name for entry in operands.evidence_directory.iterdir()),
                [GATE_A_EVIDENCE_FILENAME],
            )
        self.assertEqual(stored, evidence)
        self.assertEqual(
            stored["environment_manifest_sha256"],
            observation.environment_manifest_sha256,
        )
        self.assertEqual(stored["physical_searches_issued_by_gate_a"], 0)

    def test_gate_a_never_creates_gate_b_or_gate_c_evidence(self) -> None:
        from vdbench.exp010_gate_c_operator import _REQUIRED_STORES

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands, observation = self._prepare(root)
            initialize_v5_campaign(operands, observation)
            present = {path.name for path in operands.campaign_root.rglob("*")}
        for store in _REQUIRED_STORES:
            self.assertNotIn(store, present)

    def test_an_existing_campaign_is_refused(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands, observation = self._prepare(root)
            initialize_v5_campaign(operands, observation)
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                initialize_v5_campaign(operands, observation)
        self.assertEqual(
            raised.exception.code, "GATE_A_CAMPAIGN_ALREADY_INITIALIZED"
        )

    def test_a_partial_campaign_never_masquerades_as_a_pass(self) -> None:
        """An existing root without complete evidence is an error, never PASS."""

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands, observation = self._prepare(root)
            operands.campaign_root.mkdir()  # a bare, evidence-less root
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                initialize_v5_campaign(operands, observation)
            self.assertFalse(operands.evidence_path.exists())
        self.assertEqual(
            raised.exception.code, "GATE_A_CAMPAIGN_ALREADY_INITIALIZED"
        )

    def test_atomic_publish_failure_leaves_no_campaign_root(self) -> None:
        def failing_publish(source: Path, target: Path) -> None:
            raise OSError("simulated rename failure")

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands, observation = self._prepare(root)
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                initialize_v5_campaign(
                    operands, observation, _publish=failing_publish
                )
            self.assertFalse(operands.campaign_root.exists())
            self.assertFalse(operands.evidence_path.exists())
            # No staging residue is left behind either.
            self.assertEqual(
                sorted(entry.name for entry in root.iterdir()),
                ["gate_a_operands.json"],
            )
        self.assertEqual(raised.exception.code, "GATE_A_CAMPAIGN_WRITE_FAILED")

    def test_a_symlinked_campaign_root_is_refused(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            decoy = root / "decoy"
            decoy.mkdir()
            operands, observation = self._prepare(root)
            operands.campaign_root.symlink_to(decoy, target_is_directory=True)
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                initialize_v5_campaign(operands, observation)
            self.assertEqual(
                sorted(entry.name for entry in decoy.iterdir()), []
            )
        self.assertEqual(
            raised.exception.code, "GATE_A_CAMPAIGN_ALREADY_INITIALIZED"
        )

    def test_no_historical_campaign_path_can_be_targeted(self) -> None:
        """V1-V4 roots already exist, so every one of them is refused."""

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            for historical in ("v1", "v2", "v3", "v4"):
                target = root / f"live-l2-target075-{historical}"
                (target / "stores").mkdir(parents=True)
                marker = target / "stores" / "v2_source.sqlite3"
                marker.write_bytes(b"historical")
                operands, observation = self._prepare(root, campaign_root=target)
                with self.assertRaises(Exp010GateAOperatorError) as raised:
                    initialize_v5_campaign(operands, observation)
                self.assertEqual(
                    raised.exception.code, "GATE_A_CAMPAIGN_ALREADY_INITIALIZED"
                )
                self.assertEqual(marker.read_bytes(), b"historical")


class GateACommandTests(unittest.TestCase):
    def _argv(self, root: Path, *extra: str) -> list[str]:
        path = _write_operands(root, _operand_values(root / "v5"))
        return ["--operands", str(path), *extra]

    def _run(self, argv: list[str]) -> tuple[int, str]:
        """Drive `main` with an injected observation and captured stdout."""

        stream = io.StringIO()
        with patch(
            "vdbench.exp010_gate_a_operator.observe_environment",
            lambda operands, **_: _observe(operands),
        ), redirect_stdout(stream):
            code = main(argv)
        return code, stream.getvalue()

    def test_execute_requires_the_explicit_confirmation_flag(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            argv = self._argv(root, "--mode", "execute")
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                self._run(argv)
            self.assertFalse((root / "v5").exists())
        self.assertEqual(
            raised.exception.code, "GATE_A_INITIALIZATION_NOT_CONFIRMED"
        )

    def test_preflight_mode_returns_zero_and_creates_nothing(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            code, output = self._run(self._argv(root, "--mode", "preflight"))
            self.assertFalse((root / "v5").exists())
        self.assertEqual(code, 0)
        plan = json.loads(output)
        self.assertEqual(plan["schema_version"], "exp010-gate-a-plan-v1")
        self.assertEqual(plan["physical_searches_issued_by_preflight"], 0)

    def test_confirmed_execute_initializes_exactly_once(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            argv = self._argv(
                root, "--mode", "execute", "--confirm-initialize-v5"
            )
            code, _ = self._run(argv)
            self.assertTrue(
                (
                    root / "v5" / GATE_A_EVIDENCE_SUBDIRECTORY
                    / GATE_A_EVIDENCE_FILENAME
                ).is_file()
            )
            with self.assertRaises(Exp010GateAOperatorError) as raised:
                self._run(argv)
        self.assertEqual(code, 0)
        self.assertEqual(
            raised.exception.code, "GATE_A_CAMPAIGN_ALREADY_INITIALIZED"
        )


class GateBGateCCompatibilityTests(unittest.TestCase):
    def test_gate_c_consumes_the_gate_a_authority_shape(self) -> None:
        """ADR-017 item 9: authority flows A -> B -> C, and C is unchanged."""

        from vdbench.exp010_gate_c_operator import (
            OPERAND_FIELDS as GATE_C_FIELDS,
        )
        from vdbench.exp010_gate_c_operator import (
            load_operands as load_gate_c_operands,
        )

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands = load_operands(
                _write_operands(root, _operand_values(root / "v5"))
            )
            observation = _observe(operands)
            evidence = build_gate_a_evidence(operands, observation)

            gate_c_values = {
                "stream_id": operands.stream_id,
                "metric": operands.metric.value,
                "threshold_stratum": operands.threshold_stratum,
                "threshold_radius": operands.threshold_radius,
                "range_filter": operands.range_filter,
                "limit": operands.limit,
                "served_ef": operands.served_ef,
                "dimensions": operands.dimensions,
                "consistency_level": operands.consistency_level,
                "configuration_identity": operands.configuration_identity,
                "flat_binding_id": operands.flat_binding_id,
                "hnsw_binding_id": operands.hnsw_binding_id,
                "source_revision": operands.source_revision,
                # The whole point: Gate C binds the digest Gate A produced.
                "environment_manifest_sha256": evidence[
                    "environment_manifest_sha256"
                ],
                "detector_seed": 20260816,
                "milvus_uri": operands.milvus_uri,
                "flat_collection_name": operands.flat_collection_name,
                "hnsw_collection_name": operands.hnsw_collection_name,
                "store_root": str(operands.campaign_root / "stores"),
                "dataset001_dir": str(operands.dataset001_dir),
                "exp010_output_dir": str(operands.campaign_root / "output"),
                "etcd_container": operands.etcd_container,
                "minio_container": operands.minio_container,
            }
            self.assertEqual(sorted(gate_c_values), sorted(GATE_C_FIELDS))
            path = root / "gate_c_operands.json"
            path.write_text(json.dumps(gate_c_values), encoding="utf-8")
            gate_c = load_gate_c_operands(path)

        self.assertEqual(
            gate_c.environment_manifest_sha256,
            observation.environment_manifest_sha256,
        )
        self.assertEqual(
            gate_c.configuration_identity, operands.configuration_identity
        )
        self.assertEqual(gate_c.source_revision, _REVISION)

    def test_gate_c_still_refuses_an_uninitialized_campaign(self) -> None:
        """Gate A creates no stores, so Gate C must still refuse the campaign."""

        from vdbench.exp010_gate_c_operator import (
            Exp010GateCOperatorError,
            _require_initialized_stores,
        )

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands = load_operands(
                _write_operands(root, _operand_values(root / "v5"))
            )
            initialize_v5_campaign(operands, _observe(operands))
            with self.assertRaises(Exp010GateCOperatorError) as raised:
                _require_initialized_stores(operands.campaign_root / "stores")
        self.assertEqual(raised.exception.code, "GATE_C_STORE_ROOT_MISSING")


class GateAGuardTests(unittest.TestCase):
    def test_module_imports_no_authority_and_no_pymilvus(self) -> None:
        """Gate A creates no authority and cannot reach Milvus at import time."""

        import ast

        import vdbench.exp010_gate_a_operator as module

        tree = ast.parse(
            Path(module.__file__).read_text(encoding="utf-8"), filename=module.__file__
        )
        top_level = {
            node.module or ""
            for node in tree.body
            if isinstance(node, ast.ImportFrom)
        } | {
            alias.name
            for node in tree.body
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        forbidden = {
            "policy",
            "actuation",
            "canary_admission",
            "canary_approval",
            "canary_activation",
            "canary_route_authority",
            "canary_routing",
            "canary_live_runner",
            "canary_grant_store",
            "pymilvus",
        }
        self.assertFalse(forbidden & {name.lstrip(".") for name in top_level})

    def test_no_search_call_appears_anywhere_in_the_module(self) -> None:
        """The strongest available static proof of ADR-017 item 10."""

        import ast

        import vdbench.exp010_gate_a_operator as module

        source = Path(module.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source, filename=module.__file__)
        called = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertNotIn("search", called)
        self.assertNotIn("serve", called)


class GateAEvidenceCompletenessTests(unittest.TestCase):
    def test_evidence_independently_reconstructs_the_manifest_digest(self) -> None:
        from vdbench.exp010_live_runner import build_environment_manifest_sha256

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands = load_operands(
                _write_operands(root, _operand_values(root / "v5"))
            )
            observation = _observe(operands)
            evidence = build_gate_a_evidence(operands, observation)

        recomputed = build_environment_manifest_sha256(
            dict(evidence["environment_observation"])
        )
        self.assertEqual(recomputed, evidence["environment_manifest_sha256"])

    def test_evidence_carries_every_material_phase_four_field(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands = load_operands(
                _write_operands(root, _operand_values(root / "v5"))
            )
            evidence = build_gate_a_evidence(operands, _observe(operands))

        self.assertEqual(evidence["gate"], "A")
        self.assertEqual(evidence["schema_version"], "exp010-gate-a-evidence-v1")
        self.assertEqual(evidence["serialization_contract"], "vd-canonical-json-v2")
        self.assertEqual(
            evidence["campaign"]["deployment_identity"], "ENV-001-exp010-l2-v1"
        )
        self.assertEqual(evidence["dataset"]["version"], "DATASET-001-v1")
        self.assertEqual(evidence["flat"]["index_identity"], _FLAT_BINDING)
        self.assertEqual(evidence["hnsw"]["index_identity"], _HNSW_BINDING)
        self.assertEqual(evidence["hnsw"]["live"]["M"], 16)
        self.assertEqual(evidence["hnsw"]["live"]["efConstruction"], 200)
        self.assertEqual(evidence["serving"]["threshold_radius"], _RADIUS)
        self.assertEqual(evidence["serving"]["served_ef"], 400)
        self.assertEqual(evidence["flat"]["live"]["index_name"], INDEX_NAME)
        self.assertEqual(len(evidence["evidence_sha256"]), 64)
        for key in ("etcd", "minio", "milvus"):
            self.assertIn("started_at", evidence["containers"][key])

    def test_evidence_file_is_written_read_only(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            operands = load_operands(
                _write_operands(root, _operand_values(root / "v5"))
            )
            initialize_v5_campaign(operands, _observe(operands))
            mode = operands.evidence_path.stat().st_mode
            self.assertEqual(mode & 0o777, 0o400)
            self.assertFalse(os.access(operands.evidence_path, os.W_OK))


if __name__ == "__main__":
    unittest.main()
