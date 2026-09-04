"""ADR-021 LKG qualification operator composition tests.

Every test here runs against deterministic in-memory fakes and
test-managed temporary directories. No Milvus client, no Docker socket,
no health endpoint, no vector search, no real ENV-001 contact, and no
real LKG/Phase-1/Phase-2/Checkpoint-C/D1/D2 state is ever created.

The deep semantics of the ledger, seal, Phase-2 ingestion, Checkpoint-C
statistics, and Phase-3 resolution are owned and tested by their own
component test modules; these tests prove the *composition seam* the
operator adds on top of them.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from tests.test_lkg_window_readiness_observation import (
    _observe as _canonical_health, _rollback as _canonical_rollback,
    _container as _health_container,
)

import vdbench.lkg_qualification_operator as lkg_operator
from vdbench.lkg_dataset003_loader import LkgDataset003Workload
from vdbench.lkg_qualification_evidence import (
    LkgAttemptStatus,
    build_lkg_query_attempt,
    build_lkg_query_observation,
)
from vdbench.lkg_qualification_operator import (
    OPERAND_FIELDS,
    POSITIONS_PER_WINDOW,
    WINDOWS_PER_RUN,
    LkgOperatorDependencies,
    LkgQualificationOperatorError,
    build_lkg_qualification_plan,
    checkpoint_c_ledger_path,
    execute_lkg_qualification,
    load_operands,
    MetadataOnlyMilvusReader,
    phase1_ledger_path,
    phase2_readiness_ledger_path,
    production_dependencies,
    LkgProductionWindowReadinessObserver,
    read_route_state_record,
    readiness_store_path,
    resolve_and_persist_phase3_authority,
    run_preflight,
    verified_latest_lkg_present,
)
from vdbench.config import Metric
from vdbench.deployment_governance import (
    CANONICAL_ENV001_DEPLOYMENT_IDENTITY,
    LKG_AUTHORITY_STORE_FILENAME,
    DeploymentGovernanceError,
    canonical_deployment_governance_scope,
    ensure_deployment_scope_directory,
    resolve_deployment_governance_scope,
)
from vdbench.lkg_phase3_persistence import LkgPhase3AuthorityReferenceStore
from vdbench.lkg_qualification_runner import LkgQualificationRunner
from vdbench.lkg_run_binding import lkg_ordered_query_ids_sha256
from vdbench.lkg_window_readiness_observation import (
    LkgWindowHealthObservation,
    LkgWindowReadinessObservationError,
    LkgWindowRollbackReadiness,
)

_FIRST_QUERY_ID = 10000
_QUERY_COUNT = 2400
_QUERY_IDS = tuple(range(_FIRST_QUERY_ID, _FIRST_QUERY_ID + _QUERY_COUNT))
_REVISION = "a" * 40
_ENVIRONMENT_IDENTITY = _canonical_health().document["observed_environment_identity"]
_MANIFEST_SHA = "1" * 64
_QUERY_ID_ARRAY_SHA = "2" * 64
_QUERY_ARRAY_SHA = "3" * 64
_ORDERED_IDS_SHA = lkg_ordered_query_ids_sha256(list(_QUERY_IDS))
_NOW = "2026-09-01T00:00:00.000000Z"
_SERVING_IDENTITY = "exp010-serving-config-v1:sha256:" + "8" * 64
_SOURCE_RUN_ID = "lkg-qualification-run-0001"
_BASELINE_CONFIG_SHA = (
    "772fbd5746d27a5d04719a1b644fdba98843635efd5610e5a0e80a16889a43ee"
)


def _container():
    return {
        "Id": "c" * 64,
        "Image": "sha256:" + "d" * 64,
        "RestartCount": 0,
        "State": {
            "Status": "running",
            "OOMKilled": False,
            "StartedAt": "2026-08-26T03:51:13Z",
            "Health": {"Status": "healthy"},
        },
    }


def _image():
    return {"RepoDigests": ["repo@sha256:" + "e" * 64]}


class _MetadataReader:
    """Metadata-only fake; structurally has no search method."""

    def describe_collection(self, *, collection_name):
        return {
            "collection_name": collection_name,
            "fields": [
                {"name": "id", "data_type": "5", "is_primary": True, "params": {}},
                {"name": "vector", "data_type": "101", "is_primary": False,
                 "params": {"dim": 128}},
            ],
        }

    def describe_index(self, *, collection_name, index_name):
        return {
            "index_name": index_name,
            "index_type": "HNSW" if "hnsw" in collection_name else "FLAT",
            "metric_type": "L2", "state": "Finished",
            "pending_index_rows": 0, "indexed_rows": 10000,
            "M": "16", "efConstruction": "200",
        }

    def get_collection_stats(self, *, collection_name):
        return {"row_count": 10000}

    def get_load_state(self, *, collection_name):
        return {"state": "Loaded"}


def _scope(governance_root: Path | str):
    """One isolated test governance scope under a temporary canonical root.

    The ONLY way a test reaches an alternate root: an in-process
    ``DeploymentGovernanceScope`` passed explicitly. No environment variable,
    CLI flag, operand, or module global exists that could do this, in a test
    or in production (ADR-022 section 12).
    """

    return resolve_deployment_governance_scope(canonical_root=governance_root)


def _operand_values(**overrides: object) -> dict[str, object]:
    """Fixture operands. No deployment-global path is supplied -- there is no
    operand for one: the route-state marker and the LKG (D2) authority store
    are DERIVED from the deployment governance scope (ADR-022)."""

    values: dict[str, object] = {
        "base_data_identity": "DATASET-001-v1:sha256:" + "b" * 64,
        "database_name": "default",
        "dataset001_dir": "/srv/vd/dataset001",
        "dataset002_dir": "/srv/vd/dataset002",
        "dataset003_dir": "/srv/vd/dataset003",
        "dimensions": 128,
        "environment_identity": _ENVIRONMENT_IDENTITY,
        "etcd_container": "milvus-etcd",
        "execution_source_revision": _REVISION,
        "expected_entity_count": 10000,
        "expected_query_count": _QUERY_COUNT,
        "flat_collection_name": "vd_flat",
        "hnsw_collection_name": "vd_hnsw",
        "index_identity": "hnsw-index-v1",
        "index_name": "vector_index",
        "metric": "L2",
        "milvus_container": "milvus-standalone",
        "milvus_uri": "http://127.0.0.1:19530",
        "minio_container": "milvus-minio",
        "producer_identity": "vdbench.lkg_qualification_operator",
        "qualification_dataset_id": "DATASET-003",
        "qualification_dataset_version": "v1",
        "qualification_manifest_sha256": _MANIFEST_SHA,
        "qualification_ordered_query_ids_sha256": _ORDERED_IDS_SHA,
        "qualification_query_array_sha256": _QUERY_ARRAY_SHA,
        "qualification_query_id_array_sha256": _QUERY_ID_ARRAY_SHA,
        "qualification_query_role": "lkg_qualification",
        "served_ef": 400,
        "serving_configuration_identity": "exp010-serving-config-v1:sha256:" + "8" * 64,
        "source_run_id": "lkg-qualification-run-0001",
        "threshold_radius": 191.85897352125554,
        "threshold_stratum": "target-075",
    }
    values.update(overrides)
    return values


def _write_operands(directory: Path, **overrides: object) -> Path:
    path = directory / "operands.json"
    path.write_text(json.dumps(_operand_values(**overrides)))
    return path


def _load(directory: Path, **overrides: object):
    return load_operands(_write_operands(directory, **overrides))


def _workload(query_ids=_QUERY_IDS, **overrides) -> LkgDataset003Workload:
    fields = {
        "query_ids": tuple(query_ids),
        "query_vectors": {query_id: None for query_id in query_ids},
        "dataset_id": "DATASET-003",
        "dataset_version": "v1",
        "manifest_sha256": _MANIFEST_SHA,
        "query_role": "lkg_qualification",
        "query_id_array_sha256": _QUERY_ID_ARRAY_SHA,
        "query_array_sha256": _QUERY_ARRAY_SHA,
    }
    fields.update(overrides)
    return LkgDataset003Workload(**fields)  # type: ignore[arg-type]


class _CountingRunner(LkgQualificationRunner):
    """A real ``LkgQualificationRunner`` subclass with no adapter at all.

    Deliberately never calls ``super().__init__``: there is no
    ``LkgMilvusAdapter``, no ``MilvusHarness``, and no client anywhere in
    this object, so a physical search is impossible rather than merely
    unasked-for. ``search_calls`` counts dispatches, which is how the
    zero-search assertions below are made.
    """

    def __init__(self, *, recall: float = 1.0, latency_ms: float = 1.0, fail_at: int | None = None):
        self.search_calls = 0
        self._recall = recall
        self._latency_ms = latency_ms
        self._fail_at = fail_at

    def attempt_query(
        self,
        *,
        query_id,
        query_vector,
        metric,
        threshold_stratum,
        ef,
        radius,
        attempt_sequence,
        attempt_number,
        run_binding_sha256,
    ):
        self.search_calls += 1
        if self._fail_at is not None and attempt_sequence == self._fail_at:
            return build_lkg_query_attempt(
                query_id=query_id,
                attempt_sequence=attempt_sequence,
                attempt_number=attempt_number,
                status=LkgAttemptStatus.CLIENT_ERROR,
                error_code="CLIENT_ERROR:Injected",
                run_binding_sha256=run_binding_sha256,
            )
        observation = build_lkg_query_observation(
            query_id=query_id,
            metric=metric,
            threshold_stratum=threshold_stratum,
            ef=ef,
            recall=self._recall,
            latency_ms=self._latency_ms,
            start_ns=1_000 * attempt_sequence,
            end_ns=1_000 * attempt_sequence + 500,
            exact_cardinality=5,
            threshold_violation_count=0,
        )
        return build_lkg_query_attempt(
            query_id=query_id,
            attempt_sequence=attempt_sequence,
            attempt_number=attempt_number,
            status=LkgAttemptStatus.SUCCESS,
            run_binding_sha256=run_binding_sha256,
            observation=observation,
        )


def _health(passed: bool = True, reasons: tuple[str, ...] = (), **context) -> LkgWindowHealthObservation:
    result = _canonical_health(
        **context, identity=_ENVIRONMENT_IDENTITY,
        container=lambda: _health_container(health="healthy" if passed else "unhealthy"),
    )
    assert result.passed == passed and result.reason_codes == reasons
    return result


def _rollback(**context) -> LkgWindowRollbackReadiness:
    return _canonical_rollback(**context)


class _CountingObserver:
    """Counts readiness observations; optionally fails or refuses at a window."""

    def __init__(self, *, fail_window: int | None = None, unable_window: int | None = None):
        self.calls = 0
        self.windows: list[int] = []
        self._fail_window = fail_window
        self._unable_window = unable_window

    def observe(self, *, source_run_id, source_run_binding_sha256, window_index, readiness_check_id):
        self.calls += 1
        self.windows.append(window_index)
        if self._unable_window is not None and window_index == self._unable_window:
            raise LkgWindowReadinessObservationError("LKG_READINESS_CONTAINER_UNAVAILABLE")
        context = {"source_run_id": source_run_id, "source_run_binding_sha256": source_run_binding_sha256}
        if self._fail_window is not None and window_index == self._fail_window:
            return _health(False, ("CONTAINER_UNHEALTHY",), **context), _rollback(**context)
        return _health(**context), _rollback(**context)


class _Recorder:
    """Records every dependency call so refusals can be proven to precede them."""

    def __init__(self):
        self.workload_loads = 0
        self.environment_observations = 0
        self.source_verifications: list[str] = []
        self.runner_builds = 0
        self.observer_builds = 0


def _dependencies(
    *,
    runner: _CountingRunner | None = None,
    observer: _CountingObserver | None = None,
    workload: LkgDataset003Workload | None = None,
    observed_environment_identity: str = _ENVIRONMENT_IDENTITY,
    source_verifier_error: Exception | None = None,
    recorder: _Recorder | None = None,
) -> tuple[LkgOperatorDependencies, _CountingRunner, _CountingObserver, _Recorder]:
    runner = runner or _CountingRunner()
    observer = observer or _CountingObserver()
    recorder = recorder or _Recorder()
    resolved_workload = workload if workload is not None else _workload()
    counter = {"ns": 0}

    def _workload_loader():
        recorder.workload_loads += 1
        return resolved_workload

    def _runner_factory(configuration):
        recorder.runner_builds += 1
        return runner

    def _observer_factory(run_binding):
        recorder.observer_builds += 1
        return observer

    def _environment(run_binding):
        recorder.environment_observations += 1
        return observed_environment_identity

    def _verify_source(expected_revision):
        recorder.source_verifications.append(expected_revision)
        if source_verifier_error is not None:
            raise source_verifier_error

    def _monotonic_ns():
        counter["ns"] += 1_000
        return counter["ns"]

    return (
        LkgOperatorDependencies(
            workload_loader=_workload_loader,
            runner_factory=_runner_factory,
            observer_factory=_observer_factory,
            environment_identity_observer=_environment,
            execution_source_verifier=_verify_source,
            clock=lambda: _NOW,
            monotonic_ns=_monotonic_ns,
        ),
        runner,
        observer,
        recorder,
    )


class _RealRootGuardMixin(unittest.TestCase):
    """No test may create real deployment state under the canonical root.

    A test that forgets to inject its scope would silently fall through to
    production authority. The prepared-authority gate already refuses that
    combination before any search or store, but this asserts the stronger
    property directly: the suite never brings the real canonical deployment
    scope into being (ADR-022 section 15).
    """

    def setUp(self) -> None:
        super().setUp()
        real = Path(canonical_deployment_governance_scope().scope_root)
        existed = real.exists()
        self.addCleanup(
            lambda: self.assertEqual(
                real.exists(),
                existed,
                f"a test created real deployment state at {real}",
            )
        )


class _TempCase(_RealRootGuardMixin):
    def setUp(self) -> None:
        super().setUp()
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.directory = Path(self._temporary.name)
        self.governance_root = self.directory / "vd-governance"
        self.scope = _scope(self.governance_root)

    def operands(self, **overrides):
        return _load(self.directory, **overrides)

    def plan(self, **overrides):
        return build_lkg_qualification_plan(
            self.operands(**overrides), governance_scope=self.scope
        )

    def execute(self, operands, digest, dependencies):
        return execute_lkg_qualification(
            operands,
            dependencies=dependencies,
            confirm_live_lkg_qualification_searches=True,
            expected_prepared_authority_sha256=digest,
            governance_scope=self.scope,
        )

    @property
    def run_root(self):
        return self.scope.run_root(_SOURCE_RUN_ID)

    def assertNoStores(self) -> None:
        for path in (
            phase1_ledger_path(self.run_root),
            readiness_store_path(self.run_root),
            phase2_readiness_ledger_path(self.run_root),
            checkpoint_c_ledger_path(self.run_root),
        ):
            self.assertFalse(Path(path).exists(), f"unexpected durable store: {path}")


# ======================================================================
# 1-2. Operands and the closed key set
# ======================================================================


class OperandTests(_TempCase):
    def test_cli_and_module_import_smoke(self) -> None:
        import vdbench.lkg_qualification_operator as module

        self.assertTrue(callable(module.main))
        self.assertEqual(len(set(OPERAND_FIELDS)), len(OPERAND_FIELDS))

    def test_complete_operand_file_loads(self) -> None:
        operands = self.operands()
        self.assertEqual(operands.source_run_id, "lkg-qualification-run-0001")
        self.assertEqual(operands.search_configuration.ef, 400)
        self.assertEqual(operands.search_configuration.limit, 100)
        self.assertEqual(operands.search_configuration.consistency_level, "Strong")

    def test_missing_operand_is_refused(self) -> None:
        values = _operand_values()
        del values["served_ef"]
        path = self.directory / "bad.json"
        path.write_text(json.dumps(values))
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            load_operands(path)
        self.assertEqual(caught.exception.code, "LKG_OPERANDS_INCOMPLETE")

    def test_unexpected_operand_is_refused_not_ignored(self) -> None:
        values = _operand_values()
        values["extra_knob"] = "yes"
        path = self.directory / "bad.json"
        path.write_text(json.dumps(values))
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            load_operands(path)
        self.assertEqual(caught.exception.code, "LKG_OPERANDS_UNEXPECTED")

    def test_malformed_operand_file_is_refused(self) -> None:
        path = self.directory / "bad.json"
        path.write_text("{not json")
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            load_operands(path)
        self.assertEqual(caught.exception.code, "LKG_OPERANDS_MALFORMED")

    def test_invalid_search_configuration_is_refused(self) -> None:
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            self.operands(threshold_stratum="not-a-stratum")
        self.assertEqual(caught.exception.code, "LKG_SEARCH_CONFIGURATION_INVALID")

    def test_population_other_than_2400_is_refused(self) -> None:
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            self.operands(expected_query_count=1200)
        self.assertEqual(caught.exception.code, "LKG_OPERAND_INVALID")

    def test_run_root_is_not_an_operand(self) -> None:
        """It is DERIVED, so 'same run id, different root' is unrepresentable."""

        self.assertNotIn("run_root", OPERAND_FIELDS)
        values = _operand_values()
        values["run_root"] = "/somewhere/else"
        path = self.directory / "bad.json"
        path.write_text(json.dumps(values))
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            load_operands(path)
        self.assertEqual(caught.exception.code, "LKG_OPERANDS_UNEXPECTED")

    def test_source_run_id_cannot_escape_its_scope_root(self) -> None:
        for hostile in ("../escape", "a/b", ".", "..", "/abs", "with space", ""):
            with self.subTest(source_run_id=hostile):
                with self.assertRaises(LkgQualificationOperatorError) as caught:
                    self.operands(source_run_id=hostile)
                self.assertEqual(caught.exception.code, "LKG_OPERAND_INVALID")

    def test_legacy_global_path_operands_are_refused_not_ignored(self) -> None:
        """P1-A/P1-B closure at the operand surface (ADR-022 sections 9-10).

        Neither deployment-global path is an operand any more. An operand file
        carrying one -- a stale V1 file, or a deliberate attempt to select a
        second authority universe -- is refused by the closed schema before any
        governance store is constructed, any ledger exists, or any search is
        dispatched. It is not silently ignored, which would be worse: the
        operator would then read a canonical path while the human reviewed a
        document that named a different one.
        """

        for field, value in (
            ("route_state_path", "/serving/route.json"),
            ("lkg_authority_store_path", "/srv/other/lkg_authority.sqlite3"),
        ):
            with self.subTest(field=field):
                self.assertNotIn(field, OPERAND_FIELDS)
                values = _operand_values()
                values[field] = value
                path = self.directory / f"legacy_{field}.json"
                path.write_text(json.dumps(values))
                with self.assertRaises(LkgQualificationOperatorError) as caught:
                    load_operands(path)
                self.assertEqual(caught.exception.code, "LKG_OPERANDS_UNEXPECTED")
                self.assertIn(field, str(caught.exception))

    def test_bool_is_not_accepted_where_an_int_is_required(self) -> None:
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            self.operands(served_ef=True)
        self.assertEqual(caught.exception.code, "LKG_OPERAND_INVALID")


# ======================================================================
# 6-9, 60. The prepared authority is deterministic and freezes everything
# ======================================================================


class PreparedAuthorityTests(_TempCase):
    def test_same_operands_reproduce_identical_authority(self) -> None:
        first = self.plan()
        second = self.plan()
        self.assertEqual(first, second)
        self.assertEqual(
            first["prepared_authority_sha256"], second["prepared_authority_sha256"]
        )

    def test_authority_freezes_source_run_id_run_root_and_every_store_path(self) -> None:
        plan = self.plan()
        self.assertEqual(plan["source_run_id"], "lkg-qualification-run-0001")
        self.assertEqual(plan["run_root"], self.run_root)
        self.assertEqual(
            plan["deployment_governance_scope_root"], self.scope.scope_root
        )
        self.assertEqual(
            plan["store_paths"]["readiness_store"], readiness_store_path(self.run_root)
        )
        self.assertEqual(
            plan["store_paths"]["phase1_ledger"], phase1_ledger_path(self.run_root)
        )
        self.assertEqual(
            plan["store_paths"]["checkpoint_c_ledger"],
            checkpoint_c_ledger_path(self.run_root),
        )

    def test_exactly_one_readiness_path_is_derivable_for_a_run_root(self) -> None:
        self.assertEqual(
            readiness_store_path(self.run_root), readiness_store_path(self.run_root)
        )
        self.assertNotEqual(
            readiness_store_path(self.run_root),
            readiness_store_path(self.scope.run_root("other-run")),
        )

    def test_every_material_operand_changes_the_authority_digest(self) -> None:
        baseline = self.plan()["prepared_authority_sha256"]
        for override in (
            {"source_run_id": "lkg-qualification-run-0002"},
            {"execution_source_revision": "b" * 40},
            {"served_ef": 200},
            {"threshold_radius": 191.0},
            {"environment_identity": "lkg-env-identity-v1:sha256:" + "f" * 64},
            {"qualification_manifest_sha256": "9" * 64},
            {"qualification_ordered_query_ids_sha256": "9" * 64},
            {"hnsw_collection_name": "vd_other"},
            {"serving_configuration_identity": "exp010-serving-config-v1:sha256:" + "7" * 64},
        ):
            with self.subTest(override=override):
                changed = self.plan(**override)
                self.assertNotEqual(baseline, changed["prepared_authority_sha256"])

    def test_operator_never_generates_a_run_identity(self) -> None:
        """Structural: there is no id-minting code path at all."""

        source = Path("src/vdbench/lkg_qualification_operator.py").read_text()
        for forbidden in ("uuid", "token_hex", "token_urlsafe", "random.", "secrets"):
            self.assertNotIn(forbidden, source, f"id-minting facility present: {forbidden}")


# ======================================================================
# 3-5, 19-20, 22. Preflight and prepare: zero search, zero durable state
# ======================================================================


class PreflightAndPrepareTests(_TempCase):
    def test_preflight_reports_prospective_paths_and_creates_nothing(self) -> None:
        report = run_preflight(self.operands(), governance_scope=self.scope)
        self.assertEqual(report["mode"], "preflight")
        self.assertEqual(
            report["plan"]["store_paths"]["readiness_store"],
            readiness_store_path(self.run_root),
        )
        self.assertEqual(report["existing_store_files"], [])
        self.assertFalse(Path(self.run_root).exists())
        self.assertNoStores()

    def test_preflight_and_prepare_dispatch_no_search_and_no_observation(self) -> None:
        dependencies, runner, observer, recorder = _dependencies()
        run_preflight(self.operands(), governance_scope=self.scope)
        self.plan()
        self.assertEqual(runner.search_calls, 0)
        self.assertEqual(observer.calls, 0)
        self.assertEqual(recorder.workload_loads, 0)
        self.assertEqual(recorder.environment_observations, 0)
        self.assertNoStores()

    def test_preflight_reports_an_existing_store_without_opening_it(self) -> None:
        Path(self.run_root).mkdir(parents=True)
        Path(readiness_store_path(self.run_root)).write_bytes(b"not a database")
        report = run_preflight(self.operands(), governance_scope=self.scope)
        self.assertEqual(report["existing_store_files"], ["window_readiness.sqlite3"])


# ======================================================================
# 11-21, 55-59. The live execution gate refuses before search and before stores
# ======================================================================


class LiveExecutionGateTests(_TempCase):
    def _execute(self, *, operands=None, **kwargs):
        dependencies, runner, observer, recorder = _dependencies(
            **{k: v for k, v in kwargs.items() if k in {
                "runner", "observer", "workload", "observed_environment_identity",
                "source_verifier_error",
            }}
        )
        operands = operands if operands is not None else self.operands()
        plan = build_lkg_qualification_plan(operands, governance_scope=self.scope)
        return (
            operands,
            plan,
            dependencies,
            runner,
            observer,
            recorder,
        )

    def _expect_refusal(self, code, *, confirm=True, authority=..., **kwargs):
        operands, plan, dependencies, runner, observer, recorder = self._execute(**kwargs)
        if authority is ...:
            authority = plan["prepared_authority_sha256"]
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            execute_lkg_qualification(
                operands,
                dependencies=dependencies,
                confirm_live_lkg_qualification_searches=confirm,
                expected_prepared_authority_sha256=authority,
                governance_scope=self.scope,
            )
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(runner.search_calls, 0, "refusal must precede every search")
        self.assertEqual(observer.calls, 0, "refusal must precede every observation")
        self.assertNoStores()
        return caught.exception

    def test_missing_confirmation_refuses(self) -> None:
        self._expect_refusal("LKG_LIVE_EXECUTION_NOT_CONFIRMED", confirm=False)

    def test_missing_prepared_authority_refuses(self) -> None:
        self._expect_refusal("LKG_PREPARED_AUTHORITY_REQUIRED", authority=None)

    def test_empty_prepared_authority_refuses(self) -> None:
        self._expect_refusal("LKG_PREPARED_AUTHORITY_REQUIRED", authority="")

    def test_wrong_prepared_authority_digest_refuses(self) -> None:
        self._expect_refusal("LKG_PREPARED_AUTHORITY_MISMATCH", authority="0" * 64)

    def test_tampered_operands_no_longer_match_the_authorized_digest(self) -> None:
        authorized = self.plan()["prepared_authority_sha256"]
        tampered = self.operands(served_ef=200)
        self._expect_refusal(
            "LKG_PREPARED_AUTHORITY_MISMATCH", operands=tampered, authority=authorized
        )

    def test_different_source_run_id_against_prepared_authority_refuses(self) -> None:
        authorized = self.plan()["prepared_authority_sha256"]
        self._expect_refusal(
            "LKG_PREPARED_AUTHORITY_MISMATCH",
            operands=self.operands(source_run_id="lkg-qualification-run-0002"),
            authority=authorized,
        )

    def test_different_source_run_id_moves_the_entire_run_root(self) -> None:
        authorized = self.plan()["prepared_authority_sha256"]
        other = self.operands(source_run_id="lkg-qualification-run-0002")
        self.assertNotEqual(
            self.scope.run_root(other.source_run_id), self.run_root
        )
        self._expect_refusal(
            "LKG_PREPARED_AUTHORITY_MISMATCH", operands=other, authority=authorized
        )

    def test_wrong_execution_source_revision_refuses(self) -> None:
        self._expect_refusal(
            "LKG_EXECUTION_SOURCE_UNVERIFIED",
            source_verifier_error=RuntimeError("GATE_C_EXECUTION_SOURCE_REVISION_MISMATCH"),
        )

    def test_untracked_governed_source_refuses(self) -> None:
        drift = RuntimeError("drift")
        drift.code = "GATE_C_EXECUTION_SOURCE_DRIFT"  # type: ignore[attr-defined]
        exception = self._expect_refusal(
            "LKG_EXECUTION_SOURCE_UNVERIFIED", source_verifier_error=drift
        )
        self.assertIn("GATE_C_EXECUTION_SOURCE_DRIFT", str(exception))

    def test_wrong_dataset003_identity_refuses(self) -> None:
        self._expect_refusal(
            "LKG_DATASET003_IDENTITY_MISMATCH",
            workload=_workload(manifest_sha256="9" * 64),
        )

    def test_wrong_dataset003_query_role_refuses(self) -> None:
        self._expect_refusal(
            "LKG_DATASET003_IDENTITY_MISMATCH", workload=_workload(query_role="canary_audit")
        )

    def test_dataset002_substitution_refuses(self) -> None:
        self._expect_refusal(
            "LKG_DATASET003_IDENTITY_MISMATCH", workload=_workload(dataset_id="DATASET-002")
        )

    def test_too_few_positions_refuses(self) -> None:
        self._expect_refusal(
            "LKG_DATASET003_POPULATION_MISMATCH",
            workload=_workload(query_ids=_QUERY_IDS[:-1]),
        )

    def test_too_many_positions_refuses(self) -> None:
        self._expect_refusal(
            "LKG_DATASET003_POPULATION_MISMATCH",
            workload=_workload(query_ids=_QUERY_IDS + (12400,)),
        )

    def test_duplicate_position_refuses(self) -> None:
        ids = list(_QUERY_IDS)
        ids[5] = ids[4]
        self._expect_refusal(
            "LKG_DATASET003_POPULATION_MISMATCH", workload=_workload(query_ids=tuple(ids))
        )

    def test_reordered_positions_refuse(self) -> None:
        ids = list(_QUERY_IDS)
        ids[0], ids[1] = ids[1], ids[0]
        self._expect_refusal(
            "LKG_DATASET003_POPULATION_MISMATCH", workload=_workload(query_ids=tuple(ids))
        )

    def test_exp012_population_is_not_accepted_as_lkg_evidence(self) -> None:
        """EXP-012's 0..2399 positions are well-formed but are NOT this workload.

        Count, uniqueness, and ordering all pass, so the refusal comes from
        the stronger ordered-query-ID identity digest -- exactly the check
        that makes a substituted population impossible to slip through.
        """

        exception = self._expect_refusal(
            "LKG_DATASET003_IDENTITY_MISMATCH",
            workload=_workload(query_ids=tuple(range(0, 2400))),
        )
        self.assertIn("ordered_query_ids_sha256", str(exception))

    def test_first_and_last_canonical_positions_are_bound(self) -> None:
        operands = self.operands()
        plan = build_lkg_qualification_plan(operands)
        self.assertEqual(plan["dataset003"]["expected_query_count"], _QUERY_COUNT)
        self.assertEqual(_QUERY_IDS[0], 10000)
        self.assertEqual(_QUERY_IDS[-1], 12399)
        self.assertEqual(
            plan["dataset003"]["ordered_query_ids_sha256"],
            lkg_ordered_query_ids_sha256(list(_QUERY_IDS)),
        )

    def test_environment_identity_mismatch_refuses_before_search(self) -> None:
        self._expect_refusal(
            "LKG_ENVIRONMENT_IDENTITY_MISMATCH",
            observed_environment_identity="lkg-env-identity-v1:sha256:" + "9" * 64,
        )

    def test_refusal_order_environment_continuity_is_checked_last_before_dispatch(self) -> None:
        operands, plan, dependencies, runner, observer, recorder = self._execute(
            observed_environment_identity="lkg-env-identity-v1:sha256:" + "9" * 64
        )
        with self.assertRaises(LkgQualificationOperatorError):
            execute_lkg_qualification(
                operands,
                dependencies=dependencies,
                confirm_live_lkg_qualification_searches=True,
                expected_prepared_authority_sha256=plan["prepared_authority_sha256"],
                governance_scope=self.scope,
            )
        self.assertEqual(recorder.source_verifications, [_REVISION])
        self.assertEqual(recorder.workload_loads, 1)
        self.assertEqual(recorder.environment_observations, 1)
        self.assertEqual(runner.search_calls, 0)
        self.assertNoStores()


# ======================================================================
# 24-26, 31, 33-35, 40-42, 51. The complete authorized qualification path
# ======================================================================


class _FullRunMixin:
    """Runs the complete 2400-position qualification exactly once."""

    full_root: str
    full_report: dict
    full_operands_values: dict
    full_runner: _CountingRunner
    full_observer: _CountingObserver

    @classmethod
    def _run_full(cls, directory: Path) -> None:
        cls.full_governance_root = directory / "vd-governance"
        cls.full_scope = _scope(cls.full_governance_root)
        operands = _load(directory)
        run_root = cls.full_scope.run_root(operands.source_run_id)
        plan = build_lkg_qualification_plan(
            operands, governance_scope=cls.full_scope
        )
        dependencies, runner, observer, _ = _dependencies()
        cls.full_report = execute_lkg_qualification(
            operands,
            dependencies=dependencies,
            confirm_live_lkg_qualification_searches=True,
            expected_prepared_authority_sha256=plan["prepared_authority_sha256"],
            governance_scope=cls.full_scope,
        )
        cls.full_root = run_root
        cls.full_runner = runner
        cls.full_observer = observer


class FullQualificationTests(_RealRootGuardMixin, _FullRunMixin):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        cls.directory = Path(cls._temporary.name)
        cls._run_full(cls.directory)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def test_every_canonical_position_was_dispatched_exactly_once(self) -> None:
        self.assertEqual(self.full_runner.search_calls, _QUERY_COUNT)

    def test_exactly_twelve_readiness_captures_one_per_window(self) -> None:
        self.assertEqual(self.full_observer.calls, WINDOWS_PER_RUN)
        self.assertEqual(self.full_observer.windows, list(range(WINDOWS_PER_RUN)))
        self.assertEqual(len(self.full_report["readiness_windows"]), WINDOWS_PER_RUN)
        import sqlite3
        from vdbench.artifacts import canonical_json_bytes
        from vdbench.lkg_window_readiness_observation import (
            validate_lkg_window_health_observation, validate_lkg_window_rollback_readiness,
            LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY, LKG_ROLLBACK_READINESS_SOURCE_IDENTITY,
        )

        with closing(sqlite3.connect(readiness_store_path(self.full_root))) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone(), (2,))
            rows = connection.execute(
                "SELECT payload_document,health_source_document_bytes,rollback_source_document_bytes "
                "FROM lkg_window_readiness_evidence ORDER BY window_index"
            ).fetchall()
        self.assertEqual(len(rows), 12)
        self.assertEqual(sum(len(row[1:]) for row in rows), 24)
        for payload_raw, health_raw, rollback_raw in rows:
            payload, health_doc, rollback_doc = map(json.loads, (payload_raw, health_raw, rollback_raw))
            self.assertEqual(canonical_json_bytes(health_doc), health_raw)
            self.assertEqual(canonical_json_bytes(rollback_doc), rollback_raw)
            context = {"source_run_id": payload["source_run_id"],
                       "source_run_binding_sha256": payload["source_run_binding_sha256"]}
            validate_lkg_window_health_observation(
                LkgWindowHealthObservation(health_doc, payload["health_evidence_source_digest"], True, ()),
                source_identity=LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY,
                run_bound_environment_identity=_ENVIRONMENT_IDENTITY, **context,
            )
            validate_lkg_window_rollback_readiness(
                LkgWindowRollbackReadiness(rollback_doc, payload["rollback_evidence_source_digest"], True, ()),
                source_identity=LKG_ROLLBACK_READINESS_SOURCE_IDENTITY, **context,
            )

    def test_readiness_is_captured_after_each_two_hundred_positions(self) -> None:
        for window in self.full_report["readiness_windows"]:
            self.assertTrue(window["health_passed"])
            self.assertTrue(window["rollback_ready"])
        self.assertEqual(POSITIONS_PER_WINDOW * WINDOWS_PER_RUN, _QUERY_COUNT)

    def test_provider_run_ids_are_distinct_per_window(self) -> None:
        run_ids = {window["provider_run_id"] for window in self.full_report["readiness_windows"]}
        self.assertEqual(len(run_ids), WINDOWS_PER_RUN)

    def test_phase1_sealed_all_positions_successful(self) -> None:
        self.assertEqual(self.full_report["phase1_completion_state"], "ALL_POSITIONS_SUCCESSFUL")
        self.assertEqual(len(self.full_report["phase1_seal_digest"]), 64)

    def test_phase2_ingested_exactly_twelve_windows(self) -> None:
        self.assertEqual(self.full_report["phase2_ingested_windows"], list(range(WINDOWS_PER_RUN)))

    def test_checkpoint_c_is_passing_and_qualified(self) -> None:
        checkpoint = self.full_report["checkpoint_c"]
        self.assertEqual(checkpoint["status"], "PASSING")
        self.assertTrue(checkpoint["qualified"])
        self.assertEqual(checkpoint["evaluated_ef"], 400)
        self.assertEqual(len(checkpoint["canonical_evaluation_digest"]), 64)

    def test_qualification_mode_stops_and_creates_no_d1_or_d2(self) -> None:
        self.assertFalse(self.full_report["d1_created"])
        self.assertFalse(self.full_report["d2_created"])
        self.assertIn("STOP", self.full_report["next_step"])
        self.assertFalse(Path(self.full_scope.lkg_authority_store_path).exists())

    def test_only_the_frozen_store_paths_were_created(self) -> None:
        self.assertTrue(Path(phase1_ledger_path(self.full_root)).exists())
        self.assertTrue(Path(readiness_store_path(self.full_root)).exists())
        self.assertTrue(Path(phase2_readiness_ledger_path(self.full_root)).exists())
        self.assertTrue(Path(checkpoint_c_ledger_path(self.full_root)).exists())

    def test_post_seal_lookup_performs_zero_re_observation(self) -> None:
        """Phase 2 already ran; the observer count must not have grown."""

        self.assertEqual(self.full_observer.calls, WINDOWS_PER_RUN)

    def test_rerunning_terminal_execution_replays_without_new_evidence(self) -> None:
        operands = _load(self.directory)
        plan = build_lkg_qualification_plan(operands, governance_scope=self.full_scope)
        dependencies, runner, observer, _ = _dependencies()
        report = execute_lkg_qualification(
            operands,
            dependencies=dependencies,
            confirm_live_lkg_qualification_searches=True,
            expected_prepared_authority_sha256=plan["prepared_authority_sha256"],
            governance_scope=self.full_scope,
        )
        self.assertEqual(runner.search_calls, 0, "a completed run re-dispatches nothing")
        self.assertEqual(observer.calls, 0, "a completed run re-observes nothing")
        self.assertEqual(report["source_run_id"], self.full_report["source_run_id"])
        self.assertEqual(
            report["checkpoint_c"]["canonical_evaluation_digest"],
            self.full_report["checkpoint_c"]["canonical_evaluation_digest"],
        )
        self.assertEqual(report["phase1_seal_digest"], self.full_report["phase1_seal_digest"])


# ======================================================================
# 43-50. Phase 3 is separate and requires an externally reviewed digest
# ======================================================================


class Phase3BoundaryTests(_RealRootGuardMixin, _FullRunMixin):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        cls.directory = Path(cls._temporary.name)
        cls._run_full(cls.directory)
        cls.reviewed_digest = cls.full_report["checkpoint_c"]["canonical_evaluation_digest"]

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def _fresh_copy(self):
        """Copy the whole GOVERNANCE ROOT; the deployment scope lives inside it.

        A copied root re-derives the identical deployment namespace digest --
        the namespace is a function of the deployment identity alone -- so the
        copy is the same logical deployment's scope on an isolated tree.
        """

        directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, directory, True)
        governance_root = directory / "vd-governance"
        shutil.copytree(self.full_governance_root, governance_root)
        return directory, _scope(governance_root)

    def _phase3(self, digest, *, scope=None, directory=None, dependencies=None):
        if scope is None:
            directory, scope = self._fresh_copy()
        operands = _load(directory)
        if dependencies is None:
            dependencies, _, _, _ = _dependencies()
        return resolve_and_persist_phase3_authority(
            operands,
            dependencies=dependencies,
            expected_checkpoint_c_digest=digest,
            governance_scope=scope,
        )

    def _expect_source_refusal(self, error):
        directory, scope = self._fresh_copy()
        dependencies, runner, observer, recorder = _dependencies(
            source_verifier_error=error
        )
        with (
            patch.object(lkg_operator, "resolve_lkg_phase3_authority") as resolve_d1,
            patch.object(lkg_operator, "LkgPhase3AuthorityReferenceStore") as d2_store,
            patch.object(lkg_operator, "bind_lkg_phase3_authority") as bind_pair,
            self.assertRaises(LkgQualificationOperatorError) as caught,
        ):
            self._phase3(
                self.reviewed_digest,
                scope=scope,
                directory=directory,
                dependencies=dependencies,
            )
        self.assertEqual(caught.exception.code, "LKG_EXECUTION_SOURCE_UNVERIFIED")
        self.assertEqual(recorder.source_verifications, [_REVISION])
        self.assertEqual(recorder.workload_loads, 0)
        resolve_d1.assert_not_called()
        d2_store.assert_not_called()
        bind_pair.assert_not_called()
        self.assertFalse(Path(scope.lkg_authority_store_path).exists())
        self.assertEqual(runner.search_calls, 0)
        self.assertEqual(observer.calls, 0)
        return caught.exception

    def test_missing_expected_digest_refuses(self) -> None:
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            self._phase3("")
        self.assertEqual(caught.exception.code, "LKG_EXPECTED_CHECKPOINT_C_DIGEST_REQUIRED")

    def test_non_digest_expected_value_refuses(self) -> None:
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            self._phase3("not-a-digest")
        self.assertEqual(caught.exception.code, "LKG_EXPECTED_CHECKPOINT_C_DIGEST_REQUIRED")

    def test_wrong_expected_digest_refuses(self) -> None:
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            self._phase3("0" * 64)
        self.assertEqual(caught.exception.code, "LKG_PHASE3_AUTHORITY_REFUSED")

    def test_wrong_execution_source_revision_refuses_before_authority(self) -> None:
        exception = self._expect_source_refusal(
            RuntimeError("GATE_C_EXECUTION_SOURCE_REVISION_MISMATCH")
        )
        self.assertIn("GATE_C_EXECUTION_SOURCE_REVISION_MISMATCH", str(exception))

    def test_tracked_governed_source_drift_refuses_before_authority(self) -> None:
        drift = RuntimeError("tracked governed runtime byte drift")
        drift.code = "GATE_C_EXECUTION_SOURCE_DRIFT"  # type: ignore[attr-defined]
        exception = self._expect_source_refusal(drift)
        self.assertIn("GATE_C_EXECUTION_SOURCE_DRIFT", str(exception))

    def test_untracked_governed_executable_refuses_before_authority(self) -> None:
        drift = RuntimeError("untracked governed executable source")
        drift.code = "GATE_C_EXECUTION_SOURCE_DRIFT"  # type: ignore[attr-defined]
        exception = self._expect_source_refusal(drift)
        self.assertIn("GATE_C_EXECUTION_SOURCE_DRIFT", str(exception))

    def test_unexpected_verifier_failure_refuses_before_authority(self) -> None:
        exception = self._expect_source_refusal(ValueError("verifier failed"))
        self.assertIn("verifier failed", str(exception))

    def test_missing_source_stores_refuse(self) -> None:
        directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, directory, True)
        empty = _scope(directory / "empty-governance")
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            self._phase3(self.reviewed_digest, scope=empty, directory=directory)
        self.assertEqual(caught.exception.code, "LKG_PHASE3_SOURCE_STORE_MISSING")

    def test_reviewed_digest_resolves_d1_appends_d2_and_binds_the_pair(self) -> None:
        directory, scope = self._fresh_copy()
        dependencies, _, _, recorder = _dependencies()
        report = self._phase3(
            self.reviewed_digest,
            scope=scope,
            directory=directory,
            dependencies=dependencies,
        )
        self.assertEqual(recorder.source_verifications, [_REVISION])
        self.assertTrue(report["d1_resolved"])
        self.assertTrue(report["d2_appended"])
        self.assertTrue(report["verified_latest_present"])
        self.assertTrue(report["authority_pair_bound"])
        self.assertEqual(
            report["deployment_identity"], CANONICAL_ENV001_DEPLOYMENT_IDENTITY
        )
        self.assertEqual(
            report["lkg_authority_store_path"], scope.lkg_authority_store_path
        )
        self.assertTrue(Path(scope.lkg_authority_store_path).exists())

    def test_source_verification_precedes_phase3_authority_resolution(self) -> None:
        directory, scope = self._fresh_copy()
        dependencies, _, _, recorder = _dependencies()
        original_resolver = lkg_operator.resolve_lkg_phase3_authority

        def _resolve_after_source_verification(**kwargs):
            self.assertEqual(recorder.source_verifications, [_REVISION])
            return original_resolver(**kwargs)

        with patch.object(
            lkg_operator,
            "resolve_lkg_phase3_authority",
            side_effect=_resolve_after_source_verification,
        ):
            report = self._phase3(
                self.reviewed_digest,
                scope=scope,
                directory=directory,
                dependencies=dependencies,
            )
        self.assertTrue(report["d1_resolved"])
        self.assertTrue(report["d2_appended"])
        self.assertTrue(report["authority_pair_bound"])

    def test_phase3_dispatches_no_search(self) -> None:
        directory, scope = self._fresh_copy()
        operands = _load(directory)
        dependencies, runner, observer, _ = _dependencies()
        resolve_and_persist_phase3_authority(
            operands,
            dependencies=dependencies,
            expected_checkpoint_c_digest=self.reviewed_digest,
            governance_scope=scope,
        )
        self.assertEqual(runner.search_calls, 0)
        self.assertEqual(observer.calls, 0)


# ======================================================================
# 27-30, 32, 36-39, 53-54. Halting, readiness failure, restart authority
# ======================================================================


class ReadinessFailureAndRestartTests(_TempCase):
    def _execute(self, *, runner=None, observer=None):
        operands = self.operands()
        plan = build_lkg_qualification_plan(operands, governance_scope=self.scope)
        dependencies, runner, observer, _ = _dependencies(runner=runner, observer=observer)
        return (
            execute_lkg_qualification(
                operands,
                dependencies=dependencies,
                confirm_live_lkg_qualification_searches=True,
                expected_prepared_authority_sha256=plan["prepared_authority_sha256"],
                governance_scope=self.scope,
            ),
            runner,
            observer,
        )

    def test_observed_readiness_failure_stops_dispatch_at_that_window(self) -> None:
        report, runner, observer = self._execute(observer=_CountingObserver(fail_window=0))
        self.assertTrue(report["readiness_observed_failure"])
        self.assertEqual(observer.calls, 1)
        self.assertEqual(len(report["readiness_windows"]), 1)
        self.assertEqual(runner.search_calls, POSITIONS_PER_WINDOW)
        self.assertEqual(report["phase1_completion_state"], "INCOMPLETE_NO_FAILURE")
        self.assertEqual(report["checkpoint_c"]["status"], "FAILING")
        self.assertFalse(report["checkpoint_c"]["qualified"])
        self.assertFalse(report["d1_created"])

    def test_provider_inability_fails_closed_and_persists_nothing(self) -> None:
        operands = self.operands()
        plan = self.plan()
        dependencies, runner, observer, _ = _dependencies(
            observer=_CountingObserver(unable_window=0)
        )
        with self.assertRaises(LkgWindowReadinessObservationError):
            self.execute(operands, plan["prepared_authority_sha256"], dependencies)
        self.assertEqual(observer.calls, 1)
        self.assertFalse(
            Path(phase2_readiness_ledger_path(self.run_root)).exists(),
            "an inability must not advance the run",
        )
        self.assertFalse(Path(checkpoint_c_ledger_path(self.run_root)).exists())

    def test_a_durable_position_failure_halts_before_readiness_capture(self) -> None:
        report, runner, observer = self._execute(runner=_CountingRunner(fail_at=3))
        self.assertEqual(observer.calls, 0, "a window that never completed is never captured")
        self.assertEqual(report["readiness_windows"], [])
        self.assertEqual(report["phase1_completion_state"], "CONTAINS_DURABLE_FAILURE")
        self.assertTrue(report["halt_reasons"])
        self.assertFalse(report["d1_created"])

    def test_restart_after_a_spent_run_reuses_the_identical_run_identity(self) -> None:
        first, _, _ = self._execute(observer=_CountingObserver(fail_window=0))
        second, runner, observer = self._execute()
        self.assertEqual(second["source_run_id"], first["source_run_id"])
        self.assertEqual(second["run_binding_sha256"], first["run_binding_sha256"])
        self.assertEqual(second["store_paths"], first["store_paths"])
        self.assertEqual(
            second["phase1_seal_digest"],
            first["phase1_seal_digest"],
            "a sealed, spent run is never resealed under a new identity",
        )
        self.assertEqual(
            second["checkpoint_c"]["canonical_evaluation_digest"],
            first["checkpoint_c"]["canonical_evaluation_digest"],
        )
        self.assertEqual(runner.search_calls, 0, "a spent run dispatches nothing further")

    def test_a_spent_run_cannot_be_repaired_into_a_passing_result(self) -> None:
        first, _, _ = self._execute(observer=_CountingObserver(fail_window=0))
        self.assertEqual(first["checkpoint_c"]["status"], "FAILING")
        second, _, _ = self._execute()
        self.assertEqual(second["checkpoint_c"]["status"], "FAILING")
        self.assertFalse(second["checkpoint_c"]["qualified"])


# ======================================================================
# 10, 23, 52. Single readiness path and store-path collisions
# ======================================================================


class StorePathAuthorityTests(_TempCase):
    def test_an_alternate_readiness_path_is_not_reachable_for_a_run_root(self) -> None:
        plan = self.plan()
        self.assertEqual(
            plan["store_paths"]["readiness_store"], readiness_store_path(self.run_root)
        )
        alternate = str(Path(self.run_root) / "other_readiness.sqlite3")
        self.assertNotEqual(plan["store_paths"]["readiness_store"], alternate)
        self.assertNotIn(alternate, json.dumps(plan))

    def test_canonical_global_paths_are_derived_not_supplied(self) -> None:
        """ADR-022 sections 9-10: derived facts, never caller inputs."""

        plan = self.plan()
        self.assertEqual(
            plan["canonical_global_paths"]["route_state"], self.scope.route_state_path
        )
        self.assertEqual(
            plan["canonical_global_paths"]["lkg_authority_store"],
            self.scope.lkg_authority_store_path,
        )
        self.assertNotIn("governed_global_paths", plan)
        # Both live under the ONE deployment scope, so they can never name two
        # different deployments the way two independent operands could.
        for path in plan["canonical_global_paths"].values():
            self.assertTrue(path.startswith(self.scope.scope_root + "/"), path)

    def test_a_second_run_root_is_a_different_authority_entirely(self) -> None:
        first = self.plan()
        second = self.plan(source_run_id="lkg-qualification-run-0002")
        self.assertNotEqual(
            first["store_paths"]["readiness_store"], second["store_paths"]["readiness_store"]
        )
        self.assertNotEqual(
            first["prepared_authority_sha256"], second["prepared_authority_sha256"]
        )

    def test_an_unrelated_file_at_a_frozen_store_path_fails_closed(self) -> None:
        Path(self.run_root).mkdir(parents=True, mode=0o700)
        Path(phase1_ledger_path(self.run_root)).write_bytes(b"definitely not sqlite")
        operands = self.operands()
        plan = self.plan()
        dependencies, runner, observer, _ = _dependencies()
        with self.assertRaises(Exception) as caught:
            self.execute(operands, plan["prepared_authority_sha256"], dependencies)
        self.assertNotIsInstance(caught.exception, AssertionError)
        self.assertEqual(runner.search_calls, 0)


# ======================================================================
# 18, 21, 51. Zero actuation, by call graph rather than by discipline
# ======================================================================


class ZeroActuationTests(unittest.TestCase):
    def test_no_actuation_module_is_imported(self) -> None:
        import ast

        tree = ast.parse(Path("src/vdbench/lkg_qualification_operator.py").read_text())
        forbidden = {
            "canary_admission",
            "canary_approval",
            "canary_grant_store",
            "canary_activation",
            "canary_live_runner",
            "canary_rollback",
            "canary_routing",
            "milvus_actuation",
            "actuation_persistence",
            "policy",
        }
        modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                modules.add(node.module.lstrip("."))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    modules.add(alias.name)
        self.assertEqual(modules & forbidden, set(), f"actuation import present: {modules & forbidden}")

    def test_no_actuation_call_appears_in_the_operator_source(self) -> None:
        source = Path("src/vdbench/lkg_qualification_operator.py").read_text()
        for forbidden in (
            "begin_activation(",
            "clear_to_lkg(",
            "reserve(",
            "record_terminal(",
            "create_index(",
            "drop_index(",
            "release_collection(",
            ".search(",
        ):
            self.assertNotIn(forbidden, source, f"forbidden actuation seam: {forbidden}")

    def test_no_exp012_campaign_module_is_consumed(self) -> None:
        source = Path("src/vdbench/lkg_qualification_operator.py").read_text()
        for forbidden in ("exp012_scale_campaign", "exp012_scale_contract", "gate_c_checkpoint"):
            self.assertNotIn(forbidden, source)

    def test_the_only_live_client_construction_is_in_production_wiring(self) -> None:
        import ast

        tree = ast.parse(Path("src/vdbench/lkg_qualification_operator.py").read_text())
        builders = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "production_dependencies"
        ]
        self.assertEqual(len(builders), 1)
        builder_source = ast.unparse(builders[0])
        for live in ("build_readonly_milvus_client", "DockerExecutionMetadataInspector", "from_uri"):
            self.assertIn(live, builder_source)
        outside = ast.unparse(tree).replace(builder_source, "")
        for live in ("build_readonly_milvus_client", "DockerExecutionMetadataInspector"):
            self.assertNotIn(live, outside, f"{live} constructed outside production wiring")


# ======================================================================
# 36-38, 45-46. A failing verdict is propagated, never masked, and blocks D1
# ======================================================================


class FailingVerdictPropagationTests(_RealRootGuardMixin):
    """One full run whose observations violate both statistical gates.

    The recall floor and the p95 latency ceiling are Checkpoint-C's own
    semantics, exhaustively covered by
    ``tests/test_lkg_qualification_evaluation.py``. What is tested here is
    strictly the composition property those tests cannot show: that the
    operator reports the evaluator's FAILING verdict faithfully, creates
    no D1/D2 for it, and that Phase 3 refuses the run even when handed its
    own genuine, correctly reviewed Checkpoint-C digest.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        cls.directory = Path(cls._temporary.name)
        cls.governance_root = cls.directory / "vd-governance"
        cls.scope = _scope(cls.governance_root)
        operands = _load(cls.directory)
        cls.run_root = cls.scope.run_root(operands.source_run_id)
        plan = build_lkg_qualification_plan(operands, governance_scope=cls.scope)
        dependencies, cls.runner, cls.observer, _ = _dependencies(
            runner=_CountingRunner(recall=0.50, latency_ms=50.0)
        )
        cls.report = execute_lkg_qualification(
            operands,
            dependencies=dependencies,
            confirm_live_lkg_qualification_searches=True,
            expected_prepared_authority_sha256=plan["prepared_authority_sha256"],
            governance_scope=cls.scope,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def test_all_evidence_was_still_captured_and_sealed(self) -> None:
        self.assertEqual(self.runner.search_calls, _QUERY_COUNT)
        self.assertEqual(self.observer.calls, WINDOWS_PER_RUN)
        self.assertEqual(self.report["phase1_completion_state"], "ALL_POSITIONS_SUCCESSFUL")
        self.assertEqual(self.report["phase2_ingested_windows"], list(range(WINDOWS_PER_RUN)))

    def test_checkpoint_c_reports_failing_and_not_qualified(self) -> None:
        checkpoint = self.report["checkpoint_c"]
        self.assertEqual(checkpoint["status"], "FAILING")
        self.assertFalse(checkpoint["qualified"])
        self.assertEqual(len(checkpoint["canonical_evaluation_digest"]), 64)

    def test_a_failing_run_creates_no_d1_and_no_d2(self) -> None:
        self.assertFalse(self.report["d1_created"])
        self.assertFalse(self.report["d2_created"])
        self.assertFalse(Path(self.scope.lkg_authority_store_path).exists())

    def test_phase3_refuses_a_failing_run_even_with_its_own_correct_digest(self) -> None:
        directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, directory, True)
        governance_root = directory / "vd-governance"
        shutil.copytree(self.governance_root, governance_root)
        scope = _scope(governance_root)
        operands = _load(directory)
        dependencies, _, _, _ = _dependencies()
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            resolve_and_persist_phase3_authority(
                operands,
                dependencies=dependencies,
                expected_checkpoint_c_digest=self.report["checkpoint_c"][
                    "canonical_evaluation_digest"
                ],
                governance_scope=scope,
            )
        self.assertEqual(caught.exception.code, "LKG_PHASE3_AUTHORITY_REFUSED")
        self.assertFalse(Path(scope.lkg_authority_store_path).exists())



# ======================================================================
# P1 REGRESSION: one source_run_id -> exactly one governed history
# (ADR-020 section 39; ADR-021 sections 5-6)
# ======================================================================


class DuplicateGovernedHistoryRegressionTests(_TempCase):
    """The exact attack that was mechanically reproduced before the repair.

    Before: one ``source_run_id`` pointed at two ``run_root`` values produced
    two complete governed histories -- two Phase-1 lineages, two readiness
    lineages, two terminal Checkpoint-C digests, and two D2 references in one
    shared global authority store -- with identical ``run_binding_sha256``.

    After: ``run_root`` is DERIVED from the authority scope and the
    ``source_run_id``, so that operand set cannot be expressed at all.
    """

    def test_run_root_is_a_pure_function_of_scope_and_source_run_id(self) -> None:
        first = self.operands()
        second = self.operands()
        self.assertEqual(
            self.scope.run_root(first.source_run_id),
            self.scope.run_root(second.source_run_id),
        )
        run_root = self.scope.run_root(first.source_run_id)
        self.assertTrue(run_root.startswith(self.scope.scope_root))
        self.assertTrue(run_root.endswith(first.source_run_id))

    def test_one_source_run_id_yields_exactly_one_readiness_lineage(self) -> None:
        """No second readiness store is reachable for one run id in one scope."""

        roots = {self.scope.run_root(self.operands().source_run_id) for _ in range(5)}
        self.assertEqual(len(roots), 1)
        paths = {readiness_store_path(root) for root in roots}
        self.assertEqual(len(paths), 1)

    def test_a_different_source_run_id_is_a_different_history(self) -> None:
        a = self.scope.run_root(self.operands().source_run_id)
        b = self.scope.run_root(
            self.operands(source_run_id="lkg-qualification-run-0002").source_run_id
        )
        self.assertNotEqual(a, b)
        self.assertNotEqual(readiness_store_path(a), readiness_store_path(b))
        # Both still belong to the ONE deployment governance scope.
        for root in (a, b):
            self.assertTrue(root.startswith(self.scope.scope_root + "/"))

    def test_conflicting_second_history_refuses_before_any_search(self) -> None:
        """Same run id + same scope + conflicting semantics must refuse.

        Every conflicting variant now lands on the SAME derived run root, so
        the canonical Phase-1 ledger's own stored-binding check refuses -- and
        it refuses before the producer dispatches anything.
        """

        first, runner_a, _ = self._execute()
        self.assertEqual(runner_a.search_calls, _QUERY_COUNT)
        for override in (
            {"served_ef": 200},
            {"threshold_radius": 191.0},
            {"qualification_manifest_sha256": "9" * 64},
            {"execution_source_revision": "b" * 40},
            {"base_data_identity": "OTHER-BASE"},
        ):
            with self.subTest(override=override):
                operands = self.operands(**override)
                plan = self.plan(**override)
                dependencies, runner, observer, _ = _dependencies()
                with self.assertRaises(Exception) as caught:
                    self.execute(
                        operands, plan["prepared_authority_sha256"], dependencies
                    )
                self.assertNotIsInstance(caught.exception, AssertionError)
                self.assertEqual(
                    runner.search_calls, 0, "conflicting history must not dispatch"
                )
                self.assertEqual(observer.calls, 0)

    def test_environment_drift_under_the_same_run_id_refuses_before_search(self) -> None:
        self._execute()
        operands = self.operands()
        plan = self.plan()
        dependencies, runner, observer, _ = _dependencies(
            observed_environment_identity="lkg-env-identity-v1:sha256:" + "9" * 64
        )
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            self.execute(operands, plan["prepared_authority_sha256"], dependencies)
        self.assertEqual(caught.exception.code, "LKG_ENVIRONMENT_IDENTITY_MISMATCH")
        self.assertEqual(runner.search_calls, 0)

    def test_one_source_run_id_yields_one_checkpoint_c_and_one_d2(self) -> None:
        """The full original exploit, end to end, post-repair."""

        first, _, _ = self._execute()
        operands = self.operands()
        dependencies, _, _, _ = _dependencies()
        resolve_and_persist_phase3_authority(
            operands,
            dependencies=dependencies,
            expected_checkpoint_c_digest=first["checkpoint_c"][
                "canonical_evaluation_digest"
            ],
            governance_scope=self.scope,
        )
        # A second attempt under the same run id reaches the SAME history.
        second, runner, observer = self._execute()
        self.assertEqual(runner.search_calls, 0)
        self.assertEqual(observer.calls, 0)
        self.assertEqual(
            second["checkpoint_c"]["canonical_evaluation_digest"],
            first["checkpoint_c"]["canonical_evaluation_digest"],
        )
        store = LkgPhase3AuthorityReferenceStore(self.scope.lkg_authority_store_path)
        try:
            references = store.load_all()
        finally:
            store.close()
        self.assertEqual(len(references), 1, "exactly one D2 per source_run_id")
        self.assertEqual(references[0].source_run_id, operands.source_run_id)

    def _execute(self, *, runner=None, observer=None):
        operands = self.operands()
        plan = self.plan()
        dependencies, runner, observer, _ = _dependencies(runner=runner, observer=observer)
        report = self.execute(
            operands, plan["prepared_authority_sha256"], dependencies
        )
        return report, runner, observer


class RunIdConcurrencyTests(_TempCase):
    def test_concurrent_derivation_cannot_produce_two_roots(self) -> None:
        """Derivation is pure, so there is no allocation race to lose."""

        operands = self.operands()
        results: list[str] = []
        errors: list[BaseException] = []

        def derive():
            try:
                results.append(self.scope.run_root(operands.source_run_id))
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=derive) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10.0)
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 8)
        self.assertEqual(len(set(results)), 1, "one run id -> one run root, always")


# ======================================================================
# FIRST-LKG GLOBAL AUTHORITY GUARD (behavioural, no live client)
# ======================================================================


class FirstLkgGlobalAuthorityGuardTests(_TempCase):
    """Exercises the REAL production observer and the REAL global readers.

    Deliberately does NOT use the always-bootstrap-PASS fake observer that
    every other test injects: this is the coverage gap the self-review found.
    """

    def _observer(self, *, authority_path: str, route_path: str):
        return LkgProductionWindowReadinessObserver(
            spec=self.operands().environment_observation_spec,
            run_bound_environment_identity=_ENVIRONMENT_IDENTITY,
            baseline_search_configuration=self.operands().search_configuration,
            expected_baseline_search_configuration_sha256=_BASELINE_CONFIG_SHA,
            expected_serving_configuration_identity=_SERVING_IDENTITY,
            serving_configuration_identity_reader=lambda: _SERVING_IDENTITY,
            metadata_reader=_MetadataReader(),
            container_inspector=lambda name: _container(),
            image_inspector=lambda image_id: _image(),
            healthz_probe=lambda: True,
            route_state_reader=lambda: read_route_state_record(route_path),
            verified_latest_lkg_reader=lambda: verified_latest_lkg_present(
                authority_path
            ),
            clock=lambda: _NOW,
        )

    def _observe(self, observer):
        return observer.observe(
            source_run_id="lkg-qualification-run-0001",
            source_run_binding_sha256="a" * 64,
            window_index=0,
            readiness_check_id="b" * 64,
        )

    def test_absent_global_authority_is_genuine_first_lkg_bootstrap(self) -> None:
        authority = str(self.directory / "absent_authority.sqlite3")
        route = str(self.directory / "absent_route.json")
        self.assertFalse(Path(authority).exists())
        self.assertIs(verified_latest_lkg_present(authority), False)
        self.assertIsNone(read_route_state_record(route))
        health, rollback = self._observe(
            self._observer(authority_path=authority, route_path=route)
        )
        self.assertTrue(rollback.ready, rollback.reason_codes)
        self.assertNotIn("STEADY_STATE", str(rollback.reason_codes))

    def test_present_verified_latest_lkg_refuses_steady_state(self) -> None:
        """A real prior LKG must make first-LKG-only semantics refuse."""

        authority = self._authority_store_with_one_verified_latest()
        self.assertIs(verified_latest_lkg_present(authority), True)
        observer = self._observer(
            authority_path=authority,
            route_path=str(self.directory / "no_route.json"),
        )
        with self.assertRaises(LkgWindowReadinessObservationError) as caught:
            self._observe(observer)
        self.assertEqual(caught.exception.code, "STEADY_STATE_SEMANTICS_NOT_AUTHORIZED")

    def test_corrupt_global_authority_store_fails_closed(self) -> None:
        authority = str(self.directory / "corrupt_authority.sqlite3")
        Path(authority).write_bytes(b"this is definitely not a sqlite database")
        with self.assertRaises(Exception) as caught:
            verified_latest_lkg_present(authority)
        self.assertNotIsInstance(caught.exception, AssertionError)
        observer = self._observer(
            authority_path=authority,
            route_path=str(self.directory / "no_route.json"),
        )
        with self.assertRaises(LkgWindowReadinessObservationError) as caught:
            self._observe(observer)
        self.assertEqual(
            caught.exception.code, "LKG_READINESS_PHASE3_AUTHORITY_UNREADABLE"
        )

    def test_corrupt_global_route_state_fails_closed(self) -> None:
        route = str(self.directory / "corrupt_route.json")
        Path(route).write_text("{ this is not canonical json")
        observer = self._observer(
            authority_path=str(self.directory / "absent_authority.sqlite3"),
            route_path=route,
        )
        with self.assertRaises(LkgWindowReadinessObservationError) as caught:
            self._observe(observer)
        self.assertEqual(caught.exception.code, "LKG_READINESS_ROUTE_STATE_UNREADABLE")

    def _authority_store_with_one_verified_latest(self) -> str:
        """Produce a genuine D2 reference through the canonical operator path."""

        operands = self.operands()
        plan = self.plan()
        dependencies, _, _, _ = _dependencies()
        report = self.execute(
            operands, plan["prepared_authority_sha256"], dependencies
        )
        resolve_and_persist_phase3_authority(
            operands,
            dependencies=dependencies,
            expected_checkpoint_c_digest=report["checkpoint_c"][
                "canonical_evaluation_digest"
            ],
            governance_scope=self.scope,
        )
        return self.scope.lkg_authority_store_path



# ======================================================================
# ADR-022 P1-A / P1-B: canonical deployment governance scope
# ======================================================================


class P1ADeploymentAuthorityScopeTests(_TempCase):
    """P1-A: one deployment could be given two LKG authority universes.

    The reproduced attack held everything constant -- deployment, environment
    identity, serving configuration, DATASET-003, base data, index identity,
    Milvus semantics, execution revision, ``source_run_id`` -- and varied only
    ``lkg_authority_store_path``. Both sides prepared, both were separately
    authorizable, both ran 2400 searches, both reached terminal PASSING
    Checkpoint-C, both appended their own D2.

    The operand no longer exists, so that operand set cannot be written down.
    """

    def test_the_authority_store_path_is_not_an_operand(self) -> None:
        self.assertNotIn("lkg_authority_store_path", OPERAND_FIELDS)
        self.assertNotIn("route_state_path", OPERAND_FIELDS)
        self.assertNotIn("run_root", OPERAND_FIELDS)
        self.assertNotIn("deployment_identity", OPERAND_FIELDS)
        self.assertEqual(len(OPERAND_FIELDS), 32)

    def test_no_operand_variation_can_move_the_authority_store(self) -> None:
        """The exhaustive form: vary every operand, one at a time.

        The old attack needed only one operand to move the D2 store. Now no
        operand can: the store is a function of source authority alone, so the
        derived path is invariant across the entire operand space.
        """

        canonical = self.plan()["canonical_global_paths"]
        variations = {
            "source_run_id": "lkg-qualification-run-0002",
            "served_ef": 200,
            "threshold_radius": 191.0,
            "environment_identity": "lkg-env-identity-v1:sha256:" + "7" * 64,
            "execution_source_revision": "b" * 40,
            "base_data_identity": "OTHER-BASE",
            "index_identity": "other-index",
            "milvus_uri": "http://10.0.0.5:19531",
            "hnsw_collection_name": "other_hnsw",
            "database_name": "other",
            "producer_identity": "someone.else",
            "serving_configuration_identity": "exp010-serving-config-v1:sha256:" + "7" * 64,
            "qualification_manifest_sha256": "9" * 64,
            "dataset003_dir": "/srv/elsewhere/dataset003",
            "milvus_container": "other-milvus",
        }
        for field, value in variations.items():
            with self.subTest(field=field):
                self.assertEqual(
                    self.plan(**{field: value})["canonical_global_paths"], canonical
                )

    def test_one_deployment_yields_exactly_one_d2_store_for_any_run(self) -> None:
        stores = {
            self.plan(source_run_id=run_id)["canonical_global_paths"][
                "lkg_authority_store"
            ]
            for run_id in ("run-a", "run-b", "run-c", _SOURCE_RUN_ID)
        }
        self.assertEqual(stores, {self.scope.lkg_authority_store_path})

    def test_the_authority_store_lives_inside_the_deployment_scope(self) -> None:
        plan = self.plan()
        for path in plan["canonical_global_paths"].values():
            self.assertTrue(path.startswith(self.scope.scope_root + os.sep), path)
        self.assertTrue(
            plan["run_root"].startswith(self.scope.scope_root + os.sep)
        )

    def test_the_plan_binds_the_deployment_as_a_derived_source_fact(self) -> None:
        plan = self.plan()
        self.assertEqual(
            plan["deployment_identity"], CANONICAL_ENV001_DEPLOYMENT_IDENTITY
        )
        self.assertEqual(plan["deployment_namespace_digest"], self.scope.namespace_digest)
        self.assertEqual(
            plan["deployment_governance_scope_root"], self.scope.scope_root
        )
        self.assertEqual(plan["deployment_governance_root"], self.scope.canonical_root)
        self.assertEqual(
            plan["schema_version"], "lkg-qualification-prepared-authority-v2"
        )

    def test_the_operator_source_names_no_caller_supplied_global_path(self) -> None:
        source = Path("src/vdbench/lkg_qualification_operator.py").read_text()
        for forbidden in (
            "operands.route_state_path",
            "operands.lkg_authority_store_path",
            "operands.run_root",
            "lkg_authority_scope_root",
            "governed_global_paths",
        ):
            self.assertNotIn(forbidden, source, forbidden)
        # The production readers close over the derived scope, not an operand.
        self.assertIn("read_route_state_record(scope.route_state_path)", source)
        self.assertIn(
            "verified_latest_lkg_present(scope.lkg_authority_store_path)", source
        )


class P1BRouteStateAuthorityTests(_TempCase):
    """P1-B: a decoy route-state file made an ACTIVATING deployment look clean.

    Drives the REAL production observer through production-equivalent wiring:
    the route-state and verified-latest readers are exactly the closures
    ``production_dependencies`` builds, over the derived scope paths.
    """

    def _binding(self):
        from vdbench.canary_route_state import RouteStateBinding

        return RouteStateBinding(
            metric=Metric.L2,
            threshold_stratum="target-075",
            last_known_good_ef=400,
            configuration_identity=_SERVING_IDENTITY,
            data_identity="DATASET-001-v1:sha256:" + "b" * 64,
            flat_binding_id="flat-1",
            hnsw_binding_id="hnsw-1",
        )

    def setUp(self) -> None:
        super().setUp()
        # Exactly what ``execute`` does before its first dispatch. The canonical
        # marker's parent IS the deployment scope, and ``FileCanaryRouteStateStore``
        # independently requires a private parent directory -- so the 0700 scope
        # is what makes the route store's own hardening satisfiable at all.
        ensure_deployment_scope_directory(self.scope)

    def test_the_canonical_scope_satisfies_the_route_stores_own_hardening(self) -> None:
        mode = stat.S_IMODE(os.lstat(self.scope.scope_root).st_mode)
        self.assertEqual(mode & 0o077, 0, oct(mode))
        self.assertEqual(
            Path(self.scope.route_state_path).parent, Path(self.scope.scope_root)
        )
        self.assertIsNone(read_route_state_record(self.scope.route_state_path))

    def _activate_canonical_route(self) -> None:
        from vdbench.canary_route_state import FileCanaryRouteStateStore

        FileCanaryRouteStateStore(self.scope.route_state_path).begin_activation(
            binding=self._binding(),
            grant_id="grant-1",
            plan_sha256="f" * 64,
            changed_at_utc=_NOW,
        )

    def _production_observer(self):
        """Exactly production's wiring, minus the live Milvus/Docker clients."""

        return LkgProductionWindowReadinessObserver(
            spec=self.operands().environment_observation_spec,
            run_bound_environment_identity=_ENVIRONMENT_IDENTITY,
            baseline_search_configuration=self.operands().search_configuration,
            expected_baseline_search_configuration_sha256=_BASELINE_CONFIG_SHA,
            expected_serving_configuration_identity=_SERVING_IDENTITY,
            serving_configuration_identity_reader=lambda: _SERVING_IDENTITY,
            metadata_reader=_MetadataReader(),
            container_inspector=lambda name: _container(),
            image_inspector=lambda image_id: _image(),
            healthz_probe=lambda: True,
            route_state_reader=lambda: read_route_state_record(
                self.scope.route_state_path
            ),
            verified_latest_lkg_reader=lambda: verified_latest_lkg_present(
                self.scope.lkg_authority_store_path
            ),
            clock=lambda: _NOW,
        )

    def _observe(self):
        return self._production_observer().observe(
            source_run_id=_SOURCE_RUN_ID,
            source_run_binding_sha256="a" * 64,
            window_index=0,
            readiness_check_id="b" * 64,
        )

    def test_canonical_activating_route_state_fails_readiness(self) -> None:
        self._activate_canonical_route()
        _, rollback = self._observe()
        self.assertFalse(rollback.ready)
        self.assertIn("CANDIDATE_ROUTE_ACTIVE", rollback.reason_codes)

    def test_a_decoy_route_state_file_is_unreachable_from_production(self) -> None:
        """The decoy exists and is clean; nothing can point the observer at it."""

        self._activate_canonical_route()
        decoy = self.directory / "decoy_route_state.json"
        self.assertFalse(decoy.exists())
        self.assertIsNone(read_route_state_record(str(decoy)))

        # No operand names it.
        for field in ("route_state_path", "state_root", "governance_root"):
            self.assertNotIn(field, OPERAND_FIELDS)
        values = _operand_values()
        values["route_state_path"] = str(decoy)
        path = self.directory / "decoy_operands.json"
        path.write_text(json.dumps(values))
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            load_operands(path)
        self.assertEqual(caught.exception.code, "LKG_OPERANDS_UNEXPECTED")

        # No CLI flag names it.
        import vdbench.lkg_qualification_operator as module

        options = {
            option
            for action in module._parser()._actions
            for option in action.option_strings
        }
        self.assertNotIn("--route-state-path", options)

        # And the canonical marker still governs.
        _, rollback = self._observe()
        self.assertFalse(rollback.ready)
        self.assertIn("CANDIDATE_ROUTE_ACTIVE", rollback.reason_codes)

    def test_absent_canonical_route_state_is_genuine_bootstrap(self) -> None:
        _, rollback = self._observe()
        self.assertTrue(rollback.ready, rollback.reason_codes)
        self.assertEqual(rollback.reason_codes, ())

    def test_route_state_and_d2_stay_live_and_are_not_frozen_into_the_authority(
        self,
    ) -> None:
        """ADR-022 section 18: the authority binds WHERE, never WHAT.

        A live transition must not change the digest a human authorized --
        otherwise every legitimate state change would demand re-authorization
        and live fail-closed revalidation would degrade into a stale snapshot
        comparison.
        """

        before = self.plan()["prepared_authority_sha256"]
        self._activate_canonical_route()
        after = self.plan()["prepared_authority_sha256"]
        self.assertEqual(before, after)
        rendered = json.dumps(self.plan())
        for content in ("ACTIVATING", "grant-1", "ACTIVATION_PENDING", "f" * 64):
            self.assertNotIn(content, rendered, content)


class DeploymentScopeExecutionTests(_TempCase):
    """The scope is created only by execute, and only when it is safe."""

    def test_preflight_and_prepare_create_no_deployment_scope(self) -> None:
        report = run_preflight(self.operands(), governance_scope=self.scope)
        self.plan()
        self.assertFalse(report["deployment_scope_root_exists"])
        self.assertFalse(report["canonical_route_state_exists"])
        self.assertFalse(report["canonical_lkg_authority_store_exists"])
        self.assertFalse(Path(self.scope.canonical_root).exists())
        self.assertFalse(Path(self.scope.scope_root).exists())

    def test_execute_creates_a_private_deployment_scope(self) -> None:
        operands = self.operands()
        plan = self.plan()
        dependencies, runner, _, _ = _dependencies()
        self.execute(operands, plan["prepared_authority_sha256"], dependencies)
        self.assertEqual(runner.search_calls, _QUERY_COUNT)
        mode = stat.S_IMODE(os.lstat(self.scope.scope_root).st_mode)
        self.assertEqual(mode & 0o077, 0, oct(mode))
        self.assertTrue(Path(self.run_root).is_dir())

    def test_an_unsafe_existing_deployment_scope_refuses_before_search(self) -> None:
        Path(self.scope.scope_root).mkdir(parents=True, mode=0o700)
        os.chmod(self.scope.scope_root, 0o755)
        operands = self.operands()
        plan = self.plan()
        dependencies, runner, observer, _ = _dependencies()
        with self.assertRaises(DeploymentGovernanceError) as caught:
            self.execute(operands, plan["prepared_authority_sha256"], dependencies)
        self.assertEqual(caught.exception.code, "DEPLOYMENT_SCOPE_NOT_PRIVATE")
        self.assertEqual(runner.search_calls, 0)
        self.assertEqual(observer.calls, 0)
        self.assertFalse(Path(self.run_root).exists())


# ======================================================================
# ADR-023. Programmatic governance_scope injection cannot become authority
# ======================================================================


class ProgrammaticScopeInjectionRegressionTests(_RealRootGuardMixin, _FullRunMixin):
    """Direct regression coverage for the in-process ``governance_scope`` seam.

    Independent review attacked this seam by hand and found it safe, but the
    suite itself only ever attacked the OPERAND and CLI surfaces -- so nothing
    would have caught a regression here. These are the two attacks that matter,
    expressed against the seam itself rather than against the operand set.

    The seam is deliberately NOT redesigned (ADR-022 section 15, ADR-023): what
    is asserted is that a caller who *already* holds an arbitrary scope object
    still cannot convert one deployment's human authorization into a second
    authority universe.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        cls.directory = Path(cls._temporary.name)
        # Scope A is the deployment that is actually prepared, authorized and
        # executed to a terminal Checkpoint C. Scope B never legitimately
        # participates in anything.
        cls._run_full(cls.directory)
        cls.scope_a = cls.full_scope
        cls.scope_b = _scope(cls.directory / "vd-governance-b")

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def test_the_two_scopes_are_genuinely_distinct_universes(self) -> None:
        self.assertNotEqual(self.scope_a.scope_root, self.scope_b.scope_root)
        self.assertNotEqual(
            self.scope_a.lkg_authority_store_path,
            self.scope_b.lkg_authority_store_path,
        )
        # Same deployment identity and same namespace digest on both sides:
        # the ONLY thing that differs is the injected root, which is exactly
        # the shape of the original P1-A attack.
        self.assertEqual(
            self.scope_a.deployment_identity, self.scope_b.deployment_identity
        )
        self.assertEqual(
            self.scope_a.namespace_digest, self.scope_b.namespace_digest
        )

    def test_scope_a_authority_cannot_execute_under_scope_b(self) -> None:
        """TEST A: prepare under A, execute under B presenting A's approved digest."""

        operands = _load(self.directory)
        approved = build_lkg_qualification_plan(
            operands, governance_scope=self.scope_a
        )["prepared_authority_sha256"]
        dependencies, runner, observer, recorder = _dependencies()

        with self.assertRaises(LkgQualificationOperatorError) as caught:
            execute_lkg_qualification(
                operands,
                dependencies=dependencies,
                confirm_live_lkg_qualification_searches=True,
                expected_prepared_authority_sha256=approved,
                governance_scope=self.scope_b,
            )
        self.assertEqual(caught.exception.code, "LKG_PREPARED_AUTHORITY_MISMATCH")

        # Refused BEFORE every boundary it protects.
        self.assertEqual(runner.search_calls, 0)
        self.assertEqual(observer.calls, 0)
        self.assertEqual(recorder.workload_loads, 0)
        self.assertEqual(recorder.runner_builds, 0)
        self.assertEqual(recorder.observer_builds, 0)
        self.assertEqual(recorder.source_verifications, [])

        # And no governed history of any kind exists under scope B.
        self.assertFalse(Path(self.scope_b.scope_root).exists())
        self.assertFalse(Path(self.scope_b.runs_root).exists())
        for path in (
            phase1_ledger_path(self.scope_b.run_root(_SOURCE_RUN_ID)),
            readiness_store_path(self.scope_b.run_root(_SOURCE_RUN_ID)),
            phase2_readiness_ledger_path(self.scope_b.run_root(_SOURCE_RUN_ID)),
            checkpoint_c_ledger_path(self.scope_b.run_root(_SOURCE_RUN_ID)),
        ):
            self.assertFalse(Path(path).exists(), path)

    def test_scope_b_digest_cannot_execute_under_scope_a_either(self) -> None:
        """The mismatch is symmetric: neither side's authority crosses over."""

        operands = _load(self.directory)
        foreign = build_lkg_qualification_plan(
            operands, governance_scope=self.scope_b
        )["prepared_authority_sha256"]
        dependencies, runner, _, recorder = _dependencies()
        with self.assertRaises(LkgQualificationOperatorError) as caught:
            execute_lkg_qualification(
                operands,
                dependencies=dependencies,
                confirm_live_lkg_qualification_searches=True,
                expected_prepared_authority_sha256=foreign,
                governance_scope=self.scope_a,
            )
        self.assertEqual(caught.exception.code, "LKG_PREPARED_AUTHORITY_MISMATCH")
        self.assertEqual(runner.search_calls, 0)
        self.assertEqual(recorder.workload_loads, 0)

    def test_phase3_cannot_switch_scope_under_reviewed_checkpoint_c_authority(
        self,
    ) -> None:
        """TEST B: a terminal Checkpoint C under A cannot append D2 under B.

        This is the exact P1-A payload in its last remaining spelling: a real,
        legitimately reviewed Checkpoint-C digest, presented to Phase 3 with a
        different governance scope, trying to mint a second verified-latest LKG
        authority for one deployment.
        """

        reviewed = self.full_report["checkpoint_c"]["canonical_evaluation_digest"]
        self.assertTrue(self.full_report["checkpoint_c"]["qualified"])
        operands = _load(self.directory)
        dependencies, runner, _, _ = _dependencies()

        with self.assertRaises(LkgQualificationOperatorError) as caught:
            resolve_and_persist_phase3_authority(
                operands,
                dependencies=dependencies,
                expected_checkpoint_c_digest=reviewed,
                governance_scope=self.scope_b,
            )
        self.assertEqual(caught.exception.code, "LKG_PHASE3_SOURCE_STORE_MISSING")
        self.assertEqual(runner.search_calls, 0)

        # No D2, no verified-latest universe, and no scope at all under B.
        self.assertFalse(Path(self.scope_b.lkg_authority_store_path).exists())
        self.assertIs(
            verified_latest_lkg_present(self.scope_b.lkg_authority_store_path), False
        )
        self.assertFalse(Path(self.scope_b.scope_root).exists())

    def test_exactly_one_authority_store_exists_after_every_attack(self) -> None:
        """One deployment, one D2 location -- swept from the filesystem itself."""

        legitimate = resolve_and_persist_phase3_authority(
            _load(self.directory),
            dependencies=_dependencies()[0],
            expected_checkpoint_c_digest=self.full_report["checkpoint_c"][
                "canonical_evaluation_digest"
            ],
            governance_scope=self.scope_a,
        )
        self.assertTrue(legitimate["d2_appended"])
        stores = sorted(
            str(path) for path in self.directory.rglob(LKG_AUTHORITY_STORE_FILENAME)
        )
        self.assertEqual(len(stores), 1, stores)
        self.assertEqual(
            os.path.realpath(stores[0]),
            os.path.realpath(self.scope_a.lkg_authority_store_path),
        )

    def test_a_non_scope_object_is_refused_at_every_injection_point(self) -> None:
        """The seam accepts a DeploymentGovernanceScope or nothing at all."""

        operands = _load(self.directory)
        dependencies, runner, _, recorder = _dependencies()
        for label, call in (
            (
                "build_lkg_qualification_plan",
                lambda scope: build_lkg_qualification_plan(
                    operands, governance_scope=scope
                ),
            ),
            ("run_preflight", lambda scope: run_preflight(operands, governance_scope=scope)),
            (
                "execute_lkg_qualification",
                lambda scope: execute_lkg_qualification(
                    operands,
                    dependencies=dependencies,
                    confirm_live_lkg_qualification_searches=True,
                    expected_prepared_authority_sha256="a" * 64,
                    governance_scope=scope,
                ),
            ),
            (
                "resolve_and_persist_phase3_authority",
                lambda scope: resolve_and_persist_phase3_authority(
                    operands,
                    dependencies=dependencies,
                    expected_checkpoint_c_digest="a" * 64,
                    governance_scope=scope,
                ),
            ),
            (
                "production_dependencies",
                lambda scope: production_dependencies(operands, governance_scope=scope),
            ),
        ):
            for hostile in (
                str(self.scope_b.scope_root),
                {"scope_root": str(self.scope_b.scope_root)},
                42,
                object(),
            ):
                with self.subTest(entry_point=label, hostile=type(hostile).__name__):
                    with self.assertRaises(LkgQualificationOperatorError) as caught:
                        call(hostile)
                    self.assertEqual(
                        caught.exception.code, "LKG_GOVERNANCE_SCOPE_INVALID"
                    )
        self.assertEqual(runner.search_calls, 0)
        self.assertEqual(recorder.workload_loads, 0)


class MetadataOnlyMilvusReaderTests(unittest.TestCase):
    """ADR-020 s42 / ADR-023 s23-24: the readiness reader is metadata-only BY TYPE.

    The production wiring builds a real ``pymilvus.MilvusClient``, which is
    read-only by USE but still exposes ``search``. These tests pin the property
    that actually closes the gap: the object the observer receives structurally
    cannot search, whatever the wrapped client offers.
    """

    class _SearchCapableClient:
        """Stands in for ``pymilvus.MilvusClient``: metadata AND search."""

        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []
            self.search_calls = 0

        def describe_collection(self, *, collection_name):
            self.calls.append(("describe_collection", (collection_name,)))
            return {"collection_name": collection_name}

        def describe_index(self, *, collection_name, index_name):
            self.calls.append(("describe_index", (collection_name, index_name)))
            return {"collection_name": collection_name, "index_name": index_name}

        def get_collection_stats(self, *, collection_name):
            self.calls.append(("get_collection_stats", (collection_name,)))
            return {"row_count": 10000}

        def get_load_state(self, *, collection_name):
            self.calls.append(("get_load_state", (collection_name,)))
            return {"state": "Loaded"}

        def search(self, *args, **kwargs):  # pragma: no cover - must never run
            self.search_calls += 1
            raise AssertionError("readiness path issued a vector search")

        def insert(self, *args, **kwargs):  # pragma: no cover - must never run
            raise AssertionError("readiness path issued a mutation")

        def drop_collection(self, *args, **kwargs):  # pragma: no cover
            raise AssertionError("readiness path issued a mutation")

    def setUp(self) -> None:
        self.client = self._SearchCapableClient()
        self.reader = MetadataOnlyMilvusReader(self.client)

    def test_forwards_exactly_the_four_metadata_methods(self):
        self.assertEqual(
            self.reader.describe_collection(collection_name="vd_hnsw"),
            {"collection_name": "vd_hnsw"},
        )
        self.assertEqual(
            self.reader.describe_index(
                collection_name="vd_hnsw", index_name="vector_index"
            ),
            {"collection_name": "vd_hnsw", "index_name": "vector_index"},
        )
        self.assertEqual(
            self.reader.get_collection_stats(collection_name="vd_hnsw"),
            {"row_count": 10000},
        )
        self.assertEqual(
            self.reader.get_load_state(collection_name="vd_hnsw"), {"state": "Loaded"}
        )
        self.assertEqual(
            [name for name, _ in self.client.calls],
            [
                "describe_collection",
                "describe_index",
                "get_collection_stats",
                "get_load_state",
            ],
        )
        self.assertEqual(self.client.search_calls, 0)

    def test_search_and_mutation_surface_do_not_exist_on_the_reader(self):
        for forbidden in (
            "search",
            "insert",
            "upsert",
            "delete",
            "drop_collection",
            "create_index",
            "drop_index",
            "load_collection",
            "release_collection",
            "query",
            "hybrid_search",
        ):
            with self.subTest(method=forbidden):
                self.assertFalse(hasattr(self.reader, forbidden))
                with self.assertRaises(AttributeError):
                    getattr(self.reader, forbidden)
        self.assertEqual(self.client.search_calls, 0)

    def test_reader_admits_no_attribute_injection(self):
        with self.assertRaises(AttributeError):
            self.reader.search = lambda *a, **k: None  # type: ignore[attr-defined]
        self.assertFalse(hasattr(self.reader, "search"))
        self.assertEqual(MetadataOnlyMilvusReader.__slots__, ("_client",))
        self.assertFalse(hasattr(MetadataOnlyMilvusReader, "__getattr__"))

    def test_public_surface_is_exactly_the_protocol(self):
        public = sorted(
            name
            for name in dir(self.reader)
            if not name.startswith("_")
        )
        self.assertEqual(
            public,
            [
                "describe_collection",
                "describe_index",
                "get_collection_stats",
                "get_load_state",
            ],
        )

    def test_production_observer_completes_through_the_wrapper(self):
        """The wrapper is sufficient for the entire production observe() path.

        Proves the type restriction is not merely safe but complete: the real
        ``LkgProductionWindowReadinessObserver`` reaches a first-LKG bootstrap
        PASS while its only Milvus surface is the metadata-only wrapper, and
        the wrapped search-capable client is never asked to search.
        """

        with tempfile.TemporaryDirectory() as directory:
            operands = _load(Path(directory))
            wrapped = self._SearchCapableClient()
            observer = LkgProductionWindowReadinessObserver(
                spec=operands.environment_observation_spec,
                run_bound_environment_identity=_ENVIRONMENT_IDENTITY,
                baseline_search_configuration=operands.search_configuration,
                expected_baseline_search_configuration_sha256=_BASELINE_CONFIG_SHA,
                expected_serving_configuration_identity=_SERVING_IDENTITY,
                serving_configuration_identity_reader=lambda: _SERVING_IDENTITY,
                metadata_reader=MetadataOnlyMilvusReader(_MetadataReader()),
                container_inspector=lambda name: _container(),
                image_inspector=lambda image_id: _image(),
                healthz_probe=lambda: True,
                route_state_reader=lambda: read_route_state_record(
                    str(Path(directory) / "absent_route.json")
                ),
                verified_latest_lkg_reader=lambda: verified_latest_lkg_present(
                    str(Path(directory) / "absent_authority.sqlite3")
                ),
                clock=lambda: _NOW,
            )
            health, rollback = observer.observe(
                source_run_id="lkg-qualification-run-0001",
                source_run_binding_sha256="a" * 64,
                window_index=0,
                readiness_check_id="b" * 64,
            )
        self.assertTrue(rollback.ready, rollback.reason_codes)
        self.assertIn("observed_environment_identity", health.document)
        self.assertEqual(wrapped.search_calls, 0)



if __name__ == "__main__":
    unittest.main()
