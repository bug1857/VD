"""ADR-020 readiness observation payload tests.

Every observation here runs against in-memory fakes. No Milvus client,
no Docker socket, no vector search, no ef/route/grant/canary/rollback
actuation, and no LKG/Phase-1/Phase-2/Checkpoint-C/D1/D2 state.
"""

from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import replace
import unittest

from vdbench.canary_route_state import RouteState, RouteStateBinding, RouteStateRecord
from vdbench.config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from vdbench.artifacts import canonical_json_bytes
from vdbench.lkg_window_readiness_observation import (
    LKG_COLLECTION_SCHEMA_DOMAIN,
    LKG_COLLECTION_SCHEMA_SCHEMA_VERSION,
    LKG_ENVIRONMENT_IDENTITY_DOMAIN,
    LKG_ENVIRONMENT_IDENTITY_PREFIX,
    LKG_INDEX_IDENTITY_DOMAIN,
    LKG_INDEX_IDENTITY_SCHEMA_VERSION,
    LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY,
    LKG_ROLLBACK_READINESS_SOURCE_IDENTITY,
    LKG_ROLLBACK_VERIFICATION_MODE,
    READINESS_REASON_CODES,
    LkgEnvironmentObservationSpec,
    LkgWindowReadinessObservationError,
    derive_lkg_window_provider_run_id,
    derive_lkg_window_readiness_check_id,
    lkg_collection_schema_document,
    lkg_collection_schema_sha256,
    lkg_environment_identity,
    lkg_index_identity_document,
    lkg_index_identity_sha256,
    observe_lkg_window_health,
    verify_lkg_window_rollback_readiness,
    validate_lkg_window_health_observation,
    validate_lkg_window_rollback_readiness,
)

_RUN_ID = "lkg-run-1"
_BINDING_SHA = "a" * 64
_NOW = "2026-08-30T00:00:00.000000Z"

_SPEC = LkgEnvironmentObservationSpec(
    milvus_uri="http://127.0.0.1:19530",
    database_name="default",
    etcd_container="milvus-etcd",
    minio_container="milvus-minio",
    milvus_container="milvus-standalone",
    flat_collection_name="vd_flat",
    hnsw_collection_name="vd_hnsw",
    index_name="vector_index",
    metric="L2",
    dimensions=128,
    expected_entity_count=10000,
)

_BASELINE = SearchConfiguration(
    metric=Metric.L2, threshold_label="target-075", radius=191.85897352125554,
    index_track=IndexTrack.HNSW, ef=400, limit=100, consistency_level="Strong",
)
_BASELINE_SHA = "772fbd5746d27a5d04719a1b644fdba98843635efd5610e5a0e80a16889a43ee"
_SERVING_IDENTITY = "exp010-serving-config-v1:sha256:" + "8" * 64


def _container(*, status="running", oom=False, health="healthy", restart=0):
    state = {"Status": status, "OOMKilled": oom, "StartedAt": "2026-08-26T03:51:13Z"}
    if health is not None:
        state["Health"] = {"Status": health}
    return {"Id": "c" * 64, "Image": "sha256:" + "d" * 64, "RestartCount": restart, "State": state}


def _image():
    return {"RepoDigests": ["repo@sha256:" + "e" * 64]}


class _Reader:
    """Metadata-only fake. Deliberately has no search method."""

    def __init__(self, *, loaded=True, index_state="Finished", pending=0, rows=10000):
        self.calls: list[str] = []
        self._loaded = loaded
        self._index_state = index_state
        self._pending = pending
        self._rows = rows

    def describe_collection(self, *, collection_name):
        self.calls.append(f"describe_collection:{collection_name}")
        return {
            "collection_name": collection_name,
            "fields": [
                {"name": "id", "data_type": "5", "is_primary": True, "params": {}},
                {"name": "vector", "data_type": "101", "is_primary": False,
                 "params": {"dim": 128}},
            ],
        }

    def describe_index(self, *, collection_name, index_name):
        self.calls.append(f"describe_index:{collection_name}")
        return {
            "index_name": index_name,
            "index_type": "HNSW" if "hnsw" in collection_name else "FLAT",
            "metric_type": "L2",
            "state": self._index_state,
            "pending_index_rows": self._pending,
            "indexed_rows": self._rows,
            "M": "16",
            "efConstruction": "200",
        }

    def get_collection_stats(self, *, collection_name):
        self.calls.append(f"get_collection_stats:{collection_name}")
        return {"row_count": self._rows}

    def get_load_state(self, *, collection_name):
        self.calls.append(f"get_load_state:{collection_name}")
        return {"state": "Loaded" if self._loaded else "NotLoad"}


def _observe(reader=None, *, identity=None, healthz=True, container=None, container_inspector=None,
             source_run_id=_RUN_ID, source_run_binding_sha256=_BINDING_SHA):
    reader = reader or _Reader()
    inspect_container = container_inspector or (lambda name: container() if container else _container())
    probe_identity = identity
    if probe_identity is None:
        # First derive the identity this exact fake environment produces.
        first = observe_lkg_window_health(
            spec=_SPEC,
            run_bound_environment_identity=f"{LKG_ENVIRONMENT_IDENTITY_PREFIX}:sha256:{'0' * 64}",
            source_run_id=_RUN_ID,
            source_run_binding_sha256=_BINDING_SHA,
            metadata_reader=_Reader(),
            container_inspector=inspect_container,
            image_inspector=lambda image_id: _image(),
            healthz_probe=lambda: True,
            observed_at_utc=_NOW,
        )
        probe_identity = first.document["observed_environment_identity"]
    return observe_lkg_window_health(
        spec=_SPEC,
        run_bound_environment_identity=probe_identity,
        source_run_id=source_run_id,
        source_run_binding_sha256=source_run_binding_sha256,
        metadata_reader=reader,
        container_inspector=inspect_container,
        image_inspector=lambda image_id: _image(),
        healthz_probe=lambda: healthz,
        observed_at_utc=_NOW,
    )


class DeterministicIdentifierTests(unittest.TestCase):
    def test_readiness_check_id_is_deterministic_and_window_scoped(self) -> None:
        a = derive_lkg_window_readiness_check_id(
            source_run_id=_RUN_ID, source_run_binding_sha256=_BINDING_SHA, window_index=0
        )
        b = derive_lkg_window_readiness_check_id(
            source_run_id=_RUN_ID, source_run_binding_sha256=_BINDING_SHA, window_index=0
        )
        c = derive_lkg_window_readiness_check_id(
            source_run_id=_RUN_ID, source_run_binding_sha256=_BINDING_SHA, window_index=1
        )
        d = derive_lkg_window_readiness_check_id(
            source_run_id="other", source_run_binding_sha256=_BINDING_SHA, window_index=0
        )
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertNotEqual(a, d)
        self.assertRegex(a, r"\A[0-9a-f]{64}\Z")

    def test_readiness_check_id_rejects_out_of_range_window(self) -> None:
        with self.assertRaises(ContractViolation):
            derive_lkg_window_readiness_check_id(
                source_run_id=_RUN_ID, source_run_binding_sha256=_BINDING_SHA, window_index=12
            )

    def test_provider_run_id_is_per_logical_check_and_restart_stable(self) -> None:
        check0 = derive_lkg_window_readiness_check_id(
            source_run_id=_RUN_ID, source_run_binding_sha256=_BINDING_SHA, window_index=0
        )
        check1 = derive_lkg_window_readiness_check_id(
            source_run_id=_RUN_ID, source_run_binding_sha256=_BINDING_SHA, window_index=1
        )
        kwargs = {"source_run_id": _RUN_ID, "source_run_binding_sha256": _BINDING_SHA}
        first = derive_lkg_window_provider_run_id(readiness_check_id=check0, **kwargs)
        again = derive_lkg_window_provider_run_id(readiness_check_id=check0, **kwargs)
        other = derive_lkg_window_provider_run_id(readiness_check_id=check1, **kwargs)
        self.assertEqual(first, again, "retry/restart must reproduce provenance")
        self.assertNotEqual(first, other, "distinct windows need distinct provenance")
        self.assertNotEqual(first, check0, "must differ from readiness_check_id")


class HealthObservationTests(unittest.TestCase):
    def test_clean_environment_passes_with_no_reason_codes(self) -> None:
        result = _observe()
        self.assertTrue(result.passed)
        self.assertEqual(result.reason_codes, ())
        self.assertTrue(result.document["environment_identity_matches"])
        self.assertRegex(result.digest, r"\A[0-9a-f]{64}\Z")

    def test_identity_mismatch_is_observed_failure_not_exception(self) -> None:
        wrong = f"{LKG_ENVIRONMENT_IDENTITY_PREFIX}:sha256:{'1' * 64}"
        result = _observe(identity=wrong)
        self.assertFalse(result.passed)
        self.assertIn("ENVIRONMENT_IDENTITY_MISMATCH", result.reason_codes)
        self.assertFalse(result.document["environment_identity_matches"])
        # Still real evidence with a real digest.
        self.assertRegex(result.digest, r"\A[0-9a-f]{64}\Z")

    def test_window_zero_never_becomes_the_authority(self) -> None:
        # A changed environment must FAIL against run-bound authority
        # rather than silently re-baselining.
        baseline = _observe()
        changed = _observe(
            identity=baseline.document["run_bound_environment_identity"],
            container=lambda: _container(restart=1),
        )
        self.assertFalse(changed.passed)
        self.assertIn("ENVIRONMENT_IDENTITY_MISMATCH", changed.reason_codes)

    def test_unhealthy_container_fails(self) -> None:
        result = _observe(container=lambda: _container(health="unhealthy"))
        self.assertFalse(result.passed)
        self.assertIn("CONTAINER_UNHEALTHY", result.reason_codes)

    def test_oom_killed_container_fails(self) -> None:
        result = _observe(container=lambda: _container(oom=True))
        self.assertFalse(result.passed)
        self.assertIn("CONTAINER_OOM_KILLED", result.reason_codes)

    def test_stopped_container_fails(self) -> None:
        result = _observe(container=lambda: _container(status="exited"))
        self.assertFalse(result.passed)
        self.assertIn("CONTAINER_NOT_RUNNING", result.reason_codes)

    def test_healthz_failure_fails(self) -> None:
        result = _observe(healthz=False)
        self.assertFalse(result.passed)
        self.assertIn("MILVUS_HEALTHZ_FAILED", result.reason_codes)

    def test_collection_not_loaded_fails(self) -> None:
        result = _observe(_Reader(loaded=False))
        self.assertFalse(result.passed)
        self.assertIn("COLLECTION_NOT_LOADED", result.reason_codes)

    def test_index_not_ready_fails(self) -> None:
        result = _observe(_Reader(index_state="InProgress", pending=5))
        self.assertFalse(result.passed)
        self.assertIn("INDEX_NOT_READY", result.reason_codes)

    def test_entity_count_mismatch_fails(self) -> None:
        result = _observe(_Reader(rows=9999))
        self.assertFalse(result.passed)
        self.assertIn("ENTITY_COUNT_MISMATCH", result.reason_codes)

    def test_unavailable_transport_is_provider_inability_not_failure(self) -> None:
        def broken(_name):
            raise OSError("docker socket unavailable")

        with self.assertRaises(LkgWindowReadinessObservationError) as caught:
            observe_lkg_window_health(
                spec=_SPEC,
                run_bound_environment_identity=f"{LKG_ENVIRONMENT_IDENTITY_PREFIX}:sha256:{'0' * 64}",
                source_run_id=_RUN_ID,
                source_run_binding_sha256=_BINDING_SHA,
                metadata_reader=_Reader(),
                container_inspector=broken,
                image_inspector=lambda image_id: _image(),
                healthz_probe=lambda: True,
                observed_at_utc=_NOW,
            )
        self.assertEqual(caught.exception.code, "LKG_READINESS_CONTAINER_UNAVAILABLE")

    def test_reader_exposes_no_search_method(self) -> None:
        self.assertFalse(hasattr(_Reader(), "search"))

    def test_stable_document_excludes_transient_predicates(self) -> None:
        stable = _observe().document["observed_stable_environment_document"]
        for transient in (
            "container_health", "milvus_healthz", "collection_readiness",
            "index_readiness", "observed_at_utc",
        ):
            self.assertNotIn(transient, stable)

    def test_identity_format(self) -> None:
        identity = lkg_environment_identity({"schema_version": "x"})
        self.assertRegex(
            identity, rf"\A{LKG_ENVIRONMENT_IDENTITY_PREFIX}:sha256:[0-9a-f]{{64}}\Z"
        )

    def test_source_identity_constant(self) -> None:
        self.assertEqual(
            LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY,
            "vdbench.lkg-window-health-observation.v1",
        )


def _route_record(state: RouteState) -> RouteStateRecord:
    return RouteStateRecord(
        state=state,
        binding=RouteStateBinding(
            metric=Metric.L2,
            threshold_stratum="target-075",
            last_known_good_ef=400,
            configuration_identity=_SERVING_IDENTITY,
            data_identity="DATASET-001-v1:sha256:" + "9" * 64,
            flat_binding_id="f" * 64,
            hnsw_binding_id="a" * 64,
        ),
        grant_id=None if state is RouteState.LKG_ONLY else "grant-1",
        plan_sha256=None if state is RouteState.LKG_ONLY else "b" * 64,
        changed_at_utc="2026-08-29T00:00:00Z",
        reason_code="SEEDED",
    )


def _rollback(**overrides):
    kwargs = {
        "source_run_id": _RUN_ID,
        "source_run_binding_sha256": _BINDING_SHA,
        "baseline_search_configuration": _BASELINE,
        "expected_baseline_search_configuration_sha256": _BASELINE_SHA,
        "serving_configuration_identity": _SERVING_IDENTITY,
        "expected_serving_configuration_identity": _SERVING_IDENTITY,
        "verified_latest_lkg_present": False,
        "route_state_record": None,
        "verified_at_utc": _NOW,
    }
    kwargs.update(overrides)
    return verify_lkg_window_rollback_readiness(**kwargs)


class BootstrapRollbackReadinessTests(unittest.TestCase):
    def test_absent_route_and_no_d1d2_passes(self) -> None:
        result = _rollback()
        self.assertTrue(result.ready)
        self.assertEqual(result.reason_codes, ())
        self.assertFalse(result.document["route_state_present"])
        self.assertIsNone(result.document["route_state_state"])
        self.assertEqual(
            result.document["verification_mode"], LKG_ROLLBACK_VERIFICATION_MODE
        )
        self.assertEqual(
            result.document["restoration_target_digest"], _BASELINE_SHA
        )

    def test_activating_route_marker_fails(self) -> None:
        result = _rollback(route_state_record=_route_record(RouteState.ACTIVATING))
        self.assertFalse(result.ready)
        self.assertIn("CANDIDATE_ROUTE_ACTIVE", result.reason_codes)
        self.assertTrue(result.document["route_state_present"])
        self.assertEqual(result.document["route_state_state"], "ACTIVATING")
        self.assertEqual(result.document["route_state_grant_id"], "grant-1")

    def test_lkg_only_marker_fails_even_with_matching_context(self) -> None:
        # ADR-020 section 30: matching fields cannot substitute for
        # verified Phase-3 authority.
        result = _rollback(route_state_record=_route_record(RouteState.LKG_ONLY))
        self.assertFalse(result.ready)
        self.assertIn("BOOTSTRAP_LKG_ROUTE_MARKER_PRESENT", result.reason_codes)
        self.assertEqual(result.document["route_state_last_known_good_ef"], 400)
        self.assertEqual(
            result.document["route_state_configuration_identity"], _SERVING_IDENTITY
        )

    def test_verified_latest_lkg_present_refuses_as_unauthorized(self) -> None:
        with self.assertRaises(LkgWindowReadinessObservationError) as caught:
            _rollback(verified_latest_lkg_present=True)
        self.assertEqual(
            caught.exception.code, "STEADY_STATE_SEMANTICS_NOT_AUTHORIZED"
        )

    def test_baseline_digest_mismatch_fails(self) -> None:
        result = _rollback(expected_baseline_search_configuration_sha256="c" * 64)
        self.assertFalse(result.ready)
        self.assertIn("BASELINE_CONFIGURATION_DIGEST_MISMATCH", result.reason_codes)

    def test_serving_identity_mismatch_fails(self) -> None:
        result = _rollback(
            expected_serving_configuration_identity="exp010-serving-config-v1:sha256:" + "7" * 64
        )
        self.assertFalse(result.ready)
        self.assertIn("SERVING_CONFIGURATION_IDENTITY_MISMATCH", result.reason_codes)

    def test_document_carries_no_grant_or_steady_state_fields(self) -> None:
        document = _rollback().document
        self.assertNotIn("active_grant_present", document)
        self.assertNotIn("steady_state_restoration_target", document)
        self.assertIs(document["verified_latest_lkg_present"], False)

    def test_source_identity_encodes_bootstrap_mode(self) -> None:
        self.assertIn(
            "FIRST_LKG_BOOTSTRAP_BASELINE_RESTORABILITY",
            LKG_ROLLBACK_READINESS_SOURCE_IDENTITY,
        )

    def test_reason_codes_are_within_the_frozen_set(self) -> None:
        self.assertEqual(len(READINESS_REASON_CODES), 14)
        self.assertEqual(tuple(sorted(set(READINESS_REASON_CODES))), READINESS_REASON_CODES)
        self.assertNotIn("ACTIVE_GRANT_PRESENT", READINESS_REASON_CODES)



# ======================================================================
# Amendment (ADR-020a): LKG-specific sub-digest identities.
#
# These tests deliberately do NOT call the production payload builders to
# construct their expected values. Each expected document is written out
# by hand from fixture constants and hashed with generic hashlib +
# canonical_json_bytes, so a production regression cannot be masked by
# the test deriving its expectation from the code under test.
# ======================================================================

_FIXTURE_FIELDS = [
    {"name": "id", "data_type": "5", "is_primary": True, "dimension": None},
    {"name": "vector", "data_type": "101", "is_primary": False, "dimension": 128},
]
_FIXTURE_PARAMS = [
    {"name": "M", "value": "16"},
    {"name": "efConstruction", "value": "200"},
]


def _expected_collection_schema_digest(
    collection_name="vd_hnsw", database_name="default", fields=None
):
    """Independently computed. No production helper is consulted."""
    payload = {
        "schema_version": "lkg-collection-schema-v1",
        "collection_name": collection_name,
        "database_name": database_name,
        "fields": _FIXTURE_FIELDS if fields is None else fields,
    }
    return hashlib.sha256(
        b"vdbench.lkg-collection-schema.v1\0" + canonical_json_bytes(payload)
    ).hexdigest()


def _expected_index_identity_digest(
    *, collection_schema_sha256, collection_name="vd_hnsw", database_name="default",
    index_name="vector_index", index_type="HNSW", index_metric="L2", params=None,
):
    """Independently computed. No production helper is consulted."""
    payload = {
        "schema_version": "lkg-index-identity-v1",
        "collection_name": collection_name,
        "database_name": database_name,
        "collection_schema_sha256": collection_schema_sha256,
        "index_name": index_name,
        "index_type": index_type,
        "index_metric": index_metric,
        "index_parameters": _FIXTURE_PARAMS if params is None else params,
    }
    return hashlib.sha256(
        b"vdbench.lkg-index-identity.v1\0" + canonical_json_bytes(payload)
    ).hexdigest()


class SpecIndependentSubDigestTests(unittest.TestCase):
    def test_collection_schema_digest_matches_independent_computation(self) -> None:
        expected = _expected_collection_schema_digest()
        document = lkg_collection_schema_document(
            collection_name="vd_hnsw", database_name="default", fields=_FIXTURE_FIELDS
        )
        self.assertEqual(lkg_collection_schema_sha256(document), expected)
        self.assertEqual(
            document["schema_version"], LKG_COLLECTION_SCHEMA_SCHEMA_VERSION
        )
        self.assertEqual(
            set(document),
            {"schema_version", "collection_name", "database_name", "fields"},
        )

    def test_index_identity_digest_matches_independent_computation(self) -> None:
        schema = _expected_collection_schema_digest()
        expected = _expected_index_identity_digest(collection_schema_sha256=schema)
        document = lkg_index_identity_document(
            collection_name="vd_hnsw", database_name="default",
            collection_schema_sha256=schema, index_name="vector_index",
            index_type="HNSW", index_metric="L2", index_parameters=_FIXTURE_PARAMS,
        )
        self.assertEqual(lkg_index_identity_sha256(document), expected)
        self.assertEqual(document["schema_version"], LKG_INDEX_IDENTITY_SCHEMA_VERSION)
        self.assertEqual(set(document), {
            "schema_version", "collection_name", "database_name",
            "collection_schema_sha256", "index_name", "index_type",
            "index_metric", "index_parameters",
        })

    def test_index_identity_binds_the_lkg_schema_digest(self) -> None:
        schema = _expected_collection_schema_digest()
        other = _expected_collection_schema_digest(collection_name="vd_flat")
        a = _expected_index_identity_digest(collection_schema_sha256=schema)
        b = _expected_index_identity_digest(collection_schema_sha256=other)
        self.assertNotEqual(a, b, "index identity must bind the schema digest")

    def test_no_transient_state_in_sub_digest_payloads(self) -> None:
        schema_doc = lkg_collection_schema_document(
            collection_name="vd_hnsw", database_name="default", fields=_FIXTURE_FIELDS
        )
        index_doc = lkg_index_identity_document(
            collection_name="vd_hnsw", database_name="default",
            collection_schema_sha256=_expected_collection_schema_digest(),
            index_name="vector_index", index_type="HNSW", index_metric="L2",
            index_parameters=_FIXTURE_PARAMS,
        )
        for banned in (
            "entity_count", "load_state", "index_readiness", "collection_readiness",
            "milvus_healthz", "container_health", "observed_at_utc",
        ):
            self.assertNotIn(banned, schema_doc)
            self.assertNotIn(banned, index_doc)

    def test_malformed_schema_digest_input_is_refused(self) -> None:
        with self.assertRaises(ContractViolation):
            lkg_collection_schema_sha256({"schema_version": "lkg-collection-schema-v1"})
        with self.assertRaises(ContractViolation):
            lkg_collection_schema_sha256(
                {"schema_version": "wrong", "collection_name": "c",
                 "database_name": "d", "fields": []}
            )

    def test_index_identity_refuses_non_sha256_schema_reference(self) -> None:
        with self.assertRaises(ContractViolation):
            lkg_index_identity_document(
                collection_name="vd_hnsw", database_name="default",
                collection_schema_sha256="not-a-digest", index_name="vector_index",
                index_type="HNSW", index_metric="L2", index_parameters=[],
            )


class DomainSeparationNegativeControlTests(unittest.TestCase):
    """The LKG identity is never an EXP-012 Gate-C identity value.

    The EXP-012 domains are declared locally as literal bytes purely to
    compute a negative control. No private Gate-C helper is imported and
    these bytes are never runtime authority.
    """

    _EXP012_SCHEMA_DOMAIN = b"VD::EXP012_GATE_C_COLLECTION_SCHEMA::V1\x00"
    _EXP012_INDEX_DOMAIN = b"VD::EXP012_GATE_C_INDEX_IDENTITY::V1\x00"

    def test_collection_schema_domains_are_separated(self) -> None:
        payload = {
            "schema_version": "lkg-collection-schema-v1",
            "collection_name": "vd_hnsw",
            "database_name": "default",
            "fields": _FIXTURE_FIELDS,
        }
        lkg = hashlib.sha256(
            LKG_COLLECTION_SCHEMA_DOMAIN + canonical_json_bytes(payload)
        ).hexdigest()
        exp012 = hashlib.sha256(
            self._EXP012_SCHEMA_DOMAIN + canonical_json_bytes(payload)
        ).hexdigest()
        self.assertNotEqual(lkg, exp012)
        self.assertEqual(lkg, _expected_collection_schema_digest())

    def test_index_identity_domains_are_separated(self) -> None:
        payload = {
            "schema_version": "lkg-index-identity-v1",
            "collection_name": "vd_hnsw",
            "database_name": "default",
            "collection_schema_sha256": _expected_collection_schema_digest(),
            "index_name": "vector_index",
            "index_type": "HNSW",
            "index_metric": "L2",
            "index_parameters": _FIXTURE_PARAMS,
        }
        lkg = hashlib.sha256(
            LKG_INDEX_IDENTITY_DOMAIN + canonical_json_bytes(payload)
        ).hexdigest()
        exp012 = hashlib.sha256(
            self._EXP012_INDEX_DOMAIN + canonical_json_bytes(payload)
        ).hexdigest()
        self.assertNotEqual(lkg, exp012)

    def test_exp012_domains_absent_from_production_module(self) -> None:
        import inspect

        import vdbench.lkg_window_readiness_observation as module

        source = inspect.getsource(module)
        self.assertNotIn("EXP012_GATE_C_COLLECTION_SCHEMA", source)
        self.assertNotIn("EXP012_GATE_C_INDEX_IDENTITY", source)
        self.assertNotIn("gate_c_execution_environment", source)


class SubDigestSensitivityTests(unittest.TestCase):
    def _schema(self, **over):
        return _expected_collection_schema_digest(**over)

    def test_collection_name_changes_schema_digest(self) -> None:
        self.assertNotEqual(self._schema(), self._schema(collection_name="vd_flat"))

    def test_database_name_changes_schema_digest(self) -> None:
        self.assertNotEqual(self._schema(), self._schema(database_name="other"))

    def test_field_definition_changes_schema_digest(self) -> None:
        altered = [dict(f) for f in _FIXTURE_FIELDS]
        altered[1]["data_type"] = "102"
        self.assertNotEqual(self._schema(), self._schema(fields=altered))

    def test_field_dimension_changes_schema_digest(self) -> None:
        altered = [dict(f) for f in _FIXTURE_FIELDS]
        altered[1]["dimension"] = 64
        self.assertNotEqual(self._schema(), self._schema(fields=altered))

    def test_field_ordering_changes_schema_digest(self) -> None:
        self.assertNotEqual(
            self._schema(), self._schema(fields=list(reversed(_FIXTURE_FIELDS)))
        )

    def test_index_name_type_metric_and_parameters_change_identity(self) -> None:
        schema = self._schema()
        base = _expected_index_identity_digest(collection_schema_sha256=schema)
        for kwargs in (
            {"index_name": "other_index"},
            {"index_type": "FLAT"},
            {"index_metric": "COSINE"},
            {"params": [{"name": "M", "value": "32"},
                        {"name": "efConstruction", "value": "200"}]},
        ):
            self.assertNotEqual(
                base,
                _expected_index_identity_digest(
                    collection_schema_sha256=schema, **kwargs
                ),
                f"identity must change for {kwargs}",
            )


class TopLevelSection7ShapeTests(unittest.TestCase):
    def test_governed_collection_entry_has_exactly_the_section7_keys(self) -> None:
        stable = _observe().document["observed_stable_environment_document"]
        entries = stable["data_plane"]
        self.assertEqual(len(entries), 2)
        expected_keys = {
            "collection_name", "collection_schema_sha256", "index_identity_sha256",
            "index_type", "index_parameters", "metric", "dimensions", "entity_count",
        }
        for entry in entries:
            self.assertEqual(set(entry), expected_keys)
            for banned in ("database_name", "fields", "collection_fields",
                           "index_name", "index_metric", "role"):
                self.assertNotIn(banned, entry)

    def test_sub_digests_in_entry_are_lkg_digests(self) -> None:
        stable = _observe().document["observed_stable_environment_document"]
        hnsw = stable["data_plane"][1]
        expected_schema = _expected_collection_schema_digest(
            collection_name="vd_hnsw", database_name="default"
        )
        self.assertEqual(hnsw["collection_schema_sha256"], expected_schema)
        self.assertEqual(
            hnsw["index_identity_sha256"],
            _expected_index_identity_digest(collection_schema_sha256=expected_schema),
        )


class SpecIndependentEnvironmentIdentityTests(unittest.TestCase):
    """Build the whole expected stable document by hand, then hash it.

    The production `lkg_environment_identity` is never used to derive the
    expectation, closing the self-referential gap the earlier clean-pass
    test had.
    """

    def _expected_identity(self) -> str:
        flat_schema = _expected_collection_schema_digest(
            collection_name="vd_flat", database_name="default"
        )
        hnsw_schema = _expected_collection_schema_digest(
            collection_name="vd_hnsw", database_name="default"
        )
        expected_document = {
            "schema_version": "lkg-environment-identity-v1",
            "endpoint": {
                "scheme": "http", "host": "127.0.0.1", "port": 19530,
                "transport_security": "PLAINTEXT",
            },
            "containers": [
                {
                    "role": role,
                    "container_name": name,
                    "container_id": "c" * 64,
                    "image_id": "sha256:" + "d" * 64,
                    "repository_digests": ["repo@sha256:" + "e" * 64],
                    "restart_count": 0,
                    "oom_killed": False,
                    "started_at": "2026-08-26T03:51:13Z",
                }
                for role, name in (
                    ("etcd", "milvus-etcd"),
                    ("minio", "milvus-minio"),
                    ("milvus", "milvus-standalone"),
                )
            ],
            "data_plane": [
                {
                    "collection_name": "vd_flat",
                    "collection_schema_sha256": flat_schema,
                    "index_identity_sha256": _expected_index_identity_digest(
                        collection_schema_sha256=flat_schema,
                        collection_name="vd_flat", index_type="FLAT",
                    ),
                    "index_type": "FLAT",
                    "index_parameters": _FIXTURE_PARAMS,
                    "metric": "L2",
                    "dimensions": 128,
                    "entity_count": 10000,
                },
                {
                    "collection_name": "vd_hnsw",
                    "collection_schema_sha256": hnsw_schema,
                    "index_identity_sha256": _expected_index_identity_digest(
                        collection_schema_sha256=hnsw_schema,
                    ),
                    "index_type": "HNSW",
                    "index_parameters": _FIXTURE_PARAMS,
                    "metric": "L2",
                    "dimensions": 128,
                    "entity_count": 10000,
                },
            ],
            "expected_entity_count": 10000,
            "metric": "L2",
            "dimensions": 128,
        }
        digest = hashlib.sha256(
            b"vdbench.lkg-environment-identity.v1\0"
            + canonical_json_bytes(expected_document)
        ).hexdigest()
        return f"lkg-env-identity-v1:sha256:{digest}"

    def test_outer_identity_matches_independent_computation(self) -> None:
        observed = _observe().document["observed_environment_identity"]
        self.assertEqual(observed, self._expected_identity())

    def test_changing_a_governed_schema_fact_changes_the_identity(self) -> None:
        baseline = _observe().document["observed_environment_identity"]

        class _AlteredSchemaReader(_Reader):
            def describe_collection(self, *, collection_name):
                doc = super().describe_collection(collection_name=collection_name)
                doc["fields"][1]["params"] = {"dim": 64}
                return doc

        altered = _observe(_AlteredSchemaReader()).document[
            "observed_environment_identity"
        ]
        self.assertNotEqual(baseline, altered)

    def test_changing_an_index_parameter_changes_the_identity(self) -> None:
        baseline = _observe().document["observed_environment_identity"]

        class _AlteredParamReader(_Reader):
            def describe_index(self, *, collection_name, index_name):
                doc = super().describe_index(
                    collection_name=collection_name, index_name=index_name
                )
                doc["M"] = "32"
                return doc

        altered = _observe(_AlteredParamReader()).document[
            "observed_environment_identity"
        ]
        self.assertNotEqual(baseline, altered)

class RetainedEvidenceValidationTests(unittest.TestCase):
    def validate(self, result, **overrides):
        health = "observation_schema_version" in result.document
        context = {
            "source_run_id": _RUN_ID, "source_run_binding_sha256": _BINDING_SHA,
            "source_identity": (LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY if health
                                else LKG_ROLLBACK_READINESS_SOURCE_IDENTITY),
        }
        if health:
            context["run_bound_environment_identity"] = result.document["run_bound_environment_identity"]
        context.update(overrides)
        validator = validate_lkg_window_health_observation if health else validate_lkg_window_rollback_readiness
        return validator(result, **context)

    def test_canonical_results_accepted_as_exact_detached_preimages(self):
        for result in (_observe(), _rollback(), _observe(healthz=False),
                       _observe(_Reader(rows=9999)),
                       _rollback(route_state_record=_route_record(RouteState.ACTIVATING)),
                       _rollback(baseline_search_configuration=None)):
            with self.subTest(reasons=result.reason_codes):
                rebuilt, raw = self.validate(result)
                self.assertEqual(rebuilt, result)
                self.assertEqual(raw, canonical_json_bytes(result.document))
                self.assertIsNot(rebuilt.document, result.document)

    def test_wrong_digest_reasons_verdict_and_result_types_refuse(self):
        for original in (_observe(), _rollback()):
            verdict = "passed" if hasattr(original, "passed") else "ready"
            for changes in ({"digest": "0" * 64}, {"digest": "invalid"},
                            {"reason_codes": ("CONTAINER_UNHEALTHY",)},
                            {"reason_codes": []}, {verdict: False}, {verdict: 1}):
                with self.subTest(kind=type(original), changes=changes):
                    with self.assertRaises(ContractViolation):
                        self.validate(replace(original, **changes))
        with self.assertRaises(ContractViolation):
            validate_lkg_window_health_observation(
                _rollback(), source_identity=LKG_HEALTH_OBSERVATION_SOURCE_IDENTITY,
                source_run_id=_RUN_ID, source_run_binding_sha256=_BINDING_SHA,
                run_bound_environment_identity=_observe().document["run_bound_environment_identity"],
            )
        with self.assertRaises(ContractViolation):
            validate_lkg_window_rollback_readiness(
                _observe(), source_identity=LKG_ROLLBACK_READINESS_SOURCE_IDENTITY,
                source_run_id=_RUN_ID, source_run_binding_sha256=_BINDING_SHA,
            )

    def test_wrong_source_run_and_binding_identities_refuse(self):
        for result in (_observe(), _rollback()):
            for change in ({"source_identity": "wrong"}, {"source_run_id": "other"},
                           {"source_run_binding_sha256": "b" * 64}):
                with self.subTest(kind=type(result), change=change):
                    with self.assertRaises(ContractViolation):
                        self.validate(result, **change)
        with self.assertRaises(ContractViolation):
            self.validate(_observe(), run_bound_environment_identity="lkg-env-identity-v1:sha256:" + "0" * 64)

    def test_malformed_and_noncanonical_documents_refuse_even_with_rehashed_digest(self):
        for original in (_observe(), _rollback()):
            domain = (b"vdbench.lkg-window-health-observation.v1\0" if hasattr(original, "passed")
                      else b"vdbench.lkg-window-rollback-readiness.v1\0")
            for change in ({"extra": 1}, {"reason_codes": ["UNKNOWN"]},
                           {"reason_codes": ["SERVING_CONFIGURATION_IDENTITY_MISMATCH"] * 2},
                           {"reason_codes": ("CONTAINER_UNHEALTHY",)},
                           {"source_run_id": 7}, {"source_run_binding_sha256": None}):
                with self.subTest(kind=type(original), change=change):
                    doc = {**original.document, **change}
                    result = replace(original, document=doc, digest=hashlib.sha256(domain + canonical_json_bytes(doc)).hexdigest())
                    with self.assertRaises(ContractViolation):
                        self.validate(result)

    def test_retained_environment_and_configuration_contradictions_refuse(self):
        health = _observe()
        rollback = _rollback()
        cases = [
            (health, {"observed_environment_identity": "lkg-env-identity-v1:sha256:" + "0" * 64}),
            (health, {"environment_identity_matches": False}),
            (health, {"milvus_healthz": False}),
            (health, {"container_health": {"etcd": True}}),
            (rollback, {"baseline_search_configuration_sha256": "0" * 64}),
            (rollback, {"restoration_target_digest": None}),
            (rollback, {"route_state_state": "ACTIVATING"}),
            (rollback, {"verified_latest_lkg_present": True}),
        ]
        malformed = deepcopy(rollback.document["baseline_search_configuration_document"])
        malformed["range_filter"] = -0.0
        cases.append((rollback, {"baseline_search_configuration_document": malformed}))
        for result, changes in cases:
            with self.subTest(changes=changes):
                domain = (b"vdbench.lkg-window-health-observation.v1\0" if result is health
                          else b"vdbench.lkg-window-rollback-readiness.v1\0")
                doc = {**result.document, **changes}
                with self.assertRaises(ContractViolation):
                    self.validate(replace(result, document=doc, digest=hashlib.sha256(domain + canonical_json_bytes(doc)).hexdigest()))

    def test_health_recorded_causes_need_not_be_reconstructed_from_omitted_status(self):
        stopped = _observe(container=lambda: _container(status="exited"))
        unhealthy = _observe(container=lambda: _container(health="unhealthy"))
        self.assertEqual(
            {k: v for k, v in stopped.document.items() if k != "reason_codes"},
            {k: v for k, v in unhealthy.document.items() if k != "reason_codes"},
        )
        self.assertNotEqual(stopped.reason_codes, unhealthy.reason_codes)
        self.validate(stopped)
        self.validate(unhealthy)

    def test_container_reason_categories_preserve_canonical_builder_results(self):
        oom = "CONTAINER_OOM_KILLED"
        stopped = "CONTAINER_NOT_RUNNING"
        unhealthy = "CONTAINER_UNHEALTHY"
        cases = (
            (({}, {}, {}), ()),
            (({"oom": True}, {}, {}), (oom,)),
            (({"status": "exited"}, {}, {}), (stopped,)),
            (({"health": "unhealthy"}, {}, {}), (unhealthy,)),
            (({"oom": True}, {"status": "exited"}, {}), (stopped, oom)),
            (({"oom": True}, {"health": "unhealthy"}, {}), (oom, unhealthy)),
            (({"oom": True},) * 3, (oom,)),
            (({"status": "exited"},) * 3, (stopped,)),
            (({"health": "unhealthy"},) * 3, (unhealthy,)),
            (({"oom": True, "status": "exited"}, {}, {}), (stopped, oom)),
            (({"oom": True, "health": "unhealthy"}, {}, {}), (oom, unhealthy)),
        )
        for containers, expected in cases:
            with self.subTest(containers=containers):
                by_name = dict(zip(
                    (_SPEC.etcd_container, _SPEC.minio_container, _SPEC.milvus_container),
                    containers, strict=True,
                ))
                result = _observe(container_inspector=lambda name: _container(**by_name[name]))
                self.assertEqual(result.reason_codes, expected)
                self.validate(result)

    def test_mixed_container_retained_reason_inconsistency_refuses(self):
        for cause in ({"status": "exited"}, {"health": "unhealthy"}):
            original = _observe(container_inspector=lambda name: _container(
                **({"oom": True} if name == _SPEC.etcd_container else
                   cause if name == _SPEC.minio_container else {})
            ))
            self.assertEqual(original.document["container_health"],
                             {"etcd": False, "minio": False, "milvus": True})
            self.assertEqual(
                [entry["oom_killed"] for entry in original.document["observed_stable_environment_document"]["containers"]],
                [True, False, False],
            )
            for reasons in (("CONTAINER_OOM_KILLED",),
                            tuple(code for code in original.reason_codes if code != "CONTAINER_OOM_KILLED")):
                with self.subTest(cause=cause, reasons=reasons):
                    doc = {**original.document, "reason_codes": list(reasons)}
                    digest = hashlib.sha256(
                        b"vdbench.lkg-window-health-observation.v1\0" + canonical_json_bytes(doc)
                    ).hexdigest()
                    malformed = replace(original, document=doc, digest=digest,
                                        reason_codes=reasons, passed=False)
                    with self.assertRaises(ContractViolation):
                        self.validate(malformed)

    def test_container_reasons_without_retained_failures_refuse(self):
        original = _observe()
        for reason in ("CONTAINER_OOM_KILLED", "CONTAINER_NOT_RUNNING", "CONTAINER_UNHEALTHY"):
            with self.subTest(reason=reason):
                doc = {**original.document, "reason_codes": [reason]}
                digest = hashlib.sha256(
                    b"vdbench.lkg-window-health-observation.v1\0" + canonical_json_bytes(doc)
                ).hexdigest()
                with self.assertRaises(ContractViolation):
                    self.validate(replace(original, document=doc, digest=digest,
                                          reason_codes=(reason,), passed=False))

    def test_rollback_recorded_causes_need_not_be_reconstructed_from_omitted_expectations(self):
        passing = _rollback()
        for result in (_rollback(expected_serving_configuration_identity="different"),
                       _rollback(expected_baseline_search_configuration_sha256="0" * 64)):
            self.assertEqual(
                {k: v for k, v in passing.document.items() if k != "reason_codes"},
                {k: v for k, v in result.document.items() if k != "reason_codes"},
            )
            self.assertTrue(passing.ready)
            self.assertFalse(result.ready)
            self.validate(result)
        self.validate(passing)


if __name__ == "__main__":
    unittest.main()
