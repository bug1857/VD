"""Offline coverage for the governed v2 serving-configuration identity,
durable request-id uniqueness, and the real-application ingress.

Everything here is offline: serving and shadow capture are injected fakes, no
PyMilvus client is constructed, and no service is contacted. No workload is
generated, sampled, or replayed -- every request in these tests is an explicit
external payload.
"""

from __future__ import annotations

import ast
from pathlib import Path
import tempfile
import unittest

from vdbench.config import Metric
from vdbench.exp010_ingress import (
    PAYLOAD_FIELDS,
    Exp010IngressError,
    Exp010RequestIngress,
)
from vdbench.exp010_live_runner import (
    Exp010LiveRunner,
    Exp010OperatorConfiguration,
)
from vdbench.exp010_serving_configuration import (
    EXP010_SERVING_CONFIGURATION_PREFIX,
    FORBIDDEN_FIELDS,
    Exp010ServingConfiguration,
    Exp010ServingConfigurationError,
    derive_serving_configuration_identity,
    serving_configuration_from_mapping,
    serving_configuration_payload,
)
from vdbench.host_observation import RangeQueryRequest, ServedQueryOutcome
from vdbench.host_window_lineage import (
    HostResponseCommitError,
    SQLiteHostResponseCommitStore,
)
from vdbench.shadow_event_types import MonitorStreamKey

from tests.test_exp010_live_runner import _ShadowCapture, DATA_IDENTITY

INGRESS_MODULE = Path(__file__).parents[1] / "src" / "vdbench" / "exp010_ingress.py"
CONFIG_MODULE = (
    Path(__file__).parents[1] / "src" / "vdbench" / "exp010_serving_configuration.py"
)
DATASET001 = Path(__file__).parents[1] / "artifacts" / "exp-001" / "dataset"

_RADIUS = 191.85897352125554
_ENVIRONMENT = "e" * 64
_REVISION = "9703bbd1e8dcc1273cba56d076bbd2b0dce4f89a"
_DIMENSIONS = 128


def _configuration() -> Exp010ServingConfiguration:
    return Exp010ServingConfiguration(
        metric=Metric.L2, threshold_stratum="target-075",
        threshold_radius=_RADIUS, range_filter=0.0, limit=100,
        served_ef=400, dimensions=_DIMENSIONS, consistency_level="Strong",
    )


class ServingConfigurationIdentityTests(unittest.TestCase):
    def test_canonical_payload_is_exactly_the_nine_serving_fields(self) -> None:
        payload = serving_configuration_payload(_configuration())
        self.assertEqual(
            sorted(payload),
            sorted([
                "schema_version", "metric", "threshold_stratum", "threshold_radius",
                "range_filter", "limit", "served_ef", "dimensions",
                "consistency_level",
            ]),
        )
        self.assertEqual(payload["schema_version"], "exp010-serving-configuration-v1")
        self.assertEqual(payload["metric"], "L2")
        self.assertEqual(payload["threshold_radius"], _RADIUS)
        self.assertEqual(payload["served_ef"], 400)

    def test_identity_is_deterministic_and_self_identifying(self) -> None:
        first = derive_serving_configuration_identity(_configuration())
        second = derive_serving_configuration_identity(_configuration())
        self.assertEqual(first, second)
        prefix, algorithm, digest = first.split(":")
        self.assertEqual(prefix, EXP010_SERVING_CONFIGURATION_PREFIX)
        self.assertEqual(algorithm, "sha256")
        self.assertEqual(len(digest), 64)
        int(digest, 16)

    def test_field_order_cannot_affect_the_identity(self) -> None:
        forward = {
            "metric": Metric.L2, "threshold_stratum": "target-075",
            "threshold_radius": _RADIUS, "range_filter": 0.0, "limit": 100,
            "served_ef": 400, "dimensions": _DIMENSIONS,
            "consistency_level": "Strong",
        }
        reversed_order = dict(reversed(list(forward.items())))
        self.assertNotEqual(list(forward), list(reversed_order))
        self.assertEqual(
            derive_serving_configuration_identity(
                serving_configuration_from_mapping(forward)
            ),
            derive_serving_configuration_identity(
                serving_configuration_from_mapping(reversed_order)
            ),
        )

    def test_every_semantic_field_change_changes_the_identity(self) -> None:
        base = derive_serving_configuration_identity(_configuration())
        for label, changes in (
            ("served_ef", {"served_ef": 800}),
            ("radius", {"threshold_radius": 191.0}),
            ("stratum", {"threshold_stratum": "target-025"}),
        ):
            with self.subTest(field=label):
                values = {
                    "metric": Metric.L2, "threshold_stratum": "target-075",
                    "threshold_radius": _RADIUS, "range_filter": 0.0,
                    "limit": 100, "served_ef": 400, "dimensions": _DIMENSIONS,
                    "consistency_level": "Strong",
                }
                values.update(changes)
                self.assertNotEqual(
                    base,
                    derive_serving_configuration_identity(
                        serving_configuration_from_mapping(values)
                    ),
                )

    def test_invalid_fields_fail_closed(self) -> None:
        cases = {
            "bool_as_int_limit": {"limit": True},
            "bool_as_int_served_ef": {"served_ef": True},
            "served_ef_off_ladder": {"served_ef": 401},
            "limit_not_governed": {"limit": 50},
            "dimensions_wrong": {"dimensions": 64},
            "nonfinite_radius": {"threshold_radius": float("inf")},
            "bad_range_filter": {"range_filter": 1.0},
            "bad_stratum": {"threshold_stratum": "target-999"},
            "bad_consistency": {"consistency_level": "Bounded"},
        }
        for label, changes in cases.items():
            with self.subTest(case=label):
                values = {
                    "metric": Metric.L2, "threshold_stratum": "target-075",
                    "threshold_radius": _RADIUS, "range_filter": 0.0,
                    "limit": 100, "served_ef": 400, "dimensions": _DIMENSIONS,
                    "consistency_level": "Strong",
                }
                values.update(changes)
                with self.assertRaises(Exp010ServingConfigurationError):
                    serving_configuration_from_mapping(values)

    def test_missing_and_unknown_fields_fail_closed(self) -> None:
        base = {
            "metric": Metric.L2, "threshold_stratum": "target-075",
            "threshold_radius": _RADIUS, "range_filter": 0.0, "limit": 100,
            "served_ef": 400, "dimensions": _DIMENSIONS,
            "consistency_level": "Strong",
        }
        missing = dict(base); missing.pop("limit")
        extra = dict(base, data_identity=DATA_IDENTITY)
        for label, values in (("missing", missing), ("unknown", extra)):
            with self.subTest(case=label):
                with self.assertRaises(Exp010ServingConfigurationError) as raised:
                    serving_configuration_from_mapping(values)
                self.assertEqual(raised.exception.code, "CONFIGURATION_FIELDS_INVALID")

    def test_domain_separation_other_identities_are_excluded(self) -> None:
        """The serving identity must not absorb another authority domain."""

        payload = serving_configuration_payload(_configuration())
        self.assertFalse(set(payload) & FORBIDDEN_FIELDS)
        for excluded in (
            "data_identity", "flat_binding_id", "hnsw_binding_id",
            "environment_manifest_sha256", "deployment_identity",
            "source_revision", "stream_id", "detector_seed", "observed_at_utc",
            "sentinel_ef", "candidate_ef", "last_known_good_ef",
        ):
            self.assertNotIn(excluded, payload)

    def test_sentinel_ef_is_governed_by_the_detector_contract_not_here(self) -> None:
        """Proof the exclusion is safe: sentinel_ef is bound elsewhere."""

        from vdbench.drift import SENTINEL_EF
        from vdbench.real_detector_attestation import detector_contract_identity

        contract_source = (
            Path(__file__).parents[1] / "src" / "vdbench" / "real_detector_attestation.py"
        ).read_text(encoding="utf-8")
        self.assertIn('"sentinel_ef": SENTINEL_EF', contract_source)
        self.assertEqual(SENTINEL_EF, 100)
        self.assertEqual(len(detector_contract_identity()), 64)
        # ... and it is separately bound in every shadow trace.
        shadow_source = (
            Path(__file__).parents[1] / "src" / "vdbench" / "shadow_window.py"
        ).read_text(encoding="utf-8")
        self.assertIn("trace.sentinel_ef", shadow_source)


class _Serving:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls = 0
        self._fail = fail

    def execute(self, request: RangeQueryRequest) -> ServedQueryOutcome:
        self.calls += 1
        if self._fail:
            raise RuntimeError("injected serving failure")
        return ServedQueryOutcome(True, False, 3, 1.5)


class _IngressHarness:
    def __init__(self, root: Path, *, serving_fails: bool = False) -> None:
        configuration = _configuration()
        identity = derive_serving_configuration_identity(configuration)
        self.serving = _Serving(fail=serving_fails)
        self._tick = 0
        operator = Exp010OperatorConfiguration(
            milvus_uri="http://milvus.invalid:19530",
            flat_collection_name="vd_exp010_l2_flat_v1",
            hnsw_collection_name="vd_exp010_l2_hnsw_v1",
            metric=Metric.L2,
            threshold_stratum="target-075",
            threshold_radius=_RADIUS,
            served_ef=400,
            detector_seed=20260812,
            stream_id="vd-exp010-l2",
            configuration_identity=identity,
            flat_binding_id="b63cf68a332127416d0cdf5372d4b8f4bac0c27d8f44b59c78b0953c4669bb46",
            hnsw_binding_id="2db7944f6aa5190736ddafd1d25391aba648b5931734fc4b833ff02b3cec7bca",
            source_revision=_REVISION,
            environment_manifest_sha256=_ENVIRONMENT,
            store_root=root / "stores",
            dataset001_dir=DATASET001,
            exp010_output_dir=root / "exp010",
        )
        self.runner = Exp010LiveRunner(
            configuration=operator,
            serving_executor=self.serving,
            shadow_capture_executor=_ShadowCapture(),
            clock=lambda: "2026-08-13T00:00:00Z",
            shadow_captured_at_clock=self._shadow_clock,
        )
        self.ingress = Exp010RequestIngress(
            runner=self.runner, serving_configuration=configuration
        )

    def _shadow_clock(self) -> str:
        self._tick += 1
        return f"2026-08-13T00:00:{self._tick % 60:02d}Z"

    def close(self) -> None:
        self.runner.close()


def _payload(request_id, *, offset: float = 0.0):
    return {
        "request_id": request_id,
        "query_vector": [float(i) + offset for i in range(_DIMENSIONS)],
    }


class Exp010IngressTests(unittest.TestCase):
    def test_valid_request_reaches_serve_exactly_once_and_conserves_vector(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            h = _IngressHarness(Path(directory))
            try:
                payload = _payload("app-request-1")
                result = h.ingress.admit(payload)
                self.assertEqual(h.serving.calls, 1)
                self.assertEqual(result.request_id, "app-request-1")
                committed = h.runner.composition.response_store.poll(
                    consumer_id="probe", limit=5
                )
                self.assertEqual(len(committed), 1)
                self.assertEqual(
                    committed[0].query_vector, tuple(payload["query_vector"])
                )
                self.assertEqual(committed[0].query_id, "app-request-1")
                self.assertEqual(committed[0].source_sequence, 0)
                self.assertEqual(committed[0].served_ef, 400)
                self.assertEqual(committed[0].threshold_radius, _RADIUS)
                self.assertEqual(committed[0].limit, 100)
            finally:
                h.close()

    def test_arbitrary_genuine_ids_need_no_preregistration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            h = _IngressHarness(Path(directory))
            try:
                for rid in ("free-form-id", 987654321, "another/app:id"):
                    h.ingress.admit(_payload(rid, offset=float(len(str(rid)))))
                self.assertEqual(h.serving.calls, 3)
            finally:
                h.close()

    def test_caller_cannot_override_any_governed_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            h = _IngressHarness(Path(directory))
            try:
                for field in (
                    "served_ef", "metric", "threshold_radius", "limit",
                    "data_identity", "configuration_identity", "stream_id",
                    "flat_binding_id", "hnsw_binding_id",
                    "environment_manifest_sha256", "source_revision",
                    "detector_seed", "range_filter", "dimensions",
                ):
                    with self.subTest(field=field):
                        payload = _payload("override-attempt")
                        payload[field] = "attacker-value"
                        with self.assertRaises(Exp010IngressError) as raised:
                            h.ingress.admit(payload)
                        self.assertEqual(
                            raised.exception.code, "INGRESS_IDENTITY_OVERRIDE_REFUSED"
                        )
                self.assertEqual(h.serving.calls, 0)
            finally:
                h.close()

    def test_unknown_key_and_malformed_vectors_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            h = _IngressHarness(Path(directory))
            try:
                cases = {
                    "unknown_key": ({**_payload("x"), "note": "hi"},
                                    "INGRESS_PAYLOAD_FIELDS_INVALID"),
                    "missing_vector": ({"request_id": "x"},
                                       "INGRESS_PAYLOAD_FIELDS_INVALID"),
                    "short_vector": ({"request_id": "x", "query_vector": [1.0] * 127},
                                     "INGRESS_QUERY_VECTOR_DIMENSIONS_INVALID"),
                    "long_vector": ({"request_id": "x", "query_vector": [1.0] * 129},
                                    "INGRESS_QUERY_VECTOR_DIMENSIONS_INVALID"),
                    "nan": ({"request_id": "x",
                             "query_vector": [float("nan")] + [1.0] * 127},
                            "INGRESS_QUERY_VECTOR_NONFINITE"),
                    "inf": ({"request_id": "x",
                             "query_vector": [float("inf")] + [1.0] * 127},
                            "INGRESS_QUERY_VECTOR_NONFINITE"),
                    "bad_request_id": ({"request_id": True,
                                        "query_vector": [1.0] * 128},
                                       "INGRESS_REQUEST_ID_INVALID"),
                    "empty_request_id": ({"request_id": "  ",
                                          "query_vector": [1.0] * 128},
                                         "INGRESS_REQUEST_ID_INVALID"),
                }
                for label, (payload, code) in cases.items():
                    with self.subTest(case=label):
                        with self.assertRaises(Exp010IngressError) as raised:
                            h.ingress.admit(payload)
                        self.assertEqual(raised.exception.code, code)
                # No validation failure may create membership or call serve.
                self.assertEqual(h.serving.calls, 0)
                self.assertEqual(
                    h.runner.composition.response_store.poll(
                        consumer_id="probe", limit=5
                    ),
                    (),
                )
            finally:
                h.close()

    def test_duplicate_request_id_fails_closed_same_process(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            h = _IngressHarness(Path(directory))
            try:
                h.ingress.admit(_payload("dup-1"))
                with self.assertRaises(Exp010IngressError) as raised:
                    h.ingress.admit(_payload("dup-1", offset=5.0))
                self.assertEqual(
                    raised.exception.code, "INGRESS_REQUEST_ID_DUPLICATE"
                )
                committed = h.runner.composition.response_store.poll(
                    consumer_id="probe", limit=10
                )
                self.assertEqual(len(committed), 1)
                # The rejected duplicate consumed no sequence: the next unique
                # request is contiguous.
                h.ingress.admit(_payload("unique-2", offset=9.0))
                committed = h.runner.composition.response_store.poll(
                    consumer_id="probe2", limit=10
                )
                self.assertEqual(
                    tuple(item.source_sequence for item in committed), (0, 1)
                )
            finally:
                h.close()

    def test_duplicate_is_rejected_after_serving_not_at_admission(self) -> None:
        """Documents the true ordering: the search runs, then the commit fails.

        `ReferenceV2Host.execute` serves before it commits, so a duplicate
        request id consumes one real search's worth of serving load. That is
        accepted: the durable invariant is about *membership*, and a duplicate
        can never become a visible successful new source observation.
        """

        with tempfile.TemporaryDirectory() as directory:
            h = _IngressHarness(Path(directory))
            try:
                h.ingress.admit(_payload("dup-timing"))
                self.assertEqual(h.serving.calls, 1)
                with self.assertRaises(Exp010IngressError) as raised:
                    h.ingress.admit(_payload("dup-timing", offset=4.0))
                self.assertEqual(
                    raised.exception.code, "INGRESS_REQUEST_ID_DUPLICATE"
                )
                # The duplicate DID reach the serving executor ...
                self.assertEqual(h.serving.calls, 2)
                # ... and produced no membership and no visible response.
                committed = h.runner.composition.response_store.poll(
                    consumer_id="probe", limit=10
                )
                self.assertEqual(len(committed), 1)
                self.assertEqual(committed[0].query_id, "dup-timing")
                # The surviving member is the FIRST request's vector, never the
                # duplicate's: no silent overwrite occurred.
                self.assertEqual(
                    committed[0].query_vector,
                    tuple(_payload("dup-timing")["query_vector"]),
                )
            finally:
                h.close()

    def test_duplicate_request_id_fails_closed_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            h = _IngressHarness(root)
            try:
                h.ingress.admit(_payload("persist-1"))
            finally:
                h.close()
            reopened = _IngressHarness(root)
            try:
                with self.assertRaises(Exp010IngressError) as raised:
                    reopened.ingress.admit(_payload("persist-1", offset=3.0))
                self.assertEqual(
                    raised.exception.code, "INGRESS_REQUEST_ID_DUPLICATE"
                )
                reopened.ingress.admit(_payload("persist-2", offset=4.0))
                committed = reopened.runner.composition.response_store.poll(
                    consumer_id="probe", limit=10
                )
                self.assertEqual(
                    tuple(item.source_sequence for item in committed), (0, 1)
                )
            finally:
                reopened.close()

    def test_serving_failure_never_returns_a_fabricated_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            h = _IngressHarness(Path(directory), serving_fails=True)
            try:
                with self.assertRaises(RuntimeError):
                    h.ingress.admit(_payload("serving-fails"))
                self.assertEqual(
                    h.runner.composition.response_store.poll(
                        consumer_id="probe", limit=5
                    ),
                    (),
                )
            finally:
                h.close()

    def test_source_commit_failure_never_returns_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            h = _IngressHarness(Path(directory))
            try:
                h.runner.composition.response_store.close()
                with self.assertRaises(Exp010IngressError) as raised:
                    h.ingress.admit(_payload("commit-fails"))
                self.assertEqual(
                    raised.exception.code, "INGRESS_SOURCE_COMMIT_FAILED"
                )
            finally:
                try:
                    h.close()
                except Exception:
                    pass

    def test_ingress_refuses_a_stream_whose_configuration_identity_differs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            h = _IngressHarness(Path(directory))
            try:
                other = Exp010ServingConfiguration(
                    metric=Metric.L2, threshold_stratum="target-075",
                    threshold_radius=_RADIUS, range_filter=0.0, limit=100,
                    served_ef=800, dimensions=_DIMENSIONS,
                    consistency_level="Strong",
                )
                with self.assertRaises(Exp010IngressError) as raised:
                    Exp010RequestIngress(
                        runner=h.runner, serving_configuration=other
                    )
                self.assertEqual(
                    raised.exception.code,
                    "INGRESS_CONFIGURATION_IDENTITY_MISMATCH",
                )
            finally:
                h.close()


class DurableUniquenessStoreTests(unittest.TestCase):
    def test_store_rejects_a_duplicate_query_id_at_commit_time(self) -> None:
        from vdbench.host_observation import CompletedRangeQueryObservation

        stream = MonitorStreamKey(
            "s", Metric.L2, "target-075", "cfg", DATA_IDENTITY, "flat", "hnsw"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with SQLiteHostResponseCommitStore(
                path, stream_key=stream, source_revision="rev",
                environment_manifest_sha256=_ENVIRONMENT,
            ) as store:
                def observation(request_id, offset):
                    return CompletedRangeQueryObservation(
                        request_id, "2026-08-13T00:00:00Z", stream,
                        (1.0 + offset, 2.0), 2.0, 0.0, 100, 400,
                        ServedQueryOutcome(True, False, 1, 1.0),
                    )
                store.commit_response(observation("q-1", 0.0),
                                      committed_at_utc="2026-08-13T00:00:00Z")
                with self.assertRaises(HostResponseCommitError) as raised:
                    store.commit_response(observation("q-1", 7.0),
                                          committed_at_utc="2026-08-13T00:00:01Z")
                self.assertEqual(
                    raised.exception.code, "HOST_SOURCE_QUERY_ID_DUPLICATE"
                )
                # Sequence not consumed by the rejected duplicate.
                record = store.commit_response(
                    observation("q-2", 3.0), committed_at_utc="2026-08-13T00:00:02Z"
                )
                self.assertEqual(record.source_sequence, 1)

    def test_relational_query_id_column_is_bound_to_the_canonical_record(self) -> None:
        """The UNIQUE column is a mechanism, never an independent authority.

        Tampering with only the relational `query_id_sha256` -- leaving
        source_json, source_sha256, the outbox chain and the schema otherwise
        valid -- must fail closed on reopen, because verification requires the
        column to equal the value reconstructed from the canonical record.
        """

        import sqlite3
        from vdbench.host_observation import CompletedRangeQueryObservation
        from vdbench.host_window_lineage import _SCHEMA_SQL

        stream = MonitorStreamKey(
            "s", Metric.L2, "target-075", "cfg", DATA_IDENTITY, "flat", "hnsw"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with SQLiteHostResponseCommitStore(
                path, stream_key=stream, source_revision="rev",
                environment_manifest_sha256=_ENVIRONMENT,
            ) as store:
                for index, request_id in enumerate(("q-1", "q-2")):
                    store.commit_response(
                        CompletedRangeQueryObservation(
                            request_id, "2026-08-13T00:00:00Z", stream,
                            (1.0 + index, 2.0), 2.0, 0.0, 100, 400,
                            ServedQueryOutcome(True, False, 1, 1.0),
                        ),
                        committed_at_utc=f"2026-08-13T00:00:0{index}Z",
                    )
                genuine = store.poll(consumer_id="probe", limit=5)[0].query_id_sha256

            trigger = next(
                statement for statement in _SCHEMA_SQL
                if statement.startswith("CREATE TRIGGER source_records_no_update")
            )
            forged = "a" * 64
            self.assertNotEqual(forged, genuine)
            connection = sqlite3.connect(path)
            try:
                # The append-only trigger is the first line of defence.
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE source_records SET query_id_sha256=? "
                        "WHERE source_sequence=0",
                        (forged,),
                    )
                # Defeat it the only way an offline attacker could, then restore
                # the schema byte-for-byte so the exact-set schema check passes.
                connection.execute("DROP TRIGGER source_records_no_update")
                connection.execute(
                    "UPDATE source_records SET query_id_sha256=? "
                    "WHERE source_sequence=0",
                    (forged,),
                )
                connection.execute(trigger)
                connection.commit()
            finally:
                connection.close()

            with self.assertRaises(HostResponseCommitError) as raised:
                SQLiteHostResponseCommitStore(
                    path, stream_key=stream, source_revision="rev",
                    environment_manifest_sha256=_ENVIRONMENT,
                )
            self.assertEqual(raised.exception.code, "HOST_SOURCE_CHAIN_INVALID")

    def test_rejecting_an_older_store_leaves_its_bytes_untouched(self) -> None:
        """A v1 store is refused, never migrated, rewritten, or truncated."""

        import hashlib
        import sqlite3

        stream = MonitorStreamKey(
            "s", Metric.L2, "target-075", "cfg", DATA_IDENTITY, "flat", "hnsw"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with SQLiteHostResponseCommitStore(
                path, stream_key=stream, source_revision="rev",
                environment_manifest_sha256=_ENVIRONMENT,
            ):
                pass
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA user_version=1")
            connection.commit()
            connection.close()
            before = hashlib.sha256(path.read_bytes()).hexdigest()

            with self.assertRaises(HostResponseCommitError):
                SQLiteHostResponseCommitStore(
                    path, stream_key=stream, source_revision="rev",
                    environment_manifest_sha256=_ENVIRONMENT,
                )
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)

    def test_schema_version_bump_rejects_an_older_store_rather_than_migrating(self) -> None:
        """An existing v1 database must fail closed, never be rewritten."""

        import sqlite3
        from vdbench.host_window_lineage import _DB_VERSION

        self.assertEqual(_DB_VERSION, 2)
        stream = MonitorStreamKey(
            "s", Metric.L2, "target-075", "cfg", DATA_IDENTITY, "flat", "hnsw"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with SQLiteHostResponseCommitStore(
                path, stream_key=stream, source_revision="rev",
                environment_manifest_sha256=_ENVIRONMENT,
            ):
                pass
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA user_version=1")
            connection.commit()
            connection.close()
            with self.assertRaises(HostResponseCommitError) as raised:
                SQLiteHostResponseCommitStore(
                    path, stream_key=stream, source_revision="rev",
                    environment_manifest_sha256=_ENVIRONMENT,
                )
            self.assertEqual(raised.exception.code, "HOST_SOURCE_SCHEMA_INVALID")


class IngressGuardTests(unittest.TestCase):
    def test_ingress_cannot_reach_gate_c_d_or_e(self) -> None:
        tree = ast.parse(INGRESS_MODULE.read_text(encoding="utf-8"),
                         filename=str(INGRESS_MODULE))
        called = {
            node.func.attr for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        for forbidden in (
            "process_ready_windows", "trigger_state", "capture_exp010_population",
            "capture_real_v2_post_trigger_population", "build",
        ):
            self.assertNotIn(forbidden, called)
        self.assertIn("serve", called)

    def test_no_authority_or_milvus_imports(self) -> None:
        for module in (INGRESS_MODULE, CONFIG_MODULE):
            with self.subTest(module=module.name):
                tree = ast.parse(module.read_text(encoding="utf-8"),
                                 filename=str(module))
                imported = {
                    node.module or "" for node in ast.walk(tree)
                    if isinstance(node, ast.ImportFrom)
                } | {
                    alias.name for node in ast.walk(tree)
                    if isinstance(node, ast.Import) for alias in node.names
                }
                forbidden = {
                    "canary_admission", "canary_approval", "canary_activation",
                    "canary_route_authority", "canary_routing",
                    "canary_live_runner", "canary_grant_store", "pymilvus",
                    "actuation", "milvus_actuation",
                }
                offending = {
                    item for item in imported
                    if any(item == n or item.endswith(f".{n}") for n in forbidden)
                }
                self.assertFalse(offending, offending)
                source = module.read_text(encoding="utf-8")
                self.assertNotIn("START_CANARY", source)
                self.assertNotIn("MilvusClient", source)

    def test_ingress_contains_no_workload_generator(self) -> None:
        tree = ast.parse(INGRESS_MODULE.read_text(encoding="utf-8"),
                         filename=str(INGRESS_MODULE))
        names = {
            node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
        } | {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
        for forbidden in (
            "standard_normal", "random", "Generator", "PCG64", "choice",
            "sample", "shuffle", "replay",
        ):
            self.assertNotIn(forbidden, names)

    def test_payload_surface_is_exactly_two_caller_fields(self) -> None:
        self.assertEqual(PAYLOAD_FIELDS, frozenset({"request_id", "query_vector"}))


if __name__ == "__main__":
    unittest.main()
