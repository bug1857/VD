from __future__ import annotations

import ast
from pathlib import Path
import unittest

from vdbench.config import IndexTrack, Metric
from vdbench.host_observation import (
    BoundedHostObservationRecorder,
    RangeQueryRequest,
    ReferenceRangeGateway,
)
from vdbench.milvus import CollectionIdentity
from vdbench.milvus_actuation import CollectionIdentityBinding, StackHealth
from vdbench.shadow_event_types import MonitorStreamKey

from vdbench.milvus_serving import (  # type: ignore[import-not-found]
    HostServingPlan,
    MilvusRangeServingExecutor,
)


REPOSITORY = Path(__file__).parents[1]
MODULE_PATH = REPOSITORY / "src" / "vdbench" / "milvus_serving.py"
METRIC = Metric.L2
STRATUM = "target-025"
CONFIGURATION_ID = "serving-config-v1"
DATA_ID = "serving-data-v1"
FLAT_NAME = "serving_l2_flat"
HNSW_NAME = "serving_l2_hnsw"
FLAT_BINDING = "serving-flat-binding-v1"
HNSW_BINDING = "serving-hnsw-binding-v1"


def _description(track: IndexTrack) -> dict[str, object]:
    value: dict[str, object] = {
        "index_name": "vector_index",
        "index_type": track.value,
        "metric_type": METRIC.value,
        "state": "Finished",
    }
    if track is IndexTrack.HNSW:
        value.update({"M": "16", "efConstruction": "200"})
    return value


def _identity(track: IndexTrack) -> CollectionIdentity:
    return CollectionIdentity(
        FLAT_NAME if track is IndexTrack.FLAT else HNSW_NAME,
        METRIC.value,
        track.value,
        _description(track),
    )


class _Client:
    def __init__(self) -> None:
        self.loaded = True
        self.identity_matches = True
        self.search_exception: BaseException | None = None
        self.search_calls: list[dict[str, object]] = []
        self.load_calls: list[str] = []
        self.describe_calls: list[str] = []

    def get_load_state(self, *, collection_name: str) -> object:
        self.load_calls.append(collection_name)
        return {"state": "Loaded" if self.loaded else "NotLoaded"}

    def describe_index(self, *, collection_name: str, index_name: str) -> object:
        self.describe_calls.append(collection_name)
        track = IndexTrack.FLAT if collection_name == FLAT_NAME else IndexTrack.HNSW
        value = _description(track)
        if not self.identity_matches:
            value["unexpected"] = "different"
        return value

    def search(self, **kwargs: object) -> object:
        self.search_calls.append(kwargs)
        if self.search_exception is not None:
            raise self.search_exception
        return [[{"id": 7, "distance": 0.25}, {"id": 8, "distance": 0.5}]]


class _Health:
    def __init__(self) -> None:
        self.result = StackHealth(True, True, "healthy")
        self.calls = 0

    def check(self) -> StackHealth:
        self.calls += 1
        return self.result


class _Clock:
    def __init__(self) -> None:
        self.values = iter((100, 1_100_000, 2_000_000, 3_000_000))

    def __call__(self) -> int:
        return next(self.values)


def _key() -> MonitorStreamKey:
    return MonitorStreamKey(
        stream_id="serving-l2-target025",
        metric=METRIC,
        threshold_stratum=STRATUM,
        configuration_identity=CONFIGURATION_ID,
        data_identity=DATA_ID,
        flat_binding_id=FLAT_BINDING,
        hnsw_binding_id=HNSW_BINDING,
    )


def _plan() -> HostServingPlan:
    return HostServingPlan(
        flat_collection_name=FLAT_NAME,
        hnsw_collection_name=HNSW_NAME,
        flat_binding=CollectionIdentityBinding(FLAT_BINDING, _identity(IndexTrack.FLAT)),
        hnsw_binding=CollectionIdentityBinding(HNSW_BINDING, _identity(IndexTrack.HNSW)),
        threshold_radius=2.0,
        dimensions=2,
        allowed_served_efs=frozenset({400, 800}),
    )


def _request(**changes: object) -> RangeQueryRequest:
    values: dict[str, object] = {
        "request_id": 1,
        "stream_key": _key(),
        "query_vector": (0.25, 0.5),
        "threshold_radius": 2.0,
        "range_filter": 0.0,
        "limit": 100,
        "served_ef": 400,
    }
    values.update(changes)
    return RangeQueryRequest(**values)  # type: ignore[arg-type]


def _executor(client: _Client, health: _Health) -> MilvusRangeServingExecutor:
    return MilvusRangeServingExecutor(
        client=client,
        plans={_key(): _plan()},
        stack_health_probe=health,
        clock_ns=_Clock(),
    )


def _admitted_executor(client: _Client, health: _Health) -> MilvusRangeServingExecutor:
    executor = _executor(client, health)
    assert executor.preflight().complete
    return executor


class MilvusRangeServingExecutorTests(unittest.TestCase):
    def test_preflight_then_execute_uses_one_hnsw_search_without_control_plane_calls(self) -> None:
        client = _Client()
        health = _Health()
        executor = _executor(client, health)

        preflight = executor.preflight()
        before = (len(client.load_calls), len(client.describe_calls), health.calls)
        outcome = executor.execute(_request())

        self.assertTrue(preflight.complete)
        self.assertEqual(preflight.checked_stream_count, 1)
        self.assertEqual(before, (2, 2, 1))
        self.assertTrue(outcome.success)
        self.assertFalse(outcome.timed_out)
        self.assertEqual(outcome.result_count, 2)
        self.assertEqual(outcome.latency_ms, 1.0999)
        self.assertIsNone(outcome.error_code)
        self.assertEqual((len(client.load_calls), len(client.describe_calls), health.calls), before)
        self.assertEqual(len(client.search_calls), 1)
        call = client.search_calls[0]
        self.assertEqual(call["collection_name"], HNSW_NAME)
        self.assertEqual(call["search_params"], {
            "metric_type": "L2",
            "params": {"radius": 2.0, "range_filter": 0.0, "ef": 400},
        })

    def test_preflight_health_load_and_identity_fail_closed(self) -> None:
        for failure in ("health", "load", "identity"):
            with self.subTest(failure=failure):
                client = _Client()
                health = _Health()
                if failure == "health":
                    health.result = StackHealth(False, True, "unhealthy")
                elif failure == "load":
                    client.loaded = False
                else:
                    client.identity_matches = False

                result = _executor(client, health).preflight()

                self.assertFalse(result.complete)
                self.assertEqual(result.checked_stream_count, 0)
                self.assertNotEqual(result.reason_codes, ())
                self.assertEqual(client.search_calls, [])

    def test_preflight_is_mandatory_and_failed_recheck_revokes_admission(self) -> None:
        client = _Client()
        health = _Health()
        executor = _executor(client, health)

        before = executor.execute(_request())
        self.assertFalse(before.success)
        self.assertEqual(before.error_code, "SERVING_PREFLIGHT_REQUIRED")
        self.assertEqual(client.search_calls, [])

        self.assertTrue(executor.preflight().complete)
        health.result = StackHealth(False, True, "unhealthy")
        self.assertFalse(executor.preflight().complete)
        after = executor.execute(_request())
        self.assertFalse(after.success)
        self.assertEqual(after.error_code, "SERVING_PREFLIGHT_REQUIRED")
        self.assertEqual(client.search_calls, [])

    def test_invalid_request_is_rejected_without_search(self) -> None:
        for field, replacement in (
            ("query_vector", (0.25, 0.5, 0.75)),
            ("threshold_radius", 1.0),
            ("range_filter", 0.5),
            ("limit", 99),
            ("served_ef", 200),
            ("stream_key", MonitorStreamKey(
                stream_id="other", metric=METRIC, threshold_stratum=STRATUM,
                configuration_identity=CONFIGURATION_ID, data_identity=DATA_ID,
                flat_binding_id=FLAT_BINDING, hnsw_binding_id=HNSW_BINDING,
            )),
        ):
            with self.subTest(field=field):
                client = _Client()
                health = _Health()
                outcome = _admitted_executor(client, health).execute(
                    _request(**{field: replacement})
                )
                self.assertFalse(outcome.success)
                self.assertFalse(outcome.timed_out)
                self.assertEqual(outcome.result_count, 0)
                self.assertIsNotNone(outcome.error_code)
                self.assertEqual(client.search_calls, [])

    def test_timeout_and_backend_failure_return_non_sensitive_outcomes(self) -> None:
        client = _Client()
        client.search_exception = TimeoutError("database hostname must not leak")
        timeout = _admitted_executor(client, _Health()).execute(_request())
        self.assertFalse(timeout.success)
        self.assertTrue(timeout.timed_out)
        self.assertEqual(timeout.error_code, "MILVUS_SEARCH_TIMEOUT")
        self.assertEqual(len(client.search_calls), 1)

    def test_reference_gateway_serves_once_then_records_the_compact_outcome(self) -> None:
        client = _Client()
        executor = _admitted_executor(client, _Health())
        recorder = BoundedHostObservationRecorder(max_pending_observations=1)
        gateway = ReferenceRangeGateway(
            serving_executor=executor,
            recorder=recorder,
            clock=lambda: "2026-08-03T12:00:00Z",
        )

        result = gateway.execute(_request())
        recorded = recorder.drain(limit=1)

        self.assertTrue(result.served_outcome.success)
        self.assertEqual(result.observation_receipt.status.value, "ACCEPTED")
        self.assertEqual(len(client.search_calls), 1)
        self.assertEqual(len(recorded), 1)
        self.assertEqual(recorded[0].served_outcome, result.served_outcome)

        client = _Client()
        client.search_exception = RuntimeError("internal server detail")
        failed = _admitted_executor(client, _Health()).execute(_request())
        self.assertFalse(failed.success)
        self.assertFalse(failed.timed_out)
        self.assertEqual(failed.error_code, "MILVUS_SEARCH_FAILED")
        self.assertEqual(len(client.search_calls), 1)

    def test_float32_overflow_is_rejected_before_search(self) -> None:
        client = _Client()
        outcome = _admitted_executor(client, _Health()).execute(
            _request(query_vector=(1e100, 0.5))
        )
        self.assertFalse(outcome.success)
        self.assertEqual(outcome.error_code, "QUERY_VECTOR_OUT_OF_RANGE")
        self.assertEqual(client.search_calls, [])

    def test_source_has_no_mutation_or_policy_actuation_dependency(self) -> None:
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        }
        attributes = {
            node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
        }
        self.assertFalse(
            any("pymilvus" in item.lower() or "policy" in item or "actuation" in item for item in imports)
        )
        self.assertFalse({
            "create_collection", "drop_collection", "create_index", "load_collection",
            "insert", "delete", "start_canary", "stop_candidate",
            "restore_last_known_good", "verify_restoration",
        } & attributes)


if __name__ == "__main__":
    unittest.main()
