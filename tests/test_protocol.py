import unittest

import numpy as np

from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.oracle import OracleHit, OracleResult
from vdbench.protocol import (
    ExperimentSchedule,
    RepetitionSchedule,
    ScheduledConfiguration,
    deliberate_unreachable_probe,
    run_protocol,
)


class FakeBackend:
    def __init__(self, *, fail_hnsw: bool = False) -> None:
        self.fail_hnsw = fail_hnsw
        self.searches = []
        self.materialized = False

    def search(self, *, name, query, configuration):
        self.searches.append((name, configuration.key, tuple(query)))
        self.materialized = False
        if self.fail_hnsw and configuration.index_track is IndexTrack.HNSW:
            raise ConnectionError("synthetic unreachable endpoint")
        score = 0.25 if configuration.metric is Metric.L2 else 0.75
        response = (SearchHit(1, score),)
        self.materialized = True
        return response

    def index_identity(self, name, metric, track):
        return CollectionIdentity(
            name,
            metric.value,
            track.value,
            {
                "build_id": 7,
                "index_type": "HNSW",
                "metric_type": metric.value,
                "params": {"M": 16, "efConstruction": 200},
                "state": "Finished",
            },
        )


def configurations():
    return tuple(
        SearchConfiguration(
            metric=metric,
            threshold_label="target-005",
            radius=1.0 if metric is Metric.L2 else 0.5,
            index_track=track,
            ef=100 if track is IndexTrack.HNSW else None,
        )
        for metric in Metric
        for track in IndexTrack
    )


def schedule_for(values):
    warmup = tuple(ScheduledConfiguration(value.key, (0,), 11) for value in values)
    measured = tuple(ScheduledConfiguration(value.key, (0,), 12) for value in values)
    return ExperimentSchedule(10, warmup, (RepetitionSchedule(0, 13, measured),))


def references_for(values):
    return {
        (value.key, 0): OracleResult(
            hits=(OracleHit(1, 0.25 if value.metric is Metric.L2 else 0.75),),
            full_count=1,
            capped=False,
        )
        for value in values
    }


class ProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.values = configurations()
        self.names = {
            (metric, track): f"unit_{metric.value.lower()}_{track.value.lower()}"
            for metric in Metric
            for track in IndexTrack
        }
        self.calibration = np.asarray([[0.0, 0.0]], dtype="<f4")
        self.measured = np.asarray([[0.0, 0.0]], dtype="<f4")

    def test_timing_stops_after_materialization_and_writes_after_timer(self) -> None:
        backend = FakeBackend()
        timestamps = iter((100, 175, 200, 280, 300, 390, 400, 500))
        clock_calls = 0

        def clock():
            nonlocal clock_calls
            clock_calls += 1
            if clock_calls % 2:
                backend.materialized = False
            else:
                self.assertTrue(backend.materialized)
            return next(timestamps)

        written = []
        records = run_protocol(
            backend=backend,
            configurations=self.values,
            schedule=schedule_for(self.values),
            collection_names=self.names,
            calibration_queries=self.calibration,
            measured_queries=self.measured,
            references=references_for(self.values),
            sink=written.append,
            clock_ns=clock,
        )
        self.assertEqual([record["latency_ns"] for record in records], [75, 80, 90, 100])
        self.assertTrue(all(record["status"] == "success" for record in records))
        # Four queries, two HNSW segment identity records, and two final identities.
        self.assertEqual(len(written), 8)

    def test_unreachable_probe_records_expected_failure_and_never_success(self) -> None:
        written = []
        deliberate_unreachable_probe(
            lambda: (_ for _ in ()).throw(
                ConnectionError("synthetic unreachable endpoint")
            ),
            written.append,
        )
        self.assertEqual(len(written), 1)
        self.assertEqual(written[0]["status"], "expected_failure")
        self.assertEqual(written[0]["error_type"], "ConnectionError")
        self.assertNotEqual(written[0]["status"], "success")


if __name__ == "__main__":
    unittest.main()
