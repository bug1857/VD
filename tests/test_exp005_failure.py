"""EXP-005 H4: offline failure fixtures stop before detector, policy, actuation."""

from __future__ import annotations

from dataclasses import replace
import math
import unittest

from vdbench.config import IndexTrack, Metric
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from vdbench.oracle import OracleHit, OracleResult
from vdbench.shadow_window import (
    AssembledShadowWindow,
    PersistedShadowTraceEnvelope,
    assemble_shadow_window,
    hash_shadow_audit_trace,
)


class _DownstreamRecorder:
    """Fixture-only detector/policy/actuation continuation recorder."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def detector(self, _: AssembledShadowWindow) -> None:
        self.calls.append("detector")

    def policy(self) -> None:
        self.calls.append("policy")

    def actuation(self) -> None:
        self.calls.append("actuation")


def _identity(track: IndexTrack) -> ShadowIdentityEvidence:
    description: dict[str, object] = {
        "index_type": track.value,
        "metric_type": Metric.L2.value,
    }
    if track is IndexTrack.HNSW:
        description.update({"M": "16", "efConstruction": "200"})
    snapshot = CollectionIdentity(
        collection_name=f"exp005_failure_{track.value.lower()}",
        metric=Metric.L2.value,
        index_track=track.value,
        description=description,
    )
    capture = ShadowAuditStageEvidence(stage=f"{track.value}_IDENTITY", success=True)
    return ShadowIdentityEvidence(
        track=track,
        expected_binding_id=f"{track.value.lower()}-binding-v1",
        pre_snapshot=snapshot,
        post_snapshot=snapshot,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=capture,
        post_capture=capture,
    )


def _query(query_id: int) -> ShadowQueryAuditTrace:
    oracle = OracleResult(
        hits=(OracleHit(id=query_id, score=1.0),), full_count=1, capped=False
    )
    hit = SearchHit(id=query_id, score=1.0)
    return ShadowQueryAuditTrace(
        query_id=query_id,
        query_vector=(float(query_id), 1.0),
        threshold_radius=2.0,
        range_filter=0.0,
        limit=100,
        oracle_result=oracle,
        exact_cardinality=1,
        flat_hits=(hit,),
        sentinel_hits=(hit,),
        sentinel_recall=1.0,
        stages=(
            ShadowAuditStageEvidence(stage="FLAT", success=True, oracle_agreement=True),
            ShadowAuditStageEvidence(stage="SENTINEL_HNSW", success=True),
        ),
    )


def _trace(sequence_index: int) -> ShadowAuditTrace:
    start = sequence_index * 50
    return ShadowAuditTrace(
        metric=Metric.L2,
        threshold_stratum="target-075",
        candidate_ef=400,
        last_known_good_ef=200,
        sentinel_ef=100,
        configuration_identity="exp005-failure-config-v1",
        data_identity="exp005-failure-data-v1",
        flat_identity=_identity(IndexTrack.FLAT),
        hnsw_identity=_identity(IndexTrack.HNSW),
        queries=tuple(_query(query_id) for query_id in range(start, start + 50)),
        complete=True,
    )


def _envelopes() -> list[PersistedShadowTraceEnvelope]:
    result: list[PersistedShadowTraceEnvelope] = []
    for sequence_index in range(4):
        trace = _trace(sequence_index)
        result.append(
            PersistedShadowTraceEnvelope(
                trace_id=f"failure-trace-{sequence_index}",
                captured_at_utc=f"2026-08-03T12:00:0{sequence_index}Z",
                sequence_index=sequence_index,
                declared_observation_count=50,
                expected_trace_sha256=hash_shadow_audit_trace(trace),
                trace=trace,
            )
        )
    return result


def _with_trace(
    envelopes: list[PersistedShadowTraceEnvelope],
    index: int,
    trace: ShadowAuditTrace,
) -> tuple[PersistedShadowTraceEnvelope, ...]:
    envelopes[index] = replace(
        envelopes[index],
        trace=trace,
        expected_trace_sha256=hash_shadow_audit_trace(trace),
    )
    return tuple(envelopes)


class Exp005FailureTests(unittest.TestCase):
    """Each H4 fixture must fail closed before any downstream phase begins."""

    def _assert_fails_closed_before_downstream(
        self,
        envelopes: tuple[PersistedShadowTraceEnvelope, ...],
        expected_reason: str,
    ) -> None:
        """Model the only permitted continuation: complete assembly first."""

        recorder = _DownstreamRecorder()
        result = assemble_shadow_window(window_id="exp005-h4-window", envelopes=envelopes)
        if result.complete:
            recorder.detector(result)
            recorder.policy()
            recorder.actuation()
        self.assertFalse(result.complete, result.reason_codes)
        self.assertIn(expected_reason, result.reason_codes, result.reason_codes)
        self.assertEqual(result.query_records, ())
        self.assertIsNone(result.manifest_sha256)
        self.assertEqual(recorder.calls, [])

    def test_duplicate_trace_id_stops_downstream(self) -> None:
        values = _envelopes()
        values[3] = replace(values[3], trace_id=values[0].trace_id)
        self._assert_fails_closed_before_downstream(tuple(values), "TRACE_ID_DUPLICATE")

    def test_incomplete_trace_stops_downstream(self) -> None:
        values = _envelopes()
        self._assert_fails_closed_before_downstream(
            _with_trace(values, 0, replace(values[0].trace, complete=False)),
            "TRACE_INCOMPLETE",
        )

    def test_mismatched_metric_stops_downstream(self) -> None:
        values = _envelopes()
        self._assert_fails_closed_before_downstream(
            _with_trace(values, 3, replace(values[3].trace, metric=Metric.COSINE)),
            "METRIC_MISMATCH",
        )

    def test_failed_query_stops_downstream(self) -> None:
        values = _envelopes()
        bad = replace(
            values[0].trace.queries[0],
            stages=(ShadowAuditStageEvidence(stage="FLAT", success=False),),
        )
        trace = replace(values[0].trace, queries=(bad, *values[0].trace.queries[1:]))
        self._assert_fails_closed_before_downstream(
            _with_trace(values, 0, trace), "STAGE_FAILED"
        )

    def test_timed_out_query_stops_downstream(self) -> None:
        values = _envelopes()
        bad = replace(
            values[0].trace.queries[0],
            stages=(ShadowAuditStageEvidence(stage="FLAT", success=True, timed_out=True),),
        )
        trace = replace(values[0].trace, queries=(bad, *values[0].trace.queries[1:]))
        self._assert_fails_closed_before_downstream(
            _with_trace(values, 0, trace), "STAGE_TIMEOUT"
        )

    def test_nonfinite_query_value_stops_downstream(self) -> None:
        values = _envelopes()
        bad = replace(values[0].trace.queries[0], query_vector=(math.nan, 1.0))
        trace = replace(values[0].trace, queries=(bad, *values[0].trace.queries[1:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256="0" * 64)
        self._assert_fails_closed_before_downstream(tuple(values), "NONFINITE_VALUE")

    def test_identity_invalid_trace_stops_downstream(self) -> None:
        values = _envelopes()
        trace = replace(
            values[0].trace,
            flat_identity=replace(values[0].trace.flat_identity, pre_binding_match=False),
        )
        self._assert_fails_closed_before_downstream(
            _with_trace(values, 0, trace), "INDEX_IDENTITY_INVALID"
        )

    def test_tampered_trace_payload_stops_downstream(self) -> None:
        values = _envelopes()
        bad = replace(values[0].trace.queries[0], query_vector=(999.0, 1.0))
        tampered = replace(
            values[0].trace, queries=(bad, *values[0].trace.queries[1:])
        )
        values[0] = replace(values[0], trace=tampered)
        self._assert_fails_closed_before_downstream(
            tuple(values), "TRACE_PAYLOAD_SHA256_MISMATCH"
        )


if __name__ == "__main__":
    unittest.main()
