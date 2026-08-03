"""TDD coverage for EXP-005's four-trace shadow-window boundary."""

from __future__ import annotations

from dataclasses import replace
import math
import unittest

from vdbench.config import IndexTrack, Metric
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    CollectionIdentityBinding,
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
    verify_persisted_assembled_window,
)


def _identity(track: IndexTrack) -> CollectionIdentity:
    description: dict[str, object] = {
        "index_type": track.value,
        "metric_type": Metric.L2.value,
    }
    if track is IndexTrack.HNSW:
        description.update({"M": "16", "efConstruction": "200"})
    return CollectionIdentity(
        collection_name=f"exp005_l2_{track.value.lower()}",
        metric=Metric.L2.value,
        index_track=track.value,
        description=description,
    )


def _identity_evidence(track: IndexTrack) -> ShadowIdentityEvidence:
    snapshot = _identity(track)
    stage = ShadowAuditStageEvidence(stage=f"{track.value}_IDENTITY", success=True)
    return ShadowIdentityEvidence(
        track=track,
        expected_binding_id=f"{track.value.lower()}-binding-v1",
        pre_snapshot=snapshot,
        post_snapshot=snapshot,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=stage,
        post_capture=stage,
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
        configuration_identity="config-v1",
        data_identity="dataset-v1",
        flat_identity=_identity_evidence(IndexTrack.FLAT),
        hnsw_identity=_identity_evidence(IndexTrack.HNSW),
        queries=tuple(_query(query_id) for query_id in range(start, start + 50)),
        complete=True,
    )


def _envelope(sequence_index: int) -> PersistedShadowTraceEnvelope:
    trace = _trace(sequence_index)
    return PersistedShadowTraceEnvelope(
        trace_id=f"trace-{sequence_index}",
        captured_at_utc=f"2026-08-03T12:00:0{sequence_index}Z",
        sequence_index=sequence_index,
        declared_observation_count=50,
        expected_trace_sha256=hash_shadow_audit_trace(trace),
        trace=trace,
    )


def _envelopes() -> tuple[PersistedShadowTraceEnvelope, ...]:
    return tuple(_envelope(index) for index in range(4))


class ShadowWindowAssemblyTests(unittest.TestCase):
    maxDiff = None

    def assert_incomplete(self, result: AssembledShadowWindow, code: str) -> None:
        self.assertFalse(result.complete, result)
        self.assertIn(code, result.reason_codes, result.reason_codes)
        self.assertEqual(result.query_records, ())
        self.assertIsNone(result.manifest_sha256)

    def test_valid_assembly_is_ordered_and_deterministic(self) -> None:
        result = assemble_shadow_window(window_id="window-001", envelopes=_envelopes())
        again = assemble_shadow_window(window_id="window-001", envelopes=_envelopes())
        self.assertTrue(result.complete, result.reason_codes)
        self.assertEqual(result.manifest_sha256, again.manifest_sha256)
        self.assertEqual(tuple(item.query_id for item in result.query_records), tuple(range(200)))
        self.assertEqual(result.metric, Metric.L2)
        self.assertEqual(result.threshold_stratum, "target-075")

    def test_fewer_than_four_envelopes_fails_closed(self) -> None:
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=_envelopes()[:3]),
            "ENVELOPE_COUNT_INVALID",
        )

    def test_more_than_four_envelopes_fails_closed(self) -> None:
        extra = _envelope(0)
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=(*_envelopes(), extra)),
            "ENVELOPE_COUNT_INVALID",
        )

    def test_sequence_indexes_must_be_exactly_zero_through_three(self) -> None:
        values = list(_envelopes())
        values[3] = replace(values[3], sequence_index=4)
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "SEQUENCE_INDEX_SET_INVALID",
        )

    def test_envelope_input_order_must_match_sequence_order(self) -> None:
        values = list(_envelopes())
        values[0], values[1] = values[1], values[0]
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "MANIFEST_ORDER_MISMATCH",
        )

    def test_duplicate_trace_id_fails_closed(self) -> None:
        values = list(_envelopes())
        values[3] = replace(values[3], trace_id=values[0].trace_id)
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TRACE_ID_DUPLICATE",
        )

    def test_nfc_colliding_trace_ids_fail_closed(self) -> None:
        values = list(_envelopes())
        values[0] = replace(values[0], trace_id="e\u0301")
        values[1] = replace(values[1], trace_id="é")
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TRACE_ID_NORMALIZATION_COLLISION",
        )

    def test_invalid_calendar_timestamp_fails_closed(self) -> None:
        values = list(_envelopes())
        values[0] = replace(values[0], captured_at_utc="2026-13-03T12:00:00Z")
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TIMESTAMP_INVALID",
        )

    def test_equal_timestamps_fail_closed(self) -> None:
        values = list(_envelopes())
        values[1] = replace(values[1], captured_at_utc=values[0].captured_at_utc)
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TIMESTAMP_NOT_STRICTLY_INCREASING",
        )

    def test_nonmonotonic_timestamps_fail_closed(self) -> None:
        values = list(_envelopes())
        values[3] = replace(values[3], captured_at_utc="2026-08-03T12:00:01Z")
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TIMESTAMP_NOT_STRICTLY_INCREASING",
        )

    def test_declared_observation_count_mismatch_fails_closed(self) -> None:
        values = list(_envelopes())
        values[0] = replace(values[0], declared_observation_count=49)
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "DECLARED_OBSERVATION_COUNT_INVALID",
        )

    def test_actual_observation_count_mismatch_fails_closed(self) -> None:
        values = list(_envelopes())
        short = replace(values[0].trace, queries=values[0].trace.queries[:-1])
        values[0] = replace(
            values[0],
            trace=short,
            expected_trace_sha256=hash_shadow_audit_trace(short),
        )
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "ACTUAL_OBSERVATION_COUNT_INVALID",
        )

    def test_malformed_trace_checksum_fails_closed(self) -> None:
        values = list(_envelopes())
        values[0] = replace(values[0], expected_trace_sha256="ABC")
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TRACE_SHA256_FORMAT_INVALID",
        )

    def test_uppercase_trace_checksum_fails_closed(self) -> None:
        values = list(_envelopes())
        values[0] = replace(
            values[0], expected_trace_sha256=values[0].expected_trace_sha256.upper()
        )
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TRACE_SHA256_FORMAT_INVALID",
        )

    def test_trace_payload_checksum_mismatch_fails_closed(self) -> None:
        values = list(_envelopes())
        values[0] = replace(values[0], expected_trace_sha256="0" * 64)
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TRACE_PAYLOAD_SHA256_MISMATCH",
        )

    def test_duplicate_trace_payload_fails_closed(self) -> None:
        values = list(_envelopes())
        copied = replace(values[0], trace_id="trace-copy", sequence_index=3)
        values[3] = copied
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TRACE_PAYLOAD_DUPLICATE",
        )

    def test_incomplete_trace_fails_closed(self) -> None:
        values = list(_envelopes())
        incomplete = replace(values[0].trace, complete=False, reason_codes=("SOURCE_FAILED",))
        values[0] = replace(
            values[0],
            trace=incomplete,
            expected_trace_sha256=hash_shadow_audit_trace(incomplete),
        )
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TRACE_INCOMPLETE",
        )

    def test_missing_trace_data_fails_closed(self) -> None:
        values = list(_envelopes())
        values[0] = replace(values[0], trace=None)  # type: ignore[arg-type]
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TRACE_MISSING",
        )

    def test_failed_stage_fails_closed(self) -> None:
        values = list(_envelopes())
        bad_query = replace(
            values[0].trace.queries[0],
            stages=(ShadowAuditStageEvidence(stage="FLAT", success=False),),
        )
        trace = replace(values[0].trace, queries=(bad_query, *values[0].trace.queries[1:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "STAGE_FAILED",
        )

    def test_timeout_stage_fails_closed(self) -> None:
        values = list(_envelopes())
        bad_query = replace(
            values[0].trace.queries[0],
            stages=(ShadowAuditStageEvidence(stage="FLAT", success=True, timed_out=True),),
        )
        trace = replace(values[0].trace, queries=(bad_query, *values[0].trace.queries[1:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "STAGE_TIMEOUT",
        )

    def test_threshold_violation_fails_closed(self) -> None:
        values = list(_envelopes())
        bad_query = replace(
            values[0].trace.queries[0],
            stages=(ShadowAuditStageEvidence(stage="FLAT", success=True, threshold_violation_count=1),),
        )
        trace = replace(values[0].trace, queries=(bad_query, *values[0].trace.queries[1:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "THRESHOLD_VIOLATION",
        )

    def test_nonfinite_query_value_fails_closed(self) -> None:
        values = list(_envelopes())
        bad_query = replace(values[0].trace.queries[0], query_vector=(math.nan, 1.0))
        trace = replace(values[0].trace, queries=(bad_query, *values[0].trace.queries[1:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256="0" * 64)
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "NONFINITE_VALUE",
        )

    def test_incompatible_metric_fails_closed(self) -> None:
        values = list(_envelopes())
        trace = replace(values[3].trace, metric=Metric.COSINE)
        values[3] = replace(values[3], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "METRIC_MISMATCH",
        )

    def test_incompatible_threshold_stratum_fails_closed(self) -> None:
        values = list(_envelopes())
        trace = replace(values[3].trace, threshold_stratum="target-025")
        values[3] = replace(values[3], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "THRESHOLD_STRATUM_MISMATCH",
        )

    def test_configuration_identity_mismatch_fails_closed(self) -> None:
        values = list(_envelopes())
        trace = replace(values[3].trace, configuration_identity="config-v2")
        values[3] = replace(values[3], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "CONFIGURATION_IDENTITY_MISMATCH",
        )

    def test_data_identity_mismatch_fails_closed(self) -> None:
        values = list(_envelopes())
        trace = replace(values[3].trace, data_identity="dataset-v2")
        values[3] = replace(values[3], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "DATA_IDENTITY_MISMATCH",
        )

    def test_index_identity_mismatch_fails_closed(self) -> None:
        values = list(_envelopes())
        changed = replace(
            values[3].trace.flat_identity,
            post_snapshot=replace(_identity(IndexTrack.FLAT), collection_name="other_flat"),
        )
        trace = replace(values[3].trace, flat_identity=changed)
        values[3] = replace(values[3], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "INDEX_IDENTITY_INVALID",
        )

    def test_inconsistent_limit_fails_closed(self) -> None:
        values = list(_envelopes())
        changed = replace(values[3].trace.queries[0], limit=99)
        trace = replace(values[3].trace, queries=(changed, *values[3].trace.queries[1:]))
        values[3] = replace(values[3], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "QUERY_CONFIGURATION_INCONSISTENT",
        )

    def test_inconsistent_threshold_radius_fails_closed(self) -> None:
        values = list(_envelopes())
        changed = replace(values[3].trace.queries[0], threshold_radius=3.0)
        trace = replace(values[3].trace, queries=(changed, *values[3].trace.queries[1:]))
        values[3] = replace(values[3], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "QUERY_CONFIGURATION_INCONSISTENT",
        )

    def test_inconsistent_range_filter_fails_closed(self) -> None:
        values = list(_envelopes())
        changed = replace(values[3].trace.queries[0], range_filter=0.5)
        trace = replace(values[3].trace, queries=(changed, *values[3].trace.queries[1:]))
        values[3] = replace(values[3], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "QUERY_CONFIGURATION_INCONSISTENT",
        )

    def test_audit_ef_configuration_mismatch_fails_closed(self) -> None:
        values = list(_envelopes())
        trace = replace(values[3].trace, candidate_ef=800)
        values[3] = replace(values[3], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "EF_MISMATCH",
        )

    def test_duplicate_query_id_within_trace_fails_closed(self) -> None:
        values = list(_envelopes())
        duplicated = replace(values[0].trace.queries[1], query_id=values[0].trace.queries[0].query_id)
        trace = replace(values[0].trace, queries=(values[0].trace.queries[0], duplicated, *values[0].trace.queries[2:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "QUERY_ID_DUPLICATE",
        )

    def test_duplicate_query_id_across_traces_fails_closed(self) -> None:
        values = list(_envelopes())
        duplicated = replace(values[1].trace.queries[0], query_id=values[0].trace.queries[0].query_id)
        trace = replace(values[1].trace, queries=(duplicated, *values[1].trace.queries[1:]))
        values[1] = replace(values[1], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "QUERY_ID_DUPLICATE",
        )

    def test_mixed_query_id_schema_fails_closed(self) -> None:
        values = list(_envelopes())
        changed = replace(values[3].trace.queries[0], query_id="150")
        trace = replace(values[3].trace, queries=(changed, *values[3].trace.queries[1:]))
        values[3] = replace(values[3], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "QUERY_ID_SCHEMA_MIXED",
        )

    def test_nfc_colliding_query_ids_fail_closed(self) -> None:
        values = list(_envelopes())
        first = replace(values[0].trace.queries[0], query_id="e\u0301")
        second = replace(values[0].trace.queries[1], query_id="é")
        trace = replace(values[0].trace, queries=(first, second, *values[0].trace.queries[2:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "QUERY_ID_NORMALIZATION_COLLISION",
        )

    def test_invalid_window_id_fails_closed(self) -> None:
        self.assert_incomplete(
            assemble_shadow_window(window_id=True, envelopes=_envelopes()),
            "WINDOW_ID_INVALID",
        )

    def test_missing_oracle_evidence_fails_closed(self) -> None:
        values = list(_envelopes())
        changed = replace(values[0].trace.queries[0], oracle_result=None)
        trace = replace(values[0].trace, queries=(changed, *values[0].trace.queries[1:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "QUERY_EVIDENCE_INCOMPLETE",
        )

    def test_missing_sentinel_evidence_fails_closed(self) -> None:
        values = list(_envelopes())
        changed = replace(values[0].trace.queries[0], sentinel_hits=None)
        trace = replace(values[0].trace, queries=(changed, *values[0].trace.queries[1:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "QUERY_EVIDENCE_INCOMPLETE",
        )

    def test_exact_cardinality_must_equal_oracle_full_count(self) -> None:
        values = list(_envelopes())
        changed = replace(values[0].trace.queries[0], exact_cardinality=2)
        trace = replace(values[0].trace, queries=(changed, *values[0].trace.queries[1:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "EXACT_CARDINALITY_MISMATCH",
        )

    def test_sentinel_recall_must_match_recomputed_value(self) -> None:
        values = list(_envelopes())
        changed = replace(values[0].trace.queries[0], sentinel_recall=0.5)
        trace = replace(values[0].trace, queries=(changed, *values[0].trace.queries[1:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "SENTINEL_RECALL_MISMATCH",
        )

    def test_flat_hits_must_match_capped_oracle_id_set(self) -> None:
        values = list(_envelopes())
        changed = replace(values[0].trace.queries[0], flat_hits=(SearchHit(id=999, score=1.0),))
        trace = replace(values[0].trace, queries=(changed, *values[0].trace.queries[1:]))
        values[0] = replace(values[0], trace=trace, expected_trace_sha256=hash_shadow_audit_trace(trace))
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "FLAT_ORACLE_ID_SET_MISMATCH",
        )

    def test_unsupported_canonical_payload_value_fails_closed(self) -> None:
        values = list(_envelopes())
        unsupported_identity = replace(
            _identity(IndexTrack.FLAT), description={"unsupported": object()}
        )
        changed_identity = replace(
            values[0].trace.flat_identity,
            pre_snapshot=unsupported_identity,
            post_snapshot=unsupported_identity,
        )
        trace = replace(values[0].trace, flat_identity=changed_identity)
        values[0] = replace(values[0], trace=trace, expected_trace_sha256="0" * 64)
        self.assert_incomplete(
            assemble_shadow_window(window_id=1, envelopes=tuple(values)),
            "TRACE_PAYLOAD_CANONICALIZATION_FAILED",
        )

    def test_persisted_aggregate_manifest_mismatch_fails_closed(self) -> None:
        assembled = assemble_shadow_window(window_id="window-001", envelopes=_envelopes())
        result = verify_persisted_assembled_window(
            assembled,
            expected_manifest_sha256="0" * 64,
        )
        self.assert_incomplete(result, "AGGREGATE_MANIFEST_SHA256_MISMATCH")

    def test_persisted_aggregate_manifest_reverification_succeeds(self) -> None:
        assembled = assemble_shadow_window(window_id="window-001", envelopes=_envelopes())
        verified = verify_persisted_assembled_window(
            assembled,
            expected_manifest_sha256=assembled.manifest_sha256,
        )
        self.assertTrue(verified.complete, verified.reason_codes)
        self.assertEqual(verified.manifest_sha256, assembled.manifest_sha256)


if __name__ == "__main__":
    unittest.main()
