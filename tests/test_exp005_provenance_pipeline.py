"""ADR-004 proof: persisted shadow evidence remains bound through dry-run.

This test intentionally uses twelve independently assembled synthetic traces:
one reference window and two current windows.  It exercises the real Stage 1
assembly, Stage 2 extraction, detector, policy, safe boundary, and durable
audit persistence without importing PyMilvus or invoking any client method.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from vdbench.actuation import ActuationContext, ActuationOutcome, SafeActuationBoundary
from vdbench.actuation_persistence import JsonlAuditSink
from vdbench.config import IndexTrack, Metric
from vdbench.drift import DetectorState, evaluate_drift_decision
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from vdbench.oracle import OracleHit, OracleResult
from vdbench.policy import (
    PolicyAction,
    PolicyMode,
    PreActionSafety,
    QualificationWindow,
    ResponseEstimate,
    qualify_last_known_good,
    evaluate_tuning_policy,
)
from vdbench.shadow_extraction import extract_window_evidence
from vdbench.shadow_window import (
    PersistedShadowTraceEnvelope,
    assemble_shadow_window,
    hash_shadow_audit_trace,
)


CONFIGURATION_ID = "exp005-config-v1"
DATA_ID = "exp005-data-v1"
FLAT_BINDING_ID = "exp005-flat-v1"
HNSW_BINDING_ID = "exp005-hnsw-v1"
STRATUM = "target-025"
DETECTOR_SEED = 20260803


class _NoClient:
    """Records any accidental actuation call; the stationary path must use none."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str):
        self.calls.append(name)
        raise AssertionError(f"non-actuating pipeline called client.{name}")


class _Controller:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def disable_automatic_actions(self, *, audit_id: str, reason: str) -> None:
        self.calls.append((audit_id, reason))


def _identity(track: IndexTrack) -> ShadowIdentityEvidence:
    description: dict[str, object] = {
        "index_type": track.value,
        "metric_type": Metric.L2.value,
    }
    if track is IndexTrack.HNSW:
        description.update({"M": "16", "efConstruction": "200"})
    snapshot = CollectionIdentity(
        collection_name=f"exp005_{track.value.lower()}",
        metric=Metric.L2.value,
        index_track=track.value,
        description=description,
    )
    capture = ShadowAuditStageEvidence(
        stage=f"{track.value}_IDENTITY", success=True
    )
    return ShadowIdentityEvidence(
        track=track,
        expected_binding_id=(
            FLAT_BINDING_ID if track is IndexTrack.FLAT else HNSW_BINDING_ID
        ),
        pre_snapshot=snapshot,
        post_snapshot=snapshot,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=capture,
        post_capture=capture,
    )


def _record(query_id: int) -> ShadowQueryAuditTrace:
    oracle = OracleResult(
        hits=(OracleHit(query_id, 1.0),), full_count=1, capped=False
    )
    hit = SearchHit(query_id, 1.0)
    return ShadowQueryAuditTrace(
        query_id=query_id,
        query_vector=(float(query_id + 1), 1.0),
        threshold_radius=2.0,
        range_filter=0.0,
        limit=100,
        oracle_result=oracle,
        exact_cardinality=1,
        flat_hits=(hit,),
        sentinel_hits=(hit,),
        sentinel_recall=1.0,
        stages=(
            ShadowAuditStageEvidence("ORACLE", success=True),
            ShadowAuditStageEvidence("FLAT", success=True, oracle_agreement=True),
            ShadowAuditStageEvidence("SENTINEL_HNSW", success=True),
        ),
    )


def _assembled_window(window_id: str):
    envelopes: list[PersistedShadowTraceEnvelope] = []
    for sequence_index in range(4):
        trace = ShadowAuditTrace(
            metric=Metric.L2,
            threshold_stratum=STRATUM,
            candidate_ef=400,
            last_known_good_ef=200,
            sentinel_ef=100,
            configuration_identity=CONFIGURATION_ID,
            data_identity=DATA_ID,
            flat_identity=_identity(IndexTrack.FLAT),
            hnsw_identity=_identity(IndexTrack.HNSW),
            queries=tuple(
                _record(query_id)
                for query_id in range(sequence_index * 50, (sequence_index + 1) * 50)
            ),
            complete=True,
        )
        envelopes.append(
            PersistedShadowTraceEnvelope(
                trace_id=f"{window_id}-trace-{sequence_index}",
                captured_at_utc=f"2026-08-03T12:00:0{sequence_index}Z",
                sequence_index=sequence_index,
                declared_observation_count=50,
                expected_trace_sha256=hash_shadow_audit_trace(trace),
                trace=trace,
            )
        )
    assembled = assemble_shadow_window(window_id=window_id, envelopes=tuple(envelopes))
    assert assembled.complete, assembled.reason_codes
    return assembled


def _response_estimates() -> dict[int, ResponseEstimate]:
    return {
        400: ResponseEstimate(
            metric=Metric.L2,
            threshold_stratum=STRATUM,
            ef=400,
            mean_recall=0.97,
            recall_lower_bound_95=0.96,
            p95_latency_ms=4.0,
            latency_upper_bound_95_ms=4.5,
            validated_model=True,
            provenance="exp005-test-response-model-v1",
        ),
        800: ResponseEstimate(
            metric=Metric.L2,
            threshold_stratum=STRATUM,
            ef=800,
            mean_recall=0.98,
            recall_lower_bound_95=0.97,
            p95_latency_ms=5.0,
            latency_upper_bound_95_ms=5.5,
            validated_model=True,
            provenance="exp005-test-response-model-v1",
        ),
    }


def _qualification(sequence_number: int) -> QualificationWindow:
    return QualificationWindow(
        window_id=f"qualification-{sequence_number}",
        sequence_number=sequence_number,
        metric=Metric.L2,
        threshold_stratum=STRATUM,
        ef=400,
        mean_recall=0.97,
        recall_lower_bound_95=0.96,
        p95_latency_ms=4.0,
        latency_upper_bound_95_ms=4.5,
        configuration_identity=CONFIGURATION_ID,
        index_identity=HNSW_BINDING_ID,
        data_identity=DATA_ID,
    )


class Exp005ProvenancePipelineTests(unittest.TestCase):
    def test_twelve_traces_flow_to_durable_dry_run_noop_without_client_calls(self) -> None:
        reference = _assembled_window("reference")
        current_one = _assembled_window("current-one")
        current_two = _assembled_window("current-two")
        previous = extract_window_evidence(
            reference_window=reference,
            current_window=current_one,
            detector_seed=DETECTOR_SEED,
            metric=Metric.L2,
        )
        current = extract_window_evidence(
            reference_window=reference,
            current_window=current_two,
            detector_seed=DETECTOR_SEED,
            metric=Metric.L2,
        )

        self.assertTrue(previous.complete, previous.reason_codes)
        self.assertTrue(current.complete, current.reason_codes)
        self.assertIsNotNone(previous.provenance)
        self.assertIsNotNone(current.provenance)
        detector = evaluate_drift_decision(previous, current)
        self.assertEqual(detector.state, DetectorState.NO_DRIFT)
        self.assertEqual(detector.evidence_provenance, current.provenance)

        last_known_good = qualify_last_known_good(
            (_qualification(10), _qualification(11)), audit_id="exp005-qualification"
        )
        self.assertTrue(last_known_good.qualified, last_known_good.reasons)
        policy = evaluate_tuning_policy(
            detector,
            current_ef=400,
            response_estimates=_response_estimates(),
            pre_action=PreActionSafety(
                metric=Metric.L2,
                threshold_stratum=STRATUM,
                configuration_identity=CONFIGURATION_ID,
                index_identity=HNSW_BINDING_ID,
                flat_index_identity=FLAT_BINDING_ID,
                data_identity=DATA_ID,
                response_model_provenance="exp005-test-response-model-v1",
            ),
            canary_observation=None,
            last_known_good=last_known_good,
            mode=PolicyMode.DRY_RUN,
            threshold_stratum=STRATUM,
            audit_id="exp005-stationary-dry-run",
        )
        self.assertEqual(policy.action, PolicyAction.NO_CHANGE)
        self.assertEqual(policy.evidence_provenance, current.provenance)

        context = ActuationContext(
            metric=Metric.L2,
            threshold_stratum=STRATUM,
            collection_name="exp005_hnsw",
            configuration_identity=CONFIGURATION_ID,
            index_identity=HNSW_BINDING_ID,
            flat_index_identity=FLAT_BINDING_ID,
            data_identity=DATA_ID,
            audited_query_ids=tuple(range(50)),
            last_known_good=last_known_good,
            occurred_at_utc="2026-08-03T12:01:00Z",
        )
        client = _NoClient()
        controller = _Controller()
        with tempfile.TemporaryDirectory() as directory:
            sink = JsonlAuditSink(Path(directory) / "exp005-audit.jsonl")
            result = SafeActuationBoundary(client, sink, controller).execute(
                policy, context
            )

            self.assertEqual(result.outcome, ActuationOutcome.NO_OP)
            self.assertTrue(result.success)
            self.assertEqual(result.audit_record.evidence_provenance, current.provenance)
            self.assertTrue(sink.contains(policy.audit_id))
        self.assertEqual(client.calls, [])
        self.assertEqual(controller.calls, [])


if __name__ == "__main__":
    unittest.main()
