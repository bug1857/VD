"""TDD coverage for EXP-005's raw-window evidence extraction boundary."""

from __future__ import annotations

import ast
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from vdbench.config import IndexTrack, Metric
from vdbench.drift import Signal, SignalEvidence, select_audit_sample
from vdbench.milvus import CollectionIdentity, SearchHit
from vdbench.milvus_actuation import (
    ShadowAuditStageEvidence,
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from vdbench.oracle import OracleHit, OracleResult
from vdbench.shadow_extraction import extract_window_evidence
from vdbench.shadow_window import (
    AssembledShadowWindow,
    PersistedShadowTraceEnvelope,
    assemble_shadow_window,
    hash_shadow_audit_trace,
)


def _identity(
    track: IndexTrack,
    metric: Metric,
    *,
    binding_id: str | None = None,
) -> ShadowIdentityEvidence:
    description: dict[str, object] = {
        "index_type": track.value,
        "metric_type": metric.value,
    }
    if track is IndexTrack.HNSW:
        description.update({"M": "16", "efConstruction": "200"})
    snapshot = CollectionIdentity(
        collection_name=f"exp005_{metric.value.lower()}_{track.value.lower()}",
        metric=metric.value,
        index_track=track.value,
        description=description,
    )
    stage = ShadowAuditStageEvidence(stage=f"{track.value}_IDENTITY", success=True)
    return ShadowIdentityEvidence(
        track=track,
        expected_binding_id=binding_id or f"{track.value.lower()}-index-v1",
        pre_snapshot=snapshot,
        post_snapshot=snapshot,
        pre_binding_match=True,
        post_binding_match=True,
        pre_capture=stage,
        post_capture=stage,
    )


def _query(query_id: int, metric: Metric) -> ShadowQueryAuditTrace:
    score, radius, range_filter = (
        (1.0, 2.0, 0.0) if metric is Metric.L2 else (0.5, 0.0, 1.0)
    )
    oracle = OracleResult((OracleHit(query_id, score),), full_count=1, capped=False)
    hit = SearchHit(query_id, score)
    return ShadowQueryAuditTrace(
        query_id=query_id,
        query_vector=(float(query_id + 1), 1.0),
        threshold_radius=radius,
        range_filter=range_filter,
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


def _window(window_id: str, *, metric: Metric = Metric.L2) -> AssembledShadowWindow:
    envelopes: list[PersistedShadowTraceEnvelope] = []
    for sequence_index in range(4):
        start = sequence_index * 50
        trace = ShadowAuditTrace(
            metric=metric,
            threshold_stratum="target-075",
            candidate_ef=400,
            last_known_good_ef=200,
            sentinel_ef=100,
            configuration_identity="config-v1",
            data_identity="dataset-v1",
            flat_identity=_identity(IndexTrack.FLAT, metric),
            hnsw_identity=_identity(IndexTrack.HNSW, metric),
            queries=tuple(_query(index, metric) for index in range(start, start + 50)),
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
    result = assemble_shadow_window(window_id=window_id, envelopes=tuple(envelopes))
    assert result.complete, result.reason_codes
    return result


def _replace_traces(
    window: AssembledShadowWindow,
    transform: object,
) -> AssembledShadowWindow:
    assert callable(transform)
    return replace(
        window,
        envelopes=tuple(
            replace(envelope, trace=transform(envelope.trace))
            for envelope in window.envelopes
        ),
    )


def _complete_signal(signal: Signal, reference_count: int, current_count: int) -> SignalEvidence:
    floors = {
        Signal.QUERY_VECTOR: 0.01,
        Signal.THRESHOLD: 0.20,
        Signal.CARDINALITY: 0.20,
        Signal.RECALL: 0.02,
    }
    return SignalEvidence(
        signal=signal,
        complete=True,
        reference_count=reference_count,
        current_count=current_count,
        statistic=0.0,
        effect=0.0,
        effect_floor=floors[signal],
        raw_p_value=1.0,
    )


class ShadowExtractionTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.reference = _window("reference")
        self.current = _window("current")

    def _extract(
        self,
        *,
        reference: AssembledShadowWindow | None = None,
        current: AssembledShadowWindow | None = None,
        metric: Metric = Metric.L2,
    ):
        return extract_window_evidence(
            reference_window=reference or self.reference,
            current_window=current or self.current,
            detector_seed=20260803,
            metric=metric,
        )

    def assert_incomplete(self, evidence, code: str) -> None:
        self.assertFalse(evidence.complete, evidence.reason_codes)
        self.assertIn(code, evidence.reason_codes, evidence.reason_codes)

    def test_l2_valid_windows_produce_complete_real_evidence(self) -> None:
        evidence = self._extract()
        self.assertTrue(evidence.complete, evidence.reason_codes)
        self.assertEqual(evidence.window_id, self.current.window_id)
        self.assertEqual(tuple(item.signal for item in evidence.signals), tuple(Signal))
        self.assertEqual(
            tuple((item.reference_count, item.current_count) for item in evidence.signals),
            ((200, 200), (200, 200), (50, 50), (50, 50)),
        )
        self.assertIsNotNone(evidence.provenance)
        assert evidence.provenance is not None
        self.assertEqual(
            evidence.provenance.reference_manifest_sha256,
            self.reference.manifest_sha256,
        )
        self.assertEqual(
            evidence.provenance.current_manifest_sha256,
            self.current.manifest_sha256,
        )
        self.assertEqual(len(evidence.provenance.current_audit_ids), 50)

    def test_cosine_valid_windows_produce_complete_real_evidence(self) -> None:
        reference = _window("cosine-reference", metric=Metric.COSINE)
        current = _window("cosine-current", metric=Metric.COSINE)
        evidence = self._extract(reference=reference, current=current, metric=Metric.COSINE)
        self.assertTrue(evidence.complete, evidence.reason_codes)
        self.assertEqual(evidence.window_id, current.window_id)

    def test_identical_inputs_and_seed_are_deterministic(self) -> None:
        self.assertEqual(self._extract(), self._extract())

    def test_recall_samples_use_their_own_window_ids(self) -> None:
        samples = []

        def capture(reference_sample, current_sample, *, detector_seed):
            samples.extend((reference_sample, current_sample))
            return _complete_signal(Signal.RECALL, 50, 50)

        with patch("vdbench.shadow_extraction.recall_signal_test", side_effect=capture):
            evidence = self._extract()
        self.assertTrue(evidence.complete, evidence.reason_codes)
        self.assertEqual(samples[0].window_id, self.reference.window_id)
        self.assertEqual(samples[1].window_id, self.current.window_id)

    def test_incomplete_reference_fails_closed(self) -> None:
        self.assert_incomplete(
            self._extract(reference=replace(self.reference, complete=False)),
            "REFERENCE_WINDOW_INCOMPLETE",
        )

    def test_incomplete_current_fails_closed(self) -> None:
        self.assert_incomplete(
            self._extract(current=replace(self.current, complete=False)),
            "CURRENT_WINDOW_INCOMPLETE",
        )

    def test_window_metric_mismatch_fails_closed(self) -> None:
        self.assert_incomplete(
            self._extract(current=replace(self.current, metric=Metric.COSINE)),
            "WINDOW_METRIC_MISMATCH",
        )

    def test_passed_metric_mismatch_fails_closed(self) -> None:
        self.assert_incomplete(self._extract(metric=Metric.COSINE), "METRIC_PARAMETER_MISMATCH")

    def test_threshold_stratum_mismatch_fails_closed(self) -> None:
        self.assert_incomplete(
            self._extract(current=replace(self.current, threshold_stratum="target-025")),
            "THRESHOLD_STRATUM_MISMATCH",
        )

    def test_identical_window_ids_fail_closed(self) -> None:
        self.assert_incomplete(
            self._extract(current=replace(self.current, window_id=self.reference.window_id)),
            "WINDOW_IDS_MUST_DIFFER",
        )

    def test_data_identity_mismatch_fails_closed(self) -> None:
        changed = _replace_traces(
            self.current, lambda trace: replace(trace, data_identity="dataset-v2")
        )
        self.assert_incomplete(self._extract(current=changed), "DATA_IDENTITY_MISMATCH")

    def test_configuration_identity_mismatch_fails_closed(self) -> None:
        changed = _replace_traces(
            self.current, lambda trace: replace(trace, configuration_identity="config-v2")
        )
        self.assert_incomplete(self._extract(current=changed), "CONFIGURATION_IDENTITY_MISMATCH")

    def test_flat_binding_mismatch_fails_closed(self) -> None:
        changed = _replace_traces(
            self.current,
            lambda trace: replace(
                trace,
                flat_identity=replace(trace.flat_identity, expected_binding_id="flat-v2"),
            ),
        )
        self.assert_incomplete(self._extract(current=changed), "FLAT_BINDING_MISMATCH")

    def test_hnsw_binding_mismatch_fails_closed(self) -> None:
        changed = _replace_traces(
            self.current,
            lambda trace: replace(
                trace,
                hnsw_identity=replace(trace.hnsw_identity, expected_binding_id="hnsw-v2"),
            ),
        )
        self.assert_incomplete(self._extract(current=changed), "HNSW_BINDING_MISMATCH")

    def test_identity_inconsistency_inside_one_window_fails_closed(self) -> None:
        envelopes = list(self.current.envelopes)
        second = envelopes[1]
        assert second.trace is not None
        envelopes[1] = replace(
            second,
            trace=replace(second.trace, configuration_identity="config-inconsistent"),
        )
        self.assert_incomplete(
            self._extract(current=replace(self.current, envelopes=tuple(envelopes))),
            "CURRENT_CONFIGURATION_IDENTITY_INCONSISTENT",
        )

    def test_missing_envelope_trace_fails_closed(self) -> None:
        envelopes = list(self.current.envelopes)
        envelopes[0] = replace(envelopes[0], trace=None)
        self.assert_incomplete(
            self._extract(current=replace(self.current, envelopes=tuple(envelopes))),
            "CURRENT_TRACE_INVALID",
        )

    def test_selector_with_49_ids_fails_closed(self) -> None:
        selection = select_audit_sample(
            [item.query_id for item in self.reference.query_records],
            detector_seed=7,
            metric=Metric.L2,
            window_id=self.reference.window_id,
        )
        malformed = replace(
            selection,
            query_ids=selection.query_ids[:-1],
            digest_hex=selection.digest_hex[:-1],
        )
        with patch("vdbench.shadow_extraction.select_audit_sample", return_value=malformed):
            evidence = self._extract()
        self.assert_incomplete(evidence, "REFERENCE_AUDIT_SELECTION_COUNT_INVALID")

    def test_selected_id_absent_from_records_fails_closed(self) -> None:
        selection = select_audit_sample(
            [item.query_id for item in self.reference.query_records],
            detector_seed=7,
            metric=Metric.L2,
            window_id=self.reference.window_id,
        )
        malformed = replace(selection, query_ids=(999, *selection.query_ids[1:]))
        with patch(
            "vdbench.shadow_extraction.select_audit_sample",
            side_effect=(malformed, selection),
        ):
            evidence = self._extract()
        self.assert_incomplete(evidence, "REFERENCE_SELECTED_AUDIT_ID_MISSING")

    def test_missing_sentinel_stage_fails_closed(self) -> None:
        current = replace(
            self.current,
            query_records=tuple(
                replace(
                    record,
                    stages=tuple(
                        stage for stage in record.stages if stage.stage != "SENTINEL_HNSW"
                    ),
                )
                for record in self.current.query_records
            ),
        )
        self.assert_incomplete(
            self._extract(current=current), "CURRENT_SENTINEL_STAGE_INVALID"
        )

    def test_query_vector_exception_fails_closed(self) -> None:
        with patch(
            "vdbench.shadow_extraction.query_vector_signal_test", side_effect=RuntimeError("injected")
        ):
            evidence = self._extract()
        self.assert_incomplete(evidence, "SIGNAL_TEST_EXCEPTION:QUERY_VECTOR")

    def test_threshold_exception_fails_closed(self) -> None:
        with patch("vdbench.shadow_extraction.ks_signal_test", side_effect=RuntimeError("injected")):
            evidence = self._extract()
        self.assert_incomplete(evidence, "SIGNAL_TEST_EXCEPTION:THRESHOLD")

    def test_recall_exception_fails_closed(self) -> None:
        with patch("vdbench.shadow_extraction.recall_signal_test", side_effect=RuntimeError("injected")):
            evidence = self._extract()
        self.assert_incomplete(evidence, "SIGNAL_TEST_EXCEPTION:RECALL")

    def test_each_incomplete_signal_fails_closed(self) -> None:
        patches = (
            ("query_vector_signal_test", _complete_signal(Signal.QUERY_VECTOR, 200, 200), Signal.QUERY_VECTOR),
            ("ks_signal_test", _complete_signal(Signal.THRESHOLD, 200, 200), Signal.THRESHOLD),
            ("recall_signal_test", _complete_signal(Signal.RECALL, 50, 50), Signal.RECALL),
        )
        for target, complete, signal in patches:
            with self.subTest(signal=signal):
                incomplete = replace(
                    complete,
                    complete=False,
                    statistic=None,
                    effect=None,
                    raw_p_value=None,
                    reason="injected",
                )
                with patch(f"vdbench.shadow_extraction.{target}", return_value=incomplete):
                    evidence = self._extract()
                self.assert_incomplete(evidence, f"INCOMPLETE_SIGNAL:{signal.value}")

    def test_module_has_no_pymilvus_import(self) -> None:
        path = Path(__file__).parents[1] / "src" / "vdbench" / "shadow_extraction.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
        self.assertFalse(any(name == "pymilvus" or name.startswith("pymilvus.") for name in imported))


if __name__ == "__main__":
    unittest.main()
