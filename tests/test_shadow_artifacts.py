"""TDD coverage for restart-durable EXP-005 source-trace artifacts."""

from __future__ import annotations

import ast
from dataclasses import replace
import inspect
import json
from pathlib import Path
import tempfile
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
from vdbench.shadow_artifacts import (
    ShadowTraceArtifactError,
    load_persisted_shadow_trace_envelope,
    persist_shadow_trace_envelope,
)
import vdbench.shadow_artifacts as shadow_artifacts
from vdbench.shadow_window import PersistedShadowTraceEnvelope, hash_shadow_audit_trace


def _identity(track: IndexTrack) -> CollectionIdentity:
    return CollectionIdentity(
        collection_name=f"exp005_l2_{track.value.lower()}",
        metric=Metric.L2.value,
        index_track=track.value,
        description={"index_type": track.value, "metric_type": Metric.L2.value},
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
        stages=(ShadowAuditStageEvidence(stage="FLAT", success=True, oracle_agreement=True),),
    )


def _envelope() -> PersistedShadowTraceEnvelope:
    trace = ShadowAuditTrace(
        metric=Metric.L2,
        threshold_stratum="target-075",
        candidate_ef=400,
        last_known_good_ef=200,
        sentinel_ef=100,
        configuration_identity="config-v1",
        data_identity="dataset-v1",
        flat_identity=_identity_evidence(IndexTrack.FLAT),
        hnsw_identity=_identity_evidence(IndexTrack.HNSW),
        queries=tuple(_query(index) for index in range(50)),
        complete=True,
    )
    return PersistedShadowTraceEnvelope(
        trace_id="trace-0",
        captured_at_utc="2026-08-03T12:00:00Z",
        sequence_index=0,
        declared_observation_count=50,
        expected_trace_sha256=hash_shadow_audit_trace(trace),
        trace=trace,
    )


class ShadowTraceArtifactTests(unittest.TestCase):
    def test_persist_then_reload_preserves_typed_envelope_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "trace-0.json"
            expected = _envelope()
            persist_shadow_trace_envelope(path, expected)
            # A separate load call models a process restart.
            self.assertEqual(load_persisted_shadow_trace_envelope(path), expected)

    def test_tampered_trace_payload_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "trace-0.json"
            persist_shadow_trace_envelope(path, _envelope())
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["trace_payload"]["candidate_ef"] = 800
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ShadowTraceArtifactError, "CHECKSUM"):
                load_persisted_shadow_trace_envelope(path)

    def test_unknown_or_missing_schema_fields_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "trace-0.json"
            persist_shadow_trace_envelope(path, _envelope())
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["unexpected"] = True
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ShadowTraceArtifactError, "SCHEMA"):
                load_persisted_shadow_trace_envelope(path)
            payload.pop("unexpected")
            payload.pop("trace_id")
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ShadowTraceArtifactError, "SCHEMA"):
                load_persisted_shadow_trace_envelope(path)

    def test_existing_artifact_is_never_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "trace-0.json"
            persist_shadow_trace_envelope(path, _envelope())
            with self.assertRaises(FileExistsError):
                persist_shadow_trace_envelope(path, replace(_envelope(), trace_id="other"))
            self.assertEqual(load_persisted_shadow_trace_envelope(path).trace_id, "trace-0")

    def test_malformed_json_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "trace-0.json"
            path.write_text("{ not json", encoding="utf-8")
            with self.assertRaisesRegex(ShadowTraceArtifactError, "MALFORMED"):
                load_persisted_shadow_trace_envelope(path)

    def test_module_has_no_pymilvus_or_live_runner_import(self) -> None:
        tree = ast.parse(inspect.getsource(shadow_artifacts))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            f"{node.module}.{alias.name}" if node.module else alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        )
        self.assertFalse(any("pymilvus" in name.lower() for name in imported))
        self.assertFalse(any("execute_live" in name for name in imported))
