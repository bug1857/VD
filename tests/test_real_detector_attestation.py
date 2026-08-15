"""ADR-014 coverage: governed real-detector attestation, durable previous
window evidence, and the exact 4x50 -> 200 source/shadow conservation.

Everything here is offline. No Milvus, etcd, or MinIO is contacted, no real
workload is observed, and no EXP-010/EXP-011 run, grant, route, or canary is
performed. Shadow capture is an injected deterministic fake; the *detector*
itself is the real, unchanged ADR-002/ADR-003 pipeline.
"""

from __future__ import annotations

import ast
from pathlib import Path
import tempfile
import unittest

from vdbench.config import IndexTrack, Metric
from vdbench.drift import (
    DetectorState,
    DriftClassification,
    DriftDecision,
    build_evidence_provenance,
)
from vdbench.host_observation import CompletedRangeQueryObservation, ServedQueryOutcome
from vdbench.host_window_detector_v2 import (
    HostWindowV2Status,
    SQLiteHostWindowDetectorV2Store,
    V2DetectorHead,
    build_v2_shadow_position,
    build_v2_shadow_window,
)
from vdbench.host_window_lineage import SQLiteHostResponseCommitStore
from vdbench.milvus_actuation import ShadowAuditTrace
from vdbench.monitor_evidence import encode_persisted_window_evidence
from vdbench.real_detector_attestation import (
    GovernedV2DetectorEvaluator,
    RealDetectorAttestation,
    RealDetectorAttestationError,
    detector_contract_identity,
    position_evidence_sha256,
)
from vdbench.real_detector_attestation_store import (
    RealDetectorAttestationStoreError,
    SQLiteRealDetectorAttestationStore,
    VerifiedRealDetectorHead,
)
from vdbench.shadow_event_types import MonitorStreamKey
from vdbench.shadow_attempt_store import SQLiteShadowAttemptStore
from vdbench.shadow_window import (
    TRACE_COUNT,
    TRACE_QUERY_COUNT,
    WINDOW_QUERY_COUNT,
    PersistedShadowTraceEnvelope,
    assemble_shadow_window,
    hash_shadow_audit_trace,
)
from vdbench.v2_shadow_worker import V2ShadowWorker, V2ShadowWorkerError

from tests.test_shadow_extraction import _identity, _query

ATTESTATION_MODULE = (
    Path(__file__).parents[1] / "src" / "vdbench" / "real_detector_attestation.py"
)
STORE_MODULE = (
    Path(__file__).parents[1] / "src" / "vdbench" / "real_detector_attestation_store.py"
)
WORKER_MODULE = Path(__file__).parents[1] / "src" / "vdbench" / "v2_shadow_worker.py"
HOST_MODULE = Path(__file__).parents[1] / "src" / "vdbench" / "exp010_v2_host.py"

_SEED = 20260812
_REVISION = "revision/adr-014"
_ENVIRONMENT = "e" * 64


def _stream() -> MonitorStreamKey:
    # The binding ids must equal the shadow traces' actual index identities:
    # the real detector copies them into EvidenceProvenance, and the V2 head
    # (correctly) requires provenance identities to match the stream key.
    return MonitorStreamKey(
        "v2-stream", Metric.L2, "target-075", "config-v1", "dataset-v1",
        "flat-index-v1", "hnsw-index-v1",
    )


def _trace_for(sources, *, metric: Metric = Metric.L2) -> ShadowAuditTrace:
    """Build one 50-query trace whose query ids are the committed source ids."""

    return ShadowAuditTrace(
        metric=metric,
        threshold_stratum="target-075",
        candidate_ef=400,
        last_known_good_ef=200,
        sentinel_ef=100,
        configuration_identity="config-v1",
        data_identity="dataset-v1",
        flat_identity=_identity(IndexTrack.FLAT, metric),
        hnsw_identity=_identity(IndexTrack.HNSW, metric),
        queries=tuple(_query(int(item.query_id), metric) for item in sources),
        complete=True,
    )


class _CaptureExecutor:
    """Deterministic offline stand-in for real FLAT/HNSW/sentinel capture."""

    def __init__(self) -> None:
        self.calls = 0

    def capture(self, sources, *, trace_sequence_index: int) -> ShadowAuditTrace:
        self.calls += 1
        return _trace_for(sources)


def _commit_sources(path: Path, count: int):
    with SQLiteHostResponseCommitStore(
        path,
        stream_key=_stream(),
        source_revision=_REVISION,
        environment_manifest_sha256=_ENVIRONMENT,
    ) as store:
        for index in range(count):
            store.commit_response(
                CompletedRangeQueryObservation(
                    index, "2026-08-12T00:00:00Z", _stream(),
                    (float(index) + 1.0, 1.0), 2.0, 0.0, 100, 400,
                    ServedQueryOutcome(True, False, 1, 1.0),
                ),
                committed_at_utc="2026-08-12T00:00:00Z",
            )
        return store.poll(consumer_id="fixture", limit=count)


class _Harness:
    """One offline v2 stack: sources, shadow bundles, detector + attestation."""

    def __init__(self, root: Path, *, window_count: int = 4) -> None:
        self.root = root
        self.sources = _commit_sources(
            root / "source.sqlite3", WINDOW_QUERY_COUNT * window_count
        )
        self.executor = _CaptureExecutor()
        self._tick = 0

        def _captured_at() -> str:
            # Strictly increasing, as real sequential trace capture is.
            self._tick += 1
            return f"2026-08-12T{self._tick // 3600:02d}:{(self._tick // 60) % 60:02d}:{self._tick % 60:02d}Z"

        self.attempt_store = SQLiteShadowAttemptStore(
            root / "shadow-attempts.sqlite3",
            stream_key=_stream(),
            source_revision=_REVISION,
            environment_manifest_sha256=_ENVIRONMENT,
        )
        self.worker = V2ShadowWorker(
            capture_executor=self.executor,
            captured_at_clock=_captured_at,
            attempt_store=self.attempt_store,
        )
        self.detector_store = SQLiteHostWindowDetectorV2Store(
            root / "detector.sqlite3", stream_key=_stream()
        )
        self.attestation_store = SQLiteRealDetectorAttestationStore(
            root / "attestation.sqlite3", stream_key=_stream()
        )
        self.evaluator = GovernedV2DetectorEvaluator(
            previous_evidence_source=self.attestation_store,
            detector_seed=_SEED,
            source_revision=_REVISION,
            environment_manifest_sha256=_ENVIRONMENT,
        )
        self._bundles: dict[int, object] = {}

    def close(self) -> None:
        self.attestation_store.close()
        self.detector_store.close()
        self.attempt_store.close()

    def window_sources(self, sequence: int):
        start = sequence * WINDOW_QUERY_COUNT
        return tuple(self.sources[start : start + WINDOW_QUERY_COUNT])

    def bundle(self, sequence: int):
        if sequence not in self._bundles:
            self._bundles[sequence] = self.worker.build(self.window_sources(sequence))
        return self._bundles[sequence]

    def rebaseline(self, sequence: int, *, persisted_at: str):
        """Process one window as the structural REBASELINE (no head)."""

        return self.detector_store.process_window(
            window=self.bundle(sequence).shadow_window,
            evaluator=lambda reference, current: (_ for _ in ()).throw(
                AssertionError("rebaseline must not evaluate")
            ),
            persisted_at_utc=persisted_at,
        )

    def evaluate(self, reference_sequence: int, current_sequence: int, *, persisted_at: str):
        """Run the real governed evaluation for one window and attest it."""

        reference = self.bundle(reference_sequence)
        current = self.bundle(current_sequence)
        captured: dict[str, object] = {}

        def evaluator(reference_window, current_window) -> DriftDecision:
            decision, pending = self.evaluator.evaluate(
                reference_shadow_window=reference.shadow_window,
                current_shadow_window=current.shadow_window,
                reference_assembled=reference.assembled,
                current_assembled=current.assembled,
                reference_sources=reference.sources,
                current_sources=current.sources,
                metric=Metric.L2,
            )
            captured["pending"] = pending
            return decision

        result = self.detector_store.process_window(
            window=current.shadow_window,
            evaluator=evaluator,
            persisted_at_utc=persisted_at,
        )
        if result.detector_head is None:
            return result, None
        pending = captured["pending"]
        evidence = pending["current_window_evidence"]
        encoded = encode_persisted_window_evidence(evidence)
        attestation = self.evaluator.attest(
            pending=pending,
            head=result.detector_head,
            current_window_evidence_sha256=encoded["sha256"],
        )
        self.attestation_store.append(attestation=attestation, window_evidence=evidence)
        return result, attestation


class RealDetectorAttestationTests(unittest.TestCase):
    # -- D. 4x50 -> 200 mapping -----------------------------------------

    def test_window_is_exactly_four_fifty_query_traces(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=1)
            try:
                bundle = harness.bundle(0)
                self.assertEqual(len(bundle.envelopes), TRACE_COUNT)
                self.assertEqual(len(bundle.assembled.envelopes), 4)
                for envelope in bundle.envelopes:
                    self.assertEqual(len(envelope.trace.queries), TRACE_QUERY_COUNT)
                    self.assertEqual(envelope.declared_observation_count, 50)
                self.assertEqual(len(bundle.assembled.query_records), 200)
                self.assertEqual(len(bundle.shadow_window.positions), 200)
                self.assertEqual(harness.executor.calls, 4)
                # Window identifier namespaces are collapsed (ADR-014 item 5).
                self.assertEqual(bundle.assembled.window_id, bundle.window_sequence)
            finally:
                harness.close()

    def test_position_binding_conserves_every_source_position(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=1)
            try:
                bundle = harness.bundle(0)
                harness.evaluator.verify_position_binding(
                    shadow_window=bundle.shadow_window,
                    assembled=bundle.assembled,
                    sources=bundle.sources,
                )
                for index in range(WINDOW_QUERY_COUNT):
                    envelope = bundle.assembled.envelopes[index // TRACE_QUERY_COUNT]
                    expected = position_evidence_sha256(
                        source=bundle.sources[index],
                        trace_envelope_sha256=envelope.expected_trace_sha256,
                        trace_sequence_index=index // TRACE_QUERY_COUNT,
                        within_trace_index=index % TRACE_QUERY_COUNT,
                        query_id=bundle.assembled.query_records[index].query_id,
                    )
                    self.assertEqual(
                        bundle.shadow_window.positions[index].evaluation_evidence_sha256,
                        expected,
                    )
            finally:
                harness.close()

    def test_swapping_records_inside_a_trace_fails_window_assembly(self) -> None:
        sources = _commit_sources(
            Path(tempfile.mkdtemp()) / "source.sqlite3", WINDOW_QUERY_COUNT
        )
        trace = _trace_for(sources[:TRACE_QUERY_COUNT])
        swapped = ShadowAuditTrace(
            metric=trace.metric, threshold_stratum=trace.threshold_stratum,
            candidate_ef=trace.candidate_ef,
            last_known_good_ef=trace.last_known_good_ef,
            sentinel_ef=trace.sentinel_ef,
            configuration_identity=trace.configuration_identity,
            data_identity=trace.data_identity,
            flat_identity=trace.flat_identity, hnsw_identity=trace.hnsw_identity,
            queries=(trace.queries[1], trace.queries[0], *trace.queries[2:]),
            complete=True,
        )
        # The envelope digest is over the whole ordered trace, so an intra-trace
        # swap can no longer match its own recorded digest.
        self.assertNotEqual(hash_shadow_audit_trace(swapped), hash_shadow_audit_trace(trace))
        envelope = PersistedShadowTraceEnvelope(
            trace_id="t", captured_at_utc="2026-08-12T00:00:01Z", sequence_index=0,
            declared_observation_count=TRACE_QUERY_COUNT,
            expected_trace_sha256=hash_shadow_audit_trace(trace),
            trace=swapped,
        )
        assembled = assemble_shadow_window(window_id=0, envelopes=(envelope,))
        self.assertFalse(assembled.complete)

    def test_wrong_source_position_and_wrong_within_trace_index_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=1)
            try:
                bundle = harness.bundle(0)
                envelope = bundle.assembled.envelopes[0]
                record = bundle.assembled.query_records[0]
                # Correct envelope, wrong source position.
                wrong_source = position_evidence_sha256(
                    source=bundle.sources[1],
                    trace_envelope_sha256=envelope.expected_trace_sha256,
                    trace_sequence_index=0, within_trace_index=0,
                    query_id=record.query_id,
                )
                # Correct envelope digest, wrong within-trace position.
                wrong_index = position_evidence_sha256(
                    source=bundle.sources[0],
                    trace_envelope_sha256=envelope.expected_trace_sha256,
                    trace_sequence_index=0, within_trace_index=1,
                    query_id=record.query_id,
                )
                # Envelope from another trace in the same window.
                other_envelope = position_evidence_sha256(
                    source=bundle.sources[0],
                    trace_envelope_sha256=bundle.assembled.envelopes[1].expected_trace_sha256,
                    trace_sequence_index=0, within_trace_index=0,
                    query_id=record.query_id,
                )
                genuine = bundle.shadow_window.positions[0].evaluation_evidence_sha256
                for label, forged in (
                    ("wrong_source", wrong_source),
                    ("wrong_within_trace_index", wrong_index),
                    ("other_envelope", other_envelope),
                ):
                    with self.subTest(case=label):
                        self.assertNotEqual(forged, genuine)
                        positions = list(bundle.shadow_window.positions)
                        positions[0] = build_v2_shadow_position(
                            source=bundle.sources[0], evaluation_eligible=True,
                            evaluation_evidence_sha256=forged,
                        )
                        substituted = build_v2_shadow_window(
                            sources=bundle.sources, positions=tuple(positions)
                        )
                        with self.assertRaises(RealDetectorAttestationError) as raised:
                            harness.evaluator.verify_position_binding(
                                shadow_window=substituted,
                                assembled=bundle.assembled,
                                sources=bundle.sources,
                            )
                        self.assertEqual(
                            raised.exception.code, "POSITION_EVIDENCE_MISMATCH"
                        )
            finally:
                harness.close()

    def test_query_id_mismatch_between_shadow_and_source_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=2)
            try:
                bundle = harness.bundle(0)
                other = harness.bundle(1)
                # Another window's assembled evidence against these sources.
                with self.assertRaises(RealDetectorAttestationError) as raised:
                    harness.evaluator.verify_position_binding(
                        shadow_window=bundle.shadow_window,
                        assembled=other.assembled,
                        sources=bundle.sources,
                    )
                self.assertIn(
                    raised.exception.code,
                    {"ASSEMBLED_WINDOW_ID_MISMATCH", "POSITION_QUERY_ID_MISMATCH"},
                )
            finally:
                harness.close()

    def test_worker_rejects_capture_with_wrong_query_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = _commit_sources(root / "source.sqlite3", WINDOW_QUERY_COUNT)

            class _Mismapped:
                def capture(self, slice_sources, *, trace_sequence_index: int):
                    # Deliberately answer with a different, statistically valid
                    # slice: arrays are fine, the source mapping is not.
                    return _trace_for(sources[:TRACE_QUERY_COUNT])

            ticks = iter(
                f"2026-08-12T00:00:0{index}Z" for index in range(1, 9)
            )
            with SQLiteShadowAttemptStore(
                root / "attempts.sqlite3",
                stream_key=_stream(),
                source_revision=_REVISION,
                environment_manifest_sha256=_ENVIRONMENT,
            ) as attempt_store:
                worker = V2ShadowWorker(
                    capture_executor=_Mismapped(),
                    captured_at_clock=lambda: next(ticks),
                    attempt_store=attempt_store,
                )
                with self.assertRaises(V2ShadowWorkerError) as raised:
                    worker.build(tuple(sources))
            # Repeating one slice is caught either by the window assembler's
            # duplicate-trace/query rules or by the query-id cross-check; both
            # are fail-closed, and neither may silently accept the mismapping.
            self.assertEqual(raised.exception.code, "SHADOW_TRACE_FAILED")
            self.assertIn("SHADOW_POSITION_QUERY_ID_MISMATCH", str(raised.exception))

    # -- C. previous evidence state machine ------------------------------

    def test_first_comparison_is_insufficient_then_second_may_decide(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=3)
            try:
                rebaseline = harness.rebaseline(0, persisted_at="2026-08-12T00:00:01Z")
                self.assertEqual(rebaseline.status, HostWindowV2Status.REBASELINE)
                self.assertIsNone(rebaseline.detector_head)

                first, first_attestation = harness.evaluate(
                    0, 1, persisted_at="2026-08-12T00:00:02Z"
                )
                self.assertEqual(first.status, HostWindowV2Status.EVALUATED)
                self.assertIs(
                    first.detector_head.detector_state,
                    DetectorState.INSUFFICIENT_EVIDENCE,
                )
                self.assertIsNone(first_attestation.previous_attestation_sha256)
                self.assertIsNone(harness.detector_store.latest_drift_head())

                second, second_attestation = harness.evaluate(
                    0, 2, persisted_at="2026-08-12T00:00:03Z"
                )
                self.assertIn(
                    second.detector_head.detector_state,
                    {DetectorState.DRIFT, DetectorState.NO_DRIFT},
                )
                self.assertEqual(
                    second_attestation.previous_attestation_sha256,
                    first_attestation.attestation_sha256,
                )
                self.assertEqual(
                    second_attestation.previous_window_evidence_sha256,
                    first_attestation.current_window_evidence_sha256,
                )
            finally:
                harness.close()

    def test_previous_evidence_cannot_be_injected_by_a_caller(self) -> None:
        """`evaluate` exposes no previous-evidence parameter at all."""

        import inspect

        signature = inspect.signature(GovernedV2DetectorEvaluator.evaluate)
        for forbidden in ("previous_evidence", "previous", "previous_window_evidence"):
            self.assertNotIn(forbidden, signature.parameters)

    def test_restart_between_comparisons_preserves_the_chain(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root, window_count=3)
            harness.rebaseline(0, persisted_at="2026-08-12T00:00:01Z")
            _, first_attestation = harness.evaluate(
                0, 1, persisted_at="2026-08-12T00:00:02Z"
            )
            harness.close()

            # Reopen the durable stores only; sources are already committed.
            with SQLiteHostResponseCommitStore(
                root / "source.sqlite3", stream_key=_stream(),
                source_revision=_REVISION, environment_manifest_sha256=_ENVIRONMENT,
            ) as source_store:
                sources = source_store.poll(
                    consumer_id="fixture", limit=WINDOW_QUERY_COUNT * 3
                )
                detector_store = SQLiteHostWindowDetectorV2Store(
                    root / "detector.sqlite3", stream_key=_stream()
                )
                attestation_store = SQLiteRealDetectorAttestationStore(
                    root / "attestation.sqlite3", stream_key=_stream()
                )
                try:
                    previous = attestation_store.load_previous_attested_evidence(
                        stream_key=_stream(),
                        reference_window_sequence=0,
                        reference_source_window_sha256=first_attestation.reference_source_window_sha256,
                        expected_current_window_sequence=1,
                        detector_contract_identity=detector_contract_identity(),
                    )
                    self.assertIsNotNone(previous)
                    self.assertEqual(
                        previous.attestation_sha256,
                        first_attestation.attestation_sha256,
                    )
                    self.assertEqual(len(sources), WINDOW_QUERY_COUNT * 3)
                finally:
                    attestation_store.close()
                    detector_store.close()

    def test_previous_evidence_from_another_reference_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=3)
            try:
                harness.rebaseline(0, persisted_at="2026-08-12T00:00:01Z")
                harness.evaluate(0, 1, persisted_at="2026-08-12T00:00:02Z")
                # Same adjacency, different reference epoch digest.
                self.assertIsNone(
                    harness.attestation_store.load_previous_attested_evidence(
                        stream_key=_stream(),
                        reference_window_sequence=0,
                        reference_source_window_sha256="0" * 64,
                        expected_current_window_sequence=1,
                        detector_contract_identity=detector_contract_identity(),
                    )
                )
                # Correct reference, non-adjacent predecessor.
                self.assertIsNone(
                    harness.attestation_store.load_previous_attested_evidence(
                        stream_key=_stream(),
                        reference_window_sequence=0,
                        reference_source_window_sha256=harness.bundle(0).shadow_window.source_window_sha256,
                        expected_current_window_sequence=5,
                        detector_contract_identity=detector_contract_identity(),
                    )
                )
                # Correct everything except the governed detector contract.
                self.assertIsNone(
                    harness.attestation_store.load_previous_attested_evidence(
                        stream_key=_stream(),
                        reference_window_sequence=0,
                        reference_source_window_sha256=harness.bundle(0).shadow_window.source_window_sha256,
                        expected_current_window_sequence=1,
                        detector_contract_identity="f" * 64,
                    )
                )
            finally:
                harness.close()

    # -- B. forgery matrix ----------------------------------------------

    def test_attestation_cannot_be_publicly_constructed(self) -> None:
        with self.assertRaises(TypeError):
            RealDetectorAttestation(_token=object())
        with self.assertRaises(TypeError):
            VerifiedRealDetectorHead(_token=object())

    def test_fake_evaluator_and_forged_head_cannot_produce_a_real_head(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=2)
            try:
                harness.rebaseline(0, persisted_at="2026-08-12T00:00:01Z")
                bundle0 = harness.bundle(0)
                bundle1 = harness.bundle(1)

                def fake(reference_window, current_window) -> DriftDecision:
                    provenance = build_evidence_provenance(
                        metric=Metric.L2, threshold_stratum="target-075",
                        reference_window_id=bundle0.shadow_window.window_sequence,
                        current_window_id=bundle1.shadow_window.window_sequence,
                        reference_manifest_sha256=bundle0.shadow_window.source_window_sha256,
                        current_manifest_sha256=bundle1.shadow_window.source_window_sha256,
                        configuration_identity="config-v1", data_identity="dataset-v1",
                        flat_binding_id="flat-index-v1", hnsw_binding_id="hnsw-index-v1",
                        reference_audit_ids=tuple(range(50)),
                        reference_audit_rank_digests=tuple("a" * 64 for _ in range(50)),
                        current_audit_ids=tuple(range(50, 100)),
                        current_audit_rank_digests=tuple("b" * 64 for _ in range(50)),
                    )
                    return DriftDecision(
                        state=DetectorState.DRIFT,
                        classification=DriftClassification.INPUT_DRIFT,
                        reason_codes=("DRIFT",),
                        evidence_provenance=provenance,
                    )

                result = harness.detector_store.process_window(
                    window=bundle1.shadow_window, evaluator=fake,
                    persisted_at_utc="2026-08-12T00:00:02Z",
                )
                # A structural head persists (ADR-012) ...
                self.assertIsNotNone(result.detector_head)
                self.assertIsNotNone(harness.detector_store.latest_drift_head())
                # ... but it is not real-eligible: no attestation binds it.
                self.assertIsNone(
                    harness.attestation_store.load_verified_real_latest(
                        harness.detector_store
                    )
                )
            finally:
                harness.close()

    def test_head_without_attestation_is_never_real_eligible(self) -> None:
        """Exactly the crash-after-head-before-attestation window."""

        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=3)
            try:
                harness.rebaseline(0, persisted_at="2026-08-12T00:00:01Z")
                reference = harness.bundle(0)
                current = harness.bundle(1)

                def evaluator(reference_window, current_window) -> DriftDecision:
                    decision, _pending = harness.evaluator.evaluate(
                        reference_shadow_window=reference.shadow_window,
                        current_shadow_window=current.shadow_window,
                        reference_assembled=reference.assembled,
                        current_assembled=current.assembled,
                        reference_sources=reference.sources,
                        current_sources=current.sources,
                        metric=Metric.L2,
                    )
                    return decision

                result = harness.detector_store.process_window(
                    window=current.shadow_window, evaluator=evaluator,
                    persisted_at_utc="2026-08-12T00:00:02Z",
                )
                self.assertIsNotNone(result.detector_head)
                # Attestation deliberately not appended: simulated crash.
                self.assertIsNone(
                    harness.attestation_store.load_verified_real_latest(
                        harness.detector_store
                    )
                )
            finally:
                harness.close()

    def test_verified_real_head_is_issued_when_head_and_attestation_agree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=3)
            try:
                harness.rebaseline(0, persisted_at="2026-08-12T00:00:01Z")
                harness.evaluate(0, 1, persisted_at="2026-08-12T00:00:02Z")
                real = harness.attestation_store.load_verified_real_latest(
                    harness.detector_store
                )
                self.assertIsNotNone(real)
                self.assertIs(type(real), VerifiedRealDetectorHead)
                self.assertEqual(
                    real.attestation.detector_head_sha256,
                    real.head.detector_head_sha256,
                )
                self.assertEqual(
                    real.attestation.detector_contract_identity,
                    detector_contract_identity(),
                )
                # First comparison under a reference: evidence, not a trigger.
                self.assertIs(
                    real.head.detector_state, DetectorState.INSUFFICIENT_EVIDENCE
                )
            finally:
                harness.close()

    def test_three_digest_domains_stay_distinct_while_the_real_path_succeeds(self) -> None:
        """ADR-014 clarification: source-window, shadow-window, and assembled
        manifest digests are three different canonical domains. The head must
        not conflate them; the attestation must bind all three."""

        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=3)
            try:
                harness.rebaseline(0, persisted_at="2026-08-12T00:00:01Z")
                _, attestation = harness.evaluate(
                    0, 1, persisted_at="2026-08-12T00:00:02Z"
                )
                reference = harness.bundle(0)
                current = harness.bundle(1)
                for label, source_digest, assembled_digest, shadow_digest in (
                    (
                        "reference",
                        reference.shadow_window.source_window_sha256,
                        reference.assembled.manifest_sha256,
                        reference.shadow_window.shadow_window_sha256,
                    ),
                    (
                        "current",
                        current.shadow_window.source_window_sha256,
                        current.assembled.manifest_sha256,
                        current.shadow_window.shadow_window_sha256,
                    ),
                ):
                    with self.subTest(window=label):
                        self.assertNotEqual(source_digest, assembled_digest)
                        self.assertNotEqual(source_digest, shadow_digest)
                        self.assertNotEqual(assembled_digest, shadow_digest)
                # All three are nonetheless bound by the attestation.
                self.assertEqual(
                    attestation.reference_source_window_sha256,
                    reference.shadow_window.source_window_sha256,
                )
                self.assertEqual(
                    attestation.reference_assembled_manifest_sha256,
                    reference.assembled.manifest_sha256,
                )
                self.assertEqual(
                    attestation.reference_shadow_window_sha256,
                    reference.shadow_window.shadow_window_sha256,
                )
                self.assertEqual(
                    attestation.current_assembled_manifest_sha256,
                    current.assembled.manifest_sha256,
                )
                # And the complete real path succeeds end to end.
                real = harness.attestation_store.load_verified_real_latest(
                    harness.detector_store
                )
                self.assertIsNotNone(real)
            finally:
                harness.close()

    def test_provenance_must_match_the_exact_assembled_manifests(self) -> None:
        """Correct source digests + wrong assembled manifest must fail, and
        vice versa: the attestation asserts the equality the head no longer
        does, in the correct domain."""

        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=3)
            try:
                reference = harness.bundle(0)
                current = harness.bundle(1)
                other = harness.bundle(2)
                # Correct source/shadow windows, assembled evidence from a
                # different window: provenance manifests can no longer match.
                with self.assertRaises(RealDetectorAttestationError) as raised:
                    harness.evaluator.evaluate(
                        reference_shadow_window=reference.shadow_window,
                        current_shadow_window=current.shadow_window,
                        reference_assembled=reference.assembled,
                        current_assembled=other.assembled,
                        reference_sources=reference.sources,
                        current_sources=current.sources,
                        metric=Metric.L2,
                    )
                self.assertIn(
                    raised.exception.code,
                    {
                        "ASSEMBLED_WINDOW_ID_MISMATCH",
                        "POSITION_QUERY_ID_MISMATCH",
                        "PROVENANCE_ASSEMBLED_MANIFEST_MISMATCH",
                    },
                )
            finally:
                harness.close()

    def test_attestation_for_a_wrong_stream_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=2)
            try:
                harness.rebaseline(0, persisted_at="2026-08-12T00:00:01Z")
                other_stream = MonitorStreamKey(
                    "other", Metric.L2, "target-075", "config-v1", "dataset-v1",
                    "flat", "hnsw",
                )
                with self.assertRaises(RealDetectorAttestationStoreError) as raised:
                    harness.attestation_store.load_previous_attested_evidence(
                        stream_key=other_stream,
                        reference_window_sequence=0,
                        reference_source_window_sha256="0" * 64,
                        expected_current_window_sequence=0,
                        detector_contract_identity=detector_contract_identity(),
                    )
                self.assertEqual(raised.exception.code, "ATTESTATION_STREAM_MISMATCH")
            finally:
                harness.close()

    def test_evaluator_rejects_foreign_source_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = _Harness(Path(directory), window_count=2)
            try:
                foreign = GovernedV2DetectorEvaluator(
                    previous_evidence_source=harness.attestation_store,
                    detector_seed=_SEED,
                    source_revision="revision/other",
                    environment_manifest_sha256=_ENVIRONMENT,
                )
                reference = harness.bundle(0)
                current = harness.bundle(1)
                with self.assertRaises(RealDetectorAttestationError) as raised:
                    foreign.evaluate(
                        reference_shadow_window=reference.shadow_window,
                        current_shadow_window=current.shadow_window,
                        reference_assembled=reference.assembled,
                        current_assembled=current.assembled,
                        reference_sources=reference.sources,
                        current_sources=current.sources,
                        metric=Metric.L2,
                    )
                self.assertEqual(raised.exception.code, "SOURCE_IDENTITY_MISMATCH")
            finally:
                harness.close()

    # -- E. attestation store hardening ----------------------------------

    def test_second_store_on_the_same_path_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "attestation.sqlite3"
            with SQLiteRealDetectorAttestationStore(path, stream_key=_stream()):
                with self.assertRaises(RealDetectorAttestationStoreError) as raised:
                    SQLiteRealDetectorAttestationStore(path, stream_key=_stream())
                self.assertEqual(raised.exception.code, "ATTESTATION_STORE_BUSY")

    def test_tampered_attestation_record_fails_closed_on_reopen(self) -> None:
        import sqlite3

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = _Harness(root, window_count=2)
            try:
                harness.rebaseline(0, persisted_at="2026-08-12T00:00:01Z")
                harness.evaluate(0, 1, persisted_at="2026-08-12T00:00:02Z")
            finally:
                harness.close()
            connection = sqlite3.connect(root / "attestation.sqlite3")
            connection.execute("DROP TRIGGER attestation_records_no_update")
            connection.execute(
                "UPDATE attestation_records SET record_sha256=? WHERE record_sequence=0",
                ("0" * 64,),
            )
            connection.commit()
            connection.close()
            with self.assertRaises(RealDetectorAttestationStoreError):
                SQLiteRealDetectorAttestationStore(
                    root / "attestation.sqlite3", stream_key=_stream()
                )


class Adr014IndependenceTests(unittest.TestCase):
    def test_new_modules_have_no_authority_or_milvus_dependency(self) -> None:
        forbidden = {
            "policy", "actuation", "canary_admission", "canary_approval",
            "canary_activation", "canary_route_authority", "canary_route_state",
            "canary_routing", "canary_live_runner", "canary_grant_store",
            "pymilvus",
        }
        for module in (ATTESTATION_MODULE, STORE_MODULE, WORKER_MODULE, HOST_MODULE):
            with self.subTest(module=module.name):
                tree = ast.parse(module.read_text(encoding="utf-8"), filename=str(module))
                imported = {
                    node.module or ""
                    for node in ast.walk(tree)
                    if isinstance(node, ast.ImportFrom)
                } | {
                    alias.name
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Import)
                    for alias in node.names
                }
                offending = {
                    item
                    for item in imported
                    if any(item == name or item.endswith(f".{name}") for name in forbidden)
                }
                self.assertFalse(offending, offending)

    def test_no_milvus_client_construction_in_the_offline_path(self) -> None:
        for module in (ATTESTATION_MODULE, STORE_MODULE, WORKER_MODULE, HOST_MODULE):
            with self.subTest(module=module.name):
                source = module.read_text(encoding="utf-8")
                self.assertNotIn("MilvusClient", source)
                self.assertNotIn("START_CANARY", source)

    def test_adr_007_host_observation_is_untouched_by_the_v2_path(self) -> None:
        """The v2 composition is a sibling: it never imports the ADR-007
        recorder nor calls its nonblocking offer() boundary.

        Scanned via AST, not raw text: the module docstring legitimately names
        both to state that it leaves them alone.
        """

        tree = ast.parse(HOST_MODULE.read_text(encoding="utf-8"), filename=str(HOST_MODULE))
        imported_names = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        self.assertNotIn("HostObservationRecorder", imported_names)
        self.assertNotIn("BoundedHostObservationRecorder", imported_names)
        called = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertNotIn("offer", called)


if __name__ == "__main__":
    unittest.main()
