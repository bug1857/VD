from __future__ import annotations

from dataclasses import fields
from pathlib import Path
import tempfile
import unittest

from vdbench.config import Metric
from vdbench.drift import (
    DetectorState,
    DriftClassification,
    DriftDecision,
    build_evidence_provenance,
)
from vdbench.host_observation import CompletedRangeQueryObservation, ServedQueryOutcome
from vdbench.host_window_detector_v2 import (
    HostWindowDetectorV2Error,
    HostWindowV2Status,
    SQLiteHostWindowDetectorV2Store,
    V2ShadowPositionEvidence,
    build_v2_shadow_position,
    build_v2_shadow_window,
)
from vdbench.host_window_lineage import SQLiteHostResponseCommitStore
from vdbench.shadow_event_types import MonitorStreamKey


def _stream() -> MonitorStreamKey:
    return MonitorStreamKey("stream", Metric.L2, "target-075", "cfg", "data", "flat", "hnsw")


def _sources(path: Path, count: int = 800):
    with SQLiteHostResponseCommitStore(
        path, stream_key=_stream(), source_revision="revision",
        environment_manifest_sha256="a" * 64,
    ) as store:
        for index in range(count):
            store.commit_response(
                CompletedRangeQueryObservation(
                    index, "2026-08-12T00:00:00Z", _stream(),
                    (float(index), 1.0), 0.75, 0.0, 100, 400,
                    ServedQueryOutcome(True, False, 1, 1.0),
                ),
                committed_at_utc="2026-08-12T00:00:00Z",
            )
        return store.poll(consumer_id="fixture", limit=count)


def _window(sources, sequence: int, *, ineligible: int | None = None):
    members = tuple(sources[sequence * 200:(sequence + 1) * 200])
    positions = tuple(
        build_v2_shadow_position(
            source=item,
            evaluation_eligible=offset != ineligible,
            reason_codes=() if offset != ineligible else ("SHADOW_FAILED",),
            evaluation_evidence_sha256=(f"{offset + 1:064x}"[-64:] if offset != ineligible else None),
        )
        for offset, item in enumerate(members)
    )
    return build_v2_shadow_window(sources=members, positions=positions)


def _decision(reference, current, *, state=DetectorState.NO_DRIFT):
    provenance = build_evidence_provenance(
        metric=_stream().metric,
        threshold_stratum=_stream().threshold_stratum,
        reference_window_id=reference.window_sequence,
        current_window_id=current.window_sequence,
        reference_manifest_sha256=reference.source_window_sha256,
        current_manifest_sha256=current.source_window_sha256,
        configuration_identity="cfg", data_identity="data",
        flat_binding_id="flat", hnsw_binding_id="hnsw",
        reference_audit_ids=tuple(range(50)),
        reference_audit_rank_digests=tuple("b" * 64 for _ in range(50)),
        current_audit_ids=tuple(range(50, 100)),
        current_audit_rank_digests=tuple("c" * 64 for _ in range(50)),
    )
    return DriftDecision(
        state=state,
        classification=(
            DriftClassification.INPUT_DRIFT
            if state is DetectorState.DRIFT else DriftClassification.NONE
        ),
        reason_codes=(state.value,),
        evidence_provenance=provenance,
    )


class HostWindowDetectorV2Tests(unittest.TestCase):
    def test_progression_gap_rebaseline_persisted_head_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = _sources(root / "source.sqlite3")
            path = root / "detector.sqlite3"
            with SQLiteHostWindowDetectorV2Store(path, stream_key=_stream()) as store:
                result0 = store.process_window(
                    window=_window(sources, 0), evaluator=_decision,
                    persisted_at_utc="2026-08-12T00:00:01Z",
                )
                result1 = store.process_window(
                    window=_window(sources, 1),
                    evaluator=lambda reference, current: _decision(
                        reference, current, state=DetectorState.DRIFT
                    ),
                    persisted_at_utc="2026-08-12T00:00:02Z",
                )
                result2 = store.process_window(
                    window=_window(sources, 2, ineligible=17), evaluator=_decision,
                    persisted_at_utc="2026-08-12T00:00:03Z",
                )
                result3 = store.process_window(
                    window=_window(sources, 3), evaluator=_decision,
                    persisted_at_utc="2026-08-12T00:00:04Z",
                )
                self.assertEqual(
                    (result0.status, result1.status, result2.status, result3.status),
                    (HostWindowV2Status.REBASELINE, HostWindowV2Status.EVALUATED,
                     HostWindowV2Status.WINDOW_UNEVALUABLE, HostWindowV2Status.REBASELINE),
                )
                self.assertIsNotNone(result1.detector_head)
                self.assertIsNone(result2.detector_head)
                self.assertIsNone(result3.detector_head)
            with SQLiteHostWindowDetectorV2Store(path, stream_key=_stream()) as reopened:
                latest = reopened.load_verified_latest(_stream())
                self.assertIsNone(latest)

    def test_incomplete_never_persists_and_source_substitution_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = _sources(root / "source.sqlite3", 200)
            incomplete = build_v2_shadow_window(sources=tuple(sources[:199]), positions=())
            with SQLiteHostWindowDetectorV2Store(root / "detector.sqlite3", stream_key=_stream()) as store:
                result = store.process_window(
                    window=incomplete, evaluator=_decision,
                    persisted_at_utc="2026-08-12T00:00:01Z",
                )
                self.assertEqual(result.status, HostWindowV2Status.WINDOW_INCOMPLETE)
                self.assertIsNone(store.load_verified_latest(_stream()))
            valid = _window(sources, 0)
            forged = object.__new__(V2ShadowPositionEvidence)
            for item in fields(valid.positions[0]):
                object.__setattr__(
                    forged, item.name,
                    "0" * 64 if item.name == "source_sha256" else getattr(valid.positions[0], item.name),
                )
            with self.assertRaises(HostWindowDetectorV2Error):
                build_v2_shadow_window(
                    sources=tuple(sources), positions=(forged, *valid.positions[1:])
                )

    def test_decision_must_bind_exact_source_windows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = _sources(root / "source.sqlite3", 400)
            with SQLiteHostWindowDetectorV2Store(root / "detector.sqlite3", stream_key=_stream()) as store:
                store.process_window(
                    window=_window(sources, 0), evaluator=_decision,
                    persisted_at_utc="2026-08-12T00:00:01Z",
                )
                # ADR-014 clarification: the head no longer compares
                # `provenance.*_manifest_sha256` (the AssembledShadowWindow
                # manifest domain) against `source_window_sha256` (the
                # committed-source membership domain) -- that equality was
                # invalid and only a fabricating evaluator could satisfy it.
                # Manifest-digest substitution is now proven in the correct
                # domain by the ADR-014 attestation
                # (`test_provenance_must_match_the_exact_assembled_manifests`).
                # What the head still binds is asserted here: provenance window
                # sequences and stream identity must match the exact windows.
                def substituted(reference, current, *, changes):
                    decision = _decision(reference, current)
                    values = {
                        "metric": Metric.L2, "threshold_stratum": "target-075",
                        "reference_window_id": reference.window_sequence,
                        "current_window_id": current.window_sequence,
                        "reference_manifest_sha256": reference.source_window_sha256,
                        "current_manifest_sha256": current.source_window_sha256,
                        "configuration_identity": "cfg", "data_identity": "data",
                        "flat_binding_id": "flat", "hnsw_binding_id": "hnsw",
                        "reference_audit_ids": tuple(range(50)),
                        "reference_audit_rank_digests": tuple("b" * 64 for _ in range(50)),
                        "current_audit_ids": tuple(range(50, 100)),
                        "current_audit_rank_digests": tuple("c" * 64 for _ in range(50)),
                    }
                    values.update(changes)
                    return DriftDecision(
                        decision.state, decision.classification,
                        evidence_provenance=build_evidence_provenance(**values),
                    )

                for label, changes in (
                    ("reference_window_id", {"reference_window_id": 97}),
                    ("current_window_id", {"current_window_id": 98}),
                    ("data_identity", {"data_identity": "other-data"}),
                    ("hnsw_binding_id", {"hnsw_binding_id": "other-hnsw"}),
                ):
                    with self.subTest(substitution=label):
                        with self.assertRaises(HostWindowDetectorV2Error) as raised:
                            store.process_window(
                                window=_window(sources, 1),
                                evaluator=lambda reference, current, _c=changes: substituted(
                                    reference, current, changes=_c
                                ),
                                persisted_at_utc="2026-08-12T00:00:02Z",
                            )
                        self.assertEqual(
                            raised.exception.code, "DETECTOR_V2_DECISION_INVALID"
                        )


class InsufficientEvidenceHeadTests(unittest.TestCase):
    """ADR-014 item 7: INSUFFICIENT_EVIDENCE heads are evidence-bearing but
    never trigger-bearing."""

    def test_insufficient_evidence_head_persists_and_restarts_identically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = _sources(root / "source.sqlite3", 400)
            path = root / "detector.sqlite3"
            with SQLiteHostWindowDetectorV2Store(path, stream_key=_stream()) as store:
                store.process_window(
                    window=_window(sources, 0), evaluator=_decision,
                    persisted_at_utc="2026-08-12T00:00:01Z",
                )
                result = store.process_window(
                    window=_window(sources, 1),
                    evaluator=lambda reference, current: _decision(
                        reference, current, state=DetectorState.INSUFFICIENT_EVIDENCE
                    ),
                    persisted_at_utc="2026-08-12T00:00:02Z",
                )
                self.assertEqual(result.status, HostWindowV2Status.EVALUATED)
                self.assertIsNotNone(result.detector_head)
                self.assertIs(
                    result.detector_head.detector_state,
                    DetectorState.INSUFFICIENT_EVIDENCE,
                )
                self.assertIs(
                    result.detector_head.detector_classification,
                    DriftClassification.NONE,
                )
                digest = result.detector_head.detector_head_sha256
            with SQLiteHostWindowDetectorV2Store(path, stream_key=_stream()) as reopened:
                latest = reopened.load_verified_latest(_stream())
                self.assertIsNotNone(latest)
                self.assertEqual(latest.head.detector_head_sha256, digest)
                self.assertIs(
                    latest.head.detector_state, DetectorState.INSUFFICIENT_EVIDENCE
                )
                # Evidence-bearing, never trigger-bearing.
                self.assertIsNone(reopened.latest_drift_head())

    def test_insufficient_evidence_requires_classification_none(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = _sources(root / "source.sqlite3", 400)
            with SQLiteHostWindowDetectorV2Store(
                root / "detector.sqlite3", stream_key=_stream()
            ) as store:
                store.process_window(
                    window=_window(sources, 0), evaluator=_decision,
                    persisted_at_utc="2026-08-12T00:00:01Z",
                )

                def inconsistent(reference, current):
                    base = _decision(
                        reference, current, state=DetectorState.INSUFFICIENT_EVIDENCE
                    )
                    return DriftDecision(
                        state=DetectorState.INSUFFICIENT_EVIDENCE,
                        classification=DriftClassification.INPUT_DRIFT,
                        reason_codes=base.reason_codes,
                        evidence_provenance=base.evidence_provenance,
                    )

                with self.assertRaises(HostWindowDetectorV2Error) as raised:
                    store.process_window(
                        window=_window(sources, 1), evaluator=inconsistent,
                        persisted_at_utc="2026-08-12T00:00:02Z",
                    )
                self.assertEqual(raised.exception.code, "DETECTOR_V2_DECISION_INVALID")

    def test_no_drift_with_classification_is_rejected_at_earliest_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = _sources(root / "source.sqlite3", 400)
            with SQLiteHostWindowDetectorV2Store(
                root / "detector.sqlite3", stream_key=_stream()
            ) as store:
                store.process_window(
                    window=_window(sources, 0), evaluator=_decision,
                    persisted_at_utc="2026-08-12T00:00:01Z",
                )

                def inconsistent(reference, current):
                    base = _decision(reference, current)
                    return DriftDecision(
                        state=DetectorState.NO_DRIFT,
                        classification=DriftClassification.INPUT_DRIFT,
                        reason_codes=base.reason_codes,
                        evidence_provenance=base.evidence_provenance,
                    )

                with self.assertRaises(HostWindowDetectorV2Error) as raised:
                    store.process_window(
                        window=_window(sources, 1), evaluator=inconsistent,
                        persisted_at_utc="2026-08-12T00:00:02Z",
                    )
                self.assertEqual(raised.exception.code, "DETECTOR_V2_DECISION_INVALID")

    def test_drift_head_still_reaches_latest_drift_head(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = _sources(root / "source.sqlite3", 400)
            with SQLiteHostWindowDetectorV2Store(
                root / "detector.sqlite3", stream_key=_stream()
            ) as store:
                store.process_window(
                    window=_window(sources, 0), evaluator=_decision,
                    persisted_at_utc="2026-08-12T00:00:01Z",
                )
                store.process_window(
                    window=_window(sources, 1),
                    evaluator=lambda reference, current: _decision(
                        reference, current, state=DetectorState.DRIFT
                    ),
                    persisted_at_utc="2026-08-12T00:00:02Z",
                )
                self.assertIsNotNone(store.latest_drift_head())


if __name__ == "__main__":
    unittest.main()
