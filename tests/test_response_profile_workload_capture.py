from __future__ import annotations

import ast
import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from vdbench.config import Metric
from vdbench.drift import (
    DetectorState,
    DriftClassification,
    build_evidence_provenance,
)
from vdbench.response_profile_detector_head import build_response_profile_detector_head
from vdbench.response_profile_monitor_store import ResponseProfileMonitorStateStore
from vdbench.response_profile_vector_material import (
    load_response_profile_vector_material,
)
from vdbench.response_profile_workload_capture import (
    CAPTURE_EVIDENCE_STATUS,
    CapturePhase,
    GenuineWorkloadObservation,
    ResponseProfileWorkloadCapture,
    ResponseProfileWorkloadCaptureError,
    build_capture_environment_identity,
)
from vdbench.shadow_event_types import MonitorStreamKey
from vdbench.workload_monitor import MonitorStreamState


def _sha(character: str) -> str:
    return character * 64


def _normalized_imports(source: str, *, package: str = "vdbench") -> set[str]:
    """Resolve absolute and package-relative imports to canonical module names."""

    imported: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package.split(".")
                if node.level > len(base):
                    imported.add("<invalid-relative-import>")
                    continue
                base = base[: len(base) - (node.level - 1)]
                if node.module:
                    imported.add(".".join((*base, node.module)))
                else:
                    imported.update(
                        ".".join((*base, alias.name)) for alias in node.names
                    )
            elif node.module:
                imported.add(node.module)
    return imported


_FORBIDDEN_CAPTURE_IMPORT_PREFIXES = (
    "pymilvus",
    "vdbench.pymilvus",
    "vdbench.milvus",
    "vdbench.milvus_actuation",
    "vdbench.milvus_host_executor",
    "vdbench.milvus_serving",
    "vdbench.response_profile_milvus_adapter",
    "vdbench.lkg_milvus_adapter",
    "vdbench.policy",
    "vdbench.actuation",
    "vdbench.canary_routing",
    "vdbench.canary_activation",
    "vdbench.canary_authorization",
    "vdbench.canary_grant_store",
    "vdbench.canary_query_source",
    "vdbench.exp008_acquisition",
    "vdbench.dataset",
    "vdbench.dataset002",
    "vdbench.dataset003",
)


def _contains_forbidden_import(imports: set[str]) -> bool:
    return any(
        name == prefix or name.startswith(prefix + ".")
        for name in imports
        for prefix in _FORBIDDEN_CAPTURE_IMPORT_PREFIXES
    )


class FakeSource:
    """Structural test source; it never generates or sends a query."""

    def __init__(self, observations=()) -> None:
        self.observations = list(observations)
        self.acknowledged: list[str] = []
        self.poll_count = 0

    def poll(self, *, limit: int):
        self.poll_count += 1
        return tuple(self.observations[:limit])

    def acknowledge(self, event_ids: tuple[str, ...]) -> None:
        self.acknowledged.extend(event_ids)
        acknowledged = set(event_ids)
        self.observations = [item for item in self.observations if item.event_id not in acknowledged]


class FakeMetadataProvider:
    def __init__(self, value) -> None:
        self.value = value

    def capture(self):
        return self.value


class ResponseProfileWorkloadCaptureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.stream = MonitorStreamKey(
            "genuine-stream",
            Metric.L2,
            "target-075",
            "configuration",
            "data",
            "flat",
            "hnsw",
        )
        self.metadata = build_capture_environment_identity(
            milvus_uri="http://milvus.invalid:19530",
            deployment_identity="deployment",
            collection_name="collection",
            dimensions=4,
            metric=Metric.L2,
            hnsw_index_identity="hnsw",
            data_identity="data",
            source_revision="revision",
            observed_at_utc="2026-08-12T00:00:00Z",
            environment_manifest={"health": {"etcd": "healthy", "minio": "healthy"}},
        )
        self.monitor_path = self.root / "monitor.sqlite3"
        self.monitor = ResponseProfileMonitorStateStore(
            self.monitor_path,
            expected_stream_key=self.stream,
            utc_now=lambda: "2026-08-12T00:00:01Z",
        )

    def tearDown(self) -> None:
        self.monitor.close()
        self.directory.cleanup()

    def _provenance(self, *, current: str = "current"):
        return build_evidence_provenance(
            metric=Metric.L2,
            threshold_stratum="target-075",
            reference_window_id="reference",
            current_window_id=current,
            reference_manifest_sha256=_sha("a"),
            current_manifest_sha256=_sha("b"),
            configuration_identity="configuration",
            data_identity="data",
            flat_binding_id="flat",
            hnsw_binding_id="hnsw",
            reference_audit_ids=tuple(range(50)),
            reference_audit_rank_digests=tuple(_sha("c") for _ in range(50)),
            current_audit_ids=tuple(range(50, 100)),
            current_audit_rank_digests=tuple(_sha("d") for _ in range(50)),
        )

    def _persist_trigger(self, *, window_sequence: int = 10) -> None:
        head = build_response_profile_detector_head(
            stream_key=self.stream,
            window_sequence=window_sequence,
            detector_state=DetectorState.DRIFT,
            detector_classification=DriftClassification.INPUT_DRIFT,
            detector_provenance=self._provenance(),
        )
        self.monitor.save(
            MonitorStreamState(
                stream_key=self.stream,
                next_window_sequence=window_sequence + 1,
                latest_detector_head=head,
            )
        )

    def _observation(self, index: int, **changes: object) -> GenuineWorkloadObservation:
        window_sequence = 11 + index // 200
        within = index % 200
        values = {
            "event_id": f"event-{index:04d}",
            "source_sequence": window_sequence * 200 + within,
            "window_sequence": window_sequence,
            "within_window_index": within,
            "query_id": f"genuine-{index:04d}",
            "observed_at_utc": "2026-08-12T00:01:00Z",
            "stream_key": self.stream,
            "source_revision": "revision",
            "environment_manifest_sha256": self.metadata.environment_manifest_sha256,
            "query_vector": (float(index), float(index + 1), float(index + 2), float(index + 3)),
            "threshold_radius": 0.75,
            "range_filter": 0.0,
            "limit": 100,
            "consistency_level": "Strong",
        }
        values.update(changes)
        return GenuineWorkloadObservation(**values)

    def _capture(self, source: object, *, path: Path | None = None):
        return ResponseProfileWorkloadCapture(
            ledger_path=path or self.root / "capture.sqlite3",
            run_id="exp010-capture-001",
            created_at_utc="2026-08-12T00:00:02Z",
            stream_key=self.stream,
            source_workload_manifest_sha256=_sha("e"),
            source_revision="revision",
            monitor_store=self.monitor,
            source=source,
            metadata_provider=FakeMetadataProvider(self.metadata),
        )

    def _assert_invalid(self, observation: GenuineWorkloadObservation, code: str) -> None:
        self._persist_trigger()
        with self._capture(FakeSource((observation,))) as capture:
            with self.assertRaises(ResponseProfileWorkloadCaptureError) as raised:
                capture.run_once(max_observations=1)
            self.assertEqual(raised.exception.code, code)
            self.assertIs(capture.phase, CapturePhase.INVALID)

    def test_no_persisted_drift_trigger_means_no_source_observation(self) -> None:
        source = FakeSource((self._observation(0),))
        with self._capture(source) as capture:
            self.assertEqual(capture.run_once(max_observations=1), 0)
            self.assertIs(capture.phase, CapturePhase.OBSERVING)
        self.assertEqual(source.poll_count, 0)

    def test_trigger_transition_is_not_a_public_bypass(self) -> None:
        self.assertFalse(hasattr(ResponseProfileWorkloadCapture, "observe_trigger"))

    def test_complete_sequence_freezes_exact_roles_order_and_pipeline_material(self) -> None:
        self._persist_trigger()
        source = FakeSource(tuple(self._observation(index) for index in range(1400)))
        with self._capture(source) as capture:
            self.assertEqual(capture.run_once(max_observations=1400), 1400)
            self.assertIs(capture.phase, CapturePhase.CAPTURE_COMPLETE)
            artifacts = capture.publish(self.root / "published")

        self.assertEqual(len(artifacts.warmup_role_manifest.members), 200)
        self.assertEqual(len(artifacts.population.calibration_role_manifest.members), 1200)
        warmup_ids = tuple(item.query_identity.query_id for item in artifacts.warmup_role_manifest.members)
        calibration_ids = tuple(
            item.query_identity.query_id
            for item in artifacts.population.calibration_role_manifest.members
        )
        self.assertEqual(warmup_ids, tuple(f"genuine-{index:04d}" for index in range(200)))
        self.assertEqual(calibration_ids, tuple(f"genuine-{index:04d}" for index in range(200, 1400)))
        self.assertTrue(set(warmup_ids).isdisjoint(calibration_ids))

        vector_document = json.loads(artifacts.vector_material_path.read_text())
        verified_vectors = load_response_profile_vector_material(vector_document)
        self.assertEqual(
            verified_vectors.population.workload_manifest_sha256,
            artifacts.population.workload_manifest_sha256,
        )
        self.assertEqual(
            verified_vectors.warmup_role_manifest.role_manifest_sha256,
            artifacts.warmup_role_manifest.role_manifest_sha256,
        )
        manifest = json.loads(artifacts.capture_manifest_path.read_text())
        self.assertEqual(
            manifest["capture_manifest_payload"]["evidence_status"],
            CAPTURE_EVIDENCE_STATUS,
        )
        self.assertEqual(manifest["capture_manifest_payload"]["warmup_window_sequence"], 11)
        self.assertEqual(
            manifest["capture_manifest_payload"]["calibration_window_sequences"],
            [12, 13, 14, 15, 16, 17],
        )
        self.assertEqual(manifest["capture_manifest_payload"]["first_source_sequence"], 2200)
        self.assertEqual(manifest["capture_manifest_payload"]["last_source_sequence"], 3599)
        self.assertEqual(manifest["capture_manifest_payload"]["capture_event_count"], 1404)
        self.assertEqual(len(tuple((self.root / "published").iterdir())), 5)
        with (
            self._capture(FakeSource()) as reopened,
            self.assertRaises(ResponseProfileWorkloadCaptureError) as raised,
        ):
            reopened.publish(self.root / "published")
        self.assertEqual(raised.exception.code, "CAPTURE_OUTPUT_EXISTS")

    def test_first_200_freeze_warmup_before_calibration(self) -> None:
        self._persist_trigger()
        source = FakeSource(tuple(self._observation(index) for index in range(201)))
        with self._capture(source) as capture:
            self.assertEqual(capture.run_once(max_observations=200), 200)
            self.assertIs(capture.phase, CapturePhase.WARMUP_FROZEN)
            self.assertEqual(capture.run_once(max_observations=1), 1)
            self.assertIs(capture.phase, CapturePhase.WARMUP_FROZEN)

    def test_pretrigger_or_skipped_window_is_rejected(self) -> None:
        for changed in (
            {"window_sequence": 10, "source_sequence": 2000},
            {"window_sequence": 12, "source_sequence": 2400},
            {"within_window_index": 1, "source_sequence": 2201},
        ):
            with self.subTest(changed=changed):
                self.monitor.close()
                child = self.root / str(len(list(self.root.iterdir())))
                child.mkdir(mode=0o700)
                self.monitor_path = child / "monitor.sqlite3"
                self.monitor = ResponseProfileMonitorStateStore(
                    self.monitor_path,
                    expected_stream_key=self.stream,
                    utc_now=lambda: "2026-08-12T00:00:01Z",
                )
                self._assert_invalid(self._observation(0, **changed), "QUERY_SEQUENCE_NON_CONSECUTIVE")

    def test_repeated_or_skipped_source_sequence_is_rejected(self) -> None:
        for second_sequence in (2200, 2202):
            with self.subTest(second_sequence=second_sequence):
                self.monitor.close()
                child = self.root / f"source-{second_sequence}"
                child.mkdir(mode=0o700)
                self.monitor = ResponseProfileMonitorStateStore(
                    child / "monitor.sqlite3", expected_stream_key=self.stream
                )
                self._persist_trigger()
                source = FakeSource(
                    (self._observation(0), self._observation(1, source_sequence=second_sequence))
                )
                with self._capture(source, path=child / "capture.sqlite3") as capture:
                    with self.assertRaises(ResponseProfileWorkloadCaptureError) as raised:
                        capture.run_once(max_observations=2)
                    self.assertEqual(raised.exception.code, "QUERY_SEQUENCE_NON_CONSECUTIVE")

    def test_duplicate_identity_vector_or_payload_is_rejected(self) -> None:
        cases = (
            {"query_id": "genuine-0000"},
            {"query_vector": self._observation(0).query_vector},
            {
                "query_vector": self._observation(0).query_vector,
                "query_id": "different",
            },
        )
        for changes in cases:
            with self.subTest(changes=changes):
                self.monitor.close()
                child = self.root / f"duplicate-{len(list(self.root.iterdir()))}"
                child.mkdir(mode=0o700)
                self.monitor = ResponseProfileMonitorStateStore(
                    child / "monitor.sqlite3", expected_stream_key=self.stream
                )
                self._persist_trigger()
                source = FakeSource((self._observation(0), self._observation(1, **changes)))
                with self._capture(source, path=child / "capture.sqlite3") as capture:
                    with self.assertRaises(ResponseProfileWorkloadCaptureError) as raised:
                        capture.run_once(max_observations=2)
                    self.assertEqual(raised.exception.code, "CAPTURE_QUERY_DUPLICATE")

    def test_stream_source_revision_and_environment_changes_fail_closed(self) -> None:
        other_stream = replace(self.stream, stream_id="other")
        cases = (
            (self._observation(0, stream_key=other_stream), "CAPTURE_STREAM_CHANGED"),
            (self._observation(0, source_revision="other"), "CAPTURE_SOURCE_REVISION_CHANGED"),
            (self._observation(0, environment_manifest_sha256=_sha("f")), "CAPTURE_ENVIRONMENT_CHANGED"),
            (self._observation(0, query_vector=(1.0, 2.0)), "CAPTURE_DIMENSIONS_CHANGED"),
        )
        for observation, code in cases:
            with self.subTest(code=code):
                self.monitor.close()
                child = self.root / code
                child.mkdir(mode=0o700)
                self.monitor = ResponseProfileMonitorStateStore(
                    child / "monitor.sqlite3", expected_stream_key=self.stream
                )
                self._persist_trigger()
                with self._capture(FakeSource((observation,)), path=child / "capture.sqlite3") as capture:
                    with self.assertRaises(ResponseProfileWorkloadCaptureError) as raised:
                        capture.run_once(max_observations=1)
                    self.assertEqual(raised.exception.code, code)
                    self.assertIs(capture.phase, CapturePhase.INVALID)

    def test_exact_redelivery_is_idempotent_but_substitution_is_rejected(self) -> None:
        self._persist_trigger()
        original = self._observation(0)
        source = FakeSource((original,))
        path = self.root / "capture.sqlite3"
        with self._capture(source, path=path) as capture:
            self.assertEqual(capture.run_once(max_observations=1), 1)
        source.observations = [original]
        with self._capture(source, path=path) as reopened:
            self.assertEqual(reopened.run_once(max_observations=1), 1)
        source.observations = [replace(original, query_vector=(9.0, 8.0, 7.0, 6.0))]
        with self._capture(source, path=path) as reopened:
            with self.assertRaises(ResponseProfileWorkloadCaptureError) as raised:
                reopened.run_once(max_observations=1)
            self.assertEqual(raised.exception.code, "CAPTURE_EVENT_REDELIVERY_MISMATCH")

    def test_restart_reconstructs_partial_state_and_continues_only_next_query(self) -> None:
        self._persist_trigger()
        path = self.root / "capture.sqlite3"
        first = FakeSource(tuple(self._observation(index) for index in range(50)))
        with self._capture(first, path=path) as capture:
            self.assertEqual(capture.run_once(max_observations=50), 50)
        second = FakeSource(tuple(self._observation(index) for index in range(50, 200)))
        with self._capture(second, path=path) as reopened:
            self.assertEqual(reopened.run_once(max_observations=150), 150)
            self.assertIs(reopened.phase, CapturePhase.WARMUP_FROZEN)

    def test_detector_head_substitution_is_rejected(self) -> None:
        self._persist_trigger()
        source = FakeSource()
        with self._capture(source) as capture:
            self.assertEqual(capture.run_once(max_observations=1), 0)
            source.observations = [self._observation(0)]
            latest = self.monitor.load_verified_latest(self.stream)
            forged = object.__new__(type(latest))
            for name in ("head", "head_record_sequence", "head_record_sha256", "head_record_persisted_at_utc"):
                object.__setattr__(forged, name, getattr(latest, name))
            object.__setattr__(forged, "head_record_sha256", _sha("f"))
            with (
                patch.object(self.monitor, "load_verified_latest", return_value=forged),
                self.assertRaises(ResponseProfileWorkloadCaptureError) as raised,
            ):
                capture.run_once(max_observations=1)
            self.assertEqual(raised.exception.code, "DETECTOR_HEAD_SUBSTITUTED")

    def test_normal_later_detector_head_does_not_replace_frozen_trigger(self) -> None:
        persisted_times = iter(
            ("2026-08-12T00:00:01Z", "2026-08-12T00:00:02Z")
        )
        self.monitor._utc_now = lambda: next(persisted_times)
        self._persist_trigger()
        source = FakeSource()
        with self._capture(source) as capture:
            self.assertEqual(capture.run_once(max_observations=1), 0)
            later = build_response_profile_detector_head(
                stream_key=self.stream,
                window_sequence=11,
                detector_state=DetectorState.NO_DRIFT,
                detector_classification=DriftClassification.NONE,
                detector_provenance=self._provenance(current="later"),
            )
            self.monitor.save(
                MonitorStreamState(
                    stream_key=self.stream,
                    next_window_sequence=12,
                    latest_detector_head=later,
                )
            )
            source.observations = [self._observation(0)]
            self.assertEqual(capture.run_once(max_observations=1), 1)

    def test_non_store_issued_trigger_is_rejected(self) -> None:
        self._persist_trigger()
        raw_head = self.monitor.load_verified_latest(self.stream).head
        with self._capture(FakeSource()) as capture:
            with (
                patch.object(self.monitor, "load_verified_latest", return_value=raw_head),
                self.assertRaises(ResponseProfileWorkloadCaptureError) as raised,
            ):
                capture.run_once(max_observations=1)
            self.assertEqual(raised.exception.code, "DETECTOR_TRIGGER_INVALID")
            self.assertIs(capture.phase, CapturePhase.INVALID)

    def test_non_drift_head_cannot_trigger_capture(self) -> None:
        head = build_response_profile_detector_head(
            stream_key=self.stream,
            window_sequence=10,
            detector_state=DetectorState.NO_DRIFT,
            detector_classification=DriftClassification.NONE,
            detector_provenance=self._provenance(),
        )
        self.monitor.save(MonitorStreamState(self.stream, latest_detector_head=head))
        source = FakeSource((self._observation(0),))
        with self._capture(source) as capture:
            self.assertEqual(capture.run_once(max_observations=1), 0)
        self.assertEqual(source.poll_count, 0)

    def test_ambiguous_ledger_tamper_fails_closed(self) -> None:
        self._persist_trigger()
        path = self.root / "capture.sqlite3"
        with self._capture(FakeSource((self._observation(0),)), path=path) as capture:
            capture.run_once(max_observations=1)
        connection = sqlite3.connect(path)
        connection.execute("DROP TRIGGER capture_events_no_update")
        connection.execute("UPDATE capture_events SET kind='QUERY_CAPTURED' WHERE event_seq=0")
        connection.commit()
        connection.close()
        with self.assertRaises(ResponseProfileWorkloadCaptureError):
            self._capture(FakeSource(), path=path)

    def test_absent_source_is_reported_not_synthesized(self) -> None:
        with self.assertRaises(ResponseProfileWorkloadCaptureError) as raised:
            self._capture(object())
        self.assertEqual(raised.exception.code, "WORKLOAD_SOURCE_UNAVAILABLE")

    def test_metadata_value_is_immutable_and_reconstructively_validated(self) -> None:
        self.assertIs(type(self.metadata.environment_manifest_canonical_json), bytes)
        forged = object.__new__(type(self.metadata))
        for name in self.metadata.__slots__:
            object.__setattr__(forged, name, getattr(self.metadata, name))
        object.__setattr__(forged, "environment_manifest_canonical_json", b'{}')
        self._persist_trigger()
        with self._capture(FakeSource()) as capture:
            capture._metadata_provider.value = forged
            with self.assertRaises(ResponseProfileWorkloadCaptureError):
                capture.run_once(max_observations=1)
            self.assertIs(capture.phase, CapturePhase.INVALID)

    def test_static_boundary_has_no_historical_live_or_authority_dependencies(self) -> None:
        module_path = Path(__file__).parents[1] / "src/vdbench/response_profile_workload_capture.py"
        self.assertFalse(
            _contains_forbidden_import(_normalized_imports(module_path.read_text()))
        )
        representative_relative_bypasses = (
            "from . import pymilvus",
            "from . import policy",
            "from .milvus import Client",
            "from .milvus_actuation import Boundary",
            "from .canary_routing import select_canary_routes",
            "from .canary_grant_store import Grant",
            "from .exp008_acquisition import capture",
            "from .dataset import generate",
            "from .canary_query_source import source",
        )
        for statement in representative_relative_bypasses:
            with self.subTest(statement=statement):
                self.assertTrue(
                    _contains_forbidden_import(_normalized_imports(statement))
                )
        source = module_path.read_text()
        for token in ("DATASET-001", "DATASET-002", "DATASET-003", "EXP-008", "random"):
            self.assertNotIn(token, source)


if __name__ == "__main__":
    unittest.main()
