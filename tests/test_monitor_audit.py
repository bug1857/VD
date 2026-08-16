"""Contract tests for the restart-durable workload-monitor audit sink."""

from __future__ import annotations

import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from vdbench.config import Metric
from vdbench.shadow_event_types import MonitorStreamKey
from vdbench.workload_monitor import MonitorAuditRecord, MonitorRecordStatus


class MonitorAuditSinkTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.path = self.root / "monitor-audit.jsonl"
        self.key = MonitorStreamKey(
            stream_id="exp008-l2-stationary",
            metric=Metric.L2,
            threshold_stratum="target-075",
            configuration_identity="config-v1",
            data_identity="data-v1",
            flat_binding_id="flat-v1",
            hnsw_binding_id="hnsw-v1",
        )

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _record(self, record_id: str = "audit-1") -> MonitorAuditRecord:
        return MonitorAuditRecord(
            record_id=record_id,
            stream_key=self.key,
            window_id="window-2",
            window_sequence=2,
            event_ids=("event-1", "event-2"),
            event_trace_sha256=("a" * 64, "b" * 64),
            status=MonitorRecordStatus.EVALUATED,
            reason_codes=(),
            manifest_sha256="c" * 64,
            detector_state="NO_DRIFT",
            detector_classification="NO_DRIFT",
            policy_action="NO_CHANGE",
            policy_reason="DETECTOR_NO_DRIFT",
            policy_audit_id="policy-audit-1",
        )

    def test_append_contains_and_restart_read_are_exact(self) -> None:
        from vdbench.monitor_audit import FileMonitorAuditSink

        sink = FileMonitorAuditSink(self.path)
        record = self._record()
        sink.append(record)

        restarted = FileMonitorAuditSink(self.path)
        self.assertTrue(restarted.contains("audit-1"))
        self.assertEqual(restarted.read_records(), (record,))

    def test_duplicate_record_id_is_rejected_without_append(self) -> None:
        from vdbench.monitor_audit import (
            DuplicateMonitorAuditRecordError,
            FileMonitorAuditSink,
        )

        sink = FileMonitorAuditSink(self.path)
        sink.append(self._record())
        with self.assertRaises(DuplicateMonitorAuditRecordError):
            sink.append(self._record())
        self.assertEqual(sink.read_records(), (self._record(),))

    def test_malformed_jsonl_fails_closed_for_contains_and_reads(self) -> None:
        from vdbench.monitor_audit import (
            FileMonitorAuditSink,
            MonitorAuditLogCorruptedError,
        )

        self.path.write_text("{not-json}\n", encoding="utf-8")
        sink = FileMonitorAuditSink(self.path)
        with self.assertRaises(MonitorAuditLogCorruptedError):
            sink.contains("audit-1")
        with self.assertRaises(MonitorAuditLogCorruptedError):
            sink.read_records()

    def test_unknown_schema_field_fails_closed(self) -> None:
        from vdbench.monitor_audit import (
            FileMonitorAuditSink,
            MonitorAuditLogCorruptedError,
        )

        self.path.write_text(
            '{"record":{},"schema_version":"workload-monitor-audit-v1","unexpected":true}\n',
            encoding="utf-8",
        )
        with self.assertRaises(MonitorAuditLogCorruptedError):
            FileMonitorAuditSink(self.path).read_records()

    def test_invalid_outgoing_record_is_rejected_before_any_write(self) -> None:
        from vdbench.monitor_audit import (
            FileMonitorAuditSink,
            MonitorAuditLogCorruptedError,
        )

        invalid = replace(self._record(), event_trace_sha256=("not-a-sha256",))
        with self.assertRaises(MonitorAuditLogCorruptedError):
            FileMonitorAuditSink(self.path).append(invalid)
        self.assertFalse(self.path.exists())

    def test_concurrent_distinct_appends_preserve_both_records(self) -> None:
        from vdbench.monitor_audit import FileMonitorAuditSink

        first = FileMonitorAuditSink(self.path)
        second = FileMonitorAuditSink(self.path)
        failures: list[BaseException] = []

        def append(sink: FileMonitorAuditSink, record_id: str) -> None:
            try:
                sink.append(self._record(record_id))
            except BaseException as exc:  # pragma: no cover - failure assertion below  # noqa: BLE001
                failures.append(exc)

        threads = (
            threading.Thread(target=append, args=(first, "audit-1")),
            threading.Thread(target=append, args=(second, "audit-2")),
        )
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(failures, [])
        self.assertEqual(
            {record.record_id for record in FileMonitorAuditSink(self.path).read_records()},
            {"audit-1", "audit-2"},
        )

    def test_symlink_log_path_is_rejected(self) -> None:
        from vdbench.monitor_audit import (
            FileMonitorAuditSink,
            MonitorAuditLogCorruptedError,
        )

        target = self.root / "target.jsonl"
        target.write_text("", encoding="utf-8")
        self.path.symlink_to(target)
        with self.assertRaises(MonitorAuditLogCorruptedError):
            FileMonitorAuditSink(self.path).append(self._record())


if __name__ == "__main__":
    unittest.main()
