"""Offline durability tests for the EXP-009 lifecycle audit log."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from vdbench.canary_lifecycle_audit import (
    CanaryLifecycleAuditRecord,
    JsonlCanaryLifecycleAuditSink,
    LifecycleAuditCorruptedError,
    LifecycleAuditDuplicateError,
    lifecycle_event_id,
)


class CanaryLifecycleAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.path = Path(self._temporary.name) / "lifecycle.jsonl"

    def tearDown(self) -> None:
        self._temporary.cleanup()

    def _record(self, event_type: str = "ACTIVATION_AUTHORIZED") -> CanaryLifecycleAuditRecord:
        event_id = lifecycle_event_id(
            grant_id="grant-exp009-001", signed_payload_sha256="a" * 64,
            plan_sha256="b" * 64, event_type=event_type,
        )
        return CanaryLifecycleAuditRecord(
            event_id=event_id, event_type=event_type, grant_id="grant-exp009-001",
            signed_payload_sha256="a" * 64, policy_audit_id="policy-audit-001",
            plan_sha256="b" * 64, configuration_identity="config-v1", data_identity="data-v1",
            flat_binding_id="flat-v1", hnsw_binding_id="hnsw-v1",
            recorded_at_utc="2026-08-04T09:00:00Z", reason_code="ACTIVATION_PENDING",
        )

    def test_append_restart_readback_and_deterministic_event_identity(self) -> None:
        record = self._record()
        sink = JsonlCanaryLifecycleAuditSink(self.path)
        sink.append(record)
        restarted = JsonlCanaryLifecycleAuditSink(self.path)

        self.assertTrue(restarted.contains(record.event_id))
        self.assertEqual(restarted.records(), (record,))
        self.assertEqual(record.event_id, self._record().event_id)

    def test_duplicate_event_and_corruption_fail_closed(self) -> None:
        sink = JsonlCanaryLifecycleAuditSink(self.path)
        sink.append(self._record())
        with self.assertRaises(LifecycleAuditDuplicateError):
            sink.append(self._record())
        self.path.write_text("{bad-json\n", encoding="utf-8")
        with self.assertRaises(LifecycleAuditCorruptedError):
            sink.records()


if __name__ == "__main__":
    unittest.main()
