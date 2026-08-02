from __future__ import annotations

import ast
import json
import multiprocessing
from pathlib import Path
import tempfile
import unittest

from vdbench.actuation import (
    ActuationAuditRecord,
    ActuationContext,
    ActuationOutcome,
)
from vdbench.actuation_persistence import (
    REENABLE_CONFIRMATION_TOKEN,
    AuditLogCorruptedError,
    DuplicateAuditIdError,
    FileAutomaticActionController,
    JsonlAuditSink,
)
from vdbench.config import Metric
from vdbench.policy import (
    PolicyAction,
    QualificationResult,
    SafetyGateResult,
)

REPOSITORY = Path(__file__).parents[1]
ACTUATION_MODULE = REPOSITORY / "src" / "vdbench" / "actuation.py"
PERSISTENCE_MODULE = REPOSITORY / "src" / "vdbench" / "actuation_persistence.py"
FIXED_TIMESTAMP = "2026-08-03T18:30:00Z"


def audit_record(audit_id: str) -> ActuationAuditRecord:
    qualification = QualificationResult(
        qualified=True,
        ef=400,
        reasons=(),
        metric=Metric.L2,
        threshold_stratum="target-025",
        configuration_identity="config-v1",
        index_identity="index-v1",
        data_identity="data-v1",
        qualifying_window_ids=("window-10", "window-11"),
    )
    context = ActuationContext(
        metric=Metric.L2,
        threshold_stratum="target-025",
        collection_name="vd-l2-hnsw",
        configuration_identity="config-v1",
        index_identity="index-v1",
        data_identity="data-v1",
        audited_query_ids=tuple(range(50)),
        last_known_good=qualification,
        occurred_at_utc=FIXED_TIMESTAMP,
    )
    return ActuationAuditRecord(
        audit_id=audit_id,
        action=PolicyAction.NO_CHANGE,
        outcome=ActuationOutcome.NO_OP,
        attempted=False,
        success=True,
        reason="NON_ACTIONABLE_POLICY_DECISION",
        context=context,
        current_ef=400,
        candidate_ef=None,
        last_known_good_ef=400,
        traffic_fraction=None,
        policy_reason="stationary workload",
        safety_gate_results=(
            SafetyGateResult(
                name="PRE_ACTION_READY",
                passed=True,
                detail="fixture gate",
            ),
        ),
    )


def _append_worker(
    path: str,
    audit_id: str,
    start: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
) -> None:
    start.wait(timeout=10)
    try:
        JsonlAuditSink(path).append(audit_record(audit_id))
    except Exception as exc:
        results.put((audit_id, type(exc).__name__, str(exc)))
    else:
        results.put((audit_id, "OK", ""))


def _contains_worker(
    path: str,
    audit_id: str,
    results: multiprocessing.queues.Queue,
) -> None:
    try:
        found = JsonlAuditSink(path).contains(audit_id)
    except Exception as exc:
        results.put((type(exc).__name__, str(exc)))
    else:
        results.put(("OK", found))


def _disable_worker(path: str, results: multiprocessing.queues.Queue) -> None:
    try:
        FileAutomaticActionController(
            path,
            clock=lambda: FIXED_TIMESTAMP,
        ).disable_automatic_actions(
            audit_id="rollback-audit-001",
            reason="ROLLBACK_VERIFICATION_FAILED",
        )
    except Exception as exc:
        results.put((type(exc).__name__, str(exc)))
    else:
        results.put(("OK", True))


def _is_disabled_worker(path: str, results: multiprocessing.queues.Queue) -> None:
    results.put(
        (
            "OK",
            FileAutomaticActionController(
                path,
                clock=lambda: FIXED_TIMESTAMP,
            ).is_disabled(),
        )
    )


class JsonlAuditSinkTests(unittest.TestCase):
    def test_append_then_contains_finds_exact_audit_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            sink = JsonlAuditSink(path)

            sink.append(audit_record("audit-001"))

            self.assertTrue(sink.contains("audit-001"))
            self.assertFalse(sink.contains("audit-002"))
            envelope = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(envelope["schema_version"], 1)
            self.assertEqual(envelope["record"]["audit_id"], "audit-001")
            self.assertTrue(path.read_bytes().endswith(b"\n"))

    def test_duplicate_audit_id_is_rejected_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            sink = JsonlAuditSink(path)
            expected = audit_record("audit-duplicate")
            sink.append(expected)
            before = path.read_bytes()

            with self.assertRaises(DuplicateAuditIdError):
                sink.append(expected)

            self.assertEqual(path.read_bytes(), before)

    def test_malformed_jsonl_fails_closed_and_is_never_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            path.write_text('{"schema_version":1,"record":\n', encoding="utf-8")
            sink = JsonlAuditSink(path)

            with self.assertRaises(AuditLogCorruptedError):
                sink.contains("audit-001")
            with self.assertRaises(AuditLogCorruptedError):
                sink.append(audit_record("audit-002"))

    def test_separate_process_reader_observes_persisted_audit(self) -> None:
        process_context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            JsonlAuditSink(path).append(audit_record("audit-restart"))
            results = process_context.Queue()
            reader = process_context.Process(
                target=_contains_worker,
                args=(str(path), "audit-restart", results),
            )

            reader.start()
            reader.join(timeout=10)

            self.assertEqual(reader.exitcode, 0)
            self.assertEqual(results.get(timeout=2), ("OK", True))

    def test_concurrent_appends_are_serialized_and_duplicate_still_rejected(
        self,
    ) -> None:
        process_context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            start = process_context.Event()
            results = process_context.Queue()
            workers = [
                process_context.Process(
                    target=_append_worker,
                    args=(str(path), audit_id, start, results),
                )
                for audit_id in ("audit-concurrent-a", "audit-concurrent-b")
            ]
            for worker in workers:
                worker.start()
            start.set()
            for worker in workers:
                worker.join(timeout=10)

            self.assertEqual([worker.exitcode for worker in workers], [0, 0])
            outcomes = {results.get(timeout=2) for _ in workers}
            self.assertEqual(
                outcomes,
                {
                    ("audit-concurrent-a", "OK", ""),
                    ("audit-concurrent-b", "OK", ""),
                },
            )
            sink = JsonlAuditSink(path)
            self.assertTrue(sink.contains("audit-concurrent-a"))
            self.assertTrue(sink.contains("audit-concurrent-b"))
            self.assertEqual(len(path.read_text(encoding="utf-8").splitlines()), 2)
            with self.assertRaises(DuplicateAuditIdError):
                sink.append(audit_record("audit-concurrent-a"))


class FileAutomaticActionControllerTests(unittest.TestCase):
    def controller(self, path: Path) -> FileAutomaticActionController:
        return FileAutomaticActionController(path, clock=lambda: FIXED_TIMESTAMP)

    def test_missing_state_is_not_disabled_and_corrupt_state_is_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "automatic-actions.json"
            controller = self.controller(path)

            self.assertFalse(controller.is_disabled())
            corrupt_states = (
                "not-json",
                json.dumps(
                    {
                        "schema_version": 2,
                        "state": "ENABLED",
                        "audit_id": None,
                        "reason": "fixture",
                        "changed_at_utc": FIXED_TIMESTAMP,
                        "confirmed_by": "operator",
                    }
                ),
            )
            for payload in corrupt_states:
                with self.subTest(payload=payload):
                    path.write_text(payload, encoding="utf-8")
                    self.assertTrue(controller.is_disabled())

    def test_disable_is_restart_durable_and_records_required_evidence(self) -> None:
        process_context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "automatic-actions.json"
            results = process_context.Queue()
            writer = process_context.Process(
                target=_disable_worker,
                args=(str(path), results),
            )
            writer.start()
            writer.join(timeout=10)
            self.assertEqual(writer.exitcode, 0)
            self.assertEqual(results.get(timeout=2), ("OK", True))

            reader = process_context.Process(
                target=_is_disabled_worker,
                args=(str(path), results),
            )
            reader.start()
            reader.join(timeout=10)

            self.assertEqual(reader.exitcode, 0)
            self.assertEqual(results.get(timeout=2), ("OK", True))
            state = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(
                state,
                {
                    "schema_version": 1,
                    "state": "DISABLED",
                    "audit_id": "rollback-audit-001",
                    "reason": "ROLLBACK_VERIFICATION_FAILED",
                    "changed_at_utc": FIXED_TIMESTAMP,
                    "confirmed_by": None,
                },
            )

    def test_re_enable_requires_exact_human_confirmation_and_preserves_state_on_error(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "automatic-actions.json"
            controller = self.controller(path)
            controller.disable_automatic_actions(
                audit_id="rollback-audit-001",
                reason="ROLLBACK_VERIFICATION_FAILED",
            )
            before = path.read_bytes()

            invalid_cases = (
                {"confirmation": "yes", "confirmed_by": "operator", "reason": "ok"},
                {
                    "confirmation": REENABLE_CONFIRMATION_TOKEN,
                    "confirmed_by": "",
                    "reason": "ok",
                },
                {
                    "confirmation": REENABLE_CONFIRMATION_TOKEN,
                    "confirmed_by": "operator",
                    "reason": "",
                },
            )
            for keywords in invalid_cases:
                with self.subTest(keywords=keywords):
                    with self.assertRaises(ValueError):
                        controller.re_enable(**keywords)
                    self.assertEqual(path.read_bytes(), before)
                    self.assertTrue(controller.is_disabled())

            controller.re_enable(
                confirmation=REENABLE_CONFIRMATION_TOKEN,
                confirmed_by="operator@example.test",
                reason="manual verification completed",
            )

            self.assertFalse(self.controller(path).is_disabled())
            state = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(state["state"], "ENABLED")
            self.assertEqual(state["audit_id"], "rollback-audit-001")
            self.assertEqual(state["confirmed_by"], "operator@example.test")

    def test_actuation_module_never_references_re_enable(self) -> None:
        tree = ast.parse(ACTUATION_MODULE.read_text(encoding="utf-8"))
        references = [
            node
            for node in ast.walk(tree)
            if (
                isinstance(node, ast.Name)
                and node.id == "re_enable"
                or isinstance(node, ast.Attribute)
                and node.attr == "re_enable"
            )
        ]
        self.assertEqual(references, [])

    def test_persistence_module_has_no_pymilvus_or_execute_live_import(self) -> None:
        source = PERSISTENCE_MODULE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        self.assertFalse(any(name.startswith("pymilvus") for name in imports))
        self.assertNotIn("execute_live", source)


if __name__ == "__main__":
    unittest.main()
