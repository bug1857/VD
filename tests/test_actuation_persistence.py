from __future__ import annotations

import ast
import json
import multiprocessing
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from vdbench.actuation import (
    ActuationAuditRecord,
    ActuationIdentityContext,
    ActuationOutcome,
    RollbackActuationContext,
    RollbackVerification,
    ShadowActuationContext,
    ShadowResult,
)
from vdbench.actuation_persistence import (
    ACTUATION_CONTEXT_SCHEMA_VERSION,
    AUDIT_SCHEMA_VERSION,
    HISTORICAL_AUDIT_SCHEMA_VERSION,
    REENABLE_CONFIRMATION_TOKEN,
    AuditLogCorruptedError,
    DuplicateAuditIdError,
    FileAutomaticActionController,
    JsonlAuditSink,
)
from vdbench.config import Metric
from vdbench.drift import build_evidence_provenance
from vdbench.policy import PolicyAction, SafetyGateResult

REPOSITORY = Path(__file__).parents[1]
ACTUATION_MODULE = REPOSITORY / "src" / "vdbench" / "actuation.py"
PERSISTENCE_MODULE = REPOSITORY / "src" / "vdbench" / "actuation_persistence.py"
FIXED_TIMESTAMP = "2026-08-03T18:30:00Z"

# Frozen literal bytes emitted by the pre-D schema-v2 writer at commit
# ca28fd8237b9be79a98831da9c4b58c857338c08.  This fixture is deliberately not
# generated through any current serializer, projector, validator, or runtime
# ActuationContext/QualificationResult value.
HISTORICAL_V2_LINE = (
    b'{"record":{"action":"NO_CHANGE","attempted":false,'
    b'"audit_id":"historical-v2","automatic_actions_disabled":false,'
    b'"canary_observation":null,"candidate_ef":null,"context":{'
    b'"audited_query_ids":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,'
    b'17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,'
    b'37,38,39,40,41,42,43,44,45,46,47,48,49],'
    b'"collection_name":"vd-l2-hnsw","configuration_identity":"config-v1",'
    b'"data_identity":"data-v1","flat_index_identity":"flat-v1",'
    b'"index_identity":"index-v1","last_known_good":{'
    b'"configuration_identity":"config-v1","data_identity":"data-v1",'
    b'"ef":400,"index_identity":"index-v1","metric":"L2",'
    b'"qualified":true,"qualifying_window_ids":["window-10","window-11"],'
    b'"reasons":[],"threshold_stratum":"target-025"},"metric":"L2",'
    b'"occurred_at_utc":"2026-08-03T18:30:00Z",'
    b'"threshold_stratum":"target-025"},"current_ef":400,'
    b'"evidence_provenance":null,"last_known_good_ef":400,'
    b'"outcome":"NO_OP","policy_reason":"stationary workload",'
    b'"reason":"NON_ACTIONABLE_POLICY_DECISION",'
    b'"rollback_verification":null,"safety_gate_results":['
    b'{"detail":"fixture gate","name":"PRE_ACTION_READY","passed":true}],'
    b'"shadow_result":null,"success":true,"traffic_fraction":null},'
    b'"schema_version":2}\n'
)


def identity_context() -> ActuationIdentityContext:
    return ActuationIdentityContext(
        metric=Metric.L2,
        threshold_stratum="target-025",
        collection_name="vd-l2-hnsw",
        configuration_identity="config-v1",
        index_identity="index-v1",
        flat_index_identity="flat-v1",
        data_identity="data-v1",
        occurred_at_utc=FIXED_TIMESTAMP,
    )


def rollback_context() -> RollbackActuationContext:
    return RollbackActuationContext(
        metric=Metric.L2,
        threshold_stratum="target-025",
        collection_name="vd-l2-hnsw",
        configuration_identity="config-v1",
        index_identity="index-v1",
        flat_index_identity="flat-v1",
        data_identity="data-v1",
        occurred_at_utc=FIXED_TIMESTAMP,
        expected_last_known_good_ef=400,
        audited_query_ids=tuple(range(50)),
    )


def provenance():
    return build_evidence_provenance(
        metric=Metric.L2,
        threshold_stratum="target-025",
        reference_window_id="reference-window",
        current_window_id="current-window",
        reference_manifest_sha256="a" * 64,
        current_manifest_sha256="b" * 64,
        configuration_identity="config-v1",
        data_identity="data-v1",
        flat_binding_id="flat-v1",
        hnsw_binding_id="index-v1",
        reference_audit_ids=tuple(range(50)),
        reference_audit_rank_digests=tuple(
            f"{value:064x}" for value in range(50)
        ),
        current_audit_ids=tuple(range(50)),
        current_audit_rank_digests=tuple(
            f"{value + 50:064x}" for value in range(50)
        ),
    )


def policy_record(
    audit_id: str,
    *,
    action: PolicyAction = PolicyAction.NO_CHANGE,
) -> ActuationAuditRecord:
    retired = action is PolicyAction.START_CANARY
    return ActuationAuditRecord(
        audit_id=audit_id,
        action=action,
        outcome=(ActuationOutcome.BLOCKED if retired else ActuationOutcome.NO_OP),
        attempted=False,
        success=not retired,
        reason=(
            "GENERIC_START_CANARY_RETIRED"
            if retired
            else "NON_ACTIONABLE_POLICY_DECISION"
        ),
        context=identity_context(),
        current_ef=400,
        candidate_ef=(
            800
            if action in {PolicyAction.RECOMMEND_EF, PolicyAction.START_CANARY}
            else None
        ),
        last_known_good_ef=400,
        traffic_fraction=None,
        policy_reason="fixture policy decision",
        safety_gate_results=(
            SafetyGateResult(
                name="PRE_ACTION_READY",
                passed=True,
                detail="fixture gate",
            ),
        ),
        automatic_actions_disabled=False,
        evidence_provenance=provenance(),
    )


def successful_verification() -> RollbackVerification:
    return RollbackVerification(
        success=True,
        restored_ef=400,
        health_passed=True,
        audit_passed=True,
        configuration_identity="config-v1",
        index_identity="index-v1",
        data_identity="data-v1",
        detail="restoration verified",
    )


def rollback_record(
    audit_id: str,
    *,
    outcome: ActuationOutcome = ActuationOutcome.SUCCEEDED,
    verification: RollbackVerification | None = None,
) -> ActuationAuditRecord:
    if outcome is ActuationOutcome.SUCCEEDED:
        attempted, success, disabled = True, True, False
        reason = "ROLLBACK_VERIFIED"
        verification = verification or successful_verification()
    elif outcome is ActuationOutcome.FAILED:
        attempted, success, disabled = True, False, True
        reason = "ROLLBACK_VERIFICATION_FAILED"
    elif outcome is ActuationOutcome.BLOCKED:
        attempted, success, disabled = False, False, False
        reason = "ROLLBACK_BLOCKED_FOR_FIXTURE"
        verification = None
    else:
        raise ValueError("unsupported rollback fixture outcome")
    return ActuationAuditRecord(
        audit_id=audit_id,
        action=PolicyAction.ROLLBACK,
        outcome=outcome,
        attempted=attempted,
        success=success,
        reason=reason,
        context=rollback_context(),
        current_ef=800,
        candidate_ef=800,
        last_known_good_ef=400,
        traffic_fraction=None,
        policy_reason="fixture rollback decision",
        safety_gate_results=(
            SafetyGateResult(
                name="CANARY_HEALTH",
                passed=False,
                detail="fixture rollback trigger",
            ),
        ),
        rollback_verification=verification,
        automatic_actions_disabled=disabled,
        evidence_provenance=provenance(),
    )


def _write_payload(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


def _append_worker(
    path: str,
    audit_id: str,
    start: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
) -> None:
    start.wait(timeout=10)
    try:
        JsonlAuditSink(path).append(policy_record(audit_id))
    except Exception as exc:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
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
    except Exception as exc:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
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
    except Exception as exc:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
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
    def test_new_policy_appends_use_exact_schema_v3_projection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            sink = JsonlAuditSink(path)

            sink.append(policy_record("audit-policy"))

            envelope = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(envelope["schema_version"], AUDIT_SCHEMA_VERSION)
            self.assertEqual(AUDIT_SCHEMA_VERSION, 3)
            record = envelope["record"]
            self.assertEqual(record["action"], "NO_CHANGE")
            self.assertIsNone(record["shadow_result"])
            self.assertIsNone(record["canary_observation"])
            self.assertIsNone(record["rollback_verification"])
            self.assertEqual(
                set(record["context"]),
                {
                    "context_schema_version",
                    "context_kind",
                    "metric",
                    "threshold_stratum",
                    "collection_name",
                    "configuration_identity",
                    "index_identity",
                    "flat_index_identity",
                    "data_identity",
                    "occurred_at_utc",
                },
            )
            self.assertEqual(
                record["context"]["context_schema_version"],
                ACTUATION_CONTEXT_SCHEMA_VERSION,
            )
            self.assertEqual(record["context"]["context_kind"], "POLICY")
            self.assertNotIn("last_known_good", record["context"])
            self.assertNotIn("audited_query_ids", record["context"])
            self.assertTrue(sink.contains("audit-policy"))

    def test_new_rollback_appends_use_exact_schema_v3_projection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            sink = JsonlAuditSink(path)

            sink.append(rollback_record("audit-rollback"))

            envelope = json.loads(path.read_text(encoding="utf-8"))
            record = envelope["record"]
            context = record["context"]
            self.assertEqual(context["context_kind"], "ROLLBACK")
            self.assertEqual(context["expected_last_known_good_ef"], 400)
            self.assertEqual(context["audited_query_ids"], list(range(50)))
            self.assertEqual(len(context), 12)
            self.assertTrue(record["rollback_verification"]["success"])
            self.assertTrue(sink.contains("audit-rollback"))

    def test_every_pinned_action_outcome_shape_is_writable(self) -> None:
        records = (
            policy_record("no-change"),
            policy_record("recommend", action=PolicyAction.RECOMMEND_EF),
            policy_record("retired-start", action=PolicyAction.START_CANARY),
            rollback_record("rollback-blocked", outcome=ActuationOutcome.BLOCKED),
            rollback_record("rollback-failed", outcome=ActuationOutcome.FAILED),
            rollback_record("rollback-succeeded"),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            sink = JsonlAuditSink(path)
            for record in records:
                sink.append(record)
            self.assertEqual(len(path.read_text(encoding="utf-8").splitlines()), 6)
            self.assertTrue(all(sink.contains(record.audit_id) for record in records))

    def test_policy_audit_preserves_out_of_ladder_observational_efs(self) -> None:
        records = (
            replace(policy_record("sentinel-current"), current_ef=100),
            replace(policy_record("unsafe-current"), current_ef=123),
            replace(
                policy_record("unsafe-retired-start", action=PolicyAction.START_CANARY),
                current_ef=101,
                candidate_ef=123,
                last_known_good_ef=999,
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            sink = JsonlAuditSink(path)
            for record in records:
                sink.append(record)

            payloads = tuple(
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
            )
            self.assertEqual(
                tuple(payload["record"]["current_ef"] for payload in payloads),
                (100, 123, 101),
            )
            self.assertEqual(payloads[2]["record"]["candidate_ef"], 123)
            self.assertEqual(payloads[2]["record"]["last_known_good_ef"], 999)
            self.assertTrue(all(sink.contains(record.audit_id) for record in records))

    def test_rollback_preserves_observed_candidate_but_not_unsafe_authority(self) -> None:
        record = replace(rollback_record("rollback-observation"), candidate_ef=123)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            sink = JsonlAuditSink(path)
            sink.append(record)

            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["record"]["candidate_ef"], 123)
            self.assertEqual(
                payload["record"]["context"]["expected_last_known_good_ef"],
                400,
            )
            self.assertEqual(
                payload["record"]["rollback_verification"]["restored_ef"],
                400,
            )
            self.assertTrue(sink.contains("rollback-observation"))

    def test_failed_rollback_preserves_unsafe_restoration_evidence(self) -> None:
        records = (
            rollback_record(
                "failed-sentinel-restoration",
                outcome=ActuationOutcome.FAILED,
                verification=RollbackVerification(
                    success=False,
                    restored_ef=100,
                    health_passed=False,
                    audit_passed=False,
                    configuration_identity="wrong-config",
                    index_identity="wrong-index",
                    data_identity="wrong-data",
                    detail="sentinel restoration observed",
                ),
            ),
            rollback_record(
                "failed-unsafe-restoration",
                outcome=ActuationOutcome.FAILED,
                verification=RollbackVerification(
                    success=False,
                    restored_ef=123,
                    health_passed=True,
                    audit_passed=False,
                    configuration_identity="other-config",
                    index_identity="other-index",
                    data_identity="other-data",
                    detail="out-of-ladder restoration observed",
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            sink = JsonlAuditSink(path)
            for record in records:
                sink.append(record)

            payloads = tuple(
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
            )
            self.assertEqual(
                tuple(
                    payload["record"]["rollback_verification"]["restored_ef"]
                    for payload in payloads
                ),
                (100, 123),
            )
            self.assertEqual(
                payloads[0]["record"]["rollback_verification"][
                    "configuration_identity"
                ],
                "wrong-config",
            )
            self.assertTrue(
                all(payload["record"]["automatic_actions_disabled"] for payload in payloads)
            )
            self.assertTrue(all(sink.contains(record.audit_id) for record in records))

    def test_failed_rollback_accepts_success_flag_with_identity_mismatch(self) -> None:
        record = rollback_record(
            "failed-identity-mismatch",
            outcome=ActuationOutcome.FAILED,
            verification=replace(
                successful_verification(),
                index_identity="unexpected-index",
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            sink = JsonlAuditSink(path)
            sink.append(record)

            payload = json.loads(path.read_text(encoding="utf-8"))
            verification = payload["record"]["rollback_verification"]
            self.assertTrue(verification["success"])
            self.assertEqual(verification["index_identity"], "unexpected-index")
            self.assertTrue(payload["record"]["automatic_actions_disabled"])
            self.assertTrue(sink.contains(record.audit_id))

    def test_failed_rollback_cannot_contain_complete_success_proof(self) -> None:
        invalid_records = (
            rollback_record(
                "failed-with-success-proof",
                outcome=ActuationOutcome.FAILED,
                verification=successful_verification(),
            ),
            replace(
                rollback_record("blocked-success-reason", outcome=ActuationOutcome.BLOCKED),
                reason="ROLLBACK_VERIFIED",
            ),
            replace(
                rollback_record("failed-success-reason", outcome=ActuationOutcome.FAILED),
                reason="ROLLBACK_VERIFIED",
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            for record in invalid_records:
                path = Path(directory) / f"{record.audit_id}.jsonl"
                with self.subTest(audit_id=record.audit_id):
                    with self.assertRaises(ValueError):
                        JsonlAuditSink(path).append(record)
                    self.assertFalse(path.exists())

    def test_reader_rejects_failed_outcome_with_complete_success_proof(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            JsonlAuditSink(path).append(rollback_record("tampered-outcome"))
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["record"].update(
                {
                    "outcome": "FAILED",
                    "success": False,
                    "reason": "ROLLBACK_VERIFICATION_FAILED",
                    "automatic_actions_disabled": True,
                }
            )
            _write_payload(path, payload)

            with self.assertRaises(AuditLogCorruptedError):
                JsonlAuditSink(path).contains("tampered-outcome")

    def test_non_integer_restored_ef_fails_without_appending(self) -> None:
        invalid_records = (
            rollback_record(
                "bool-restored-ef",
                outcome=ActuationOutcome.FAILED,
                verification=RollbackVerification(
                    success=False,
                    restored_ef=True,
                    health_passed=False,
                    audit_passed=False,
                    configuration_identity="config-v1",
                    index_identity="index-v1",
                    data_identity="data-v1",
                    detail="invalid boolean observation",
                ),
            ),
            rollback_record(
                "string-restored-ef",
                outcome=ActuationOutcome.FAILED,
                verification=RollbackVerification(
                    success=False,
                    restored_ef="123",
                    health_passed=False,
                    audit_passed=False,
                    configuration_identity="config-v1",
                    index_identity="index-v1",
                    data_identity="data-v1",
                    detail="invalid string observation",
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            for record in invalid_records:
                path = Path(directory) / f"{record.audit_id}.jsonl"
                with self.subTest(audit_id=record.audit_id):
                    with self.assertRaises(ValueError):
                        JsonlAuditSink(path).append(record)
                    self.assertFalse(path.exists())

    def test_non_integer_observational_efs_fail_without_appending(self) -> None:
        invalid_records = (
            replace(policy_record("bool-current"), current_ef=True),
            replace(policy_record("string-current"), current_ef="100"),
            replace(policy_record("bool-candidate"), candidate_ef=False),
            replace(policy_record("string-candidate"), candidate_ef="800"),
            replace(policy_record("bool-lkg"), last_known_good_ef=True),
            replace(policy_record("string-lkg"), last_known_good_ef="400"),
        )
        with tempfile.TemporaryDirectory() as directory:
            for record in invalid_records:
                path = Path(directory) / f"{record.audit_id}.jsonl"
                with self.subTest(audit_id=record.audit_id):
                    with self.assertRaises(ValueError):
                        JsonlAuditSink(path).append(record)
                    self.assertFalse(path.exists())

    def test_shadow_context_and_shadow_or_canary_results_are_rejected(self) -> None:
        shadow_context = ShadowActuationContext(
            metric=Metric.L2,
            threshold_stratum="target-025",
            collection_name="vd-l2-hnsw",
            configuration_identity="config-v1",
            index_identity="index-v1",
            flat_index_identity="flat-v1",
            data_identity="data-v1",
            occurred_at_utc=FIXED_TIMESTAMP,
            audited_query_ids=tuple(range(50)),
        )
        invalid_records = (
            replace(policy_record("shadow-context"), context=shadow_context),
            replace(
                policy_record("shadow-result"),
                shadow_result=ShadowResult(True, 50, 0, 0, 0, True, True),
            ),
            replace(policy_record("canary-result"), canary_observation=object()),
        )
        with tempfile.TemporaryDirectory() as directory:
            for record in invalid_records:
                path = Path(directory) / f"{record.audit_id}.jsonl"
                with self.subTest(audit_id=record.audit_id):
                    with self.assertRaises((TypeError, ValueError)):
                        JsonlAuditSink(path).append(record)
                    self.assertFalse(path.exists())

    def test_invalid_rollback_context_fails_without_appending(self) -> None:
        invalid_contexts = (
            replace(rollback_context(), expected_last_known_good_ef=100),
            replace(rollback_context(), expected_last_known_good_ef=800),
            replace(rollback_context(), audited_query_ids=tuple(range(49))),
            replace(
                rollback_context(),
                audited_query_ids=tuple(range(49)) + (0,),
            ),
            replace(
                rollback_context(),
                audited_query_ids=tuple(range(49)) + ("e\u0301",),
            ),
            object.__new__(RollbackActuationContext),
        )
        with tempfile.TemporaryDirectory() as directory:
            for index, context in enumerate(invalid_contexts):
                path = Path(directory) / f"invalid-{index}.jsonl"
                record = replace(rollback_record(f"invalid-{index}"), context=context)
                with self.subTest(index=index):
                    with self.assertRaises(ValueError):
                        JsonlAuditSink(path).append(record)
                    self.assertFalse(path.exists())

    def test_invalid_action_outcome_combinations_are_rejected(self) -> None:
        invalid_records = (
            replace(policy_record("bad-success"), success=False),
            replace(
                policy_record("bad-reason", action=PolicyAction.START_CANARY),
                reason="SOMETHING_ELSE",
            ),
            replace(rollback_record("bad-blocked"), attempted=False),
            replace(
                rollback_record("bad-disabled"),
                automatic_actions_disabled=True,
            ),
            replace(
                rollback_record("bad-verification"),
                rollback_verification=replace(
                    successful_verification(), health_passed=False
                ),
            ),
            replace(
                rollback_record("bad-restored-ef"),
                rollback_verification=replace(
                    successful_verification(), restored_ef=100
                ),
            ),
            replace(
                rollback_record("bad-out-of-ladder-restored-ef"),
                rollback_verification=replace(
                    successful_verification(), restored_ef=123
                ),
            ),
            replace(
                rollback_record("bad-mismatched-restored-ef"),
                rollback_verification=replace(
                    successful_verification(), restored_ef=200
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            for record in invalid_records:
                path = Path(directory) / f"{record.audit_id}.jsonl"
                with self.subTest(audit_id=record.audit_id):
                    with self.assertRaises(ValueError):
                        JsonlAuditSink(path).append(record)
                    self.assertFalse(path.exists())

    def test_audit_id_contract_preserves_exact_string_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            sink = JsonlAuditSink(path)
            sink.append(policy_record(" audit-id "))
            sink.append(policy_record("audit-id"))
            sink.append(policy_record("e\u0301"))
            sink.append(policy_record("é"))
            self.assertTrue(sink.contains(" audit-id "))
            self.assertTrue(sink.contains("audit-id"))
            self.assertTrue(sink.contains("e\u0301"))
            self.assertTrue(sink.contains("é"))
            with self.assertRaises(ValueError):
                sink.append(policy_record("   "))
            with self.assertRaises(ValueError):
                sink.contains("\t")

    def test_literal_historical_v2_is_read_without_runtime_authority(self) -> None:
        self.assertEqual(HISTORICAL_AUDIT_SCHEMA_VERSION, 2)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            path.write_bytes(HISTORICAL_V2_LINE)
            before = path.read_bytes()

            with patch(
                "vdbench.actuation_persistence._project_v3_record",
                side_effect=AssertionError("v2 reader called v3 projector"),
            ):
                self.assertTrue(JsonlAuditSink(path).contains("historical-v2"))

            self.assertEqual(path.read_bytes(), before)

    def test_historical_v2_prefix_is_unchanged_when_v3_is_appended(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            path.write_bytes(HISTORICAL_V2_LINE)

            JsonlAuditSink(path).append(policy_record("current-v3"))

            combined = path.read_bytes()
            self.assertTrue(combined.startswith(HISTORICAL_V2_LINE))
            self.assertEqual(combined[: len(HISTORICAL_V2_LINE)], HISTORICAL_V2_LINE)
            self.assertTrue(JsonlAuditSink(path).contains("historical-v2"))
            self.assertTrue(JsonlAuditSink(path).contains("current-v3"))

    def test_duplicate_audit_ids_are_rejected_across_v2_and_v3(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            path.write_bytes(HISTORICAL_V2_LINE)
            before = path.read_bytes()

            with self.assertRaises(DuplicateAuditIdError):
                JsonlAuditSink(path).append(policy_record("historical-v2"))

            self.assertEqual(path.read_bytes(), before)

    def test_malformed_json_duplicate_keys_and_versions_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as source_directory:
            source_path = Path(source_directory) / "source.jsonl"
            JsonlAuditSink(source_path).append(policy_record("duplicate-source"))
            valid_v3 = source_path.read_bytes()
        malformed_lines = (
            b'{"schema_version":3,"record":\n',
            HISTORICAL_V2_LINE.rstrip(b"\n").replace(
                b'"schema_version":2',
                b'"schema_version":2,"schema_version":2',
            )
            + b"\n",
            valid_v3.replace(
                b'"action":"NO_CHANGE"',
                b'"action":"NO_CHANGE","action":"NO_CHANGE"',
            ),
            valid_v3.replace(
                b'"context_kind":"POLICY"',
                b'"context_kind":"POLICY","context_kind":"POLICY"',
            ),
            HISTORICAL_V2_LINE.replace(b'"schema_version":2', b'"schema_version":1'),
            HISTORICAL_V2_LINE.replace(b'"schema_version":2', b'"schema_version":4'),
            HISTORICAL_V2_LINE.replace(
                b'"schema_version":2', b'"schema_version":true'
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            for index, line in enumerate(malformed_lines):
                path = Path(directory) / f"malformed-{index}.jsonl"
                path.write_bytes(line)
                sink = JsonlAuditSink(path)
                with self.subTest(index=index):
                    with self.assertRaises(AuditLogCorruptedError):
                        sink.contains("historical-v2")
                    before = path.read_bytes()
                    with self.assertRaises(AuditLogCorruptedError):
                        sink.append(policy_record(f"new-{index}"))
                    self.assertEqual(path.read_bytes(), before)

    def test_v3_strict_scalar_context_gate_and_global_invariants_fail_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.jsonl"
            JsonlAuditSink(source).append(policy_record("source"))
            original = json.loads(source.read_text(encoding="utf-8"))
            mutations = (
                ("attempted", 1),
                ("action", "UNKNOWN"),
                ("audit_id", "   "),
                ("shadow_result", {}),
                ("canary_observation", {}),
            )
            variants = []
            for field, value in mutations:
                payload = json.loads(json.dumps(original))
                payload["record"][field] = value
                variants.append(payload)
            for field, value in (
                ("context_schema_version", "actuation-context-v2"),
                ("metric", "IP"),
                ("threshold_stratum", "unknown"),
                ("occurred_at_utc", "not-a-time"),
            ):
                payload = json.loads(json.dumps(original))
                payload["record"]["context"][field] = value
                variants.append(payload)
            malformed_gate = json.loads(json.dumps(original))
            malformed_gate["record"]["safety_gate_results"][0]["passed"] = 1
            variants.append(malformed_gate)

            for index, payload in enumerate(variants):
                path = Path(directory) / f"strict-{index}.jsonl"
                _write_payload(path, payload)
                with (
                    self.subTest(index=index),
                    self.assertRaises(AuditLogCorruptedError),
                ):
                    JsonlAuditSink(path).contains("source")

    def test_v3_extra_missing_and_mixed_version_contexts_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.jsonl"
            JsonlAuditSink(source).append(policy_record("source"))
            original = json.loads(source.read_text(encoding="utf-8"))
            variants = []
            envelope_extra = json.loads(json.dumps(original))
            envelope_extra["extra"] = True
            variants.append(envelope_extra)
            envelope_missing = json.loads(json.dumps(original))
            del envelope_missing["record"]
            variants.append(envelope_missing)
            record_extra = json.loads(json.dumps(original))
            record_extra["record"]["extra"] = True
            variants.append(record_extra)
            record_missing = json.loads(json.dumps(original))
            del record_missing["record"]["reason"]
            variants.append(record_missing)
            extra = json.loads(json.dumps(original))
            extra["record"]["context"]["extra"] = True
            variants.append(extra)
            missing = json.loads(json.dumps(original))
            del missing["record"]["context"]["data_identity"]
            variants.append(missing)
            wrong_kind = json.loads(json.dumps(original))
            wrong_kind["record"]["context"]["context_kind"] = "ROLLBACK"
            variants.append(wrong_kind)
            downgraded = json.loads(HISTORICAL_V2_LINE)
            downgraded["schema_version"] = 3
            variants.append(downgraded)
            substituted = json.loads(json.dumps(original))
            substituted["schema_version"] = 2
            variants.append(substituted)

            for index, payload in enumerate(variants):
                path = Path(directory) / f"variant-{index}.jsonl"
                _write_payload(path, payload)
                with (
                    self.subTest(index=index),
                    self.assertRaises(AuditLogCorruptedError),
                ):
                    JsonlAuditSink(path).contains("source")

    def test_v3_reader_rejects_tampered_provenance_and_verification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            policy_path = Path(directory) / "policy.jsonl"
            JsonlAuditSink(policy_path).append(policy_record("policy"))
            policy_payload = json.loads(policy_path.read_text(encoding="utf-8"))
            policy_payload["record"]["evidence_provenance"]["sha256"] = "0" * 64
            _write_payload(policy_path, policy_payload)
            with self.assertRaises(AuditLogCorruptedError):
                JsonlAuditSink(policy_path).contains("policy")

            rollback_path = Path(directory) / "rollback.jsonl"
            JsonlAuditSink(rollback_path).append(rollback_record("rollback"))
            rollback_payload = json.loads(rollback_path.read_text(encoding="utf-8"))
            rollback_payload["record"]["rollback_verification"][
                "configuration_identity"
            ] = "other-config"
            _write_payload(rollback_path, rollback_payload)
            with self.assertRaises(AuditLogCorruptedError):
                JsonlAuditSink(rollback_path).contains("rollback")

            inconsistent_path = Path(directory) / "inconsistent.jsonl"
            JsonlAuditSink(inconsistent_path).append(
                rollback_record("inconsistent")
            )
            inconsistent = json.loads(
                inconsistent_path.read_text(encoding="utf-8")
            )
            inconsistent["record"]["rollback_verification"]["restored_ef"] = None
            _write_payload(inconsistent_path, inconsistent)
            with self.assertRaises(AuditLogCorruptedError):
                JsonlAuditSink(inconsistent_path).contains("inconsistent")

    def test_v3_reader_rejects_malformed_rollback_ef_and_query_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.jsonl"
            JsonlAuditSink(source).append(rollback_record("rollback"))
            original = json.loads(source.read_text(encoding="utf-8"))
            variants = []
            for field, value in (
                ("expected_last_known_good_ef", 100),
                ("expected_last_known_good_ef", 800),
                ("audited_query_ids", list(range(49))),
                ("audited_query_ids", list(range(49)) + [0]),
                ("audited_query_ids", list(range(49)) + [True]),
                ("audited_query_ids", list(range(49)) + ["e\u0301"]),
            ):
                payload = json.loads(json.dumps(original))
                payload["record"]["context"][field] = value
                variants.append(payload)
            for index, payload in enumerate(variants):
                path = Path(directory) / f"rollback-{index}.jsonl"
                _write_payload(path, payload)
                with (
                    self.subTest(index=index),
                    self.assertRaises(AuditLogCorruptedError),
                ):
                    JsonlAuditSink(path).contains("rollback")

    def test_separate_process_reader_observes_persisted_v3_audit(self) -> None:
        process_context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            JsonlAuditSink(path).append(policy_record("audit-restart"))
            results = process_context.Queue()
            reader = process_context.Process(
                target=_contains_worker,
                args=(str(path), "audit-restart", results),
            )
            reader.start()
            reader.join(timeout=10)
            self.assertEqual(reader.exitcode, 0)
            self.assertEqual(results.get(timeout=2), ("OK", True))

    def test_concurrent_v3_appends_serialize_and_duplicate_still_rejects(
        self,
    ) -> None:
        process_context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "actuation-audit.jsonl"
            path.write_bytes(HISTORICAL_V2_LINE)
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
            self.assertEqual(
                {results.get(timeout=2) for _ in workers},
                {
                    ("audit-concurrent-a", "OK", ""),
                    ("audit-concurrent-b", "OK", ""),
                },
            )
            sink = JsonlAuditSink(path)
            self.assertTrue(sink.contains("historical-v2"))
            self.assertTrue(sink.contains("audit-concurrent-a"))
            self.assertTrue(sink.contains("audit-concurrent-b"))
            with self.assertRaises(DuplicateAuditIdError):
                sink.append(policy_record("audit-concurrent-a"))

    def test_persistence_module_has_no_legacy_or_authority_import(self) -> None:
        source = PERSISTENCE_MODULE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_names = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        self.assertNotIn("ActuationContext", imported_names)
        self.assertNotIn("QualificationResult", imported_names)
        self.assertNotIn("LkgPhase3Authority", source)
        self.assertNotIn("bind_lkg_phase3_authority", source)
        self.assertNotIn("Stage4AdmissionReceipt", source)


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
