"""Offline composition tests for the EXP-009 activation coordinator."""

from __future__ import annotations

import ast
import base64
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import tempfile
import unittest

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from vdbench.canary_activation import ActivationTimestamps, CanaryActivationCoordinator
from vdbench.canary_approval import (
    ApprovalVerificationContext,
    ApprovalVerificationResult,
    CanaryApprovalGrant,
    StaticCanaryApprovalTrustStore,
    approval_grant_signing_bytes,
    policy_decision_sha256,
)
from vdbench.canary_grant_store import CanaryGrantUseStore, GrantUseResult, GrantUseStatus
from vdbench.canary_lifecycle_audit import CanaryLifecycleAuditRecord, JsonlCanaryLifecycleAuditSink
from vdbench.canary_routing import CanaryRoutePlan, build_canary_route_plan
from vdbench.canary_route_authority import CanaryRouteAuthority, RouteAuthoritySnapshot, RouteAuthorityState
from vdbench.canary_route_state import FileCanaryRouteStateStore, RouteState, RouteStateBinding, RouteStateRecord
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    CandidateSelectionRecord,
    WorkloadIdentityBinding,
    build_eligible_workload_manifest,
)
from vdbench.actuation_persistence import FileAutomaticActionController
from vdbench.artifacts import canonical_json_bytes, sha256_file, write_dataset_artifacts
from vdbench.config import EXP001_DATASET_SPEC, Metric
from vdbench.dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from vdbench.dataset002 import Dataset002Spec, generate_dataset002, write_dataset002_artifacts
from vdbench.drift import build_evidence_provenance
from vdbench.policy import PolicyAction, PolicyDecision, PolicyMode, SafetyGateResult


def _sha(character: str) -> str:
    return character * 64


def _binding(plan: CanaryRoutePlan) -> RouteStateBinding:
    return RouteStateBinding(
        metric=plan.metric,
        threshold_stratum=plan.threshold_stratum,
        last_known_good_ef=plan.last_known_good_ef,
        configuration_identity=plan.configuration_identity,
        data_identity=plan.data_identity,
        flat_binding_id=plan.flat_binding_id,
        hnsw_binding_id=plan.hnsw_binding_id,
    )


def _grant(plan: CanaryRoutePlan) -> CanaryApprovalGrant:
    return CanaryApprovalGrant(
        grant_id="grant-001",
        key_id="operator-001",
        issued_at_utc="2026-08-04T10:00:00Z",
        expires_at_utc="2026-08-04T10:30:00Z",
        experiment_id="EXP-009",
        policy_decision_sha256=_sha("d"),
        policy_audit_id="policy-audit-001",
        metric=plan.metric,
        threshold_stratum=plan.threshold_stratum,
        current_ef=plan.last_known_good_ef,
        candidate_ef=plan.candidate_ef,
        last_known_good_ef=plan.last_known_good_ef,
        configuration_identity=plan.configuration_identity,
        data_identity=plan.data_identity,
        flat_binding_id=plan.flat_binding_id,
        hnsw_binding_id=plan.hnsw_binding_id,
        eligible_workload_sha256=plan.eligible_workload_sha256,
        candidate_selection_sha256=plan.candidate_selection_sha256,
        routing_population_count=600,
        candidate_count=60,
        maximum_fraction=0.10,
        rollback_pre_authorized=True,
        signature="test-signature",
    )


_TIMESTAMPS = ActivationTimestamps(
    reserved_at_utc="2026-08-04T10:01:00Z",
    authorized_at_utc="2026-08-04T10:01:01Z",
    marker_at_utc="2026-08-04T10:01:02Z",
    failure_at_utc="2026-08-04T10:01:03Z",
)


def _real_decision(plan: CanaryRoutePlan) -> PolicyDecision:
    provenance = build_evidence_provenance(
        metric=plan.metric,
        threshold_stratum=plan.threshold_stratum,
        reference_window_id="reference-window-001",
        current_window_id="current-window-001",
        reference_manifest_sha256=_sha("1"),
        current_manifest_sha256=_sha("2"),
        configuration_identity=plan.configuration_identity,
        data_identity=plan.data_identity,
        flat_binding_id=plan.flat_binding_id,
        hnsw_binding_id=plan.hnsw_binding_id,
        reference_audit_ids=tuple(f"reference-audit-{index:02d}" for index in range(50)),
        reference_audit_rank_digests=tuple(_sha("3") for _ in range(50)),
        current_audit_ids=tuple(f"current-audit-{index:02d}" for index in range(50)),
        current_audit_rank_digests=tuple(_sha("4") for _ in range(50)),
    )
    return PolicyDecision(
        action=PolicyAction.START_CANARY,
        current_ef=plan.last_known_good_ef,
        candidate_ef=plan.candidate_ef,
        last_known_good_ef=plan.last_known_good_ef,
        expected_mean_recall=0.99,
        expected_recall_lower_bound_95=0.98,
        expected_p95_latency_ms=4.0,
        expected_latency_upper_bound_95_ms=5.0,
        predicted_recall_improvement=0.02,
        predicted_latency_reduction_fraction=None,
        reason="QUALITY_DRIFT_RECOVERY",
        detector_confidence=0.999,
        detector_magnitude=2.0,
        safety_gate_results=(SafetyGateResult("PRE_ACTION", True, "all checks passed"),),
        mode=PolicyMode.CANARY_ENABLED,
        audit_id="policy-audit-real-001",
        evidence_provenance=provenance,
    )


def _signed_real_grant(
    plan: CanaryRoutePlan, decision: PolicyDecision, private_key: Ed25519PrivateKey
) -> CanaryApprovalGrant:
    unsigned = replace(
        _grant(plan),
        policy_decision_sha256=policy_decision_sha256(decision),
        policy_audit_id=decision.audit_id,
        signature=None,
    )
    return replace(
        unsigned,
        signature=base64.urlsafe_b64encode(
            private_key.sign(approval_grant_signing_bytes(unsigned))
        ).decode("ascii").rstrip("="),
    )


class FakeVerifier:
    def __init__(self, *, approved: bool = True, reason_code: str | None = None) -> None:
        self.approved = approved
        self.reason_code = reason_code
        self.calls = 0
        self.order: list[str] = []

    def __call__(self, grant, *, trust_store, context):
        del trust_store, context
        self.calls += 1
        self.order.append("verify")
        return ApprovalVerificationResult(
            approved=self.approved,
            reason_code=self.reason_code,
            grant=grant if self.approved else None,
            grant_sha256=_sha("e") if self.approved else None,
        )


class FakeGrantStore:
    def __init__(self, order: list[str], *, accepted: bool = True) -> None:
        self.order = order
        self.accepted = accepted
        self.reserve_calls = []
        self.terminal_calls = []

    def reserve(self, **kwargs):
        self.order.append("reserve")
        self.reserve_calls.append(kwargs)
        return GrantUseResult(self.accepted, None if self.accepted else "GRANT_ID_ALREADY_RESERVED", None)

    def record_terminal(self, **kwargs):
        self.order.append("terminal")
        self.terminal_calls.append(kwargs)
        return GrantUseResult(True, None, None)


class FakeAuditSink:
    def __init__(self, order: list[str], *, fail: bool = False) -> None:
        self.order = order
        self.fail = fail
        self.records: list[CanaryLifecycleAuditRecord] = []

    def append(self, record: CanaryLifecycleAuditRecord) -> None:
        self.order.append("audit")
        if self.fail:
            raise OSError("audit unavailable")
        self.records.append(record)


class FakeRouteStateStore:
    def __init__(self, order: list[str], *, fail_marker: bool = False) -> None:
        self.order = order
        self.fail_marker = fail_marker
        self.marker_calls = []
        self.clear_calls = []

    def begin_activation(self, **kwargs):
        self.order.append("marker")
        self.marker_calls.append(kwargs)
        if self.fail_marker:
            raise OSError("route state unavailable")
        return RouteStateRecord(
            state=RouteState.ACTIVATING,
            binding=kwargs["binding"],
            grant_id=kwargs["grant_id"],
            plan_sha256=kwargs["plan_sha256"],
            changed_at_utc=kwargs["changed_at_utc"],
            reason_code="ACTIVATION_PENDING",
        )

    def clear_to_lkg(self, **kwargs):
        self.order.append("state_clear")
        self.clear_calls.append(kwargs)
        return RouteStateRecord(
            state=RouteState.LKG_ONLY,
            binding=kwargs["binding"],
            grant_id=None,
            plan_sha256=None,
            changed_at_utc=kwargs["changed_at_utc"],
            reason_code=kwargs["reason_code"],
        )


class FakeAuthority:
    def __init__(self, order: list[str], *, fail_activation: bool = False) -> None:
        self.order = order
        self.fail_activation = fail_activation
        self.activate_calls = []
        self.clear_calls = []
        self.claim_calls = 0

    def activate(self, **kwargs):
        self.order.append("authority")
        self.activate_calls.append(kwargs)
        if self.fail_activation:
            raise ValueError("authority rejected marker")
        return RouteAuthoritySnapshot(
            RouteAuthorityState.ACTIVE,
            kwargs["activation_marker"].grant_id,
            kwargs["plan"].plan_sha256,
            0,
            "ROUTE_ACTIVE",
        )

    def clear(self, **kwargs):
        self.order.append("authority_clear")
        self.clear_calls.append(kwargs)
        return RouteAuthoritySnapshot(RouteAuthorityState.LKG_ONLY, None, None, 0, kwargs["reason_code"])

    def resolve_and_claim(self, occurrence_id):
        self.claim_calls += 1
        raise AssertionError(f"no foreground route claim is permitted: {occurrence_id}")


class FakeAutomaticActionController:
    def __init__(self, *, disabled: bool = False, fail: bool = False) -> None:
        self.disabled = disabled
        self.fail = fail
        self.calls = 0

    def is_disabled(self) -> bool:
        self.calls += 1
        if self.fail:
            raise OSError("controller unavailable")
        return self.disabled


class CanaryActivationCoordinatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        root = Path(cls._temporary.name)
        dataset001 = root / "dataset001"
        dataset002 = root / "dataset002"
        source = generate_dataset(
            replace(
                EXP001_DATASET_SPEC,
                version="dataset001-activation-fixture-v1",
                dimensions=4,
                base_count=100,
                calibration_query_count=5,
                measured_query_count=7,
            )
        )
        write_dataset_artifacts(
            dataset001,
            source,
            calibrate_thresholds(source.base_vectors, source.calibration_queries),
            boundary_fixtures(),
        )
        write_dataset002_artifacts(
            dataset002,
            generate_dataset002(
                Dataset002Spec(
                    dataset_id="DATASET-002",
                    version="dataset002-activation-fixture-v1",
                    seed=20260809,
                    dimensions=4,
                    routing_query_count=600,
                    recall_audit_query_count=1200,
                    dtype="<f4",
                    distribution="independent standard normal",
                    generator="numpy.random.Generator(numpy.random.PCG64(seed))",
                )
            ),
            dataset001_dir=dataset001,
        )
        identity = WorkloadIdentityBinding(
            configuration_identity="exp009-activation-config-v1",
            data_identity=(
                "dataset001-activation-fixture-v1:sha256:"
                + sha256_file(dataset001 / "generation_manifest.json")
            ),
            flat_binding_id="flat-activation-binding-v1",
            hnsw_binding_id="hnsw-activation-binding-v1",
        )
        manifest = build_eligible_workload_manifest(
            dataset002_dir=dataset002,
            dataset001_dir=dataset001,
            metric=Metric.L2,
            threshold_stratum="target-075",
            candidate_ef=800,
            last_known_good_ef=400,
            identity=identity,
            created_at_utc="2026-08-04T10:00:00Z",
        )
        manifest_sha256 = hashlib.sha256(canonical_json_bytes(manifest.to_document())).hexdigest()
        selection = CandidateSelectionRecord(
            schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
            selected_at_utc="2026-08-04T10:00:01Z",
            eligible_manifest_sha256=manifest_sha256,
            population_count=600,
            candidate_count=60,
            candidate_fraction=0.10,
            candidate_occurrence_ids=tuple(
                item.occurrence_id for item in manifest.occurrences if item.sequence_index % 10 == 0
            ),
            random_source="python.secrets.SystemRandom.sample",
            selected_before_candidate_results=True,
        )
        cls.plan = build_canary_route_plan(manifest, selection)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def _coordinator(self, *, verifier=None, grant_store=None, audit=None, state=None, authority=None, controller=None):
        order: list[str] = []
        verifier = verifier or FakeVerifier()
        grant_store = grant_store or FakeGrantStore(order)
        audit = audit or FakeAuditSink(order)
        state = state or FakeRouteStateStore(order)
        authority = authority or FakeAuthority(order)
        controller = controller or FakeAutomaticActionController()
        verifier.order = order
        grant_store.order = order
        audit.order = order
        state.order = order
        authority.order = order
        return (
            CanaryActivationCoordinator(
                verifier=verifier,
                grant_store=grant_store,
                lifecycle_audit_sink=audit,
                route_state_store=state,
                route_authority=authority,
                automatic_action_controller=controller,
            ),
            order,
            grant_store,
            audit,
            state,
            authority,
            controller,
        )

    def _activate(self, coordinator, *, grant=None, plan=None):
        selected_plan = self.plan if plan is None else plan
        return coordinator.activate(
            grant=_grant(selected_plan) if grant is None else grant,
            trust_store=object(),
            approval_context=object(),
            plan=selected_plan,
            binding=_binding(selected_plan),
            timestamps=_TIMESTAMPS,
        )

    def test_success_reserves_audits_marks_then_publishes_without_a_route_claim(self) -> None:
        coordinator, order, store, audit, state, authority, _ = self._coordinator()

        result = self._activate(coordinator)

        self.assertTrue(result.activated)
        self.assertEqual(result.reason_code, "ACTIVATION_PUBLISHED")
        self.assertEqual(order, ["verify", "reserve", "audit", "marker", "authority"])
        self.assertEqual(audit.records[0].event_type, "ACTIVATION_AUTHORIZED")
        self.assertEqual(audit.records[0].reason_code, "ACTIVATION_PENDING")
        self.assertEqual(state.marker_calls[0]["plan_sha256"], self.plan.plan_sha256)
        self.assertEqual(
            authority.activate_calls[0]["expires_at_utc"],
            _grant(self.plan).expires_at_utc,
        )
        self.assertEqual(authority.claim_calls, 0)

    def test_disabled_or_unavailable_controller_blocks_before_verifier_or_route_work(self) -> None:
        for controller, reason in (
            (FakeAutomaticActionController(disabled=True), "AUTOMATIC_ACTIONS_DISABLED"),
            (FakeAutomaticActionController(fail=True), "AUTOMATIC_ACTION_CONTROLLER_UNAVAILABLE"),
        ):
            with self.subTest(reason=reason):
                verifier = FakeVerifier()
                coordinator, order, store, audit, state, authority, selected_controller = self._coordinator(
                    verifier=verifier,
                    controller=controller,
                )

                result = self._activate(coordinator)

                self.assertFalse(result.activated)
                self.assertEqual(result.reason_code, reason)
                self.assertEqual(selected_controller.calls, 1)
                self.assertEqual(order, [])
                self.assertEqual(verifier.calls, 0)
                self.assertEqual(store.reserve_calls, [])
                self.assertEqual(audit.records, [])
                self.assertEqual(state.marker_calls, [])
                self.assertEqual(authority.activate_calls, [])
        self.assertEqual(store.terminal_calls, [])

    def test_restart_durable_disabled_controller_blocks_activation_before_verification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller_path = Path(directory) / "automatic-actions.json"
            FileAutomaticActionController(
                controller_path,
                clock=lambda: "2026-08-04T10:02:00Z",
            ).disable_automatic_actions(
                audit_id="stage3-rollback-audit-001",
                reason="ROLLBACK_QUERY_FAILURE",
            )
            verifier = FakeVerifier()
            coordinator, order, store, audit, state, authority, controller = self._coordinator(
                verifier=verifier,
                controller=FileAutomaticActionController(controller_path),
            )

            result = self._activate(coordinator)

            self.assertFalse(result.activated)
            self.assertEqual(result.reason_code, "AUTOMATIC_ACTIONS_DISABLED")
            self.assertEqual(order, [])
            self.assertEqual(verifier.calls, 0)
            self.assertEqual(store.reserve_calls, [])
            self.assertEqual(audit.records, [])
            self.assertEqual(state.marker_calls, [])
            self.assertEqual(authority.activate_calls, [])
            self.assertTrue(controller.is_disabled())

    def test_real_signed_components_persist_a_published_plan_without_a_route_claim(self) -> None:
        directory = Path(self._temporary.name) / "real-composition"
        directory.mkdir(mode=0o700)
        private_key = Ed25519PrivateKey.generate()
        decision = _real_decision(self.plan)
        grant = _signed_real_grant(self.plan, decision, private_key)
        authority = CanaryRouteAuthority(
            clock=lambda: datetime(2026, 8, 4, 10, 5, tzinfo=timezone.utc)
        )
        coordinator = CanaryActivationCoordinator(
            grant_store=CanaryGrantUseStore(directory / "grant-ledger.sqlite"),
            lifecycle_audit_sink=JsonlCanaryLifecycleAuditSink(directory / "lifecycle.jsonl"),
            route_state_store=FileCanaryRouteStateStore(directory / "route-state.json"),
            route_authority=authority,
            automatic_action_controller=FileAutomaticActionController(
                directory / "automatic-actions.json"
            ),
        )
        trust_store = StaticCanaryApprovalTrustStore(
            public_keys={"operator-001": private_key.public_key()}
        )
        context = ApprovalVerificationContext(
            decision=decision,
            expected_experiment_id="EXP-009",
            eligible_workload_sha256=self.plan.eligible_workload_sha256,
            candidate_selection_sha256=self.plan.candidate_selection_sha256,
            now_utc="2026-08-04T10:05:00Z",
        )

        result = coordinator.activate(
            grant=grant,
            trust_store=trust_store,
            approval_context=context,
            plan=self.plan,
            binding=_binding(self.plan),
            timestamps=_TIMESTAMPS,
        )

        ledger = CanaryGrantUseStore(directory / "grant-ledger.sqlite").load(grant.grant_id)
        audit = JsonlCanaryLifecycleAuditSink(directory / "lifecycle.jsonl").records()
        marker = FileCanaryRouteStateStore(directory / "route-state.json").load()
        self.assertTrue(result.activated)
        self.assertEqual(result.reason_code, "ACTIVATION_PUBLISHED")
        self.assertIsNotNone(ledger)
        self.assertEqual(ledger.status, GrantUseStatus.RESERVED)
        self.assertEqual(len(audit), 1)
        self.assertEqual(audit[0].event_id, result.authorization_event_id)
        self.assertIsNotNone(marker)
        self.assertEqual(marker.state, RouteState.ACTIVATING)
        self.assertEqual(authority.snapshot().claimed_occurrence_count, 0)

    def test_verifier_refusal_calls_no_downstream_dependency(self) -> None:
        verifier = FakeVerifier(approved=False, reason_code="GRANT_EXPIRED")
        coordinator, order, store, audit, state, authority, _ = self._coordinator(verifier=verifier)

        result = self._activate(coordinator)

        self.assertFalse(result.activated)
        self.assertEqual(result.reason_code, "GRANT_EXPIRED")
        self.assertEqual(order, ["verify"])
        self.assertEqual(store.reserve_calls, [])
        self.assertEqual(audit.records, [])
        self.assertEqual(state.marker_calls, [])
        self.assertEqual(authority.activate_calls, [])
        self.assertEqual(authority.claim_calls, 0)

    def test_grant_plan_binding_mismatch_calls_no_downstream_dependency(self) -> None:
        coordinator, order, store, audit, state, authority, _ = self._coordinator()

        result = self._activate(coordinator, grant=replace(_grant(self.plan), candidate_ef=1600))

        self.assertFalse(result.activated)
        self.assertEqual(result.reason_code, "GRANT_PLAN_BINDING_MISMATCH")
        self.assertEqual(order, ["verify"])
        self.assertEqual(store.reserve_calls, [])
        self.assertEqual(audit.records, [])
        self.assertEqual(state.marker_calls, [])
        self.assertEqual(authority.activate_calls, [])

    def test_reservation_refusal_does_not_create_audit_marker_or_authority(self) -> None:
        coordinator, order, store, audit, state, authority, _ = self._coordinator(
            grant_store=FakeGrantStore([], accepted=False)
        )

        result = self._activate(coordinator)

        self.assertFalse(result.activated)
        self.assertEqual(result.reason_code, "GRANT_ID_ALREADY_RESERVED")
        self.assertEqual(order, ["verify", "reserve"])
        self.assertEqual(len(store.reserve_calls), 1)
        self.assertEqual(audit.records, [])
        self.assertEqual(state.marker_calls, [])
        self.assertEqual(authority.activate_calls, [])
        self.assertEqual(authority.claim_calls, 0)

    def test_audit_failure_consumes_grant_without_marker_or_authority(self) -> None:
        coordinator, order, store, audit, state, authority, _ = self._coordinator(
            audit=FakeAuditSink([], fail=True)
        )

        result = self._activate(coordinator)

        self.assertFalse(result.activated)
        self.assertEqual(result.reason_code, "REFUSED_AUDIT_WRITE_FAILED")
        self.assertEqual(order, ["verify", "reserve", "audit", "terminal"])
        self.assertEqual(store.terminal_calls[0]["reason_code"], "REFUSED_AUDIT_WRITE_FAILED")
        self.assertEqual(state.marker_calls, [])
        self.assertEqual(authority.activate_calls, [])
        self.assertEqual(authority.claim_calls, 0)

    def test_marker_failure_clears_authority_restores_lkg_and_consumes_grant(self) -> None:
        coordinator, order, store, _, state, authority, _ = self._coordinator(
            state=FakeRouteStateStore([], fail_marker=True)
        )

        result = self._activate(coordinator)

        self.assertFalse(result.activated)
        self.assertEqual(result.reason_code, "REFUSED_ROUTE_STATE_WRITE_FAILED")
        self.assertEqual(order, ["verify", "reserve", "audit", "marker", "authority_clear", "state_clear", "terminal"])
        self.assertEqual(authority.activate_calls, [])
        self.assertEqual(authority.clear_calls[0]["reason_code"], "ACTIVATION_MARKER_WRITE_FAILED")
        self.assertEqual(state.clear_calls[0]["reason_code"], "ACTIVATION_MARKER_WRITE_FAILED")
        self.assertEqual(store.terminal_calls[0]["reason_code"], "REFUSED_ROUTE_STATE_WRITE_FAILED")

    def test_authority_failure_clears_lkg_marker_and_consumes_grant(self) -> None:
        coordinator, order, store, _, state, authority, _ = self._coordinator(
            authority=FakeAuthority([], fail_activation=True)
        )

        result = self._activate(coordinator)

        self.assertFalse(result.activated)
        self.assertEqual(result.reason_code, "REFUSED_ROUTE_AUTHORITY_FAILED")
        self.assertEqual(order, ["verify", "reserve", "audit", "marker", "authority", "authority_clear", "state_clear", "terminal"])
        self.assertEqual(authority.clear_calls[0]["reason_code"], "ACTIVATION_AUTHORITY_REFUSED")
        self.assertEqual(state.clear_calls[0]["reason_code"], "ACTIVATION_AUTHORITY_REFUSED")
        self.assertEqual(store.terminal_calls[0]["reason_code"], "REFUSED_ROUTE_AUTHORITY_FAILED")
        self.assertEqual(authority.claim_calls, 0)

    def test_module_cannot_import_a_live_milvus_or_request_execution_layer(self) -> None:
        source = Path("src/vdbench/canary_activation.py").read_text(encoding="utf-8")
        imported = {
            alias.name
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            node.module or ""
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.ImportFrom)
        )

        self.assertFalse(
            {"pymilvus", "vdbench.milvus", "vdbench.milvus_actuation", "vdbench.execute_live"}
            & imported
        )


if __name__ == "__main__":
    unittest.main()
