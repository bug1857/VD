"""Fake-port verification for the human-gated EXP-009 Stage-4 live root.

These tests deliberately exercise no network or database client.  They prove
that the composition root neither selects routes itself nor leaves candidate
routing active after a failed or completed serial schedule.
"""

from __future__ import annotations

import ast
import hashlib
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from tests.test_canary_admission import _phase3_authority
from vdbench.artifacts import canonical_json_bytes
from vdbench.canary_activation import (
    ActivationAttempt,
    ActivationTimestamps,
    ActiveCanaryContext,
)
from vdbench.canary_admission import (
    Stage4AdmissionReceipt,
    Stage4AdmissionResult,
    Stage4LkgAuthorityPair,
    Stage4RepositoryEvidence,
    Stage4RuntimeReadiness,
    bind_stage4_lkg_authority,
    evaluate_stage4_admission,
)
from vdbench.canary_execution_ledger import (
    Stage4ExecutionLedger,
    Stage4LedgerError,
    Stage4LedgerStatus,
    Stage4SlotObservation,
)
from vdbench.canary_live_runner import (
    Stage4LiveRunner,
    Stage4LiveRunRequest,
    Stage4SlotSafety,
)
from vdbench.canary_rollback import RollbackResult, RollbackTrigger
from vdbench.canary_route_authority import RouteClaim
from vdbench.canary_route_state import RouteStateBinding
from vdbench.canary_routing import build_canary_route_plan
from vdbench.canary_schedule import build_stage4_execution_schedule
from vdbench.canary_serial_runner import Dataset002ScheduleVectorSource
from vdbench.canary_stage4_evidence_binding import Stage4EvidenceBinding
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING,
    SCHEDULE_EXECUTION_MODE,
    SCHEDULE_INTERLEAVED_SWEEP_COUNT,
    SCHEDULE_MEDIAN_RELATIVE_CEILING,
    SCHEDULE_P95_RELATIVE_CEILING,
    SCHEDULE_POST_SWEEP_COUNT,
    SCHEDULE_PRE_SWEEP_COUNT,
    SCHEDULE_ROUTING_BLOCK_SIZE,
    SCHEDULE_STABILITY_SCHEMA_VERSION,
    CandidateSelectionRecord,
    EligibleOccurrence,
    EligibleWorkloadManifest,
    ScheduleControl,
    ScheduleStabilityContract,
    WorkloadIdentityBinding,
)
from vdbench.config import RESULT_LIMIT, Metric
from vdbench.drift import build_evidence_provenance
from vdbench.host_observation import ServedQueryOutcome
from vdbench.lkg_phase3_persistence import LkgPhase3AuthorityReferenceStore
from vdbench.policy import (
    PolicyAction,
    PolicyDecision,
    PolicyMode,
    SafetyGateResult,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class _Clock:
    def __init__(self, *, fail_at_call: int | None = None) -> None:
        self._calls = 0
        self._value = 0
        self._fail_at_call = fail_at_call

    def __call__(self) -> int:
        self._calls += 1
        if self._calls == self._fail_at_call:
            raise RuntimeError("clock unavailable")
        self._value += 1_000_000
        return self._value


class _Source:
    def __init__(self, *, fail_on_first_call: bool = False) -> None:
        self.calls: list[tuple[str, int]] = []
        self._fail_on_first_call = fail_on_first_call

    def control_vector(self, *, control: ScheduleControl) -> tuple[float, ...]:
        self.calls.append(("control", control.query_id))
        if self._fail_on_first_call and len(self.calls) == 1:
            raise RuntimeError("source unavailable")
        return (float(control.query_id), 0.0)

    def routing_vector(
        self,
        *,
        occurrence_id: str,
        dataset_query_id: int,
        vector_sha256: str,
    ) -> tuple[float, ...]:
        del occurrence_id, vector_sha256
        self.calls.append(("routing", dataset_query_id))
        if self._fail_on_first_call and len(self.calls) == 1:
            raise RuntimeError("source unavailable")
        return (float(dataset_query_id), 1.0)


class _Activation:
    def __init__(self, *, binding: RouteStateBinding, plan_sha256: str, mismatch: bool = False) -> None:
        self.calls: list[dict[str, object]] = []
        self._binding = binding
        self._plan_sha256 = plan_sha256
        self._mismatch = mismatch

    def activate(self, **kwargs: object) -> ActivationAttempt:
        self.calls.append(kwargs)
        context = ActiveCanaryContext(
            grant_id="grant-live-runner-001",
            signed_payload_sha256="b" * 64,
            policy_audit_id="policy-audit-live-runner-001",
            plan_sha256=("c" * 64 if self._mismatch else self._plan_sha256),
            binding=self._binding,
            activated_at_utc="2026-08-04T20:00:01Z",
        )
        return ActivationAttempt(
            activated=True,
            reason_code="ACTIVATED",
            grant_id=context.grant_id,
            plan_sha256=context.plan_sha256,
            authorization_event_id="activation-event-001",
            active_context=context,
        )


class _Authority:
    def __init__(self, plan, *, mismatch: bool = False) -> None:
        self._plan = plan
        self._mismatch = mismatch
        self.calls: list[str] = []

    def resolve_and_claim(self, occurrence_id: object) -> RouteClaim:
        self.calls.append(str(occurrence_id))
        resolution = self._plan.resolve(occurrence_id)
        if self._mismatch:
            return RouteClaim(
                accepted=True,
                occurrence_id=resolution.occurrence_id,
                dataset_query_id=resolution.dataset_query_id,
                ef=400 if resolution.ef == 800 else 800,
                kind=resolution.kind,
            )
        return RouteClaim(
            accepted=resolution.accepted,
            occurrence_id=resolution.occurrence_id,
            dataset_query_id=resolution.dataset_query_id,
            ef=resolution.ef,
            kind=resolution.kind,
            reason_code=resolution.reason_code,
        )


class _RuntimeProbe:
    def __init__(
        self,
        binding: RouteStateBinding,
        *,
        preflight_complete: tuple[bool, ...] = (True, True),
        unsafe_slot_call: int | None = None,
        unsafe_identity: bool = False,
    ) -> None:
        self._binding = binding
        self._preflight_complete = preflight_complete
        self._unsafe_slot_call = unsafe_slot_call
        self._unsafe_identity = unsafe_identity
        self.preflight_calls = 0
        self.slot_calls = 0

    def preflight(self, *, binding: object) -> Stage4RuntimeReadiness:
        self.preflight_calls += 1
        if binding != self._binding:
            raise AssertionError("unexpected binding")
        value = self._preflight_complete[min(self.preflight_calls - 1, len(self._preflight_complete) - 1)]
        return Stage4RuntimeReadiness(
            binding=self._binding,
            serving_preflight_complete=value,
            observed_at_utc=f"2026-08-04T20:00:0{self.preflight_calls}Z",
            reason_codes=() if value else ("STACK_HEALTH_UNHEALTHY",),
        )

    def slot_safety(self, *, binding: object) -> Stage4SlotSafety:
        self.slot_calls += 1
        if binding != self._binding:
            raise AssertionError("unexpected binding")
        if self.slot_calls == self._unsafe_slot_call:
            return Stage4SlotSafety(
                self._unsafe_identity,
                not self._unsafe_identity,
                "COLLECTION_IDENTITY_MISMATCH" if self._unsafe_identity else "STACK_HEALTH_UNHEALTHY",
            )
        return Stage4SlotSafety(True, True)


class _Serving:
    def __init__(
        self,
        *,
        failure_at_call: int | None = None,
        failure_code: str = "MILVUS_SEARCH_FAILED",
        timed_out: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self._failure_at_call = failure_at_call
        self._failure_code = failure_code
        self._timed_out = timed_out
        self._order = order

    def execute(self, request):
        self.calls.append(request)
        if self._order is not None:
            self._order.append(f"serving.execute:{request.request_id}")
        if len(self.calls) == self._failure_at_call:
            return ServedQueryOutcome(False, self._timed_out, 0, 1.0, self._failure_code)
        return ServedQueryOutcome(True, False, 2, 1.0)


class _Rollback:
    def __init__(self) -> None:
        self.calls = []

    def rollback(self, request):
        self.calls.append(request)
        return RollbackResult(
            contained=True,
            restoration_verified=True,
            reason_code="RESTORED",
            trigger_event_id="rollback-event-001",
            restoration_event_id="restoration-event-001",
            automatic_actions_disabled=False,
        )


class _InvalidRollback:
    def __init__(self) -> None:
        self.calls = []

    def rollback(self, request):
        self.calls.append(request)
        return object()


class _FreshAuthorityProvider:
    def __init__(
        self,
        pairs: tuple[Stage4LkgAuthorityPair, ...],
        *,
        fail_at_call: int | None = None,
    ) -> None:
        self._pairs = pairs
        self._fail_at_call = fail_at_call
        self.calls = 0
        self.returned: list[Stage4LkgAuthorityPair] = []

    def refresh(self) -> Stage4LkgAuthorityPair:
        self.calls += 1
        if self.calls == self._fail_at_call:
            raise RuntimeError("fresh Phase-3 authority unavailable")
        pair = self._pairs[min(self.calls - 1, len(self._pairs) - 1)]
        self.returned.append(pair)
        return pair


def _forge_canonical_receipt(
    receipt: Stage4AdmissionReceipt,
    **changes: object,
) -> Stage4AdmissionReceipt:
    payload = receipt.to_document()
    payload.pop("canonical_receipt_digest")
    payload.update(changes)
    forged = object.__new__(Stage4AdmissionReceipt)
    for field, value in payload.items():
        object.__setattr__(forged, field, value)
    object.__setattr__(
        forged,
        "canonical_receipt_digest",
        hashlib.sha256(
            b"vdbench.stage4-admission-receipt.v1\0"
            + canonical_json_bytes(payload)
        ).hexdigest(),
    )
    return forged


class _AdmissionEvaluator:
    def __init__(
        self,
        *,
        mutate_second: tuple[str, object] | None = None,
        corrupt_first: bool = False,
    ) -> None:
        self.calls = []
        self._mutate_second = mutate_second
        self._corrupt_first = corrupt_first

    def __call__(self, request: object) -> Stage4AdmissionResult:
        self.calls.append(request)
        result = evaluate_stage4_admission(request)
        if result.receipt is None:
            return result
        if self._corrupt_first and len(self.calls) == 1:
            object.__setattr__(result.receipt, "canonical_receipt_digest", "0" * 64)
            return result
        if self._mutate_second is not None and len(self.calls) == 2:
            field, value = self._mutate_second
            return Stage4AdmissionResult(
                receipt=_forge_canonical_receipt(result.receipt, **{field: value}),
                reason_codes=(),
            )
        return result


class _FailingAppendLedger(Stage4ExecutionLedger):
    """Test-only durable ledger that refuses its first post-search append."""

    def complete_slot(self, observation: object, *, started_record_sha256: object):
        del observation, started_record_sha256
        raise Stage4LedgerError("simulated ledger unavailable")


class _InvalidAppendLedger(Stage4ExecutionLedger):
    """Test-only port violation: a ledger append has no usable receipt."""

    def complete_slot(self, observation: object, *, started_record_sha256: object):
        del observation, started_record_sha256
        return object()


class _FailingStartLedger(Stage4ExecutionLedger):
    """Test-only durable ledger that refuses every start_slot commit."""

    def start_slot(self, execution_index: object, **kwargs: object):
        del execution_index, kwargs
        raise Stage4LedgerError("simulated ledger start unavailable")


class _RecordingSlotOrderLedger(Stage4ExecutionLedger):
    """Test-only ledger recording call order to prove STARTED precedes dispatch."""

    def __init__(self, *args: object, order: list[str], **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.order = order

    def start_slot(self, execution_index: object, **kwargs: object):
        self.order.append(f"start_slot:{execution_index}")
        return super().start_slot(execution_index, **kwargs)

    def complete_slot(self, observation: object, *, started_record_sha256: object):
        index = getattr(observation, "execution_index", None)
        self.order.append(f"complete_slot:{index}")
        return super().complete_slot(
            observation, started_record_sha256=started_record_sha256
        )


class CanaryLiveRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        controls = tuple(ScheduleControl(600 + index, _sha(f"control-{index}")) for index in range(50))
        stability = ScheduleStabilityContract(
            schema_version=SCHEDULE_STABILITY_SCHEMA_VERSION,
            control_role="recall_audit",
            control_ef=400,
            controls=controls,
            pre_sweep_count=SCHEDULE_PRE_SWEEP_COUNT,
            routing_block_size=SCHEDULE_ROUTING_BLOCK_SIZE,
            interleaved_sweep_count=SCHEDULE_INTERLEAVED_SWEEP_COUNT,
            post_sweep_count=SCHEDULE_POST_SWEEP_COUNT,
            execution_mode=SCHEDULE_EXECUTION_MODE,
            absolute_p95_latency_ms_ceiling=SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING,
            p95_relative_ceiling=SCHEDULE_P95_RELATIVE_CEILING,
            median_relative_ceiling=SCHEDULE_MEDIAN_RELATIVE_CEILING,
            require_all_success=True,
            require_identity_and_health_per_sweep=True,
        )
        cls.manifest = EligibleWorkloadManifest(
            schema_version="exp009-eligible-workload-manifest-v2",
            created_at_utc="2026-08-04T20:00:00Z",
            dataset002_manifest_sha256=_sha("dataset002"),
            dataset001_generation_manifest_sha256=_sha("dataset001"),
            metric=Metric.L2,
            threshold_stratum="target-075",
            candidate_ef=800,
            last_known_good_ef=400,
            radius=0.75,
            range_filter=0.0,
            limit=RESULT_LIMIT,
            identity=WorkloadIdentityBinding("config", "data", "flat", "hnsw"),
            vector_mapping="one_to_one_unique_dataset002_routing_vectors",
            schedule_stability=stability,
            occurrences=tuple(
                EligibleOccurrence(index, f"exp009-routing-{index:06d}", index, _sha(f"route-{index}"), 0.75, 0.0, RESULT_LIMIT)
                for index in range(600)
            ),
        )
        cls.manifest.validate()
        cls.selection = CandidateSelectionRecord(
            schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
            selected_at_utc="2026-08-04T20:00:00Z",
            eligible_manifest_sha256=hashlib.sha256(canonical_json_bytes(cls.manifest.to_document())).hexdigest(),
            population_count=600,
            candidate_count=60,
            candidate_fraction=0.10,
            candidate_occurrence_ids=tuple(item.occurrence_id for item in cls.manifest.occurrences if item.sequence_index % 10 == 0),
            random_source="python.secrets.SystemRandom.sample",
            selected_before_candidate_results=True,
        )
        cls.plan = build_canary_route_plan(cls.manifest, cls.selection)
        cls.schedule = build_stage4_execution_schedule(cls.manifest, cls.plan)
        cls.binding = RouteStateBinding(Metric.L2, "target-075", 400, "config", "data", "flat", "hnsw")
        cls._phase3_temporary = tempfile.TemporaryDirectory()
        phase3_root = Path(cls._phase3_temporary.name)
        cls.authority = _phase3_authority(
            plan=cls.plan,
            radius=cls.manifest.radius,
            identifier=201,
        )
        cls.other_authority = _phase3_authority(
            plan=cls.plan,
            radius=cls.manifest.radius,
            identifier=202,
        )
        cls.lkg_pair = cls._persisted_pair(
            cls.authority, phase3_root / "first.db", "2026-08-08T14:00:00.000000Z"
        )
        cls.other_lkg_pair = cls._persisted_pair(
            cls.other_authority,
            phase3_root / "second.db",
            "2026-08-08T14:01:00.000000Z",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._phase3_temporary.cleanup()

    @staticmethod
    def _persisted_pair(authority, path: Path, timestamp: str) -> Stage4LkgAuthorityPair:
        with LkgPhase3AuthorityReferenceStore(path) as store:
            store.append(authority, persisted_at_utc=timestamp)
            latest = store.load_verified_latest()
        assert latest is not None
        return bind_stage4_lkg_authority(
            authority=authority, verified_latest_reference=latest
        )

    def _policy_decision(self) -> PolicyDecision:
        provenance = build_evidence_provenance(
            metric=self.plan.metric,
            threshold_stratum=self.plan.threshold_stratum,
            reference_window_id="reference-window-live-001",
            current_window_id="current-window-live-001",
            reference_manifest_sha256=_sha("reference-manifest"),
            current_manifest_sha256=_sha("current-manifest"),
            configuration_identity=self.plan.configuration_identity,
            data_identity=self.plan.data_identity,
            flat_binding_id=self.plan.flat_binding_id,
            hnsw_binding_id=self.plan.hnsw_binding_id,
            reference_audit_ids=tuple(f"reference-{index:02d}" for index in range(50)),
            reference_audit_rank_digests=tuple(_sha(f"rr-{index}") for index in range(50)),
            current_audit_ids=tuple(f"current-{index:02d}" for index in range(50)),
            current_audit_rank_digests=tuple(_sha(f"cr-{index}") for index in range(50)),
        )
        return PolicyDecision(
            PolicyAction.START_CANARY, 400, 800, 400, 0.99, 0.98, 4.0, 5.0,
            0.02, None, "QUALITY_DRIFT_RECOVERY", 0.999, 2.0,
            (SafetyGateResult("PRE_ACTION", True, "passed"),),
            PolicyMode.CANARY_ENABLED, "policy-audit-live-runner-001",
            evidence_provenance=provenance,
        )

    def _evidence_binding(self) -> Stage4EvidenceBinding:
        return Stage4EvidenceBinding(
            run_id="exp009-live-runner-test",
            source_revision="a" * 40,
            metric=self.plan.metric,
            threshold_stratum=self.plan.threshold_stratum,
            current_ef=self.plan.last_known_good_ef,
            candidate_ef=self.plan.candidate_ef,
            last_known_good_ef=self.plan.last_known_good_ef,
            candidate_search_configuration=replace(
                self.authority.search_configuration, ef=self.plan.candidate_ef
            ),
            identity=self.manifest.identity,
            dataset002_manifest_sha256=self.manifest.dataset002_manifest_sha256,
            frozen_recall_audit_ids_sha256=_sha("frozen-recall-audit"),
            eligible_workload_sha256=self.plan.eligible_workload_sha256,
            candidate_selection_sha256=self.plan.candidate_selection_sha256,
            execution_schedule_sha256=self.schedule.schedule_sha256,
            recall_evidence_schema_version="recall-audit-hoeffding-1200-v1",
            latency_evidence_schema_version="stage4-schedule-evaluation-v1",
        )

    def _live_request(self) -> Stage4LiveRunRequest:
        return Stage4LiveRunRequest(
            manifest=self.manifest,
            selection=self.selection,
            plan=self.plan,
            schedule=self.schedule,
            policy_decision=self._policy_decision(),
            evidence_binding=self._evidence_binding(),
            repository=Stage4RepositoryEvidence(
                "a" * 40, True, "2026-08-04T20:00:00Z"
            ),
            runtime_binding=self.binding,
            grant=object(),
            trust_store=object(),
            approval_context=object(),
            activation_timestamps=ActivationTimestamps(
                "2026-08-04T20:00:00Z",
                "2026-08-04T20:00:01Z",
                "2026-08-04T20:00:02Z",
                "2026-08-04T20:00:03Z",
            ),
            run_id="exp009-live-runner-test",
        )

    def test_live_request_run_id_must_match_stage4_evidence_lineage(self) -> None:
        request = self._live_request()
        with self.assertRaisesRegex(
            ValueError, "run_id must equal evidence_binding.run_id"
        ):
            replace(request, run_id="different-stage4-run")

    def _ledger(self, root: Path) -> Stage4ExecutionLedger:
        private = root / "private"
        private.mkdir(mode=0o700)
        return Stage4ExecutionLedger(private / "stage4.sqlite3", run_id="exp009-live-runner-test", schedule=self.schedule)

    def _runner(
        self,
        root: Path,
        *,
        preflight_complete: tuple[bool, ...] = (True, True),
        unsafe_slot_call: int | None = None,
        unsafe_identity: bool = False,
        serving_failure_at_call: int | None = None,
        serving_failure_code: str = "MILVUS_SEARCH_FAILED",
        serving_timed_out: bool = False,
        wrong_claim: bool = False,
        activation_mismatch: bool = False,
        source_failure: bool = False,
        rollback_port: object | None = None,
        clock: _Clock | None = None,
        ledger: Stage4ExecutionLedger | None = None,
        provider: _FreshAuthorityProvider | None = None,
        evaluator: _AdmissionEvaluator | None = None,
    ):
        actual_ledger = ledger or self._ledger(root)
        activation = _Activation(binding=self.binding, plan_sha256=self.plan.plan_sha256, mismatch=activation_mismatch)
        authority = _Authority(self.plan, mismatch=wrong_claim)
        runtime = _RuntimeProbe(
            self.binding,
            preflight_complete=preflight_complete,
            unsafe_slot_call=unsafe_slot_call,
            unsafe_identity=unsafe_identity,
        )
        serving = _Serving(
            failure_at_call=serving_failure_at_call,
            failure_code=serving_failure_code,
            timed_out=serving_timed_out,
        )
        rollback = _Rollback() if rollback_port is None else rollback_port
        actual_evaluator = evaluator or _AdmissionEvaluator()
        actual_provider = provider or _FreshAuthorityProvider(
            (self.lkg_pair, self.lkg_pair)
        )
        request = self._live_request()
        runner = Stage4LiveRunner(
            request=request, activation=activation, authority=authority, runtime_probe=runtime,
            serving=serving, vector_source=Dataset002ScheduleVectorSource(
                _Source(fail_on_first_call=source_failure)
            ),
            ledger=actual_ledger,
            rollback=rollback,
            lkg_authority_provider=actual_provider,
            admission_evaluator=actual_evaluator,
            monotonic_ns=clock or _Clock(), utc_now=lambda: "2026-08-04T20:00:04Z",
        )
        return runner, activation, authority, runtime, serving, rollback, actual_ledger

    def test_initial_preflight_refusal_dispatches_and_activates_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runner, activation, authority, runtime, serving, rollback, ledger = self._runner(Path(directory), preflight_complete=(False,))
            result = runner.run()
            self.assertEqual(result.reason_codes, ("INITIAL_ADMISSION_REFUSED",))
            self.assertEqual(len(activation.calls), 0)
            self.assertEqual(authority.calls, [])
            self.assertEqual(serving.calls, [])
            self.assertEqual(rollback.calls, [])
            self.assertEqual(ledger.records(), ())
            self.assertEqual(runtime.preflight_calls, 1)

    def test_post_activation_preflight_refusal_contains_without_search(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runner, activation, authority, runtime, serving, rollback, ledger = self._runner(Path(directory), preflight_complete=(True, False))
            result = runner.run()
            self.assertEqual(result.reason_codes, ("POST_ACTIVATION_ADMISSION_REFUSED",))
            self.assertEqual(len(activation.calls), 1)
            self.assertEqual(authority.calls, [])
            self.assertEqual(serving.calls, [])
            self.assertEqual(len(rollback.calls), 1)
            self.assertIs(rollback.calls[0].trigger, RollbackTrigger.RUNTIME_PREFLIGHT_FAILURE)
            self.assertEqual(ledger.records(), ())
            self.assertEqual(runtime.preflight_calls, 2)

    def test_complete_schedule_is_exactly_serial_and_always_finally_rolls_back(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            provider = _FreshAuthorityProvider((self.lkg_pair, self.lkg_pair))
            evaluator = _AdmissionEvaluator()
            runner, activation, authority, runtime, serving, rollback, ledger = self._runner(
                Path(directory), provider=provider, evaluator=evaluator
            )
            result = runner.run()
            self.assertEqual(result.dispatched_slot_count, 1200)
            self.assertEqual(ledger.progress().status, Stage4LedgerStatus.COMPLETE)
            self.assertEqual(len(serving.calls), 1200)
            self.assertEqual(len(authority.calls), 600)
            self.assertEqual(sum(request.served_ef == 800 for request in serving.calls), 60)
            self.assertEqual(sum(request.served_ef == 400 for request in serving.calls), 1140)
            self.assertEqual(runtime.slot_calls, 2400)
            self.assertEqual(len(activation.calls), 1)
            self.assertEqual(len(rollback.calls), 1)
            self.assertIs(rollback.calls[0].trigger, RollbackTrigger.COMPLETED_CANARY)
            self.assertEqual(result.reason_codes, ())
            self.assertEqual(provider.calls, 2)
            self.assertEqual(len(evaluator.calls), 2)
            self.assertIs(evaluator.calls[0].lkg_authority, provider.returned[0])
            self.assertIs(evaluator.calls[1].lkg_authority, provider.returned[1])
            self.assertIsNot(evaluator.calls[0], evaluator.calls[1])
            first_receipt = result.first_admission.receipt
            second_receipt = result.post_activation_admission.receipt
            assert first_receipt is not None and second_receipt is not None
            self.assertEqual(
                first_receipt.checkpoint_c_evaluation_digest,
                self.authority.canonical_evaluation_digest,
            )
            self.assertEqual(
                first_receipt.d2_canonical_record_digest,
                self.lkg_pair.verified_latest_reference.canonical_record_digest,
            )
            self.assertNotEqual(
                first_receipt.runtime_observed_at_utc,
                second_receipt.runtime_observed_at_utc,
            )
            self.assertNotEqual(
                first_receipt.canonical_receipt_digest,
                second_receipt.canonical_receipt_digest,
            )
            self.assertTrue(first_receipt.stable_lineage_matches(second_receipt))

    def test_first_authority_refresh_failure_prevents_activation_and_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            provider = _FreshAuthorityProvider((self.lkg_pair,), fail_at_call=1)
            runner, activation, _authority, _runtime, serving, rollback, ledger = self._runner(
                Path(directory), provider=provider
            )
            result = runner.run()
            records = ledger.records()
        self.assertEqual(result.reason_codes, ("INITIAL_ADMISSION_REFUSED",))
        self.assertEqual(provider.calls, 1)
        self.assertEqual(activation.calls, [])
        self.assertEqual(serving.calls, [])
        self.assertEqual(rollback.calls, [])
        self.assertEqual(records, ())

    def test_second_authority_refresh_failure_rolls_back_without_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            provider = _FreshAuthorityProvider(
                (self.lkg_pair, self.lkg_pair), fail_at_call=2
            )
            runner, activation, _authority, _runtime, serving, rollback, ledger = self._runner(
                Path(directory), provider=provider
            )
            result = runner.run()
            records = ledger.records()
        self.assertEqual(result.reason_codes, ("POST_ACTIVATION_ADMISSION_REFUSED",))
        self.assertEqual(provider.calls, 2)
        self.assertEqual(len(activation.calls), 1)
        self.assertEqual(serving.calls, [])
        self.assertEqual(len(rollback.calls), 1)
        self.assertEqual(records, ())

    def test_changed_verified_d2_head_after_activation_rolls_back_without_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            provider = _FreshAuthorityProvider((self.lkg_pair, self.other_lkg_pair))
            evaluator = _AdmissionEvaluator()
            runner, _activation, _authority, _runtime, serving, rollback, ledger = self._runner(
                Path(directory), provider=provider, evaluator=evaluator
            )
            result = runner.run()
            records = ledger.records()
        self.assertEqual(
            result.reason_codes, ("POST_ACTIVATION_STABLE_LINEAGE_MISMATCH",)
        )
        self.assertEqual(provider.calls, 2)
        self.assertIs(evaluator.calls[0].lkg_authority, self.lkg_pair)
        self.assertIs(evaluator.calls[1].lkg_authority, self.other_lkg_pair)
        self.assertEqual(serving.calls, [])
        self.assertEqual(len(rollback.calls), 1)
        self.assertEqual(records, ())

    def test_every_changed_stable_receipt_lineage_rolls_back_without_dispatch(self) -> None:
        changes = {
            "checkpoint_c_evaluation_digest": _sha("changed-c"),
            "d2_canonical_record_digest": _sha("changed-d2"),
            "d2_sequence_number": 99,
            "stage4_evidence_binding_sha256": _sha("changed-evidence"),
            "execution_schedule_sha256": _sha("changed-schedule"),
            "route_plan_sha256": _sha("changed-plan"),
            "policy_audit_id": "changed-policy-audit",
            "configuration_identity": "changed-configuration",
            "data_identity": "changed-data",
            "hnsw_identity": "changed-hnsw",
            "evaluated_lkg_ef": 200,
            "lkg_search_configuration_digest": _sha("changed-lkg-config"),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, (field, value) in enumerate(changes.items()):
                with self.subTest(field=field):
                    case_root = root / str(index)
                    case_root.mkdir()
                    evaluator = _AdmissionEvaluator(mutate_second=(field, value))
                    runner, _activation, _authority, _runtime, serving, rollback, ledger = self._runner(
                        case_root, evaluator=evaluator
                    )
                    result = runner.run()
                    self.assertIn(
                        result.reason_codes,
                        (
                            ("POST_ACTIVATION_STABLE_LINEAGE_MISMATCH",),
                            ("POST_ACTIVATION_ADMISSION_REFUSED",),
                        ),
                    )
                    self.assertEqual(serving.calls, [])
                    self.assertEqual(len(rollback.calls), 1)
                    self.assertEqual(ledger.records(), ())

    def test_noncanonical_admission_receipt_refuses_before_activation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            evaluator = _AdmissionEvaluator(corrupt_first=True)
            runner, activation, _authority, _runtime, serving, rollback, _ledger = self._runner(
                Path(directory), evaluator=evaluator
            )
            result = runner.run()
        self.assertEqual(result.reason_codes, ("INITIAL_ADMISSION_REFUSED",))
        self.assertEqual(activation.calls, [])
        self.assertEqual(serving.calls, [])
        self.assertEqual(rollback.calls, [])

    def test_claim_refusal_and_slot_failure_each_contain_once_without_later_search(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "claim").mkdir()
            runner, _activation, authority, _runtime, serving, rollback, ledger = self._runner(root / "claim", wrong_claim=True)
            result = runner.run()
            self.assertEqual(result.reason_codes, ("ROUTE_CLAIM_REFUSED",))
            self.assertEqual(
                len(serving.calls),
                next(
                    step.execution_index
                    for step in self.schedule.steps
                    if step.kind.value == "ROUTING"
                ),
            )
            self.assertEqual(len(authority.calls), 1)
            self.assertEqual(len(rollback.calls), 1)
            self.assertEqual(ledger.progress().status, Stage4LedgerStatus.FAILED)
            (root / "serving").mkdir()
            runner, _activation, authority, _runtime, serving, rollback, ledger = self._runner(root / "serving", serving_failure_at_call=1)
            result = runner.run()
            self.assertEqual(result.reason_codes, ("MILVUS_SEARCH_FAILED",))
            self.assertEqual(len(serving.calls), 1)
            self.assertEqual(len(rollback.calls), 1)
            self.assertIs(rollback.calls[0].trigger, RollbackTrigger.SLOT_SAFETY_FAILURE)
            self.assertEqual(ledger.progress().status, Stage4LedgerStatus.FAILED)

    def test_clock_failure_and_activation_binding_mismatch_both_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "clock").mkdir()
            runner, _activation, _authority, _runtime, serving, rollback, _ledger = self._runner(root / "clock", clock=_Clock(fail_at_call=1))
            result = runner.run()
            self.assertIn("CLOCK", result.reason_codes[0])
            self.assertEqual(len(serving.calls), 0)
            self.assertEqual(len(rollback.calls), 1)
            (root / "binding").mkdir()
            runner, _activation, _authority, _runtime, serving, rollback, _ledger = self._runner(
                root / "binding", activation_mismatch=True
            )
            result = runner.run()
            self.assertEqual(result.reason_codes, ("ACTIVATION_CONTEXT_MISMATCH",))
            self.assertEqual(len(serving.calls), 0)
            self.assertEqual(len(rollback.calls), 1)

    def test_source_serving_timeout_threshold_and_probe_failures_stop_without_retry(self) -> None:
        cases = (
            ("source", {"source_failure": True}, "QUERY_SOURCE_FAILURE", 0),
            (
                "threshold",
                {"serving_failure_at_call": 1, "serving_failure_code": "MILVUS_THRESHOLD_SEMANTICS_INVALID"},
                "MILVUS_THRESHOLD_SEMANTICS_INVALID",
                1,
            ),
            (
                "timeout",
                {"serving_failure_at_call": 1, "serving_failure_code": "MILVUS_SEARCH_TIMEOUT", "serving_timed_out": True},
                "MILVUS_SEARCH_TIMEOUT",
                1,
            ),
            ("pre-health", {"unsafe_slot_call": 1}, "STACK_HEALTH_UNHEALTHY", 0),
            (
                "post-identity",
                {"unsafe_slot_call": 2, "unsafe_identity": True},
                "COLLECTION_IDENTITY_MISMATCH",
                1,
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, options, expected_reason, expected_serving_calls in cases:
                with self.subTest(name=name):
                    case_root = root / name
                    case_root.mkdir()
                    runner, _activation, _authority, _runtime, serving, rollback, ledger = self._runner(case_root, **options)
                    result = runner.run()
                    self.assertEqual(result.reason_codes, (expected_reason,))
                    self.assertEqual(len(serving.calls), expected_serving_calls)
                    self.assertEqual(len(rollback.calls), 1)
                    self.assertIs(rollback.calls[0].trigger, RollbackTrigger.SLOT_SAFETY_FAILURE)
                    self.assertEqual(ledger.progress().status, Stage4LedgerStatus.FAILED)

    def test_ledger_append_failure_contains_without_another_search(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            private.mkdir(mode=0o700)
            ledger = _FailingAppendLedger(
                private / "stage4.sqlite3",
                run_id="exp009-live-runner-test",
                schedule=self.schedule,
            )
            runner, _activation, _authority, _runtime, serving, rollback, actual_ledger = self._runner(root, ledger=ledger)
            result = runner.run()
            self.assertEqual(result.reason_codes, ("LEDGER_UNAVAILABLE",))
            self.assertEqual(len(serving.calls), 1)
            self.assertEqual(len(rollback.calls), 1)
            self.assertIs(rollback.calls[0].trigger, RollbackTrigger.SLOT_SAFETY_FAILURE)
            self.assertEqual(actual_ledger.records(), ())

    def test_invalid_ledger_or_rollback_port_result_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "ledger-private"
            private.mkdir(mode=0o700)
            ledger = _InvalidAppendLedger(
                private / "stage4.sqlite3",
                run_id="exp009-live-runner-test",
                schedule=self.schedule,
            )
            runner, _activation, _authority, _runtime, serving, rollback, actual_ledger = self._runner(
                root, ledger=ledger
            )
            result = runner.run()
            self.assertEqual(result.reason_codes, ("LEDGER_APPEND_INVALID",))
            self.assertEqual(len(serving.calls), 1)
            self.assertEqual(len(rollback.calls), 1)
            self.assertEqual(actual_ledger.records(), ())

            invalid_rollback = _InvalidRollback()
            post_root = root / "post"
            post_root.mkdir()
            runner, _activation, _authority, _runtime, serving, rollback, ledger = self._runner(
                post_root,
                preflight_complete=(True, False),
                rollback_port=invalid_rollback,
            )
            result = runner.run()
            self.assertEqual(result.reason_codes, ("POST_ACTIVATION_ADMISSION_REFUSED",))
            self.assertIsNotNone(result.rollback)
            self.assertEqual(result.rollback.reason_code, "ROLLBACK_RESULT_INVALID")
            self.assertEqual(len(invalid_rollback.calls), 1)

    def test_request_rejects_noncanonical_run_id_and_invalid_failure_timestamp(self) -> None:
        request = self._live_request()
        values = {
            field: getattr(request, field)
            for field in Stage4LiveRunRequest.__dataclass_fields__
        }
        self.assertNotIn("admission_request", values)
        self.assertNotIn("lkg_authority", values)
        with self.assertRaises(ValueError):
            Stage4LiveRunRequest(**(values | {"run_id": " exp009"}))
        with self.assertRaises(ValueError):
            Stage4LiveRunRequest(
                **(
                    values
                    | {
                        "activation_timestamps": ActivationTimestamps(
                            "2026-08-04T20:00:00Z",
                            "2026-08-04T20:00:01Z",
                            "2026-08-04T20:00:02Z",
                            "not-a-timestamp",
                        )
                    }
                )
            )

    def test_nonempty_ledger_never_resumes_an_activated_plan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = self._ledger(root)
            start = ledger.start_slot(0, started_monotonic_ns=1, recorded_at_utc="2026-08-04T20:00:00Z")
            assert start.accepted and start.start_sha256 is not None
            completed = ledger.complete_slot(
                Stage4SlotObservation(0, 400, 1, 2, "2026-08-04T20:00:00Z", True, False, True, True, True, True, True, 0, 0.001, None),
                started_record_sha256=start.start_sha256,
            )
            assert completed.accepted
            runner, activation, _authority, _runtime, serving, rollback, actual_ledger = self._runner(root, ledger=ledger)
            result = runner.run()
            self.assertEqual(result.reason_codes, ("LEDGER_NOT_FRESH",))
            self.assertEqual(len(activation.calls), 0)
            self.assertEqual(serving.calls, [])
            self.assertEqual(rollback.calls, [])
            self.assertEqual(actual_ledger.progress().record_count, 1)

    def test_ast_rejects_direct_database_or_approval_imports(self) -> None:
        source = Path("src/vdbench/canary_live_runner.py").read_text(encoding="utf-8")
        parsed = ast.parse(source)
        imported = set()
        for node in ast.walk(parsed):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
        forbidden = {
            "pymilvus",
            "vdbench.milvus",
            "vdbench.milvus_serving",
            "vdbench.canary_approval",
            "vdbench.lkg_phase3_persistence",
            "vdbench.lkg_qualification_evaluation_ledger",
        }
        self.assertTrue(forbidden.isdisjoint(imported), imported)

    def test_started_marker_is_committed_before_serving_execute_is_ever_called(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            private.mkdir(mode=0o700)
            order: list[str] = []
            ledger = _RecordingSlotOrderLedger(
                private / "stage4.sqlite3",
                run_id="exp009-live-runner-test",
                schedule=self.schedule,
                order=order,
            )
            provider = _FreshAuthorityProvider((self.lkg_pair, self.lkg_pair))
            evaluator = _AdmissionEvaluator()
            activation = _Activation(binding=self.binding, plan_sha256=self.plan.plan_sha256)
            authority = _Authority(self.plan)
            runtime = _RuntimeProbe(self.binding)
            serving = _Serving(order=order)
            rollback = _Rollback()
            request = self._live_request()
            runner = Stage4LiveRunner(
                request=request,
                activation=activation,
                authority=authority,
                runtime_probe=runtime,
                serving=serving,
                vector_source=Dataset002ScheduleVectorSource(_Source()),
                ledger=ledger,
                rollback=rollback,
                lkg_authority_provider=provider,
                admission_evaluator=evaluator,
                monotonic_ns=_Clock(),
                utc_now=lambda: "2026-08-04T20:00:04Z",
            )
            result = runner.run()
            self.assertEqual(result.dispatched_slot_count, 1200)
            self.assertEqual(len(order), 3600)
            triples = [order[i : i + 3] for i in range(0, len(order), 3)]
            for index, triple in enumerate(triples):
                self.assertEqual(
                    triple,
                    [
                        f"start_slot:{index}",
                        f"serving.execute:{index}",
                        f"complete_slot:{index}",
                    ],
                )

    def test_start_commit_failure_dispatches_zero_searches(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            private.mkdir(mode=0o700)
            ledger = _FailingStartLedger(
                private / "stage4.sqlite3",
                run_id="exp009-live-runner-test",
                schedule=self.schedule,
            )
            runner, _activation, _authority, _runtime, serving, rollback, _actual_ledger = self._runner(
                root, ledger=ledger
            )
            result = runner.run()
            self.assertEqual(result.reason_codes, ("LEDGER_START_UNAVAILABLE",))
            self.assertEqual(len(serving.calls), 0)
            self.assertEqual(len(rollback.calls), 1)
            self.assertIs(rollback.calls[0].trigger, RollbackTrigger.SLOT_SAFETY_FAILURE)

    def test_preexisting_orphan_started_blocks_a_fresh_runner_with_zero_dispatch(self) -> None:
        """Covers both crash timings (before AND after the search itself):
        both leave an identical durable orphan STARTED marker with no
        terminal record, which is indistinguishable to any later reader --
        so one scenario suffices to prove the restart contract for both."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seed_ledger = self._ledger(root)
            seed_start = seed_ledger.start_slot(
                0, started_monotonic_ns=1, recorded_at_utc="2026-08-04T20:00:00Z"
            )
            self.assertTrue(seed_start.accepted)
            del seed_ledger  # simulate the crash: no complete_slot ever called

            fresh_ledger = Stage4ExecutionLedger(
                root / "private" / "stage4.sqlite3",
                run_id="exp009-live-runner-test",
                schedule=self.schedule,
            )
            self.assertEqual(fresh_ledger.progress().status, Stage4LedgerStatus.AMBIGUOUS)
            runner, activation, authority, _runtime, serving, rollback, actual_ledger = self._runner(
                root, ledger=fresh_ledger
            )
            result = runner.run()
            self.assertEqual(result.reason_codes, ("LEDGER_NOT_FRESH",))
            self.assertEqual(len(activation.calls), 0)
            self.assertEqual(serving.calls, [])
            self.assertEqual(authority.calls, [])
            self.assertEqual(rollback.calls, [])
            self.assertEqual(actual_ledger.progress().status, Stage4LedgerStatus.AMBIGUOUS)

    def test_orphan_started_refusal_is_consistent_across_repeated_restart_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seed_ledger = self._ledger(root)
            seed_start = seed_ledger.start_slot(
                0, started_monotonic_ns=1, recorded_at_utc="2026-08-04T20:00:00Z"
            )
            self.assertTrue(seed_start.accepted)
            del seed_ledger

            for attempt in range(3):
                with self.subTest(attempt=attempt):
                    attempt_ledger = Stage4ExecutionLedger(
                        root / "private" / "stage4.sqlite3",
                        run_id="exp009-live-runner-test",
                        schedule=self.schedule,
                    )
                    runner, _activation, _authority, _runtime, serving, _rollback, _actual_ledger = self._runner(
                        root, ledger=attempt_ledger
                    )
                    result = runner.run()
                    self.assertEqual(result.reason_codes, ("LEDGER_NOT_FRESH",))
                    self.assertEqual(serving.calls, [])

            final_ledger = Stage4ExecutionLedger(
                root / "private" / "stage4.sqlite3",
                run_id="exp009-live-runner-test",
                schedule=self.schedule,
            )
            self.assertEqual(final_ledger.progress().status, Stage4LedgerStatus.AMBIGUOUS)
            self.assertEqual(len(final_ledger.records()), 0)


if __name__ == "__main__":
    unittest.main()
