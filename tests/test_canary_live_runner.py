"""Fake-port verification for the human-gated EXP-009 Stage-4 live root.

These tests deliberately exercise no network or database client.  They prove
that the composition root neither selects routes itself nor leaves candidate
routing active after a failed or completed serial schedule.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import tempfile
import unittest

from vdbench.artifacts import canonical_json_bytes
from vdbench.canary_activation import (
    ActivationAttempt,
    ActivationTimestamps,
    ActiveCanaryContext,
)
from vdbench.canary_admission import (
    Stage4AdmissionRequest,
    Stage4AdmissionResult,
    Stage4RepositoryEvidence,
    Stage4RuntimeReadiness,
)
from vdbench.canary_execution_ledger import (
    Stage4ExecutionLedger,
    Stage4LedgerError,
    Stage4LedgerStatus,
    Stage4SlotObservation,
)
from vdbench.canary_live_runner import (
    Stage4LiveRunRequest,
    Stage4LiveRunner,
    Stage4SlotSafety,
)
from vdbench.canary_route_authority import RouteClaim
from vdbench.canary_route_state import RouteStateBinding
from vdbench.canary_routing import build_canary_route_plan
from vdbench.canary_schedule import build_stage4_execution_schedule
from vdbench.canary_serial_runner import Dataset002ScheduleVectorSource
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    CandidateSelectionRecord,
    EligibleOccurrence,
    EligibleWorkloadManifest,
    SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING,
    SCHEDULE_CONTROL_COUNT,
    SCHEDULE_EXECUTION_MODE,
    SCHEDULE_INTERLEAVED_SWEEP_COUNT,
    SCHEDULE_MEDIAN_RELATIVE_CEILING,
    SCHEDULE_POST_SWEEP_COUNT,
    SCHEDULE_PRE_SWEEP_COUNT,
    SCHEDULE_P95_RELATIVE_CEILING,
    SCHEDULE_ROUTING_BLOCK_SIZE,
    SCHEDULE_STABILITY_SCHEMA_VERSION,
    ScheduleControl,
    ScheduleStabilityContract,
    WorkloadIdentityBinding,
)
from vdbench.config import Metric, RESULT_LIMIT
from vdbench.host_observation import ServedQueryOutcome
from vdbench.policy import (
    PolicyAction,
    PolicyDecision,
    PolicyMode,
    QualificationResult,
    SafetyGateResult,
)
from vdbench.canary_rollback import RollbackResult, RollbackTrigger


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
                False if self._unsafe_identity else True,
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
    ) -> None:
        self.calls = []
        self._failure_at_call = failure_at_call
        self._failure_code = failure_code
        self._timed_out = timed_out

    def execute(self, request):
        self.calls.append(request)
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


class _AdmissionEvaluator:
    def __init__(self, plan_sha256: str) -> None:
        self._plan_sha256 = plan_sha256
        self.calls = []

    def __call__(self, request: object) -> Stage4AdmissionResult:
        self.calls.append(request)
        complete = getattr(getattr(request, "runtime", None), "serving_preflight_complete", False)
        return Stage4AdmissionResult(
            admitted=complete is True,
            reason_codes=() if complete is True else ("RUNTIME_PREFLIGHT_INCOMPLETE",),
            plan_sha256=self._plan_sha256,
            policy_audit_id="policy-audit-live-runner-001",
            repository_commit_sha="a" * 40,
        )


class _FailingAppendLedger(Stage4ExecutionLedger):
    """Test-only durable ledger that refuses its first post-search append."""

    def append(self, observation: object):
        del observation
        raise Stage4LedgerError("simulated ledger unavailable")


class _InvalidAppendLedger(Stage4ExecutionLedger):
    """Test-only port violation: a ledger append has no usable receipt."""

    def append(self, observation: object):
        del observation
        return object()


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

    def _admission_request(self) -> Stage4AdmissionRequest:
        return Stage4AdmissionRequest(
            manifest=self.manifest,
            selection=self.selection,
            plan=self.plan,
            policy_decision=PolicyDecision(
                PolicyAction.START_CANARY, 400, 800, 400, 0.99, 0.98, 4.0, 5.0,
                0.02, None, "QUALITY_DRIFT_RECOVERY", 0.999, 2.0,
                (SafetyGateResult("PRE_ACTION", True, "passed"),),
                PolicyMode.CANARY_ENABLED, "policy-audit-live-runner-001",
            ),
            qualification=QualificationResult(True, 400, ()),
            repository=Stage4RepositoryEvidence("a" * 40, True, "2026-08-04T20:00:00Z"),
            runtime=Stage4RuntimeReadiness(self.binding, True, "2026-08-04T20:00:00Z"),
        )

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
        evaluator = _AdmissionEvaluator(self.plan.plan_sha256)
        request = Stage4LiveRunRequest(
            admission_request=self._admission_request(),
            schedule=self.schedule,
            grant=object(), trust_store=object(), approval_context=object(),
            activation_timestamps=ActivationTimestamps(
                "2026-08-04T20:00:00Z", "2026-08-04T20:00:01Z", "2026-08-04T20:00:02Z", "2026-08-04T20:00:03Z"
            ),
            run_id="exp009-live-runner-test",
        )
        runner = Stage4LiveRunner(
            request=request, activation=activation, authority=authority, runtime_probe=runtime,
            serving=serving, vector_source=Dataset002ScheduleVectorSource(
                _Source(fail_on_first_call=source_failure)
            ),
            ledger=actual_ledger, rollback=rollback, admission_evaluator=evaluator,
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
            runner, activation, authority, runtime, serving, rollback, ledger = self._runner(Path(directory))
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

    def test_claim_refusal_and_slot_failure_each_contain_once_without_later_search(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "claim").mkdir()
            runner, activation, authority, runtime, serving, rollback, ledger = self._runner(root / "claim", wrong_claim=True)
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
            runner, activation, authority, runtime, serving, rollback, ledger = self._runner(root / "serving", serving_failure_at_call=1)
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
            runner, activation, authority, runtime, serving, rollback, ledger = self._runner(root / "clock", clock=_Clock(fail_at_call=1))
            result = runner.run()
            self.assertIn("CLOCK", result.reason_codes[0])
            self.assertEqual(len(serving.calls), 0)
            self.assertEqual(len(rollback.calls), 1)
            (root / "binding").mkdir()
            runner, activation, authority, runtime, serving, rollback, ledger = self._runner(
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
                    runner, activation, authority, runtime, serving, rollback, ledger = self._runner(case_root, **options)
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
            runner, activation, authority, runtime, serving, rollback, actual_ledger = self._runner(root, ledger=ledger)
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
            runner, activation, authority, runtime, serving, rollback, actual_ledger = self._runner(
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
            runner, activation, authority, runtime, serving, rollback, ledger = self._runner(
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
        values = {
            "admission_request": self._admission_request(),
            "schedule": self.schedule,
            "grant": object(),
            "trust_store": object(),
            "approval_context": object(),
            "activation_timestamps": ActivationTimestamps(
                "2026-08-04T20:00:00Z",
                "2026-08-04T20:00:01Z",
                "2026-08-04T20:00:02Z",
                "2026-08-04T20:00:03Z",
            ),
            "run_id": "exp009-live-runner-test",
        }
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
            ledger.append(Stage4SlotObservation(0, 400, 1, 2, "2026-08-04T20:00:00Z", True, False, True, True, True, True, True, 0, 0.001, None))
            runner, activation, authority, runtime, serving, rollback, actual_ledger = self._runner(root, ledger=ledger)
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
        forbidden = {"pymilvus", "vdbench.milvus", "vdbench.milvus_serving", "vdbench.canary_approval"}
        self.assertTrue(forbidden.isdisjoint(imported), imported)


if __name__ == "__main__":
    unittest.main()
