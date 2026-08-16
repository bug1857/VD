"""TDD coverage for the offline-only EXP-009 Stage-4 serial composition seam."""

from __future__ import annotations

import ast
import hashlib
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from tests.test_canary_admission import _phase3_authority
from vdbench.artifacts import canonical_json_bytes
from vdbench.canary_admission import (
    Stage4AdmissionReceipt,
    Stage4AdmissionRequest,
    Stage4RepositoryEvidence,
    bind_stage4_lkg_authority,
    evaluate_stage4_admission,
)
from vdbench.canary_execution_ledger import Stage4ExecutionLedger, Stage4LedgerStatus
from vdbench.canary_route_state import RouteStateBinding
from vdbench.canary_routing import build_canary_route_plan
from vdbench.canary_runtime_types import Stage4RuntimeReadiness
from vdbench.canary_schedule import build_stage4_execution_schedule
from vdbench.canary_serial_runner import (
    Dataset002ScheduleVectorSource,
    Stage4SerialRunner,
    Stage4SlotExecutorOutcome,
)
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
from vdbench.lkg_phase3_persistence import LkgPhase3AuthorityReferenceStore
from vdbench.policy import (
    PolicyAction,
    PolicyDecision,
    PolicyMode,
    SafetyGateResult,
)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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


class _Clock:
    def __init__(self, initial: int = 0) -> None:
        self._value = initial

    def __call__(self) -> int:
        self._value += 1_000_000
        return self._value


class _FakeDataset002Source:
    def __init__(self, *, fail_query_id: int | None = None) -> None:
        self.calls: list[tuple[str, int]] = []
        self._fail_query_id = fail_query_id

    def control_vector(self, *, control: ScheduleControl) -> tuple[float, ...]:
        self.calls.append(("control", control.query_id))
        if control.query_id == self._fail_query_id:
            raise RuntimeError("source failure")
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
        if dataset_query_id == self._fail_query_id:
            raise RuntimeError("source failure")
        return (float(dataset_query_id), 1.0)


class _FakeExecutor:
    def __init__(
        self,
        *,
        raise_at_index: int | None = None,
        unsafe_at_index: int | None = None,
        invalid_at_index: int | None = None,
    ) -> None:
        self.calls: list[tuple[int, int, tuple[float, ...]]] = []
        self._raise_at_index = raise_at_index
        self._unsafe_at_index = unsafe_at_index
        self._invalid_at_index = invalid_at_index

    def execute(self, *, step, query_vector: tuple[float, ...]) -> Stage4SlotExecutorOutcome:
        self.calls.append((step.execution_index, step.expected_ef, query_vector))
        if step.execution_index == self._raise_at_index:
            raise RuntimeError("executor failure")
        if step.execution_index == self._invalid_at_index:
            return object()  # type: ignore[return-value]
        if step.execution_index == self._unsafe_at_index:
            return Stage4SlotExecutorOutcome(
                success=True,
                timed_out=False,
                threshold_semantics_valid=True,
                health_before_ok=False,
                health_after_ok=True,
                identity_before_ok=True,
                identity_after_ok=True,
                result_count=0,
                reason_code="STACK_HEALTH_UNHEALTHY",
            )
        return Stage4SlotExecutorOutcome(
            success=True,
            timed_out=False,
            threshold_semantics_valid=True,
            health_before_ok=True,
            health_after_ok=True,
            identity_before_ok=True,
            identity_after_ok=True,
            result_count=0,
            reason_code=None,
        )


class CanarySerialRunnerTests(unittest.TestCase):
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
        manifest = EligibleWorkloadManifest(
            schema_version="exp009-eligible-workload-manifest-v2",
            created_at_utc="2026-08-04T18:00:00Z",
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
                EligibleOccurrence(
                    index,
                    f"exp009-routing-{index:06d}",
                    index,
                    _sha(f"route-{index}"),
                    0.75,
                    0.0,
                    RESULT_LIMIT,
                )
                for index in range(600)
            ),
        )
        manifest.validate()
        selection = CandidateSelectionRecord(
            schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
            selected_at_utc="2026-08-04T18:01:00Z",
            eligible_manifest_sha256=hashlib.sha256(
                canonical_json_bytes(manifest.to_document())
            ).hexdigest(),
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
        cls.schedule = build_stage4_execution_schedule(manifest, cls.plan)
        cls.manifest = manifest
        cls.selection = selection
        cls.binding = RouteStateBinding(
            Metric.L2, "target-075", 400, "config", "data", "flat", "hnsw"
        )
        cls._phase3_temporary = tempfile.TemporaryDirectory()
        authority = _phase3_authority(
            plan=cls.plan,
            radius=cls.manifest.radius,
            identifier=301,
        )
        with LkgPhase3AuthorityReferenceStore(
            Path(cls._phase3_temporary.name) / "phase3.db"
        ) as store:
            store.append(
                authority, persisted_at_utc="2026-08-08T15:00:00.000000Z"
            )
            latest = store.load_verified_latest()
        assert latest is not None
        pair = bind_stage4_lkg_authority(
            authority=authority, verified_latest_reference=latest
        )
        provenance = build_evidence_provenance(
            metric=cls.plan.metric,
            threshold_stratum=cls.plan.threshold_stratum,
            reference_window_id="reference-window-serial-001",
            current_window_id="current-window-serial-001",
            reference_manifest_sha256=_sha("reference-manifest"),
            current_manifest_sha256=_sha("current-manifest"),
            configuration_identity=cls.plan.configuration_identity,
            data_identity=cls.plan.data_identity,
            flat_binding_id=cls.plan.flat_binding_id,
            hnsw_binding_id=cls.plan.hnsw_binding_id,
            reference_audit_ids=tuple(f"reference-{index:02d}" for index in range(50)),
            reference_audit_rank_digests=tuple(_sha(f"rr-{index}") for index in range(50)),
            current_audit_ids=tuple(f"current-{index:02d}" for index in range(50)),
            current_audit_rank_digests=tuple(_sha(f"cr-{index}") for index in range(50)),
        )
        policy = PolicyDecision(
            PolicyAction.START_CANARY, 400, 800, 400, 0.99, 0.98, 4.0, 5.0,
            0.02, None, "QUALITY_DRIFT_RECOVERY", 0.999, 2.0,
            (SafetyGateResult("PRE_ACTION", True, "passed"),),
            PolicyMode.CANARY_ENABLED,
            "policy-audit-serial-001",
            evidence_provenance=provenance,
        )
        evidence_binding = Stage4EvidenceBinding(
            run_id="exp009-serial-runner-test",
            source_revision="a" * 40,
            metric=cls.plan.metric,
            threshold_stratum=cls.plan.threshold_stratum,
            current_ef=cls.plan.last_known_good_ef,
            candidate_ef=cls.plan.candidate_ef,
            last_known_good_ef=cls.plan.last_known_good_ef,
            candidate_search_configuration=replace(
                authority.search_configuration, ef=cls.plan.candidate_ef
            ),
            identity=cls.manifest.identity,
            dataset002_manifest_sha256=cls.manifest.dataset002_manifest_sha256,
            frozen_recall_audit_ids_sha256=_sha("frozen-recall-audit"),
            eligible_workload_sha256=cls.plan.eligible_workload_sha256,
            candidate_selection_sha256=cls.plan.candidate_selection_sha256,
            execution_schedule_sha256=cls.schedule.schedule_sha256,
            recall_evidence_schema_version="recall-audit-hoeffding-1200-v1",
            latency_evidence_schema_version="stage4-schedule-evaluation-v1",
        )
        result = evaluate_stage4_admission(
            Stage4AdmissionRequest(
                manifest=cls.manifest,
                selection=cls.selection,
                plan=cls.plan,
                schedule=cls.schedule,
                policy_decision=policy,
                lkg_authority=pair,
                evidence_binding=evidence_binding,
                repository=Stage4RepositoryEvidence(
                    "a" * 40, True, "2026-08-04T18:01:30Z"
                ),
                runtime=Stage4RuntimeReadiness(
                    cls.binding, True, "2026-08-04T18:01:45Z"
                ),
            )
        )
        if result.receipt is None:
            raise AssertionError(f"serial admission fixture failed: {result.reason_codes}")
        cls.receipt = result.receipt

    @classmethod
    def tearDownClass(cls) -> None:
        cls._phase3_temporary.cleanup()

    def _ledger(self, root: Path) -> Stage4ExecutionLedger:
        private = root / "private"
        private.mkdir(mode=0o700)
        return Stage4ExecutionLedger(
            private / "stage4.sqlite3",
            run_id="exp009-serial-runner-test",
            schedule=self.schedule,
        )

    def _runner(
        self,
        ledger: Stage4ExecutionLedger,
        *,
        receipt: Stage4AdmissionReceipt | None = None,
        schedule=None,
        source: _FakeDataset002Source | None = None,
        executor: _FakeExecutor | None = None,
    ) -> tuple[Stage4SerialRunner, _FakeDataset002Source, _FakeExecutor]:
        fake_source = source or _FakeDataset002Source()
        fake_executor = executor or _FakeExecutor()
        actual_schedule = self.schedule if schedule is None else schedule
        return (
            Stage4SerialRunner(
                admission_receipt=self.receipt if receipt is None else receipt,
                schedule=actual_schedule,
                vector_source=Dataset002ScheduleVectorSource(fake_source),
                executor=fake_executor,
                ledger=ledger,
                monotonic_ns=_Clock(ledger.progress().record_count * 2_000_000),
                utc_now=lambda: "2026-08-04T18:02:00Z",
            ),
            fake_source,
            fake_executor,
        )

    def test_composes_all_1200_slots_and_evaluates_only_complete_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ledger = self._ledger(Path(temporary))
            runner, source, executor = self._runner(ledger)
            result = runner.run()

        self.assertEqual(result.ledger_progress.status, Stage4LedgerStatus.COMPLETE)
        self.assertEqual(result.dispatched_slot_count, 1200)
        self.assertEqual(len(source.calls), 1200)
        self.assertEqual(len(executor.calls), 1200)
        self.assertEqual(sum(kind == "control" for kind, _ in source.calls), 600)
        self.assertEqual(sum(kind == "routing" for kind, _ in source.calls), 600)
        self.assertEqual(sum(ef == 800 for _, ef, _ in executor.calls), 60)
        self.assertTrue(result.evaluation.finite_manifest_latency_applicable)
        self.assertFalse(result.evaluation.recall_bound_evaluated)

    def test_refused_or_mismatched_admission_dispatches_nothing(self) -> None:
        noncanonical = _forge_canonical_receipt(self.receipt)
        object.__setattr__(noncanonical, "canonical_receipt_digest", "0" * 64)
        cases = (
            noncanonical,
            _forge_canonical_receipt(self.receipt, route_plan_sha256="0" * 64),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for index, receipt in enumerate(cases):
                with self.subTest(index=index):
                    case_root = root / str(index)
                    case_root.mkdir()
                    ledger = self._ledger(case_root)
                    runner, source, executor = self._runner(ledger, receipt=receipt)
                    result = runner.run()
                    self.assertEqual(result.dispatched_slot_count, 0)
                    self.assertEqual(source.calls, [])
                    self.assertEqual(executor.calls, [])
                    self.assertEqual(ledger.records(), ())

    def test_source_or_executor_failure_persists_one_terminal_slot(self) -> None:
        cases = (
            (_FakeDataset002Source(fail_query_id=600), _FakeExecutor(), "QUERY_SOURCE_FAILURE", 0),
            (_FakeDataset002Source(), _FakeExecutor(raise_at_index=0), "EXECUTOR_EXCEPTION", 1),
            (
                _FakeDataset002Source(),
                _FakeExecutor(invalid_at_index=0),
                "EXECUTOR_OUTCOME_INVALID",
                1,
            ),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, (source, executor, reason, executor_calls) in enumerate(cases):
                with self.subTest(reason=reason):
                    case_root = root / str(name)
                    case_root.mkdir()
                    ledger = self._ledger(case_root)
                    runner, returned_source, returned_executor = self._runner(
                        ledger, source=source, executor=executor
                    )
                    result = runner.run()
                    self.assertEqual(result.ledger_progress.status, Stage4LedgerStatus.FAILED)
                    self.assertEqual(result.ledger_progress.reason_code, reason)
                    self.assertEqual(len(ledger.records()), 1)
                    self.assertEqual(len(returned_source.calls), 1)
                    self.assertEqual(len(returned_executor.calls), executor_calls)

    def test_ledger_schedule_substitution_dispatches_nothing(self) -> None:
        alternate_selection = replace(
            self.selection,
            candidate_occurrence_ids=tuple(
                item.occurrence_id
                for item in self.manifest.occurrences
                if item.sequence_index % 10 == 1
            ),
        )
        alternate_schedule = build_stage4_execution_schedule(
            self.manifest, build_canary_route_plan(self.manifest, alternate_selection)
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            private = root / "private"
            private.mkdir(mode=0o700)
            ledger = Stage4ExecutionLedger(
                private / "stage4.sqlite3",
                run_id="exp009-serial-runner-test",
                schedule=alternate_schedule,
            )
            runner, source, executor = self._runner(ledger)
            result = runner.run()
            record_count = len(ledger.records())

        self.assertEqual(result.reason_codes, ("LEDGER_SCHEDULE_MISMATCH",))
        self.assertEqual(source.calls, [])
        self.assertEqual(executor.calls, [])
        self.assertEqual(record_count, 0)

    def test_receipt_schedule_substitution_refuses_before_dispatch(self) -> None:
        alternate_selection = replace(
            self.selection,
            candidate_occurrence_ids=tuple(
                item.occurrence_id
                for item in self.manifest.occurrences
                if item.sequence_index % 10 == 1
            ),
        )
        alternate_schedule = build_stage4_execution_schedule(
            self.manifest, build_canary_route_plan(self.manifest, alternate_selection)
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            private = root / "private"
            private.mkdir(mode=0o700)
            ledger = Stage4ExecutionLedger(
                private / "stage4.sqlite3",
                run_id="exp009-serial-runner-test",
                schedule=alternate_schedule,
            )
            runner, source, executor = self._runner(
                ledger, schedule=alternate_schedule
            )
            result = runner.run()

        self.assertEqual(result.reason_codes, ("ADMISSION_SCHEDULE_BINDING_MISMATCH",))
        self.assertEqual(result.dispatched_slot_count, 0)
        self.assertEqual(source.calls, [])
        self.assertEqual(executor.calls, [])

    def test_unsafe_outcome_is_persisted_and_stops_without_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ledger = self._ledger(Path(temporary))
            runner, source, executor = self._runner(ledger, executor=_FakeExecutor(unsafe_at_index=0))
            result = runner.run()
            record_count = len(ledger.records())

        self.assertEqual(result.ledger_progress.status, Stage4LedgerStatus.FAILED)
        self.assertEqual(result.ledger_progress.reason_code, "STACK_HEALTH_UNHEALTHY")
        self.assertEqual(len(source.calls), 1)
        self.assertEqual(len(executor.calls), 1)
        self.assertEqual(record_count, 1)

    def test_restart_resumes_exact_next_slot_without_redelivery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = self._ledger(root)
            first_runner, _, first_executor = self._runner(ledger)
            first = first_runner.run(max_slots=2)
            restarted = Stage4ExecutionLedger(
                root / "private" / "stage4.sqlite3",
                run_id="exp009-serial-runner-test",
                schedule=self.schedule,
            )
            second_runner, _, second_executor = self._runner(restarted)
            second = second_runner.run(max_slots=2)

        self.assertEqual(first.ledger_progress.record_count, 2)
        self.assertEqual(second.ledger_progress.record_count, 4)
        self.assertEqual([call[0] for call in first_executor.calls], [0, 1])
        self.assertEqual([call[0] for call in second_executor.calls], [2, 3])

    def test_runner_is_offline_only_and_has_no_grant_or_route_authority_import(self) -> None:
        source = Path("src/vdbench/canary_serial_runner.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        modules = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        forbidden = {
            "milvus",
            "milvus_serving",
            "pymilvus",
            "canary_activation",
            "canary_approval",
            "canary_route_authority",
            "canary_routing",
            "lkg_phase3_authority",
            "lkg_phase3_persistence",
            "lkg_qualification_evaluation_ledger",
            "lkg_qualification_ledger",
            "lkg_phase2_readiness_ledger",
        }
        self.assertFalse(any(name.split(".")[-1] in forbidden for name in modules))


if __name__ == "__main__":
    unittest.main()
