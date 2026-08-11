"""Tests for the human-facing, read-only Stage-4 qualification report/CLI.

Two distinct, explicitly-named report kinds:

- ``recall-only``: emits a ``Stage4RecallAuditReport``. May exit 0 when the
  1,200-query recall audit passes, with no latency evidence at all. Its
  ``report_kind`` field can never be mistaken for a full qualification.
- ``full-qualification``: emits a ``Stage4Decision``. Requires latency
  evidence bound to the identical ADR-008 ``Stage4EvidenceBinding`` as the
  recall evidence; if not supplied, the decision is INCOMPLETE and the CLI
  exits non-zero -- it never silently reports PASSING on recall alone.

Every CLI-supplied artifact (``--context-json``, ``--frozen-query-ids-json``,
``--binding-json``, ``--schedule-json``) is verified against an
independently-supplied expected digest before being trusted -- internal
self-consistency with a ledger is never sufficient on its own. There is no
free-form latency-evaluation input: ADR-008's Stage-4 evidence-binding
repair explicitly prohibits one.

This module is a read-only evidence reporter. It never applies, promotes,
routes, or mutates Milvus state.
"""

from __future__ import annotations

import contextlib
from dataclasses import replace
import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from vdbench.artifacts import canonical_json_bytes
from vdbench.canary_execution_ledger import Stage4ExecutionLedger, Stage4SlotObservation
from vdbench.canary_recall_audit_evaluation import RECALL_AUDIT_EVALUATOR_VERSION
from vdbench.canary_recall_audit_ledger import CanaryRecallAuditLedger, RecallAuditObservation
from vdbench.canary_routing import build_canary_route_plan
from vdbench.canary_schedule import build_stage4_execution_schedule
from vdbench.canary_stage4_decision import Stage4DecisionStatus
from vdbench.canary_stage4_evidence_binding import Stage4EvidenceBinding
from vdbench.canary_stage4_qualification_report import HUMAN_AUTHORIZATION_NOTICE, main
from vdbench.canary_statistics import EXP009_RECALL_AUDIT_COUNT
from vdbench.canary_workload import (
    CANDIDATE_SELECTION_SCHEMA_VERSION,
    CandidateSelectionRecord,
    EligibleOccurrence,
    EligibleWorkloadManifest,
    SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING,
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
from vdbench.config import IndexTrack, Metric, SearchConfiguration
from vdbench.dataset002 import DATASET002_SCHEMA_VERSION

import vdbench.canary_stage4_qualification_report as report_module


def _sha(value: str | bytes) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(data).hexdigest()


# The binding's ``frozen_recall_audit_ids_sha256`` must be computed over the
# exact same bytes as the ``--frozen-query-ids-json`` file main() digests --
# internal consistency between the two is what the binding-mismatch gate
# checks, so both must derive from this single shared byte string.
_FROZEN_QUERY_IDS_BYTES = json.dumps(list(range(EXP009_RECALL_AUDIT_COUNT))).encode("utf-8")
_FROZEN_QUERY_IDS_SHA256 = hashlib.sha256(_FROZEN_QUERY_IDS_BYTES).hexdigest()


_CONTEXT = dict(
    search_configuration=SearchConfiguration(
        metric=Metric.L2,
        threshold_label="target-075",
        radius=0.6,
        index_track=IndexTrack.HNSW,
        ef=800,
        limit=100,
        consistency_level="Strong",
    ),
    identity=WorkloadIdentityBinding(
        configuration_identity="a" * 16,
        data_identity="DATASET-001-v1:sha256:" + "b" * 64,
        flat_binding_id="c" * 16,
        hnsw_binding_id="d" * 16,
    ),
    dataset002_manifest_sha256="e" * 64,
    dataset002_schema_version=DATASET002_SCHEMA_VERSION,
)


def _build_schedule():
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
        created_at_utc="2026-08-04T15:00:00Z",
        dataset002_manifest_sha256=_sha("dataset002"),
        dataset001_generation_manifest_sha256=_sha("dataset001"),
        metric=_CONTEXT["search_configuration"].metric,
        threshold_stratum=_CONTEXT["search_configuration"].threshold_label,
        candidate_ef=800,
        last_known_good_ef=400,
        radius=_CONTEXT["search_configuration"].radius,
        range_filter=0.0,
        limit=_CONTEXT["search_configuration"].limit,
        identity=_CONTEXT["identity"],
        vector_mapping="one_to_one_unique_dataset002_routing_vectors",
        schedule_stability=stability,
        occurrences=tuple(
            EligibleOccurrence(
                index,
                f"exp009-routing-{index:06d}",
                index,
                _sha(f"route-{index}"),
                _CONTEXT["search_configuration"].radius,
                0.0,
                _CONTEXT["search_configuration"].limit,
            )
            for index in range(600)
        ),
    )
    manifest.validate()
    selection = CandidateSelectionRecord(
        schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
        selected_at_utc="2026-08-04T15:01:00Z",
        eligible_manifest_sha256=_sha("manifest-binding"),
        population_count=600,
        candidate_count=60,
        candidate_fraction=0.10,
        candidate_occurrence_ids=tuple(
            item.occurrence_id for item in manifest.occurrences if item.sequence_index % 10 == 0
        ),
        random_source="python.secrets.SystemRandom.sample",
        selected_before_candidate_results=True,
    )
    selection = replace(
        selection,
        eligible_manifest_sha256=hashlib.sha256(canonical_json_bytes(manifest.to_document())).hexdigest(),
    )
    return build_stage4_execution_schedule(manifest, build_canary_route_plan(manifest, selection))


def _build_binding(schedule) -> Stage4EvidenceBinding:
    return Stage4EvidenceBinding(
        run_id="exp009-stage4-report-test",
        source_revision="0" * 40,
        metric=schedule.metric,
        threshold_stratum=schedule.threshold_stratum,
        current_ef=schedule.last_known_good_ef,
        candidate_ef=schedule.candidate_ef,
        last_known_good_ef=schedule.last_known_good_ef,
        candidate_search_configuration=_CONTEXT["search_configuration"],
        identity=_CONTEXT["identity"],
        dataset002_manifest_sha256=_CONTEXT["dataset002_manifest_sha256"],
        frozen_recall_audit_ids_sha256=_FROZEN_QUERY_IDS_SHA256,
        eligible_workload_sha256=_sha("eligible-workload"),
        candidate_selection_sha256=_sha("candidate-selection"),
        execution_schedule_sha256=schedule.schedule_sha256,
        recall_evidence_schema_version=RECALL_AUDIT_EVALUATOR_VERSION,
        latency_evidence_schema_version="exp009-stage4-execution-schedule-v1",
    )


def _schedule_document(schedule) -> dict:
    return {
        "schema_version": schedule.schema_version,
        "plan_sha256": schedule.plan_sha256,
        "metric": schedule.metric.value,
        "threshold_stratum": schedule.threshold_stratum,
        "candidate_ef": schedule.candidate_ef,
        "last_known_good_ef": schedule.last_known_good_ef,
        "control_ef": schedule.control_ef,
        "steps": [
            {
                "execution_index": step.execution_index,
                "kind": step.kind.value,
                "expected_ef": step.expected_ef,
                "sweep_index": step.sweep_index,
                "control_query_id": step.control_query_id,
                "control_vector_sha256": step.control_vector_sha256,
                "routing_sequence_index": step.routing_sequence_index,
                "occurrence_id": step.occurrence_id,
                "dataset_query_id": step.dataset_query_id,
                "vector_sha256": step.vector_sha256,
                "threshold_radius": step.threshold_radius,
                "range_filter": step.range_filter,
                "limit": step.limit,
                "route_kind": None if step.route_kind is None else step.route_kind.value,
            }
            for step in schedule.steps
        ],
        "schedule_sha256": schedule.schedule_sha256,
    }


def _populate_ledger(
    path: Path, *, run_id: str, binding_sha256: str, all_perfect: bool, count: int = EXP009_RECALL_AUDIT_COUNT
) -> None:
    """``count`` defaults to the full 1,200-observation contract population.
    Tests that only need SOME evidence present (never a PASSING/FAILING
    verdict -- e.g. proving a missing/partial *latency* argument fails
    closed regardless of the recall side) may pass a small ``count`` to
    avoid the expensive full hash-chain-verified population; an undercount
    already makes the recall side INCOMPLETE on its own, which is a strict
    superset of what those tests assert."""

    ledger = CanaryRecallAuditLedger(path, run_id=run_id, binding_sha256=binding_sha256)
    for query_id in range(count):
        base = query_id * 100
        oracle_ids = tuple(range(base, base + 100))
        candidate_ids = oracle_ids if all_perfect else oracle_ids[:1]
        ledger.append(
            RecallAuditObservation(
                query_id=query_id,
                oracle_result_ids=oracle_ids,
                candidate_result_ids=candidate_ids,
                producer_run_id=run_id,
                recorded_at_utc="2026-08-04T00:00:00Z",
                **_CONTEXT,
            )
        )


def _populate_latency_ledger(path: Path, *, run_id: str, schedule) -> Stage4ExecutionLedger:
    ledger = Stage4ExecutionLedger(path, run_id=run_id, schedule=schedule)
    for step in schedule.steps:
        latency = 1.0 if step.control_query_id is not None else 2.0
        start_result = ledger.start_slot(
            step.execution_index,
            started_monotonic_ns=step.execution_index * 10,
            recorded_at_utc="2026-08-04T15:01:00Z",
        )
        assert start_result.accepted and start_result.start_sha256 is not None
        completed = ledger.complete_slot(
            Stage4SlotObservation(
                execution_index=step.execution_index,
                observed_ef=step.expected_ef,
                started_monotonic_ns=step.execution_index * 10,
                finished_monotonic_ns=step.execution_index * 10 + 5,
                recorded_at_utc="2026-08-04T15:02:00Z",
                success=True,
                timed_out=False,
                threshold_semantics_valid=True,
                health_before_ok=True,
                health_after_ok=True,
                identity_before_ok=True,
                identity_after_ok=True,
                result_count=0,
                latency_ms=latency,
                reason_code=None,
            ),
            started_record_sha256=start_result.start_sha256,
        )
        assert completed.accepted
    return ledger


def _context_document() -> dict:
    sc = _CONTEXT["search_configuration"]
    identity = _CONTEXT["identity"]
    return {
        "search_configuration": {
            "metric": sc.metric.value,
            "threshold_label": sc.threshold_label,
            "radius": sc.radius,
            "index_track": sc.index_track.value,
            "ef": sc.ef,
            "limit": sc.limit,
            "consistency_level": sc.consistency_level,
        },
        "identity": {
            "configuration_identity": identity.configuration_identity,
            "data_identity": identity.data_identity,
            "flat_binding_id": identity.flat_binding_id,
            "hnsw_binding_id": identity.hnsw_binding_id,
        },
        "dataset002_manifest_sha256": _CONTEXT["dataset002_manifest_sha256"],
        "dataset002_schema_version": _CONTEXT["dataset002_schema_version"],
    }


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class QualificationReportCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schedule = _build_schedule()

    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)
        self.ledger_path = Path(self._tempdir.name) / "recall_audit.sqlite3"

        self.context_path = Path(self._tempdir.name) / "context.json"
        context_bytes = json.dumps(_context_document()).encode("utf-8")
        self.context_path.write_bytes(context_bytes)
        self.expected_context_sha256 = _sha256_bytes(context_bytes)

        self.frozen_ids_path = Path(self._tempdir.name) / "frozen_query_ids.json"
        self.frozen_ids_path.write_bytes(_FROZEN_QUERY_IDS_BYTES)
        self.expected_frozen_ids_sha256 = _FROZEN_QUERY_IDS_SHA256

        self.binding = _build_binding(self.schedule)
        self.binding_path = Path(self._tempdir.name) / "binding.json"
        binding_bytes = json.dumps(self.binding.to_document()).encode("utf-8")
        self.binding_path.write_bytes(binding_bytes)
        self.expected_binding_sha256 = _sha256_bytes(binding_bytes)
        self.binding_sha256 = self.binding.sha256

        self.schedule_path = Path(self._tempdir.name) / "schedule.json"
        schedule_bytes = json.dumps(_schedule_document(self.schedule)).encode("utf-8")
        self.schedule_path.write_bytes(schedule_bytes)
        self.expected_schedule_sha256 = _sha256_bytes(schedule_bytes)

        self.latency_ledger_path = Path(self._tempdir.name) / "latency.sqlite3"

    def _base_argv(self, *, ledger_path: Path, run_id: str) -> list[str]:
        return [
            "--recall-ledger", str(ledger_path),
            "--recall-run-id", run_id,
            "--context-json", str(self.context_path),
            "--expected-context-sha256", self.expected_context_sha256,
            "--frozen-query-ids-json", str(self.frozen_ids_path),
            "--expected-frozen-query-ids-sha256", self.expected_frozen_ids_sha256,
            "--binding-json", str(self.binding_path),
            "--expected-binding-sha256", self.expected_binding_sha256,
        ]

    def _full_latency_argv(self, *, run_id: str) -> list[str]:
        _populate_latency_ledger(self.latency_ledger_path, run_id=run_id, schedule=self.schedule)
        return [
            "--schedule-json", str(self.schedule_path),
            "--expected-schedule-sha256", self.expected_schedule_sha256,
            "--latency-ledger", str(self.latency_ledger_path),
            "--latency-run-id", run_id,
        ]

    # -- trust anchor -----------------------------------------------------
    #
    # Every test in this section fails inside main()'s digest-verification
    # sequence *before* the recall ledger is ever opened, so none of them
    # need a populated (let alone fully populated) ledger file to exist --
    # only a plausible path. Keeping these cheap matters because a fully
    # populated ledger's hash-chain-verified 1,200-observation append is
    # deliberately expensive (see CanaryRecallAuditLedgerTests).

    def test_context_digest_mismatch_fails_closed(self) -> None:
        argv = self._base_argv(ledger_path=self.ledger_path, run_id="run-a")
        argv[argv.index("--expected-context-sha256") + 1] = "f" * 64  # wrong digest
        exit_code = main(["--report-kind", "recall-only", *argv])
        self.assertNotEqual(exit_code, 0)

    def test_frozen_query_ids_digest_mismatch_fails_closed(self) -> None:
        argv = self._base_argv(ledger_path=self.ledger_path, run_id="run-b")
        argv[argv.index("--expected-frozen-query-ids-sha256") + 1] = "f" * 64
        exit_code = main(["--report-kind", "recall-only", *argv])
        self.assertNotEqual(exit_code, 0)

    def test_binding_digest_mismatch_fails_closed(self) -> None:
        argv = self._base_argv(ledger_path=self.ledger_path, run_id="run-binding")
        argv[argv.index("--expected-binding-sha256") + 1] = "f" * 64
        exit_code = main(["--report-kind", "recall-only", *argv])
        self.assertNotEqual(exit_code, 0)

    # -- strict v2 binding.json loading -------------------------------------
    #
    # Each case below writes a *structurally* tampered binding.json whose
    # bytes correctly match its own --expected-binding-sha256 (the outer
    # digest-verify step is deliberately made to pass), so the test actually
    # exercises the inner structural/byte-identity checks in
    # _binding_from_document, not the outer trust-anchor check above.

    def _tampered_binding_argv(self, *, run_id: str, document: dict) -> list[str]:
        tampered_path = Path(self._tempdir.name) / f"binding-{run_id}.json"
        tampered_bytes = json.dumps(document).encode("utf-8")
        tampered_path.write_bytes(tampered_bytes)
        argv = self._base_argv(ledger_path=self.ledger_path, run_id=run_id)
        argv[argv.index("--binding-json") + 1] = str(tampered_path)
        argv[argv.index("--expected-binding-sha256") + 1] = _sha256_bytes(tampered_bytes)
        return argv

    def test_unknown_top_level_field_is_rejected(self) -> None:
        document = dict(self.binding.to_document(), unexpected_field="x")
        argv = self._tampered_binding_argv(run_id="run-unknown-top", document=document)
        exit_code = main(["--report-kind", "recall-only", *argv])
        self.assertNotEqual(exit_code, 0)

    def test_unknown_nested_configuration_field_is_rejected(self) -> None:
        document = dict(self.binding.to_document())
        document["candidate_search_configuration"] = dict(
            document["candidate_search_configuration"], unexpected_field="x"
        )
        argv = self._tampered_binding_argv(run_id="run-unknown-nested", document=document)
        exit_code = main(["--report-kind", "recall-only", *argv])
        self.assertNotEqual(exit_code, 0)

    def test_wrong_nested_schema_version_is_rejected(self) -> None:
        document = dict(self.binding.to_document())
        document["candidate_search_configuration"] = dict(
            document["candidate_search_configuration"], schema_version="not-a-real-version"
        )
        argv = self._tampered_binding_argv(run_id="run-wrong-nested-version", document=document)
        exit_code = main(["--report-kind", "recall-only", *argv])
        self.assertNotEqual(exit_code, 0)

    def test_contradictory_range_filter_is_rejected(self) -> None:
        document = dict(self.binding.to_document())
        # self.binding is L2, whose canonical range_filter is 0.0.
        document["candidate_search_configuration"] = dict(
            document["candidate_search_configuration"], range_filter=1.0
        )
        argv = self._tampered_binding_argv(run_id="run-contradictory-range-filter", document=document)
        exit_code = main(["--report-kind", "recall-only", *argv])
        self.assertNotEqual(exit_code, 0)

    def test_noncanonical_negative_zero_radius_is_rejected(self) -> None:
        """The COSINE-only -0.0-vs-0.0 finding: an externally supplied
        document carrying the noncanonical -0.0 byte form must be rejected,
        never silently normalized on the loader's behalf. L2 (self.binding's
        own metric) cannot express this case at all -- L2 radius must be
        strictly greater than 0.0 -- so a fresh, otherwise-valid COSINE
        binding is built here to exercise it."""
        cosine_config = SearchConfiguration(
            metric=Metric.COSINE, threshold_label="target-025", radius=0.2,
            index_track=IndexTrack.HNSW, ef=self.schedule.candidate_ef, limit=100, consistency_level="Strong",
        )
        cosine_binding = Stage4EvidenceBinding(
            run_id="exp009-stage4-report-test-cosine", source_revision="0" * 40,
            metric=Metric.COSINE, threshold_stratum="target-025",
            current_ef=self.schedule.last_known_good_ef, candidate_ef=self.schedule.candidate_ef,
            last_known_good_ef=self.schedule.last_known_good_ef,
            candidate_search_configuration=cosine_config, identity=_CONTEXT["identity"],
            dataset002_manifest_sha256=_CONTEXT["dataset002_manifest_sha256"],
            frozen_recall_audit_ids_sha256=_FROZEN_QUERY_IDS_SHA256,
            eligible_workload_sha256=_sha("eligible-workload"), candidate_selection_sha256=_sha("candidate-selection"),
            execution_schedule_sha256=self.schedule.schedule_sha256,
            recall_evidence_schema_version=RECALL_AUDIT_EVALUATOR_VERSION,
            latency_evidence_schema_version="exp009-stage4-execution-schedule-v1",
        )
        document = dict(cosine_binding.to_document())
        document["candidate_search_configuration"] = dict(
            document["candidate_search_configuration"], radius=-0.0
        )
        argv = self._tampered_binding_argv(run_id="run-noncanonical-negative-zero", document=document)
        exit_code = main(["--report-kind", "recall-only", *argv])
        self.assertNotEqual(exit_code, 0)

    def test_valid_canonical_v2_document_still_loads(self) -> None:
        """A genuinely canonical document (this test's own untampered
        ``self.binding.to_document()``) must be accepted by the strict
        loader -- proven by reaching real recall evaluation (an
        OBSERVATION_COUNT_INVALID INCOMPLETE from a 5-of-1200 ledger, never
        a "binding-json is malformed" rejection)."""
        document = dict(self.binding.to_document())
        run_id = self.binding.run_id
        argv = self._tampered_binding_argv(run_id=run_id, document=document)
        _populate_ledger(
            self.ledger_path, run_id=run_id, binding_sha256=self.binding_sha256,
            all_perfect=True, count=5,
        )
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            exit_code = main(["--report-kind", "recall-only", *argv])
        self.assertNotEqual(exit_code, 0)
        output = json.loads(buffer.getvalue())
        self.assertNotIn("error", output)
        self.assertEqual(output["recall_evaluation"]["reason_codes"], ["OBSERVATION_COUNT_INVALID"])

    def test_internally_consistent_but_undigested_context_is_insufficient(self) -> None:
        """A context.json that is perfectly self-consistent with the ledger's
        own data, but supplied without a matching independently-known
        digest, must still fail -- consistency with the ledger is not trust."""
        argv = self._base_argv(ledger_path=self.ledger_path, run_id="run-c")
        argv[argv.index("--expected-context-sha256") + 1] = "0" * 64
        exit_code = main(["--report-kind", "recall-only", *argv])
        self.assertNotEqual(exit_code, 0)

    # -- recall-only mode ---------------------------------------------------

    def test_recall_only_exits_zero_when_passing_with_no_latency_evidence(self) -> None:
        run_id = self.binding.run_id
        _populate_ledger(self.ledger_path, run_id=run_id, binding_sha256=self.binding_sha256, all_perfect=True)
        exit_code = main(
            ["--report-kind", "recall-only", *self._base_argv(ledger_path=self.ledger_path, run_id=run_id)]
        )
        self.assertEqual(exit_code, 0)

    def test_recall_only_report_self_identifies_and_never_claims_qualification(self) -> None:
        run_id = self.binding.run_id
        _populate_ledger(self.ledger_path, run_id=run_id, binding_sha256=self.binding_sha256, all_perfect=True)
        completed = subprocess.run(
            [
                sys.executable, "-m", "vdbench.canary_stage4_qualification_report",
                "--report-kind", "recall-only",
                *self._base_argv(ledger_path=self.ledger_path, run_id=run_id),
            ],
            capture_output=True, text=True, env=_subprocess_env(),
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["report_kind"], "RECALL_AUDIT_ONLY")
        self.assertNotIn("decision_status", payload)
        self.assertEqual(completed.returncode, 0)

    def test_recall_only_exits_nonzero_when_recall_fails(self) -> None:
        _populate_ledger(self.ledger_path, run_id="run-fail", binding_sha256=self.binding_sha256, all_perfect=False)
        exit_code = main(
            ["--report-kind", "recall-only", *self._base_argv(ledger_path=self.ledger_path, run_id="run-fail")]
        )
        self.assertNotEqual(exit_code, 0)

    # -- full-qualification mode --------------------------------------------

    def test_full_qualification_exits_nonzero_when_latency_not_supplied(self) -> None:
        # This test's claim is about the *missing latency* args, not the
        # recall side's own pass/fail -- a small population already forces
        # INCOMPLETE via the recall side too, which is a strict superset of
        # "non-zero exit," so the full 1,200-observation population is
        # unnecessary expense here.
        _populate_ledger(self.ledger_path, run_id="run-pass", binding_sha256=self.binding_sha256, all_perfect=True, count=5)
        exit_code = main(
            ["--report-kind", "full-qualification", *self._base_argv(ledger_path=self.ledger_path, run_id="run-pass")]
        )
        self.assertNotEqual(exit_code, 0)

    def test_full_qualification_reports_incomplete_when_latency_not_supplied(self) -> None:
        _populate_ledger(self.ledger_path, run_id="run-pass", binding_sha256=self.binding_sha256, all_perfect=True)
        completed = subprocess.run(
            [
                sys.executable, "-m", "vdbench.canary_stage4_qualification_report",
                "--report-kind", "full-qualification",
                *self._base_argv(ledger_path=self.ledger_path, run_id="run-pass"),
            ],
            capture_output=True, text=True, env=_subprocess_env(),
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["decision_status"], Stage4DecisionStatus.INCOMPLETE.value)

    def test_full_qualification_exits_zero_when_both_present_bound_and_passing(self) -> None:
        run_id = self.binding.run_id
        _populate_ledger(self.ledger_path, run_id=run_id, binding_sha256=self.binding_sha256, all_perfect=True)
        exit_code = main(
            [
                "--report-kind", "full-qualification",
                *self._base_argv(ledger_path=self.ledger_path, run_id=run_id),
                *self._full_latency_argv(run_id=run_id),
            ]
        )
        self.assertEqual(exit_code, 0)

    def test_full_qualification_exits_nonzero_when_recall_fails_even_with_passing_latency(self) -> None:
        _populate_ledger(self.ledger_path, run_id="run-fail", binding_sha256=self.binding_sha256, all_perfect=False)
        exit_code = main(
            [
                "--report-kind", "full-qualification",
                *self._base_argv(ledger_path=self.ledger_path, run_id="run-fail"),
                *self._full_latency_argv(run_id="run-fail"),
            ]
        )
        self.assertNotEqual(exit_code, 0)

    def test_full_qualification_partial_latency_args_is_an_explicit_error(self) -> None:
        """Supplying only some of the four latency args (e.g. a stray
        --schedule-json without the rest) must fail closed with a clear
        error, not silently ignore the partial input. This is checked before
        the recall evidence's own pass/fail matters, so a small population
        is sufficient."""
        _populate_ledger(self.ledger_path, run_id="run-pass", binding_sha256=self.binding_sha256, all_perfect=True, count=5)
        exit_code = main(
            [
                "--report-kind", "full-qualification",
                *self._base_argv(ledger_path=self.ledger_path, run_id="run-pass"),
                "--schedule-json", str(self.schedule_path),
                "--expected-schedule-sha256", self.expected_schedule_sha256,
            ]
        )
        self.assertNotEqual(exit_code, 0)

    def test_full_qualification_hash_mismatched_schedule_fails_closed(self) -> None:
        """Even a schedule document that is internally self-consistent must
        still fail if its claimed digest doesn't match what was recorded
        independently -- consistency with itself is not trust. The digest
        check happens before the latency ledger is ever opened, so neither
        ledger needs to be populated for this test: a nonexistent latency
        ledger path proves the point just as well, since main() must never
        reach it."""
        _populate_ledger(self.ledger_path, run_id="run-pass", binding_sha256=self.binding_sha256, all_perfect=True, count=5)
        exit_code = main(
            [
                "--report-kind", "full-qualification",
                *self._base_argv(ledger_path=self.ledger_path, run_id="run-pass"),
                "--schedule-json", str(self.schedule_path),
                "--expected-schedule-sha256", "0" * 64,  # wrong digest
                "--latency-ledger", str(Path(self._tempdir.name) / "never-created.sqlite3"),
                "--latency-run-id", "run-pass",
            ]
        )
        self.assertNotEqual(exit_code, 0)

    def test_full_qualification_no_free_form_latency_flag_exists(self) -> None:
        """ADR-008's Stage-4 evidence-binding repair explicitly prohibits a
        free-form latency-evaluation document; the CLI must not accept one
        under any flag name. argparse rejects the unrecognized flag itself
        by exiting non-zero before main()'s own logic ever runs."""
        with self.assertRaises(SystemExit) as ctx:
            main(["--report-kind", "full-qualification", "--latency-evaluation-json", "x"])
        self.assertNotEqual(ctx.exception.code, 0)

    def test_report_never_contains_an_apply_or_promote_action_field(self) -> None:
        """The disclaimer text legitimately says "does not apply, promote,
        route, or modify" -- banning those words outright would also ban the
        very sentence that proves no action was taken. What must actually be
        absent is a structured field that *represents* such an action. This
        holds regardless of the recall verdict, so a small population is
        sufficient -- the report is emitted either way."""
        _populate_ledger(self.ledger_path, run_id="run-pass", binding_sha256=self.binding_sha256, all_perfect=True, count=5)
        completed = subprocess.run(
            [
                sys.executable, "-m", "vdbench.canary_stage4_qualification_report",
                "--report-kind", "recall-only",
                *self._base_argv(ledger_path=self.ledger_path, run_id="run-pass"),
            ],
            capture_output=True, text=True, env=_subprocess_env(),
        )
        payload = json.loads(completed.stdout)
        forbidden_keys = {
            "action", "applied_action", "promoted", "route_installed",
            "candidate_route", "mutation", "apply_result",
        }
        self.assertEqual(forbidden_keys & set(payload.keys()), set())
        self.assertEqual(
            forbidden_keys & set(payload.get("recall_evaluation", {}).keys()), set()
        )
        self.assertIn(HUMAN_AUTHORIZATION_NOTICE, completed.stdout)

    def test_report_json_is_deterministic_across_two_runs(self) -> None:
        # Determinism holds regardless of the recall verdict; a small
        # population is sufficient and keeps this test cheap.
        _populate_ledger(self.ledger_path, run_id="run-pass", binding_sha256=self.binding_sha256, all_perfect=True, count=5)
        outputs = []
        for _ in range(2):
            completed = subprocess.run(
                [
                    sys.executable, "-m", "vdbench.canary_stage4_qualification_report",
                    "--report-kind", "recall-only",
                    *self._base_argv(ledger_path=self.ledger_path, run_id="run-pass"),
                ],
                capture_output=True, text=True, env=_subprocess_env(),
            )
            outputs.append(completed.stdout)
        self.assertEqual(outputs[0], outputs[1])


class NoActuationTests(unittest.TestCase):
    """Strengthened proof, beyond banning pymilvus, that this module cannot
    perform or trigger any actuation, routing, or state mutation."""

    def setUp(self) -> None:
        self.source = Path(report_module.__file__).read_text(encoding="utf-8")

    def test_no_pymilvus_or_raw_network_import(self) -> None:
        for forbidden in ("pymilvus", "import socket", "import requests", "urllib", "grpc"):
            self.assertNotIn(forbidden, self.source)

    def test_no_actuation_or_routing_module_import(self) -> None:
        forbidden_imports = (
            "from .actuation import",
            "from .actuation_persistence import",
            "from .milvus_actuation import",
            "from .milvus import",
            "from .milvus_serving import",
            "from .milvus_host_executor import",
            "from .canary_route_authority import",
            "from .canary_route_state import",
            "from .canary_routing import",
            "from .canary_activation import",
            "from .canary_rollback import",
            "from .canary_live_runner import",
            "from .canary_serial_runner import",
            "from .canary_approval import",
            "from .canary_grant_store import",
            "from .last_known_good import",
        )
        for forbidden in forbidden_imports:
            self.assertNotIn(forbidden, self.source)

    def test_no_subprocess_import_in_production_module(self) -> None:
        self.assertNotIn("import subprocess", self.source)

    def test_no_write_or_delete_filesystem_calls(self) -> None:
        for forbidden in ("os.remove", "shutil.rmtree", ".write(", "os.chmod"):
            self.assertNotIn(forbidden, self.source)

    def test_running_the_report_does_not_modify_the_ledgers(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            schedule = _build_schedule()
            binding = _build_binding(schedule)
            binding_document = binding.to_document()
            ledger_path = Path(tempdir) / "recall_audit.sqlite3"
            _populate_ledger(
                ledger_path, run_id="run-readonly-check", binding_sha256=binding.sha256,
                all_perfect=True, count=5,
            )
            before = hashlib.sha256(ledger_path.read_bytes()).hexdigest()

            context_path = Path(tempdir) / "context.json"
            context_bytes = json.dumps(_context_document()).encode("utf-8")
            context_path.write_bytes(context_bytes)
            frozen_ids_path = Path(tempdir) / "frozen_query_ids.json"
            frozen_ids_bytes = _FROZEN_QUERY_IDS_BYTES
            frozen_ids_path.write_bytes(frozen_ids_bytes)
            binding_path = Path(tempdir) / "binding.json"
            binding_bytes = json.dumps(binding_document).encode("utf-8")
            binding_path.write_bytes(binding_bytes)

            main(
                [
                    "--report-kind", "recall-only",
                    "--recall-ledger", str(ledger_path),
                    "--recall-run-id", "run-readonly-check",
                    "--context-json", str(context_path),
                    "--expected-context-sha256", _sha256_bytes(context_bytes),
                    "--frozen-query-ids-json", str(frozen_ids_path),
                    "--expected-frozen-query-ids-sha256", _sha256_bytes(frozen_ids_bytes),
                    "--binding-json", str(binding_path),
                    "--expected-binding-sha256", _sha256_bytes(binding_bytes),
                ]
            )

            after = hashlib.sha256(ledger_path.read_bytes()).hexdigest()
            self.assertEqual(before, after)


def _subprocess_env():
    import os

    env = dict(os.environ)
    src_path = str(Path(__file__).resolve().parent.parent / "src")
    env["PYTHONPATH"] = src_path
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


if __name__ == "__main__":
    unittest.main()
