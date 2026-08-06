"""Human-facing, read-only Stage-4 qualification report/CLI.

Purpose:
    Report on durable recall-audit (and, optionally, latency) evidence for a
    human to review. Two distinct, explicitly-named report kinds:
    ``recall-only`` (emits ``Stage4RecallAuditReport``, no latency evidence
    required) and ``full-qualification`` (emits ``Stage4Decision``, requires
    both evidence streams present and bound to the identical ADR-008
    ``Stage4EvidenceBinding`` digest).
Inputs:
    A path to an existing ``CanaryRecallAuditLedger`` database; an expected
    ``(SearchConfiguration, WorkloadIdentityBinding, dataset002 manifest)``
    context and a frozen query-ID population; and the canonical
    ``Stage4EvidenceBinding`` document -- every one of these is verified
    against an independently-supplied expected SHA-256 digest before being
    trusted. For full-qualification only, the latency side is additionally
    rebuilt from a verified ``Stage4ExecutionSchedule`` document plus a path
    to an existing, hash-chain-verified ``Stage4ExecutionLedger``. There is
    no free-form latency-evaluation input: ADR-008's Stage-4
    evidence-binding repair explicitly prohibits one, because a hand-authored
    document cannot prove it describes the same run/configuration as the
    recall evidence it would be combined with.
Outputs:
    Deterministic JSON to stdout; a process exit code (0 only on a passing
    verdict for the requested report kind).
Dependencies:
    Pure evidence/evaluation modules, plus ``Stage4ExecutionLedger`` (a
    durable, hash-chain-verifying reader -- never a live Milvus dependency).
Failure modes:
    Any missing ledger/schedule path, digest mismatch, malformed context or
    binding, or incomplete/failing/binding-mismatched evidence yields a
    non-zero exit code and a JSON error or INCOMPLETE/FAILING report --
    never a fabricated passing result.
Scope:
    This module never applies, promotes, routes, or mutates any Milvus
    state. It has no PyMilvus, network, actuation, routing, or live-runner
    dependency, and it never writes to either ledger it reads -- it opens
    existing files only, and refuses to proceed if they do not already
    exist, so it cannot side-effect a new database into existence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

from .canary_execution_ledger import Stage4ExecutionLedger, Stage4LedgerError
from .canary_recall_audit_evaluation import (
    EvaluationStatus,
    Stage4RecallAuditEvaluation,
    Stage4RecallAuditReport,
    build_recall_audit_report,
    evaluate_recall_audit_evidence,
)
from .canary_recall_audit_ledger import CanaryRecallAuditLedger, RecallAuditLedgerError
from .canary_schedule import (
    CanaryRouteKind,
    Stage4ExecutionSchedule,
    Stage4ScheduleStep,
    Stage4ScheduleStepKind,
)
from .canary_schedule_evaluation import Stage4ScheduleEvaluation
from .canary_stage4_decision import Stage4Decision, Stage4DecisionStatus, combine_stage4_decision
from .canary_stage4_evidence_binding import Stage4EvidenceBinding
from .canary_stage4_latency_evidence import Stage4LatencyEvidence, build_stage4_latency_evidence
from .canary_workload import WorkloadIdentityBinding
from .config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from .artifacts import canonical_json_bytes
from .search_configuration_digest import (
    SEARCH_CONFIGURATION_DOCUMENT_SCHEMA_VERSION,
    search_configuration_document,
)


__all__ = [
    "HUMAN_AUTHORIZATION_NOTICE",
    "build_qualification_document",
    "main",
]


HUMAN_AUTHORIZATION_NOTICE = (
    "This report is read-only. It does not apply, promote, route, or modify "
    "any Milvus configuration. Any live action requires separate, explicit "
    "human authorization."
)

# Explicit, immutable expected-field sets for the active v2 binding.json
# loader. An externally supplied document is rejected -- not silently
# trimmed -- if its key set differs from these in any way.
_BINDING_DOCUMENT_FIELDS = frozenset(
    {
        "schema_version",
        "run_id",
        "source_revision",
        "metric",
        "threshold_stratum",
        "current_ef",
        "candidate_ef",
        "last_known_good_ef",
        "candidate_search_configuration",
        "identity",
        "dataset002_manifest_sha256",
        "frozen_recall_audit_ids_sha256",
        "eligible_workload_sha256",
        "candidate_selection_sha256",
        "execution_schedule_sha256",
        "recall_evidence_schema_version",
        "latency_evidence_schema_version",
    }
)
_SEARCH_CONFIGURATION_DOCUMENT_FIELDS = frozenset(
    {
        "schema_version",
        "metric",
        "threshold_label",
        "radius",
        "range_filter",
        "index_track",
        "ef",
        "limit",
        "consistency_level",
    }
)


class _ReportError(RuntimeError):
    """A fail-closed condition in report assembly, never in evidence itself."""


def build_qualification_document(decision: Stage4Decision) -> dict:
    """Build the deterministic JSON document for a full Stage-4 qualification."""

    return {
        "decision_status": decision.decision_status.value,
        "latency_status": decision.latency_status.value,
        "recall_status": decision.recall_status.value,
        "evidence_binding_sha256": decision.evidence_binding_sha256,
        "reason_codes": list(decision.reason_codes),
        "consumed_evidence_digests": list(decision.consumed_evidence_digests),
        "recall_evaluation": _recall_evaluation_document(decision.recall_evaluation),
        "latency_evaluation": _latency_evaluation_document(decision.latency_evaluation),
        "human_authorization_notice": HUMAN_AUTHORIZATION_NOTICE,
    }


def _build_recall_only_document(report: Stage4RecallAuditReport) -> dict:
    return {
        "report_kind": report.report_kind,
        "status": report.status.value,
        "recall_evaluation": _recall_evaluation_document(report.recall_evaluation),
        "human_authorization_notice": HUMAN_AUTHORIZATION_NOTICE,
    }


def _recall_evaluation_document(evaluation: Stage4RecallAuditEvaluation | None) -> dict | None:
    if evaluation is None:
        return None
    return {
        "recall_audit_complete_and_passing": evaluation.recall_audit_complete_and_passing,
        "status": evaluation.status.value,
        "reason_codes": list(evaluation.reason_codes),
        "sample_count": evaluation.sample_count,
        "observed_mean": evaluation.observed_mean,
        "margin": evaluation.margin,
        "lower_bound": evaluation.lower_bound,
        "confidence_level": evaluation.confidence_level,
        "recall_floor": evaluation.recall_floor,
        "alpha": evaluation.alpha,
        "evaluator_method_version": evaluation.evaluator_method_version,
        "evidence_digest": evaluation.evidence_digest,
    }


def _latency_evaluation_document(evaluation: Stage4ScheduleEvaluation | None) -> dict | None:
    if evaluation is None:
        return None
    return {
        "finite_manifest_latency_applicable": evaluation.finite_manifest_latency_applicable,
        "reason_codes": list(evaluation.reason_codes),
        "baseline_median_ms": evaluation.baseline_median_ms,
        "baseline_p95_ms": evaluation.baseline_p95_ms,
        "candidate_latency_count": evaluation.candidate_latency_count,
        "candidate_latency_max_ms": evaluation.candidate_latency_max_ms,
        "finite_population_coverage_probability": (
            evaluation.finite_population_coverage_probability
        ),
    }


def _load_and_verify(path: Path, *, expected_sha256: str, field: str) -> bytes:
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise _ReportError(f"{field} could not be read: {exc}") from exc
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_sha256:
        raise _ReportError(
            f"{field} digest mismatch: internal consistency with a ledger is not "
            f"trust; expected {expected_sha256}, got {actual}"
        )
    return data


def _context_from_document(
    document: dict,
) -> tuple[SearchConfiguration, WorkloadIdentityBinding, str, int]:
    try:
        sc_doc = document["search_configuration"]
        search_configuration = SearchConfiguration(
            metric=Metric(sc_doc["metric"]),
            threshold_label=sc_doc["threshold_label"],
            radius=sc_doc["radius"],
            index_track=IndexTrack(sc_doc["index_track"]),
            ef=sc_doc["ef"],
            limit=sc_doc["limit"],
            consistency_level=sc_doc["consistency_level"],
        )
        search_configuration.validate()
        identity = WorkloadIdentityBinding(**document["identity"])
        identity.validate()
        manifest_sha256 = document["dataset002_manifest_sha256"]
        schema_version = document["dataset002_schema_version"]
    except (KeyError, TypeError, ValueError, ContractViolation) as exc:
        raise _ReportError(f"context.json is malformed: {exc}") from exc
    return search_configuration, identity, manifest_sha256, schema_version


def _binding_from_document(document: dict) -> Stage4EvidenceBinding:
    """Reconstruct the canonical ADR-008 evidence binding via its own public
    constructor only -- never a free-form/loosely-validated document. The
    binding's own ``__post_init__`` re-validates every field; this function
    adds no additional trust beyond the byte-level digest check already
    performed by ``_load_and_verify`` on the raw file contents.

    Strict v2 loading, two layers:

    1. Explicit immutable field sets for the top-level document and the
       embedded ``candidate_search_configuration`` document give a fast,
       clearly-named rejection for gross structural problems (unknown/missing
       fields, wrong nested schema version) before any object is built.
    2. After the complete ``Stage4EvidenceBinding`` is reconstructed, the
       *entire supplied document* must be byte-identical to
       ``canonical_json_bytes(reconstructed.to_document())``. This is the
       comprehensive final gate: it covers every field -- schema_version,
       run/source fields, metric/threshold/ef headers,
       candidate_search_configuration, identity, every governed SHA-256
       field, both evidence schema versions -- so a noncanonical numeric
       representation, a contradictory derived field, or any other
       byte-level divergence anywhere in the document is caught even if it
       happened to survive layer 1 and the object's own field-level
       validation.
    """

    try:
        if not isinstance(document, dict) or frozenset(document) != _BINDING_DOCUMENT_FIELDS:
            raise ValueError("binding document top-level fields are invalid")
        sc_doc = document["candidate_search_configuration"]
        if not isinstance(sc_doc, dict) or frozenset(sc_doc) != _SEARCH_CONFIGURATION_DOCUMENT_FIELDS:
            raise ValueError("candidate_search_configuration fields are invalid")
        if sc_doc["schema_version"] != SEARCH_CONFIGURATION_DOCUMENT_SCHEMA_VERSION:
            raise ValueError("candidate_search_configuration schema_version is unsupported")
        identity_document = document["identity"]
        if not isinstance(identity_document, dict):
            raise ValueError("identity must be a document")

        candidate_search_configuration = SearchConfiguration(
            metric=Metric(sc_doc["metric"]),
            threshold_label=sc_doc["threshold_label"],
            radius=sc_doc["radius"],
            index_track=IndexTrack(sc_doc["index_track"]),
            ef=sc_doc["ef"],
            limit=sc_doc["limit"],
            consistency_level=sc_doc["consistency_level"],
        )
        binding = Stage4EvidenceBinding(
            run_id=document["run_id"],
            source_revision=document["source_revision"],
            metric=Metric(document["metric"]),
            threshold_stratum=document["threshold_stratum"],
            current_ef=document["current_ef"],
            candidate_ef=document["candidate_ef"],
            last_known_good_ef=document["last_known_good_ef"],
            candidate_search_configuration=candidate_search_configuration,
            identity=WorkloadIdentityBinding(**identity_document),
            dataset002_manifest_sha256=document["dataset002_manifest_sha256"],
            frozen_recall_audit_ids_sha256=document["frozen_recall_audit_ids_sha256"],
            eligible_workload_sha256=document["eligible_workload_sha256"],
            candidate_selection_sha256=document["candidate_selection_sha256"],
            execution_schedule_sha256=document["execution_schedule_sha256"],
            recall_evidence_schema_version=document["recall_evidence_schema_version"],
            latency_evidence_schema_version=document["latency_evidence_schema_version"],
            schema_version=document["schema_version"],
        )

        supplied_bytes = canonical_json_bytes(document)
        canonical_bytes = canonical_json_bytes(binding.to_document())
        if supplied_bytes != canonical_bytes:
            raise ValueError(
                "binding document is not byte-identical to its canonical "
                "reconstruction (noncanonical numeric representation, a "
                "contradictory derived field, or a value that does not "
                "round-trip through the binding's own public constructor)"
            )
        return binding
    except (KeyError, TypeError, ValueError, ContractViolation) as exc:
        raise _ReportError(f"binding-json is malformed: {exc}") from exc


def _schedule_from_document(document: dict) -> Stage4ExecutionSchedule:
    """Reconstruct the frozen Stage-4 execution schedule via its own public
    constructors only. ``Stage4ExecutionSchedule.__post_init__`` independently
    recomputes and compares its own content digest, so a malformed or
    tampered document fails closed here even if the outer file-level digest
    check somehow passed -- this is the same defense-in-depth pattern
    already used by ``Stage4EvidenceBinding`` and every other reconstructed
    value object in this module."""

    try:
        steps = tuple(
            Stage4ScheduleStep(
                execution_index=step["execution_index"],
                kind=Stage4ScheduleStepKind(step["kind"]),
                expected_ef=step["expected_ef"],
                sweep_index=step["sweep_index"],
                control_query_id=step["control_query_id"],
                control_vector_sha256=step["control_vector_sha256"],
                routing_sequence_index=step["routing_sequence_index"],
                occurrence_id=step["occurrence_id"],
                dataset_query_id=step["dataset_query_id"],
                vector_sha256=step["vector_sha256"],
                threshold_radius=step["threshold_radius"],
                range_filter=step["range_filter"],
                limit=step["limit"],
                route_kind=(
                    None if step["route_kind"] is None else CanaryRouteKind(step["route_kind"])
                ),
            )
            for step in document["steps"]
        )
        return Stage4ExecutionSchedule(
            schema_version=document["schema_version"],
            plan_sha256=document["plan_sha256"],
            metric=Metric(document["metric"]),
            threshold_stratum=document["threshold_stratum"],
            candidate_ef=document["candidate_ef"],
            last_known_good_ef=document["last_known_good_ef"],
            control_ef=document["control_ef"],
            steps=steps,
            schedule_sha256=document["schedule_sha256"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise _ReportError(f"schedule-json is malformed: {exc}") from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="canary_stage4_qualification_report",
        description=(
            "Read-only Stage-4 recall-audit / qualification report. "
            + HUMAN_AUTHORIZATION_NOTICE
        ),
    )
    parser.add_argument(
        "--report-kind", required=True, choices=["recall-only", "full-qualification"]
    )
    parser.add_argument("--recall-ledger", required=True, type=Path)
    parser.add_argument("--recall-run-id", required=True)
    parser.add_argument("--context-json", required=True, type=Path)
    parser.add_argument("--expected-context-sha256", required=True)
    parser.add_argument("--frozen-query-ids-json", required=True, type=Path)
    parser.add_argument("--expected-frozen-query-ids-sha256", required=True)
    parser.add_argument("--binding-json", required=True, type=Path)
    parser.add_argument("--expected-binding-sha256", required=True)
    # Latency inputs are required only for --report-kind full-qualification;
    # there is deliberately no free-form latency-evaluation document flag --
    # ADR-008's Stage-4 evidence-binding repair prohibits one outright.
    parser.add_argument("--schedule-json", type=Path, default=None)
    parser.add_argument("--expected-schedule-sha256", default=None)
    parser.add_argument("--latency-ledger", type=Path, default=None)
    parser.add_argument("--latency-run-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        context_bytes = _load_and_verify(
            args.context_json,
            expected_sha256=args.expected_context_sha256,
            field="context.json",
        )
        search_configuration, identity, manifest_sha256, schema_version = (
            _context_from_document(json.loads(context_bytes))
        )

        frozen_ids_bytes = _load_and_verify(
            args.frozen_query_ids_json,
            expected_sha256=args.expected_frozen_query_ids_sha256,
            field="frozen_query_ids.json",
        )
        frozen_query_ids = frozenset(json.loads(frozen_ids_bytes))

        binding_bytes = _load_and_verify(
            args.binding_json,
            expected_sha256=args.expected_binding_sha256,
            field="binding.json",
        )
        binding = _binding_from_document(json.loads(binding_bytes))

        if not args.recall_ledger.exists():
            raise _ReportError(
                "recall ledger does not exist; this reporter never creates one"
            )
        ledger = CanaryRecallAuditLedger(
            args.recall_ledger, run_id=args.recall_run_id, binding_sha256=binding.sha256
        )
        observations = ledger.records()

        recall_evaluation = evaluate_recall_audit_evidence(
            expected_query_ids=frozen_query_ids,
            search_configuration=search_configuration,
            identity=identity,
            dataset002_manifest_sha256=manifest_sha256,
            dataset002_schema_version=schema_version,
            observations=observations,
            binding=binding,
            frozen_query_ids_sha256=args.expected_frozen_query_ids_sha256,
        )
    except (
        _ReportError,
        RecallAuditLedgerError,
        ValueError,
        TypeError,
        OSError,
        json.JSONDecodeError,
    ) as exc:
        print(json.dumps({"error": str(exc)}, sort_keys=True))
        return 1

    if args.report_kind == "recall-only":
        report = build_recall_audit_report(recall_evaluation)
        print(json.dumps(_build_recall_only_document(report), sort_keys=True))
        return 0 if report.status is EvaluationStatus.PASSING else 1

    latency_evidence = None
    latency_args = (
        args.schedule_json,
        args.expected_schedule_sha256,
        args.latency_ledger,
        args.latency_run_id,
    )
    if any(value is not None for value in latency_args):
        if any(value is None for value in latency_args):
            print(
                json.dumps(
                    {
                        "error": (
                            "full-qualification latency inputs must be supplied "
                            "together: --schedule-json, --expected-schedule-sha256, "
                            "--latency-ledger, --latency-run-id"
                        )
                    },
                    sort_keys=True,
                )
            )
            return 1
        try:
            schedule_bytes = _load_and_verify(
                args.schedule_json,
                expected_sha256=args.expected_schedule_sha256,
                field="schedule.json",
            )
            schedule = _schedule_from_document(json.loads(schedule_bytes))

            if not args.latency_ledger.exists():
                raise _ReportError(
                    "latency ledger does not exist; this reporter never creates one"
                )
            latency_ledger = Stage4ExecutionLedger(
                args.latency_ledger, run_id=args.latency_run_id, schedule=schedule
            )

            latency_evidence = build_stage4_latency_evidence(
                binding=binding, schedule=schedule, ledger=latency_ledger
            )
        except (
            _ReportError,
            Stage4LedgerError,
            ValueError,
            TypeError,
            OSError,
            json.JSONDecodeError,
        ) as exc:
            print(json.dumps({"error": str(exc)}, sort_keys=True))
            return 1

    decision = combine_stage4_decision(
        latency_evidence=latency_evidence, recall_evaluation=recall_evaluation
    )
    print(json.dumps(build_qualification_document(decision), sort_keys=True))
    return 0 if decision.decision_status is Stage4DecisionStatus.PASSING else 1


if __name__ == "__main__":
    sys.exit(main())
