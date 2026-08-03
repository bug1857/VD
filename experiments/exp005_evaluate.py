"""Offline EXP-005 Stages 8–9 evaluation from persisted live-shadow traces.

This script never imports PyMilvus or connects to Milvus.  It reconstructs the
three captured raw windows, derives real detector evidence, evaluates the
policy in DRY_RUN mode with no fabricated qualification or response estimate,
and proves the resulting NO_CHANGE is a safe-boundary no-op.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import tempfile
from typing import NoReturn

from vdbench.actuation import ActuationContext, SafeActuationBoundary
from vdbench.actuation_persistence import JsonlAuditSink
from vdbench.config import Metric
from vdbench.drift import evaluate_drift_decision
from vdbench.policy import (
    PolicyAction,
    PolicyMode,
    PreActionSafety,
    QualificationResult,
    evaluate_tuning_policy,
)
from vdbench.shadow_artifacts import load_persisted_shadow_trace_envelope
from vdbench.shadow_extraction import extract_window_evidence
from vdbench.shadow_window import AssembledShadowWindow, assemble_shadow_window


WINDOW_ROLES = ("reference", "current-1", "current-2")
TRACE_COUNT_PER_WINDOW = 4
AUDIT_ID = "exp005-l2-target075-001:offline-dry-run"
DETECTOR_SEED = 20260804
_NO_ACTUATION_FLAG_NAMES = (
    "collection_create_called",
    "collection_mutation_called",
    "restore_last_known_good_called",
    "rollback_called",
    "start_canary_called",
)


class EvaluationError(RuntimeError):
    """Raised when immutable EXP-005 evidence is incomplete or incompatible."""


class _NoActuationClient:
    """Fails immediately if the safe boundary tries any client action."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> NoReturn:
        self.calls.append(name)
        raise AssertionError(f"offline NO_CHANGE invoked client.{name}")


class _NoOpController:
    """Records accidental automatic-action disabling without performing I/O."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def disable_automatic_actions(self, *, audit_id: str, reason: str) -> None:
        self.calls.append((audit_id, reason))


def _load_window(
    capture_dir: Path,
    *,
    capture_id: str,
    role: str,
) -> AssembledShadowWindow:
    envelopes = tuple(
        load_persisted_shadow_trace_envelope(
            capture_dir / "traces" / f"{role}-{sequence_index}.json"
        )
        for sequence_index in range(TRACE_COUNT_PER_WINDOW)
    )
    window = assemble_shadow_window(
        window_id=f"{capture_id}:{role}",
        envelopes=envelopes,
    )
    if not window.complete:
        raise EvaluationError(
            f"ASSEMBLED_WINDOW_INCOMPLETE:{role}:{','.join(window.reason_codes)}"
        )
    return window


def _live_capture_no_actuation(completion: object) -> dict[str, object]:
    if not isinstance(completion, dict):
        raise EvaluationError("CAPTURE_COMPLETION_INVALID")
    flags = completion.get("no_actuation")
    if not isinstance(flags, dict) or set(flags) != set(_NO_ACTUATION_FLAG_NAMES):
        raise EvaluationError("CAPTURE_NO_ACTUATION_FLAGS_INVALID")
    if any(type(flags[name]) is not bool for name in _NO_ACTUATION_FLAG_NAMES):
        raise EvaluationError("CAPTURE_NO_ACTUATION_FLAGS_INVALID")
    no_actuation = all(flags[name] is False for name in _NO_ACTUATION_FLAG_NAMES)
    if not no_actuation:
        raise EvaluationError("LIVE_CAPTURE_ACTUATION_FLAG_SET")
    return {"no_actuation": no_actuation, "flags": flags}


def _context_from_current(
    current: AssembledShadowWindow,
    qualification: QualificationResult,
    audit_ids: tuple[int | str, ...],
) -> ActuationContext:
    trace = current.envelopes[0].trace
    if trace is None or trace.flat_identity is None or trace.hnsw_identity is None:
        raise EvaluationError("CURRENT_TRACE_IDENTITY_MISSING")
    hnsw_snapshot = trace.hnsw_identity.pre_snapshot
    if hnsw_snapshot is None:
        raise EvaluationError("CURRENT_HNSW_SNAPSHOT_MISSING")
    return ActuationContext(
        metric=current.metric,
        threshold_stratum=current.threshold_stratum,
        collection_name=hnsw_snapshot.collection_name,
        configuration_identity=trace.configuration_identity,
        index_identity=trace.hnsw_identity.expected_binding_id,
        flat_index_identity=trace.flat_identity.expected_binding_id,
        data_identity=trace.data_identity,
        audited_query_ids=audit_ids,
        last_known_good=qualification,
        occurred_at_utc="2026-08-03T08:04:38.860624Z",
    )


def evaluate(capture_dir: Path) -> None:
    """Run the approved, entirely offline reconstruction and no-op proof."""

    completion_path = capture_dir / "capture_completion.json"
    try:
        completion = json.loads(completion_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvaluationError(f"CAPTURE_COMPLETION_UNREADABLE:{type(exc).__name__}") from exc
    live_capture = _live_capture_no_actuation(completion)
    capture_id = completion.get("capture_id") if isinstance(completion, dict) else None
    if not isinstance(capture_id, str) or not capture_id:
        raise EvaluationError("CAPTURE_ID_INVALID")

    windows = {
        role: _load_window(capture_dir, capture_id=capture_id, role=role)
        for role in WINDOW_ROLES
    }
    reference = windows["reference"]
    current_one = windows["current-1"]
    current_two = windows["current-2"]
    if not all(window.complete for window in windows.values()):
        raise EvaluationError("ASSEMBLED_WINDOW_INCOMPLETE")

    metric = Metric(reference.metric)
    previous = extract_window_evidence(
        reference_window=reference,
        current_window=current_one,
        metric=metric,
        detector_seed=DETECTOR_SEED,
    )
    current = extract_window_evidence(
        reference_window=reference,
        current_window=current_two,
        metric=metric,
        detector_seed=DETECTOR_SEED,
    )
    if not previous.complete:
        raise EvaluationError(
            f"WINDOW_EVIDENCE_INCOMPLETE:current-1:{','.join(previous.reason_codes)}"
        )
    if not current.complete:
        raise EvaluationError(
            f"WINDOW_EVIDENCE_INCOMPLETE:current-2:{','.join(current.reason_codes)}"
        )

    drift = evaluate_drift_decision(previous, current)
    trace = current_two.envelopes[0].trace
    if trace is None or trace.flat_identity is None or trace.hnsw_identity is None:
        raise EvaluationError("CURRENT_TRACE_IDENTITY_MISSING")
    qualification = QualificationResult(
        qualified=False,
        ef=None,
        reasons=("EXP005_NO_LIVE_QUALIFICATION",),
    )
    policy = evaluate_tuning_policy(
        drift,
        current_ef=trace.last_known_good_ef,
        response_estimates={},
        pre_action=PreActionSafety(
            metric=metric,
            threshold_stratum=current_two.threshold_stratum,
            configuration_identity=trace.configuration_identity,
            index_identity=trace.hnsw_identity.expected_binding_id,
            flat_index_identity=trace.flat_identity.expected_binding_id,
            data_identity=trace.data_identity,
            response_model_provenance="EXP005_NO_RESPONSE_ESTIMATES",
        ),
        canary_observation=None,
        last_known_good=qualification,
        mode=PolicyMode.DRY_RUN,
        threshold_stratum=current_two.threshold_stratum,
        audit_id=AUDIT_ID,
    )
    if policy.action is not PolicyAction.NO_CHANGE:
        raise EvaluationError(f"POLICY_NOT_NO_CHANGE:{policy.action}")

    provenance = current.provenance
    if provenance is None:
        raise EvaluationError("CURRENT_EVIDENCE_PROVENANCE_MISSING")
    audit_ids = tuple(provenance.current_audit_ids)
    context = _context_from_current(current_two, qualification, audit_ids)
    client = _NoActuationClient()
    controller = _NoOpController()
    with tempfile.TemporaryDirectory(prefix="vdbench-exp005-audit-") as directory:
        sink = JsonlAuditSink(Path(directory) / "offline-noop-audit.jsonl")
        actuation = SafeActuationBoundary(client, sink, controller).execute(
            policy, context
        )
        audit_record_persisted = sink.contains(AUDIT_ID)
    offline_noop = {
        "no_actuation": (
            actuation.executed is False
            and actuation.success is True
            and client.calls == []
            and controller.calls == []
        ),
        "fake_client_calls": client.calls,
        "controller_calls": controller.calls,
        "actuation_executed": actuation.executed,
        "audit_record_persisted_in_temporary_sink": audit_record_persisted,
    }
    if not offline_noop["no_actuation"]:
        raise EvaluationError("OFFLINE_NO_OP_PROOF_FAILED")

    print(f"DriftDecision: {drift!r}")
    print(f"PolicyDecision: {policy!r}")
    print(f"ActuationResult: {actuation!r}")
    print("live-capture evidence (read from capture_completion.json):")
    print(json.dumps(live_capture, sort_keys=True))
    print("offline no-op proof:")
    print(json.dumps(offline_noop, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--capture-dir",
        type=Path,
        required=True,
        help="EXP-005 capture directory containing capture_completion.json and traces/.",
    )
    args = parser.parse_args()
    try:
        evaluate(args.capture_dir)
    except EvaluationError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc


if __name__ == "__main__":
    main()
